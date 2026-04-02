import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, it, expect, vi, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { CoalesceApiError } from "../../src/client.js";
import { resolveCacheResourceUri } from "../../src/cache-dir.js";
import { planPipeline } from "../../src/services/pipelines/planning.js";
import {
  createPipelineFromPlan,
  createPipelineFromSql,
} from "../../src/services/pipelines/execution.js";
import { registerPipelineTools, buildPlanConfirmationToken } from "../../src/mcp/pipelines.js";
import {
  createMockClient,
  buildSourceColumn,
  buildSourceNode,
  buildCreatedStageNode,
} from "../helpers/fixtures.js";

// Mock completeNodeConfiguration so pipeline tests don't need corpus/repo files
vi.mock("../../src/services/config/intelligent.js", () => ({
  completeNodeConfiguration: vi.fn(async (client: any, params: { workspaceID: string; nodeID: string }) => {
    const node = await client.get(`/api/v1/workspaces/${params.workspaceID}/nodes/${params.nodeID}`);
    return {
      node,
      schemaSource: "corpus",
      classification: { required: [], conditionalRequired: [], optionalWithDefaults: [], contextual: [], columnSelectors: [] },
      context: { hasMultipleSources: false, hasAggregates: false, hasTimestampColumns: false, hasType2Pattern: false, materializationType: "table" },
      appliedConfig: {},
      configChanges: { required: {}, contextual: {}, preserved: {}, defaults: {} },
      columnAttributeChanges: { applied: [], reasoning: [] },
      reasoning: [],
      detectedPatterns: { candidateColumns: [] },
    };
  }),
}));

const fixtureRepoPath = resolve("tests/fixtures/repo-backed-coalesce");
const originalEnv = process.env;
const tempDirs: string[] = [];

afterEach(() => {
  vi.restoreAllMocks();
  process.env = originalEnv;
  for (const tempDir of tempDirs.splice(0, tempDirs.length)) {
    rmSync(tempDir, { recursive: true, force: true });
  }
});

function buildCreatedProjectionNode(predecessorNodeID: string) {
  return {
    ...buildCreatedStageNode(predecessorNodeID),
    name: "CWRK_CUSTOMER",
    config: {},
  };
}

describe("Pipeline Tools", () => {
  it("registers the pipeline tools without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerPipelineTools(server, client as any);
    expect(true).toBe(true);
  });

  it("plan-pipeline tool accepts ref-based SQL unchanged", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source", locationName: "RAW" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(buildSourceNode("source-1", "CUSTOMER", "RAW"));
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    registerPipelineTools(server, client as any);

    const planToolCall = toolSpy.mock.calls.find(
      (call) => call[0] === "plan_pipeline"
    );
    const handler = planToolCall?.[2] as
      | ((params: { workspaceID: string; sql: string }) => Promise<{
          isError?: boolean;
          content: { type: "text"; text: string }[];
        }>)
      | undefined;

    expect(typeof handler).toBe("function");

    const result = await handler!({
      workspaceID: "ws-1",
      sql: "SELECT * FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
    });

    expect(result).toMatchObject({
      structuredContent: {
        status: "ready",
        nodes: [
          expect.objectContaining({
            predecessorNodeIDs: ["source-1"],
            joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
          }),
        ],
      },
    });
    expect((result as any).isError).toBeUndefined();
    expect(client.post).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it("plan-pipeline refreshes the cached summary when repo-backed rankings change at the same path", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "coalesce-plan-summary-"));
    tempDirs.push(tempDir);
    const repoCopyPath = join(tempDir, "repo-copy");
    cpSync(fixtureRepoPath, repoCopyPath, { recursive: true });
    vi.spyOn(process, "cwd").mockReturnValue(tempDir);

    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [
            { nodeType: "package-alpha:::65" },
            { nodeType: "Stage" },
            { nodeType: "Source" },
          ],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(buildSourceNode("source-1", "CUSTOMER", "RAW"));
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    registerPipelineTools(server, client as any);

    const planToolCall = toolSpy.mock.calls.find(
      (call) => call[0] === "plan_pipeline"
    );
    const handler = planToolCall?.[2] as
      | ((params: {
          workspaceID: string;
          goal: string;
          sourceNodeIDs: string[];
          repoPath: string;
        }) => Promise<{
          structuredContent?: Record<string, unknown>;
        }>)
      | undefined;

    expect(typeof handler).toBe("function");

    const firstResult = await handler!({
      workspaceID: "ws-1",
      goal: "Build a customer work node",
      sourceNodeIDs: ["source-1"],
      repoPath: repoCopyPath,
    });
    const firstStructured = (firstResult as any).structuredContent as Record<string, unknown>;
    const firstSummaryUri = firstStructured.planSummaryUri as string;
    const firstSummaryPath = resolveCacheResourceUri(firstSummaryUri, tempDir)?.filePath;

    expect(firstStructured.planCached).toBe(false);
    expect(firstSummaryPath).toBeDefined();
    expect(readFileSync(firstSummaryPath!, "utf8")).toContain("Custom Work");

    const secondResult = await handler!({
      workspaceID: "ws-1",
      goal: "Build a customer work node",
      sourceNodeIDs: ["source-1"],
      repoPath: repoCopyPath,
    });
    const secondStructured = (secondResult as any).structuredContent as Record<string, unknown>;

    expect(secondStructured.planCached).toBe(true);
    expect(secondStructured.planSummaryUri).toBe(firstSummaryUri);

    const definitionPath = join(repoCopyPath, "nodeTypes", "Custom-65", "definition.yml");
    writeFileSync(
      definitionPath,
      readFileSync(definitionPath, "utf8").replace(/Custom Work/g, "Custom Work Reloaded"),
      "utf8"
    );

    const refreshedResult = await handler!({
      workspaceID: "ws-1",
      goal: "Build a customer work node",
      sourceNodeIDs: ["source-1"],
      repoPath: repoCopyPath,
    });
    const refreshedStructured = (refreshedResult as any).structuredContent as Record<string, unknown>;
    const refreshedSummaryPath = resolveCacheResourceUri(
      refreshedStructured.planSummaryUri as string,
      tempDir
    )?.filePath;

    expect(refreshedStructured.planCached).toBe(false);
    expect(refreshedSummaryPath).toBeDefined();
    expect(readFileSync(refreshedSummaryPath!, "utf8")).toContain("Custom Work Reloaded");
  });

  it("create-pipeline-from-sql tool accepts ref-based SQL unchanged", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source", locationName: "RAW" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(buildSourceNode("source-1", "CUSTOMER", "RAW"));
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    registerPipelineTools(server, client as any);

    const createToolCall = toolSpy.mock.calls.find(
      (call) => call[0] === "create_pipeline_from_sql"
    );
    const handler = createToolCall?.[2] as
      | ((params: { workspaceID: string; sql: string }) => Promise<{
          isError?: boolean;
          content: { type: "text"; text: string }[];
        }>)
      | undefined;

    expect(typeof handler).toBe("function");

    const result = await handler!({
      workspaceID: "ws-1",
      sql: "SELECT * FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
    });

    expect(result).toMatchObject({
      structuredContent: {
        created: false,
        confirmationToken: expect.any(String),
        plan: expect.objectContaining({
          status: "ready",
        }),
        STOP_AND_CONFIRM: expect.stringContaining("Ask for explicit approval"),
      },
    });
    expect((result as any).isError).toBeUndefined();
    expect(client.post).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it("planPipeline builds a ready Stage plan from ref-based SQL", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: [
        "SELECT CUSTOMER.CUSTOMER_ID, CUSTOMER.CUSTOMER_NAME AS NAME",
        "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
      ].join("\n"),
      targetName: "STG_CUSTOMER",
      locationName: "STAGING",
      database: "STAGING",
      schema: "ANALYTICS",
    });

    expect(result.status).toBe("ready");
    expect(result.nodes).toHaveLength(1);
    expect(result.nodes[0]?.name).toBe("STG_CUSTOMER");
    expect(result.nodes[0]?.nodeType).toBe("Stage");
    expect(result.nodes[0]?.predecessorNodeIDs).toEqual(["source-1"]);
    expect(result.nodes[0]?.outputColumnNames).toEqual([
      "CUSTOMER_ID",
      "NAME",
    ]);
    expect(result.nodes[0]?.joinCondition).toBe(
      "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER"
    );
    expect(result.nodes[0]?.sourceRefs).toEqual([
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
        alias: "CUSTOMER",
        nodeID: "source-1",
      },
    ]);
  });

  it("planPipeline builds a ready Stage plan from raw SQL and normalizes joinCondition to ref syntax", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source", locationName: "RAW" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: [
        "SELECT RAW.CUSTOMER.CUSTOMER_ID, RAW.CUSTOMER.CUSTOMER_NAME AS NAME",
        "FROM RAW.CUSTOMER",
      ].join("\n"),
      targetName: "STG_CUSTOMER",
      locationName: "STAGING",
      database: "STAGING",
      schema: "ANALYTICS",
    });

    expect(result.status).toBe("ready");
    expect(result.nodes).toHaveLength(1);
    expect(result.nodes[0]?.name).toBe("STG_CUSTOMER");
    expect(result.nodes[0]?.nodeType).toBe("Stage");
    expect(result.nodes[0]?.predecessorNodeIDs).toEqual(["source-1"]);
    expect(result.nodes[0]?.outputColumnNames).toEqual([
      "CUSTOMER_ID",
      "NAME",
    ]);
    expect(result.nodes[0]?.joinCondition).toBe(
      "FROM {{ ref('RAW', 'CUSTOMER') }}"
    );
    expect(result.nodes[0]?.sourceRefs).toEqual([
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
        alias: null,
        nodeID: "source-1",
      },
    ]);
  });

  it("planPipeline ignores block hints before raw-table sources", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source", locationName: "RAW" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: [
        "SELECT CUSTOMER.CUSTOMER_ID",
        "FROM /*+ hint */ RAW.CUSTOMER CUSTOMER",
      ].join("\n"),
      targetName: "STG_CUSTOMER",
    });

    expect(result.status).toBe("ready");
    expect(result.nodes[0]?.sourceRefs).toEqual([
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
        alias: "CUSTOMER",
        nodeID: "source-1",
      },
    ]);
    expect(result.nodes[0]?.joinCondition).toBe(
      "FROM /*+ hint */ {{ ref('RAW', 'CUSTOMER') }} CUSTOMER"
    );
  });

  it("planPipeline ignores comments between raw-table sources and aliases", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source", locationName: "RAW" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: [
        "SELECT CUSTOMER.CUSTOMER_ID",
        "FROM RAW.CUSTOMER /* trailing */ CUSTOMER",
      ].join("\n"),
      targetName: "STG_CUSTOMER",
    });

    expect(result.status).toBe("ready");
    expect(result.nodes[0]?.sourceRefs).toEqual([
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
        alias: "CUSTOMER",
        nodeID: "source-1",
      },
    ]);
    expect(result.nodes[0]?.joinCondition).toBe(
      "FROM {{ ref('RAW', 'CUSTOMER') }} /* trailing */ CUSTOMER"
    );
  });

  it("planPipeline ignores line comments between FROM and raw-table sources", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source", locationName: "RAW" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: [
        "SELECT CUSTOMER.CUSTOMER_ID",
        "FROM -- source hint",
        "RAW.CUSTOMER CUSTOMER",
      ].join("\n"),
      targetName: "STG_CUSTOMER",
    });

    expect(result.status).toBe("ready");
    expect(result.nodes[0]?.sourceRefs).toEqual([
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
        alias: "CUSTOMER",
        nodeID: "source-1",
      },
    ]);
    expect(result.nodes[0]?.joinCondition).toBe(
      "FROM -- source hint\n{{ ref('RAW', 'CUSTOMER') }} CUSTOMER"
    );
  });

  it("planPipeline keeps self-join aliases while deduping predecessor node IDs", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source", locationName: "RAW" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: [
        "SELECT c1.CUSTOMER_ID AS LEFT_CUSTOMER_ID, c2.CUSTOMER_NAME AS RIGHT_CUSTOMER_NAME",
        "FROM RAW.CUSTOMER c1",
        "INNER JOIN RAW.CUSTOMER c2 ON c1.CUSTOMER_ID = c2.CUSTOMER_ID",
      ].join("\n"),
      targetName: "STG_CUSTOMER_SELF_JOIN",
      locationName: "STAGING",
      database: "STAGING",
      schema: "ANALYTICS",
    });

    expect(result.status).toBe("ready");
    expect(result.nodes[0]?.predecessorNodeIDs).toEqual(["source-1"]);
    expect(result.nodes[0]?.sourceRefs).toEqual([
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
        alias: "c1",
        nodeID: "source-1",
      },
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
        alias: "c2",
        nodeID: "source-1",
      },
    ]);
    expect(result.nodes[0]?.joinCondition).toContain("FROM {{ ref('RAW', 'CUSTOMER') }} c1");
    expect(result.nodes[0]?.joinCondition).toContain(
      "INNER JOIN {{ ref('RAW', 'CUSTOMER') }} c2 ON c1.CUSTOMER_ID = c2.CUSTOMER_ID"
    );
  });

  it("planPipeline ignores comments before JOIN keywords in raw-table self-joins", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source", locationName: "RAW" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: [
        "SELECT CUSTOMER.CUSTOMER_ID AS LEFT_CUSTOMER_ID, C2.CUSTOMER_NAME AS RIGHT_CUSTOMER_NAME",
        "FROM RAW.CUSTOMER CUSTOMER /* join note */",
        "INNER JOIN RAW.CUSTOMER C2 ON CUSTOMER.CUSTOMER_ID = C2.CUSTOMER_ID",
      ].join("\n"),
      targetName: "STG_CUSTOMER_SELF_JOIN",
    });

    expect(result.status).toBe("ready");
    expect(result.nodes[0]?.predecessorNodeIDs).toEqual(["source-1"]);
    expect(result.nodes[0]?.sourceRefs).toEqual([
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
        alias: "CUSTOMER",
        nodeID: "source-1",
      },
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
        alias: "C2",
        nodeID: "source-1",
      },
    ]);
    expect(result.nodes[0]?.joinCondition).toContain(
      "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER /* join note */"
    );
    expect(result.nodes[0]?.joinCondition).toContain(
      "INNER JOIN {{ ref('RAW', 'CUSTOMER') }} C2 ON CUSTOMER.CUSTOMER_ID = C2.CUSTOMER_ID"
    );
  });

  it("planPipeline does not mark non-SELECT SQL as ready even when refs resolve", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(buildSourceNode("source-1", "CUSTOMER", "RAW"));
      }
      throw new Error(`Unexpected GET ${path}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: "DELETE FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
    });

    expect(result.status).toBe("needs_clarification");
    expect(result.nodes[0]?.outputColumnNames).toEqual([]);
    expect(result.openQuestions).toContain(
      "Provide a top-level SELECT ... FROM query using direct column projections before creating this pipeline."
    );
    expect(result.warnings).toContain(
      "Could not find a top-level SELECT ... FROM clause in the SQL."
    );
  });

  it("planPipeline returns clarification questions for goal-only requests", async () => {
    const client = createMockClient();

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      goal: "Build a retention reporting pipeline",
    });

    expect(result.status).toBe("needs_clarification");
    expect(result.intent).toBe("goal");
    expect(result.openQuestions).toHaveLength(1);
    expect(result.openQuestions[0]).toContain("upstream Coalesce node IDs");
  });

  it("planPipeline resolves duplicate ref names by requested location", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [
            { id: "source-raw", name: "CUSTOMER", nodeType: "Source" },
            { id: "source-stg", name: "CUSTOMER", nodeType: "Stage" },
          ],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-raw") {
        return Promise.resolve(buildSourceNode("source-raw", "CUSTOMER", "RAW"));
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-stg") {
        return Promise.resolve(buildSourceNode("source-stg", "CUSTOMER", "STAGING"));
      }
      throw new Error(`Unexpected GET ${path}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: "SELECT CUSTOMER_ID FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
    });

    expect(result.status).toBe("ready");
    expect(result.nodes[0]?.predecessorNodeIDs).toEqual(["source-raw"]);
    expect(result.nodes[0]?.sourceRefs[0]?.locationName).toBe("RAW");
  });

  it("planPipeline builds a ready pass-through Stage plan from goal plus sourceNodeIDs", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(buildSourceNode("source-1", "CUSTOMER", "RAW"));
      }
      throw new Error(`Unexpected GET ${path}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      goal: "Build a customer staging layer",
      sourceNodeIDs: ["source-1"],
      targetName: "STG_CUSTOMER",
    });

    expect(result.status).toBe("ready");
    expect(result.intent).toBe("goal");
    expect(result.nodes).toHaveLength(1);
    expect(result.nodes[0]?.name).toBe("STG_CUSTOMER");
    expect(result.nodes[0]?.predecessorNodeIDs).toEqual(["source-1"]);
    expect(result.nodes[0]?.outputColumnNames).toEqual([
      "CUSTOMER_ID",
      "CUSTOMER_NAME",
    ]);
    expect(result.nodes[0]?.joinCondition).toBe(
      `FROM {{ ref('RAW', 'CUSTOMER') }} "CUSTOMER"`
    );
  });

  it("planPipeline ranks repo-backed node types and selects the best work-like type for the goal", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [
            { nodeType: "package-alpha:::65" },
            { nodeType: "Stage" },
            { nodeType: "Source" },
          ],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(buildSourceNode("source-1", "CUSTOMER", "RAW"));
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      goal: "Build a customer work node",
      sourceNodeIDs: ["source-1"],
      repoPath: fixtureRepoPath,
    });

    expect(result.status).toBe("ready");
    expect(result.nodes[0]?.nodeType).toBe("package-alpha:::65");
    expect(result.nodes[0]?.nodeTypeFamily).toBe("work");
    expect((result.nodes[0] as any)?.templateDefaults?.inferredConfig).toMatchObject({
      strategy: "APPEND",
    });
    expect((result as any).nodeTypeSelection.selectedNodeType).toBe(
      "package-alpha:::65"
    );
  });

  it("planPipeline requires source location metadata before marking a goal plan ready", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(buildSourceNode("source-1", "CUSTOMER", null));
      }
      throw new Error(`Unexpected GET ${path}`);
    });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      goal: "Build a customer staging layer",
      sourceNodeIDs: ["source-1"],
      targetName: "STG_CUSTOMER",
    });

    expect(result.status).toBe("needs_clarification");
    expect(result.openQuestions).toContain(
      "Source node CUSTOMER does not expose locationName. Clarify the Coalesce location before generating ref() SQL for this pipeline."
    );
  });

  it("planPipeline surfaces node type validation fetch failures in the plan warnings", async () => {
    // Ensure no repo path leaks from the host environment — a real repo would
    // produce additional selection warnings that inflate the warnings array.
    delete process.env.COALESCE_REPO_PATH;

    const client = createMockClient();
    client.get.mockRejectedValue(new Error("workspace list unavailable"));

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      goal: "Build a retention reporting pipeline",
    });

    expect(result.status).toBe("needs_clarification");
    expect(result.warnings).toHaveLength(1);
    expect(result.warnings[0]).toContain("Observed workspace node types could not be fetched for workspace ws-1");
    expect(result.warnings[0]).toContain("workspace list unavailable");
  });

  it("planPipeline warns but still returns a plan when COALESCE_REPO_PATH is stale", async () => {
    process.env = { ...originalEnv, COALESCE_REPO_PATH: "/nonexistent/path" };

    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ nodeType: "Stage" }] });

    const result = await planPipeline(client as any, {
      workspaceID: "ws-1",
      goal: "Build a customer staging layer",
    });

    expect(result.warnings).toContain(
      "Repo path does not exist. Check the provided path or COALESCE_REPO_PATH environment variable."
    );
    expect(result.status).toBe("needs_clarification");
  });

  it("createPipelineFromPlan rejects workspaceID mismatch between plan and params", async () => {
    const client = createMockClient();
    const plan = {
      version: 1,
      intent: "sql",
      status: "ready",
      workspaceID: "ws-OTHER",
      platform: null,
      goal: null,
      sql: null,
      nodes: [],
      assumptions: [],
      openQuestions: [],
      warnings: [],
      supportedNodeTypes: [],
    };

    await expect(
      createPipelineFromPlan(client as any, { workspaceID: "ws-1", plan })
    ).rejects.toThrow("does not match requested workspaceID");
  });

  it("createPipelineFromPlan returns warning for needs_clarification plans", async () => {
    const client = createMockClient();
    const plan = {
      version: 1,
      intent: "sql",
      status: "needs_clarification",
      workspaceID: "ws-1",
      platform: null,
      goal: null,
      sql: null,
      nodes: [],
      assumptions: [],
      openQuestions: ["Which source table?"],
      warnings: [],
      supportedNodeTypes: [],
    };

    const result = (await createPipelineFromPlan(client as any, {
      workspaceID: "ws-1",
      plan,
    })) as any;

    expect(result.created).toBe(false);
    expect(result.warning).toContain("needs clarification");
    // Should NOT have attempted any API calls for node creation
    expect(client.post).not.toHaveBeenCalled();
  });

  it("createPipelineFromPlan throws when a node has zero resolved predecessors", async () => {
    const client = createMockClient();
    const plan = {
      version: 1,
      intent: "sql",
      status: "ready",
      workspaceID: "ws-1",
      platform: null,
      goal: null,
      sql: null,
      nodes: [
        {
          planNodeID: "node-1",
          name: "ORPHAN_NODE",
          nodeType: "Stage",
          predecessorNodeIDs: [],
          predecessorPlanNodeIDs: [],
          predecessorNodeNames: [],
          description: null,
          sql: null,
          selectItems: [],
          outputColumnNames: [],
          configOverrides: {},
          sourceRefs: [],
          joinCondition: null,
          location: {},
          requiresFullSetNode: false,
        },
      ],
      assumptions: [],
      openQuestions: [],
      warnings: [],
      supportedNodeTypes: ["Stage"],
    };

    await expect(
      createPipelineFromPlan(client as any, { workspaceID: "ws-1", plan })
    ).rejects.toThrow("has no resolved predecessor node IDs");
  });

  it("createPipelineFromPlan re-throws original error after successful rollback", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");
    const createdNode = { ...buildCreatedStageNode("source-1"), id: "created-1" };
    let postCallCount = 0;
    let savedBody: Record<string, unknown> | null = null;

    client.post.mockImplementation(() => {
      postCallCount++;
      if (postCallCount === 1) return Promise.resolve({ id: "created-1" });
      return Promise.reject(new CoalesceApiError("Server error", 500));
    });
    client.put.mockImplementation((_path: string, body: Record<string, unknown>) => {
      savedBody = body;
      return Promise.resolve(body);
    });
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({ data: [{ nodeType: "Stage" }] });
      }
      if (path.includes("/nodes/source-1")) return Promise.resolve(sourceNode);
      if (path.includes("/nodes/created-1")) return Promise.resolve(savedBody ?? createdNode);
      return Promise.resolve({ data: [] });
    });
    // Rollback delete succeeds
    client.delete.mockResolvedValue({});

    const nodeTemplate = {
      planNodeID: "node-1",
      name: "STG_CUSTOMER",
      nodeType: "Stage",
      predecessorNodeIDs: ["source-1"],
      predecessorPlanNodeIDs: [],
      predecessorNodeNames: ["CUSTOMER"],
      description: "Test",
      sql: "SELECT CUSTOMER.CUSTOMER_ID FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
      selectItems: [
        {
          expression: "CUSTOMER.CUSTOMER_ID",
          outputName: "CUSTOMER_ID",
          sourceNodeAlias: "CUSTOMER",
          sourceNodeName: "CUSTOMER",
          sourceNodeID: "source-1",
          sourceColumnName: "CUSTOMER_ID",
          kind: "column" as const,
          supported: true,
        },
      ],
      outputColumnNames: ["CUSTOMER_ID"],
      configOverrides: {},
      sourceRefs: [{ locationName: "RAW", nodeName: "CUSTOMER", alias: "CUSTOMER", nodeID: "source-1" }],
      joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
      location: { locationName: "STAGING", database: "STAGING", schema: "ANALYTICS" },
      requiresFullSetNode: true,
    };
    const plan = {
      version: 1,
      intent: "sql",
      status: "ready",
      workspaceID: "ws-1",
      platform: null,
      goal: null,
      sql: null,
      nodes: [
        nodeTemplate,
        { ...nodeTemplate, planNodeID: "node-2", name: "STG_B" },
      ],
      assumptions: [],
      openQuestions: [],
      warnings: [],
      supportedNodeTypes: ["Stage"],
    };

    // Should re-throw the original 500 error after successful rollback
    await expect(
      createPipelineFromPlan(client as any, { workspaceID: "ws-1", plan })
    ).rejects.toThrow("Server error");
    // Rollback should have deleted the first node
    expect(client.delete).toHaveBeenCalledTimes(1);
  });

  it("createPipelineFromPlan returns error result when rollback fails", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");
    const createdNode = { ...buildCreatedStageNode("source-1"), id: "created-1" };
    let postCallCount = 0;
    let savedBody: Record<string, unknown> | null = null;

    client.post.mockImplementation(() => {
      postCallCount++;
      if (postCallCount === 1) return Promise.resolve({ id: "created-1" });
      return Promise.reject(new CoalesceApiError("Server error", 500));
    });
    client.put.mockImplementation((_path: string, body: Record<string, unknown>) => {
      savedBody = body;
      return Promise.resolve(body);
    });
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({ data: [{ nodeType: "Stage" }] });
      }
      if (path.includes("/nodes/source-1")) return Promise.resolve(sourceNode);
      if (path.includes("/nodes/created-1")) return Promise.resolve(savedBody ?? createdNode);
      return Promise.resolve({ data: [] });
    });
    // Rollback also fails
    client.delete.mockRejectedValue(new CoalesceApiError("Delete failed", 500));

    const nodeTemplate = {
      planNodeID: "node-1",
      name: "STG_CUSTOMER",
      nodeType: "Stage",
      predecessorNodeIDs: ["source-1"],
      predecessorPlanNodeIDs: [],
      predecessorNodeNames: ["CUSTOMER"],
      description: "Test",
      sql: "SELECT CUSTOMER.CUSTOMER_ID FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
      selectItems: [
        {
          expression: "CUSTOMER.CUSTOMER_ID",
          outputName: "CUSTOMER_ID",
          sourceNodeAlias: "CUSTOMER",
          sourceNodeName: "CUSTOMER",
          sourceNodeID: "source-1",
          sourceColumnName: "CUSTOMER_ID",
          kind: "column" as const,
          supported: true,
        },
      ],
      outputColumnNames: ["CUSTOMER_ID"],
      configOverrides: {},
      sourceRefs: [{ locationName: "RAW", nodeName: "CUSTOMER", alias: "CUSTOMER", nodeID: "source-1" }],
      joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
      location: { locationName: "STAGING", database: "STAGING", schema: "ANALYTICS" },
      requiresFullSetNode: true,
    };
    const plan = {
      version: 1,
      intent: "sql",
      status: "ready",
      workspaceID: "ws-1",
      platform: null,
      goal: null,
      sql: null,
      nodes: [
        nodeTemplate,
        { ...nodeTemplate, planNodeID: "node-2", name: "STG_B" },
      ],
      assumptions: [],
      openQuestions: [],
      warnings: [],
      supportedNodeTypes: ["Stage"],
    };

    // Should return structured error (not throw) when rollback fails
    const result = (await createPipelineFromPlan(client as any, {
      workspaceID: "ws-1",
      plan,
    })) as any;

    expect(result.created).toBe(false);
    expect(result.isError).toBe(true);
    expect(result.incomplete).toBe(true);
    expect(result.cleanupFailedNodeIDs).toContain("created-1");
    expect(result.error).toBeDefined();
    expect(result.warning).toContain("cleanup did not fully succeed");
  });

  it("createPipelineFromPlan creates a Stage node and persists the SQL-derived source mapping", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");
    const createdNode = buildCreatedStageNode("source-1");
    let savedBody: Record<string, unknown> | null = null;

    client.post.mockResolvedValue({ id: "new-node" });
    client.put.mockImplementation((_path: string, body: Record<string, unknown>) => {
      savedBody = body;
      return Promise.resolve(body);
    });
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node") {
        return Promise.resolve(savedBody ?? createdNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const plan = {
      version: 1,
      intent: "sql",
      status: "ready",
      workspaceID: "ws-1",
      platform: null,
      goal: null,
      sql: [
        "SELECT CUSTOMER.CUSTOMER_ID, CUSTOMER.CUSTOMER_NAME AS NAME",
        "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
      ].join("\n"),
      nodes: [
        {
          planNodeID: "node-1",
          name: "STG_CUSTOMER",
          nodeType: "Stage",
          predecessorNodeIDs: ["source-1"],
          predecessorPlanNodeIDs: [],
          predecessorNodeNames: ["CUSTOMER"],
          description: "Customer staging node",
          sql: [
            "SELECT CUSTOMER.CUSTOMER_ID, CUSTOMER.CUSTOMER_NAME AS NAME",
            "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
          ].join("\n"),
          selectItems: [
            {
              expression: "CUSTOMER.CUSTOMER_ID",
              outputName: "CUSTOMER_ID",
              sourceNodeAlias: "CUSTOMER",
              sourceNodeName: "CUSTOMER",
              sourceNodeID: "source-1",
              sourceColumnName: "CUSTOMER_ID",
              kind: "column",
              supported: true,
            },
            {
              expression: "CUSTOMER.CUSTOMER_NAME",
              outputName: "NAME",
              sourceNodeAlias: "CUSTOMER",
              sourceNodeName: "CUSTOMER",
              sourceNodeID: "source-1",
              sourceColumnName: "CUSTOMER_NAME",
              kind: "column",
              supported: true,
            },
          ],
          outputColumnNames: ["CUSTOMER_ID", "NAME"],
          configOverrides: {
            testsEnabled: false,
          },
          sourceRefs: [
            {
              locationName: "RAW",
              nodeName: "CUSTOMER",
              alias: "CUSTOMER",
              nodeID: "source-1",
            },
          ],
          joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
          location: {
            locationName: "STAGING",
            database: "STAGING",
            schema: "ANALYTICS",
          },
          requiresFullSetNode: true,
        },
      ],
      assumptions: [],
      openQuestions: [],
      warnings: [],
      supportedNodeTypes: ["Stage"],
    };

    const result = await createPipelineFromPlan(client as any, {
      workspaceID: "ws-1",
      plan,
    });

    expect(result).toMatchObject({
      created: true,
      workspaceID: "ws-1",
      nodeCount: 1,
    });
    expect(client.put).toHaveBeenCalledTimes(1);
    expect(savedBody).not.toBeNull();
    expect((savedBody as any).name).toBe("STG_CUSTOMER");
    expect((savedBody as any).description).toBe("Customer staging node");
    expect((savedBody as any).config.testsEnabled).toBe(false);
    expect((savedBody as any).metadata.columns.map((column: any) => column.name)).toEqual([
      "CUSTOMER_ID",
      "NAME",
    ]);
    expect((savedBody as any).metadata.sourceMapping[0].dependencies).toEqual([
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
      },
    ]);
    expect((savedBody as any).metadata.sourceMapping[0].join.joinCondition).toBe(
      "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER"
    );
  });

  it("createPipelineFromPlan applies template defaults for non-Stage projection node types", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");
    const createdNode = buildCreatedProjectionNode("source-1");
    let savedBody: Record<string, unknown> | null = null;

    client.post.mockResolvedValue({ id: "new-node" });
    client.put.mockImplementation((_path: string, body: Record<string, unknown>) => {
      savedBody = body;
      return Promise.resolve(body);
    });
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "package-alpha:::65" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node") {
        return Promise.resolve(savedBody ?? createdNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await createPipelineFromPlan(client as any, {
      workspaceID: "ws-1",
      plan: {
        version: 1,
        intent: "goal",
        status: "ready",
        workspaceID: "ws-1",
        platform: null,
        goal: "Build a customer work node",
        sql: null,
        nodes: [
          {
            planNodeID: "node-1",
            name: "CWRK_CUSTOMER",
            nodeType: "package-alpha:::65",
            nodeTypeFamily: "work",
            predecessorNodeIDs: ["source-1"],
            predecessorPlanNodeIDs: [],
            predecessorNodeNames: ["CUSTOMER"],
            description: "Custom work node",
            sql: null,
            selectItems: [
              {
                expression: "CUSTOMER.CUSTOMER_ID",
                outputName: "CUSTOMER_ID",
                sourceNodeAlias: "CUSTOMER",
                sourceNodeName: "CUSTOMER",
                sourceNodeID: "source-1",
                sourceColumnName: "CUSTOMER_ID",
                kind: "column",
                supported: true,
              },
              {
                expression: "CUSTOMER.CUSTOMER_NAME",
                outputName: "CUSTOMER_NAME",
                sourceNodeAlias: "CUSTOMER",
                sourceNodeName: "CUSTOMER",
                sourceNodeID: "source-1",
                sourceColumnName: "CUSTOMER_NAME",
                kind: "column",
                supported: true,
              },
            ],
            outputColumnNames: ["CUSTOMER_ID", "CUSTOMER_NAME"],
            configOverrides: {},
            sourceRefs: [
              {
                locationName: "RAW",
                nodeName: "CUSTOMER",
                alias: "CUSTOMER",
                nodeID: "source-1",
              },
            ],
            joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
            location: {},
            requiresFullSetNode: true,
            templateDefaults: {
              inferredTopLevelFields: {},
              inferredConfig: {
                strategy: "APPEND",
              },
            },
          },
        ],
        assumptions: [],
        openQuestions: [],
        warnings: [],
        supportedNodeTypes: ["package-alpha:::65", "Stage"],
      },
    });

    expect(result).toMatchObject({
      created: true,
      workspaceID: "ws-1",
      nodeCount: 1,
    });
    expect((savedBody as any).config.strategy).toBe("APPEND");
    expect((savedBody as any).metadata.columns.map((column: any) => column.name)).toEqual([
      "CUSTOMER_ID",
      "CUSTOMER_NAME",
    ]);
  });

  it("createPipelineFromPlan supports self-joins when the saved node dedupes dependency entries", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");
    const createdNode = buildCreatedStageNode("source-1");
    let savedBody: Record<string, unknown> | null = null;

    client.post.mockResolvedValue({ id: "new-node" });
    client.put.mockImplementation((_path: string, body: Record<string, unknown>) => {
      savedBody = body;
      return Promise.resolve(body);
    });
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node") {
        if (!savedBody) {
          return Promise.resolve(createdNode);
        }
        const sourceMapping = ((savedBody as any).metadata.sourceMapping ?? []) as any[];
        return Promise.resolve({
          ...savedBody,
          metadata: {
            ...(savedBody as any).metadata,
            sourceMapping: sourceMapping.map((entry, index) =>
              index === 0
                ? {
                    ...entry,
                    dependencies: [
                      {
                        locationName: "RAW",
                        nodeName: "CUSTOMER",
                      },
                    ],
                  }
                : entry
            ),
          },
        });
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await createPipelineFromPlan(client as any, {
      workspaceID: "ws-1",
      plan: {
        version: 1,
        intent: "sql",
        status: "ready",
        workspaceID: "ws-1",
        platform: null,
        goal: null,
        sql: [
          "SELECT c1.CUSTOMER_ID AS LEFT_CUSTOMER_ID, c2.CUSTOMER_NAME AS RIGHT_CUSTOMER_NAME",
          "FROM {{ ref('RAW', 'CUSTOMER') }} c1",
          "INNER JOIN {{ ref('RAW', 'CUSTOMER') }} c2 ON c1.CUSTOMER_ID = c2.CUSTOMER_ID",
        ].join("\n"),
        nodes: [
          {
            planNodeID: "node-1",
            name: "STG_CUSTOMER_SELF_JOIN",
            nodeType: "Stage",
            predecessorNodeIDs: ["source-1", "source-1"],
            predecessorPlanNodeIDs: [],
            predecessorNodeNames: ["CUSTOMER", "CUSTOMER"],
            description: null,
            sql: [
              "SELECT c1.CUSTOMER_ID AS LEFT_CUSTOMER_ID, c2.CUSTOMER_NAME AS RIGHT_CUSTOMER_NAME",
              "FROM {{ ref('RAW', 'CUSTOMER') }} c1",
              "INNER JOIN {{ ref('RAW', 'CUSTOMER') }} c2 ON c1.CUSTOMER_ID = c2.CUSTOMER_ID",
            ].join("\n"),
            selectItems: [
              {
                expression: "c1.CUSTOMER_ID",
                outputName: "LEFT_CUSTOMER_ID",
                sourceNodeAlias: "c1",
                sourceNodeName: "CUSTOMER",
                sourceNodeID: "source-1",
                sourceColumnName: "CUSTOMER_ID",
                kind: "column",
                supported: true,
              },
              {
                expression: "c2.CUSTOMER_NAME",
                outputName: "RIGHT_CUSTOMER_NAME",
                sourceNodeAlias: "c2",
                sourceNodeName: "CUSTOMER",
                sourceNodeID: "source-1",
                sourceColumnName: "CUSTOMER_NAME",
                kind: "column",
                supported: true,
              },
            ],
            outputColumnNames: ["LEFT_CUSTOMER_ID", "RIGHT_CUSTOMER_NAME"],
            configOverrides: {
              testsEnabled: false,
            },
            sourceRefs: [
              {
                locationName: "RAW",
                nodeName: "CUSTOMER",
                alias: "c1",
                nodeID: "source-1",
              },
              {
                locationName: "RAW",
                nodeName: "CUSTOMER",
                alias: "c2",
                nodeID: "source-1",
              },
            ],
            joinCondition: [
              "FROM {{ ref('RAW', 'CUSTOMER') }} c1",
              "INNER JOIN {{ ref('RAW', 'CUSTOMER') }} c2 ON c1.CUSTOMER_ID = c2.CUSTOMER_ID",
            ].join("\n"),
            location: {
              locationName: "STAGING",
              database: "STAGING",
              schema: "ANALYTICS",
            },
            requiresFullSetNode: true,
          },
        ],
        assumptions: [],
        openQuestions: [],
        warnings: [],
        supportedNodeTypes: ["Stage"],
      },
    });

    expect(result).toMatchObject({
      created: true,
      workspaceID: "ws-1",
      nodeCount: 1,
    });
    expect(client.post).toHaveBeenCalledWith("/api/v1/workspaces/ws-1/nodes", {
      nodeType: "Stage",
      predecessorNodeIDs: ["source-1"],
    });
    expect(savedBody).not.toBeNull();
    expect((savedBody as any).metadata.sourceMapping[0].dependencies).toEqual([
      {
        locationName: "RAW",
        nodeName: "CUSTOMER",
      },
    ]);
    expect((savedBody as any).metadata.sourceMapping[0].aliases).toEqual({
      c1: "source-1",
      c2: "source-1",
    });
    expect((savedBody as any).metadata.sourceMapping[0].join.joinCondition).toContain(
      "INNER JOIN {{ ref('RAW', 'CUSTOMER') }} c2 ON c1.CUSTOMER_ID = c2.CUSTOMER_ID"
    );
  });

  it("createPipelineFromSql returns a dry-run plan without creating nodes", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await createPipelineFromSql(client as any, {
      workspaceID: "ws-1",
      sql: "SELECT * FROM RAW.CUSTOMER",
      dryRun: true,
    });

    expect(result).toMatchObject({
      created: false,
      dryRun: true,
    });
    expect((result as any).plan.status).toBe("ready");
    expect(client.post).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it("createPipelineFromSql returns STOP_AND_CONFIRM for ready plans until confirmed", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await createPipelineFromSql(client as any, {
      workspaceID: "ws-1",
      sql: "SELECT * FROM RAW.CUSTOMER",
    });

    expect(result).toMatchObject({
      created: false,
      confirmationToken: expect.any(String),
      STOP_AND_CONFIRM: expect.stringContaining("Present the pipeline summary"),
    });
    expect((result as any).plan.status).toBe("ready");
    expect(client.post).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it("createPipelineFromSql rejects confirmed=true without a matching confirmation token", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const result = await createPipelineFromSql(client as any, {
      workspaceID: "ws-1",
      sql: "SELECT * FROM RAW.CUSTOMER",
      confirmed: true,
    });

    expect(result).toMatchObject({
      created: false,
      confirmationToken: expect.any(String),
      STOP_AND_CONFIRM: expect.stringContaining("confirmationToken is missing or does not match"),
    });
    expect((result as any).plan.status).toBe("ready");
    expect(client.post).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it("createPipelineFromSql executes when confirmed=true with a matching confirmation token", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");
    const createdNode = buildCreatedStageNode("source-1");
    let savedBody: Record<string, unknown> | null = null;

    client.post.mockResolvedValue({ id: "new-node" });
    client.put.mockImplementation((_path: string, body: Record<string, unknown>) => {
      savedBody = body;
      return Promise.resolve(body);
    });
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({
          data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source", locationName: "RAW" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node") {
        return Promise.resolve(savedBody ?? createdNode);
      }
      throw new Error(`Unexpected GET ${path} ${JSON.stringify(params)}`);
    });

    const readyPlan = await planPipeline(client as any, {
      workspaceID: "ws-1",
      sql: "SELECT CUSTOMER.CUSTOMER_ID FROM RAW.CUSTOMER CUSTOMER",
    });

    expect(readyPlan.status).toBe("ready");

    const result = await createPipelineFromSql(client as any, {
      workspaceID: "ws-1",
      sql: "SELECT CUSTOMER.CUSTOMER_ID FROM RAW.CUSTOMER CUSTOMER",
      confirmed: true,
      confirmationToken: buildPlanConfirmationToken(readyPlan),
    });

    expect(result).toMatchObject({
      created: true,
      workspaceID: "ws-1",
      nodeCount: 1,
      plan: expect.objectContaining({
        status: "ready",
      }),
    });
    expect(client.post).toHaveBeenCalledTimes(1);
    expect(client.put).toHaveBeenCalledTimes(1);
  });

  it("create-pipeline-from-sql tool executes when confirmed=true without elicitation", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerPipelineTools(server, client as any);

    const planningModule = await import("../../src/services/pipelines/planning.js");
    const executionModule = await import("../../src/services/pipelines/execution.js");
    const readyPlan = {
      version: 1,
      intent: "sql",
      status: "ready",
      workspaceID: "ws-1",
      platform: null,
      goal: null,
      sql: "select 1 as customer_id",
      nodes: [{ name: "STG_CUSTOMER", nodeType: "Stage" }],
      assumptions: [],
      openQuestions: [],
      warnings: [],
      supportedNodeTypes: ["Stage"],
    };
    const planSpy = vi
      .spyOn(planningModule, "planPipeline")
      .mockResolvedValue(readyPlan as any);
    const executeSpy = vi
      .spyOn(executionModule, "createPipelineFromPlan")
      .mockResolvedValue({
        created: true,
        workspaceID: "ws-1",
        nodeCount: 1,
      });

    const elicitSpy = vi.spyOn(server.server, "elicitInput");
    const createToolCall = toolSpy.mock.calls.find(
      (call) => call[0] === "create_pipeline_from_sql"
    );
    const handler = createToolCall?.[2] as
      | ((params: { workspaceID: string; sql: string; confirmed?: boolean; confirmationToken?: string }) => Promise<{
          structuredContent?: Record<string, unknown>;
        }>)
      | undefined;

    expect(typeof handler).toBe("function");

    const result = await handler!({
      workspaceID: "ws-1",
      sql: "select 1 as customer_id",
      confirmed: true,
      confirmationToken: buildPlanConfirmationToken(readyPlan),
    });

    expect(result).toMatchObject({
      structuredContent: {
        created: true,
        workspaceID: "ws-1",
        nodeCount: 1,
        plan: expect.objectContaining({
          status: "ready",
        }),
      },
    });
    expect(elicitSpy).not.toHaveBeenCalled();
    expect(planSpy).toHaveBeenCalledTimes(1);
    expect(executeSpy).toHaveBeenCalledWith(client, {
      workspaceID: "ws-1",
      plan: readyPlan,
    });
  });

  it("createPipelineFromPlan regenerates unique output column identities for repeated source columns", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");
    const createdNode = buildCreatedStageNode("source-1");
    let savedBody: Record<string, unknown> | null = null;

    client.post.mockResolvedValue({ id: "new-node" });
    client.put.mockImplementation((_path: string, body: Record<string, unknown>) => {
      savedBody = body;
      return Promise.resolve(body);
    });
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node") {
        return Promise.resolve(savedBody ?? createdNode);
      }
      throw new Error(`Unexpected GET ${path}`);
    });

    await createPipelineFromPlan(client as any, {
      workspaceID: "ws-1",
      plan: {
        version: 1,
        intent: "sql",
        status: "ready",
        workspaceID: "ws-1",
        platform: null,
        goal: null,
        sql: "SELECT CUSTOMER.CUSTOMER_ID, CUSTOMER.CUSTOMER_ID AS CUSTOMER_ID_COPY FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
        nodes: [
          {
            planNodeID: "node-1",
            name: "STG_CUSTOMER_DUP",
            nodeType: "Stage",
            predecessorNodeIDs: ["source-1"],
            predecessorPlanNodeIDs: [],
            predecessorNodeNames: ["CUSTOMER"],
            description: null,
            sql: null,
            selectItems: [
              {
                expression: "CUSTOMER.CUSTOMER_ID",
                outputName: "CUSTOMER_ID",
                sourceNodeAlias: "CUSTOMER",
                sourceNodeName: "CUSTOMER",
                sourceNodeID: "source-1",
                sourceColumnName: "CUSTOMER_ID",
                kind: "column",
                supported: true,
              },
              {
                expression: "CUSTOMER.CUSTOMER_ID",
                outputName: "CUSTOMER_ID_COPY",
                sourceNodeAlias: "CUSTOMER",
                sourceNodeName: "CUSTOMER",
                sourceNodeID: "source-1",
                sourceColumnName: "CUSTOMER_ID",
                kind: "column",
                supported: true,
              },
            ],
            outputColumnNames: ["CUSTOMER_ID", "CUSTOMER_ID_COPY"],
            configOverrides: {},
            sourceRefs: [
              {
                locationName: "RAW",
                nodeName: "CUSTOMER",
                alias: "CUSTOMER",
                nodeID: "source-1",
              },
            ],
            joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
            location: {},
            requiresFullSetNode: true,
          },
        ],
        assumptions: [],
        openQuestions: [],
        warnings: [],
        supportedNodeTypes: ["Stage"],
      },
    });

    expect(savedBody).not.toBeNull();
    const columns = (savedBody as any).metadata.columns;
    expect(columns.map((column: any) => column.name)).toEqual([
      "CUSTOMER_ID",
      "CUSTOMER_ID_COPY",
    ]);
    expect(columns[0].columnReference.columnCounter).not.toBe(
      columns[1].columnReference.columnCounter
    );
  });

  it("createPipelineFromPlan rolls back earlier created nodes when a later node fails", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");
    const savedBodies = new Map<string, Record<string, unknown>>();

    client.post
      .mockResolvedValueOnce({ id: "new-node-1" })
      .mockResolvedValueOnce({ id: "new-node-2" });

    client.put.mockImplementation((path: string, body: Record<string, unknown>) => {
      if (path.endsWith("/new-node-1")) {
        savedBodies.set("new-node-1", body);
        return Promise.resolve(body);
      }
      if (path.endsWith("/new-node-2")) {
        return Promise.reject(new Error("set failed"));
      }
      throw new Error(`Unexpected PUT ${path}`);
    });

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node-1") {
        return Promise.resolve(
          savedBodies.get("new-node-1") ?? {
            ...buildCreatedStageNode("source-1"),
            id: "new-node-1",
          }
        );
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node-2") {
        const predecessorBody = savedBodies.get("new-node-1");
        const predecessorColumn = (predecessorBody as any)?.metadata?.columns?.[0];
        return Promise.resolve({
          id: "new-node-2",
          name: "STG_CUSTOMER_SECOND",
          metadata: {
            columns: predecessorColumn
              ? [
                  {
                    name: predecessorColumn.name,
                    columnID: "new-node-2-customer-id",
                    sources: [
                      {
                        columnReferences: [
                          {
                            nodeID: "new-node-1",
                            columnID: predecessorColumn.columnID,
                          },
                        ],
                      },
                    ],
                  },
                ]
              : [],
            sourceMapping: [
              {
                dependencies: [{ nodeName: "STG_CUSTOMER" }],
              },
            ],
          },
        });
      }
      throw new Error(`Unexpected GET ${path}`);
    });
    client.delete.mockResolvedValue(undefined);

    await expect(
      createPipelineFromPlan(client as any, {
        workspaceID: "ws-1",
        plan: {
          version: 1,
          intent: "sql",
          status: "ready",
          workspaceID: "ws-1",
          platform: null,
          goal: null,
          sql: "SELECT CUSTOMER.CUSTOMER_ID FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
          nodes: [
            {
              planNodeID: "node-1",
              name: "STG_CUSTOMER",
              nodeType: "Stage",
              predecessorNodeIDs: ["source-1"],
              predecessorPlanNodeIDs: [],
              predecessorNodeNames: ["CUSTOMER"],
              description: null,
              sql: null,
              selectItems: [
                {
                  expression: "CUSTOMER.CUSTOMER_ID",
                  outputName: "CUSTOMER_ID",
                  sourceNodeAlias: "CUSTOMER",
                  sourceNodeName: "CUSTOMER",
                  sourceNodeID: "source-1",
                  sourceColumnName: "CUSTOMER_ID",
                  kind: "column",
                  supported: true,
                },
              ],
              outputColumnNames: ["CUSTOMER_ID"],
              configOverrides: {},
              sourceRefs: [
                {
                  locationName: "RAW",
                  nodeName: "CUSTOMER",
                  alias: "CUSTOMER",
                  nodeID: "source-1",
                },
              ],
              joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
              location: {},
              requiresFullSetNode: true,
            },
            {
              planNodeID: "node-2",
              name: "STG_CUSTOMER_SECOND",
              nodeType: "Stage",
              predecessorNodeIDs: [],
              predecessorPlanNodeIDs: ["node-1"],
              predecessorNodeNames: ["STG_CUSTOMER"],
              description: null,
              sql: null,
              selectItems: [
                {
                  expression: "STG_CUSTOMER.CUSTOMER_ID",
                  outputName: "CUSTOMER_ID",
                  sourceNodeAlias: "STG_CUSTOMER",
                  sourceNodeName: "STG_CUSTOMER",
                  sourceNodeID: "new-node-1",
                  sourceColumnName: "CUSTOMER_ID",
                  kind: "column",
                  supported: true,
                },
              ],
              outputColumnNames: ["CUSTOMER_ID"],
              configOverrides: {},
              sourceRefs: [
                {
                  locationName: "TRANSFORM",
                  nodeName: "STG_CUSTOMER",
                  alias: "STG_CUSTOMER",
                  nodeID: "new-node-1",
                },
              ],
              joinCondition: `FROM {{ ref('TRANSFORM', 'STG_CUSTOMER') }} "STG_CUSTOMER"`,
              location: {},
              requiresFullSetNode: true,
            },
          ],
          assumptions: [],
          openQuestions: [],
          warnings: [],
          supportedNodeTypes: ["Stage"],
        },
      })
    ).rejects.toThrow("set failed");

    expect(client.delete).toHaveBeenNthCalledWith(
      1,
      "/api/v1/workspaces/ws-1/nodes/new-node-2"
    );
    expect(client.delete).toHaveBeenNthCalledWith(
      2,
      "/api/v1/workspaces/ws-1/nodes/new-node-1"
    );
  });

  it("createPipelineFromPlan returns incomplete state when predecessor-create warning cleanup fails", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.post.mockResolvedValue({ id: "new-node" });
    client.delete.mockRejectedValue(new Error("delete failed"));
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node") {
        return Promise.resolve({
          id: "new-node",
          name: "STG_CUSTOMER",
          metadata: {
            columns: [],
            sourceMapping: [],
          },
        });
      }
      throw new Error(`Unexpected GET ${path}`);
    });

    const result = await createPipelineFromPlan(client as any, {
      workspaceID: "ws-1",
      plan: {
        version: 1,
        intent: "sql",
        status: "ready",
        workspaceID: "ws-1",
        platform: null,
        goal: null,
        sql: "SELECT CUSTOMER_ID FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
        nodes: [
          {
            planNodeID: "node-1",
            name: "STG_CUSTOMER",
            nodeType: "Stage",
            predecessorNodeIDs: ["source-1"],
            predecessorPlanNodeIDs: [],
            predecessorNodeNames: ["CUSTOMER"],
            description: null,
            sql: null,
            selectItems: [
              {
                expression: "CUSTOMER_ID",
                outputName: "CUSTOMER_ID",
                sourceNodeAlias: "CUSTOMER",
                sourceNodeName: "CUSTOMER",
                sourceNodeID: "source-1",
                sourceColumnName: "CUSTOMER_ID",
                kind: "column",
                supported: true,
              },
            ],
            outputColumnNames: ["CUSTOMER_ID"],
            configOverrides: {},
            sourceRefs: [
              {
                locationName: "RAW",
                nodeName: "CUSTOMER",
                alias: "CUSTOMER",
                nodeID: "source-1",
              },
            ],
            joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
            location: {},
            requiresFullSetNode: true,
          },
        ],
        assumptions: [],
        openQuestions: [],
        warnings: [],
        supportedNodeTypes: ["Stage"],
      },
    });

    expect(result).toMatchObject({
      created: false,
      incomplete: true,
      failedPlanNodeID: "node-1",
      cleanupFailedNodeIDs: ["new-node"],
      cleanupFailures: [
        {
          nodeID: "new-node",
          message: "delete failed",
        },
      ],
      error: {
        message: expect.stringContaining(
          "Predecessor-based creation for STG_CUSTOMER did not confirm full auto-population"
        ),
      },
    });
    expect((result as any).warning).toContain("automatic cleanup did not fully succeed");
  });

  it("createPipelineFromPlan includes rollback delete status and detail when cleanup fails", async () => {
    const client = createMockClient();
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    client.post.mockResolvedValue({ id: "new-node" });
    client.delete.mockRejectedValue(
      new CoalesceApiError("Insufficient permissions for this operation", 403, {
        endpoint: "/api/v1/workspaces/ws-1/nodes/new-node",
      })
    );
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
        return Promise.resolve({
          data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
        return Promise.resolve(sourceNode);
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node") {
        return Promise.resolve({
          id: "new-node",
          name: "STG_CUSTOMER",
          metadata: {
            columns: [],
            sourceMapping: [],
          },
        });
      }
      throw new Error(`Unexpected GET ${path}`);
    });

    const result = await createPipelineFromPlan(client as any, {
      workspaceID: "ws-1",
      plan: {
        version: 1,
        intent: "sql",
        status: "ready",
        workspaceID: "ws-1",
        platform: null,
        goal: null,
        sql: "SELECT CUSTOMER_ID FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
        nodes: [
          {
            planNodeID: "node-1",
            name: "STG_CUSTOMER",
            nodeType: "Stage",
            predecessorNodeIDs: ["source-1"],
            predecessorPlanNodeIDs: [],
            predecessorNodeNames: ["CUSTOMER"],
            description: null,
            sql: null,
            selectItems: [
              {
                expression: "CUSTOMER_ID",
                outputName: "CUSTOMER_ID",
                sourceNodeAlias: "CUSTOMER",
                sourceNodeName: "CUSTOMER",
                sourceNodeID: "source-1",
                sourceColumnName: "CUSTOMER_ID",
                kind: "column",
                supported: true,
              },
            ],
            outputColumnNames: ["CUSTOMER_ID"],
            configOverrides: {},
            sourceRefs: [
              {
                locationName: "RAW",
                nodeName: "CUSTOMER",
                alias: "CUSTOMER",
                nodeID: "source-1",
              },
            ],
            joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
            location: {},
            requiresFullSetNode: true,
          },
        ],
        assumptions: [],
        openQuestions: [],
        warnings: [],
        supportedNodeTypes: ["Stage"],
      },
    });

    expect(result).toMatchObject({
      created: false,
      incomplete: true,
      failedPlanNodeID: "node-1",
      cleanupFailedNodeIDs: ["new-node"],
      cleanupFailures: [
        {
          nodeID: "new-node",
          message: "Insufficient permissions for this operation",
          status: 403,
          detail: {
            endpoint: "/api/v1/workspaces/ws-1/nodes/new-node",
          },
        },
      ],
    });
  });

  describe("plan-pipeline node type validation", () => {
    it("should warn when recommended node type does not exist in workspace", async () => {
      const client = createMockClient();
      const sourceNode = buildSourceNode("source-1", "CUSTOMER", "RAW");

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        // list-workspace-node-types call (needs detail: false)
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({
            data: [
              { nodeType: "Stage" },
              { nodeType: "Source" }
            ]
          });
        }
        // list-workspace-nodes call for planning
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({ data: [] });
        }
        // get-workspace-node call for source
        if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
          return Promise.resolve(sourceNode);
        }
        throw new Error(`Unexpected GET ${path} with params ${JSON.stringify(params)}`);
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        goal: "Build a dimension table",
        sourceNodeIDs: ["source-1"],
        targetNodeType: "Dimension",
      });

      expect(result.warnings.some(w => w.includes("not observed in current workspace nodes"))).toBe(true);
      expect(result.status).toBe("needs_clarification");
    });
  });

  describe("create-pipeline-from-sql node type validation", () => {
    it("should return plan with warnings instead of executing when node types missing", async () => {
      const client = createMockClient();
      const sourceNode = buildSourceNode("source-1", "CUSTOMER", "RAW");

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        // list-workspace-node-types call
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({
            data: [{ nodeType: "Stage" }]
          });
        }
        // list-workspace-nodes call
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({
            data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source" }]
          });
        }
        // get-workspace-node call
        if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
          return Promise.resolve(sourceNode);
        }
        throw new Error(`Unexpected GET ${path} with params ${JSON.stringify(params)}`);
      });

      const result = await createPipelineFromSql(client as any, {
        workspaceID: "ws-1",
        sql: "SELECT * FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER",
        targetNodeType: "Dimension",
      });

      expect(result).toMatchObject({
        created: false,
      });
      expect((result as any).plan.warnings).toBeDefined();
      expect((result as any).plan.warnings.some((w: string) => w.includes("not observed in current workspace nodes"))).toBe(true);
    });
  });

  describe("CTE detection", () => {
    it("planPipeline returns needs_clarification for SQL with CTEs", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [{ nodeType: "Stage" }] });
        }
        return Promise.resolve({ data: [] });
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        sql: `WITH stg_orders AS (
          SELECT order_id, customer_id FROM orders
        ),
        stg_customers AS (
          SELECT customer_id, name FROM customers
        )
        SELECT * FROM stg_orders JOIN stg_customers ON stg_orders.customer_id = stg_customers.customer_id`,
      });

      expect(result.status).toBe("needs_clarification");
      expect(result.warnings.length).toBeGreaterThan(0);
      expect(result.warnings[0]).toContain("CTEs");
      expect(result.warnings[0]).toContain("STG_ORDERS");
      expect(result.warnings[0]).toContain("STG_CUSTOMERS");
      expect(result.nodes).toHaveLength(0);

      // Validate STOP_AND_CONFIRM
      expect(result.STOP_AND_CONFIRM).toBeDefined();
      expect(result.STOP_AND_CONFIRM).toContain("Present the pipeline summary");

      // Validate cteNodeSummary
      expect(result.cteNodeSummary).toBeDefined();
      expect(result.cteNodeSummary).toHaveLength(2);
      expect(result.cteNodeSummary![0]).toMatchObject({
        name: "STG_ORDERS",
        pattern: "staging",
        transforms: expect.any(Array),
        passthroughColumns: expect.any(Array),
      });
      expect(result.cteNodeSummary![1]).toMatchObject({
        name: "STG_CUSTOMERS",
        pattern: "staging",
      });
      // Each CTE summary should have a nodeType
      for (const summary of result.cteNodeSummary!) {
        expect(summary.nodeType).toBeDefined();
        expect(typeof summary.nodeType).toBe("string");
      }
    });

    it("planPipeline extracts transforms from CTE columns", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [{ nodeType: "Stage" }] });
        }
        return Promise.resolve({ data: [] });
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        sql: `WITH stg_location AS (
          SELECT location_id,
                 placekey,
                 upper(city) AS city,
                 country
          FROM FROSTBYTE_TASTY_BYTES.RAW_POS.LOCATION
          WHERE location_id IS NOT NULL AND location_id != 0
        ),
        stg_customer AS (
          SELECT customer_id,
                 first_name,
                 left(postal_code, 5) AS postal_code,
                 coalesce(e_mail, phone_number) AS contact_info
          FROM FROSTBYTE_TASTY_BYTES.RAW_CUSTOMER.CUSTOMER_LOYALTY
          WHERE customer_id IS NOT NULL
        )
        SELECT * FROM stg_location JOIN stg_customer ON 1=1`,
      });

      expect(result.status).toBe("needs_clarification");
      expect(result.cteNodeSummary).toBeDefined();
      expect(result.cteNodeSummary).toHaveLength(2);

      // stg_location: upper(city) is a transform; location_id, placekey, country are passthrough
      const loc = result.cteNodeSummary!.find((s) => s.name === "STG_LOCATION");
      expect(loc).toBeDefined();
      expect(loc!.transforms.length).toBe(1);
      expect(loc!.transforms[0]!.column).toBe("CITY");
      expect(loc!.transforms[0]!.expression).toMatch(/upper/i);
      expect(loc!.passthroughColumns).toContain("LOCATION_ID");
      expect(loc!.passthroughColumns).toContain("PLACEKEY");
      expect(loc!.passthroughColumns).toContain("COUNTRY");
      expect(loc!.whereFilter).toMatch(/location_id/i);

      // stg_customer: left() and coalesce() are transforms; customer_id, first_name are passthrough
      const cust = result.cteNodeSummary!.find((s) => s.name === "STG_CUSTOMER");
      expect(cust).toBeDefined();
      expect(cust!.transforms.length).toBe(2);
      expect(cust!.transforms.map((t) => t.column).sort()).toEqual(["CONTACT_INFO", "POSTAL_CODE"]);
      expect(cust!.passthroughColumns).toContain("CUSTOMER_ID");
      expect(cust!.passthroughColumns).toContain("FIRST_NAME");

      // Verify columnsParam for single-call creation
      expect(loc!.columnsParam).toBeDefined();
      expect(loc!.columnsParam).toHaveLength(4); // LOCATION_ID, CITY (transform), PLACEKEY, COUNTRY
      const cityParam = loc!.columnsParam!.find((c: any) => c.name === "CITY");
      expect(cityParam).toBeDefined();
      expect(cityParam!.transform).toMatch(/upper/i);
      // Passthrough columns should NOT have a transform field
      const locIdParam = loc!.columnsParam!.find((c: any) => c.name === "LOCATION_ID");
      expect(locIdParam).toBeDefined();
      expect(locIdParam!.transform).toBeUndefined();

      // Verify single-call workflow instructions
      expect(result.openQuestions.some((q: string) => q.includes("1 call") && q.includes("columns"))).toBe(true);
    });

    it("planPipeline detects column renames as transforms", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [{ nodeType: "Stage" }] });
        }
        return Promise.resolve({ data: [] });
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        sql: `WITH stg_data AS (
          SELECT first_name AS cust_first_name,
                 last_name AS cust_last_name,
                 city
          FROM SOME_TABLE
        )
        SELECT * FROM stg_data`,
      });

      const stg = result.cteNodeSummary!.find((s) => s.name === "STG_DATA");
      expect(stg).toBeDefined();
      // Renames (first_name AS cust_first_name) should be detected as transforms
      expect(stg!.transforms.length).toBe(2);
      expect(stg!.transforms.map((t) => t.column).sort()).toEqual(["CUST_FIRST_NAME", "CUST_LAST_NAME"]);
      // city (no rename) should be passthrough
      expect(stg!.passthroughColumns).toContain("CITY");
      // columnsParam should include transform for renamed columns
      expect(stg!.columnsParam).toBeDefined();
      const firstNameParam = stg!.columnsParam!.find((c: any) => c.name === "CUST_FIRST_NAME");
      expect(firstNameParam!.transform).toBe("first_name");
    });

    it("planPipeline generates groupByColumnsParam and aggregatesParam for GROUP BY CTEs", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [{ nodeType: "Stage" }] });
        }
        return Promise.resolve({ data: [] });
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        sql: `WITH stg_totals AS (
          SELECT order_id,
                 order_currency,
                 sum(order_amount) AS total_amount,
                 count(order_id) AS order_count
          FROM orders
          GROUP BY order_id, order_currency
        )
        SELECT * FROM stg_totals`,
      });

      const totals = result.cteNodeSummary!.find((s) => s.name === "STG_TOTALS");
      expect(totals).toBeDefined();
      expect(totals!.hasGroupBy).toBe(true);
      expect(totals!.pattern).toBe("aggregation");

      // Should NOT have columnsParam (that's for non-GROUP-BY)
      expect(totals!.columnsParam).toBeUndefined();

      // Should have groupByColumnsParam
      expect(totals!.groupByColumnsParam).toBeDefined();
      expect(totals!.groupByColumnsParam).toHaveLength(2);
      expect(totals!.groupByColumnsParam![0]).toBe("order_id");
      expect(totals!.groupByColumnsParam![1]).toBe("order_currency");

      // Should have aggregatesParam
      expect(totals!.aggregatesParam).toBeDefined();
      expect(totals!.aggregatesParam).toHaveLength(2);
      expect(totals!.aggregatesParam![0]).toMatchObject({
        name: "TOTAL_AMOUNT",
        function: "SUM",
        expression: "order_amount",
      });
      expect(totals!.aggregatesParam![1]).toMatchObject({
        name: "ORDER_COUNT",
        function: "COUNT",
        expression: "order_id",
      });
    });

    it("planPipeline parses SELECT * FROM (subquery) CTEs", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [{ nodeType: "Stage" }] });
        }
        return Promise.resolve({ data: [] });
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        sql: `WITH stg_orders AS (
          SELECT order_id, customer_id, order_total FROM orders
        ),
        orders_cleaned AS (
          SELECT *
          FROM
            (SELECT o.order_id,
                    o.customer_id,
                    o.order_total,
                    row_number() OVER (PARTITION BY order_id ORDER BY order_id) AS dupe_count
             FROM stg_orders AS o)
          WHERE dupe_count = 1
        )
        SELECT * FROM orders_cleaned`,
      });

      expect(result.status).toBe("needs_clarification");
      expect(result.cteNodeSummary).toHaveLength(2);

      // orders_cleaned: SELECT * FROM (subquery) — should parse the inner subquery columns
      const cleaned = result.cteNodeSummary!.find((s) => s.name === "ORDERS_CLEANED");
      expect(cleaned).toBeDefined();
      // Inner subquery has: order_id, customer_id, order_total (passthrough) + row_number() as dupe_count (transform)
      expect(cleaned!.transforms.length).toBe(1);
      expect(cleaned!.transforms[0]!.column).toBe("DUPE_COUNT");
      expect(cleaned!.transforms[0]!.expression).toMatch(/row_number/i);
      expect(cleaned!.passthroughColumns).toContain("ORDER_ID");
      expect(cleaned!.passthroughColumns).toContain("CUSTOMER_ID");
      expect(cleaned!.passthroughColumns).toContain("ORDER_TOTAL");
      // Outer WHERE should still be captured
      expect(cleaned!.whereFilter).toMatch(/dupe_count\s*=\s*1/i);

      // Final SELECT is just `SELECT * FROM orders_cleaned` — should be flagged as redundant
      const questions = result.openQuestions ?? [];
      const finalNote = questions.find((q: string) => /redundant/i.test(q) || /Do NOT create an additional/i.test(q));
      expect(finalNote).toBeDefined();
    });

    it("planPipeline handles CTE bodies with double-quoted identifiers containing parens", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [{ nodeType: "Stage" }] });
        }
        return Promise.resolve({ data: [] });
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        sql: `WITH stg_data AS (
          SELECT "col(x)" AS col_x, name FROM my_table
        ),
        stg_other AS (
          SELECT id, value FROM other_table
        )
        SELECT * FROM stg_data JOIN stg_other ON stg_data.col_x = stg_other.id`,
      });

      expect(result.status).toBe("needs_clarification");
      // Both CTEs must be found despite the double-quoted identifier with parens
      expect(result.cteNodeSummary).toHaveLength(2);
      expect(result.cteNodeSummary![0]!.name).toBe("STG_DATA");
      expect(result.cteNodeSummary![1]!.name).toBe("STG_OTHER");
    });

    it("planPipeline ignores CTE-like keywords inside string literals", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [{ nodeType: "Stage" }] });
        }
        return Promise.resolve({ data: [] });
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        sql: `WITH stg_data AS (
          SELECT id, 'WHERE x > 1' AS label FROM my_table
        )
        SELECT * FROM stg_data`,
      });

      expect(result.status).toBe("needs_clarification");
      expect(result.cteNodeSummary).toHaveLength(1);
      // The string 'WHERE x > 1' should NOT be parsed as a WHERE clause
      expect(result.cteNodeSummary![0]!.whereFilter).toBeNull();
    });

    it("planPipeline handles block comments with parens in CTE bodies", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [{ nodeType: "Stage" }] });
        }
        return Promise.resolve({ data: [] });
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        sql: `WITH stg_data AS (
          SELECT id, /* func() */ name FROM my_table
        ),
        stg_next AS (
          SELECT a, b FROM next_table
        )
        SELECT * FROM stg_data JOIN stg_next ON stg_data.id = stg_next.a`,
      });

      expect(result.status).toBe("needs_clarification");
      // Block comment with parens must not corrupt paren depth
      expect(result.cteNodeSummary).toHaveLength(2);
      expect(result.cteNodeSummary![0]!.name).toBe("STG_DATA");
      expect(result.cteNodeSummary![1]!.name).toBe("STG_NEXT");
    });

    it("planPipeline allows SQL without CTEs", async () => {
      const client = createMockClient();
      const sourceNode = buildSourceNode("source-1", "CUSTOMER");

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [{ nodeType: "Stage" }] });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({
            data: [{ id: "source-1", name: "CUSTOMER", nodeType: "Source" }],
          });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
          return Promise.resolve(sourceNode);
        }
        return Promise.resolve({ data: [] });
      });

      const result = await planPipeline(client as any, {
        workspaceID: "ws-1",
        sql: `SELECT customer_id, name FROM {{ ref('RAW', 'CUSTOMER') }} CUSTOMER`,
        repoPath: fixtureRepoPath,
      });

      // Non-CTE SQL should be parsed normally
      expect(result.status).toBe("ready");
    });
  });

  describe("review_pipeline tool", () => {
    it("returns findings for an empty workspace", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({ data: [] });
        }
        return Promise.resolve({ data: [] });
      });

      registerPipelineTools(server, client as any);

      const handler = toolSpy.mock.calls.find(
        (call) => call[0] === "review_pipeline"
      )?.[2] as ((params: { workspaceID: string }) => Promise<{ content: { text: string }[] }>) | undefined;

      expect(typeof handler).toBe("function");

      const result = await handler!({ workspaceID: "ws-1" });

      const data = JSON.parse(result.content[0]!.text);
      expect(data.workspaceID).toBe("ws-1");
      expect(data.nodeCount).toBe(0);
      expect(Array.isArray(data.findings)).toBe(true);
      expect(typeof data.summary).toBe("object");
    });

    it("returns findings for a workspace with a passthrough node", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      const summaryNode = {
        id: "stg-1",
        name: "STG_CUSTOMERS",
        nodeType: "Stage",
        locationName: "STAGING",
        predecessorNodeIDs: ["src-1"],
      };
      const sourceNode = {
        id: "src-1",
        name: "CUSTOMERS",
        nodeType: "Source",
        locationName: "RAW",
        predecessorNodeIDs: [],
      };

      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({ data: [summaryNode, sourceNode] });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/stg-1") {
          return Promise.resolve({
            id: "stg-1",
            name: "STG_CUSTOMERS",
            nodeType: "Stage",
            config: {},
            metadata: {
              columns: [
                { name: "ID", transform: "", sources: [{ columnReferences: [{ nodeID: "src-1" }] }] },
              ],
            },
          });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/src-1") {
          return Promise.resolve({
            id: "src-1",
            name: "CUSTOMERS",
            nodeType: "Source",
            config: {},
            metadata: { columns: [{ name: "ID", transform: "" }] },
          });
        }
        return Promise.resolve({ data: [] });
      });

      registerPipelineTools(server, client as any);

      const handler = toolSpy.mock.calls.find(
        (call) => call[0] === "review_pipeline"
      )?.[2] as ((params: { workspaceID: string }) => Promise<{ content: { text: string }[] }>) | undefined;

      const result = await handler!({ workspaceID: "ws-1" });

      const data = JSON.parse(result.content[0]!.text);
      expect(data.workspaceID).toBe("ws-1");
      expect(data.nodeCount).toBeGreaterThan(0);
      expect(typeof data.summary.critical).toBe("number");
      expect(typeof data.summary.warning).toBe("number");
      expect(typeof data.summary.suggestion).toBe("number");
    });

    it("returns isError when workspace node fetch fails", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      client.get.mockRejectedValue(new CoalesceApiError("Resource not found", 404));

      registerPipelineTools(server, client as any);

      const handler = toolSpy.mock.calls.find(
        (call) => call[0] === "review_pipeline"
      )?.[2] as ((params: { workspaceID: string }) => Promise<{ isError?: boolean; content: { text: string }[] }>) | undefined;

      const result = await handler!({ workspaceID: "missing" });
      expect(result.isError).toBe(true);
    });
  });

  describe("build_pipeline_from_intent tool", () => {
    it("returns needs_clarification when workspace has no matching entities", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
        // workspace nodes list (for node-type observation)
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return Promise.resolve({ data: [] });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({ data: [] });
        }
        return Promise.resolve({ data: [] });
      });

      registerPipelineTools(server, client as any);

      const handler = toolSpy.mock.calls.find(
        (call) => call[0] === "build_pipeline_from_intent"
      )?.[2] as ((params: { workspaceID: string; intent: string }) => Promise<{ content: { text: string }[] }>) | undefined;

      expect(typeof handler).toBe("function");

      const result = await handler!({
        workspaceID: "ws-1",
        intent: "join CUSTOMERS and ORDERS on CUSTOMER_ID",
      });

      const data = JSON.parse(result.content[0]!.text);
      // Should not have created nodes (empty workspace → can't resolve entities)
      expect(data.created).toBe(false);
    });

    it("returns isError when workspace fetch fails", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      client.get.mockRejectedValue(new CoalesceApiError("Unauthorized", 401));

      registerPipelineTools(server, client as any);

      const handler = toolSpy.mock.calls.find(
        (call) => call[0] === "build_pipeline_from_intent"
      )?.[2] as ((params: { workspaceID: string; intent: string }) => Promise<{ isError?: boolean; content: { text: string }[] }>) | undefined;

      const result = await handler!({ workspaceID: "ws-1", intent: "stage customers" });
      expect(result.isError).toBe(true);
    });
  });
});
