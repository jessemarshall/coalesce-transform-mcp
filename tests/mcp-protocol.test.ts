import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { buildCacheResourceUri } from "../src/cache-dir.js";
import { buildSourceNode, buildCreatedStageNode } from "./helpers/fixtures.js";
import {
  createConnectedMcpHarness,
  createMockApiClient,
} from "./helpers/mcp-harness.js";

const tempDirs: string[] = [];

afterEach(() => {
  vi.restoreAllMocks();
  for (const directory of tempDirs.splice(0, tempDirs.length)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("MCP Protocol Surface", () => {
  it("initializes and lists tool metadata over a real MCP client/transport", async () => {
    const harness = await createConnectedMcpHarness(createMockApiClient());

    try {
      expect(harness.client.getServerVersion()).toEqual({
        name: "coalesce-transform-mcp",
        version: expect.any(String),
      });
      expect(harness.client.getInstructions()).toContain("coalesce_plan_pipeline");

      const capabilities = harness.client.getServerCapabilities();
      expect(capabilities?.tools).toBeDefined();
      expect(capabilities?.resources).toBeDefined();
      expect(capabilities?.prompts).toBeDefined();

      const result = await harness.client.listTools();
      expect(result.tools.length).toBeGreaterThan(70);

      const toolNames = result.tools.map((tool) => tool.name);
      expect(toolNames).toContain("coalesce_set_workspace_node");
      expect(toolNames).toContain("coalesce_replace_workspace_node_columns");
      expect(toolNames).toContain("coalesce_create_pipeline_from_plan");
      expect(toolNames).toContain("coalesce_clear_data_cache");

      const setTool = result.tools.find((tool) => tool.name === "coalesce_set_workspace_node");
      expect(setTool?.annotations).toMatchObject({
        readOnlyHint: false,
        idempotentHint: true,
        destructiveHint: false,
      });
      expect(setTool?.outputSchema?.type).toBe("object");
      expect(setTool?.outputSchema?.properties).toMatchObject({
        validation: expect.objectContaining({ type: "object" }),
        warning: expect.objectContaining({ type: "string" }),
      });
      expect(setTool?.outputSchema?.properties).toHaveProperty("configCompletion");
      expect(setTool?.inputSchema).toMatchObject({
        type: "object",
        properties: {
          body: expect.objectContaining({
            type: "object",
            properties: expect.objectContaining({
              config: expect.objectContaining({ type: "object" }),
              metadata: expect.objectContaining({ type: "object" }),
            }),
          }),
        },
      });

      const replaceColumnsTool = result.tools.find(
        (tool) => tool.name === "coalesce_replace_workspace_node_columns"
      );
      expect(replaceColumnsTool?.inputSchema).toMatchObject({
        type: "object",
        properties: {
          columns: expect.objectContaining({
            type: "array",
            items: expect.objectContaining({ type: "object" }),
          }),
        },
      });

      const createFromPlanTool = result.tools.find(
        (tool) => tool.name === "coalesce_create_pipeline_from_plan"
      );
      expect(createFromPlanTool?.inputSchema).toMatchObject({
        type: "object",
        properties: {
          plan: expect.objectContaining({
            type: "object",
            properties: expect.objectContaining({
              nodes: expect.objectContaining({ type: "array" }),
            }),
          }),
        },
      });

      const clearCacheTool = result.tools.find(
        (tool) => tool.name === "coalesce_clear_data_cache"
      );
      expect(clearCacheTool?.annotations).toMatchObject({
        destructiveHint: true,
      });

      const listRunsTool = result.tools.find((tool) => tool.name === "coalesce_list_runs");
      expect(listRunsTool?.outputSchema).toMatchObject({
        type: "object",
        properties: expect.objectContaining({
          data: expect.objectContaining({
            type: "array",
          }),
          next: expect.objectContaining({ type: "string" }),
        }),
      });

      const planPipelineTool = result.tools.find(
        (tool) => tool.name === "coalesce_plan_pipeline"
      );
      expect(planPipelineTool?.description).toContain("planSummaryUri");
      expect(planPipelineTool?.description).not.toContain("data/plans/");
      expect(planPipelineTool?.outputSchema).toMatchObject({
        type: "object",
        properties: expect.objectContaining({
          status: expect.objectContaining({ type: "string" }),
          planSummaryUri: expect.objectContaining({ type: "string" }),
          USE_THIS_NODE_TYPE: expect.objectContaining({ type: "string" }),
        }),
      });

      const runAndWaitTool = result.tools.find(
        (tool) => tool.name === "coalesce_run_and_wait"
      );
      expect(runAndWaitTool?.outputSchema).toMatchObject({
        type: "object",
        properties: expect.objectContaining({
          status: expect.objectContaining({}),
          results: expect.objectContaining({}),
          timedOut: expect.objectContaining({ type: "boolean" }),
          incomplete: expect.objectContaining({ type: "boolean" }),
        }),
      });

      const prompts = await harness.client.listPrompts();
      const promptNames = prompts.prompts.map((prompt) => prompt.name);
      expect(promptNames).toEqual(
        expect.arrayContaining([
          "coalesce-start-here",
          "safe-pipeline-planning",
          "run-operations-guide",
          "large-result-handling",
        ])
      );

      const planningPrompt = await harness.client.getPrompt({
        name: "safe-pipeline-planning",
      });
      expect(planningPrompt.messages).toEqual([
        expect.objectContaining({
          role: "user",
          content: expect.objectContaining({
            type: "text",
            text: expect.stringContaining("Always call coalesce_plan_pipeline"),
          }),
        }),
      ]);
    } finally {
      await harness.close();
    }
  });

  it("lists fixed resources, exposes the cache template, and reads resources over MCP", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "coalesce-mcp-resource-protocol-"));
    tempDirs.push(tempDir);

    const cacheFilePath = join(
      tempDir,
      "coalesce_transform_mcp_data_cache",
      "auto-cache",
      "cached-response.json"
    );
    mkdirSync(join(tempDir, "coalesce_transform_mcp_data_cache", "auto-cache"), {
      recursive: true,
    });
    writeFileSync(cacheFilePath, JSON.stringify({ ok: true }, null, 2), "utf8");

    vi.spyOn(process, "cwd").mockReturnValue(tempDir);

    const harness = await createConnectedMcpHarness(createMockApiClient());

    try {
      const resources = await harness.client.listResources();
      expect(resources.resources.length).toBeGreaterThan(15);
      expect(resources.resources).toContainEqual(
        expect.objectContaining({
          uri: "coalesce://context/overview",
          name: "Coalesce Overview",
          mimeType: "text/markdown",
        })
      );

      const templates = await harness.client.listResourceTemplates();
      expect(templates.resourceTemplates).toContainEqual(
        expect.objectContaining({
          name: "Coalesce Cache Artifact",
          uriTemplate: "coalesce://cache/{cacheKey}",
        })
      );

      const overview = await harness.client.readResource({
        uri: "coalesce://context/overview",
      });
      expect(overview.contents).toEqual([
        expect.objectContaining({
          uri: "coalesce://context/overview",
          mimeType: "text/markdown",
          text: expect.stringContaining("Coalesce"),
        }),
      ]);

      const nodeOperations = await harness.client.readResource({
        uri: "coalesce://context/node-operations",
      });
      expect(nodeOperations.contents).toEqual([
        expect.objectContaining({
          uri: "coalesce://context/node-operations",
          mimeType: "text/markdown",
          text: expect.stringContaining(
            "Pass the user's exact SQL unchanged to `coalesce_plan_pipeline` or `coalesce_create_pipeline_from_sql`"
          ),
        }),
      ]);
      expect(nodeOperations.contents[0]?.text).not.toContain(
        "Rewrite with `{{ ref('LOCATION', 'NODE') }}` syntax, preserving original aliases"
      );

      const toolUsage = await harness.client.readResource({
        uri: "coalesce://context/tool-usage",
      });
      expect(toolUsage.contents).toEqual([
        expect.objectContaining({
          uri: "coalesce://context/tool-usage",
          mimeType: "text/markdown",
          text: expect.stringContaining("`coalesce://cache/...` resource URI"),
        }),
      ]);
      expect(toolUsage.contents[0]?.text).not.toContain("plus the file path");

      const pipelineWorkflows = await harness.client.readResource({
        uri: "coalesce://context/pipeline-workflows",
      });
      expect(pipelineWorkflows.contents).toEqual([
        expect.objectContaining({
          uri: "coalesce://context/pipeline-workflows",
          mimeType: "text/markdown",
          text: expect.stringContaining(
            "still call `coalesce_plan_pipeline` before creating anything"
          ),
        }),
      ]);
      expect(pipelineWorkflows.contents[0]?.text).not.toContain(
        "skip discovery and create directly"
      );

      const cacheUri = buildCacheResourceUri(cacheFilePath, tempDir);
      expect(cacheUri).toBeTruthy();

      const cached = await harness.client.readResource({
        uri: cacheUri!,
      });
      expect(cached.contents).toEqual([
        {
          uri: cacheUri,
          mimeType: "application/json",
          text: JSON.stringify({ ok: true }, null, 2),
        },
      ]);
    } finally {
      await harness.close();
    }
  });

  it("calls tools and returns MCP-native validation errors over the protocol", async () => {
    const harness = await createConnectedMcpHarness(
      createMockApiClient({
        get: vi.fn(async (path: unknown) => {
          if (path === "/api/v1/runs") {
            return {
              data: [
                {
                  id: "run-1",
                  status: "completed",
                  userCredentials: { snowflakeUsername: "secret-user" },
                },
              ],
            };
          }
          return { data: [] };
        }),
      })
    );

    try {
      const listRunsResult = await harness.client.callTool({
        name: "coalesce_list_runs",
        arguments: {},
      });

      expect(listRunsResult.isError).toBeUndefined();
      expect(listRunsResult.content[0]).toMatchObject({
        type: "text",
      });
      expect((listRunsResult.content[0] as { text: string }).text).not.toContain(
        "userCredentials"
      );
      expect(listRunsResult.structuredContent).toEqual({
        data: [{ id: "run-1", status: "completed" }],
      });

      const invalidSetNodeResult = await harness.client.callTool({
        name: "coalesce_set_workspace_node",
        arguments: {
          workspaceID: "ws-1",
          nodeID: "node-1",
          body: "invalid-body",
        },
      });

      expect(invalidSetNodeResult).toMatchObject({
        isError: true,
        content: [
          {
            type: "text",
            text: expect.stringContaining("Input validation error"),
          },
        ],
      });
    } finally {
      await harness.close();
    }
  });

  it("executes create-pipeline-from-sql with confirmation token over MCP harness", async () => {
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");
    const createdNode = buildCreatedStageNode("source-1");
    let savedBody: Record<string, unknown> | null = null;

    const apiClient = createMockApiClient({
      get: vi.fn(async (path: unknown, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return {
            data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
          };
        }
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return {
            data: [
              {
                id: "source-1",
                name: "CUSTOMER",
                nodeType: "Source",
                locationName: "RAW",
              },
            ],
          };
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
          return sourceNode;
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/new-node") {
          return savedBody ?? createdNode;
        }
        throw new Error(`Unexpected GET ${String(path)} ${JSON.stringify(params)}`);
      }),
      post: vi.fn(async () => ({ id: "new-node" })),
      put: vi.fn(async (_path: unknown, body: unknown) => {
        savedBody = body as Record<string, unknown>;
        return body;
      }),
    });

    const harness = await createConnectedMcpHarness(apiClient);

    try {
      // Simulate a client that does not support elicitation
      vi.spyOn(harness.server.server, "elicitInput").mockRejectedValue(
        new Error("Client does not support elicitation")
      );

      // Step 1: call without confirmed — should return STOP_AND_CONFIRM with token
      const preview = await harness.client.callTool({
        name: "coalesce_create_pipeline_from_sql",
        arguments: {
          workspaceID: "ws-1",
          sql: "SELECT * FROM RAW.CUSTOMER",
        },
      });

      expect(preview.isError).toBeUndefined();
      const previewContent = preview.structuredContent as Record<string, unknown>;
      expect(previewContent.created).toBe(false);
      expect(previewContent.STOP_AND_CONFIRM).toBeDefined();
      expect(typeof previewContent.confirmationToken).toBe("string");
      expect(apiClient.post).not.toHaveBeenCalled();
      expect(apiClient.put).not.toHaveBeenCalled();

      // Step 2: call with confirmed=true and the matching token — should execute
      const result = await harness.client.callTool({
        name: "coalesce_create_pipeline_from_sql",
        arguments: {
          workspaceID: "ws-1",
          sql: "SELECT * FROM RAW.CUSTOMER",
          confirmed: true,
          confirmationToken: previewContent.confirmationToken as string,
        },
      });

      expect(result.isError).toBeUndefined();
      expect(result.structuredContent).toMatchObject({
        created: true,
        workspaceID: "ws-1",
        nodeCount: 1,
        plan: expect.objectContaining({
          status: "ready",
          sql: "SELECT * FROM RAW.CUSTOMER",
          nodes: [
            expect.objectContaining({
              joinCondition: "FROM {{ ref('RAW', 'CUSTOMER') }}",
            }),
          ],
        }),
      });
      expect(result.structuredContent).not.toHaveProperty("STOP_AND_CONFIRM");
      expect(apiClient.post).toHaveBeenCalledTimes(1);
      expect(apiClient.put).toHaveBeenCalledTimes(1);
      expect(savedBody).not.toBeNull();
      expect((savedBody as any).metadata.sourceMapping[0].join.joinCondition).toBe(
        "FROM {{ ref('RAW', 'CUSTOMER') }}"
      );
    } finally {
      await harness.close();
    }
  });

  it("rejects create-pipeline-from-sql with confirmed=true but no token over MCP harness", async () => {
    const sourceNode = buildSourceNode("source-1", "CUSTOMER");

    const apiClient = createMockApiClient({
      get: vi.fn(async (path: unknown, params?: Record<string, unknown>) => {
        if (path === "/api/v1/workspaces/ws-1/nodes" && params?.detail === false) {
          return {
            data: [{ nodeType: "Stage" }, { nodeType: "Source" }],
          };
        }
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return {
            data: [
              {
                id: "source-1",
                name: "CUSTOMER",
                nodeType: "Source",
                locationName: "RAW",
              },
            ],
          };
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/source-1") {
          return sourceNode;
        }
        throw new Error(`Unexpected GET ${String(path)} ${JSON.stringify(params)}`);
      }),
      post: vi.fn(async () => ({ id: "new-node" })),
      put: vi.fn(),
    });

    const harness = await createConnectedMcpHarness(apiClient);

    try {
      // Simulate a client that does not support elicitation
      vi.spyOn(harness.server.server, "elicitInput").mockRejectedValue(
        new Error("Client does not support elicitation")
      );

      // confirmed=true without a token should be rejected with STOP_AND_CONFIRM
      const result = await harness.client.callTool({
        name: "coalesce_create_pipeline_from_sql",
        arguments: {
          workspaceID: "ws-1",
          sql: "SELECT * FROM RAW.CUSTOMER",
          confirmed: true,
        },
      });

      expect(result.isError).toBeUndefined();
      const content = result.structuredContent as Record<string, unknown>;
      expect(content.created).toBe(false);
      expect(content.STOP_AND_CONFIRM).toBeDefined();
      expect(typeof content.confirmationToken).toBe("string");
      expect(apiClient.post).not.toHaveBeenCalled();
      expect(apiClient.put).not.toHaveBeenCalled();
    } finally {
      await harness.close();
    }
  });
});
