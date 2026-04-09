import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { defineLineageTools } from "../../src/mcp/lineage.js";

// Mock the lineage cache service so tool-handler tests don't hit the real API
vi.mock("../../src/services/lineage/lineage-cache.js", () => ({
  buildLineageCache: vi.fn(),
  walkUpstream: vi.fn(),
  walkDownstream: vi.fn(),
  walkColumnLineage: vi.fn(),
  analyzeNodeImpact: vi.fn(),
  propagateColumnChange: vi.fn(),
  searchWorkspaceContent: vi.fn(),
}));

import {
  buildLineageCache,
  walkUpstream,
  walkDownstream,
  walkColumnLineage,
  analyzeNodeImpact,
  propagateColumnChange,
  searchWorkspaceContent,
} from "../../src/services/lineage/lineage-cache.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ ok: true }),
    put: vi.fn().mockResolvedValue({ ok: true }),
    patch: vi.fn().mockResolvedValue({ ok: true }),
    delete: vi.fn().mockResolvedValue({ ok: true }),
  };
}

// Minimal fake cache that satisfies the handler node-lookup checks
function buildFakeCache(nodes: Record<string, { name: string; nodeType: string; columns: { id: string; name: string }[] }>) {
  const nodesMap = new Map(
    Object.entries(nodes).map(([id, n]) => [id, { id, ...n }])
  );
  return { nodes: nodesMap };
}

beforeEach(() => {
  vi.resetAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

// Helper to extract a registered tool's handler from the spy
function extractHandler<T extends object>(
  spy: ReturnType<typeof vi.spyOn<McpServer, "registerTool">>,
  toolName: string
): (params: T, extra?: unknown) => Promise<{ content: Array<{ text: string }>; isError?: boolean }> {
  const call = spy.mock.calls.find((c) => c[0] === toolName);
  if (!call) throw new Error(`Tool "${toolName}" was not registered`);
  return call[2] as (params: T, extra?: unknown) => Promise<{ content: Array<{ text: string }>; isError?: boolean }>;
}

describe("Lineage Tool Handlers", () => {
  it("registers all 6 lineage tools without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    expect(() => defineLineageTools(server, client as never)).not.toThrow();
  });

  // --- get_upstream_nodes ---
  describe("get_upstream_nodes", () => {
    it("returns ancestors when node exists in cache", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const fakeCache = buildFakeCache({
        "n1": { name: "SRC_RAW", nodeType: "Source", columns: [] },
        "n2": { name: "STG_ORDERS", nodeType: "Stage", columns: [] },
      });
      vi.mocked(buildLineageCache).mockResolvedValue(fakeCache as never);
      vi.mocked(walkUpstream).mockReturnValue([
        { nodeID: "n1", nodeName: "SRC_RAW", nodeType: "Source", depth: 1 },
      ]);

      const handler = extractHandler<{ workspaceID: string; nodeID: string }>(spy, "get_upstream_nodes");
      const result = await handler({ workspaceID: "ws-1", nodeID: "n2" });

      expect(result.isError).toBeUndefined();
      const data = JSON.parse(result.content[0]!.text);
      expect(data.nodeID).toBe("n2");
      expect(data.nodeName).toBe("STG_ORDERS");
      expect(data.totalAncestors).toBe(1);
      expect(data.ancestors).toHaveLength(1);
      expect(data.ancestors[0].nodeID).toBe("n1");
    });

    it("returns isError when nodeID is not found in cache", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const fakeCache = buildFakeCache({ "n1": { name: "SRC_RAW", nodeType: "Source", columns: [] } });
      vi.mocked(buildLineageCache).mockResolvedValue(fakeCache as never);

      const handler = extractHandler<{ workspaceID: string; nodeID: string }>(spy, "get_upstream_nodes");
      const result = await handler({ workspaceID: "ws-1", nodeID: "missing-node" });

      expect(result.isError).toBe(true);
      expect(result.content[0]!.text).toContain("missing-node");
    });

    it("returns isError when workspaceID is empty", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const handler = extractHandler<{ workspaceID: string; nodeID: string }>(spy, "get_upstream_nodes");
      const result = await handler({ workspaceID: "", nodeID: "n1" });

      expect(result.isError).toBe(true);
    });

    it("returns isError when buildLineageCache throws", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      vi.mocked(buildLineageCache).mockRejectedValue(new Error("API unreachable"));

      const handler = extractHandler<{ workspaceID: string; nodeID: string }>(spy, "get_upstream_nodes");
      const result = await handler({ workspaceID: "ws-1", nodeID: "n1" });

      expect(result.isError).toBe(true);
      expect(result.content[0]!.text).toContain("API unreachable");
    });
  });

  // --- get_downstream_nodes ---
  describe("get_downstream_nodes", () => {
    it("returns dependents when node exists in cache", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const fakeCache = buildFakeCache({
        "n1": { name: "SRC_RAW", nodeType: "Source", columns: [] },
        "n2": { name: "STG_ORDERS", nodeType: "Stage", columns: [] },
      });
      vi.mocked(buildLineageCache).mockResolvedValue(fakeCache as never);
      vi.mocked(walkDownstream).mockReturnValue([
        { nodeID: "n2", nodeName: "STG_ORDERS", nodeType: "Stage", depth: 1 },
      ]);

      const handler = extractHandler<{ workspaceID: string; nodeID: string }>(spy, "get_downstream_nodes");
      const result = await handler({ workspaceID: "ws-1", nodeID: "n1" });

      expect(result.isError).toBeUndefined();
      const data = JSON.parse(result.content[0]!.text);
      expect(data.nodeID).toBe("n1");
      expect(data.totalDependents).toBe(1);
      expect(data.dependents[0].nodeID).toBe("n2");
    });

    it("returns isError when nodeID not in cache", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      vi.mocked(buildLineageCache).mockResolvedValue(buildFakeCache({}) as never);

      const handler = extractHandler<{ workspaceID: string; nodeID: string }>(spy, "get_downstream_nodes");
      const result = await handler({ workspaceID: "ws-1", nodeID: "ghost" });

      expect(result.isError).toBe(true);
      expect(result.content[0]!.text).toContain("ghost");
    });
  });

  // --- get_column_lineage ---
  describe("get_column_lineage", () => {
    it("returns column lineage when node and column exist", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const fakeCache = buildFakeCache({
        "n1": { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "order_id" }] },
      });
      vi.mocked(buildLineageCache).mockResolvedValue(fakeCache as never);
      vi.mocked(walkColumnLineage).mockReturnValue([
        { nodeID: "n2", nodeName: "STG_ORDERS", nodeType: "Stage", columnID: "c2", columnName: "order_id", direction: "downstream" as const, depth: 1 },
      ]);

      const handler = extractHandler<{ workspaceID: string; nodeID: string; columnID: string }>(spy, "get_column_lineage");
      const result = await handler({ workspaceID: "ws-1", nodeID: "n1", columnID: "c1" });

      expect(result.isError).toBeUndefined();
      const data = JSON.parse(result.content[0]!.text);
      expect(data.nodeID).toBe("n1");
      expect(data.columnID).toBe("c1");
      expect(data.columnName).toBe("order_id");
      expect(data.totalDownstream).toBe(1);
      expect(data.totalUpstream).toBe(0);
    });

    it("returns isError when columnID not found on node", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const fakeCache = buildFakeCache({
        "n1": { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "order_id" }] },
      });
      vi.mocked(buildLineageCache).mockResolvedValue(fakeCache as never);

      const handler = extractHandler<{ workspaceID: string; nodeID: string; columnID: string }>(spy, "get_column_lineage");
      const result = await handler({ workspaceID: "ws-1", nodeID: "n1", columnID: "bad-col" });

      expect(result.isError).toBe(true);
      expect(result.content[0]!.text).toContain("bad-col");
    });

    it("returns isError when node not found", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      vi.mocked(buildLineageCache).mockResolvedValue(buildFakeCache({}) as never);

      const handler = extractHandler<{ workspaceID: string; nodeID: string; columnID: string }>(spy, "get_column_lineage");
      const result = await handler({ workspaceID: "ws-1", nodeID: "missing", columnID: "c1" });

      expect(result.isError).toBe(true);
    });
  });

  // --- analyze_impact ---
  describe("analyze_impact", () => {
    it("returns full impact analysis for a node", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      vi.mocked(buildLineageCache).mockResolvedValue(buildFakeCache({
        "n1": { name: "SRC_RAW", nodeType: "Source", columns: [] },
      }) as never);
      vi.mocked(analyzeNodeImpact).mockReturnValue({
        sourceNodeID: "n1",
        sourceNodeName: "SRC_RAW",
        sourceNodeType: "Source",
        impactedNodes: [{ nodeID: "n2", nodeName: "STG_ORDERS", nodeType: "Stage", depth: 1 }],
        impactedColumns: [],
        totalImpactedNodes: 1,
        totalImpactedColumns: 0,
        byDepth: { "1": ["STG_ORDERS"] },
        criticalPath: ["SRC_RAW", "STG_ORDERS"],
      });

      const handler = extractHandler<{ workspaceID: string; nodeID: string; columnID?: string }>(spy, "analyze_impact");
      const result = await handler({ workspaceID: "ws-1", nodeID: "n1" });

      expect(result.isError).toBeUndefined();
      const data = JSON.parse(result.content[0]!.text);
      expect(data.sourceNodeID).toBe("n1");
      expect(data.totalImpactedNodes).toBe(1);
      expect(data.criticalPath).toEqual(["SRC_RAW", "STG_ORDERS"]);
    });

    it("returns isError when analyzeNodeImpact throws (unknown node)", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      vi.mocked(buildLineageCache).mockResolvedValue(buildFakeCache({}) as never);
      vi.mocked(analyzeNodeImpact).mockImplementation(() => {
        throw new Error("Node nope not found in lineage cache");
      });

      const handler = extractHandler<{ workspaceID: string; nodeID: string }>(spy, "analyze_impact");
      const result = await handler({ workspaceID: "ws-1", nodeID: "nope" });

      expect(result.isError).toBe(true);
      expect(result.content[0]!.text).toContain("not found");
    });
  });

  // --- propagate_column_change ---
  describe("propagate_column_change", () => {
    it("returns propagation result when changes are applied", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      vi.mocked(buildLineageCache).mockResolvedValue(buildFakeCache({
        "n1": { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "order_id" }] },
      }) as never);
      vi.mocked(walkColumnLineage).mockReturnValue([
        { nodeID: "n2", nodeName: "STG_ORDERS", nodeType: "Stage", columnID: "c2", columnName: "order_id", direction: "downstream" as const, depth: 1 },
      ]);
      vi.mocked(propagateColumnChange).mockResolvedValue({
        sourceNodeID: "n1",
        sourceColumnID: "c1",
        changes: { columnName: "renamed_order_id" },
        preMutationSnapshot: [{ nodeID: "n2", nodeName: "STG_ORDERS", columnID: "c2", previousColumnName: "order_id", previousDataType: "VARCHAR", capturedAt: "2026-04-06T00:00:00.000Z", nodeBody: { id: "n2", name: "STG_ORDERS" } }],
        snapshotPath: "/tmp/test-snapshot.json",
        updatedNodes: [{ nodeID: "n2", nodeName: "STG_ORDERS", columnID: "c2", columnName: "renamed_order_id", previousName: "order_id" }],
        totalUpdated: 1,
        errors: [],
      });

      const handler = extractHandler<{
        workspaceID: string;
        nodeID: string;
        columnID: string;
        changes: { columnName?: string; dataType?: string };
        confirmed?: boolean;
      }>(spy, "propagate_column_change");
      const result = await handler({
        workspaceID: "ws-1",
        nodeID: "n1",
        columnID: "c1",
        changes: { columnName: "renamed_order_id" },
        confirmed: true,
      });

      expect(result.isError).toBeUndefined();
      const data = JSON.parse(result.content[0]!.text);
      expect(data.totalUpdated).toBe(1);
      expect(data.errors).toHaveLength(0);
      expect(data.updatedNodes[0].nodeID).toBe("n2");
      expect(data.preMutationSnapshot).toHaveLength(1);
      expect(data.preMutationSnapshot[0].nodeID).toBe("n2");
      expect(data.preMutationSnapshot[0].previousColumnName).toBe("order_id");
      expect(data.preMutationSnapshot[0].nodeBody).toBeDefined();
    });

    it("returns STOP_AND_CONFIRM when not confirmed", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      vi.mocked(buildLineageCache).mockResolvedValue(buildFakeCache({
        "n1": { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "order_id" }] },
      }) as never);
      vi.mocked(walkColumnLineage).mockReturnValue([
        { nodeID: "n2", nodeName: "STG_ORDERS", nodeType: "Stage", columnID: "c2", columnName: "order_id", direction: "downstream" as const, depth: 1 },
      ]);

      const handler = extractHandler<{
        workspaceID: string;
        nodeID: string;
        columnID: string;
        changes: { columnName?: string; dataType?: string };
        confirmed?: boolean;
      }>(spy, "propagate_column_change");
      const result = await handler({
        workspaceID: "ws-1",
        nodeID: "n1",
        columnID: "c1",
        changes: { columnName: "renamed_order_id" },
      });

      expect(result.isError).toBeUndefined();
      const data = JSON.parse(result.content[0]!.text);
      expect(data.executed).toBe(false);
      expect(data.STOP_AND_CONFIRM).toBeDefined();
      expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
      expect(propagateColumnChange).not.toHaveBeenCalled();
    });

    it("returns isError when propagateColumnChange throws", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      vi.mocked(buildLineageCache).mockResolvedValue(buildFakeCache({
        "n1": { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "order_id" }] },
      }) as never);
      vi.mocked(walkColumnLineage).mockReturnValue([]);
      vi.mocked(propagateColumnChange).mockRejectedValue(new Error("At least one change required"));

      const handler = extractHandler<{
        workspaceID: string;
        nodeID: string;
        columnID: string;
        changes: { columnName?: string; dataType?: string };
        confirmed?: boolean;
      }>(spy, "propagate_column_change");
      const result = await handler({
        workspaceID: "ws-1",
        nodeID: "n1",
        columnID: "c1",
        changes: {},
        confirmed: true,
      });

      expect(result.isError).toBe(true);
      expect(result.content[0]!.text).toContain("At least one change required");
    });

    it("returns isError when workspaceID contains path-traversal characters", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const handler = extractHandler<{
        workspaceID: string;
        nodeID: string;
        columnID: string;
        changes: { columnName?: string };
      }>(spy, "propagate_column_change");
      const result = await handler({
        workspaceID: "../../etc/passwd",
        nodeID: "n1",
        columnID: "c1",
        changes: { columnName: "x" },
      });

      expect(result.isError).toBe(true);
    });
  });

  // --- search_workspace_content ---
  describe("search_workspace_content", () => {
    it("returns search results from cached workspace data", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const fakeCache = buildFakeCache({
        "n1": { name: "STG_ORDERS", nodeType: "Stage", columns: [{ id: "c1", name: "order_id" }] },
      });
      vi.mocked(buildLineageCache).mockResolvedValue(fakeCache as never);
      vi.mocked(searchWorkspaceContent).mockReturnValue({
        query: "order",
        totalResults: 1,
        results: [
          {
            nodeID: "n1",
            nodeName: "STG_ORDERS",
            nodeType: "Stage",
            matches: [{ field: "name", snippet: "STG_ORDERS" }],
          },
        ],
      });

      const handler = extractHandler<{
        workspaceID: string;
        query: string;
        fields?: string[];
        nodeType?: string;
        limit?: number;
      }>(spy, "search_workspace_content");
      const result = await handler({ workspaceID: "ws-1", query: "order" });

      expect(result.isError).toBeUndefined();
      const data = JSON.parse(result.content[0]!.text);
      expect(data.query).toBe("order");
      expect(data.totalResults).toBe(1);
      expect(data.results[0].nodeID).toBe("n1");
    });

    it("returns isError when workspaceID is empty", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const handler = extractHandler<{
        workspaceID: string;
        query: string;
      }>(spy, "search_workspace_content");
      const result = await handler({ workspaceID: "", query: "test" });

      expect(result.isError).toBe(true);
    });

    it("returns isError when buildLineageCache throws", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      vi.mocked(buildLineageCache).mockRejectedValue(new Error("Cache build failed"));

      const handler = extractHandler<{
        workspaceID: string;
        query: string;
      }>(spy, "search_workspace_content");
      const result = await handler({ workspaceID: "ws-1", query: "test" });

      expect(result.isError).toBe(true);
      expect(result.content[0]!.text).toContain("Cache build failed");
    });

    it("passes optional fields and limit to searchWorkspaceContent", async () => {
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      defineLineageTools(server, client as never).forEach(t => server.registerTool(...t));

      const fakeCache = buildFakeCache({});
      vi.mocked(buildLineageCache).mockResolvedValue(fakeCache as never);
      vi.mocked(searchWorkspaceContent).mockReturnValue({
        query: "test",
        totalResults: 0,
        results: [],
      });

      const handler = extractHandler<{
        workspaceID: string;
        query: string;
        fields?: string[];
        nodeType?: string;
        limit?: number;
      }>(spy, "search_workspace_content");
      await handler({
        workspaceID: "ws-1",
        query: "test",
        fields: ["name", "sql"],
        nodeType: "Stage",
        limit: 10,
      });

      expect(searchWorkspaceContent).toHaveBeenCalledWith(fakeCache, {
        query: "test",
        fields: ["name", "sql"],
        nodeType: "Stage",
        limit: 10,
      });
    });
  });
});
