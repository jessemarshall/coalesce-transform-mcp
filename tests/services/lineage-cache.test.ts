import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  buildLineageCache,
  walkUpstream,
  walkDownstream,
  walkColumnLineage,
  analyzeNodeImpact,
  propagateColumnChange,
  invalidateLineageCache,
  parseColumnKey,
  type LineageCacheEntry,
  type PropagationChange,
} from "../../src/services/lineage/lineage-cache.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

// --- Test data: a realistic 5-node pipeline ---
// SRC_RAW → STG_ORDERS → INT_ORDERS → DIM_ORDERS → RPT_SALES

function makeNode(
  id: string,
  name: string,
  nodeType: string,
  columns: Array<{
    id: string;
    name: string;
    dataType?: string;
    sources?: Array<{
      columnReferences: Array<{ sourceNodeID: string; sourceColumnID: string }>;
    }>;
  }>,
  sourceMapping: Array<{ dependencies: string[] }> = []
) {
  return {
    id,
    name,
    nodeType,
    metadata: {
      columns: columns.map((c) => ({
        id: c.id,
        name: c.name,
        ...(c.dataType ? { dataType: c.dataType } : {}),
        sources: c.sources ?? [],
      })),
      sourceMapping,
    },
  };
}

const SRC_RAW = makeNode("n1", "SRC_RAW", "Source", [
  { id: "c1", name: "order_id", dataType: "NUMBER" },
  { id: "c2", name: "customer_name", dataType: "VARCHAR" },
]);

const STG_ORDERS = makeNode(
  "n2",
  "STG_ORDERS",
  "Stage",
  [
    {
      id: "c3",
      name: "order_id",
      dataType: "NUMBER",
      sources: [{ columnReferences: [{ sourceNodeID: "n1", sourceColumnID: "c1" }] }],
    },
    {
      id: "c4",
      name: "customer_name",
      dataType: "VARCHAR",
      sources: [{ columnReferences: [{ sourceNodeID: "n1", sourceColumnID: "c2" }] }],
    },
  ],
  [{ dependencies: ["n1"] }]
);

const INT_ORDERS = makeNode(
  "n3",
  "INT_ORDERS",
  "Intermediate",
  [
    {
      id: "c5",
      name: "order_id",
      dataType: "NUMBER",
      sources: [{ columnReferences: [{ sourceNodeID: "n2", sourceColumnID: "c3" }] }],
    },
    {
      id: "c6",
      name: "customer_name",
      dataType: "VARCHAR",
      sources: [{ columnReferences: [{ sourceNodeID: "n2", sourceColumnID: "c4" }] }],
    },
  ],
  [{ dependencies: ["n2"] }]
);

const DIM_ORDERS = makeNode(
  "n4",
  "DIM_ORDERS",
  "Dimension",
  [
    {
      id: "c7",
      name: "order_id",
      dataType: "NUMBER",
      sources: [{ columnReferences: [{ sourceNodeID: "n3", sourceColumnID: "c5" }] }],
    },
    {
      id: "c8",
      name: "customer_name",
      dataType: "VARCHAR",
      sources: [{ columnReferences: [{ sourceNodeID: "n3", sourceColumnID: "c6" }] }],
    },
  ],
  [{ dependencies: ["n3"] }]
);

const RPT_SALES = makeNode(
  "n5",
  "RPT_SALES",
  "Report",
  [
    {
      id: "c9",
      name: "order_id",
      dataType: "NUMBER",
      sources: [{ columnReferences: [{ sourceNodeID: "n4", sourceColumnID: "c7" }] }],
    },
  ],
  [{ dependencies: ["n4"] }]
);

const ALL_NODES = [SRC_RAW, STG_ORDERS, INT_ORDERS, DIM_ORDERS, RPT_SALES];

function mockPaginatedNodes(client: ReturnType<typeof createMockClient>, nodes: unknown[]) {
  client.get.mockImplementation((_path: string, params?: Record<string, unknown>) => {
    if (!params?.startingFrom) {
      return Promise.resolve({ data: nodes });
    }
    return Promise.resolve({ data: [] });
  });
}

describe("lineage-cache", () => {
  const tempDirs: string[] = [];

  function createTempDir(): string {
    const directory = mkdtempSync(join(tmpdir(), "coalesce-lineage-test-"));
    tempDirs.push(directory);
    return directory;
  }

  beforeEach(() => {
    invalidateLineageCache();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    invalidateLineageCache();
    for (const directory of tempDirs.splice(0, tempDirs.length)) {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  describe("parseColumnKey", () => {
    it("splits nodeID:columnID correctly", () => {
      expect(parseColumnKey("n1:c1")).toEqual({ nodeID: "n1", columnID: "c1" });
    });

    it("handles colons in columnID", () => {
      expect(parseColumnKey("n1:col:with:colons")).toEqual({
        nodeID: "n1",
        columnID: "col:with:colons",
      });
    });
  });

  describe("buildLineageCache", () => {
    it("fetches all nodes and builds indexes", async () => {
      const client = createMockClient();
      mockPaginatedNodes(client, ALL_NODES);

      const cache = await buildLineageCache(client, "ws-1", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });

      expect(cache.nodes.size).toBe(5);
      expect(cache.workspaceID).toBe("ws-1");

      // Upstream checks
      expect(cache.upstreamNodes.get("n1")?.size).toBe(0);
      expect(cache.upstreamNodes.get("n2")?.has("n1")).toBe(true);
      expect(cache.upstreamNodes.get("n3")?.has("n2")).toBe(true);
      expect(cache.upstreamNodes.get("n5")?.has("n4")).toBe(true);

      // Downstream checks
      expect(cache.downstreamNodes.get("n1")?.has("n2")).toBe(true);
      expect(cache.downstreamNodes.get("n4")?.has("n5")).toBe(true);
      expect(cache.downstreamNodes.get("n5")?.size).toBe(0);
    });

    it("builds column-level indexes", async () => {
      const client = createMockClient();
      mockPaginatedNodes(client, ALL_NODES);

      const cache = await buildLineageCache(client, "ws-1", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });

      // c3 on STG_ORDERS traces upstream to c1 on SRC_RAW
      expect(cache.columnUpstream.get("n2:c3")?.has("n1:c1")).toBe(true);

      // c1 on SRC_RAW traces downstream to c3 on STG_ORDERS
      expect(cache.columnDownstream.get("n1:c1")?.has("n2:c3")).toBe(true);

      // Full chain: c1 → c3 → c5 → c7 → c9
      expect(cache.columnDownstream.get("n1:c1")?.has("n2:c3")).toBe(true);
      expect(cache.columnDownstream.get("n2:c3")?.has("n3:c5")).toBe(true);
      expect(cache.columnDownstream.get("n3:c5")?.has("n4:c7")).toBe(true);
      expect(cache.columnDownstream.get("n4:c7")?.has("n5:c9")).toBe(true);
    });

    it("uses in-memory cache on repeated calls", async () => {
      const client = createMockClient();
      mockPaginatedNodes(client, ALL_NODES);
      const baseDir = createTempDir();

      await buildLineageCache(client, "ws-1", { baseDir, forceRefresh: true });
      const callCount = client.get.mock.calls.length;

      // Second call should use cache
      await buildLineageCache(client, "ws-1", { baseDir });
      expect(client.get.mock.calls.length).toBe(callCount);
    });

    it("reuses existing snapshot from disk", async () => {
      const client = createMockClient();
      const baseDir = createTempDir();

      // Write a snapshot to disk
      const nodesDir = join(baseDir, "coalesce_transform_mcp_data_cache", "nodes");
      mkdirSync(nodesDir, { recursive: true });

      const ndjson = ALL_NODES.map((n) => JSON.stringify(n)).join("\n") + "\n";
      writeFileSync(join(nodesDir, "workspace-ws-snap-nodes.ndjson"), ndjson);
      writeFileSync(
        join(nodesDir, "workspace-ws-snap-nodes.meta.json"),
        JSON.stringify({ cachedAt: new Date().toISOString(), totalItems: 5 })
      );

      const cache = await buildLineageCache(client, "ws-snap", { baseDir });

      // Should NOT have called the API
      expect(client.get).not.toHaveBeenCalled();
      expect(cache.nodes.size).toBe(5);
    });

    it("handles paginated responses across multiple pages", async () => {
      const client = createMockClient();

      client.get.mockImplementation((_path: string, params?: Record<string, unknown>) => {
        if (!params?.startingFrom) {
          return Promise.resolve({
            data: [SRC_RAW, STG_ORDERS],
            next: "cursor-2",
          });
        }
        if (params.startingFrom === "cursor-2") {
          return Promise.resolve({
            data: [INT_ORDERS, DIM_ORDERS],
            next: "cursor-3",
          });
        }
        if (params.startingFrom === "cursor-3") {
          return Promise.resolve({
            data: [RPT_SALES],
          });
        }
        throw new Error("Unexpected cursor");
      });

      const cache = await buildLineageCache(client, "ws-paged", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });

      expect(cache.nodes.size).toBe(5);
      expect(client.get).toHaveBeenCalledTimes(3);
    });

    it("fires progress notifications", async () => {
      const client = createMockClient();
      mockPaginatedNodes(client, ALL_NODES);

      const messages: string[] = [];
      const reportProgress = async (msg: string) => {
        messages.push(msg);
      };

      await buildLineageCache(client, "ws-1", {
        baseDir: createTempDir(),
        reportProgress,
        forceRefresh: true,
      });

      expect(messages.length).toBeGreaterThan(0);
      expect(messages.some((m) => m.includes("Fetching all workspace nodes"))).toBe(true);
      expect(messages.some((m) => m.includes("Building lineage indexes"))).toBe(true);
    });
  });

  describe("walkUpstream", () => {
    let cache: LineageCacheEntry;

    beforeEach(async () => {
      const client = createMockClient();
      mockPaginatedNodes(client, ALL_NODES);
      cache = await buildLineageCache(client, "ws-walk", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });
    });

    it("returns empty for source node", () => {
      const result = walkUpstream(cache, "n1");
      expect(result).toHaveLength(0);
    });

    it("returns full upstream chain for leaf node", () => {
      const result = walkUpstream(cache, "n5");
      expect(result).toHaveLength(4);
      expect(result.map((r) => r.nodeID).sort()).toEqual(["n1", "n2", "n3", "n4"]);
    });

    it("returns correct depth levels", () => {
      const result = walkUpstream(cache, "n5");
      const byID = Object.fromEntries(result.map((r) => [r.nodeID, r.depth]));
      expect(byID["n4"]).toBe(1);
      expect(byID["n3"]).toBe(2);
      expect(byID["n2"]).toBe(3);
      expect(byID["n1"]).toBe(4);
    });

    it("returns partial chain from middle node", () => {
      const result = walkUpstream(cache, "n3");
      expect(result).toHaveLength(2);
      expect(result.map((r) => r.nodeID).sort()).toEqual(["n1", "n2"]);
    });
  });

  describe("walkDownstream", () => {
    let cache: LineageCacheEntry;

    beforeEach(async () => {
      const client = createMockClient();
      mockPaginatedNodes(client, ALL_NODES);
      cache = await buildLineageCache(client, "ws-down", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });
    });

    it("returns empty for leaf node", () => {
      const result = walkDownstream(cache, "n5");
      expect(result).toHaveLength(0);
    });

    it("returns full downstream chain for source node", () => {
      const result = walkDownstream(cache, "n1");
      expect(result).toHaveLength(4);
      expect(result.map((r) => r.nodeID).sort()).toEqual(["n2", "n3", "n4", "n5"]);
    });

    it("returns correct depth levels", () => {
      const result = walkDownstream(cache, "n1");
      const byID = Object.fromEntries(result.map((r) => [r.nodeID, r.depth]));
      expect(byID["n2"]).toBe(1);
      expect(byID["n3"]).toBe(2);
      expect(byID["n4"]).toBe(3);
      expect(byID["n5"]).toBe(4);
    });
  });

  describe("walkColumnLineage", () => {
    let cache: LineageCacheEntry;

    beforeEach(async () => {
      const client = createMockClient();
      mockPaginatedNodes(client, ALL_NODES);
      cache = await buildLineageCache(client, "ws-col", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });
    });

    it("traces order_id column through full pipeline", () => {
      // Start from SRC_RAW.order_id (c1)
      const lineage = walkColumnLineage(cache, "n1", "c1");

      const upstream = lineage.filter((e) => e.direction === "upstream");
      const downstream = lineage.filter((e) => e.direction === "downstream");

      expect(upstream).toHaveLength(0); // source has no upstream
      expect(downstream).toHaveLength(4); // c3 → c5 → c7 → c9
      expect(downstream.map((d) => d.columnID)).toEqual(["c3", "c5", "c7", "c9"]);
    });

    it("traces column from middle of pipeline", () => {
      // Start from INT_ORDERS.order_id (c5)
      const lineage = walkColumnLineage(cache, "n3", "c5");

      const upstream = lineage.filter((e) => e.direction === "upstream");
      const downstream = lineage.filter((e) => e.direction === "downstream");

      expect(upstream).toHaveLength(2); // c3, c1
      expect(downstream).toHaveLength(2); // c7, c9
    });

    it("traces customer_name which stops at DIM_ORDERS (not in RPT_SALES)", () => {
      const lineage = walkColumnLineage(cache, "n1", "c2");

      const downstream = lineage.filter((e) => e.direction === "downstream");
      expect(downstream).toHaveLength(3); // c4, c6, c8 (RPT_SALES only has order_id)
      expect(downstream.map((d) => d.columnName)).toEqual([
        "customer_name",
        "customer_name",
        "customer_name",
      ]);
    });
  });

  describe("analyzeNodeImpact", () => {
    let cache: LineageCacheEntry;

    beforeEach(async () => {
      const client = createMockClient();
      mockPaginatedNodes(client, ALL_NODES);
      cache = await buildLineageCache(client, "ws-impact", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });
    });

    it("analyzes full node impact", () => {
      const result = analyzeNodeImpact(cache, "n2");

      expect(result.sourceNodeID).toBe("n2");
      expect(result.sourceNodeName).toBe("STG_ORDERS");
      expect(result.totalImpactedNodes).toBe(3); // n3, n4, n5
      expect(result.totalImpactedColumns).toBeGreaterThan(0);
      expect(result.criticalPath.length).toBeGreaterThanOrEqual(4);
    });

    it("analyzes column-specific impact", () => {
      const result = analyzeNodeImpact(cache, "n1", "c2");

      expect(result.sourceColumnID).toBe("c2");
      expect(result.sourceColumnName).toBe("customer_name");
      // customer_name doesn't reach RPT_SALES
      expect(result.impactedColumns.every((c) => c.columnName === "customer_name")).toBe(true);
      expect(result.impactedColumns).toHaveLength(3);
    });

    it("groups impacted nodes by depth", () => {
      const result = analyzeNodeImpact(cache, "n1");

      expect(result.byDepth[1]).toContain("STG_ORDERS");
      expect(result.byDepth[2]).toContain("INT_ORDERS");
      expect(result.byDepth[3]).toContain("DIM_ORDERS");
      expect(result.byDepth[4]).toContain("RPT_SALES");
    });

    it("returns critical path", () => {
      const result = analyzeNodeImpact(cache, "n1");
      expect(result.criticalPath).toEqual([
        "SRC_RAW",
        "STG_ORDERS",
        "INT_ORDERS",
        "DIM_ORDERS",
        "RPT_SALES",
      ]);
    });

    it("throws for unknown node", () => {
      expect(() => analyzeNodeImpact(cache, "nonexistent")).toThrow(
        "not found in lineage cache"
      );
    });

    it("throws for unknown columnID", () => {
      expect(() => analyzeNodeImpact(cache, "n1", "nonexistent")).toThrow(
        "Column nonexistent not found on node n1"
      );
    });

    it("leaf node has zero impact", () => {
      const result = analyzeNodeImpact(cache, "n5");
      expect(result.totalImpactedNodes).toBe(0);
      expect(result.totalImpactedColumns).toBe(0);
    });
  });

  describe("propagateColumnChange", () => {
    let cache: LineageCacheEntry;
    let client: ReturnType<typeof createMockClient>;

    beforeEach(async () => {
      client = createMockClient();
      mockPaginatedNodes(client, ALL_NODES);
      cache = await buildLineageCache(client, "ws-prop", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });
    });

    it("propagates column name change to all downstream", async () => {
      // Mock GET for each downstream node (returns current state)
      const nodeMap = new Map(ALL_NODES.map((n) => [n.id, n]));
      client.get.mockImplementation((path: string) => {
        for (const [id, node] of nodeMap) {
          if (path.includes(`/nodes/${id}`)) {
            return Promise.resolve(JSON.parse(JSON.stringify(node)));
          }
        }
        return Promise.resolve({ data: [] });
      });
      client.put.mockResolvedValue({ ok: true });

      const changes: PropagationChange = { columnName: "renamed_order_id" };
      const result = await propagateColumnChange(
        client as any,
        cache,
        "ws-prop",
        "n1",
        "c1",
        changes
      );

      // order_id propagates through c3→c5→c7→c9
      expect(result.totalUpdated).toBe(4);
      expect(result.errors).toHaveLength(0);
      expect(result.updatedNodes.map((u) => u.nodeID)).toEqual(["n2", "n3", "n4", "n5"]);

      // Verify PUT was called for each downstream node
      expect(client.put).toHaveBeenCalledTimes(4);
    });

    it("throws when no changes provided", async () => {
      await expect(
        propagateColumnChange(client as any, cache, "ws-prop", "n1", "c1", {})
      ).rejects.toThrow("At least one change");
    });

    it("throws for unknown node", async () => {
      await expect(
        propagateColumnChange(client as any, cache, "ws-prop", "xxx", "c1", {
          columnName: "test",
        })
      ).rejects.toThrow("not found in lineage cache");
    });

    it("throws for unknown column", async () => {
      await expect(
        propagateColumnChange(client as any, cache, "ws-prop", "n1", "xxx", {
          columnName: "test",
        })
      ).rejects.toThrow("not found on node");
    });

    it("records errors when API call fails", async () => {
      client.get.mockRejectedValue(new Error("API down"));

      const result = await propagateColumnChange(
        client as any,
        cache,
        "ws-prop",
        "n1",
        "c1",
        { columnName: "renamed" }
      );

      expect(result.totalUpdated).toBe(0);
      expect(result.errors.length).toBeGreaterThan(0);
      expect(result.errors[0].message).toContain("API down");
    });
  });

  describe("deep chain traversal (20+ nodes)", () => {
    it("traverses a 25-node linear chain", async () => {
      const client = createMockClient();
      const nodes: unknown[] = [];

      for (let i = 0; i < 25; i++) {
        const deps = i > 0 ? [{ dependencies: [`chain-${i - 1}`] }] : [];
        const colSources =
          i > 0
            ? [{ columnReferences: [{ sourceNodeID: `chain-${i - 1}`, sourceColumnID: `col-${i - 1}` }] }]
            : [];
        nodes.push(
          makeNode(
            `chain-${i}`,
            `CHAIN_NODE_${i}`,
            "Stage",
            [{ id: `col-${i}`, name: `column_${i}`, dataType: "VARCHAR", sources: colSources }],
            deps
          )
        );
      }

      mockPaginatedNodes(client, nodes);
      const cache = await buildLineageCache(client, "ws-deep", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });

      // Upstream from last node
      const upstream = walkUpstream(cache, "chain-24");
      expect(upstream).toHaveLength(24);
      expect(upstream.find((a) => a.nodeID === "chain-0")?.depth).toBe(24);

      // Downstream from first node
      const downstream = walkDownstream(cache, "chain-0");
      expect(downstream).toHaveLength(24);
      expect(downstream.find((d) => d.nodeID === "chain-24")?.depth).toBe(24);

      // Column lineage through full chain
      const colLineage = walkColumnLineage(cache, "chain-0", "col-0");
      const downCols = colLineage.filter((e) => e.direction === "downstream");
      expect(downCols).toHaveLength(24);
    });
  });

  describe("real API format (columnID, aliases, nodeID/columnID refs)", () => {
    it("builds lineage from Coalesce API response format", async () => {
      const client = createMockClient();

      // Mirrors actual Coalesce API shape: columnID (not id), aliases map,
      // dependencies with locationName/nodeName, columnReferences with nodeID/columnID
      const srcNode = {
        id: "src-1",
        name: "ORDERS_SF1",
        nodeType: "Source",
        metadata: {
          columns: [
            { columnID: "col-src-1", name: "O_ORDERKEY", dataType: "NUMBER(38,0)", sources: [] },
            { columnID: "col-src-2", name: "O_CUSTKEY", dataType: "NUMBER(38,0)", sources: [] },
          ],
          sourceMapping: [],
        },
      };

      const stgNode = {
        id: "stg-1",
        name: "ORDERS_SF1",
        nodeType: "Stage",
        metadata: {
          columns: [
            {
              columnID: "col-stg-1",
              name: "O_ORDERKEY",
              dataType: "NUMBER(38,0)",
              sources: [{ columnReferences: [{ nodeID: "src-1", columnID: "col-src-1" }] }],
            },
            {
              columnID: "col-stg-2",
              name: "O_CUSTKEY",
              dataType: "NUMBER(38,0)",
              sources: [{ columnReferences: [{ nodeID: "src-1", columnID: "col-src-2" }] }],
            },
          ],
          sourceMapping: [
            {
              aliases: { ORDERS_SF1: "src-1" },
              dependencies: [{ locationName: "SRC_INGEST", nodeName: "ORDERS_SF1" }],
              join: { joinCondition: "FROM {{ ref('SRC_INGEST', 'ORDERS_SF1') }} \"ORDERS_SF1\"" },
              name: "ORDERS_SF1",
            },
          ],
        },
      };

      mockPaginatedNodes(client, [srcNode, stgNode]);
      const cache = await buildLineageCache(client, "ws-api-format", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });

      expect(cache.nodes.size).toBe(2);

      // Node-level lineage via aliases
      expect(cache.upstreamNodes.get("stg-1")?.has("src-1")).toBe(true);
      expect(cache.downstreamNodes.get("src-1")?.has("stg-1")).toBe(true);

      // Column-level lineage via nodeID/columnID refs
      expect(cache.columnUpstream.get("stg-1:col-stg-1")?.has("src-1:col-src-1")).toBe(true);
      expect(cache.columnUpstream.get("stg-1:col-stg-2")?.has("src-1:col-src-2")).toBe(true);
      expect(cache.columnDownstream.get("src-1:col-src-1")?.has("stg-1:col-stg-1")).toBe(true);

      // Walk upstream from stg node
      const upstream = walkUpstream(cache, "stg-1");
      expect(upstream).toHaveLength(1);
      expect(upstream[0].nodeID).toBe("src-1");
      expect(upstream[0].nodeName).toBe("ORDERS_SF1");

      // Walk downstream from src node
      const downstream = walkDownstream(cache, "src-1");
      expect(downstream).toHaveLength(1);
      expect(downstream[0].nodeID).toBe("stg-1");
    });
  });

  describe("diamond dependency pattern", () => {
    it("handles nodes with multiple upstream parents", async () => {
      const client = createMockClient();

      // Diamond: A → B, A → C, B → D, C → D
      const nodes = [
        makeNode("a", "NODE_A", "Source", [{ id: "ca", name: "id", dataType: "NUMBER" }]),
        makeNode(
          "b",
          "NODE_B",
          "Stage",
          [{ id: "cb", name: "id", dataType: "NUMBER", sources: [{ columnReferences: [{ sourceNodeID: "a", sourceColumnID: "ca" }] }] }],
          [{ dependencies: ["a"] }]
        ),
        makeNode(
          "c",
          "NODE_C",
          "Stage",
          [{ id: "cc", name: "id", dataType: "NUMBER", sources: [{ columnReferences: [{ sourceNodeID: "a", sourceColumnID: "ca" }] }] }],
          [{ dependencies: ["a"] }]
        ),
        makeNode(
          "d",
          "NODE_D",
          "Dimension",
          [
            {
              id: "cd",
              name: "id",
              dataType: "NUMBER",
              sources: [
                { columnReferences: [{ sourceNodeID: "b", sourceColumnID: "cb" }] },
                { columnReferences: [{ sourceNodeID: "c", sourceColumnID: "cc" }] },
              ],
            },
          ],
          [{ dependencies: ["b", "c"] }]
        ),
      ];

      mockPaginatedNodes(client, nodes);
      const cache = await buildLineageCache(client, "ws-diamond", {
        baseDir: createTempDir(),
        forceRefresh: true,
      });

      // D has two upstream parents
      const upFromD = walkUpstream(cache, "d");
      expect(upFromD.map((u) => u.nodeID).sort()).toEqual(["a", "b", "c"]);

      // A has three downstream (B, C, D)
      const downFromA = walkDownstream(cache, "a");
      expect(downFromA.map((d) => d.nodeID).sort()).toEqual(["b", "c", "d"]);

      // Column lineage from A.ca should reach all
      const colLineage = walkColumnLineage(cache, "a", "ca");
      const downCols = colLineage.filter((e) => e.direction === "downstream");
      expect(downCols.map((d) => d.columnID).sort()).toEqual(["cb", "cc", "cd"]);
    });
  });
});
