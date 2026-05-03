import { describe, it, expect } from "vitest";
import {
  searchWorkspaceContent,
  type LineageCacheEntry,
  type LineageNode,
} from "../../src/services/lineage/lineage-cache.js";

function buildCache(nodes: LineageNode[]): LineageCacheEntry {
  const nodeMap = new Map<string, LineageNode>();
  for (const n of nodes) nodeMap.set(n.id, n);
  return {
    workspaceID: "ws-1",
    nodes: nodeMap,
    upstreamNodes: new Map(),
    downstreamNodes: new Map(),
    columnUpstream: new Map(),
    columnDownstream: new Map(),
    cachedAt: Date.now(),
    ttlMs: 30 * 60 * 1000,
  };
}

const sampleNodes: LineageNode[] = [
  {
    id: "n1",
    name: "STG_ORDERS",
    nodeType: "Stage",
    columns: [
      { id: "c1", name: "ORDER_ID", dataType: "NUMBER", sourceColumnRefs: [] },
      { id: "c2", name: "CUSTOMER_ID", dataType: "NUMBER", sourceColumnRefs: [] },
      { id: "c3", name: "ORDER_DATE", dataType: "TIMESTAMP_NTZ", sourceColumnRefs: [] },
    ],
    raw: {
      id: "n1",
      name: "STG_ORDERS",
      nodeType: "Stage",
      description: "Staging layer for raw orders",
      metadata: {
        query: "SELECT order_id, customer_id, order_date FROM RAW.ORDERS",
        columns: [],
      },
      config: { materializationType: "table", insertStrategy: "TRUNCATE AND INSERT" },
    },
  },
  {
    id: "n2",
    name: "DIM_CUSTOMER",
    nodeType: "Dimension",
    columns: [
      { id: "c4", name: "CUSTOMER_KEY", dataType: "NUMBER", sourceColumnRefs: [] },
      { id: "c5", name: "CUSTOMER_NAME", dataType: "VARCHAR", sourceColumnRefs: [] },
    ],
    raw: {
      id: "n2",
      name: "DIM_CUSTOMER",
      nodeType: "Dimension",
      metadata: {
        description: "Customer dimension table for analytics",
        columns: [],
      },
      config: { materializationType: "view" },
    },
  },
  {
    id: "n3",
    name: "FCT_SALES",
    nodeType: "Fact",
    columns: [
      { id: "c6", name: "SALE_AMOUNT", dataType: "DECIMAL", sourceColumnRefs: [] },
      { id: "c7", name: "ORDER_ID", dataType: "NUMBER", sourceColumnRefs: [] },
    ],
    raw: {
      id: "n3",
      name: "FCT_SALES",
      nodeType: "Fact",
      metadata: {
        sqlQuery: "SELECT s.sale_amount, o.order_id FROM sales s JOIN orders o ON s.order_id = o.order_id",
        columns: [],
      },
      config: {},
    },
  },
];

describe("searchWorkspaceContent", () => {
  const cache = buildCache(sampleNodes);

  it("searches across all fields by default", () => {
    const result = searchWorkspaceContent(cache, { query: "orders" });
    expect(result.totalMatches).toBeGreaterThanOrEqual(2);
    expect(result.fields).toHaveLength(7); // all fields
    const nodeNames = result.results.map((r) => r.nodeName);
    expect(nodeNames).toContain("STG_ORDERS");
    expect(nodeNames).toContain("FCT_SALES"); // matches SQL
  });

  it("searches by node name", () => {
    const result = searchWorkspaceContent(cache, { query: "STG", fields: ["name"] });
    expect(result.totalMatches).toBe(1);
    expect(result.results[0].nodeName).toBe("STG_ORDERS");
    expect(result.results[0].matchedFields).toEqual(["name"]);
  });

  it("searches by node type", () => {
    const result = searchWorkspaceContent(cache, { query: "Dimension", fields: ["nodeType"] });
    expect(result.totalMatches).toBe(1);
    expect(result.results[0].nodeName).toBe("DIM_CUSTOMER");
  });

  it("searches SQL content", () => {
    const result = searchWorkspaceContent(cache, { query: "sale_amount", fields: ["sql"] });
    expect(result.totalMatches).toBe(1);
    expect(result.results[0].nodeName).toBe("FCT_SALES");
    expect(result.results[0].matches[0].field).toBe("sql");
  });

  it("searches column names", () => {
    const result = searchWorkspaceContent(cache, { query: "CUSTOMER", fields: ["columnName"] });
    expect(result.totalMatches).toBe(2);
    const nodeNames = result.results.map((r) => r.nodeName);
    expect(nodeNames).toContain("STG_ORDERS"); // CUSTOMER_ID
    expect(nodeNames).toContain("DIM_CUSTOMER"); // CUSTOMER_KEY, CUSTOMER_NAME
  });

  it("searches column data types", () => {
    const result = searchWorkspaceContent(cache, { query: "TIMESTAMP", fields: ["columnDataType"] });
    expect(result.totalMatches).toBe(1);
    expect(result.results[0].nodeName).toBe("STG_ORDERS");
    expect(result.results[0].matches[0].columnName).toBe("ORDER_DATE");
  });

  it("searches descriptions", () => {
    const result = searchWorkspaceContent(cache, { query: "analytics", fields: ["description"] });
    expect(result.totalMatches).toBe(1);
    expect(result.results[0].nodeName).toBe("DIM_CUSTOMER");
  });

  it("searches config values", () => {
    const result = searchWorkspaceContent(cache, { query: "TRUNCATE", fields: ["config"] });
    expect(result.totalMatches).toBe(1);
    expect(result.results[0].nodeName).toBe("STG_ORDERS");
  });

  it("is case-insensitive", () => {
    const result = searchWorkspaceContent(cache, { query: "stg_orders", fields: ["name"] });
    expect(result.totalMatches).toBe(1);
  });

  it("filters by nodeType", () => {
    const result = searchWorkspaceContent(cache, { query: "ORDER", nodeType: "Fact" });
    expect(result.totalMatches).toBe(1);
    expect(result.results[0].nodeName).toBe("FCT_SALES");
  });

  it("respects limit", () => {
    const result = searchWorkspaceContent(cache, { query: "o", limit: 1 });
    expect(result.returnedCount).toBe(1);
    expect(result.results).toHaveLength(1);
    expect(result.truncated).toBe(true);
    expect(result.totalMatches).toBeGreaterThan(1);
  });

  it("throws on empty query", () => {
    expect(() => searchWorkspaceContent(cache, { query: "" })).toThrow("must not be empty");
  });

  it("sets truncated false when all results fit", () => {
    const result = searchWorkspaceContent(cache, { query: "STG", fields: ["name"] });
    expect(result.truncated).toBe(false);
    expect(result.totalMatches).toBe(result.returnedCount);
  });

  it("returns empty results for no matches", () => {
    const result = searchWorkspaceContent(cache, { query: "ZZZZNOTFOUND" });
    expect(result.totalMatches).toBe(0);
    expect(result.results).toEqual([]);
  });

  it("includes cacheAge string", () => {
    const result = searchWorkspaceContent(cache, { query: "orders" });
    expect(result.cacheAge).toBeDefined();
    expect(typeof result.cacheAge).toBe("string");
  });

  it("handles multiple column matches per node", () => {
    const result = searchWorkspaceContent(cache, { query: "CUSTOMER", fields: ["columnName"] });
    const dimResult = result.results.find((r) => r.nodeName === "DIM_CUSTOMER");
    expect(dimResult).toBeDefined();
    // DIM_CUSTOMER has CUSTOMER_KEY and CUSTOMER_NAME
    expect(dimResult!.matches.length).toBe(2);
  });

  it("extracts SQL from sourceMapping.query fallback", () => {
    const nodeWithSourceMapping: LineageNode = {
      id: "n4",
      name: "NODE_WITH_SOURCE_MAPPING",
      nodeType: "Stage",
      columns: [],
      raw: {
        id: "n4",
        name: "NODE_WITH_SOURCE_MAPPING",
        nodeType: "Stage",
        metadata: {
          sourceMapping: [{ query: "SELECT * FROM fallback_table", aliases: {} }],
          columns: [],
        },
        config: {},
      },
    };
    const testCache = buildCache([nodeWithSourceMapping]);
    const result = searchWorkspaceContent(testCache, { query: "fallback_table", fields: ["sql"] });
    expect(result.totalMatches).toBe(1);
  });

  // The columnName and columnDataType loops in lineage-search.ts share
  // the same MAX_MATCHES_PER_NODE break logic. Cover both with one it.each so
  // a regression in either branch is caught — outer `limit` slices node count,
  // not match count, so a single wide node would otherwise bloat the response.
  it.each([
    {
      field: "columnName" as const,
      query: "CUSTOMER",
      makeColumns: (n: number) =>
        Array.from({ length: n }, (_, i) => ({
          id: `col-${i}`,
          name: `CUSTOMER_FIELD_${i}`,
          dataType: "VARCHAR",
          sourceColumnRefs: [] as string[],
        })),
    },
    {
      field: "columnDataType" as const,
      query: "VARCHAR",
      makeColumns: (n: number) =>
        Array.from({ length: n }, (_, i) => ({
          id: `col-${i}`,
          name: `FIELD_${i}`,
          dataType: "VARCHAR(255)",
          sourceColumnRefs: [] as string[],
        })),
    },
  ])("caps the per-node match list and flags truncatedMatches ($field)", ({ field, query, makeColumns }) => {
    const wideNode: LineageNode = {
      id: "wide",
      name: "WIDE_TABLE",
      nodeType: "Stage",
      columns: makeColumns(200),
      raw: { id: "wide", name: "WIDE_TABLE", nodeType: "Stage", metadata: {}, config: {} },
    };
    const testCache = buildCache([wideNode]);
    const result = searchWorkspaceContent(testCache, { query, fields: [field] });
    expect(result.totalMatches).toBe(1);
    const wide = result.results[0];
    expect(wide.matches.length).toBeLessThanOrEqual(50);
    expect(wide.truncatedMatches).toBe(true);
    expect(wide.matches.every((m) => m.field === field)).toBe(true);
  });

  it("does not flag truncatedMatches when matches fit under the cap", () => {
    const result = searchWorkspaceContent(cache, { query: "CUSTOMER", fields: ["columnName"] });
    for (const r of result.results) {
      expect(r.truncatedMatches).toBeUndefined();
    }
  });
});
