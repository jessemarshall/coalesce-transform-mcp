import { describe, it, expect } from "vitest";
import { auditDocumentationCoverage } from "../../src/services/lineage/lineage-documentation.js";
import type { LineageCacheEntry, LineageNode } from "../../src/services/lineage/lineage-cache.js";

function buildCache(nodes: LineageNode[], workspaceID = "ws-1"): LineageCacheEntry {
  const map = new Map<string, LineageNode>();
  for (const n of nodes) map.set(n.id, n);
  return {
    workspaceID,
    nodes: map,
    upstreamNodes: new Map(),
    downstreamNodes: new Map(),
    columnUpstream: new Map(),
    columnDownstream: new Map(),
    cachedAt: Date.now(),
    ttlMs: 1800000,
  };
}

function makeNode(overrides: Partial<LineageNode> & { id: string; raw: Record<string, unknown> }): LineageNode {
  return {
    name: overrides.name ?? overrides.id,
    nodeType: overrides.nodeType ?? "Stage",
    columns: overrides.columns ?? [],
    ...overrides,
  };
}

describe("auditDocumentationCoverage", () => {
  it("reports 100% when all nodes and columns are documented", () => {
    const cache = buildCache([
      makeNode({
        id: "n1",
        name: "STG_ORDERS",
        raw: { description: "Orders staging" },
        columns: [{ id: "c1", name: "order_id", sourceColumnRefs: [] }],
      }),
    ]);
    // Also set raw metadata columns with descriptions
    (cache.nodes.get("n1")!.raw as Record<string, unknown>).metadata = {
      columns: [{ id: "c1", description: "The order identifier" }],
    };

    const result = auditDocumentationCoverage(cache);

    expect(result.totalNodes).toBe(1);
    expect(result.documentedNodes).toBe(1);
    expect(result.undocumentedNodes).toBe(0);
    expect(result.nodeDocumentationPercent).toBe(100);
    expect(result.totalColumns).toBe(1);
    expect(result.documentedColumns).toBe(1);
    expect(result.columnDocumentationPercent).toBe(100);
    expect(result.undocumentedNodeList).toHaveLength(0);
    expect(result.undocumentedColumnList).toHaveLength(0);
  });

  it("reports 0% when nothing is documented", () => {
    const cache = buildCache([
      makeNode({
        id: "n1",
        name: "STG_ORDERS",
        raw: {},
        columns: [{ id: "c1", name: "order_id", sourceColumnRefs: [] }],
      }),
    ]);

    const result = auditDocumentationCoverage(cache);

    expect(result.documentedNodes).toBe(0);
    expect(result.undocumentedNodes).toBe(1);
    expect(result.nodeDocumentationPercent).toBe(0);
    expect(result.documentedColumns).toBe(0);
    expect(result.undocumentedColumnList).toHaveLength(1);
    expect(result.undocumentedColumnList[0]).toEqual({
      nodeID: "n1",
      nodeName: "STG_ORDERS",
      columnID: "c1",
      columnName: "order_id",
    });
  });

  it("handles empty workspace", () => {
    const cache = buildCache([]);

    const result = auditDocumentationCoverage(cache);

    expect(result.totalNodes).toBe(0);
    expect(result.totalColumns).toBe(0);
    expect(result.nodeDocumentationPercent).toBe(0);
    expect(result.columnDocumentationPercent).toBe(0);
  });

  it("detects description in metadata.description (not just raw.description)", () => {
    const cache = buildCache([
      makeNode({
        id: "n1",
        name: "STG_ORDERS",
        raw: {
          // No raw.description, but metadata.description exists
          metadata: { description: "Orders from metadata" },
        },
        columns: [],
      }),
    ]);

    const result = auditDocumentationCoverage(cache);

    expect(result.documentedNodes).toBe(1);
    expect(result.undocumentedNodes).toBe(0);
    expect(result.nodeDocumentationPercent).toBe(100);
  });

  it("treats whitespace-only descriptions as undocumented", () => {
    const cache = buildCache([
      makeNode({
        id: "n1",
        name: "STG_ORDERS",
        raw: { description: "   " },
        columns: [{ id: "c1", name: "col_a", sourceColumnRefs: [] }],
      }),
    ]);
    (cache.nodes.get("n1")!.raw as Record<string, unknown>).metadata = {
      columns: [{ id: "c1", description: "  \t  " }],
    };

    const result = auditDocumentationCoverage(cache);

    expect(result.documentedNodes).toBe(0);
    expect(result.documentedColumns).toBe(0);
  });

  it("truncates undocumented column list at 200 entries", () => {
    const columns = Array.from({ length: 250 }, (_, i) => ({
      id: `c${i}`,
      name: `col_${i}`,
      sourceColumnRefs: [] as Array<{ nodeID: string; columnID: string }>,
    }));
    const cache = buildCache([
      makeNode({
        id: "n1",
        name: "WIDE_TABLE",
        raw: { description: "documented node" },
        columns,
      }),
    ]);

    const result = auditDocumentationCoverage(cache);

    expect(result.totalColumns).toBe(250);
    expect(result.undocumentedColumns).toBe(250);
    expect(result.undocumentedColumnList).toHaveLength(200);
    expect(result.truncatedColumns).toBe(true);
  });

  it("does not set truncatedColumns when under the limit", () => {
    const cache = buildCache([
      makeNode({
        id: "n1",
        name: "SMALL_TABLE",
        raw: {},
        columns: [{ id: "c1", name: "col_a", sourceColumnRefs: [] }],
      }),
    ]);

    const result = auditDocumentationCoverage(cache);
    expect(result.truncatedColumns).toBe(false);
  });

  it("handles nodes without columns", () => {
    const cache = buildCache([
      makeNode({
        id: "n1",
        name: "NO_COLS",
        raw: { description: "documented" },
        columns: [],
      }),
    ]);

    const result = auditDocumentationCoverage(cache);

    expect(result.totalNodes).toBe(1);
    expect(result.documentedNodes).toBe(1);
    expect(result.totalColumns).toBe(0);
  });

  it("matches columns by columnID or id", () => {
    const cache = buildCache([
      makeNode({
        id: "n1",
        name: "STG",
        raw: {
          description: "node desc",
          metadata: {
            columns: [
              { columnID: "c1", description: "by columnID" },
              { id: "c2", description: "by id" },
            ],
          },
        },
        columns: [
          { id: "c1", name: "col_a", sourceColumnRefs: [] },
          { id: "c2", name: "col_b", sourceColumnRefs: [] },
        ],
      }),
    ]);

    const result = auditDocumentationCoverage(cache);

    expect(result.documentedColumns).toBe(2);
    expect(result.undocumentedColumnList).toHaveLength(0);
  });

  it("rounds percentages to two decimal places", () => {
    // 1 out of 3 = 33.33%
    const cache = buildCache([
      makeNode({ id: "n1", name: "A", raw: { description: "yes" }, columns: [] }),
      makeNode({ id: "n2", name: "B", raw: {}, columns: [] }),
      makeNode({ id: "n3", name: "C", raw: {}, columns: [] }),
    ]);

    const result = auditDocumentationCoverage(cache);

    expect(result.nodeDocumentationPercent).toBe(33.33);
  });

  it("includes workspaceID and auditedAt in result", () => {
    const cache = buildCache([], "ws-test-123");

    const result = auditDocumentationCoverage(cache);

    expect(result.workspaceID).toBe("ws-test-123");
    expect(result.auditedAt).toBeDefined();
    // Should be a valid ISO date string
    expect(() => new Date(result.auditedAt)).not.toThrow();
  });
});
