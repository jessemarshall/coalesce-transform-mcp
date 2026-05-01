import { describe, it, expect } from "vitest";
import {
  walkUpstream,
  walkDownstream,
  walkColumnLineage,
  analyzeNodeImpact,
} from "../../src/services/lineage/lineage-traversal.js";
import {
  columnKey,
  type LineageCacheEntry,
  type LineageNode,
} from "../../src/services/lineage/lineage-cache.js";

function makeNode(
  id: string,
  name: string,
  nodeType: string,
  columns: Array<{ id: string; name: string }> = []
): LineageNode {
  return {
    id,
    name,
    nodeType,
    columns: columns.map((c) => ({ id: c.id, name: c.name, sourceColumnRefs: [] })),
    raw: {},
  };
}

/**
 * Build a minimal LineageCacheEntry. Caller passes a graph as
 * `{ A: ['B', 'C'] }` meaning A → B and A → C downstream. The helper inverts
 * for upstreamNodes automatically.
 */
function buildCache(args: {
  nodes: Record<string, { name: string; nodeType: string; columns?: Array<{ id: string; name: string }> }>;
  edges: Record<string, string[]>; // upstream -> downstream
  columnEdges?: Record<string, string[]>; // upstream column key -> downstream column keys
}): LineageCacheEntry {
  const nodes = new Map<string, LineageNode>();
  for (const [id, meta] of Object.entries(args.nodes)) {
    nodes.set(id, makeNode(id, meta.name, meta.nodeType, meta.columns));
  }

  const downstreamNodes = new Map<string, Set<string>>();
  const upstreamNodes = new Map<string, Set<string>>();
  for (const [up, downs] of Object.entries(args.edges)) {
    if (!downstreamNodes.has(up)) downstreamNodes.set(up, new Set());
    for (const down of downs) {
      downstreamNodes.get(up)!.add(down);
      if (!upstreamNodes.has(down)) upstreamNodes.set(down, new Set());
      upstreamNodes.get(down)!.add(up);
    }
  }

  const columnUpstream = new Map<string, Set<string>>();
  const columnDownstream = new Map<string, Set<string>>();
  for (const [upKey, downKeys] of Object.entries(args.columnEdges ?? {})) {
    if (!columnDownstream.has(upKey)) columnDownstream.set(upKey, new Set());
    for (const downKey of downKeys) {
      columnDownstream.get(upKey)!.add(downKey);
      if (!columnUpstream.has(downKey)) columnUpstream.set(downKey, new Set());
      columnUpstream.get(downKey)!.add(upKey);
    }
  }

  return {
    workspaceID: "w1",
    nodes,
    upstreamNodes,
    downstreamNodes,
    columnUpstream,
    columnDownstream,
    cachedAt: Date.now(),
    ttlMs: 30 * 60 * 1000,
  };
}

describe("walkUpstream", () => {
  it("returns empty when the start node has no upstream", () => {
    const cache = buildCache({
      nodes: { A: { name: "A", nodeType: "src" } },
      edges: {},
    });
    expect(walkUpstream(cache, "A")).toEqual([]);
  });

  it("walks a linear chain A → B → C from C upstream", () => {
    const cache = buildCache({
      nodes: {
        A: { name: "A", nodeType: "src" },
        B: { name: "B", nodeType: "stg" },
        C: { name: "C", nodeType: "fact" },
      },
      edges: { A: ["B"], B: ["C"] },
    });
    const result = walkUpstream(cache, "C");
    expect(result.map((n) => n.nodeName)).toEqual(["B", "A"]);
    expect(result.map((n) => n.depth)).toEqual([1, 2]);
  });

  it("does not include the start node in the result", () => {
    const cache = buildCache({
      nodes: {
        A: { name: "A", nodeType: "src" },
        B: { name: "B", nodeType: "stg" },
      },
      edges: { A: ["B"] },
    });
    const result = walkUpstream(cache, "B");
    expect(result.map((n) => n.nodeID)).not.toContain("B");
  });

  it("handles a diamond DAG without revisiting", () => {
    // A → B, A → C, B → D, C → D
    const cache = buildCache({
      nodes: {
        A: { name: "A", nodeType: "src" },
        B: { name: "B", nodeType: "stg" },
        C: { name: "C", nodeType: "stg" },
        D: { name: "D", nodeType: "fact" },
      },
      edges: { A: ["B", "C"], B: ["D"], C: ["D"] },
    });
    const upstream = walkUpstream(cache, "D");
    const ids = upstream.map((n) => n.nodeID).sort();
    expect(ids).toEqual(["A", "B", "C"]);
    // A appears once even though there are two paths to it.
    expect(upstream.filter((n) => n.nodeID === "A")).toHaveLength(1);
  });

  it("survives a cycle without infinite-looping", () => {
    // A → B → A (pathological but the visited set must catch this)
    const cache = buildCache({
      nodes: {
        A: { name: "A", nodeType: "src" },
        B: { name: "B", nodeType: "stg" },
      },
      edges: { A: ["B"], B: ["A"] },
    });
    const result = walkUpstream(cache, "A");
    // Self should not be re-added; only B reached upstream.
    expect(result.map((n) => n.nodeID)).toEqual(["B"]);
  });

  it("skips nodes that aren't in the cache.nodes map", () => {
    // upstream points at a phantom ID — walk should still terminate cleanly.
    const cache = buildCache({
      nodes: { B: { name: "B", nodeType: "stg" } },
      edges: { GHOST: ["B"] },
    });
    const result = walkUpstream(cache, "B");
    // GHOST is in the upstream map but not in cache.nodes, so it's elided.
    expect(result).toEqual([]);
  });
});

describe("walkDownstream", () => {
  it("walks a linear chain A → B → C from A downstream", () => {
    const cache = buildCache({
      nodes: {
        A: { name: "A", nodeType: "src" },
        B: { name: "B", nodeType: "stg" },
        C: { name: "C", nodeType: "fact" },
      },
      edges: { A: ["B"], B: ["C"] },
    });
    const result = walkDownstream(cache, "A");
    expect(result.map((n) => n.nodeName)).toEqual(["B", "C"]);
    expect(result.map((n) => n.depth)).toEqual([1, 2]);
  });

  it("returns empty for a leaf node", () => {
    const cache = buildCache({
      nodes: { A: { name: "A", nodeType: "src" } },
      edges: {},
    });
    expect(walkDownstream(cache, "A")).toEqual([]);
  });

  it("handles a fan-out (one upstream, many downstreams)", () => {
    const cache = buildCache({
      nodes: {
        A: { name: "A", nodeType: "src" },
        B: { name: "B", nodeType: "stg" },
        C: { name: "C", nodeType: "stg" },
        D: { name: "D", nodeType: "stg" },
      },
      edges: { A: ["B", "C", "D"] },
    });
    const result = walkDownstream(cache, "A");
    expect(result.map((n) => n.nodeID).sort()).toEqual(["B", "C", "D"]);
    expect(result.every((n) => n.depth === 1)).toBe(true);
  });

  it("survives a cycle without infinite-looping", () => {
    // A → B → A (mirror of the upstream cycle test — defensive parity so a
    // future regression on one walker side doesn't go undetected)
    const cache = buildCache({
      nodes: {
        A: { name: "A", nodeType: "src" },
        B: { name: "B", nodeType: "stg" },
      },
      edges: { A: ["B"], B: ["A"] },
    });
    const result = walkDownstream(cache, "A");
    expect(result.map((n) => n.nodeID)).toEqual(["B"]);
  });
});

describe("walkColumnLineage", () => {
  it("walks both upstream and downstream and tags each entry with direction", () => {
    // src.col_x → stg.col_y → fact.col_z
    const cache = buildCache({
      nodes: {
        src: { name: "src", nodeType: "src", columns: [{ id: "col_x", name: "x" }] },
        stg: { name: "stg", nodeType: "stg", columns: [{ id: "col_y", name: "y" }] },
        fact: { name: "fact", nodeType: "fact", columns: [{ id: "col_z", name: "z" }] },
      },
      edges: { src: ["stg"], stg: ["fact"] },
      columnEdges: {
        [columnKey("src", "col_x")]: [columnKey("stg", "col_y")],
        [columnKey("stg", "col_y")]: [columnKey("fact", "col_z")],
      },
    });
    const result = walkColumnLineage(cache, "stg", "col_y");
    const upstream = result.filter((e) => e.direction === "upstream");
    const downstream = result.filter((e) => e.direction === "downstream");
    expect(upstream.map((e) => e.columnName)).toEqual(["x"]);
    expect(downstream.map((e) => e.columnName)).toEqual(["z"]);
  });

  it("returns empty when the start column has no edges", () => {
    const cache = buildCache({
      nodes: {
        a: { name: "a", nodeType: "src", columns: [{ id: "c1", name: "c1" }] },
      },
      edges: {},
    });
    expect(walkColumnLineage(cache, "a", "c1")).toEqual([]);
  });

  it("does not include the start column in the result", () => {
    const cache = buildCache({
      nodes: {
        a: { name: "a", nodeType: "src", columns: [{ id: "c1", name: "c1" }] },
        b: { name: "b", nodeType: "stg", columns: [{ id: "c2", name: "c2" }] },
      },
      edges: { a: ["b"] },
      columnEdges: { [columnKey("a", "c1")]: [columnKey("b", "c2")] },
    });
    const result = walkColumnLineage(cache, "a", "c1");
    expect(result.find((e) => e.columnID === "c1" && e.nodeID === "a")).toBeUndefined();
  });

  it("skips column-edge targets whose node/column are missing from the cache", () => {
    // Edge points at a phantom column — walker shouldn't crash.
    const cache = buildCache({
      nodes: {
        a: { name: "a", nodeType: "src", columns: [{ id: "c1", name: "c1" }] },
      },
      edges: {},
      columnEdges: { [columnKey("a", "c1")]: ["ghost:gone"] },
    });
    const result = walkColumnLineage(cache, "a", "c1");
    expect(result).toEqual([]);
  });

  it("emits a converged downstream column once when two upstream paths reach it", () => {
    // src.x → stgA.x → fact.merged
    // src.x → stgB.x → fact.merged   (same target)
    // The visited-key set must dedupe on `fact:merged` so the entry appears once.
    const cache = buildCache({
      nodes: {
        src: { name: "src", nodeType: "src", columns: [{ id: "x", name: "x" }] },
        stgA: { name: "stgA", nodeType: "stg", columns: [{ id: "x", name: "x" }] },
        stgB: { name: "stgB", nodeType: "stg", columns: [{ id: "x", name: "x" }] },
        fact: { name: "fact", nodeType: "fact", columns: [{ id: "merged", name: "merged" }] },
      },
      edges: { src: ["stgA", "stgB"], stgA: ["fact"], stgB: ["fact"] },
      columnEdges: {
        [columnKey("src", "x")]: [columnKey("stgA", "x"), columnKey("stgB", "x")],
        [columnKey("stgA", "x")]: [columnKey("fact", "merged")],
        [columnKey("stgB", "x")]: [columnKey("fact", "merged")],
      },
    });
    const result = walkColumnLineage(cache, "src", "x");
    const factMerged = result.filter(
      (e) => e.direction === "downstream" && e.nodeID === "fact" && e.columnID === "merged"
    );
    expect(factMerged).toHaveLength(1);
  });
});

describe("analyzeNodeImpact", () => {
  it("throws an actionable error when the source node is not in the cache", () => {
    const cache = buildCache({ nodes: {}, edges: {} });
    expect(() => analyzeNodeImpact(cache, "missing")).toThrow(
      /Node missing not found in lineage cache/
    );
  });

  it("throws when the source column does not exist on the node", () => {
    const cache = buildCache({
      nodes: {
        a: {
          name: "a",
          nodeType: "src",
          columns: [{ id: "c1", name: "alpha" }],
        },
      },
      edges: {},
    });
    expect(() => analyzeNodeImpact(cache, "a", "missing-col")).toThrow(
      /Column missing-col not found on node a \(a\). Available columns: c1 \(alpha\)/
    );
  });

  it("aggregates downstream impact for the whole node", () => {
    // a → b → c, plus a separate column edge so impactedColumns is non-empty.
    const cache = buildCache({
      nodes: {
        a: {
          name: "a",
          nodeType: "src",
          columns: [{ id: "ac1", name: "x" }],
        },
        b: {
          name: "b",
          nodeType: "stg",
          columns: [{ id: "bc1", name: "x" }],
        },
        c: {
          name: "c",
          nodeType: "fact",
          columns: [{ id: "cc1", name: "x" }],
        },
      },
      edges: { a: ["b"], b: ["c"] },
      columnEdges: {
        [columnKey("a", "ac1")]: [columnKey("b", "bc1")],
        [columnKey("b", "bc1")]: [columnKey("c", "cc1")],
      },
    });
    const result = analyzeNodeImpact(cache, "a");
    expect(result.totalImpactedNodes).toBe(2);
    expect(result.impactedNodes.map((n) => n.nodeName).sort()).toEqual(["b", "c"]);
    expect(result.totalImpactedColumns).toBeGreaterThan(0);
    expect(result.byDepth[1]).toEqual(["b"]);
    expect(result.byDepth[2]).toEqual(["c"]);
    expect(result.criticalPath).toEqual(["a", "b", "c"]);
  });

  it("scopes impact to a single column when columnID is provided", () => {
    const cache = buildCache({
      nodes: {
        a: {
          name: "a",
          nodeType: "src",
          columns: [
            { id: "c1", name: "alpha" },
            { id: "c2", name: "beta" },
          ],
        },
        b: {
          name: "b",
          nodeType: "stg",
          columns: [
            { id: "bc1", name: "alpha" },
            { id: "bc2", name: "beta" },
          ],
        },
      },
      edges: { a: ["b"] },
      columnEdges: {
        [columnKey("a", "c1")]: [columnKey("b", "bc1")],
        [columnKey("a", "c2")]: [columnKey("b", "bc2")],
      },
    });
    const scoped = analyzeNodeImpact(cache, "a", "c1");
    expect(scoped.sourceColumnID).toBe("c1");
    expect(scoped.sourceColumnName).toBe("alpha");
    expect(scoped.impactedColumns.map((c) => c.columnName)).toEqual(["alpha"]);
    expect(scoped.impactedColumns.find((c) => c.columnName === "beta")).toBeUndefined();
  });

  it("computes the longest critical path on a diamond", () => {
    // a → b1 → b2 → d (length 3)
    // a → c → d           (length 2)
    const cache = buildCache({
      nodes: {
        a: { name: "a", nodeType: "src" },
        b1: { name: "b1", nodeType: "stg" },
        b2: { name: "b2", nodeType: "stg" },
        c: { name: "c", nodeType: "stg" },
        d: { name: "d", nodeType: "fact" },
      },
      edges: { a: ["b1", "c"], b1: ["b2"], b2: ["d"], c: ["d"] },
    });
    const result = analyzeNodeImpact(cache, "a");
    expect(result.criticalPath).toEqual(["a", "b1", "b2", "d"]);
  });

  it("returns an empty critical path when the start has no downstream", () => {
    const cache = buildCache({
      nodes: { a: { name: "a", nodeType: "src" } },
      edges: {},
    });
    const result = analyzeNodeImpact(cache, "a");
    expect(result.criticalPath).toEqual(["a"]);
    expect(result.totalImpactedNodes).toBe(0);
  });

  it("deduplicates impactedColumns across multiple source columns hitting the same target", () => {
    // a.c1 → b.bc1
    // a.c2 → b.bc1   (same target)
    const cache = buildCache({
      nodes: {
        a: {
          name: "a",
          nodeType: "src",
          columns: [
            { id: "c1", name: "alpha" },
            { id: "c2", name: "beta" },
          ],
        },
        b: { name: "b", nodeType: "stg", columns: [{ id: "bc1", name: "merged" }] },
      },
      edges: { a: ["b"] },
      columnEdges: {
        [columnKey("a", "c1")]: [columnKey("b", "bc1")],
        [columnKey("a", "c2")]: [columnKey("b", "bc1")],
      },
    });
    const result = analyzeNodeImpact(cache, "a");
    const matches = result.impactedColumns.filter(
      (c) => c.nodeID === "b" && c.columnID === "bc1"
    );
    expect(matches).toHaveLength(1);
  });
});
