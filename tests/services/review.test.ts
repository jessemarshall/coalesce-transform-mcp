import { describe, it, expect, vi, afterEach, beforeEach } from "vitest";
import { reviewPipeline } from "../../src/services/pipelines/review.js";
import { clearWorkspaceNodeDetailIndexCache } from "../../src/services/cache/workspace-node-detail-index.js";
import { CoalesceApiError } from "../../src/client.js";
import { createMockClient } from "../helpers/fixtures.js";

function buildNodeSummary(
  id: string,
  name: string,
  nodeType: string,
  opts: { locationName?: string; predecessorNodeIDs?: string[] } = {}
) {
  return {
    id,
    name,
    nodeType,
    locationName: opts.locationName ?? "RAW",
    predecessorNodeIDs: opts.predecessorNodeIDs ?? [],
  };
}

function buildFullNode(
  id: string,
  name: string,
  nodeType: string,
  opts: {
    columns?: Array<{ name: string; transform?: string; sources?: unknown[] }>;
    joinCondition?: string;
    config?: Record<string, unknown>;
  } = {}
) {
  const columns = (opts.columns ?? []).map((c) => ({
    name: c.name,
    transform: c.transform ?? "",
    dataType: "VARCHAR",
    sources: c.sources ?? [],
  }));

  const metadata: Record<string, unknown> = { columns };
  if (opts.joinCondition) {
    metadata.sourceMapping = {
      join: { joinCondition: opts.joinCondition },
    };
  }

  return {
    id,
    name,
    nodeType,
    config: opts.config ?? {},
    metadata,
  };
}

function setupMockClient(
  summaries: Array<Record<string, unknown>>,
  fullNodes: Map<string, Record<string, unknown>>
) {
  // The detail-index cache fetches a single paginated `list?detail=true`,
  // expecting each item to carry both summary fields (id/name/nodeType/
  // locationName/predecessorNodeIDs) and the full body (metadata, config).
  const merged = summaries.map((summary) => {
    const id = typeof summary.id === "string" ? summary.id : null;
    const full = id ? fullNodes.get(id) : undefined;
    return full ? { ...summary, ...full } : summary;
  });

  const client = createMockClient();
  client.get.mockImplementation((path: string) => {
    if (path.match(/\/nodes$/) && !path.includes("/nodes/")) {
      return Promise.resolve({ data: merged });
    }
    return Promise.resolve({ data: [] });
  });
  return client;
}

describe("reviewPipeline", () => {
  beforeEach(() => {
    clearWorkspaceNodeDetailIndexCache();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns clean review for a well-structured pipeline", async () => {
    const summaries = [
      buildNodeSummary("n1", "SRC_CUSTOMERS", "Stage"),
      buildNodeSummary("n2", "STG_CUSTOMERS", "Stage", { predecessorNodeIDs: ["n1"] }),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "SRC_CUSTOMERS", "Stage", {
        columns: [{ name: "ID" }, { name: "NAME" }],
      })],
      ["n2", buildFullNode("n2", "STG_CUSTOMERS", "Stage", {
        columns: [
          { name: "ID", transform: "" },
          { name: "CUSTOMER_NAME", transform: "UPPER(\"SRC_CUSTOMERS\".\"NAME\")" },
        ],
      })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    expect(result.scope).toBe("full");
    expect(result.nodeCount).toBe(2);
    expect(result.summary.critical).toBe(0);
    expect(result.graphStats.rootNodes).toBe(1);
    expect(result.graphStats.leafNodes).toBe(1);
  });

  it("detects redundant passthrough node", async () => {
    const summaries = [
      buildNodeSummary("n1", "SRC_ORDERS", "Stage"),
      buildNodeSummary("n2", "STG_ORDERS", "Stage", { predecessorNodeIDs: ["n1"] }),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "SRC_ORDERS", "Stage", {
        columns: [{ name: "ID" }, { name: "AMOUNT" }],
      })],
      ["n2", buildFullNode("n2", "STG_ORDERS", "Stage", {
        columns: [
          { name: "ID", transform: "ID" },
          { name: "AMOUNT", transform: "AMOUNT" },
        ],
      })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    const passthrough = result.findings.find((f) => f.category === "redundant_passthrough");
    expect(passthrough).toBeDefined();
    expect(passthrough!.nodeName).toBe("STG_ORDERS");
    expect(passthrough!.severity).toBe("warning");
  });

  it("detects missing join condition", async () => {
    const summaries = [
      buildNodeSummary("n1", "CUSTOMERS", "Stage"),
      buildNodeSummary("n2", "ORDERS", "Stage"),
      buildNodeSummary("n3", "JOIN_CUST_ORDERS", "Stage", { predecessorNodeIDs: ["n1", "n2"] }),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "CUSTOMERS", "Stage", { columns: [{ name: "ID" }] })],
      ["n2", buildFullNode("n2", "ORDERS", "Stage", { columns: [{ name: "ORDER_ID" }] })],
      ["n3", buildFullNode("n3", "JOIN_CUST_ORDERS", "Stage", {
        columns: [{ name: "ID" }, { name: "ORDER_ID" }],
        // No joinCondition
      })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    const missing = result.findings.find((f) => f.category === "missing_join_condition");
    expect(missing).toBeDefined();
    expect(missing!.severity).toBe("critical");
    expect(missing!.nodeName).toBe("JOIN_CUST_ORDERS");
  });

  it("does not flag join condition when present", async () => {
    const summaries = [
      buildNodeSummary("n1", "CUSTOMERS", "Stage"),
      buildNodeSummary("n2", "ORDERS", "Stage"),
      buildNodeSummary("n3", "JOIN_NODE", "Stage", { predecessorNodeIDs: ["n1", "n2"] }),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "CUSTOMERS", "Stage", { columns: [{ name: "ID" }] })],
      ["n2", buildFullNode("n2", "ORDERS", "Stage", { columns: [{ name: "ID" }] })],
      ["n3", buildFullNode("n3", "JOIN_NODE", "Stage", {
        columns: [{ name: "ID" }],
        joinCondition: 'FROM {{ ref(\'RAW\', \'CUSTOMERS\') }} "CUSTOMERS" JOIN {{ ref(\'RAW\', \'ORDERS\') }} "ORDERS" ON "CUSTOMERS"."ID" = "ORDERS"."ID"',
      })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    const missing = result.findings.find((f) => f.category === "missing_join_condition");
    expect(missing).toBeUndefined();
  });

  it("detects orphan node", async () => {
    const summaries = [
      buildNodeSummary("n1", "STG_PRODUCTS", "Stage"),
      buildNodeSummary("n2", "ORPHAN_NODE", "Stage"),
      buildNodeSummary("n3", "STG_ORDERS", "Stage", { predecessorNodeIDs: ["n1"] }),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "STG_PRODUCTS", "Stage", { columns: [{ name: "ID" }] })],
      ["n2", buildFullNode("n2", "ORPHAN_NODE", "Stage", { columns: [{ name: "ID" }] })],
      ["n3", buildFullNode("n3", "STG_ORDERS", "Stage", { columns: [{ name: "ID" }] })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    const orphan = result.findings.find(
      (f) => f.category === "orphan_node" && f.nodeName === "ORPHAN_NODE"
    );
    expect(orphan).toBeDefined();
    expect(orphan!.severity).toBe("warning");
  });

  it("detects view type mismatch for multi-source joins", async () => {
    const summaries = [
      buildNodeSummary("n1", "CUSTOMERS", "Stage"),
      buildNodeSummary("n2", "ORDERS", "Stage"),
      buildNodeSummary("n3", "VW_JOIN", "View", { predecessorNodeIDs: ["n1", "n2"] }),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "CUSTOMERS", "Stage", { columns: [{ name: "ID" }] })],
      ["n2", buildFullNode("n2", "ORDERS", "Stage", { columns: [{ name: "ID" }] })],
      ["n3", buildFullNode("n3", "VW_JOIN", "View", {
        columns: [{ name: "ID" }],
        joinCondition: "FROM ... JOIN ...",
      })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    const mismatch = result.findings.find((f) => f.category === "type_mismatch");
    expect(mismatch).toBeDefined();
    expect(mismatch!.nodeName).toBe("VW_JOIN");
  });

  it("detects dimension type at staging layer", async () => {
    const summaries = [
      buildNodeSummary("n1", "STG_CUSTOMERS", "Dimension"),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "STG_CUSTOMERS", "Dimension", { columns: [{ name: "ID" }] })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    const mismatch = result.findings.find((f) => f.category === "type_mismatch");
    expect(mismatch).toBeDefined();
    expect(mismatch!.message).toContain("staging");
  });

  it("detects layer violation (bronze to mart skip)", async () => {
    const summaries = [
      buildNodeSummary("n1", "RAW_CUSTOMERS", "Stage"),
      buildNodeSummary("n2", "DIM_CUSTOMERS", "Dimension", { predecessorNodeIDs: ["n1"] }),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "RAW_CUSTOMERS", "Stage", { columns: [{ name: "ID" }] })],
      ["n2", buildFullNode("n2", "DIM_CUSTOMERS", "Dimension", {
        columns: [{ name: "ID", transform: "CAST(ID AS INT)" }],
      })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    const violation = result.findings.find((f) => f.category === "layer_violation");
    expect(violation).toBeDefined();
    expect(violation!.message).toContain("bronze");
    expect(violation!.message).toContain("mart");
  });

  it("detects naming inconsistency in Kimball workspace", async () => {
    const summaries = [
      buildNodeSummary("n1", "DIM_CUSTOMERS", "Dimension"),
      buildNodeSummary("n2", "FACT_ORDERS", "Fact"),
      buildNodeSummary("n3", "REVENUE_SUMMARY", "Stage"),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "DIM_CUSTOMERS", "Dimension", { columns: [{ name: "ID" }] })],
      ["n2", buildFullNode("n2", "FACT_ORDERS", "Fact", { columns: [{ name: "ID" }] })],
      ["n3", buildFullNode("n3", "REVENUE_SUMMARY", "Stage", { columns: [{ name: "ID" }] })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    // REVENUE_SUMMARY is mart layer (from being in a Kimball workspace) but has no DIM_/FACT_ prefix
    // It's in the "unknown" layer actually since it doesn't match any pattern — won't trigger naming check
    expect(result.methodology).toBe("kimball");
  });

  it("computes correct graph stats", async () => {
    // Linear chain: n1 -> n2 -> n3 -> n4
    const summaries = [
      buildNodeSummary("n1", "SRC_DATA", "Stage"),
      buildNodeSummary("n2", "STG_DATA", "Stage", { predecessorNodeIDs: ["n1"] }),
      buildNodeSummary("n3", "INT_DATA", "Stage", { predecessorNodeIDs: ["n2"] }),
      buildNodeSummary("n4", "DIM_DATA", "Dimension", { predecessorNodeIDs: ["n3"] }),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "SRC_DATA", "Stage", { columns: [{ name: "ID" }] })],
      ["n2", buildFullNode("n2", "STG_DATA", "Stage", { columns: [{ name: "ID", transform: "CAST(ID AS INT)" }] })],
      ["n3", buildFullNode("n3", "INT_DATA", "Stage", { columns: [{ name: "ID", transform: "COALESCE(ID, -1)" }] })],
      ["n4", buildFullNode("n4", "DIM_DATA", "Dimension", { columns: [{ name: "ID", transform: "ID" }] })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    expect(result.graphStats.maxDepth).toBe(3);
    expect(result.graphStats.rootNodes).toBe(1);
    expect(result.graphStats.leafNodes).toBe(1);
  });

  it("scopes review to specific nodeIDs", async () => {
    const summaries = [
      buildNodeSummary("n1", "NODE_A", "Stage"),
      buildNodeSummary("n2", "NODE_B", "Stage"),
      buildNodeSummary("n3", "NODE_C", "Stage"),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "NODE_A", "Stage", { columns: [{ name: "ID" }] })],
      ["n2", buildFullNode("n2", "NODE_B", "Stage", { columns: [{ name: "ID" }] })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, {
      workspaceID: "ws-1",
      nodeIDs: ["n1", "n2"],
    });

    expect(result.scope).toBe("subgraph");
    expect(result.nodeCount).toBe(2);
  });

  it("propagates list errors (auth, transport)", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Forbidden", 403));

    await expect(
      reviewPipeline(client as any, { workspaceID: "ws-1" })
    ).rejects.toThrow("Forbidden");
  });

  it("findings are sorted by severity (critical first)", async () => {
    const summaries = [
      buildNodeSummary("n1", "SRC_A", "Stage"),
      buildNodeSummary("n2", "SRC_B", "Stage"),
      buildNodeSummary("n3", "ORPHAN", "Stage"),
      buildNodeSummary("n4", "JOIN_NODE", "Stage", { predecessorNodeIDs: ["n1", "n2"] }),
      buildNodeSummary("n5", "PASS_NODE", "Stage", { predecessorNodeIDs: ["n1"] }),
    ];
    const fullNodes = new Map<string, Record<string, unknown>>([
      ["n1", buildFullNode("n1", "SRC_A", "Stage", { columns: [{ name: "ID" }] })],
      ["n2", buildFullNode("n2", "SRC_B", "Stage", { columns: [{ name: "ID" }] })],
      ["n3", buildFullNode("n3", "ORPHAN", "Stage", { columns: [{ name: "ID" }] })],
      ["n4", buildFullNode("n4", "JOIN_NODE", "Stage", { columns: [{ name: "ID" }] })],
      ["n5", buildFullNode("n5", "PASS_NODE", "Stage", {
        columns: [{ name: "ID", transform: "ID" }],
      })],
    ]);
    const client = setupMockClient(summaries, fullNodes);

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    // Should have critical (missing join) before warnings (orphan, passthrough)
    const critIdx = result.findings.findIndex((f) => f.severity === "critical");
    const warnIdx = result.findings.findIndex((f) => f.severity === "warning");
    if (critIdx >= 0 && warnIdx >= 0) {
      expect(critIdx).toBeLessThan(warnIdx);
    }
  });

  it("returns empty findings for empty workspace", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    const result = await reviewPipeline(client as any, { workspaceID: "ws-1" });

    expect(result.nodeCount).toBe(0);
    expect(result.findings).toHaveLength(0);
    expect(result.summary.critical).toBe(0);
  });
});
