import { describe, it, expect, vi, afterEach } from "vitest";
import { parseIntent, resolveIntentEntities, buildPipelinePlanFromIntent } from "../../src/services/pipelines/intent.js";
import { createMockClient, buildSourceNode } from "../helpers/fixtures.js";

// Mock completeNodeConfiguration so pipeline tests don't need corpus/repo files
vi.mock("../../src/services/config/intelligent.js", () => ({
  completeNodeConfiguration: vi.fn(async () => ({})),
}));

describe("parseIntent", () => {
  it("detects a simple staging intent", () => {
    const result = parseIntent("stage the raw payments table");
    expect(result.steps).toHaveLength(1);
    expect(result.steps[0]!.operation).toBe("stage");
    expect(result.openQuestions).toHaveLength(0);
  });

  it("detects a join intent with two entities", () => {
    const result = parseIntent("join CUSTOMERS and ORDERS on CUSTOMER_ID");
    expect(result.steps).toHaveLength(1);
    expect(result.steps[0]!.operation).toBe("join");
    expect(result.steps[0]!.entityNames).toContain("CUSTOMERS");
    expect(result.steps[0]!.entityNames).toContain("ORDERS");
    expect(result.steps[0]!.joinKey).toBe("CUSTOMER_ID");
  });

  it("detects a combine intent as join", () => {
    const result = parseIntent("combine PRODUCTS with INVENTORY");
    expect(result.steps.length).toBeGreaterThanOrEqual(1);
    expect(result.steps[0]!.operation).toBe("join");
    expect(result.steps[0]!.entityNames).toContain("PRODUCTS");
    expect(result.steps[0]!.entityNames).toContain("INVENTORY");
  });

  it("detects aggregate with group by", () => {
    const result = parseIntent("aggregate total revenue by region");
    const aggStep = result.steps.find((s) => s.operation === "aggregate");
    expect(aggStep).toBeDefined();
    expect(aggStep!.columns.length).toBeGreaterThanOrEqual(1);
    expect(aggStep!.columns[0]!.aggregateFunction).toBe("SUM");
  });

  it("detects join followed by aggregation", () => {
    const result = parseIntent(
      "combine CUSTOMERS and ORDERS on CUSTOMER_ID, aggregate total REVENUE by REGION"
    );
    expect(result.steps.length).toBe(2);
    expect(result.steps[0]!.operation).toBe("join");
    expect(result.steps[1]!.operation).toBe("aggregate");
  });

  it("asks for clarification when no entities found", () => {
    const result = parseIntent("do something with the data");
    expect(result.openQuestions.length).toBeGreaterThan(0);
  });

  it("asks for join key when missing", () => {
    const result = parseIntent("join CUSTOMERS and ORDERS");
    expect(result.steps[0]!.operation).toBe("join");
    expect(result.openQuestions.some((q) => q.includes("column"))).toBe(true);
  });

  it("detects left join type", () => {
    const result = parseIntent("left join CUSTOMERS and ORDERS on CUSTOMER_ID");
    expect(result.steps[0]!.joinType).toBe("LEFT");
  });

  it("detects filter keywords", () => {
    const result = parseIntent("stage PAYMENTS filter to active customers");
    const stepWithFilter = result.steps.find((s) => s.filters.length > 0);
    expect(stepWithFilter).toBeDefined();
  });

  it("detects multiple aggregate functions", () => {
    const result = parseIntent("aggregate sum REVENUE and count ORDERS by REGION");
    const aggStep = result.steps.find((s) => s.operation === "aggregate");
    expect(aggStep).toBeDefined();
    expect(aggStep!.columns.length).toBeGreaterThanOrEqual(2);
    const fns = aggStep!.columns.map((c) => c.aggregateFunction);
    expect(fns).toContain("SUM");
    expect(fns).toContain("COUNT");
  });

  it("extracts group by columns", () => {
    const result = parseIntent("aggregate total revenue group by region, category");
    const aggStep = result.steps.find((s) => s.operation === "aggregate");
    expect(aggStep).toBeDefined();
    expect(aggStep!.groupByColumns).toContain("REGION");
    expect(aggStep!.groupByColumns).toContain("CATEGORY");
  });
});

describe("resolveIntentEntities", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("resolves exact name match", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "node-1", name: "CUSTOMERS", nodeType: "Stage", locationName: "RAW" },
        { id: "node-2", name: "ORDERS", nodeType: "Stage", locationName: "RAW" },
      ],
    });

    const result = await resolveIntentEntities(client as any, "ws-1", ["CUSTOMERS"]);
    expect(result).toHaveLength(1);
    expect(result[0]!.resolvedNodeID).toBe("node-1");
    expect(result[0]!.confidence).toBe("exact");
  });

  it("resolves fuzzy match with prefix strip", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "node-1", name: "STG_CUSTOMERS", nodeType: "Stage", locationName: "STAGING" },
      ],
    });

    const result = await resolveIntentEntities(client as any, "ws-1", ["CUSTOMERS"]);
    expect(result).toHaveLength(1);
    expect(result[0]!.resolvedNodeID).toBe("node-1");
    expect(result[0]!.confidence).toBe("exact"); // score 85 >= 85 threshold
  });

  it("marks unresolved when no match found", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "node-1", name: "PRODUCTS", nodeType: "Stage", locationName: "RAW" },
      ],
    });

    const result = await resolveIntentEntities(client as any, "ws-1", ["PAYMENTS"]);
    expect(result).toHaveLength(1);
    expect(result[0]!.confidence).toBe("unresolved");
    expect(result[0]!.resolvedNodeID).toBeNull();
  });

  it("marks ambiguous when multiple exact matches", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "node-1", name: "CUSTOMERS", nodeType: "Stage", locationName: "RAW" },
        { id: "node-2", name: "CUSTOMERS", nodeType: "Stage", locationName: "STAGING" },
      ],
    });

    const result = await resolveIntentEntities(client as any, "ws-1", ["CUSTOMERS"]);
    expect(result).toHaveLength(1);
    expect(result[0]!.confidence).toBe("unresolved");
    expect(result[0]!.candidates).toHaveLength(2);
  });

  it("resolves pluralization (singular to plural)", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "node-1", name: "CUSTOMERS", nodeType: "Stage", locationName: "RAW" },
      ],
    });

    const result = await resolveIntentEntities(client as any, "ws-1", ["CUSTOMER"]);
    expect(result).toHaveLength(1);
    expect(result[0]!.resolvedNodeID).toBe("node-1");
    expect(result[0]!.confidence).toBe("fuzzy"); // score 82 < 85 threshold
  });

  it("resolves multiple entities independently", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "node-1", name: "CUSTOMERS", nodeType: "Stage", locationName: "RAW" },
        { id: "node-2", name: "ORDERS", nodeType: "Stage", locationName: "RAW" },
      ],
    });

    const result = await resolveIntentEntities(client as any, "ws-1", [
      "CUSTOMERS",
      "ORDERS",
    ]);
    expect(result).toHaveLength(2);
    expect(result[0]!.resolvedNodeID).toBe("node-1");
    expect(result[1]!.resolvedNodeID).toBe("node-2");
  });
});

describe("buildPipelinePlanFromIntent", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function setupMockClient(nodes: Array<{ id: string; name: string; locationName: string }>) {
    const client = createMockClient();
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      // listWorkspaceNodes
      if (path.includes("/nodes") && params?.detail === false) {
        return Promise.resolve({
          data: nodes.map((n) => ({ nodeType: "Stage" })),
        });
      }
      if (path.match(/\/nodes$/) && !path.includes("/nodes/")) {
        return Promise.resolve({
          data: nodes.map((n) => ({
            id: n.id,
            name: n.name,
            nodeType: "Stage",
            locationName: n.locationName,
          })),
        });
      }
      // getWorkspaceNode by ID
      for (const n of nodes) {
        if (path.includes(`/nodes/${n.id}`)) {
          return Promise.resolve(buildSourceNode(n.id, n.name, n.locationName));
        }
      }
      return Promise.resolve({ data: [] });
    });
    return client;
  }

  it("builds a ready plan for a single-entity stage intent", async () => {
    const client = setupMockClient([
      { id: "node-1", name: "PAYMENTS", locationName: "RAW" },
    ]);

    const result = await buildPipelinePlanFromIntent(client as any, {
      workspaceID: "ws-1",
      intent: "stage the raw payments table",
    });

    expect(result.status).toBe("ready");
    expect(result.plan).not.toBeNull();
    expect(result.resolvedEntities).toHaveLength(1);
    expect(result.resolvedEntities[0]!.resolvedNodeName).toBe("PAYMENTS");

    const plan = result.plan as Record<string, unknown>;
    const nodes = plan.nodes as Array<Record<string, unknown>>;
    expect(nodes).toHaveLength(1);
    expect(nodes[0]!.predecessorNodeIDs).toContain("node-1");
  });

  it("builds a ready plan for a two-entity join with join key", async () => {
    const client = setupMockClient([
      { id: "node-1", name: "CUSTOMERS", locationName: "RAW" },
      { id: "node-2", name: "ORDERS", locationName: "RAW" },
    ]);

    const result = await buildPipelinePlanFromIntent(client as any, {
      workspaceID: "ws-1",
      intent: "join CUSTOMERS and ORDERS on CUSTOMER_ID",
    });

    expect(result.status).toBe("ready");
    expect(result.plan).not.toBeNull();

    const plan = result.plan as Record<string, unknown>;
    const nodes = plan.nodes as Array<Record<string, unknown>>;
    expect(nodes).toHaveLength(1);
    expect(nodes[0]!.predecessorNodeIDs).toContain("node-1");
    expect(nodes[0]!.predecessorNodeIDs).toContain("node-2");

    // Verify join condition contains ref() syntax
    const joinCondition = nodes[0]!.joinCondition as string;
    expect(joinCondition).toContain("{{ ref('RAW', 'CUSTOMERS') }}");
    expect(joinCondition).toContain("{{ ref('RAW', 'ORDERS') }}");
    expect(joinCondition).toContain("CUSTOMER_ID");
  });

  it("returns needs_entity_resolution when entity not found", async () => {
    const client = setupMockClient([
      { id: "node-1", name: "PRODUCTS", locationName: "RAW" },
    ]);

    const result = await buildPipelinePlanFromIntent(client as any, {
      workspaceID: "ws-1",
      intent: "stage the PAYMENTS table",
    });

    expect(result.status).toBe("needs_entity_resolution");
    expect(result.plan).toBeNull();
    expect(result.openQuestions.some((q) => q.includes("PAYMENTS"))).toBe(true);
  });

  it("builds multi-step plan for join then aggregate", async () => {
    const client = setupMockClient([
      { id: "node-1", name: "CUSTOMERS", locationName: "RAW" },
      { id: "node-2", name: "ORDERS", locationName: "RAW" },
    ]);

    const result = await buildPipelinePlanFromIntent(client as any, {
      workspaceID: "ws-1",
      intent: "combine CUSTOMERS and ORDERS on CUSTOMER_ID, aggregate total REVENUE by REGION",
    });

    expect(result.plan).not.toBeNull();
    const plan = result.plan as Record<string, unknown>;
    const nodes = plan.nodes as Array<Record<string, unknown>>;
    expect(nodes.length).toBe(2);
  });

  it("adds warning when predecessor node fetch fails", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path.includes("/nodes") && params?.detail === false) {
        return Promise.resolve({ data: [{ nodeType: "Stage" }] });
      }
      if (path.match(/\/nodes$/) && !path.includes("/nodes/")) {
        return Promise.resolve({
          data: [{ id: "node-1", name: "CUSTOMERS", nodeType: "Stage", locationName: "RAW" }],
        });
      }
      if (path.includes("/nodes/node-1")) {
        return Promise.reject(new Error("Network timeout"));
      }
      return Promise.resolve({ data: [] });
    });

    const result = await buildPipelinePlanFromIntent(client as any, {
      workspaceID: "ws-1",
      intent: "stage the CUSTOMERS table",
    });

    // Should still produce a plan but with a warning about the fetch failure
    expect(result.warnings.some((w) => w.includes("Could not fetch predecessor"))).toBe(true);
  });

  it("adds warning when location name is missing", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path.includes("/nodes") && params?.detail === false) {
        return Promise.resolve({ data: [{ nodeType: "Stage" }] });
      }
      if (path.match(/\/nodes$/) && !path.includes("/nodes/")) {
        return Promise.resolve({
          data: [{ id: "node-1", name: "CUSTOMERS", nodeType: "Stage" }],
        });
      }
      if (path.includes("/nodes/node-1")) {
        return Promise.resolve({
          id: "node-1",
          name: "CUSTOMERS",
          metadata: { columns: [{ name: "ID", columnID: "col-1", dataType: "VARCHAR" }] },
        });
      }
      return Promise.resolve({ data: [] });
    });

    const result = await buildPipelinePlanFromIntent(client as any, {
      workspaceID: "ws-1",
      intent: "stage the CUSTOMERS table",
    });

    expect(result.warnings.some((w) => w.includes("no location name"))).toBe(true);
  });
});
