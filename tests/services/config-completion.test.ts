import { describe, it, expect, vi } from "vitest";
import { convertJoinToAggregation } from "../../src/services/workspace/mutations.js";

// Mock the intelligent config module
vi.mock("../../src/services/config/intelligent.js", () => ({
  completeNodeConfiguration: vi.fn().mockResolvedValue({
    node: {
      id: "fact-node",
      nodeType: "Dimension",
      config: {
        businessKey: "CUSTOMER_ID",
        changeTracking: "TOTAL_ORDERS",
        insertStrategy: "UNION ALL",
        selectDistinct: false,
        truncateBefore: false,
      },
    },
    schemaSource: "corpus",
    classification: {
      required: [],
      conditionalRequired: [],
      optionalWithDefaults: ["insertStrategy", "selectDistinct", "truncateBefore"],
      contextual: [],
    },
    context: {
      hasMultipleSources: false,
      hasAggregates: true,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table",
    },
    appliedConfig: {
      businessKey: "CUSTOMER_ID",
      changeTracking: "TOTAL_ORDERS",
      insertStrategy: "UNION ALL",
      selectDistinct: false,
      truncateBefore: false,
    },
    configChanges: {
      required: {},
      contextual: {
        selectDistinct: false,
        truncateBefore: false,
      },
      preserved: {},
      defaults: {},
    },
    columnAttributeChanges: {
      applied: [],
      reasoning: [],
    },
    reasoning: [
      "Aggregates are incompatible with SELECT DISTINCT; suggests selectDistinct: false",
      "Table materialization suggests truncateBefore: false to preserve existing data",
    ],
    detectedPatterns: {
      candidateColumns: ["CUSTOMER_ID"],
    },
  }),
}));

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ id: "new-node" }),
    put: vi.fn().mockResolvedValue({ id: "updated-node" }),
    delete: vi.fn(),
  };
}

describe("Config Auto-Completion in convert-join-to-aggregation", () => {
  it("automatically sets businessKey from GROUP BY columns", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
        return Promise.resolve({
          id: "fact-node",
          name: "FCT_METRICS",
          nodeType: "Dimension",
          metadata: {
            sourceMapping: [],
          },
        });
      }
      return Promise.resolve({});
    });

    client.put.mockResolvedValue({ id: "fact-node" });

    await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "fact-node",
      groupByColumns: ['"ORDERS"."CUSTOMER_ID"', '"ORDERS"."REGION"'],
      aggregates: [
        {
          name: "TOTAL_ORDERS",
          function: "COUNT",
          expression: 'DISTINCT "ORDERS"."ORDER_ID"',
        },
      ],
      maintainJoins: false,
    });

    const putCall = client.put.mock.calls[0];
    const updatedNode = putCall[1];

    // Column-level attributes: isBusinessKey set on GROUP BY columns
    const columns = updatedNode.metadata.columns;
    const customerCol = columns.find((c: any) => c.name === "CUSTOMER_ID");
    const regionCol = columns.find((c: any) => c.name === "REGION");
    const totalOrdersCol = columns.find((c: any) => c.name === "TOTAL_ORDERS");
    expect(customerCol.isBusinessKey).toBe(true);
    expect(regionCol.isBusinessKey).toBe(true);
    expect(totalOrdersCol.isBusinessKey).toBeUndefined();
  });

  it("automatically sets changeTracking from aggregate columns", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
        return Promise.resolve({
          id: "fact-node",
          name: "FCT_METRICS",
          nodeType: "Dimension",
          metadata: {
            sourceMapping: [],
          },
        });
      }
      return Promise.resolve({});
    });

    client.put.mockResolvedValue({ id: "fact-node" });

    await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "fact-node",
      groupByColumns: ['"ORDERS"."CUSTOMER_ID"'],
      aggregates: [
        {
          name: "TOTAL_ORDERS",
          function: "COUNT",
          expression: 'DISTINCT "ORDERS"."ORDER_ID"',
        },
        {
          name: "LIFETIME_VALUE",
          function: "SUM",
          expression: '"ORDERS"."ORDER_TOTAL"',
        },
        {
          name: "AVG_ORDER_VALUE",
          function: "AVG",
          expression: '"ORDERS"."ORDER_TOTAL"',
        },
      ],
      maintainJoins: false,
    });

    const putCall = client.put.mock.calls[0];
    const updatedNode = putCall[1];

    // Column-level attributes: isChangeTracking set on aggregate columns
    const columns = updatedNode.metadata.columns;
    const customerCol = columns.find((c: any) => c.name === "CUSTOMER_ID");
    const totalOrdersCol = columns.find((c: any) => c.name === "TOTAL_ORDERS");
    const lifetimeValueCol = columns.find((c: any) => c.name === "LIFETIME_VALUE");
    const avgOrderValueCol = columns.find((c: any) => c.name === "AVG_ORDER_VALUE");
    expect(customerCol.isChangeTracking).toBeUndefined();
    expect(totalOrdersCol.isChangeTracking).toBe(true);
    expect(lifetimeValueCol.isChangeTracking).toBe(true);
    expect(avgOrderValueCol.isChangeTracking).toBe(true);
  });

  it("sets both businessKey and changeTracking together", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
        return Promise.resolve({
          id: "fact-node",
          name: "FCT_CUSTOMER_METRICS",
          nodeType: "Dimension",
          metadata: {
            sourceMapping: [],
          },
        });
      }
      return Promise.resolve({});
    });

    client.put.mockResolvedValue({ id: "fact-node" });

    await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "fact-node",
      groupByColumns: ['"STG_ORDER_HEADER"."CUSTOMER_ID"'],
      aggregates: [
        {
          name: "TOTAL_ORDERS",
          function: "COUNT",
          expression: 'DISTINCT "STG_ORDER_HEADER"."ORDER_ID"',
        },
        {
          name: "LIFETIME_VALUE",
          function: "SUM",
          expression: '"STG_ORDER_HEADER"."ORDER_TOTAL"',
        },
        {
          name: "AVG_ORDER_VALUE",
          function: "AVG",
          expression: '"STG_ORDER_HEADER"."ORDER_TOTAL"',
        },
        {
          name: "FIRST_ORDER_DATE",
          function: "MIN",
          expression: '"STG_ORDER_HEADER"."ORDER_TS"',
        },
        {
          name: "LAST_ORDER_DATE",
          function: "MAX",
          expression: '"STG_ORDER_HEADER"."ORDER_TS"',
        },
      ],
      maintainJoins: false,
    });

    const putCall = client.put.mock.calls[0];
    const updatedNode = putCall[1];

    // Column-level attributes: isBusinessKey and isChangeTracking on appropriate columns
    const columns = updatedNode.metadata.columns;
    const customerCol = columns.find((c: any) => c.name === "CUSTOMER_ID");
    const totalOrdersCol = columns.find((c: any) => c.name === "TOTAL_ORDERS");
    const lifetimeValueCol = columns.find((c: any) => c.name === "LIFETIME_VALUE");
    const avgOrderValueCol = columns.find((c: any) => c.name === "AVG_ORDER_VALUE");
    const firstOrderDateCol = columns.find((c: any) => c.name === "FIRST_ORDER_DATE");
    const lastOrderDateCol = columns.find((c: any) => c.name === "LAST_ORDER_DATE");
    expect(customerCol.isBusinessKey).toBe(true);
    expect(customerCol.isChangeTracking).toBeUndefined();
    expect(totalOrdersCol.isChangeTracking).toBe(true);
    expect(lifetimeValueCol.isChangeTracking).toBe(true);
    expect(avgOrderValueCol.isChangeTracking).toBe(true);
    expect(firstOrderDateCol.isChangeTracking).toBe(true);
    expect(lastOrderDateCol.isChangeTracking).toBe(true);
  });

  it("extracts column names correctly from fully-qualified GROUP BY columns", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
        return Promise.resolve({
          id: "fact-node",
          name: "FCT_METRICS",
          nodeType: "Dimension",
          metadata: {
            sourceMapping: [],
          },
        });
      }
      return Promise.resolve({});
    });

    client.put.mockResolvedValue({ id: "fact-node" });

    await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "fact-node",
      groupByColumns: [
        '"DATABASE"."SCHEMA"."TABLE"."CUSTOMER_ID"',
        '"ANOTHER_TABLE"."REGION_CODE"',
      ],
      aggregates: [
        {
          name: "METRIC_1",
          function: "COUNT",
          expression: "*",
        },
      ],
      maintainJoins: false,
    });

    const putCall = client.put.mock.calls[0];
    const updatedNode = putCall[1];

    // Should extract just the column names and set isBusinessKey on those columns
    const columns = updatedNode.metadata.columns;
    const customerCol = columns.find((c: any) => c.name === "CUSTOMER_ID");
    const regionCol = columns.find((c: any) => c.name === "REGION_CODE");
    const metricCol = columns.find((c: any) => c.name === "METRIC_1");
    expect(customerCol.isBusinessKey).toBe(true);
    expect(regionCol.isBusinessKey).toBe(true);
    expect(metricCol.isBusinessKey).toBeUndefined();
    expect(metricCol.isChangeTracking).toBe(true);
  });

  it("includes configCompletion result with intelligent configuration", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
        return Promise.resolve({
          id: "fact-node",
          name: "FCT_METRICS",
          nodeType: "Dimension",
          metadata: {
            columns: [
              { name: "CUSTOMER_ID", transform: "CUSTOMER_ID" },
              { name: "TOTAL_ORDERS", transform: "COUNT(DISTINCT ORDER_ID)" },
            ],
            sourceMapping: [],
          },
          config: {},
        });
      }
      return Promise.resolve({});
    });

    client.put.mockResolvedValue({
      id: "fact-node",
      config: {
        businessKey: "CUSTOMER_ID",
        changeTracking: "TOTAL_ORDERS",
      },
    });

    const result = await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "fact-node",
      groupByColumns: ['"ORDERS"."CUSTOMER_ID"'],
      aggregates: [
        {
          name: "TOTAL_ORDERS",
          function: "COUNT",
          expression: 'DISTINCT "ORDERS"."ORDER_ID"',
        },
      ],
      maintainJoins: false,
      repoPath: "/path/to/repo",
    });

    // Verify configCompletion is included in result
    expect(result).toHaveProperty("configCompletion");
    expect(result.configCompletion).toHaveProperty("schemaSource");
    expect(result.configCompletion).toHaveProperty("classification");
    expect(result.configCompletion).toHaveProperty("context");
    expect(result.configCompletion).toHaveProperty("appliedConfig");
    expect(result.configCompletion).toHaveProperty("configChanges");
    expect(result.configCompletion).toHaveProperty("reasoning");

    // Verify intelligent config was applied
    expect(result.configCompletion.appliedConfig).toHaveProperty("selectDistinct");
    expect(result.configCompletion.appliedConfig.selectDistinct).toBe(false);

    // Verify reasoning is included
    expect(result.configCompletion.reasoning).toContain(
      "Aggregates are incompatible with SELECT DISTINCT; suggests selectDistinct: false"
    );
  });
});
