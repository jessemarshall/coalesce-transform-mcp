import { describe, it, expect, vi } from "vitest";
import { convertJoinToAggregation } from "../../src/services/workspace/mutations.js";

// Mock the intelligent config module
vi.mock("../../src/services/config/intelligent.js", () => ({
  completeNodeConfiguration: vi.fn().mockResolvedValue({
    node: {
      id: "fact-node",
      nodeType: "Fact",
      config: {
        selectDistinct: false,
        truncateBefore: false,
      },
    },
    schemaSource: "corpus",
    classification: {
      required: [],
      conditionalRequired: [],
      optionalWithDefaults: ["selectDistinct", "truncateBefore"],
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
    },
    reasoning: [
      "Aggregates are incompatible with SELECT DISTINCT; suggests selectDistinct: false",
      "Table materialization suggests truncateBefore: false to preserve existing data",
    ],
    detectedPatterns: {
      candidateColumns: [],
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

describe("Join to Aggregation Conversion", () => {
  describe("convertJoinToAggregation", () => {
    it("converts a join node to aggregated fact with GROUP BY", async () => {
      const client = createMockClient();

      // Mock the current node (a join node)
      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
          return Promise.resolve({
            id: "fact-node",
            name: "FCT_PURCHASE",
            nodeType: "Fact",
            metadata: {
              columns: [
                { name: "ORDER_ID" },
                { name: "CUSTOMER_ID" },
                { name: "ORDER_TOTAL" },
              ],
              sourceMapping: [
                {
                  aliases: {
                    STG_ORDERS: "stg-orders",
                    STG_CUSTOMERS: "stg-customers",
                  },
                  dependencies: [
                    { nodeName: "STG_ORDERS", locationName: "WORK" },
                    { nodeName: "STG_CUSTOMERS", locationName: "WORK" },
                  ],
                },
              ],
            },
          });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/stg-orders") {
          return Promise.resolve({
            id: "stg-orders",
            name: "STG_ORDERS",
            metadata: {
              columns: [
                { name: "ORDER_ID" },
                { name: "CUSTOMER_ID" },
                { name: "ORDER_TOTAL" },
              ],
            },
          });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/stg-customers") {
          return Promise.resolve({
            id: "stg-customers",
            name: "STG_CUSTOMERS",
            metadata: {
              columns: [{ name: "CUSTOMER_ID" }, { name: "CUSTOMER_NAME" }],
            },
          });
        }
        return Promise.resolve({});
      });

      client.put.mockResolvedValue({ id: "fact-node" });

      const result = await convertJoinToAggregation(client as any, {
        workspaceID: "ws-1",
        nodeID: "fact-node",
        groupByColumns: ['"STG_ORDERS"."CUSTOMER_ID"'],
        aggregates: [
          {
            name: "TOTAL_ORDERS",
            function: "COUNT",
            expression: 'DISTINCT "STG_ORDERS"."ORDER_ID"',
          },
          {
            name: "LIFETIME_VALUE",
            function: "SUM",
            expression: '"STG_ORDERS"."ORDER_TOTAL"',
          },
        ],
        maintainJoins: true,
      });

      // Verify the node was updated with correct columns
      expect(client.put).toHaveBeenCalled();
      const putCall = client.put.mock.calls[0];
      const updatedNode = putCall[1];

      expect(updatedNode.metadata.columns).toHaveLength(3);
      expect(updatedNode.metadata.columns[0].name).toBe("CUSTOMER_ID");
      expect(updatedNode.metadata.columns[1].name).toBe("TOTAL_ORDERS");
      expect(updatedNode.metadata.columns[1].transform).toBe(
        'COUNT(DISTINCT "STG_ORDERS"."ORDER_ID")'
      );
      expect(updatedNode.metadata.columns[1].dataType).toBe("NUMBER");
      expect(updatedNode.metadata.columns[2].name).toBe("LIFETIME_VALUE");
      expect(updatedNode.metadata.columns[2].transform).toBe(
        'SUM("STG_ORDERS"."ORDER_TOTAL")'
      );
      expect(updatedNode.metadata.columns[2].dataType).toBe("NUMBER(38,4)");

      // Verify GROUP BY analysis
      expect(result.groupByAnalysis.hasAggregates).toBe(true);
      expect(result.groupByAnalysis.groupByColumns).toHaveLength(1);
      expect(result.groupByAnalysis.aggregateColumns).toHaveLength(2);
      expect(result.groupByAnalysis.groupByClause).toContain("GROUP BY");

      // Verify JOIN SQL generation
      expect(result.joinSQL.fromClause).toContain("FROM");
      expect(result.joinSQL.fullSQL).toContain("INNER JOIN");
      expect(result.joinSQL.fullSQL).toContain("ON");
      expect(result.joinSQL.fullSQL).toContain("CUSTOMER_ID");

      // Verify validation
      expect(result.validation.valid).toBe(true);
      expect(result.validation.warnings).toHaveLength(0);
    });

    it("infers datatypes correctly for various aggregate functions", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
          return Promise.resolve({
            id: "fact-node",
            name: "FCT_TABLE",
            metadata: {
              sourceMapping: [],
            },
          });
        }
        return Promise.resolve({});
      });

      client.put.mockResolvedValue({ id: "fact-node" });

      const result = await convertJoinToAggregation(client as any, {
        workspaceID: "ws-1",
        nodeID: "fact-node",
        groupByColumns: ['"TABLE"."DIM_COL"'],
        aggregates: [
          {
            name: "COUNT_ORDERS",
            function: "COUNT",
            expression: 'DISTINCT "TABLE"."ORDER_ID"',
          },
          {
            name: "TOTAL_AMOUNT",
            function: "SUM",
            expression: '"TABLE"."AMOUNT"',
          },
          {
            name: "AVG_AMOUNT",
            function: "AVG",
            expression: '"TABLE"."AMOUNT"',
          },
          {
            name: "FIRST_ORDER",
            function: "MIN",
            expression: '"TABLE"."ORDER_TS"',
          },
          {
            name: "LAST_ORDER",
            function: "MAX",
            expression: '"TABLE"."ORDER_TS"',
          },
          {
            name: "DAYS_ACTIVE",
            function: "DATEDIFF",
            expression: 'day, MIN("TABLE"."ORDER_TS"), MAX("TABLE"."ORDER_TS")',
          },
        ],
        maintainJoins: false,
      });

      const putCall = client.put.mock.calls[0];
      const updatedNode = putCall[1];

      const columns = updatedNode.metadata.columns;
      expect(columns[1].dataType).toBe("NUMBER"); // COUNT
      expect(columns[2].dataType).toBe("NUMBER(38,4)"); // SUM
      expect(columns[3].dataType).toBe("NUMBER(38,4)"); // AVG
      expect(columns[4].dataType).toBe("TIMESTAMP_NTZ(9)"); // MIN(_TS)
      expect(columns[5].dataType).toBe("TIMESTAMP_NTZ(9)"); // MAX(_TS)
      expect(columns[6].dataType).toBe("NUMBER"); // DATEDIFF
    });

    it("validates GROUP BY requirements and returns errors for invalid queries", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
          return Promise.resolve({
            id: "fact-node",
            name: "FCT_TABLE",
            metadata: {
              sourceMapping: [],
            },
          });
        }
        return Promise.resolve({});
      });

      client.put.mockResolvedValue({ id: "fact-node" });

      const result = await convertJoinToAggregation(client as any, {
        workspaceID: "ws-1",
        nodeID: "fact-node",
        groupByColumns: [], // No GROUP BY columns, but has aggregates - should warn
        aggregates: [
          {
            name: "TOTAL_ORDERS",
            function: "COUNT",
            expression: 'DISTINCT "TABLE"."ORDER_ID"',
          },
          {
            name: "CUSTOMER_NAME", // Non-aggregate column but not in GROUP BY
            function: "FIRST_VALUE",
            expression: '"TABLE"."CUSTOMER_NAME" OVER (ORDER BY ORDER_DATE)',
          },
        ],
        maintainJoins: false,
      });

      // Should have warnings about missing GROUP BY
      expect(result.validation.warnings.length).toBeGreaterThan(0);
      expect(result.groupByAnalysis.validation.valid).toBe(false);
    });

    it("generates correct JOIN SQL from predecessor common columns", async () => {
      const client = createMockClient();

      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
          return Promise.resolve({
            id: "fact-node",
            name: "FCT_TABLE",
            metadata: {
              sourceMapping: [
                {
                  aliases: {
                    ORDERS: "orders",
                    CUSTOMERS: "customers",
                  },
                  dependencies: [
                    { nodeName: "ORDERS", locationName: "WORK" },
                    { nodeName: "CUSTOMERS", locationName: "WORK" },
                  ],
                },
              ],
            },
          });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/orders") {
          return Promise.resolve({
            id: "orders",
            name: "ORDERS",
            locationName: "WORK",
            metadata: {
              columns: [
                { name: "ORDER_ID" },
                { name: "CUSTOMER_ID" },
                { name: "ORDER_DATE" },
              ],
            },
          });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/customers") {
          return Promise.resolve({
            id: "customers",
            name: "CUSTOMERS",
            locationName: "WORK",
            metadata: {
              columns: [{ name: "CUSTOMER_ID" }, { name: "CUSTOMER_NAME" }],
            },
          });
        }
        return Promise.resolve({});
      });

      client.put.mockResolvedValue({ id: "fact-node" });

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
        joinType: "LEFT JOIN",
        maintainJoins: true,
      });

      // Verify JOIN SQL generation with {{ ref() }} syntax
      expect(result.joinSQL.fromClause).toContain("{{ ref('WORK', 'ORDERS') }}");
      expect(result.joinSQL.joinClauses).toHaveLength(1);
      expect(result.joinSQL.joinClauses[0]).toContain("LEFT JOIN");
      expect(result.joinSQL.joinClauses[0]).toContain("{{ ref('WORK', 'CUSTOMERS') }}");
      expect(result.joinSQL.joinClauses[0]).toContain(
        '"ORDERS"."CUSTOMER_ID" = "CUSTOMERS"."CUSTOMER_ID"'
      );

      // Full SQL should include FROM with ref, JOIN with ref, and ON
      expect(result.joinSQL.fullSQL).toContain("{{ ref('WORK', 'ORDERS') }}");
      expect(result.joinSQL.fullSQL).toContain("LEFT JOIN {{ ref('WORK', 'CUSTOMERS') }}");
      expect(result.joinSQL.fullSQL).toContain("ON");
      expect(result.joinSQL.fullSQL).toContain("CUSTOMER_ID");
    });
  });
});
