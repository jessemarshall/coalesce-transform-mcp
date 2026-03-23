import { describe, it, expect } from "vitest";
import { analyzeNodeContext } from "../../src/services/config/context-analyzer.js";

describe("Intelligent Node Configuration Integration", () => {
  it("detects multi-source patterns and aggregates in node context", () => {
    const node = {
      id: "fact-1",
      name: "FCT_CUSTOMER_METRICS",
      nodeType: "DataVault:::33",
      metadata: {
        sourceMapping: [
          {
            dependencies: [
              { nodeName: "ORDERS", locationName: "WORK" },
              { nodeName: "CUSTOMERS", locationName: "WORK" },
            ],
          },
        ],
        columns: [
          {
            name: "CUSTOMER_ID",
            transform: '"CUSTOMERS"."CUSTOMER_ID"',
            dataType: "VARCHAR",
          },
          {
            name: "TOTAL_ORDERS",
            transform: 'COUNT(DISTINCT "ORDERS"."ORDER_ID")',
            dataType: "NUMBER",
          },
          {
            name: "LIFETIME_VALUE",
            transform: 'SUM("ORDERS"."ORDER_TOTAL")',
            dataType: "NUMBER(38,4)",
          },
        ],
      },
      config: {},
    };

    const context = analyzeNodeContext(node as any);

    // Verify multi-source detection
    expect(context.hasMultipleSources).toBe(true);

    // Verify aggregate detection
    expect(context.hasAggregates).toBe(true);
  });

  it("detects timestamp columns as candidates for lastModifiedColumn", () => {
    const node = {
      id: "dim-1",
      name: "DIM_CUSTOMERS",
      nodeType: "DataVault:::33",
      metadata: {
        sourceMapping: [
          {
            dependencies: [{ nodeName: "SRC", locationName: "WORK" }],
          },
        ],
        columns: [
          {
            name: "CUSTOMER_ID",
            transform: '"SRC"."ID"',
            dataType: "VARCHAR",
          },
          {
            name: "CREATED_TS",
            transform: '"SRC"."CREATED_TS"',
            dataType: "TIMESTAMP_NTZ",
          },
          {
            name: "UPDATED_TS",
            transform: '"SRC"."UPDATED_TS"',
            dataType: "TIMESTAMP_NTZ",
          },
        ],
      },
      config: {},
    };

    const context = analyzeNodeContext(node as any);

    // Verify timestamp detection
    expect(context.hasTimestampColumns).toBe(true);
    expect(context.columnPatterns.timestamps).toContain("CREATED_TS");
    expect(context.columnPatterns.timestamps).toContain("UPDATED_TS");
    expect(context.columnPatterns.timestamps).toHaveLength(2);
  });
});
