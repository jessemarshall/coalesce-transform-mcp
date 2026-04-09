import { describe, it, expect } from "vitest";
import { analyzeNodeContext } from "../../../src/services/config/context-analyzer.js";

describe("analyzeNodeContext", () => {
  it("detects multi-source nodes from sourceMapping dependencies", () => {
    const node = {
      metadata: {
        sourceMapping: [
          {
            dependencies: [
              { nodeName: "ORDERS", locationName: "WORK" },
              { nodeName: "CUSTOMERS", locationName: "WORK" },
            ],
          },
        ],
        columns: [],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasMultipleSources).toBe(true);
  });

  it("detects single-source nodes from sourceMapping dependencies", () => {
    const node = {
      metadata: {
        sourceMapping: [
          {
            dependencies: [
              { nodeName: "ORDERS", locationName: "WORK" },
            ],
          },
        ],
        columns: [],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasMultipleSources).toBe(false);
  });

  it("detects aggregate columns", () => {
    const node = {
      metadata: {
        sourceMapping: [],
        columns: [
          { name: "CUSTOMER_ID", transform: '"ORDERS"."CUSTOMER_ID"' },
          { name: "TOTAL_ORDERS", transform: 'COUNT(DISTINCT "ORDERS"."ORDER_ID")' },
          { name: "REVENUE", transform: 'SUM("ORDERS"."AMOUNT")' },
        ],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasAggregates).toBe(true);
  });

  it("detects non-aggregate columns", () => {
    const node = {
      metadata: {
        sourceMapping: [],
        columns: [
          { name: "CUSTOMER_ID", transform: '"ORDERS"."CUSTOMER_ID"' },
          { name: "NAME", transform: '"CUSTOMERS"."NAME"' },
        ],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasAggregates).toBe(false);
  });

  it("does not detect aggregates in column names containing aggregate keywords", () => {
    const node = {
      metadata: {
        sourceMapping: [],
        columns: [
          { name: "SUMMARY", transform: '"CUSTOMERS"."SUMMARY"' },
          { name: "MINIMUM_PRICE", transform: '"PRODUCTS"."MINIMUM_PRICE"' },
          { name: "COUNTER", transform: '"ACCOUNTS"."COUNTER"' },
        ],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasAggregates).toBe(false);
  });

  it("detects timestamp columns", () => {
    const node = {
      metadata: {
        sourceMapping: [],
        columns: [
          { name: "CUSTOMER_ID", transform: '"ORDERS"."CUSTOMER_ID"' },
          { name: "CREATED_TS", transform: '"ORDERS"."CREATED_TS"' },
          { name: "ORDER_DATE", transform: '"ORDERS"."ORDER_DATE"' },
          { name: "LAST_UPDATED_TIMESTAMP", transform: 'MAX("ORDERS"."UPDATED_TS")' },
        ],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasTimestampColumns).toBe(true);
    expect(result.columnPatterns.timestamps).toContain("CREATED_TS");
    expect(result.columnPatterns.timestamps).toContain("LAST_UPDATED_TIMESTAMP");
    expect(result.columnPatterns.dates).toContain("ORDER_DATE");
  });

  it("detects Type 2 SCD pattern with START_DATE, END_DATE, IS_CURRENT", () => {
    const node = {
      metadata: {
        sourceMapping: [],
        columns: [
          { name: "CUSTOMER_ID", transform: '"CUSTOMERS"."CUSTOMER_ID"' },
          { name: "START_DATE", transform: '"CUSTOMERS"."START_DATE"' },
          { name: "END_DATE", transform: '"CUSTOMERS"."END_DATE"' },
          { name: "IS_CURRENT", transform: '"CUSTOMERS"."IS_CURRENT"' },
        ],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasType2Pattern).toBe(true);
  });

  it("does not detect Type 2 pattern without all three required columns", () => {
    const node = {
      metadata: {
        sourceMapping: [],
        columns: [
          { name: "CUSTOMER_ID", transform: '"CUSTOMERS"."CUSTOMER_ID"' },
          { name: "START_DATE", transform: '"CUSTOMERS"."START_DATE"' },
          { name: "END_DATE", transform: '"CUSTOMERS"."END_DATE"' },
        ],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasType2Pattern).toBe(false);
  });

  it("detects business key candidates from column name patterns", () => {
    const node = {
      metadata: {
        sourceMapping: [],
        columns: [
          { name: "CUSTOMER_ID", transform: '"SRC"."CUSTOMER_ID"' },
          { name: "ORDER_KEY", transform: '"SRC"."ORDER_KEY"' },
          { name: "FULL_NAME", transform: '"SRC"."FULL_NAME"' },
          { name: "EMAIL", transform: '"SRC"."EMAIL"' },
        ],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.columnPatterns.businessKeys).toContain("CUSTOMER_ID");
    expect(result.columnPatterns.businessKeys).toContain("ORDER_KEY");
    expect(result.columnPatterns.businessKeys).not.toContain("FULL_NAME");
    expect(result.columnPatterns.businessKeys).not.toContain("EMAIL");
  });

  it("detects change tracking candidates excluding keys and system columns", () => {
    const node = {
      metadata: {
        sourceMapping: [],
        columns: [
          { name: "CUSTOMER_ID", transform: '"SRC"."CUSTOMER_ID"' },
          { name: "FULL_NAME", transform: '"SRC"."FULL_NAME"' },
          { name: "EMAIL", transform: '"SRC"."EMAIL"' },
          { name: "CREATED_AT", transform: '"SRC"."CREATED_AT"' },
        ],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.columnPatterns.changeTrackingCandidates).toContain("FULL_NAME");
    expect(result.columnPatterns.changeTrackingCandidates).toContain("EMAIL");
    // Business keys and system columns should be excluded
    expect(result.columnPatterns.changeTrackingCandidates).not.toContain("CUSTOMER_ID");
    expect(result.columnPatterns.changeTrackingCandidates).not.toContain("CREATED_AT");
  });
});
