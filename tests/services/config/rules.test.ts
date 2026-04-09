import { describe, it, expect } from "vitest";
import { applyIntelligenceRules } from "../../../src/services/config/rules.js";

describe("applyIntelligenceRules", () => {
  it("suggests UNION ALL for multi-source nodes without aggregates", () => {
    const context = {
      hasMultipleSources: true,
      hasAggregates: false,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table" as const,
      columnPatterns: {
        timestamps: [],
        dates: [],
        businessKeys: [],
      },
    };

    const result = applyIntelligenceRules(context);

    expect(result.suggestions.insertStrategy).toBe("UNION ALL");
    expect(result.reasoning.join(" ")).toContain("UNION ALL");
    expect(result.reasoning.join(" ").toLowerCase()).toContain("multi");
  });

  it("suggests UNION for multi-source nodes with aggregates", () => {
    const context = {
      hasMultipleSources: true,
      hasAggregates: true,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table" as const,
      columnPatterns: {
        timestamps: [],
        dates: [],
        businessKeys: [],
      },
    };

    const result = applyIntelligenceRules(context);

    expect(result.suggestions.insertStrategy).toBe("UNION");
    expect(result.reasoning.join(" ")).toContain("UNION");
    expect(result.reasoning.join(" ")).toContain("aggregates");
  });

  it("does not suggest insertStrategy for single-source nodes", () => {
    const context = {
      hasMultipleSources: false,
      hasAggregates: false,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table" as const,
      columnPatterns: {
        timestamps: [],
        dates: [],
        businessKeys: [],
      },
    };

    const result = applyIntelligenceRules(context);

    expect(result.suggestions.insertStrategy).toBeUndefined();
  });

  it("suggests selectDistinct: false when node has aggregates", () => {
    const context = {
      hasMultipleSources: false,
      hasAggregates: true,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table" as const,
      columnPatterns: {
        timestamps: [],
        dates: [],
        businessKeys: [],
      },
    };

    const result = applyIntelligenceRules(context);

    expect(result.suggestions.selectDistinct).toBe(false);
    expect(result.reasoning.join(" ").toLowerCase()).toContain("aggregates");
    expect(result.reasoning.join(" ").toLowerCase()).toContain("distinct");
  });

  it("does not suggest selectDistinct when node has no aggregates", () => {
    const context = {
      hasMultipleSources: false,
      hasAggregates: false,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table" as const,
      columnPatterns: {
        timestamps: [],
        dates: [],
        businessKeys: [],
      },
    };

    const result = applyIntelligenceRules(context);

    expect(result.suggestions.selectDistinct).toBeUndefined();
  });

  it("suggests truncateBefore: false for table materialization", () => {
    const context = {
      hasMultipleSources: false,
      hasAggregates: false,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table" as const,
      columnPatterns: {
        timestamps: [],
        dates: [],
        businessKeys: [],
      },
    };

    const result = applyIntelligenceRules(context);

    expect(result.suggestions.truncateBefore).toBe(false);
    expect(result.reasoning.join(" ").toLowerCase()).toContain("table");
    expect(result.reasoning.join(" ").toLowerCase()).toContain("truncate");
  });

  it("does not suggest truncateBefore for view materialization", () => {
    const context = {
      hasMultipleSources: false,
      hasAggregates: false,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "view" as const,
      columnPatterns: {
        timestamps: [],
        dates: [],
        businessKeys: [],
      },
    };

    const result = applyIntelligenceRules(context);

    expect(result.suggestions.truncateBefore).toBeUndefined();
  });
});
