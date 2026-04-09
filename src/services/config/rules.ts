import type { NodeContext } from "./context-analyzer.js";

export interface IntelligenceRulesResult {
  suggestions: Record<string, unknown>;
  reasoning: string[];
}

export function applyIntelligenceRules(context: NodeContext): IntelligenceRulesResult {
  const suggestions: Record<string, unknown> = {};
  const reasoning: string[] = [];

  // Rule: Multi-source → insertStrategy (UNION vs UNION ALL based on aggregates)
  if (context.hasMultipleSources) {
    if (context.hasAggregates) {
      suggestions.insertStrategy = "UNION";
      reasoning.push(
        "Multi-source node with aggregates suggests UNION to avoid duplicate aggregated rows"
      );
    } else {
      suggestions.insertStrategy = "UNION ALL";
      reasoning.push(
        "Multi-source node without aggregates suggests UNION ALL for better performance"
      );
    }
  }

  // Rule: aggregates → selectDistinct: false
  if (context.hasAggregates) {
    suggestions.selectDistinct = false;
    reasoning.push(
      "Aggregates are incompatible with SELECT DISTINCT; suggests selectDistinct: false"
    );
  }

  // Rule: table materialization → truncateBefore: false (preserve data by default)
  if (context.materializationType === "table") {
    suggestions.truncateBefore = false;
    reasoning.push(
      "Table materialization suggests truncateBefore: false to preserve existing data"
    );
  }

  // Rule: view materialization → selectDistinct is often useful
  if (context.materializationType === "view" && !context.hasAggregates) {
    suggestions.selectDistinct = false;
    reasoning.push(
      "View without aggregates — selectDistinct defaults to false; set to true only if deduplication is needed"
    );
  }

  // Rule: Type 2 SCD pattern detected → suggest enableIf-dependent fields
  if (context.hasType2Pattern) {
    reasoning.push(
      "Type 2 SCD pattern detected (START_DATE + END_DATE + IS_CURRENT columns). " +
      "Verify that the node type's SCD config is set appropriately."
    );
  }

  // Rule: No timestamp/date columns in a table → note for auditing
  if (context.materializationType === "table" && !context.hasTimestampColumns) {
    reasoning.push(
      "No timestamp or date columns detected. Consider adding audit columns " +
      "(e.g., DW_LOAD_TS, DW_UPDATE_TS) for data lineage tracking."
    );
  }

  return {
    suggestions,
    reasoning,
  };
}
