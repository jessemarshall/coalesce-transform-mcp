/**
 * Pure functions for join analysis, SQL generation, and GROUP BY analysis.
 * No API calls — these operate on in-memory node data.
 */

import { isPlainObject, uniqueInOrder } from "../../utils.js";
import {
  getNodeColumnCount,
  getNodeColumnNames,
  normalizeColumnName,
} from "./node-inspection.js";

// ── Types ────────────────────────────────────────────────────────────────────

export type PredecessorSummary = {
  nodeID: string;
  nodeName: string | null;
  columnCount: number;
  columnNames: string[];
};

export type JoinColumnSuggestion = {
  normalizedName: string;
  leftColumnName: string;
  rightColumnName: string;
};

export type JoinSuggestion = {
  leftPredecessorNodeID: string;
  leftPredecessorName: string | null;
  rightPredecessorNodeID: string;
  rightPredecessorName: string | null;
  commonColumns: JoinColumnSuggestion[];
};

export type JoinClause = {
  type: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN";
  rightTable: string;
  rightTableAlias: string;
  onConditions: string[];
};

export type GroupByAnalysis = {
  groupByColumns: string[];
  aggregateColumns: { name: string; transform: string }[];
  hasAggregates: boolean;
  groupByClause: string;
  validation: {
    valid: boolean;
    errors: string[];
  };
};

export type ColumnTransform = {
  name: string;
  transform: string;
  dataType?: string;
  description?: string;
};

export type PredecessorRefInfo = {
  nodeID: string;
  nodeName: string;
  locationName: string;
  columnNames: string[];
};

// ── Functions ────────────────────────────────────────────────────────────────

export function buildPredecessorSummary(
  requestedNodeID: string,
  node: Record<string, unknown>
): PredecessorSummary {
  return {
    nodeID: requestedNodeID,
    nodeName: typeof node.name === "string" ? node.name : null,
    columnCount: getNodeColumnCount(node),
    columnNames: getNodeColumnNames(node),
  };
}

export function getReferencedPredecessorNodeIDs(
  node: Record<string, unknown>,
  predecessorNodeIDs: string[]
): string[] {
  const uniquePredecessorNodeIDs = uniqueInOrder(predecessorNodeIDs);
  const predecessorSet = new Set(uniquePredecessorNodeIDs);
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!Array.isArray(metadata?.columns)) {
    return [];
  }

  const referenced = new Set<string>();
  for (const column of metadata.columns) {
    if (!isPlainObject(column) || !Array.isArray(column.sources)) {
      continue;
    }
    for (const source of column.sources) {
      if (!isPlainObject(source) || !Array.isArray(source.columnReferences)) {
        continue;
      }
      for (const ref of source.columnReferences) {
        if (isPlainObject(ref) && typeof ref.nodeID === "string" && predecessorSet.has(ref.nodeID)) {
          referenced.add(ref.nodeID);
        }
      }
    }
  }

  return uniquePredecessorNodeIDs.filter((nodeID) => referenced.has(nodeID));
}

/**
 * Extract predecessor node IDs from a node's sourceMapping aliases
 * and column-level source references (fallback).
 *
 * In Coalesce, sourceMapping.dependencies[] has nodeName/locationName but NOT nodeID.
 * The nodeID is available in sourceMapping.aliases (name→nodeID map) and in
 * column sources[].columnReferences[].nodeID.
 */
export function extractPredecessorNodeIDs(metadata: Record<string, unknown>): string[] {
  const sourceMapping = Array.isArray(metadata.sourceMapping)
    ? metadata.sourceMapping
    : [];

  const ids = new Set<string>();

  // First: extract from aliases (alias → nodeID map)
  for (const mapping of sourceMapping) {
    if (isPlainObject(mapping) && isPlainObject(mapping.aliases)) {
      for (const nodeID of Object.values(mapping.aliases)) {
        if (typeof nodeID === "string" && nodeID.length > 0) {
          ids.add(nodeID);
        }
      }
    }
  }

  // Second: extract from column-level source references as fallback
  if (ids.size === 0 && Array.isArray(metadata.columns)) {
    for (const column of metadata.columns) {
      if (!isPlainObject(column) || !Array.isArray(column.sources)) continue;
      for (const source of column.sources) {
        if (!isPlainObject(source) || !Array.isArray(source.columnReferences)) continue;
        for (const ref of source.columnReferences) {
          if (isPlainObject(ref) && typeof ref.nodeID === "string") {
            ids.add(ref.nodeID);
          }
        }
      }
    }
  }

  return Array.from(ids);
}

export function extractPredecessorRefInfo(
  nodeID: string,
  node: Record<string, unknown>
): PredecessorRefInfo | null {
  const nodeName = typeof node.name === "string" ? node.name : null;
  const locationName = typeof node.locationName === "string" ? node.locationName : null;
  if (!nodeName || !locationName) return null;
  return {
    nodeID,
    nodeName,
    locationName,
    columnNames: getNodeColumnNames(node),
  };
}

export function buildJoinSuggestions(
  predecessors: PredecessorSummary[]
): JoinSuggestion[] {
  const suggestions: JoinSuggestion[] = [];

  for (let leftIndex = 0; leftIndex < predecessors.length; leftIndex += 1) {
    for (
      let rightIndex = leftIndex + 1;
      rightIndex < predecessors.length;
      rightIndex += 1
    ) {
      const left = predecessors[leftIndex];
      const right = predecessors[rightIndex];

      const leftColumns = new Map<string, string>();
      for (const columnName of left.columnNames) {
        const normalized = normalizeColumnName(columnName);
        if (!leftColumns.has(normalized)) {
          leftColumns.set(normalized, columnName);
        }
      }

      const rightColumns = new Map<string, string>();
      for (const columnName of right.columnNames) {
        const normalized = normalizeColumnName(columnName);
        if (!rightColumns.has(normalized)) {
          rightColumns.set(normalized, columnName);
        }
      }

      const commonColumns: JoinColumnSuggestion[] = [];
      for (const [normalizedName, leftColumnName] of leftColumns.entries()) {
        const rightColumnName = rightColumns.get(normalizedName);
        if (rightColumnName) {
          commonColumns.push({
            normalizedName,
            leftColumnName,
            rightColumnName,
          });
        }
      }

      commonColumns.sort((a, b) =>
        a.normalizedName.localeCompare(b.normalizedName)
      );

      suggestions.push({
        leftPredecessorNodeID: left.nodeID,
        leftPredecessorName: left.nodeName,
        rightPredecessorNodeID: right.nodeID,
        rightPredecessorName: right.nodeName,
        commonColumns,
      });
    }
  }

  return suggestions;
}

export function generateJoinSQL(
  joinSuggestions: JoinSuggestion[],
  joinType: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN" = "INNER JOIN"
): {
  fromClause: string;
  joinClauses: JoinClause[];
  fullSQL: string;
} {
  if (joinSuggestions.length === 0) {
    return {
      fromClause: "",
      joinClauses: [],
      fullSQL: "",
    };
  }

  const firstSuggestion = joinSuggestions[0];
  const leftTableName = firstSuggestion.leftPredecessorName || "LEFT_TABLE";
  const leftAlias = `"${leftTableName}"`;

  const fromClause = `FROM ${leftAlias}`;
  const joinClauses: JoinClause[] = [];
  const sqlParts: string[] = [fromClause];

  for (const suggestion of joinSuggestions) {
    const suggestionLeftName = suggestion.leftPredecessorName || "LEFT_TABLE";
    const suggestionLeftAlias = `"${suggestionLeftName}"`;
    const rightTableName = suggestion.rightPredecessorName || "RIGHT_TABLE";
    const rightAlias = `"${rightTableName}"`;

    const onConditions = suggestion.commonColumns.map(
      (col) =>
        `${suggestionLeftAlias}."${col.leftColumnName}" = ${rightAlias}."${col.rightColumnName}"`
    );

    const joinClause: JoinClause = {
      type: joinType,
      rightTable: rightTableName,
      rightTableAlias: rightAlias,
      onConditions,
    };

    joinClauses.push(joinClause);

    const joinSQL = [
      `${joinType} ${rightAlias}`,
      `  ON ${onConditions.join("\n  AND ")}`,
    ].join("\n");

    sqlParts.push(joinSQL);
  }

  return {
    fromClause,
    joinClauses,
    fullSQL: sqlParts.join("\n"),
  };
}

export function generateRefJoinSQL(
  predecessors: PredecessorRefInfo[],
  joinSuggestions: JoinSuggestion[],
  joinType: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN",
  joinColumnOverrides?: Array<{
    leftPredecessor: string;
    rightPredecessor: string;
    leftColumn: string;
    rightColumn: string;
  }>
): {
  fromClause: string;
  joinClauses: string[];
  fullSQL: string;
  warnings: string[];
} {
  if (predecessors.length === 0) {
    return { fromClause: "", joinClauses: [], fullSQL: "", warnings: [] };
  }

  const warnings: string[] = [];
  const primary = predecessors[0];
  const fromClause = `FROM {{ ref('${primary.locationName}', '${primary.nodeName}') }} "${primary.nodeName}"`;
  const joinClauses: string[] = [];

  // Build a lookup from nodeID → PredecessorRefInfo
  const predByID = new Map(predecessors.map((p) => [p.nodeID, p]));
  const predByName = new Map(predecessors.map((p) => [p.nodeName.toUpperCase(), p]));

  // Track which predecessors got joined
  const joinedPredecessors = new Set<string>([primary.nodeID]);

  for (const suggestion of joinSuggestions) {
    const right = predByID.get(suggestion.rightPredecessorNodeID)
      ?? predByName.get((suggestion.rightPredecessorName ?? "").toUpperCase());

    if (!right) continue;
    if (joinedPredecessors.has(right.nodeID)) continue; // Already joined — skip duplicate pair
    joinedPredecessors.add(right.nodeID);

    // Check for explicit overrides for this pair
    const overridesForPair = joinColumnOverrides?.filter(
      (o) =>
        (o.leftPredecessor === suggestion.leftPredecessorName ||
          o.leftPredecessor === suggestion.leftPredecessorNodeID) &&
        (o.rightPredecessor === suggestion.rightPredecessorName ||
          o.rightPredecessor === suggestion.rightPredecessorNodeID)
    );

    let onConditions: string[];
    if (overridesForPair && overridesForPair.length > 0) {
      onConditions = overridesForPair.map(
        (o) => `"${suggestion.leftPredecessorName}"."${o.leftColumn}" = "${right.nodeName}"."${o.rightColumn}"`
      );
    } else if (suggestion.commonColumns.length > 0) {
      onConditions = suggestion.commonColumns.map(
        (col) =>
          `"${suggestion.leftPredecessorName}"."${col.leftColumnName}" = "${right.nodeName}"."${col.rightColumnName}"`
      );
    } else {
      warnings.push(
        `No common columns between "${suggestion.leftPredecessorName}" and "${right.nodeName}". ` +
        `Provide joinColumnOverrides to specify the join keys explicitly.`
      );
      continue;
    }

    const clause = `${joinType} {{ ref('${right.locationName}', '${right.nodeName}') }} "${right.nodeName}"\n  ON ${onConditions.join("\n  AND ")}`;
    joinClauses.push(clause);
  }

  // Join any remaining predecessors that have explicit overrides but no joinSuggestion
  if (joinColumnOverrides) {
    for (const pred of predecessors) {
      if (joinedPredecessors.has(pred.nodeID)) continue;
      // Never join the primary predecessor to itself
      if (pred.nodeID === primary.nodeID) continue;

      // Find overrides where this predecessor is the right side
      const overridesForPred = joinColumnOverrides.filter(
        (o) =>
          o.rightPredecessor === pred.nodeName ||
          o.rightPredecessor === pred.nodeID ||
          o.rightPredecessor.toUpperCase() === pred.nodeName.toUpperCase()
      );

      if (overridesForPred.length > 0) {
        joinedPredecessors.add(pred.nodeID);

        const onConditions = overridesForPred.map((o) => {
          // Resolve the left predecessor name for the ON clause
          const leftPred = predecessors.find(
            (p) =>
              p.nodeName === o.leftPredecessor ||
              p.nodeID === o.leftPredecessor ||
              p.nodeName.toUpperCase() === o.leftPredecessor.toUpperCase()
          );
          const leftName = leftPred?.nodeName ?? o.leftPredecessor;
          return `"${leftName}"."${o.leftColumn}" = "${pred.nodeName}"."${o.rightColumn}"`;
        });

        const clause = `${joinType} {{ ref('${pred.locationName}', '${pred.nodeName}') }} "${pred.nodeName}"\n  ON ${onConditions.join("\n  AND ")}`;
        joinClauses.push(clause);
      }
    }
  }

  // Warn about predecessors that weren't joined
  for (const pred of predecessors) {
    if (!joinedPredecessors.has(pred.nodeID)) {
      warnings.push(
        `Predecessor "${pred.nodeName}" was not included in any join. ` +
        `It has no common columns with other predecessors. Provide joinColumnOverrides to specify the join keys.`
      );
    }
  }

  const fullSQL = [fromClause, ...joinClauses].join("\n");
  return { fromClause, joinClauses, fullSQL, warnings };
}

export function inferDatatype(transform: string): string | undefined {
  const upperTransform = transform.toUpperCase();

  // Date/Time functions - check these FIRST before MIN/MAX
  if (upperTransform.includes("DATEDIFF(")) return "NUMBER";
  if (upperTransform.includes("DATEADD(")) return "DATE";
  if (upperTransform.includes("CURRENT_DATE")) return "DATE";
  if (upperTransform.includes("CURRENT_TIMESTAMP")) return "TIMESTAMP_NTZ(9)";

  // Aggregate functions
  if (upperTransform.includes("COUNT(DISTINCT")) return "NUMBER";
  if (upperTransform.includes("COUNT(")) return "NUMBER";
  if (upperTransform.includes("SUM(")) return "NUMBER(38,4)";
  if (upperTransform.includes("AVG(")) return "NUMBER(38,4)";
  if (upperTransform.includes("STDDEV(")) return "NUMBER(38,4)";
  if (upperTransform.includes("VARIANCE(")) return "NUMBER(38,4)";

  // MIN/MAX with timestamp/date context
  if (upperTransform.includes("MIN(") && upperTransform.includes("_TS"))
    return "TIMESTAMP_NTZ(9)";
  if (upperTransform.includes("MAX(") && upperTransform.includes("_TS"))
    return "TIMESTAMP_NTZ(9)";
  if (upperTransform.includes("MIN(") && upperTransform.includes("_DATE"))
    return "DATE";
  if (upperTransform.includes("MAX(") && upperTransform.includes("_DATE"))
    return "DATE";

  // String functions
  if (upperTransform.includes("CONCAT(")) return "VARCHAR";
  if (upperTransform.includes("UPPER(")) return "VARCHAR";
  if (upperTransform.includes("LOWER(")) return "VARCHAR";
  if (upperTransform.includes("TRIM(")) return "VARCHAR";
  if (upperTransform.includes("SUBSTR(")) return "VARCHAR";
  if (upperTransform.includes("LEFT(")) return "VARCHAR";
  if (upperTransform.includes("RIGHT(")) return "VARCHAR";

  // Boolean
  if (upperTransform.includes("CASE")) return "VARCHAR";

  // Window functions
  if (upperTransform.includes("ROW_NUMBER()")) return "NUMBER";
  if (upperTransform.includes("RANK()")) return "NUMBER";
  if (upperTransform.includes("DENSE_RANK()")) return "NUMBER";

  return undefined;
}

export function analyzeColumnsForGroupBy(
  columns: ColumnTransform[]
): GroupByAnalysis {
  const aggregateFunctions = [
    "COUNT(",
    "SUM(",
    "AVG(",
    "MIN(",
    "MAX(",
    "STDDEV(",
    "VARIANCE(",
    "LISTAGG(",
    "ARRAY_AGG(",
  ];

  const windowFunctions = [
    "ROW_NUMBER()",
    "RANK()",
    "DENSE_RANK()",
    "LEAD(",
    "LAG(",
    "FIRST_VALUE(",
    "LAST_VALUE(",
  ];

  const groupByColumns: string[] = [];
  const aggregateColumns: { name: string; transform: string }[] = [];
  const errors: string[] = [];

  for (const col of columns) {
    const upperTransform = col.transform.toUpperCase();

    const isAggregate = aggregateFunctions.some((fn) =>
      upperTransform.includes(fn)
    );
    const isWindow = windowFunctions.some((fn) => upperTransform.includes(fn));

    if (isAggregate || isWindow) {
      aggregateColumns.push({ name: col.name, transform: col.transform });
    } else {
      // This is a non-aggregate column, needs to be in GROUP BY
      groupByColumns.push(col.transform);
    }
  }

  const hasAggregates = aggregateColumns.length > 0;

  // Non-aggregate columns are always captured in groupByColumns, so a mix of aggregates
  // and bare columns is automatically handled by the GROUP BY clause generation above.
  // Pure-aggregate queries (all columns are aggregates, groupByColumns empty) are valid SQL.
  const valid = errors.length === 0;

  const groupByClause =
    hasAggregates && groupByColumns.length > 0
      ? `GROUP BY ${groupByColumns.join(", ")}`
      : "";

  return {
    groupByColumns,
    aggregateColumns,
    hasAggregates,
    groupByClause,
    validation: {
      valid,
      errors,
    },
  };
}

/**
 * Generate a FROM clause from a sourceMapping entry's dependencies.
 * Returns null if no dependencies are available.
 */
function generateFromClauseFromDependencies(
  sourceMappingEntry: Record<string, unknown>
): string | null {
  const deps = Array.isArray(sourceMappingEntry.dependencies) ? sourceMappingEntry.dependencies : [];
  if (deps.length === 0) return null;

  const dep = deps[0];
  if (!isPlainObject(dep) || typeof dep.nodeName !== "string") return null;

  const locName = typeof dep.locationName === "string" ? dep.locationName : "";
  return locName
    ? `FROM {{ ref('${locName}', '${dep.nodeName}') }} "${dep.nodeName}"`
    : `FROM {{ ref('${dep.nodeName}') }} "${dep.nodeName}"`;
}

/**
 * Ensure the FROM clause exists in sourceMapping for single-predecessor nodes.
 * After column replacement, the Coalesce API may strip the auto-populated
 * joinCondition. This function regenerates it from dependencies.
 */
export function ensureFromClauseInSourceMapping(
  body: Record<string, unknown>
): void {
  const metadata = isPlainObject(body.metadata) ? body.metadata : null;
  if (!metadata) return;

  const sourceMapping = Array.isArray(metadata.sourceMapping) ? metadata.sourceMapping : [];
  if (sourceMapping.length === 0) return;

  const first = sourceMapping[0];
  if (!isPlainObject(first)) return;

  const join = isPlainObject(first.join) ? first.join : {};
  const existing = typeof join.joinCondition === "string" ? join.joinCondition.trim() : "";

  if (existing) return;

  const fromClause = generateFromClauseFromDependencies(first);
  if (fromClause) {
    first.join = { ...join, joinCondition: fromClause };
  }
}

/**
 * Append a WHERE condition to the existing joinCondition in the first sourceMapping entry.
 * The FROM/JOIN clause from node creation is preserved — only the WHERE is added.
 * If no existing joinCondition exists, generates a FROM clause from dependencies first.
 */
export function appendWhereToJoinCondition(
  body: Record<string, unknown>,
  whereCondition: string
): void {
  const metadata = isPlainObject(body.metadata) ? body.metadata : null;
  if (!metadata) return;

  const sourceMapping = Array.isArray(metadata.sourceMapping) ? metadata.sourceMapping : [];
  if (sourceMapping.length === 0) return;

  const first = sourceMapping[0];
  if (!isPlainObject(first)) return;

  const join = isPlainObject(first.join) ? { ...first.join } : {};
  let existing = typeof join.joinCondition === "string" ? join.joinCondition.trim() : "";

  // If joinCondition is empty but dependencies exist, generate the FROM clause
  // using the shared helper.
  if (!existing) {
    existing = generateFromClauseFromDependencies(first) ?? "";
  }

  // Strip backslash-escaped quotes (agents sometimes over-escape: \" → ")
  const unescaped = whereCondition.replace(/\\"/g, '"');
  // Normalize: strip leading "WHERE" if the user included it
  const cleanWhere = unescaped.replace(/^\s*WHERE\s+/i, "").trim();
  if (!cleanWhere) return;

  if (existing) {
    // Append WHERE to existing FROM/JOIN clause
    // Check if existing already has a WHERE — if so, add with AND
    if (/\bWHERE\b/i.test(existing)) {
      join.joinCondition = `${existing}\n  AND ${cleanWhere}`;
    } else {
      join.joinCondition = `${existing}\nWHERE ${cleanWhere}`;
    }
  } else {
    join.joinCondition = `WHERE ${cleanWhere}`;
  }
  first.join = join;
}
