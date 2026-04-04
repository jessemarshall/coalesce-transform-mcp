import type {
  PlannedSelectItem,
  PipelinePlan,
  ParsedSqlSourceRef,
  CteNodeSummary,
  SqlParseResult,
} from "./planning-types.js";
import {
  isIdentifierChar,
  stripIdentifierQuotes,
  findTopLevelKeywordIndex,
  scanTopLevel,
  splitTopLevel,
  tokenizeTopLevelWhitespace,
  skipSqlTrivia,
  matchesKeywordAt,
  findClosingParen,
  extractParenBody,
} from "./sql-tokenizer.js";
import {
  type PipelineNodeTypeSelection,
} from "./node-type-selection.js";

export function normalizeSqlIdentifier(identifier: string): string {
  return identifier.trim().replace(/^["`[]|["`\]]$/g, "").toUpperCase();
}

export function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

export function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

export function buildSourceDependencyKey(
  locationName: string | null | undefined,
  nodeName: string
): string {
  return `${normalizeSqlIdentifier(locationName ?? "")}::${normalizeSqlIdentifier(nodeName)}`;
}

export function getUniqueSourceDependencies(
  sourceRefs: Array<{ locationName: string; nodeName: string }>
): Array<{ locationName: string; nodeName: string }> {
  const seen = new Set<string>();
  const dependencies: Array<{ locationName: string; nodeName: string }> = [];

  for (const ref of sourceRefs) {
    const key = buildSourceDependencyKey(ref.locationName, ref.nodeName);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    dependencies.push({
      locationName: ref.locationName,
      nodeName: ref.nodeName,
    });
  }

  return dependencies;
}

export function extractSelectClause(sql: string): string | null {
  const selectIndex = findTopLevelKeywordIndex(sql, "select");
  if (selectIndex < 0) {
    return null;
  }
  const fromIndex = findTopLevelKeywordIndex(sql, "from", selectIndex + 6);
  if (fromIndex < 0) {
    return null;
  }
  return sql.slice(selectIndex + 6, fromIndex).trim();
}

export function extractFromClause(sql: string): string | null {
  const selectIndex = findTopLevelKeywordIndex(sql, "select");
  if (selectIndex < 0) {
    return null;
  }
  const fromIndex = findTopLevelKeywordIndex(sql, "from", selectIndex + 6);
  if (fromIndex < 0) {
    return null;
  }
  return sql
    .slice(fromIndex)
    .trim()
    .replace(/;+\s*$/u, "");
}

/** Keywords that terminate a source segment in a FROM clause. */
const SOURCE_SEGMENT_TERMINATORS = [
  "join", "left", "right", "inner", "full", "cross", "natural", "lateral",
  "on", "using",
  "where", "group", "order", "having", "limit", "qualify",
  "union", "intersect", "except", "window", "fetch",
];

function findTerminatorKeyword(value: string, index: number): string | null {
  for (const keyword of SOURCE_SEGMENT_TERMINATORS) {
    if (matchesKeywordAt(value, index, keyword)) {
      return keyword;
    }
  }
  return null;
}

function extractTopLevelSourceSegments(
  fromClause: string
): Array<{ text: string; relationStart: number; relationEnd: number }> {
  const segments: Array<{ text: string; relationStart: number; relationEnd: number }> = [];
  let captureStart: number | null = null;

  const pushSegment = (endIndex: number) => {
    if (captureStart === null) {
      return;
    }
    let trimmedEnd = endIndex;
    while (trimmedEnd > captureStart && /\s/u.test(fromClause[trimmedEnd - 1] ?? "")) {
      trimmedEnd -= 1;
    }
    if (trimmedEnd > captureStart) {
      segments.push({
        text: fromClause.slice(captureStart, trimmedEnd),
        relationStart: captureStart,
        relationEnd: trimmedEnd,
      });
    }
  };

  scanTopLevel(fromClause, (char, index, parenDepth) => {
    if (parenDepth !== 0) {
      return true;
    }

    if (captureStart === null) {
      if (matchesKeywordAt(fromClause, index, "from")) {
        captureStart = skipSqlTrivia(fromClause, index + 4);
      } else if (matchesKeywordAt(fromClause, index, "join")) {
        captureStart = skipSqlTrivia(fromClause, index + 4);
      } else if (char === ",") {
        captureStart = skipSqlTrivia(fromClause, index + 1);
      }
      return true;
    }

    if (char === ",") {
      pushSegment(index);
      captureStart = skipSqlTrivia(fromClause, index + 1);
      return true;
    }

    const terminator = findTerminatorKeyword(fromClause, index);
    if (terminator) {
      pushSegment(index);
      captureStart =
        terminator === "join"
          ? skipSqlTrivia(fromClause, index + terminator.length)
          : null;
    }

    return true;
  });

  pushSegment(fromClause.length);
  return segments;
}

function isSupportedIdentifierToken(token: string): boolean {
  return (
    /^[A-Za-z_][\w$]*$/u.test(token) ||
    /^"[^"]+"$/u.test(token) ||
    /^`[^`]+`$/u.test(token) ||
    /^\[[^\]]+\]$/u.test(token)
  );
}

function parseSqlSourceSegment(
  segment: { text: string; relationStart: number; relationEnd: number }
): ParsedSqlSourceRef | null {
  const relationOffset = skipSqlTrivia(segment.text, 0);
  if (relationOffset >= segment.text.length) {
    return null;
  }

  let relationText: string;
  let relationTokenStart: number;
  let relationTokenEnd: number;
  let aliasTokens: string[];

  if (segment.text.slice(relationOffset).startsWith("{{")) {
    const closingIndex = segment.text.indexOf("}}", relationOffset);
    if (closingIndex < 0) {
      return null;
    }

    relationTokenStart = relationOffset;
    relationTokenEnd = closingIndex + 2;
    relationText = segment.text.slice(relationTokenStart, relationTokenEnd).trim();
    aliasTokens = tokenizeTopLevelWhitespace(segment.text.slice(relationTokenEnd)).map(
      (token) => token.text
    );
  } else {
    const tokens = tokenizeTopLevelWhitespace(segment.text);
    if (tokens.length === 0) {
      return null;
    }

    const relationToken = tokens[0]!;
    relationText = relationToken.text;
    relationTokenStart = relationToken.start;
    relationTokenEnd = relationToken.end;
    aliasTokens = tokens.slice(1).map((token) => token.text);
  }

  const alias =
    aliasTokens[0]?.toLowerCase() === "as"
      ? (aliasTokens[1] ? stripIdentifierQuotes(aliasTokens[1]) : null)
      : aliasTokens[0]
        ? stripIdentifierQuotes(aliasTokens[0])
        : null;

  const refMatch = relationText.match(
    /^\{\{\s*ref\(\s*(['"])([^'"]+)\1\s*,\s*(['"])([^'"]+)\3\s*\)\s*\}\}$/iu
  );
  if (refMatch) {
    return {
      locationName: refMatch[2] ?? "",
      nodeName: refMatch[4] ?? "",
      alias,
      nodeID: null,
      sourceStyle: "coalesce_ref",
      locationCandidates: refMatch[2] ? [refMatch[2]] : [],
      relationStart: segment.relationStart + relationTokenStart,
      relationEnd: segment.relationStart + relationTokenEnd,
    };
  }

  if (relationText.startsWith("(")) {
    return null;
  }

  const parts = splitTopLevel(relationText, ".").map((part) => part.trim());
  if (
    parts.length === 0 ||
    parts.some((part) => part.length === 0 || !isSupportedIdentifierToken(part))
  ) {
    return null;
  }

  const normalizedParts = parts.map(stripIdentifierQuotes);
  const nodeName = normalizedParts[normalizedParts.length - 1] ?? "";

  return {
    locationName: "",
    nodeName,
    alias,
    nodeID: null,
    sourceStyle: "table_name",
    locationCandidates: normalizedParts.slice(0, -1).reverse(),
    relationStart: segment.relationStart + relationTokenStart,
    relationEnd: segment.relationStart + relationTokenEnd,
  };
}

type SqlSourceParseResult = {
  fromClause: string;
  refs: ParsedSqlSourceRef[];
};

export function parseSqlSourceRefs(sql: string): SqlSourceParseResult {
  const fromClause = extractFromClause(sql);
  if (!fromClause) {
    return { fromClause: "", refs: [] };
  }

  const refs = extractTopLevelSourceSegments(fromClause)
    .map(parseSqlSourceSegment)
    .filter((ref): ref is ParsedSqlSourceRef => ref !== null);

  return { fromClause, refs };
}

export function splitExpressionAlias(rawItem: string): { expression: string; outputName: string | null } {
  const asMatch = rawItem.match(
    /^(.*?)(?:\s+AS\s+)([A-Za-z_][\w$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])$/i
  );
  if (asMatch) {
    return {
      expression: asMatch[1]?.trim() ?? rawItem.trim(),
      outputName: stripIdentifierQuotes(asMatch[2] ?? ""),
    };
  }

  const bareAliasMatch = rawItem.match(
    /^(.*?)(?:\s+)([A-Za-z_][\w$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])$/
  );
  if (bareAliasMatch) {
    const candidateExpression = bareAliasMatch[1]?.trim() ?? rawItem.trim();
    if (candidateExpression.includes(".") || candidateExpression.includes("(")) {
      return {
        expression: candidateExpression,
        outputName: stripIdentifierQuotes(bareAliasMatch[2] ?? ""),
      };
    }
  }

  return {
    expression: rawItem.trim(),
    outputName: null,
  };
}

function parseDirectColumnExpression(expression: string): {
  sourceNodeAlias: string | null;
  sourceColumnName: string;
} | null {
  const trimmed = expression.trim();
  if (trimmed === "*") {
    return null;
  }

  const parts = splitTopLevel(trimmed, ".").map((part) => part.trim());
  if (
    parts.length === 0 ||
    parts.some((part) => part.length === 0 || !isSupportedIdentifierToken(part))
  ) {
    return null;
  }

  return {
    sourceNodeAlias:
      parts.length >= 2 ? stripIdentifierQuotes(parts[parts.length - 2] ?? "") : null,
    sourceColumnName: stripIdentifierQuotes(parts[parts.length - 1] ?? ""),
  };
}

function parseWildcardExpression(expression: string): {
  sourceNodeAlias: string | null;
} | null {
  const trimmed = expression.trim();
  if (trimmed === "*") {
    return { sourceNodeAlias: null };
  }
  const parts = splitTopLevel(trimmed, ".").map((part) => part.trim());
  if (
    parts.length < 2 ||
    parts[parts.length - 1] !== "*" ||
    parts.slice(0, -1).some((part) => part.length === 0 || !isSupportedIdentifierToken(part))
  ) {
    return null;
  }
  return {
    sourceNodeAlias: stripIdentifierQuotes(parts[parts.length - 2] ?? ""),
  };
}

function listToQuestion(values: string[]): string {
  return values.join(", ");
}

export function parseSqlSelectItems(sql: string, refs: ParsedSqlSourceRef[]): SqlParseResult {
  const warnings: string[] = [];
  const refsByAlias = new Map<string, ParsedSqlSourceRef>();
  for (const ref of refs) {
    refsByAlias.set(normalizeSqlIdentifier(ref.alias ?? ref.nodeName), ref);
  }

  const selectClause = extractSelectClause(sql);
  if (!selectClause) {
    return {
      refs,
      selectItems: [],
      warnings: ["Could not find a top-level SELECT ... FROM clause in the SQL."],
    };
  }

  const rawItems = splitTopLevel(selectClause, ",");
  const selectItems: PlannedSelectItem[] = [];

  for (const rawItem of rawItems) {
    const { expression, outputName } = splitExpressionAlias(rawItem);
    const wildcard = parseWildcardExpression(expression);
    if (wildcard) {
      if (wildcard.sourceNodeAlias === null && refs.length !== 1) {
        selectItems.push({
          expression,
          outputName: null,
          sourceNodeAlias: null,
          sourceNodeName: null,
          sourceNodeID: null,
          sourceColumnName: null,
          kind: "expression",
          supported: false,
          reason: "Unqualified * is only supported when exactly one predecessor ref is present.",
        });
        continue;
      }

      const ref =
        wildcard.sourceNodeAlias === null
          ? refs[0] ?? null
          : refsByAlias.get(normalizeSqlIdentifier(wildcard.sourceNodeAlias)) ?? null;
      if (!ref) {
        selectItems.push({
          expression,
          outputName: null,
          sourceNodeAlias: wildcard.sourceNodeAlias,
          sourceNodeName: null,
          sourceNodeID: null,
          sourceColumnName: null,
          kind: "expression",
          supported: false,
          reason: "Wildcard source alias could not be resolved to a predecessor ref.",
        });
        continue;
      }

      // Wildcards are expanded later after predecessor nodes are fetched.
      selectItems.push({
        expression,
        outputName: null,
        sourceNodeAlias: wildcard.sourceNodeAlias ?? ref.alias ?? ref.nodeName,
        sourceNodeName: ref.nodeName,
        sourceNodeID: ref.nodeID,
        sourceColumnName: "*",
        kind: "expression",
        supported: true,
      });
      continue;
    }

    const directColumn = parseDirectColumnExpression(expression);
    if (!directColumn) {
      // Expression is not a direct column reference - it's a computed expression
      // Support it if it has an output name (alias)
      if (outputName === null) {
        selectItems.push({
          expression,
          outputName: null,
          sourceNodeAlias: null,
          sourceNodeName: null,
          sourceNodeID: null,
          sourceColumnName: null,
          kind: "expression",
          supported: false,
          reason: "Computed expressions require an alias (e.g., CASE ... END AS column_name)",
        });
        continue;
      }

      // Computed expression with alias - supported
      selectItems.push({
        expression,
        outputName,
        sourceNodeAlias: null,
        sourceNodeName: null,
        sourceNodeID: null,
        sourceColumnName: null,
        kind: "expression",
        supported: true,
      });
      continue;
    }

    const ref =
      directColumn.sourceNodeAlias === null
        ? refs.length === 1
          ? refs[0] ?? null
          : null
        : refsByAlias.get(normalizeSqlIdentifier(directColumn.sourceNodeAlias)) ?? null;
    if (!ref) {
      selectItems.push({
        expression,
        outputName: outputName ?? directColumn.sourceColumnName,
        sourceNodeAlias: directColumn.sourceNodeAlias,
        sourceNodeName: null,
        sourceNodeID: null,
        sourceColumnName: directColumn.sourceColumnName,
        kind: "column",
        supported: false,
        reason:
          directColumn.sourceNodeAlias === null
            ? "Unqualified columns are only supported when exactly one predecessor ref is present."
            : `The source alias ${directColumn.sourceNodeAlias} did not match a predecessor ref.`,
      });
      continue;
    }

    selectItems.push({
      expression,
      outputName: outputName ?? directColumn.sourceColumnName,
      sourceNodeAlias: directColumn.sourceNodeAlias ?? ref.alias ?? ref.nodeName,
      sourceNodeName: ref.nodeName,
      sourceNodeID: ref.nodeID,
      sourceColumnName: directColumn.sourceColumnName,
      kind: "column",
      supported: true,
    });
  }

  if (selectItems.length === 0) {
    warnings.push("The SQL SELECT clause did not produce any supported projected columns.");
  }

  return { refs, selectItems, warnings };
}

/**
 * Parsed CTE with name and body SQL.
 */
export type ParsedCte = {
  name: string;
  body: string;
  columns: CteColumn[];
  whereClause: string | null;
  sourceTable: string | null;
  hasGroupBy: boolean;
  hasJoin: boolean;
};

export type CteColumn = {
  outputName: string;
  expression: string;
  isTransform: boolean;
};

/**
 * Extract CTEs with their bodies from SQL.
 * Uses quoting-aware scanning to find CTE headers and balanced parentheses,
 * avoiding false matches inside string literals, quoted identifiers, and comments.
 */
export function extractCtes(sql: string): ParsedCte[] {
  const trimmed = sql.trim();

  // Check for leading WITH keyword using quoting-aware search
  const withIdx = findTopLevelKeywordIndex(trimmed, "WITH");
  if (withIdx !== 0) return [];

  const ctes: ParsedCte[] = [];
  // Scan for CTE definitions: name AS ( ... )
  // After WITH, and after each CTE body followed by a comma, look for: identifier AS (
  let cursor = withIdx + 4; // skip past "WITH"

  while (cursor < trimmed.length) {
    // Skip whitespace and commas between CTEs
    const rest = trimmed.slice(cursor);
    const leadingMatch = rest.match(/^[\s,]+/);
    if (leadingMatch) cursor += leadingMatch[0].length;
    if (cursor >= trimmed.length) break;

    // Try to match: identifier AS (
    // identifier can be unquoted, double-quoted, backtick-quoted, or bracket-quoted
    const headerMatch = trimmed.slice(cursor).match(
      /^([A-Za-z_][\w$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])\s+AS\s*\(/i
    );
    if (!headerMatch) break; // No more CTE headers — rest is the final SELECT

    const rawName = stripIdentifierQuotes(headerMatch[1]!);
    const name = rawName.toUpperCase();
    const bodyStart = cursor + headerMatch[0].length;
    const body = extractParenBody(trimmed, bodyStart);

    const closeIdx = findClosingParen(trimmed, bodyStart);
    if (closeIdx >= 0) {
      const body = trimmed.slice(bodyStart, closeIdx).trim();
      const columns = parseCteColumns(body);
      const whereClause = extractCteWhereClause(body);
      const sourceTable = extractCteSourceTable(body);
      const hasGroupBy = findTopLevelKeywordIndex(body, "GROUP") >= 0;
      const hasJoin = findTopLevelKeywordIndex(body, "JOIN") >= 0;
      ctes.push({ name, body, columns, whereClause, sourceTable, hasGroupBy, hasJoin });
      // Move cursor past the closing paren
      cursor = closeIdx + 1;
    } else {
      ctes.push({ name, body: "", columns: [], whereClause: null, sourceTable: null, hasGroupBy: false, hasJoin: false });
      break;
    }
  }

  return ctes;
}

/**
 * Parse a CTE body's SELECT list into columns with transform detection.
 *
 * Handles `SELECT * FROM (subquery) WHERE ...` by recursing into the subquery.
 */
function parseCteColumns(body: string): CteColumn[] {
  const selectClause = extractSelectClause(body);
  if (!selectClause) return [];

  const rawItems = splitTopLevel(selectClause, ",");

  // Detect "SELECT * FROM (subquery)" — recurse into the subquery
  if (rawItems.length === 1 && /^\*$/.test(rawItems[0]!.trim())) {
    const subqueryBody = extractSubqueryFromFrom(body);
    if (subqueryBody) {
      return parseCteColumns(subqueryBody);
    }
    return [];
  }

  const columns: CteColumn[] = [];

  for (const rawItem of rawItems) {
    const { expression, outputName } = splitExpressionAlias(rawItem);
    const trimmedExpr = expression.trim();

    // Skip wildcards
    if (/^\*$/.test(trimmedExpr) || /\.\*$/.test(trimmedExpr)) continue;

    const bareColName = extractBareColumnName(trimmedExpr)?.toUpperCase() ?? null;
    const colName = (outputName?.toUpperCase() ?? bareColName);
    if (!colName) continue;

    // Detect transforms: anything that isn't a simple column reference,
    // OR a column rename (AS alias differs from the source column name).
    // Renames need a transform so preserveColumnLinkage can match by the NEW name
    // and propagate the expression into sources[*].transform.
    const isRename = outputName !== null && bareColName !== null && outputName.toUpperCase() !== bareColName;
    const isTransform = !isSimpleColumnRef(trimmedExpr) || isRename;

    columns.push({
      outputName: colName,
      expression: trimmedExpr,
      isTransform,
    });
  }

  return columns;
}

/**
 * Extract the subquery body from `FROM (subquery)`.
 * Returns the SQL inside the parentheses, or null if FROM doesn't start with a subquery.
 */
function extractSubqueryFromFrom(sql: string): string | null {
  const fromIndex = findTopLevelKeywordIndex(sql, "from");
  if (fromIndex < 0) return null;
  const afterFrom = sql.slice(fromIndex + 4).trimStart();
  if (!afterFrom.startsWith("(")) return null;
  return extractParenBody(afterFrom, 1);
}

/**
 * Check if an expression is a simple column reference (no transform needed).
 * Simple: `col`, `"col"`, `table.col`, `table."col"`, `"table"."col"`
 */
function isSimpleColumnRef(expr: string): boolean {
  // Simple: identifier or qualified identifier (with optional quotes)
  return /^(?:[A-Za-z_][\w$]*|"[^"]+")(?:\.(?:[A-Za-z_][\w$]*|"[^"]+"))?$/.test(expr.trim());
}

/**
 * Extract a bare column name from a simple reference like `table.col` or `col`.
 */
function extractBareColumnName(expr: string): string | null {
  const match = expr.trim().match(/(?:.*\.)?([A-Za-z_][\w$]*|"[^"]+")$/);
  if (!match?.[1]) return null;
  return stripIdentifierQuotes(match[1]);
}

/**
 * Extract WHERE clause from a CTE body (ignoring subqueries).
 * Uses quoting-aware keyword search to avoid matching inside strings or comments.
 */
function extractCteWhereClause(body: string): string | null {
  const whereIdx = findTopLevelKeywordIndex(body, "WHERE");
  if (whereIdx < 0) return null;

  const afterWhere = whereIdx + 5; // "WHERE".length
  // Find the first clause terminator after WHERE
  const terminators = ["GROUP", "ORDER", "HAVING", "LIMIT", "QUALIFY"] as const;
  let endIdx = body.length;
  for (const kw of terminators) {
    const idx = findTopLevelKeywordIndex(body, kw, afterWhere);
    if (idx >= 0 && idx < endIdx) {
      endIdx = idx;
    }
  }

  const clause = body.slice(afterWhere, endIdx).trim();
  return clause || null;
}

const AGGREGATE_FUNCTIONS = new Set([
  "COUNT", "SUM", "AVG", "MIN", "MAX",
  "LISTAGG", "ARRAY_AGG", "MEDIAN", "MODE",
  "STDDEV", "VARIANCE", "ANY_VALUE",
  "COUNT_IF", "SUM_IF", "AVG_IF",
  "APPROX_COUNT_DISTINCT", "HLL",
]);

function isAggregateFn(name: string): boolean {
  return AGGREGATE_FUNCTIONS.has(name.toUpperCase());
}

/**
 * Extract the main source table from a CTE body's FROM clause.
 * Uses quoting-aware keyword search to avoid matching FROM inside strings or comments.
 */
function extractCteSourceTable(body: string): string | null {
  const fromIdx = findTopLevelKeywordIndex(body, "FROM");
  if (fromIdx < 0) return null;

  const afterFrom = body.slice(fromIdx + 4).trimStart();
  const tableMatch = afterFrom.match(/^([A-Za-z_][\w$.]*(?:\.[A-Za-z_][\w$]*)*)/);
  return tableMatch?.[1]?.toUpperCase() ?? null;
}

/**
 * Classify a CTE's pattern to pick the right node type.
 */
function classifyCtePattern(cte: ParsedCte): "staging" | "multiSource" | "aggregation" {
  if (cte.hasGroupBy) return "aggregation";
  if (cte.hasJoin) return "multiSource";
  return "staging";
}

/**
 * Build a per-CTE instruction block that tells the agent exactly what transforms
 * and filters to apply for this CTE.
 */
function buildCteNodeInstruction(cte: ParsedCte, nodeType: string): string {
  const lines: string[] = [];
  lines.push(`## ${cte.name}`);
  lines.push(`- nodeType: "${nodeType}"`);

  if (cte.sourceTable) {
    lines.push(`- source: ${cte.sourceTable}`);
  }

  const transforms = cte.columns.filter((c) => c.isTransform);
  const passthroughCols = cte.columns.filter((c) => !c.isTransform);

  if (cte.hasGroupBy) {
    lines.push(`- AGGREGATION NODE: pass groupByColumns + aggregates directly to create_workspace_node_from_predecessor (single call)`);
  } else if (cte.columns.length > 0) {
    lines.push(`- Pass columns array + whereCondition directly to create_workspace_node_from_predecessor (single call)`);
  }

  if (transforms.length > 0) {
    lines.push(`- Column transforms:`);
    for (const col of transforms) {
      lines.push(`  - ${col.outputName}: ${col.expression}`);
    }
  }

  if (passthroughCols.length > 0) {
    lines.push(`- Passthrough columns: ${passthroughCols.map((c) => c.outputName).join(", ")}`);
  }

  if (cte.columns.length > 0) {
    lines.push(`- ONLY keep these ${cte.columns.length} columns: ${cte.columns.map((c) => c.outputName).join(", ")}`);
  }

  if (cte.whereClause) {
    lines.push(`- WHERE filter (pass as whereCondition — do NOT construct {{ ref() }}): ${cte.whereClause}`);
  }

  if (cte.hasJoin) {
    lines.push(`- Has JOIN — use apply_join_condition or update_workspace_node for join setup`);
  }

  return lines.join("\n");
}

/**
 * When the user's SQL contains CTEs, return a plan that instructs the agent
 * to break each CTE into a separate Coalesce node using the declarative tools.
 * CTEs are not supported in Coalesce — each CTE should be its own node.
 *
 * The plan includes per-CTE structured data: column transforms, WHERE clauses,
 * source tables, and which columns to keep/remove.
 */
export function buildCtePlan(
  params: {
    workspaceID: string;
    goal?: string;
    sql?: string;
    targetName?: string;
  },
  ctes: ParsedCte[],
  nodeTypeSelections: {
    staging: PipelineNodeTypeSelection;
    multiSource: PipelineNodeTypeSelection;
    aggregation: PipelineNodeTypeSelection;
  }
): PipelinePlan {
  const stagingType = nodeTypeSelections.staging.selectedNodeType ?? "Stage";
  const multiSourceType = nodeTypeSelections.multiSource.selectedNodeType ?? stagingType;
  const aggregationType = nodeTypeSelections.aggregation.selectedNodeType ?? stagingType;

  const typeMap: Record<string, string> = {
    staging: stagingType,
    multiSource: multiSourceType,
    aggregation: aggregationType,
  };

  // Build per-CTE instructions
  const cteInstructions: string[] = [];
  for (const cte of ctes) {
    const pattern = classifyCtePattern(cte);
    const nodeType = typeMap[pattern]!;
    cteInstructions.push(buildCteNodeInstruction(cte, nodeType));
  }

  // Detect if any CTE references another CTE (pipeline dependency)
  const cteNameSet = new Set(ctes.map((c) => c.name));
  const cteDependencies: string[] = [];
  for (const cte of ctes) {
    const deps = ctes
      .filter((other) => other.name !== cte.name && cte.body.toUpperCase().includes(other.name))
      .map((other) => other.name);
    if (deps.length > 0) {
      cteDependencies.push(`${cte.name} depends on: ${deps.join(", ")}`);
    }
  }

  // Detect the final SELECT after all CTEs
  const finalSelectNote = extractFinalSelectFromCteQuery(params.sql ?? "", cteNameSet);

  const allTransformCount = ctes.reduce(
    (sum, cte) => sum + cte.columns.filter((c) => c.isTransform).length,
    0
  );
  const allFilterCount = ctes.filter((c) => c.whereClause).length;

  // Build structured per-CTE summary for easy agent consumption
  // Includes columnsParam / groupByColumnsParam / aggregatesParam for single-call creation
  const cteNodeSummary: CteNodeSummary[] = ctes.map((cte) => {
    const pattern = classifyCtePattern(cte);
    const nodeType = typeMap[pattern]!;
    const transforms = cte.columns.filter((c) => c.isTransform);

    const summary: CteNodeSummary = {
      name: cte.name,
      nodeType,
      pattern,
      sourceTable: cte.sourceTable,
      columnCount: cte.columns.length,
      transforms: transforms.map((c) => ({ column: c.outputName, expression: c.expression })),
      passthroughColumns: cte.columns.filter((c) => !c.isTransform).map((c) => c.outputName),
      whereFilter: cte.whereClause,
      hasGroupBy: cte.hasGroupBy,
      hasJoin: cte.hasJoin,
      dependsOn: ctes
        .filter((other) => other.name !== cte.name && new RegExp(`\\b${escapeRegExp(other.name)}\\b`, "iu").test(cte.body))
        .map((other) => other.name),
    };

    // Add structured params for single-call creation
    if (cte.hasGroupBy && cte.columns.length > 0) {
      // GROUP BY CTEs: split columns into group-by (passthrough) and aggregates (transforms with agg functions)
      const groupByCols: string[] = [];
      const aggCols: Array<{ name: string; function: string; expression: string }> = [];
      for (const col of cte.columns) {
        const aggMatch = col.expression.match(/^(\w+)\s*\((.*)\)$/s);
        if (col.isTransform && aggMatch && isAggregateFn(aggMatch[1]!)) {
          aggCols.push({
            name: col.outputName,
            function: aggMatch[1]!.toUpperCase(),
            expression: aggMatch[2]!.trim(),
          });
        } else {
          // Non-aggregate columns in a GROUP BY CTE are the GROUP BY dimensions
          groupByCols.push(col.expression);
        }
      }
      if (groupByCols.length > 0 && aggCols.length > 0) {
        summary.groupByColumnsParam = groupByCols;
        summary.aggregatesParam = aggCols;
      }
    } else if (cte.columns.length > 0 && !cte.hasJoin) {
      // Only set columnsParam for single-source CTEs where expressions can be passed directly.
      // Multi-source JOIN CTEs have SQL aliases (soh.*, sl.*) that don't map to Coalesce node names —
      // the agent must translate these to "NODE_NAME"."COLUMN" format.
      summary.columnsParam = cte.columns.map((c) => ({
        name: c.outputName,
        ...(c.isTransform ? { transform: c.expression } : {}),
      }));
    }

    return summary;
  });

  return {
    version: 1,
    intent: "sql",
    status: "needs_clarification",
    STOP_AND_CONFIRM: `STOP. Present the pipeline summary to the user in a table format and ask for confirmation BEFORE creating any nodes. For EACH node in cteNodeSummary, display: name, the EXACT nodeType string (e.g. "Coalesce-Base-Node-Types:::Stage"), pattern, transforms, and whereFilter. Use the cteNodeSummary array — do NOT paraphrase or simplify the nodeType values. Do NOT proceed until the user explicitly approves.`,
    workspaceID: params.workspaceID,
    platform: null,
    goal: params.goal ?? null,
    sql: params.sql ?? null,
    nodes: [],
    cteNodeSummary,
    assumptions: [
      `Parsed ${ctes.length} CTEs with ${allTransformCount} column transforms and ${allFilterCount} WHERE filters.`,
      `Staging and aggregation CTEs: 1 call per node. Multi-source JOIN CTEs: 2 calls (create + apply_join_condition).`,
    ],
    openQuestions: [
      `STOP: Present this pipeline summary to the user and ask "Should I proceed with creating these ${ctes.length} nodes?" Do NOT create nodes until the user confirms.`,
      `This SQL uses CTEs (WITH ... AS), which Coalesce does not support as a single node. Each CTE must become a separate node.`,
      `--- PER-CTE INSTRUCTIONS ---\n\n${cteInstructions.join("\n\n")}`,
      ...(cteDependencies.length > 0
        ? [`CTE dependencies (create in order):\n${cteDependencies.map((d) => `  - ${d}`).join("\n")}`]
        : []),
      ...(finalSelectNote ? [finalSelectNote] : []),
      `Node type guidance (do NOT use list_workspace_node_types):\n` +
        `- Staging CTEs (single-source): nodeType "${stagingType}"\n` +
        `- Join/transform CTEs (multi-source): nodeType "${multiSourceType}"\n` +
        `- Aggregation CTEs (GROUP BY): nodeType "${aggregationType}"`,
      `Workflow per CTE:\n` +
        `create_workspace_node_from_predecessor accepts columns, whereCondition, groupByColumns, and aggregates directly:\n` +
        `- For staging/transform CTEs (single-source): 1 call — pass columns (from cteNodeSummary.columnsParam) + whereCondition\n` +
        `- For GROUP BY CTEs: 1 call — pass groupByColumns (from cteNodeSummary.groupByColumnsParam) + aggregates (from cteNodeSummary.aggregatesParam)\n` +
        `- For multi-source JOIN CTEs: 2 calls — first create_workspace_node_from_predecessor with columns + whereCondition, then apply_join_condition to set up FROM/JOIN/ON\n` +
        `- Do NOT construct {{ ref() }} syntax — the FROM clause and joins are auto-generated\n` +
        `- Pass repoPath to each call for automatic config completion`,
    ],
    warnings: [
      `SQL contains ${ctes.length} CTEs: ${ctes.map((c) => c.name).join(", ")}. Each must be a separate Coalesce node.` +
        (allTransformCount > 0 ? ` ${allTransformCount} column transforms detected.` : ``),
    ],
    supportedNodeTypes: nodeTypeSelections.staging.supportedNodeTypes.length > 0
      ? nodeTypeSelections.staging.supportedNodeTypes
      : [stagingType],
    nodeTypeSelection: nodeTypeSelections.staging,
  };
}

/**
 * Extract information about the final SELECT after all CTEs.
 */
/**
 * Escape a string for use in a RegExp constructor, ensuring special characters
 * like `$` in CTE names are treated as literals.
 */
export function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function extractFinalSelectFromCteQuery(sql: string, cteNames: Set<string>): string | null {
  // Find the last top-level SELECT using quoting-aware scanning.
  const trimmed = sql.trim();
  let lastSelectIdx = -1;

  scanTopLevel(trimmed, (_char, index, parenDepth) => {
    if (
      parenDepth === 0 &&
      trimmed.slice(index, index + 6).toUpperCase() === "SELECT" &&
      !isIdentifierChar(trimmed[index - 1]) &&
      !isIdentifierChar(trimmed[index + 6])
    ) {
      lastSelectIdx = index;
    }
    return true;
  });

  if (lastSelectIdx < 0) return null;

  const finalSelect = trimmed.slice(lastSelectIdx).trim();
  // Check which CTEs the final SELECT references (escape names for safe regex)
  const referencedCtes = [...cteNames].filter((name) =>
    new RegExp(`\\b${escapeRegExp(name)}\\b`, "i").test(finalSelect)
  );

  if (referencedCtes.length === 0) return null;

  // Check if the final SELECT is just `SELECT * FROM single_cte` — redundant
  const selectStarFromOne =
    referencedCtes.length === 1 &&
    /^SELECT\s+\*\s+FROM\s+\w+\s*;?\s*$/i.test(finalSelect);

  if (selectStarFromOne) {
    return (
      `Final SELECT is just \`SELECT * FROM ${referencedCtes[0]}\` — this is redundant. ` +
      `The last CTE node (${referencedCtes[0]}) already represents the final output. ` +
      `Do NOT create an additional node for this.`
    );
  }

  return (
    `Final output query references: ${referencedCtes.join(", ")}. ` +
    `Create a final node with these as predecessors. ` +
    `The final SELECT is:\n${finalSelect.slice(0, 500)}${finalSelect.length > 500 ? "..." : ""}`
  );
}
