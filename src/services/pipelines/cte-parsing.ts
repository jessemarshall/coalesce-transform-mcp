import {
  stripIdentifierQuotes,
  findTopLevelKeywordIndex,
  splitTopLevel,
  findClosingParen,
  extractParenBody,
} from "./sql-tokenizer.js";
import { isSimpleColumnRef, extractBareColumnName } from "./sql-utils.js";
import { extractSelectClause } from "./clause-extraction.js";
import { splitExpressionAlias } from "./select-parsing.js";

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

  const withIdx = findTopLevelKeywordIndex(trimmed, "WITH");
  if (withIdx !== 0) return [];

  const ctes: ParsedCte[] = [];
  let cursor = withIdx + 4; // skip past "WITH"

  // Skip optional RECURSIVE keyword
  const afterWith = trimmed.slice(cursor).match(/^\s+RECURSIVE\b/i);
  if (afterWith) cursor += afterWith[0].length;

  while (cursor < trimmed.length) {
    const rest = trimmed.slice(cursor);
    const leadingMatch = rest.match(/^[\s,]+/);
    if (leadingMatch) cursor += leadingMatch[0].length;
    if (cursor >= trimmed.length) break;

    const headerMatch = trimmed.slice(cursor).match(
      /^([A-Za-z_][\w$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])\s+AS\s*\(/i
    );
    if (!headerMatch) break;

    const rawName = stripIdentifierQuotes(headerMatch[1]!);
    const name = rawName.toUpperCase();
    const bodyStart = cursor + headerMatch[0].length;

    const closeIdx = findClosingParen(trimmed, bodyStart);
    if (closeIdx >= 0) {
      const body = trimmed.slice(bodyStart, closeIdx).trim();
      const columns = parseCteColumns(body);
      const whereClause = extractCteWhereClause(body);
      const sourceTable = extractCteSourceTable(body);
      const hasGroupBy = findTopLevelKeywordIndex(body, "GROUP") >= 0;
      const hasJoin = findTopLevelKeywordIndex(body, "JOIN") >= 0;
      ctes.push({ name, body, columns, whereClause, sourceTable, hasGroupBy, hasJoin });
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
 * Extract WHERE clause from a CTE body (ignoring subqueries).
 * Uses quoting-aware keyword search to avoid matching inside strings or comments.
 */
function extractCteWhereClause(body: string): string | null {
  const whereIdx = findTopLevelKeywordIndex(body, "WHERE");
  if (whereIdx < 0) return null;

  const afterWhere = whereIdx + 5; // "WHERE".length
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
export function classifyCtePattern(cte: ParsedCte): "staging" | "multiSource" | "aggregation" {
  if (cte.hasGroupBy) return "aggregation";
  if (cte.hasJoin) return "multiSource";
  return "staging";
}

const AGGREGATE_FUNCTIONS = new Set([
  "COUNT", "SUM", "AVG", "MIN", "MAX",
  "LISTAGG", "ARRAY_AGG", "MEDIAN", "MODE",
  "STDDEV", "VARIANCE", "ANY_VALUE",
  "COUNT_IF", "SUM_IF", "AVG_IF",
  "APPROX_COUNT_DISTINCT", "HLL",
]);

export function isAggregateFn(name: string): boolean {
  return AGGREGATE_FUNCTIONS.has(name.toUpperCase());
}
