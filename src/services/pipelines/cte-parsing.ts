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
/**
 * Find a WITH keyword that introduces CTEs, at any paren depth.
 * Disambiguates from non-CTE uses (e.g., "WITH TIME ZONE") by verifying
 * the match is followed by a CTE header pattern: [RECURSIVE] name AS (.
 * Note: uses indexOf so it does NOT skip matches inside string literals —
 * the CTE header check provides sufficient disambiguation for real-world SQL.
 */
function findWithCteIndex(sql: string): number {
  const lower = sql.toLowerCase();
  let pos = 0;
  while (pos < sql.length) {
    const idx = lower.indexOf("with", pos);
    if (idx < 0) return -1;

    // Check word boundary before
    if (idx > 0 && /\w/.test(sql[idx - 1]!)) {
      pos = idx + 4;
      continue;
    }
    // Check word boundary after
    if (idx + 4 < sql.length && /\w/.test(sql[idx + 4]!)) {
      pos = idx + 4;
      continue;
    }

    // Verify it's followed by a CTE pattern: [RECURSIVE] name AS (
    const afterWith = sql.slice(idx + 4).trimStart();
    const cteHeaderCheck = afterWith.match(
      /^(?:RECURSIVE\s+)?([A-Za-z_][\w$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])\s+AS\s*\(/i
    );
    if (cteHeaderCheck) return idx;

    pos = idx + 4;
  }
  return -1;
}

/**
 * Extract inline subqueries from SQL like `FROM (SELECT ... ) alias`.
 * Converts them to synthetic CTEs so the pipeline builder can handle them
 * the same way as WITH-based CTEs.
 */
function extractInlineSubqueries(sql: string): CteExtractionResult {
  // Find the outermost SELECT
  const selectIdx = findTopLevelKeywordIndex(sql, "SELECT");
  if (selectIdx < 0) return { ctes: [], finalSelectSQL: null };

  // Find FROM (
  const fromIdx = findTopLevelKeywordIndex(sql, "FROM", selectIdx);
  if (fromIdx < 0) return { ctes: [], finalSelectSQL: null };

  const afterFrom = sql.slice(fromIdx + 4).trimStart();
  if (!afterFrom.startsWith("(")) return { ctes: [], finalSelectSQL: null };

  // Find the matching closing paren for the subquery
  const subqueryStart = fromIdx + 4 + (sql.slice(fromIdx + 4).length - afterFrom.length);
  const openParenIdx = subqueryStart;
  const closeParenIdx = findClosingParen(sql, openParenIdx + 1);
  if (closeParenIdx < 0) return { ctes: [], finalSelectSQL: null };

  // Extract the subquery body
  const subqueryBody = sql.slice(openParenIdx + 1, closeParenIdx).trim();

  // Find the alias after the closing paren: ) SRC or ) AS SRC
  const afterClose = sql.slice(closeParenIdx + 1).trimStart();
  const aliasMatch = afterClose.match(/^(?:AS\s+)?([A-Za-z_]\w*)/i);
  const alias = aliasMatch?.[1]?.toUpperCase() ?? "SUBQUERY";

  // Parse the subquery as if it were a CTE body
  const columns = parseCteColumns(subqueryBody);
  const whereClause = extractCteWhereClause(subqueryBody);
  const sourceTable = extractCteSourceTable(subqueryBody);
  const hasGroupBy = findTopLevelKeywordIndex(subqueryBody, "GROUP") >= 0;
  const hasJoin = findTopLevelKeywordIndex(subqueryBody, "JOIN") >= 0;

  const syntheticCte: ParsedCte = {
    name: alias,
    body: subqueryBody,
    columns,
    whereClause,
    sourceTable,
    hasGroupBy,
    hasJoin,
  };

  // The final SELECT is the outer query with the subquery replaced
  // Extract from the original SELECT to end, replacing FROM (...) alias with FROM alias
  const finalSelectSQL = sql.slice(selectIdx, fromIdx + 4) + " " + alias + sql.slice(closeParenIdx + 1 + (aliasMatch?.[0]?.length ?? 0));

  return { ctes: [syntheticCte], finalSelectSQL: finalSelectSQL.trim() };
}

export type CteExtractionResult = {
  ctes: ParsedCte[];
  /** SQL remaining after the last CTE definition — the final SELECT that consumes the CTEs. */
  finalSelectSQL: string | null;
};

export function extractCtes(sql: string): CteExtractionResult {
  const trimmed = sql.trim();

  // Find WITH at any depth — handles wrapped SQL like CREATE TABLE ... AS (... WITH ...)
  const withIdx = findWithCteIndex(trimmed);
  if (withIdx < 0) {
    // No CTEs — try extracting inline subqueries: FROM (SELECT ...) alias
    return extractInlineSubqueries(trimmed);
  }

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

  // Resolve TABLE.* references in CTE columns by expanding them with the
  // referenced CTE's columns. E.g., "J1.*" → all columns from the J1 CTE.
  const cteColumnMap = new Map<string, CteColumn[]>();
  for (const cte of ctes) {
    cteColumnMap.set(cte.name, cte.columns);
  }
  for (const cte of ctes) {
    const expanded: CteColumn[] = [];
    for (const col of cte.columns) {
      // Check for TABLE.* pattern (e.g., "J1.*")
      const starMatch = col.expression.match(/^(\w+)\.\*$/);
      if (starMatch) {
        const refName = starMatch[1]!.toUpperCase();
        const refColumns = cteColumnMap.get(refName);
        if (refColumns && refColumns.length > 0) {
          // Expand: add all columns from the referenced CTE
          for (const refCol of refColumns) {
            expanded.push({
              outputName: refCol.outputName,
              expression: `${starMatch[1]}.${refCol.outputName}`,
              isTransform: false,
            });
          }
          continue;
        }
      }
      expanded.push(col);
    }
    cte.columns = expanded;
  }

  // Extract the final SELECT after the last CTE.
  // The SQL after the CTEs may be wrapped in parens: (...SELECT...FROM J9 JOIN dims...)
  // We want the innermost SELECT that consumes the CTEs.
  let remaining = trimmed.slice(cursor).trim();

  // Skip past leading whitespace, commas, and opening parens to find the SELECT
  remaining = remaining.replace(/^[\s,(]+/, "");

  // Find the SELECT keyword
  const selectIdx = findTopLevelKeywordIndex(remaining, "SELECT");
  let finalSelectSQL: string | null = null;

  if (selectIdx >= 0) {
    let body = remaining.slice(selectIdx);

    // Truncate at top-level statements that follow (DELETE, INSERT, CREATE)
    for (const kw of ["DELETE", "INSERT", "CREATE"]) {
      const kwIdx = findTopLevelKeywordIndex(body, kw, 1);
      if (kwIdx > 0) {
        body = body.slice(0, kwIdx);
      }
    }

    // Strip trailing wrapper: closing parens, "as alias", semicolons
    body = body.trim().replace(/\)\s*(?:as\s+\w+\s*)?[);]*\s*$/gi, "").trim();

    // Also strip any trailing unbalanced closing parens
    let depth = 0;
    for (const ch of body) {
      if (ch === "(") depth++;
      else if (ch === ")") depth--;
    }
    while (depth < 0 && body.endsWith(")")) {
      body = body.slice(0, -1).trim();
      depth++;
    }

    if (body.length > 0) {
      finalSelectSQL = body;
    }
  }

  return { ctes, finalSelectSQL };
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

    // Bare "*" without table prefix — skip (will be handled by subquery recursion above)
    if (/^\*$/.test(trimmedExpr)) continue;

    // TABLE.* — preserve as a placeholder for cross-CTE expansion
    if (/^\w+\.\*$/.test(trimmedExpr)) {
      columns.push({
        outputName: trimmedExpr,  // e.g., "J1.*"
        expression: trimmedExpr,
        isTransform: false,
      });
      continue;
    }

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

  // Match any combination of quoted/unquoted identifiers:
  //   "DB"."SCHEMA"."TABLE", "SCHEMA".TABLE, SCHEMA."TABLE", SCHEMA.TABLE, "TABLE", TABLE
  const identPart = `(?:"([^"]+)"|([A-Za-z_]\\w*))`;
  const fullPattern = new RegExp(
    `^${identPart}(?:\\s*\\.\\s*${identPart}(?:\\s*\\.\\s*${identPart})?)?`
  );
  const m = afterFrom.match(fullPattern);
  if (!m) return null;

  const part1 = (m[1] ?? m[2])?.toUpperCase();
  const part2 = (m[3] ?? m[4])?.toUpperCase();
  const part3 = (m[5] ?? m[6])?.toUpperCase();

  if (!part1) return null;
  if (part3) return `${part1}.${part2}.${part3}`;
  if (part2) return `${part1}.${part2}`;
  return part1;
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
