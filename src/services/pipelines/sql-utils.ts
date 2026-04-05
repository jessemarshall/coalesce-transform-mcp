import { stripIdentifierQuotes } from "./sql-tokenizer.js";

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

/**
 * Escape a string for use in a RegExp constructor, ensuring special characters
 * like `$` in CTE names are treated as literals.
 */
export function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Check if an expression is a simple column reference (no transform needed).
 * Simple: `col`, `"col"`, `table.col`, `table."col"`, `"table"."col"`
 */
export function isSimpleColumnRef(expr: string): boolean {
  return /^(?:[A-Za-z_][\w$]*|"[^"]+")(?:\.(?:[A-Za-z_][\w$]*|"[^"]+"))?$/.test(expr.trim());
}

/**
 * Extract a bare column name from a simple reference like `table.col` or `col`.
 */
export function extractBareColumnName(expr: string): string | null {
  const match = expr.trim().match(/(?:.*\.)?([A-Za-z_][\w$]*|"[^"]+")$/);
  if (!match?.[1]) return null;
  return stripIdentifierQuotes(match[1]);
}

/**
 * Check if a token is a supported SQL identifier (unquoted, double-quoted, backtick-quoted, or bracket-quoted).
 */
export function isSupportedIdentifierToken(token: string): boolean {
  return (
    /^[A-Za-z_][\w$]*$/u.test(token) ||
    /^"[^"]+"$/u.test(token) ||
    /^`[^`]+`$/u.test(token) ||
    /^\[[^\]]+\]$/u.test(token)
  );
}
