import { isPlainObject } from "../../utils.js";

/**
 * Lightweight index entry for a workspace node — used when scanning
 * the full node list for name/ID/type resolution without fetching
 * full node bodies.
 */
export type WorkspaceNodeIndexEntry = {
  id: string;
  name: string;
  nodeType: string | null;
  locationName: string | null;
};

/**
 * Extracts an array of node objects from an API response that may be
 * a bare array or a `{ data: [...] }` wrapper.
 */
export function extractNodeArray(raw: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(raw)) return raw.filter(isPlainObject);
  if (isPlainObject(raw) && Array.isArray(raw.data)) return raw.data.filter(isPlainObject);
  return [];
}

/**
 * Detects whether a column transform is just a passthrough — i.e., it only
 * references the column's own name without any actual transformation.
 *
 * Passthrough patterns:
 *   empty string
 *   COLUMN_NAME (bare name)
 *   "COLUMN_NAME" (quoted bare name)
 *   "ALIAS"."COLUMN_NAME" (alias-qualified)
 *   {{ ref('NODE', 'SOURCE') }}."COLUMN_NAME" (ref-qualified)
 */
export function isPassthroughTransform(transform: string, columnName: string): boolean {
  const trimmed = transform.trim();
  if (trimmed.length === 0) return true;

  const upperName = columnName.trim().toUpperCase();
  const upperTransform = trimmed.toUpperCase();

  // Bare column name: COLUMN_NAME
  if (upperTransform === upperName) return true;

  // Quoted bare name: "COLUMN_NAME"
  if (upperTransform === `"${upperName}"`) return true;

  // "ALIAS"."COLUMN_NAME" — any single-segment alias
  const aliasColPattern = /^"[^"]+"\s*\.\s*"([^"]+)"$/i;
  const aliasMatch = trimmed.match(aliasColPattern);
  if (aliasMatch && aliasMatch[1].toUpperCase() === upperName) return true;

  // {{ ref(...) }}."COLUMN_NAME"
  const refPattern = /^\{\{\s*ref\s*\([^)]*\)\s*\}\}\s*\.\s*"([^"]+)"$/i;
  const refMatch = trimmed.match(refPattern);
  if (refMatch && refMatch[1].toUpperCase() === upperName) return true;

  return false;
}

/**
 * Deep-clones a JSON-serializable value via JSON round-trip.
 */
export function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
