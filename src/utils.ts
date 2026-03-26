/**
 * Type guard that narrows `unknown` to a plain key-value object.
 * Used throughout the codebase to safely access dynamic API responses.
 */
export function isPlainObject(
  value: unknown
): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Returns the first occurrence of each distinct item while preserving input order.
 */
export function uniqueInOrder<T>(values: T[]): T[] {
  const seen = new Set<T>();
  const unique: T[] = [];

  for (const value of values) {
    if (seen.has(value)) {
      continue;
    }
    seen.add(value);
    unique.push(value);
  }

  return unique;
}
