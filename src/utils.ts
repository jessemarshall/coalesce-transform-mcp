/**
 * Type guard that narrows `unknown` to a plain key-value object.
 * Used throughout the codebase to safely access dynamic API responses.
 */
export function isPlainObject(
  value: unknown
): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
