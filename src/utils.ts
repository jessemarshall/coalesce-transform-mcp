import { CoalesceApiError } from "./client.js";

/**
 * Type guard that narrows `unknown` to a plain key-value object.
 * Used throughout the codebase to safely access dynamic API responses.
 */
export function isPlainObject(
  value: unknown
): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const NON_RECOVERABLE_STATUSES = [401, 403, 503];
const NON_RECOVERABLE_WITH_SERVER_ERROR_STATUSES = [401, 403, 500, 503];

/**
 * Re-throws CoalesceApiError for non-recoverable HTTP statuses (401, 403, 503).
 * These indicate auth failures or service unavailability that should
 * propagate rather than being caught and silenced.
 */
export function rethrowNonRecoverableApiError(error: unknown): void {
  if (error instanceof CoalesceApiError && NON_RECOVERABLE_STATUSES.includes(error.status)) {
    throw error;
  }
}

/**
 * Like {@link rethrowNonRecoverableApiError} but also rethrows on HTTP 500.
 * Use in contexts where a server error indicates a broken session rather than
 * a transient failure worth swallowing (e.g. node-type inventory fetches).
 */
export function rethrowNonRecoverableOrServerError(error: unknown): void {
  if (error instanceof CoalesceApiError && NON_RECOVERABLE_WITH_SERVER_ERROR_STATUSES.includes(error.status)) {
    throw error;
  }
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
