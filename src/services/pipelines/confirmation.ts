import { createHash } from "node:crypto";
import { isPlainObject } from "../../utils.js";

function sortJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortJsonValue);
  }
  if (!isPlainObject(value)) {
    return value;
  }

  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    const nested = sortJsonValue(value[key]);
    if (nested !== undefined) {
      sorted[key] = nested;
    }
  }
  return sorted;
}

/**
 * Generates a confirmation token for a pipeline plan to prevent bypass of user approval.
 *
 * The token is a SHA256 hash (truncated to 16 hex chars) of the canonicalized
 * plan JSON, so structurally identical plans always yield the same token.
 */
export function buildPlanConfirmationToken(plan: unknown): string {
  return createHash("sha256")
    .update(JSON.stringify(sortJsonValue(plan)))
    .digest("hex")
    .slice(0, 16);
}
