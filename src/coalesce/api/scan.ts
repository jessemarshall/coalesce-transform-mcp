import type { CoalesceClient } from "../../client.js";
import { CoalesceApiError } from "../../client.js";

const SCAN_BATCH_SIZE = 50;
const SCAN_CONCURRENCY = 20;
const SCAN_TAIL_THRESHOLD = 10;
const SCAN_MAX_ID = 10_000;

function compareById(a: unknown, b: unknown): number {
  const aRaw = typeof a === "object" && a !== null && "id" in a ? Number((a as Record<string, unknown>).id) : 0;
  const bRaw = typeof b === "object" && b !== null && "id" in b ? Number((b as Record<string, unknown>).id) : 0;
  return (Number.isFinite(aRaw) ? aRaw : 0) - (Number.isFinite(bRaw) ? bRaw : 0);
}

/**
 * Scans sequential numeric IDs to discover resources when the API lacks
 * a collection GET endpoint. Fetches `GET {basePath}/1`, `GET {basePath}/2`, ...
 * in parallel batches. Continues scanning if resources are found near the
 * end of a batch (within the last SCAN_TAIL_THRESHOLD IDs). Stops when a
 * full batch has no hits in the tail, or SCAN_MAX_ID is reached.
 * An optional `limit` caps the number of returned resources, stopping the
 * scan early once reached.
 */
export async function scanResourcesByID(
  client: CoalesceClient,
  basePath: string,
  limit?: number
): Promise<{ data: unknown[] }> {
  if (!basePath || typeof basePath !== "string" || !/^\/[a-zA-Z0-9/._-]+$/.test(basePath)) {
    throw new Error(`Invalid basePath for resource scan: "${basePath}". Must be an absolute API path with safe characters.`);
  }
  if (limit !== undefined && (!Number.isFinite(limit) || !Number.isInteger(limit) || limit < 1)) {
    throw new Error(`Invalid scan limit: ${limit}. Must be a positive integer.`);
  }
  const found: unknown[] = [];
  let batchStart = 1;

  while (batchStart <= SCAN_MAX_ID) {
    const batchEnd = Math.min(batchStart + SCAN_BATCH_SIZE - 1, SCAN_MAX_ID);
    let highestFoundInBatch = 0;

    for (let chunk = batchStart; chunk <= batchEnd; chunk += SCAN_CONCURRENCY) {
      const chunkEnd = Math.min(chunk + SCAN_CONCURRENCY - 1, batchEnd);
      const promises: Promise<void>[] = [];

      for (let id = chunk; id <= chunkEnd; id++) {
        promises.push(
          client.get(`${basePath}/${id}`, {}).then((resource) => {
            found.push(resource);
            if (id > highestFoundInBatch) highestFoundInBatch = id;
          }).catch((err: unknown) => {
            if (err instanceof CoalesceApiError && err.status === 404) return;
            throw err;
          })
        );
      }

      await Promise.all(promises);
      if (limit && found.length >= limit) {
        found.sort(compareById);
        return { data: found.slice(0, limit) };
      }
    }

    const tailStart = batchEnd - SCAN_TAIL_THRESHOLD + 1;
    if (highestFoundInBatch < tailStart) break;

    batchStart = batchEnd + 1;
  }

  // Sort by ID for deterministic ordering (resources arrive in non-deterministic
  // order due to concurrent fetches).
  found.sort(compareById);

  if (found.length === 0) {
    process.stderr.write(
      `[scanResourcesByID] Scan of ${basePath} IDs 1-${Math.min(SCAN_BATCH_SIZE, SCAN_MAX_ID)} found no resources. ` +
      `Verify the basePath and parent resource ID are correct.\n`
    );
  }

  return { data: found };
}
