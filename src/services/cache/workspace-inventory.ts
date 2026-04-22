/**
 * Per-process, short-lived cache for workspace node-type inventories.
 *
 * Repeated MCP calls (e.g. plan_pipeline + build_pipeline_from_intent within
 * the same session) each call listWorkspaceNodeTypes, which paginates the
 * entire node graph. On large workspaces that's tens of seconds of
 * re-fetching the same data. This cache coalesces those calls.
 *
 * Writes are guarded by a per-workspace generation counter so that an
 * invalidate() during an in-flight fetch is not silently overwritten when
 * the stale fetch resolves.
 */

export type WorkspaceNodeInventoryEntry = {
  workspaceID: string;
  basis: "observed_nodes";
  nodeTypes: string[];
  counts: Record<string, number>;
  total: number;
};

const DEFAULT_TTL_MS = 5 * 60 * 1000;

function resolveTtlMs(): number {
  const raw = process.env.COALESCE_MCP_INVENTORY_CACHE_TTL_MS;
  if (!raw) return DEFAULT_TTL_MS;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return DEFAULT_TTL_MS;
  return parsed;
}

type CacheRecord = {
  value: WorkspaceNodeInventoryEntry;
  expiresAt: number;
};

const cache = new Map<string, CacheRecord>();
const inflight = new Map<string, Promise<WorkspaceNodeInventoryEntry>>();
const generations = new Map<string, number>();

function currentGeneration(workspaceID: string): number {
  return generations.get(workspaceID) ?? 0;
}

export function getCachedInventory(
  workspaceID: string
): WorkspaceNodeInventoryEntry | undefined {
  const hit = cache.get(workspaceID);
  if (!hit) return undefined;
  if (hit.expiresAt <= Date.now()) {
    cache.delete(workspaceID);
    return undefined;
  }
  return hit.value;
}

export function setCachedInventory(
  workspaceID: string,
  value: WorkspaceNodeInventoryEntry
): void {
  const ttl = resolveTtlMs();
  if (ttl === 0) return;
  cache.set(workspaceID, { value, expiresAt: Date.now() + ttl });
}

export function invalidateWorkspaceInventory(workspaceID: string): void {
  cache.delete(workspaceID);
  inflight.delete(workspaceID);
  generations.set(workspaceID, currentGeneration(workspaceID) + 1);
}

export function clearWorkspaceInventoryCache(): void {
  cache.clear();
  inflight.clear();
  generations.clear();
}

export async function loadInventoryWithCache(
  workspaceID: string,
  loader: () => Promise<WorkspaceNodeInventoryEntry>
): Promise<WorkspaceNodeInventoryEntry> {
  const cached = getCachedInventory(workspaceID);
  if (cached) return cached;

  const existing = inflight.get(workspaceID);
  if (existing) return existing;

  const startGeneration = currentGeneration(workspaceID);
  let promise!: Promise<WorkspaceNodeInventoryEntry>;
  promise = (async () => {
    try {
      const value = await loader();
      // Skip the write if the workspace was invalidated mid-flight — the
      // fetched value may no longer reflect current state.
      if (currentGeneration(workspaceID) === startGeneration) {
        setCachedInventory(workspaceID, value);
      }
      return value;
    } finally {
      // Only clear the inflight entry if it's still ours — an invalidate()
      // plus a new call could have replaced it while we awaited.
      if (inflight.get(workspaceID) === promise) {
        inflight.delete(workspaceID);
      }
    }
  })();

  inflight.set(workspaceID, promise);
  return promise;
}
