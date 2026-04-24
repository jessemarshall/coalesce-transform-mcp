/**
 * Per-process, short-lived cache for workspace node-type inventories.
 *
 * Repeated MCP calls (e.g. plan_pipeline + build_pipeline_from_intent within
 * the same session) each call listWorkspaceNodeTypes, which paginates the
 * entire node graph. On large workspaces that's tens of seconds of
 * re-fetching the same data. This cache coalesces those calls.
 */

import { createTtlCache, parseTtlMs } from "./ttl-cache.js";

export type WorkspaceNodeInventoryEntry = {
  workspaceID: string;
  basis: "observed_nodes";
  nodeTypes: string[];
  counts: Record<string, number>;
  total: number;
};

const DEFAULT_TTL_MS = 5 * 60 * 1000;

const ttlCache = createTtlCache<string, WorkspaceNodeInventoryEntry>(() =>
  parseTtlMs(process.env.COALESCE_MCP_INVENTORY_CACHE_TTL_MS, DEFAULT_TTL_MS)
);

export function getCachedInventory(
  workspaceID: string
): WorkspaceNodeInventoryEntry | undefined {
  return ttlCache.get(workspaceID);
}

export function setCachedInventory(
  workspaceID: string,
  value: WorkspaceNodeInventoryEntry
): void {
  ttlCache.set(workspaceID, value);
}

export function invalidateWorkspaceInventory(workspaceID: string): void {
  ttlCache.invalidate(workspaceID);
}

export function clearWorkspaceInventoryCache(): void {
  ttlCache.clear();
}

export function loadInventoryWithCache(
  workspaceID: string,
  loader: () => Promise<WorkspaceNodeInventoryEntry>
): Promise<WorkspaceNodeInventoryEntry> {
  return ttlCache.loadWithCache(workspaceID, loader);
}
