/**
 * Per-process, short-lived cache of full workspace node bodies, keyed by nodeID.
 *
 * `listWorkspaceNodes(..., { detail: true })` returns the same payload as
 * `getWorkspaceNode` (config / metadata.columns / metadata.sourceMapping / ...),
 * so paginating once and indexing by ID lets callers avoid N+1 single-node
 * fetches when they need full bodies for many nodes (review, predecessor
 * lookups, upstream column resolution).
 *
 * The cache is opt-in: callers that don't need bulk data keep using
 * `getWorkspaceNode` directly.
 */

import { type CoalesceClient } from "../../client.js";
import { listWorkspaceNodes, getWorkspaceNode } from "../../coalesce/api/nodes.js";
import { isPlainObject } from "../../utils.js";
import { WORKSPACE_NODE_PAGE_LIMIT } from "../pipelines/planning-types.js";
import { createTtlCache, parseTtlMs } from "./ttl-cache.js";
import { MAX_PAGES } from "../../constants.js";

const DEFAULT_TTL_MS = 5 * 60 * 1000;

type DetailMap = Map<string, Record<string, unknown>>;

const ttlCache = createTtlCache<string, DetailMap>(() =>
  parseTtlMs(process.env.COALESCE_MCP_NODE_DETAIL_CACHE_TTL_MS, DEFAULT_TTL_MS)
);

export function invalidateWorkspaceNodeDetailIndex(workspaceID: string): void {
  ttlCache.invalidate(workspaceID);
}

export function clearWorkspaceNodeDetailIndexCache(): void {
  ttlCache.clear();
}

/**
 * Read a single node from the detail cache without triggering a load.
 * Returns undefined when the workspace has no cached index, or the index
 * doesn't include this nodeID, or the entry has expired.
 */
export function peekWorkspaceNodeDetail(
  workspaceID: string,
  nodeID: string
): Record<string, unknown> | undefined {
  return ttlCache.get(workspaceID)?.get(nodeID);
}

/**
 * Write-through a single node body into the cache. Used after a successful
 * `getWorkspaceNode` or after a mutation returns a fresh body, so subsequent
 * predecessor lookups in the same TTL window are free.
 *
 * No-op when the workspace has no cached index — we don't want a single
 * write-through to imply we have a complete index.
 */
export function populateWorkspaceNodeDetail(
  workspaceID: string,
  node: Record<string, unknown>
): void {
  const id = typeof node.id === "string" ? node.id : null;
  if (!id) return;
  const existing = ttlCache.get(workspaceID);
  if (!existing) return;
  existing.set(id, node);
}

async function fetchWorkspaceNodeDetailIndex(
  client: CoalesceClient,
  workspaceID: string
): Promise<DetailMap> {
  const map: DetailMap = new Map();
  const seenCursors = new Set<string>();
  let next: string | undefined;
  let isFirstPage = true;
  let pageCount = 0;

  while (isFirstPage || next) {
    if (++pageCount > MAX_PAGES) {
      throw new Error(
        `Workspace node detail pagination exceeded ${MAX_PAGES} pages (${map.size} nodes fetched). ` +
          `This likely indicates an API bug.`
      );
    }
    const response = await listWorkspaceNodes(client, {
      workspaceID,
      limit: WORKSPACE_NODE_PAGE_LIMIT,
      orderBy: "id",
      detail: true,
      ...(next ? { startingFrom: next } : {}),
    });

    if (!isPlainObject(response)) {
      throw new Error("Workspace node detail list response was not an object");
    }

    if (Array.isArray(response.data)) {
      for (const item of response.data) {
        if (!isPlainObject(item) || typeof item.id !== "string") continue;
        map.set(item.id, item);
      }
    }

    const responseNext =
      typeof response.next === "string" && response.next.trim().length > 0
        ? response.next
        : typeof response.next === "number"
          ? String(response.next)
          : undefined;
    if (responseNext) {
      if (seenCursors.has(responseNext)) {
        throw new Error(`Workspace node detail pagination repeated cursor ${responseNext}`);
      }
      seenCursors.add(responseNext);
    }

    next = responseNext;
    isFirstPage = false;
  }

  return map;
}

/**
 * Eagerly load (or return cached) full node bodies for an entire workspace,
 * indexed by nodeID. Use when you need detail for many nodes — the alternative
 * is N single-node fetches.
 */
export async function getWorkspaceNodeDetailIndex(
  client: CoalesceClient,
  workspaceID: string
): Promise<DetailMap> {
  return ttlCache.loadWithCache(workspaceID, () =>
    fetchWorkspaceNodeDetailIndex(client, workspaceID)
  );
}

/**
 * Lazy single-node read. Returns the cached body if the workspace's detail
 * index is already warm; otherwise falls back to a single `getWorkspaceNode`
 * call and write-throughs the result. Use in fan-out predecessor loops where
 * eagerly paginating the whole workspace would be wasteful on a cold cache
 * but free after a recent review/lineage call.
 */
export async function getCachedOrFetchWorkspaceNodeDetail(
  client: CoalesceClient,
  workspaceID: string,
  nodeID: string
): Promise<unknown> {
  const cached = peekWorkspaceNodeDetail(workspaceID, nodeID);
  if (cached !== undefined) return cached;
  const fetched = await getWorkspaceNode(client, { workspaceID, nodeID });
  if (isPlainObject(fetched)) populateWorkspaceNodeDetail(workspaceID, fetched);
  return fetched;
}
