/**
 * Per-process, short-lived cache of the lightweight workspace node index
 * (id / name / nodeType / locationName tuples).
 *
 * Both intent resolution and SQL-ref resolution paginate the full node
 * list. On a large workspace that's expensive and repeated across
 * sequential tool calls. This cache coalesces them.
 *
 * Writes are guarded by a per-workspace generation counter so that an
 * invalidate() during an in-flight fetch is not silently overwritten when
 * the stale fetch resolves.
 */

import { type CoalesceClient } from "../../client.js";
import { listWorkspaceNodes } from "../../coalesce/api/nodes.js";
import { isPlainObject } from "../../utils.js";
import { type WorkspaceNodeIndexEntry } from "../shared/node-helpers.js";
import { WORKSPACE_NODE_PAGE_LIMIT } from "../pipelines/planning-types.js";

const DEFAULT_TTL_MS = 5 * 60 * 1000;
const MAX_PAGES = 500;

function resolveTtlMs(): number {
  const raw = process.env.COALESCE_MCP_NODE_INDEX_CACHE_TTL_MS;
  if (!raw) return DEFAULT_TTL_MS;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return DEFAULT_TTL_MS;
  return parsed;
}

type CacheRecord = {
  value: WorkspaceNodeIndexEntry[];
  expiresAt: number;
};

const cache = new Map<string, CacheRecord>();
const inflight = new Map<string, Promise<WorkspaceNodeIndexEntry[]>>();
const generations = new Map<string, number>();

function currentGeneration(workspaceID: string): number {
  return generations.get(workspaceID) ?? 0;
}

export function invalidateWorkspaceNodeIndex(workspaceID: string): void {
  cache.delete(workspaceID);
  inflight.delete(workspaceID);
  generations.set(workspaceID, currentGeneration(workspaceID) + 1);
}

export function clearWorkspaceNodeIndexCache(): void {
  cache.clear();
  inflight.clear();
  generations.clear();
}

async function fetchWorkspaceNodeIndex(
  client: CoalesceClient,
  workspaceID: string
): Promise<WorkspaceNodeIndexEntry[]> {
  const nodes: WorkspaceNodeIndexEntry[] = [];
  const seenCursors = new Set<string>();
  let next: string | undefined;
  let isFirstPage = true;
  let pageCount = 0;

  while (isFirstPage || next) {
    if (++pageCount > MAX_PAGES) {
      throw new Error(
        `Workspace node pagination exceeded ${MAX_PAGES} pages (${nodes.length} nodes fetched). ` +
          `This likely indicates an API bug.`
      );
    }
    const response = await listWorkspaceNodes(client, {
      workspaceID,
      limit: WORKSPACE_NODE_PAGE_LIMIT,
      orderBy: "id",
      ...(next ? { startingFrom: next } : {}),
    });

    if (!isPlainObject(response)) {
      throw new Error("Workspace node list response was not an object");
    }

    if (Array.isArray(response.data)) {
      for (const item of response.data) {
        if (
          !isPlainObject(item) ||
          typeof item.id !== "string" ||
          typeof item.name !== "string"
        ) {
          continue;
        }
        nodes.push({
          id: item.id,
          name: item.name,
          nodeType: typeof item.nodeType === "string" ? item.nodeType : null,
          locationName:
            typeof item.locationName === "string" ? item.locationName : null,
        });
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
        throw new Error(`Workspace node pagination repeated cursor ${responseNext}`);
      }
      seenCursors.add(responseNext);
    }

    next = responseNext;
    isFirstPage = false;
  }

  return nodes;
}

export async function getWorkspaceNodeIndex(
  client: CoalesceClient,
  workspaceID: string
): Promise<WorkspaceNodeIndexEntry[]> {
  const hit = cache.get(workspaceID);
  if (hit && hit.expiresAt > Date.now()) {
    return hit.value;
  }
  if (hit) cache.delete(workspaceID);

  const existing = inflight.get(workspaceID);
  if (existing) return existing;

  const startGeneration = currentGeneration(workspaceID);
  let promise!: Promise<WorkspaceNodeIndexEntry[]>;
  promise = (async () => {
    try {
      const value = await fetchWorkspaceNodeIndex(client, workspaceID);
      if (currentGeneration(workspaceID) === startGeneration) {
        const ttl = resolveTtlMs();
        if (ttl > 0) {
          cache.set(workspaceID, { value, expiresAt: Date.now() + ttl });
        }
      }
      return value;
    } finally {
      if (inflight.get(workspaceID) === promise) {
        inflight.delete(workspaceID);
      }
    }
  })();

  inflight.set(workspaceID, promise);
  return promise;
}
