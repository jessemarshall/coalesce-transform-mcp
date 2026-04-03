import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { CoalesceClient, QueryParams } from "../../client.js";
import { listWorkspaceNodes, setWorkspaceNode } from "../../coalesce/api/nodes.js";
import { buildUpdatedWorkspaceNodeBody } from "../workspace/node-update-helpers.js";
import { validatePathSegment } from "../../coalesce/types.js";
import { isPlainObject } from "../../utils.js";
import { CACHE_DIR_NAME } from "../../cache-dir.js";
import type { WorkflowProgressReporter } from "../../workflows/progress.js";

const DEFAULT_PAGE_SIZE = 250;
const DEFAULT_TTL_MS = 30 * 60 * 1000; // 30 minutes
const MAX_CACHE_ENTRIES = 50;
const PROGRESS_INTERVAL = 500;
const DETAIL_FETCH_TIMEOUT_MS = 120_000; // 2 minutes per page for detail=true fetches

export type LineageNode = {
  id: string;
  name: string;
  nodeType: string;
  columns: LineageColumn[];
  raw: Record<string, unknown>;
};

export type LineageColumn = {
  id: string;
  name: string;
  dataType?: string;
  sourceColumnRefs: ColumnRef[];
};

export type ColumnRef = {
  sourceNodeID: string;
  sourceColumnID: string;
};

export type LineageCacheEntry = {
  workspaceID: string;
  nodes: Map<string, LineageNode>;
  upstreamNodes: Map<string, Set<string>>;
  downstreamNodes: Map<string, Set<string>>;
  columnUpstream: Map<string, Set<string>>; // "nodeID:colID" → Set<"srcNodeID:srcColID">
  columnDownstream: Map<string, Set<string>>; // "nodeID:colID" → Set<"dstNodeID:dstColID">
  cachedAt: number;
  ttlMs: number;
};

const cacheStore = new Map<string, LineageCacheEntry>();

export function getLineageTtlMs(): number {
  const raw = process.env.COALESCE_MCP_LINEAGE_TTL_MS;
  if (raw === undefined) return DEFAULT_TTL_MS;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return DEFAULT_TTL_MS;
  return parsed;
}

function columnKey(nodeID: string, columnID: string): string {
  return `${nodeID}:${columnID}`;
}

export function parseColumnKey(key: string): { nodeID: string; columnID: string } {
  const sepIndex = key.indexOf(":");
  if (sepIndex < 1 || sepIndex === key.length - 1) {
    throw new Error(`Malformed column key: expected "nodeID:columnID" but got "${key}"`);
  }
  return {
    nodeID: key.slice(0, sepIndex),
    columnID: key.slice(sepIndex + 1),
  };
}

function extractColumns(metadata: Record<string, unknown>): LineageColumn[] {
  const rawColumns = metadata.columns;
  if (!Array.isArray(rawColumns)) return [];

  const columns: LineageColumn[] = [];
  for (const col of rawColumns) {
    if (!isPlainObject(col)) continue;
    const id = typeof col.columnID === "string" ? col.columnID : typeof col.id === "string" ? col.id : undefined;
    const name = typeof col.name === "string" ? col.name : undefined;
    if (!id || !name) continue;

    const refs: ColumnRef[] = [];
    const sources = Array.isArray(col.sources) ? col.sources : [];
    for (const source of sources) {
      if (!isPlainObject(source)) continue;
      const colRefs = Array.isArray(source.columnReferences) ? source.columnReferences : [];
      for (const ref of colRefs) {
        if (!isPlainObject(ref)) continue;
        const srcNodeID = typeof ref.nodeID === "string" ? ref.nodeID : typeof ref.sourceNodeID === "string" ? ref.sourceNodeID : undefined;
        const srcColumnID = typeof ref.columnID === "string" ? ref.columnID : typeof ref.sourceColumnID === "string" ? ref.sourceColumnID : undefined;
        if (srcNodeID && srcColumnID) {
          refs.push({ sourceNodeID: srcNodeID, sourceColumnID: srcColumnID });
        }
      }
    }

    columns.push({
      id,
      name,
      dataType: typeof col.dataType === "string" ? col.dataType : undefined,
      sourceColumnRefs: refs,
    });
  }
  return columns;
}

function extractUpstreamNodeIDs(metadata: Record<string, unknown>): string[] {
  const sourceMapping = metadata.sourceMapping;
  if (!Array.isArray(sourceMapping)) return [];

  const ids: string[] = [];
  for (const mapping of sourceMapping) {
    if (!isPlainObject(mapping)) continue;

    // Primary: extract node IDs from aliases map (most reliable — contains resolved UUIDs)
    const aliases = isPlainObject(mapping.aliases) ? mapping.aliases : {};
    for (const value of Object.values(aliases)) {
      if (typeof value === "string") {
        ids.push(value);
      }
    }

    // Fallback: extract from dependencies if they contain nodeID directly
    const deps = Array.isArray(mapping.dependencies) ? mapping.dependencies : [];
    for (const dep of deps) {
      if (typeof dep === "string") {
        ids.push(dep);
      } else if (isPlainObject(dep) && typeof dep.nodeID === "string") {
        ids.push(dep.nodeID);
      }
    }
  }

  // Deduplicate — aliases and dependencies may reference the same nodes
  return [...new Set(ids)];
}

function buildLineageNode(raw: Record<string, unknown>): LineageNode | null {
  const id = typeof raw.id === "string" ? raw.id : undefined;
  const name = typeof raw.name === "string" ? raw.name : undefined;
  const nodeType = typeof raw.nodeType === "string" ? raw.nodeType : undefined;
  if (!id || !name || !nodeType) return null;

  const metadata = isPlainObject(raw.metadata) ? raw.metadata : {};
  return {
    id,
    name,
    nodeType,
    columns: extractColumns(metadata),
    raw,
  };
}

function buildIndexes(nodes: Map<string, LineageNode>): Pick<
  LineageCacheEntry,
  "upstreamNodes" | "downstreamNodes" | "columnUpstream" | "columnDownstream"
> {
  const upstreamNodes = new Map<string, Set<string>>();
  const downstreamNodes = new Map<string, Set<string>>();
  const columnUpstream = new Map<string, Set<string>>();
  const columnDownstream = new Map<string, Set<string>>();

  for (const node of nodes.values()) {
    if (!upstreamNodes.has(node.id)) {
      upstreamNodes.set(node.id, new Set());
    }
    if (!downstreamNodes.has(node.id)) {
      downstreamNodes.set(node.id, new Set());
    }

    // Node-level upstream from sourceMapping
    const metadata = isPlainObject(node.raw.metadata) ? node.raw.metadata : {};
    const upstreamIDs = extractUpstreamNodeIDs(metadata);
    for (const upID of upstreamIDs) {
      if (!nodes.has(upID)) continue; // skip references to nodes outside workspace
      upstreamNodes.get(node.id)!.add(upID);
      if (!downstreamNodes.has(upID)) {
        downstreamNodes.set(upID, new Set());
      }
      downstreamNodes.get(upID)!.add(node.id);
    }

    // Column-level lineage from columnReferences
    for (const col of node.columns) {
      const key = columnKey(node.id, col.id);
      if (!columnUpstream.has(key)) {
        columnUpstream.set(key, new Set());
      }

      for (const ref of col.sourceColumnRefs) {
        const srcKey = columnKey(ref.sourceNodeID, ref.sourceColumnID);
        columnUpstream.get(key)!.add(srcKey);

        if (!columnDownstream.has(srcKey)) {
          columnDownstream.set(srcKey, new Set());
        }
        columnDownstream.get(srcKey)!.add(key);
      }
    }
  }

  return { upstreamNodes, downstreamNodes, columnUpstream, columnDownstream };
}

function tryLoadFromSnapshot(workspaceID: string, baseDir: string): Record<string, unknown>[] | null {
  const safeID = validatePathSegment(workspaceID, "workspaceID");
  const ndjsonPath = join(baseDir, CACHE_DIR_NAME, "nodes", `workspace-${safeID}-nodes.ndjson`);
  const metaPath = join(baseDir, CACHE_DIR_NAME, "nodes", `workspace-${safeID}-nodes.meta.json`);

  if (!existsSync(ndjsonPath) || !existsSync(metaPath)) return null;

  try {
    const meta = JSON.parse(readFileSync(metaPath, "utf8"));
    if (!isPlainObject(meta) || typeof meta.cachedAt !== "string") return null;

    // Check if snapshot is within TTL
    const cachedAt = new Date(meta.cachedAt).getTime();
    if (Date.now() - cachedAt > getLineageTtlMs()) return null;

    const lines = readFileSync(ndjsonPath, "utf8").trimEnd().split("\n");
    const items: Record<string, unknown>[] = [];
    for (const line of lines) {
      if (line.trim().length === 0) continue;
      const parsed = JSON.parse(line);
      if (isPlainObject(parsed)) items.push(parsed);
    }

    // Verify the snapshot was fetched with detail=true by checking the first
    // node has metadata — summary snapshots lack the metadata.columns field
    // needed for column lineage indexes.
    if (items.length > 0 && !isPlainObject(items[0].metadata)) return null;

    return items;
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    process.stderr.write(`[tryLoadFromSnapshot] Failed to load snapshot for workspace ${workspaceID}: ${reason}\n`);
    return null;
  }
}

type PaginatedResponse = {
  data: unknown[];
  next?: string;
};

function parsePage(response: unknown): PaginatedResponse {
  if (!isPlainObject(response)) {
    throw new Error("Paginated collection response was not an object");
  }
  return {
    data: Array.isArray(response.data) ? response.data : [],
    next:
      typeof response.next === "string" && response.next.trim().length > 0
        ? response.next
        : typeof response.next === "number"
          ? String(response.next)
          : undefined,
  };
}

async function fetchAllNodes(
  client: CoalesceClient,
  workspaceID: string,
  reportProgress?: WorkflowProgressReporter
): Promise<Record<string, unknown>[]> {
  const safeID = validatePathSegment(workspaceID, "workspaceID");
  const items: Record<string, unknown>[] = [];
  const seenCursors = new Set<string>();
  let next: string | undefined;
  let isFirstPage = true;

  while (isFirstPage || next) {
    const response = await listWorkspaceNodes(client, {
      workspaceID: safeID,
      detail: true,
      limit: DEFAULT_PAGE_SIZE,
      orderBy: "id",
      ...(next ? { startingFrom: next } : {}),
    } as QueryParams & { workspaceID: string }, { timeoutMs: DETAIL_FETCH_TIMEOUT_MS });

    const page = parsePage(response);
    for (const item of page.data) {
      if (isPlainObject(item)) {
        items.push(item);
      }
    }

    if (reportProgress && items.length % PROGRESS_INTERVAL < DEFAULT_PAGE_SIZE) {
      await reportProgress(`Fetched ${items.length} nodes`, undefined);
    }

    if (page.next) {
      if (seenCursors.has(page.next)) {
        throw new Error(`Pagination repeated cursor ${page.next}`);
      }
      seenCursors.add(page.next);
    }

    next = page.next;
    isFirstPage = false;
  }

  return items;
}

export type BuildLineageCacheOptions = {
  baseDir?: string;
  ttlMs?: number;
  reportProgress?: WorkflowProgressReporter;
  forceRefresh?: boolean;
};

export async function buildLineageCache(
  client: CoalesceClient,
  workspaceID: string,
  options: BuildLineageCacheOptions = {}
): Promise<LineageCacheEntry> {
  const safeID = validatePathSegment(workspaceID, "workspaceID");
  const ttlMs = options.ttlMs ?? getLineageTtlMs();

  // Check in-memory cache first
  if (!options.forceRefresh) {
    const cached = cacheStore.get(safeID);
    if (cached && Date.now() - cached.cachedAt < cached.ttlMs) {
      return cached;
    }
  }

  const baseDir = options.baseDir ?? process.cwd();

  // Try loading from disk snapshot
  let rawNodes: Record<string, unknown>[] | null = null;
  if (!options.forceRefresh) {
    rawNodes = tryLoadFromSnapshot(safeID, baseDir);
    if (rawNodes && options.reportProgress) {
      await options.reportProgress(`Loaded ${rawNodes.length} nodes from snapshot cache`);
    }
  }

  // Fetch from API if no snapshot available
  if (!rawNodes) {
    if (options.reportProgress) {
      await options.reportProgress("Fetching all workspace nodes with detail=true");
    }
    rawNodes = await fetchAllNodes(client, safeID, options.reportProgress);
  }

  // Build node map
  const nodes = new Map<string, LineageNode>();
  for (const raw of rawNodes) {
    const node = buildLineageNode(raw);
    if (node) {
      nodes.set(node.id, node);
    }
  }

  if (options.reportProgress) {
    await options.reportProgress(`Building lineage indexes for ${nodes.size} nodes`);
  }

  // Build indexes
  const indexes = buildIndexes(nodes);

  const entry: LineageCacheEntry = {
    workspaceID: safeID,
    nodes,
    ...indexes,
    cachedAt: Date.now(),
    ttlMs,
  };

  // Evict expired entries and enforce max cache size
  const now = Date.now();
  for (const [key, cached] of cacheStore) {
    if (now - cached.cachedAt > cached.ttlMs) {
      cacheStore.delete(key);
    }
  }
  if (cacheStore.size >= MAX_CACHE_ENTRIES) {
    // Evict the oldest entry by cachedAt
    let oldestKey: string | undefined;
    let oldestTime = Infinity;
    for (const [key, cached] of cacheStore) {
      if (cached.cachedAt < oldestTime) {
        oldestTime = cached.cachedAt;
        oldestKey = key;
      }
    }
    if (oldestKey) cacheStore.delete(oldestKey);
  }

  cacheStore.set(safeID, entry);
  return entry;
}

export function invalidateLineageCache(workspaceID?: string): void {
  if (workspaceID) {
    cacheStore.delete(workspaceID);
  } else {
    cacheStore.clear();
  }
}

export function getLineageCacheEntry(workspaceID: string): LineageCacheEntry | undefined {
  const entry = cacheStore.get(workspaceID);
  if (!entry) return undefined;
  if (Date.now() - entry.cachedAt >= entry.ttlMs) {
    cacheStore.delete(workspaceID);
    return undefined;
  }
  return entry;
}

// --- Graph traversal helpers ---

export type AncestorNode = {
  nodeID: string;
  nodeName: string;
  nodeType: string;
  depth: number;
};

export function walkUpstream(cache: LineageCacheEntry, startNodeID: string): AncestorNode[] {
  const result: AncestorNode[] = [];
  const visited = new Set<string>();
  const queue: Array<{ nodeID: string; depth: number }> = [{ nodeID: startNodeID, depth: 0 }];

  while (queue.length > 0) {
    const { nodeID, depth } = queue.shift()!;
    if (visited.has(nodeID)) continue;
    visited.add(nodeID);

    if (nodeID !== startNodeID) {
      const node = cache.nodes.get(nodeID);
      if (node) {
        result.push({ nodeID: node.id, nodeName: node.name, nodeType: node.nodeType, depth });
      }
    }

    const upstream = cache.upstreamNodes.get(nodeID);
    if (upstream) {
      for (const upID of upstream) {
        if (!visited.has(upID)) {
          queue.push({ nodeID: upID, depth: depth + 1 });
        }
      }
    }
  }

  return result;
}

export function walkDownstream(cache: LineageCacheEntry, startNodeID: string): AncestorNode[] {
  const result: AncestorNode[] = [];
  const visited = new Set<string>();
  const queue: Array<{ nodeID: string; depth: number }> = [{ nodeID: startNodeID, depth: 0 }];

  while (queue.length > 0) {
    const { nodeID, depth } = queue.shift()!;
    if (visited.has(nodeID)) continue;
    visited.add(nodeID);

    if (nodeID !== startNodeID) {
      const node = cache.nodes.get(nodeID);
      if (node) {
        result.push({ nodeID: node.id, nodeName: node.name, nodeType: node.nodeType, depth });
      }
    }

    const downstream = cache.downstreamNodes.get(nodeID);
    if (downstream) {
      for (const downID of downstream) {
        if (!visited.has(downID)) {
          queue.push({ nodeID: downID, depth: depth + 1 });
        }
      }
    }
  }

  return result;
}

export type ColumnLineageEntry = {
  nodeID: string;
  nodeName: string;
  nodeType: string;
  columnID: string;
  columnName: string;
  direction: "upstream" | "downstream";
  depth: number;
};

export function walkColumnLineage(
  cache: LineageCacheEntry,
  nodeID: string,
  columnID: string
): ColumnLineageEntry[] {
  const result: ColumnLineageEntry[] = [];
  const startKey = columnKey(nodeID, columnID);

  // Walk upstream
  const visitedUp = new Set<string>();
  const queueUp: Array<{ key: string; depth: number }> = [{ key: startKey, depth: 0 }];
  while (queueUp.length > 0) {
    const { key, depth } = queueUp.shift()!;
    if (visitedUp.has(key)) continue;
    visitedUp.add(key);

    if (key !== startKey) {
      const parsed = parseColumnKey(key);
      const node = cache.nodes.get(parsed.nodeID);
      if (node) {
        const col = node.columns.find((c) => c.id === parsed.columnID);
        if (col) {
          result.push({
            nodeID: parsed.nodeID,
            nodeName: node.name,
            nodeType: node.nodeType,
            columnID: parsed.columnID,
            columnName: col.name,
            direction: "upstream",
            depth,
          });
        }
      }
    }

    const upstream = cache.columnUpstream.get(key);
    if (upstream) {
      for (const srcKey of upstream) {
        if (!visitedUp.has(srcKey)) {
          queueUp.push({ key: srcKey, depth: depth + 1 });
        }
      }
    }
  }

  // Walk downstream
  const visitedDown = new Set<string>();
  const queueDown: Array<{ key: string; depth: number }> = [{ key: startKey, depth: 0 }];
  while (queueDown.length > 0) {
    const { key, depth } = queueDown.shift()!;
    if (visitedDown.has(key)) continue;
    visitedDown.add(key);

    if (key !== startKey) {
      const parsed = parseColumnKey(key);
      const node = cache.nodes.get(parsed.nodeID);
      if (node) {
        const col = node.columns.find((c) => c.id === parsed.columnID);
        if (col) {
          result.push({
            nodeID: parsed.nodeID,
            nodeName: node.name,
            nodeType: node.nodeType,
            columnID: parsed.columnID,
            columnName: col.name,
            direction: "downstream",
            depth,
          });
        }
      }
    }

    const downstream = cache.columnDownstream.get(key);
    if (downstream) {
      for (const dstKey of downstream) {
        if (!visitedDown.has(dstKey)) {
          queueDown.push({ key: dstKey, depth: depth + 1 });
        }
      }
    }
  }

  return result;
}

export type ImpactResult = {
  sourceNodeID: string;
  sourceNodeName: string;
  sourceNodeType: string;
  sourceColumnID?: string;
  sourceColumnName?: string;
  impactedNodes: AncestorNode[];
  impactedColumns: ColumnLineageEntry[];
  totalImpactedNodes: number;
  totalImpactedColumns: number;
  byDepth: Record<number, string[]>;
  criticalPath: string[];
};

export function analyzeNodeImpact(
  cache: LineageCacheEntry,
  nodeID: string,
  columnID?: string
): ImpactResult {
  const node = cache.nodes.get(nodeID);
  if (!node) {
    throw new Error(`Node ${nodeID} not found in lineage cache. Ensure the workspace has been cached with detail=true.`);
  }

  let sourceColumnName: string | undefined;
  if (columnID) {
    const col = node.columns.find((c) => c.id === columnID);
    if (!col) {
      const available = node.columns.map((c) => `${c.id} (${c.name})`).join(", ");
      throw new Error(
        `Column ${columnID} not found on node ${nodeID} (${node.name}). Available columns: ${available || "none"}`
      );
    }
    sourceColumnName = col.name;
  }

  // Node-level impact: all downstream nodes
  const impactedNodes = walkDownstream(cache, nodeID);

  // Column-level impact
  let impactedColumns: ColumnLineageEntry[] = [];
  if (columnID) {
    impactedColumns = walkColumnLineage(cache, nodeID, columnID).filter(
      (e) => e.direction === "downstream"
    );
  } else {
    // All columns on this node → trace each downstream
    for (const col of node.columns) {
      const downstream = walkColumnLineage(cache, nodeID, col.id).filter(
        (e) => e.direction === "downstream"
      );
      impactedColumns.push(...downstream);
    }
    // Deduplicate
    const seen = new Set<string>();
    impactedColumns = impactedColumns.filter((e) => {
      const key = columnKey(e.nodeID, e.columnID);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  // Group by depth
  const byDepth: Record<number, string[]> = {};
  for (const n of impactedNodes) {
    if (!byDepth[n.depth]) byDepth[n.depth] = [];
    byDepth[n.depth].push(n.nodeName);
  }

  // Critical path: longest path from source to leaf
  const criticalPath = findCriticalPath(cache, nodeID);

  return {
    sourceNodeID: nodeID,
    sourceNodeName: node.name,
    sourceNodeType: node.nodeType,
    ...(columnID ? { sourceColumnID: columnID } : {}),
    ...(sourceColumnName ? { sourceColumnName } : {}),
    impactedNodes,
    impactedColumns,
    totalImpactedNodes: impactedNodes.length,
    totalImpactedColumns: impactedColumns.length,
    byDepth,
    criticalPath,
  };
}

function findCriticalPath(cache: LineageCacheEntry, startNodeID: string): string[] {
  const node = cache.nodes.get(startNodeID);
  if (!node) return [];

  // Collect reachable downstream subgraph via BFS
  const reachable = new Set<string>();
  const bfsQueue = [startNodeID];
  while (bfsQueue.length > 0) {
    const id = bfsQueue.shift()!;
    if (reachable.has(id)) continue;
    reachable.add(id);
    const downstream = cache.downstreamNodes.get(id);
    if (downstream) {
      for (const nextID of downstream) {
        if (!reachable.has(nextID)) bfsQueue.push(nextID);
      }
    }
  }

  // Topological sort (Kahn's algorithm) over the reachable subgraph
  const inDegree = new Map<string, number>();
  for (const id of reachable) inDegree.set(id, 0);
  for (const id of reachable) {
    const downstream = cache.downstreamNodes.get(id);
    if (downstream) {
      for (const nextID of downstream) {
        if (reachable.has(nextID)) {
          inDegree.set(nextID, (inDegree.get(nextID) ?? 0) + 1);
        }
      }
    }
  }
  const topoQueue: string[] = [];
  for (const [id, deg] of inDegree) {
    if (deg === 0) topoQueue.push(id);
  }
  const topoOrder: string[] = [];
  while (topoQueue.length > 0) {
    const id = topoQueue.shift()!;
    topoOrder.push(id);
    const downstream = cache.downstreamNodes.get(id);
    if (downstream) {
      for (const nextID of downstream) {
        if (!reachable.has(nextID)) continue;
        const newDeg = (inDegree.get(nextID) ?? 1) - 1;
        inDegree.set(nextID, newDeg);
        if (newDeg === 0) topoQueue.push(nextID);
      }
    }
  }

  // DP longest path from startNodeID — O(V+E)
  const dist = new Map<string, number>();
  const predecessor = new Map<string, string>();
  dist.set(startNodeID, 0);

  for (const id of topoOrder) {
    const d = dist.get(id);
    if (d === undefined) continue; // not reachable from start
    const downstream = cache.downstreamNodes.get(id);
    if (downstream) {
      for (const nextID of downstream) {
        if (!reachable.has(nextID)) continue;
        if (d + 1 > (dist.get(nextID) ?? -1)) {
          dist.set(nextID, d + 1);
          predecessor.set(nextID, id);
        }
      }
    }
  }

  // Find the farthest node
  let farthestID = startNodeID;
  let maxDist = 0;
  for (const [id, d] of dist) {
    if (d > maxDist) {
      maxDist = d;
      farthestID = id;
    }
  }

  // Reconstruct path
  const pathIDs: string[] = [];
  let cur: string | undefined = farthestID;
  while (cur !== undefined) {
    pathIDs.push(cur);
    cur = predecessor.get(cur);
  }
  pathIDs.reverse();

  return pathIDs.map((id) => cache.nodes.get(id)?.name ?? id);
}

export type PropagationChange = {
  columnName?: string;
  dataType?: string;
};

export type PropagationResult = {
  sourceNodeID: string;
  sourceColumnID: string;
  changes: PropagationChange;
  updatedNodes: Array<{
    nodeID: string;
    nodeName: string;
    columnID: string;
    columnName: string;
    previousName?: string;
    previousDataType?: string;
  }>;
  totalUpdated: number;
  errors: Array<{ nodeID: string; columnID: string; message: string }>;
};

export async function propagateColumnChange(
  client: CoalesceClient,
  cache: LineageCacheEntry,
  workspaceID: string,
  nodeID: string,
  columnID: string,
  changes: PropagationChange,
  reportProgress?: WorkflowProgressReporter
): Promise<PropagationResult> {
  if (!changes.columnName && !changes.dataType) {
    throw new Error("At least one change (columnName or dataType) must be specified");
  }

  const node = cache.nodes.get(nodeID);
  if (!node) {
    throw new Error(`Node ${nodeID} not found in lineage cache`);
  }

  const sourceCol = node.columns.find((c) => c.id === columnID);
  if (!sourceCol) {
    throw new Error(`Column ${columnID} not found on node ${nodeID} (${node.name})`);
  }

  // Find all downstream columns that reference this column
  const downstreamEntries = walkColumnLineage(cache, nodeID, columnID).filter(
    (e) => e.direction === "downstream"
  );

  const updatedNodes: PropagationResult["updatedNodes"] = [];
  const errors: PropagationResult["errors"] = [];
  const safeWorkspaceID = validatePathSegment(workspaceID, "workspaceID");

  for (let i = 0; i < downstreamEntries.length; i++) {
    const entry = downstreamEntries[i];

    if (reportProgress) {
      await reportProgress(
        `Updating column ${entry.columnName} on ${entry.nodeName} (${i + 1}/${downstreamEntries.length})`,
        downstreamEntries.length
      );
    }

    try {
      // Fetch current node state
      const currentNode = await client.get(
        `/api/v1/workspaces/${safeWorkspaceID}/nodes/${validatePathSegment(entry.nodeID, "nodeID")}`,
        {}
      );

      if (!isPlainObject(currentNode) || !isPlainObject(currentNode.metadata)) {
        errors.push({ nodeID: entry.nodeID, columnID: entry.columnID, message: "Could not read node metadata" });
        continue;
      }

      const metadata = currentNode.metadata as Record<string, unknown>;
      const columns = Array.isArray(metadata.columns) ? [...metadata.columns] : [];
      const colIndex = columns.findIndex(
        (c) =>
          isPlainObject(c) &&
          (c.id === entry.columnID || c.columnID === entry.columnID)
      );

      if (colIndex === -1) {
        errors.push({ nodeID: entry.nodeID, columnID: entry.columnID, message: "Column not found in current node state" });
        continue;
      }

      const col = columns[colIndex] as Record<string, unknown>;
      const previousName = typeof col.name === "string" ? col.name : undefined;
      const previousDataType = typeof col.dataType === "string" ? col.dataType : undefined;

      const updatedCol = { ...col };
      if (changes.columnName) updatedCol.name = changes.columnName;
      if (changes.dataType) updatedCol.dataType = changes.dataType;
      columns[colIndex] = updatedCol;

      // Update the node via the standard validation pipeline
      const body = buildUpdatedWorkspaceNodeBody(currentNode, { metadata: { columns } });
      await setWorkspaceNode(client, {
        workspaceID: safeWorkspaceID,
        nodeID: validatePathSegment(entry.nodeID, "nodeID"),
        body,
      });

      updatedNodes.push({
        nodeID: entry.nodeID,
        nodeName: entry.nodeName,
        columnID: entry.columnID,
        columnName: changes.columnName ?? entry.columnName,
        ...(previousName ? { previousName } : {}),
        ...(previousDataType ? { previousDataType } : {}),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push({ nodeID: entry.nodeID, columnID: entry.columnID, message });
    }
  }

  // Invalidate cache after writes
  invalidateLineageCache(workspaceID);

  return {
    sourceNodeID: nodeID,
    sourceColumnID: columnID,
    changes,
    updatedNodes,
    totalUpdated: updatedNodes.length,
    errors,
  };
}
