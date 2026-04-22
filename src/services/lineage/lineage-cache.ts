import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { CoalesceClient, QueryParams } from "../../client.js";
import { listWorkspaceNodes } from "../../coalesce/api/nodes.js";
import { validatePathSegment } from "../../coalesce/types.js";
import { isPlainObject } from "../../utils.js";
import { CACHE_DIR_NAME, getCacheBaseDir } from "../../cache-dir.js";
import type { WorkflowProgressReporter } from "../../workflows/progress.js";
import { DEFAULT_PAGE_SIZE, getDetailFetchTimeoutMs } from "../../constants.js";

const DEFAULT_TTL_MS = 30 * 60 * 1000; // 30 minutes
const MAX_CACHE_ENTRIES = 50;
const MAX_PAGES = 500;
const PROGRESS_INTERVAL = 500;

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

export function columnKey(nodeID: string, columnID: string): string {
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
  const candidatePaths = [
    {
      ndjsonPath: join(baseDir, CACHE_DIR_NAME, `workspace-${safeID}`, "nodes", "nodes.ndjson"),
      metaPath: join(baseDir, CACHE_DIR_NAME, `workspace-${safeID}`, "nodes", "nodes.meta.json"),
    },
    {
      ndjsonPath: join(baseDir, CACHE_DIR_NAME, safeID, "nodes", "nodes.ndjson"),
      metaPath: join(baseDir, CACHE_DIR_NAME, safeID, "nodes", "nodes.meta.json"),
    },
    {
      ndjsonPath: join(baseDir, CACHE_DIR_NAME, "nodes", `workspace-${safeID}-nodes.ndjson`),
      metaPath: join(baseDir, CACHE_DIR_NAME, "nodes", `workspace-${safeID}-nodes.meta.json`),
    },
  ];
  const snapshot = candidatePaths.find(
    ({ ndjsonPath, metaPath }) => existsSync(ndjsonPath) && existsSync(metaPath)
  );
  if (!snapshot) return null;

  try {
    const meta = JSON.parse(readFileSync(snapshot.metaPath, "utf8"));
    if (!isPlainObject(meta) || typeof meta.cachedAt !== "string") return null;

    // Check if snapshot is within TTL
    const cachedAt = new Date(meta.cachedAt).getTime();
    if (Date.now() - cachedAt > getLineageTtlMs()) return null;

    const lines = readFileSync(snapshot.ndjsonPath, "utf8").trimEnd().split("\n");
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
  if ("data" in response && !Array.isArray(response.data)) {
    throw new Error(
      `Paginated collection response.data was ${typeof response.data}, expected array`
    );
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
  let pageCount = 0;

  while (isFirstPage || next) {
    if (++pageCount > MAX_PAGES) {
      throw new Error(
        `Workspace node pagination exceeded ${MAX_PAGES} pages (${items.length} nodes fetched). ` +
        `This likely indicates an API bug. The nodes fetched so far are not returned.`
      );
    }
    const response = await listWorkspaceNodes(client, {
      workspaceID: safeID,
      detail: true,
      limit: DEFAULT_PAGE_SIZE,
      orderBy: "id",
      ...(next ? { startingFrom: next } : {}),
    } as QueryParams & { workspaceID: string }, { timeoutMs: getDetailFetchTimeoutMs() });

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

  const baseDir = getCacheBaseDir(options.baseDir);

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

// Re-export traversal, search, and propagation modules so existing imports continue to work
export { walkUpstream, walkDownstream, walkColumnLineage, analyzeNodeImpact } from "./lineage-traversal.js";
export type { LineageGraphNode, ColumnLineageEntry, ImpactResult } from "./lineage-traversal.js";

export { searchWorkspaceContent } from "./lineage-search.js";
export type { SearchField, WorkspaceSearchParams, SearchMatch, WorkspaceSearchResult } from "./lineage-search.js";

export { propagateColumnChange } from "./lineage-propagation.js";
export type { PropagationChange, PropagationResult, PreMutationSnapshotEntry, PreMutationSnapshotSummary } from "./lineage-propagation.js";

export { auditDocumentationCoverage } from "./lineage-documentation.js";
export type { DocumentationAuditResult } from "./lineage-documentation.js";
