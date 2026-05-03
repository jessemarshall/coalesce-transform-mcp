import { randomUUID } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import type { CoalesceClient, QueryParams } from "../../client.js";
import { listEnvironmentNodes, listWorkspaceNodes } from "../../coalesce/api/nodes.js";
import { listRuns } from "../../coalesce/api/runs.js";
import { listOrgUsers } from "../../coalesce/api/users.js";
import { sanitizeResponse, validatePathSegment } from "../../coalesce/types.js";
import type { NodeSummary } from "../workspace/analysis.js";
import { isPlainObject } from "../../utils.js";
import { CACHE_DIR_NAME, getCacheBaseDir } from "../../cache-dir.js";
import { DEFAULT_PAGE_SIZE, getDetailFetchTimeoutMs, type RunStatus } from "../../constants.js";

type PaginatedParams = {
  pageSize?: number;
  orderBy?: string;
  orderByDirection?: "asc" | "desc";
};

type FetchPage = (params: QueryParams) => Promise<unknown>;

type CollectionPage = {
  data: unknown[];
  next?: string;
};

type PaginatedCollectionResult = {
  items: unknown[];
  pageCount: number;
  pageSize: number;
  orderBy: string;
  orderByDirection?: "asc" | "desc";
};

type CacheWriteOptions = {
  baseDir?: string;
};

/**
 * Defense-in-depth cap on pagination loops. The seen-cursors check catches
 * perfect cycles, but an API returning unique cursors indefinitely (bug or
 * misbehavior) would otherwise exhaust memory or fill the disk before any
 * error trips. Mirrors the protection in `services/lineage/lineage-cache.ts`
 * and `services/cache/workspace-node-index.ts`.
 */
const MAX_PAGES = 500;

function parseCollectionPage(response: unknown): CollectionPage {
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

async function fetchAllPaginatedToMemory(
  fetchPage: FetchPage,
  baseParams: QueryParams,
  params: PaginatedParams
): Promise<PaginatedCollectionResult> {
  const items: unknown[] = [];
  const seenCursors = new Set<string>();
  const pageSize = Math.max(1, Math.floor(params.pageSize ?? DEFAULT_PAGE_SIZE));
  const orderBy = params.orderBy ?? "id";
  const orderByDirection = params.orderByDirection;

  let next: string | undefined;
  let isFirstPage = true;
  let pageCount = 0;

  while (isFirstPage || next) {
    if (pageCount >= MAX_PAGES) {
      throw new Error(
        `Pagination exceeded ${MAX_PAGES} pages (${items.length} items collected). ` +
          `This likely indicates an API bug returning unique cursors indefinitely.`
      );
    }
    const response = await fetchPage({
      ...baseParams,
      limit: pageSize,
      orderBy,
      ...(orderByDirection ? { orderByDirection } : {}),
      ...(next ? { startingFrom: next } : {}),
    });

    const page = parseCollectionPage(response);
    items.push(...page.data);
    pageCount += 1;

    if (page.next) {
      if (seenCursors.has(page.next)) {
        throw new Error(`Pagination repeated cursor ${page.next}`);
      }
      seenCursors.add(page.next);
    }

    next = page.next;
    isFirstPage = false;
  }

  return {
    items,
    pageCount,
    pageSize,
    orderBy,
    ...(orderByDirection ? { orderByDirection } : {}),
  };
}

type StreamToDiskOptions = {
  ndjsonPath: string;
  metaPath: string;
  itemTransform?: (item: unknown) => unknown;
};

type StreamToDiskResult = {
  totalItems: number;
  pageCount: number;
  pageSize: number;
  orderBy: string;
  orderByDirection?: "asc" | "desc";
  cachedAt: string;
};

type SnapshotPromotionFs = {
  existsSync: typeof existsSync;
  renameSync: typeof renameSync;
  rmSync: typeof rmSync;
};

export function promoteSnapshotArtifacts(
  tempNdjsonPath: string,
  ndjsonPath: string,
  tempMetaPath: string,
  metaPath: string,
  fsOps: SnapshotPromotionFs = {
    existsSync,
    renameSync,
    rmSync,
  }
): void {
  const { existsSync, renameSync, rmSync } = fsOps;
  const backupSuffix = `.bak-${process.pid}-${randomUUID()}`;
  const ndjsonBackupPath = `${ndjsonPath}${backupSuffix}`;
  const metaBackupPath = `${metaPath}${backupSuffix}`;
  const hadNdjson = existsSync(ndjsonPath);
  const hadMeta = existsSync(metaPath);

  try {
    // Move any current pair out of the final location first so readers never see
    // a mixed generation where one file is new and the other is stale.
    if (hadNdjson) {
      renameSync(ndjsonPath, ndjsonBackupPath);
    }
    if (hadMeta) {
      renameSync(metaPath, metaBackupPath);
    }

    renameSync(tempNdjsonPath, ndjsonPath);
    renameSync(tempMetaPath, metaPath);

    if (hadNdjson) {
      rmSync(ndjsonBackupPath, { force: true });
    }
    if (hadMeta) {
      rmSync(metaBackupPath, { force: true });
    }
  } catch (error) {
    // Remove any partially promoted new files before restoring the previous pair.
    if (existsSync(ndjsonPath) && existsSync(ndjsonBackupPath)) {
      rmSync(ndjsonPath, { force: true });
    }
    if (existsSync(metaPath) && existsSync(metaBackupPath)) {
      rmSync(metaPath, { force: true });
    }

    if (existsSync(ndjsonBackupPath)) {
      renameSync(ndjsonBackupPath, ndjsonPath);
    }
    if (existsSync(metaBackupPath)) {
      renameSync(metaBackupPath, metaPath);
    }

    throw error;
  }
}

export async function streamAllPaginatedToDisk(
  fetchPage: FetchPage,
  baseParams: QueryParams,
  params: PaginatedParams,
  options: StreamToDiskOptions
): Promise<StreamToDiskResult> {
  const { ndjsonPath, metaPath, itemTransform } = options;
  const seenCursors = new Set<string>();
  const pageSize = Math.max(1, Math.floor(params.pageSize ?? DEFAULT_PAGE_SIZE));
  const orderBy = params.orderBy ?? "id";
  const orderByDirection = params.orderByDirection;
  const cachedAt = new Date().toISOString();
  const tempSuffix = `.tmp-${process.pid}-${randomUUID()}`;
  const tempNdjsonPath = `${ndjsonPath}${tempSuffix}`;
  const tempMetaPath = `${metaPath}${tempSuffix}`;

  // Ensure parent directories exist
  mkdirSync(dirname(ndjsonPath), { recursive: true });
  mkdirSync(dirname(metaPath), { recursive: true });

  // Start with isolated temp files so failed streams never leave partial snapshots behind.
  writeFileSync(tempNdjsonPath, "", "utf8");

  try {
    let totalItems = 0;
    let next: string | undefined;
    let isFirstPage = true;
    let pageCount = 0;

    while (isFirstPage || next) {
      if (pageCount >= MAX_PAGES) {
        throw new Error(
          `Pagination exceeded ${MAX_PAGES} pages (${totalItems} items streamed). ` +
            `This likely indicates an API bug returning unique cursors indefinitely.`
        );
      }
      const response = await fetchPage({
        ...baseParams,
        limit: pageSize,
        orderBy,
        ...(orderByDirection ? { orderByDirection } : {}),
        ...(next ? { startingFrom: next } : {}),
      });

      const page = parseCollectionPage(response);
      pageCount += 1;

      // Write each item as a single NDJSON line
      for (const item of page.data) {
        const transformed = itemTransform ? itemTransform(item) : item;
        appendFileSync(tempNdjsonPath, JSON.stringify(transformed) + "\n", "utf8");
        totalItems += 1;
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

    // Write meta file only on successful completion, then promote both files into place.
    const meta: StreamToDiskResult = {
      totalItems,
      pageCount,
      pageSize,
      orderBy,
      ...(orderByDirection ? { orderByDirection } : {}),
      cachedAt,
    };
    writeFileSync(tempMetaPath, JSON.stringify(meta, null, 2) + "\n", "utf8");
    promoteSnapshotArtifacts(tempNdjsonPath, ndjsonPath, tempMetaPath, metaPath);

    return meta;
  } catch (error) {
    rmSync(tempNdjsonPath, { force: true });
    rmSync(tempMetaPath, { force: true });
    throw error;
  }
}

function ensureDirectory(...parts: string[]): string {
  const directory = join(...parts);
  mkdirSync(directory, { recursive: true });
  return directory;
}

function buildNodeCacheRelativePath(
  scope: "workspace" | "environment",
  id: string,
  detail: boolean
): { scopeDir: string; fileName: string } {
  const safeID = validatePathSegment(id, `${scope}ID`);
  const scopeDir = `${scope}-${safeID}`;
  const fileName = detail ? "nodes.ndjson" : "nodes-summary.ndjson";
  return { scopeDir, fileName };
}

function buildRunsCacheFileName(params: {
  runType?: string;
  runStatus?: string;
  environmentID?: string;
  detail?: boolean;
}): string {
  const parts = ["runs"];
  if (params.environmentID) {
    parts.push(`env-${validatePathSegment(params.environmentID, "environmentID")}`);
  }
  if (params.runType) {
    parts.push(params.runType);
  }
  if (params.runStatus) {
    parts.push(params.runStatus);
  }
  parts.push(params.detail === true ? "detail" : "summary");
  return `${parts.join("-")}.ndjson`;
}

export async function fetchAllWorkspaceNodes(
  client: CoalesceClient,
  params: { workspaceID: string; detail?: boolean } & PaginatedParams
): Promise<PaginatedCollectionResult> {
  const requestOptions = params.detail ? { timeoutMs: getDetailFetchTimeoutMs() } : undefined;
  return fetchAllPaginatedToMemory(
    (queryParams) => listWorkspaceNodes(client, queryParams as QueryParams & { workspaceID: string }, requestOptions),
    {
      workspaceID: validatePathSegment(params.workspaceID, "workspaceID"),
      ...(params.detail !== undefined ? { detail: params.detail } : {}),
    },
    params
  );
}

export async function fetchAllEnvironmentNodes(
  client: CoalesceClient,
  params: { environmentID: string; detail?: boolean } & PaginatedParams
): Promise<PaginatedCollectionResult> {
  const requestOptions = params.detail ? { timeoutMs: getDetailFetchTimeoutMs() } : undefined;
  return fetchAllPaginatedToMemory(
    (queryParams) =>
      listEnvironmentNodes(client, queryParams as QueryParams & { environmentID: string }, requestOptions),
    {
      environmentID: validatePathSegment(params.environmentID, "environmentID"),
      ...(params.detail !== undefined ? { detail: params.detail } : {}),
    },
    params
  );
}

export async function fetchAllRuns(
  client: CoalesceClient,
  params: {
    runType?: "deploy" | "refresh";
    runStatus?: RunStatus;
    environmentID?: string;
    detail?: boolean;
  } & PaginatedParams
): Promise<PaginatedCollectionResult> {
  return fetchAllPaginatedToMemory(
    (queryParams) => listRuns(client, queryParams),
    {
      ...(params.runType ? { runType: params.runType } : {}),
      ...(params.runStatus ? { runStatus: params.runStatus } : {}),
      ...(params.environmentID
        ? { environmentID: validatePathSegment(params.environmentID, "environmentID") }
        : {}),
      ...(params.detail !== undefined ? { detail: params.detail } : {}),
    },
    params
  );
}

export async function fetchAllOrgUsers(
  client: CoalesceClient,
  params: PaginatedParams
): Promise<PaginatedCollectionResult> {
  return fetchAllPaginatedToMemory((queryParams) => listOrgUsers(client, queryParams), {}, params);
}

export function toNodeSummaries(nodes: unknown[]): NodeSummary[] {
  return nodes.flatMap((node) => {
    if (!isPlainObject(node)) {
      return [];
    }
    if (typeof node.nodeType !== "string" || typeof node.name !== "string") {
      return [];
    }
    return [
      {
        nodeType: node.nodeType,
        name: node.name,
      },
    ];
  });
}

export async function cacheWorkspaceNodes(
  client: CoalesceClient,
  params: { workspaceID: string; detail?: boolean } & PaginatedParams,
  options?: CacheWriteOptions
): Promise<{
  workspaceID: string;
  detail: boolean;
  totalNodes: number;
  pageCount: number;
  pageSize: number;
  orderBy: string;
  orderByDirection?: "asc" | "desc";
  filePath: string;
  metaPath: string;
  cachedAt: string;
}> {
  const detail = params.detail ?? true;
  const baseDir = getCacheBaseDir(options?.baseDir);
  const { scopeDir, fileName } = buildNodeCacheRelativePath("workspace", params.workspaceID, detail);
  const directory = ensureDirectory(baseDir, CACHE_DIR_NAME, scopeDir, "nodes");
  const ndjsonPath = join(directory, fileName);
  const metaPath = join(directory, fileName.replace(/\.ndjson$/, ".meta.json"));

  const requestOptions = detail ? { timeoutMs: getDetailFetchTimeoutMs() } : undefined;
  const result = await streamAllPaginatedToDisk(
    (queryParams) =>
      listWorkspaceNodes(client, queryParams as QueryParams & { workspaceID: string }, requestOptions),
    {
      workspaceID: validatePathSegment(params.workspaceID, "workspaceID"),
      ...(detail !== undefined ? { detail } : {}),
    },
    params,
    { ndjsonPath, metaPath }
  );

  return {
    workspaceID: params.workspaceID,
    detail,
    totalNodes: result.totalItems,
    pageCount: result.pageCount,
    pageSize: result.pageSize,
    orderBy: result.orderBy,
    ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
    filePath: ndjsonPath,
    metaPath,
    cachedAt: result.cachedAt,
  };
}

export async function cacheEnvironmentNodes(
  client: CoalesceClient,
  params: { environmentID: string; detail?: boolean } & PaginatedParams,
  options?: CacheWriteOptions
): Promise<{
  environmentID: string;
  detail: boolean;
  totalNodes: number;
  pageCount: number;
  pageSize: number;
  orderBy: string;
  orderByDirection?: "asc" | "desc";
  filePath: string;
  metaPath: string;
  cachedAt: string;
}> {
  const detail = params.detail ?? true;
  const baseDir = getCacheBaseDir(options?.baseDir);
  const { scopeDir, fileName } = buildNodeCacheRelativePath("environment", params.environmentID, detail);
  const directory = ensureDirectory(baseDir, CACHE_DIR_NAME, scopeDir, "nodes");
  const ndjsonPath = join(directory, fileName);
  const metaPath = join(directory, fileName.replace(/\.ndjson$/, ".meta.json"));

  const requestOptions = detail ? { timeoutMs: getDetailFetchTimeoutMs() } : undefined;
  const result = await streamAllPaginatedToDisk(
    (queryParams) =>
      listEnvironmentNodes(client, queryParams as QueryParams & { environmentID: string }, requestOptions),
    {
      environmentID: validatePathSegment(params.environmentID, "environmentID"),
      ...(detail !== undefined ? { detail } : {}),
    },
    params,
    { ndjsonPath, metaPath }
  );

  return {
    environmentID: params.environmentID,
    detail,
    totalNodes: result.totalItems,
    pageCount: result.pageCount,
    pageSize: result.pageSize,
    orderBy: result.orderBy,
    ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
    filePath: ndjsonPath,
    metaPath,
    cachedAt: result.cachedAt,
  };
}

export async function cacheRuns(
  client: CoalesceClient,
  params: {
    runType?: "deploy" | "refresh";
    runStatus?: RunStatus;
    environmentID?: string;
    detail?: boolean;
  } & PaginatedParams,
  options?: CacheWriteOptions
): Promise<{
  detail: boolean;
  totalRuns: number;
  pageCount: number;
  pageSize: number;
  orderBy: string;
  orderByDirection?: "asc" | "desc";
  filePath: string;
  metaPath: string;
  cachedAt: string;
  runType?: "deploy" | "refresh";
  runStatus?: RunStatus;
  environmentID?: string;
}> {
  const detail = params.detail ?? false;
  const baseDir = getCacheBaseDir(options?.baseDir);
  const directory = ensureDirectory(baseDir, CACHE_DIR_NAME, "runs");
  const baseName = buildRunsCacheFileName({ ...params, detail });
  const ndjsonPath = join(directory, baseName);
  const metaPath = join(directory, baseName.replace(/\.ndjson$/, ".meta.json"));

  const result = await streamAllPaginatedToDisk(
    (queryParams) => listRuns(client, queryParams),
    {
      ...(params.runType ? { runType: params.runType } : {}),
      ...(params.runStatus ? { runStatus: params.runStatus } : {}),
      ...(params.environmentID
        ? { environmentID: validatePathSegment(params.environmentID, "environmentID") }
        : {}),
      ...(detail !== undefined ? { detail } : {}),
    },
    params,
    {
      ndjsonPath,
      metaPath,
      itemTransform: (item) => sanitizeResponse(item),
    }
  );

  return {
    detail,
    totalRuns: result.totalItems,
    pageCount: result.pageCount,
    pageSize: result.pageSize,
    orderBy: result.orderBy,
    ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
    filePath: ndjsonPath,
    metaPath,
    cachedAt: result.cachedAt,
    ...(params.runType ? { runType: params.runType } : {}),
    ...(params.runStatus ? { runStatus: params.runStatus } : {}),
    ...(params.environmentID ? { environmentID: params.environmentID } : {}),
  };
}

export async function cacheOrgUsers(
  client: CoalesceClient,
  params: PaginatedParams,
  options?: CacheWriteOptions
): Promise<{
  totalUsers: number;
  pageCount: number;
  pageSize: number;
  orderBy: string;
  orderByDirection?: "asc" | "desc";
  filePath: string;
  metaPath: string;
  cachedAt: string;
}> {
  const baseDir = getCacheBaseDir(options?.baseDir);
  const directory = ensureDirectory(baseDir, CACHE_DIR_NAME, "users");
  const ndjsonPath = join(directory, "org-users.ndjson");
  const metaPath = join(directory, "org-users.meta.json");

  // Intentionally no `itemTransform: sanitizeResponse` here — org-user
  // records don't carry credentials. cacheRuns sanitizes because run payloads
  // embed Snowflake auth from the refresh request. If a future API change
  // starts returning OAuth state or tokens on org-users, add the transform
  // and a parallel test under tests/services/snapshots.test.ts.
  const result = await streamAllPaginatedToDisk(
    (queryParams) => listOrgUsers(client, queryParams),
    {},
    params,
    { ndjsonPath, metaPath }
  );

  return {
    totalUsers: result.totalItems,
    pageCount: result.pageCount,
    pageSize: result.pageSize,
    orderBy: result.orderBy,
    ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
    filePath: ndjsonPath,
    metaPath,
    cachedAt: result.cachedAt,
  };
}
