import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import type { CoalesceClient, QueryParams } from "../../client.js";
import { listEnvironmentNodes, listWorkspaceNodes } from "../../coalesce/api/nodes.js";
import { listRuns } from "../../coalesce/api/runs.js";
import { listOrgUsers } from "../../coalesce/api/users.js";
import { sanitizeResponse, validatePathSegment } from "../../coalesce/types.js";
import type { NodeSummary } from "../workspace/analysis.js";
import { isPlainObject } from "../../utils.js";
import { CACHE_DIR_NAME } from "../../cache-dir.js";

const DEFAULT_PAGE_SIZE = 250;

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

function parseCollectionPage(response: unknown): CollectionPage {
  if (!isPlainObject(response)) {
    throw new Error("Paginated collection response was not an object");
  }

  return {
    data: Array.isArray(response.data) ? response.data : [],
    next:
      typeof response.next === "string" && response.next.trim().length > 0
        ? response.next
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

  // Ensure parent directory exists
  mkdirSync(dirname(ndjsonPath), { recursive: true });

  // Write empty file to start (truncates any previous file)
  writeFileSync(ndjsonPath, "", "utf8");

  let totalItems = 0;
  let next: string | undefined;
  let isFirstPage = true;
  let pageCount = 0;

  while (isFirstPage || next) {
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
      appendFileSync(ndjsonPath, JSON.stringify(transformed) + "\n", "utf8");
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

  // Write meta file only on successful completion
  const meta: StreamToDiskResult = {
    totalItems,
    pageCount,
    pageSize,
    orderBy,
    ...(orderByDirection ? { orderByDirection } : {}),
    cachedAt,
  };
  writeFileSync(metaPath, JSON.stringify(meta, null, 2) + "\n", "utf8");

  return meta;
}

function ensureDirectory(...parts: string[]): string {
  const directory = join(...parts);
  mkdirSync(directory, { recursive: true });
  return directory;
}

function buildNodeCacheFileName(
  scope: "workspace" | "environment",
  id: string,
  detail: boolean
): string {
  const safeID = validatePathSegment(id, `${scope}ID`);
  if (detail) {
    return `${scope}-${safeID}-nodes.ndjson`;
  }
  return `${scope}-${safeID}-nodes-summary.ndjson`;
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
  return fetchAllPaginatedToMemory(
    (queryParams) => listWorkspaceNodes(client, queryParams as QueryParams & { workspaceID: string }),
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
  return fetchAllPaginatedToMemory(
    (queryParams) =>
      listEnvironmentNodes(client, queryParams as QueryParams & { environmentID: string }),
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
    runStatus?: "completed" | "failed" | "canceled" | "running" | "waitingToRun";
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
  const baseDir = options?.baseDir ?? process.cwd();
  const directory = ensureDirectory(baseDir, CACHE_DIR_NAME, "nodes");
  const baseName = buildNodeCacheFileName("workspace", params.workspaceID, detail);
  const ndjsonPath = join(directory, baseName);
  const metaPath = join(directory, baseName.replace(/\.ndjson$/, ".meta.json"));

  const result = await streamAllPaginatedToDisk(
    (queryParams) =>
      listWorkspaceNodes(client, queryParams as QueryParams & { workspaceID: string }),
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
  const baseDir = options?.baseDir ?? process.cwd();
  const directory = ensureDirectory(baseDir, CACHE_DIR_NAME, "nodes");
  const baseName = buildNodeCacheFileName("environment", params.environmentID, detail);
  const ndjsonPath = join(directory, baseName);
  const metaPath = join(directory, baseName.replace(/\.ndjson$/, ".meta.json"));

  const result = await streamAllPaginatedToDisk(
    (queryParams) =>
      listEnvironmentNodes(client, queryParams as QueryParams & { environmentID: string }),
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
    runStatus?: "completed" | "failed" | "canceled" | "running" | "waitingToRun";
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
  runStatus?: "completed" | "failed" | "canceled" | "running" | "waitingToRun";
  environmentID?: string;
}> {
  const detail = params.detail ?? false;
  const baseDir = options?.baseDir ?? process.cwd();
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
  const baseDir = options?.baseDir ?? process.cwd();
  const directory = ensureDirectory(baseDir, CACHE_DIR_NAME, "users");
  const ndjsonPath = join(directory, "org-users.ndjson");
  const metaPath = join(directory, "org-users.meta.json");

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
