import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { CoalesceClient, QueryParams } from "../../client.js";
import { listEnvironmentNodes, listWorkspaceNodes } from "../../coalesce/api/nodes.js";
import { listRuns } from "../../coalesce/api/runs.js";
import { listOrgUsers } from "../../coalesce/api/users.js";
import { sanitizeResponse, validatePathSegment } from "../../coalesce/types.js";
import type { NodeSummary } from "../workspace/analysis.js";
import { isPlainObject } from "../../utils.js";

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

async function fetchAllPaginated(
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

function ensureDirectory(...parts: string[]): string {
  const directory = join(...parts);
  mkdirSync(directory, { recursive: true });
  return directory;
}

function writeSnapshotFile(
  relativeDirectory: string[],
  fileName: string,
  body: unknown,
  options?: CacheWriteOptions
): string {
  const baseDir = options?.baseDir ?? process.cwd();
  const directory = ensureDirectory(baseDir, "data", ...relativeDirectory);
  const filePath = join(directory, fileName);
  writeFileSync(filePath, `${JSON.stringify(body, null, 2)}\n`, "utf8");
  return filePath;
}

function buildNodeCacheFileName(
  scope: "workspace" | "environment",
  id: string,
  detail: boolean
): string {
  const safeID = validatePathSegment(id, `${scope}ID`);
  if (detail) {
    return `${scope}-${safeID}-nodes.json`;
  }
  return `${scope}-${safeID}-nodes-summary.json`;
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
  return `${parts.join("-")}.json`;
}

export async function fetchAllWorkspaceNodes(
  client: CoalesceClient,
  params: { workspaceID: string; detail?: boolean } & PaginatedParams
): Promise<PaginatedCollectionResult> {
  return fetchAllPaginated(
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
  return fetchAllPaginated(
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
  return fetchAllPaginated(
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
  return fetchAllPaginated((queryParams) => listOrgUsers(client, queryParams), {}, params);
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
  cachedAt: string;
}> {
  const detail = params.detail ?? true;
  const cachedAt = new Date().toISOString();
  const result = await fetchAllWorkspaceNodes(client, { ...params, detail });
  const filePath = writeSnapshotFile(
    ["nodes"],
    buildNodeCacheFileName("workspace", params.workspaceID, detail),
    {
      cachedAt,
      scope: "workspace",
      workspaceID: params.workspaceID,
      detail,
      totalNodes: result.items.length,
      pageCount: result.pageCount,
      pageSize: result.pageSize,
      orderBy: result.orderBy,
      ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
      nodes: result.items,
    },
    options
  );

  return {
    workspaceID: params.workspaceID,
    detail,
    totalNodes: result.items.length,
    pageCount: result.pageCount,
    pageSize: result.pageSize,
    orderBy: result.orderBy,
    ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
    filePath,
    cachedAt,
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
  cachedAt: string;
}> {
  const detail = params.detail ?? true;
  const cachedAt = new Date().toISOString();
  const result = await fetchAllEnvironmentNodes(client, { ...params, detail });
  const filePath = writeSnapshotFile(
    ["nodes"],
    buildNodeCacheFileName("environment", params.environmentID, detail),
    {
      cachedAt,
      scope: "environment",
      environmentID: params.environmentID,
      detail,
      totalNodes: result.items.length,
      pageCount: result.pageCount,
      pageSize: result.pageSize,
      orderBy: result.orderBy,
      ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
      nodes: result.items,
    },
    options
  );

  return {
    environmentID: params.environmentID,
    detail,
    totalNodes: result.items.length,
    pageCount: result.pageCount,
    pageSize: result.pageSize,
    orderBy: result.orderBy,
    ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
    filePath,
    cachedAt,
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
  cachedAt: string;
  runType?: "deploy" | "refresh";
  runStatus?: "completed" | "failed" | "canceled" | "running" | "waitingToRun";
  environmentID?: string;
}> {
  const detail = params.detail ?? false;
  const cachedAt = new Date().toISOString();
  const result = await fetchAllRuns(client, { ...params, detail });
  const runs = sanitizeResponse(result.items);
  const filePath = writeSnapshotFile(
    ["runs"],
    buildRunsCacheFileName({ ...params, detail }),
    {
      cachedAt,
      detail,
      ...(params.runType ? { runType: params.runType } : {}),
      ...(params.runStatus ? { runStatus: params.runStatus } : {}),
      ...(params.environmentID ? { environmentID: params.environmentID } : {}),
      totalRuns: result.items.length,
      pageCount: result.pageCount,
      pageSize: result.pageSize,
      orderBy: result.orderBy,
      ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
      runs,
    },
    options
  );

  return {
    detail,
    totalRuns: result.items.length,
    pageCount: result.pageCount,
    pageSize: result.pageSize,
    orderBy: result.orderBy,
    ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
    filePath,
    cachedAt,
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
  cachedAt: string;
}> {
  const cachedAt = new Date().toISOString();
  const result = await fetchAllOrgUsers(client, params);
  const filePath = writeSnapshotFile(
    ["users"],
    "org-users.json",
    {
      cachedAt,
      totalUsers: result.items.length,
      pageCount: result.pageCount,
      pageSize: result.pageSize,
      orderBy: result.orderBy,
      ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
      users: result.items,
    },
    options
  );

  return {
    totalUsers: result.items.length,
    pageCount: result.pageCount,
    pageSize: result.pageSize,
    orderBy: result.orderBy,
    ...(result.orderByDirection ? { orderByDirection: result.orderByDirection } : {}),
    filePath,
    cachedAt,
  };
}
