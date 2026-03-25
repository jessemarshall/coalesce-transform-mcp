# Streaming Pagination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace unbounded in-memory pagination with streaming NDJSON writes for cache tools and a 250-item safety cap for in-memory consumers.

**Architecture:** Split `fetchAllPaginated` into two functions: `fetchAllPaginatedToMemory` (250-item cap, used by analysis/overview tools) and `streamAllPaginatedToDisk` (writes NDJSON line-by-line, used by cache tools). Cache output changes from a single `.json` file to a `.ndjson` data file + `.meta.json` envelope.

**Tech Stack:** Node.js `fs` (writeFileSync per page for crash safety), existing Zod schemas, vitest.

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/services/cache/snapshots.ts` | Split `fetchAllPaginated` into two functions, update all cache/fetch exports |
| Modify | `tests/tools/cache.test.ts` | Update cache tests for NDJSON + meta output |
| Create | `tests/services/snapshots.test.ts` | Unit tests for `fetchAllPaginatedToMemory` (safety cap) and `streamAllPaginatedToDisk` (NDJSON streaming) |
| Modify | `tests/workflows/get-environment-overview.test.ts` | Add test for safety cap breach |

**Downstream callers:** The only non-test callers of the four `cache*` functions are in `src/mcp/cache.ts`. Each handler passes the result through `buildJsonToolResponse(toolName, result)`, which serializes whatever is returned. The new `metaPath` field will appear as a new property in the tool response — no code changes needed there.

---

### Task 1: Add safety cap to in-memory pagination

**Files:**
- Create: `tests/services/snapshots.test.ts`
- Modify: `src/services/cache/snapshots.ts:52-98`

- [ ] **Step 1: Write the failing test for safety cap**

Create `tests/services/snapshots.test.ts`:

```typescript
import { describe, it, expect, vi } from "vitest";
import {
  fetchAllWorkspaceNodes,
} from "../../src/services/cache/snapshots.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

describe("fetchAllPaginatedToMemory safety cap", () => {
  it("throws when item count exceeds 250", async () => {
    const client = createMockClient();
    // First page: 200 items with a cursor
    const page1Items = Array.from({ length: 200 }, (_, i) => ({
      id: `node-${i}`,
      name: `NODE_${i}`,
      nodeType: "Stage",
    }));
    // Second page: 100 items (total 300 > 250 cap)
    const page2Items = Array.from({ length: 100 }, (_, i) => ({
      id: `node-${200 + i}`,
      name: `NODE_${200 + i}`,
      nodeType: "Stage",
    }));

    client.get.mockImplementation((_path: string, params?: Record<string, unknown>) => {
      if (!params?.startingFrom) {
        return Promise.resolve({ data: page1Items, next: "cursor-2" });
      }
      return Promise.resolve({ data: page2Items });
    });

    await expect(
      fetchAllWorkspaceNodes(client as any, { workspaceID: "ws-1", detail: false })
    ).rejects.toThrow(
      /exceeded 250 item safety limit/
    );
  });

  it("succeeds when item count is within 250", async () => {
    const client = createMockClient();
    const items = Array.from({ length: 250 }, (_, i) => ({
      id: `node-${i}`,
      name: `NODE_${i}`,
      nodeType: "Stage",
    }));

    client.get.mockResolvedValue({ data: items });

    const result = await fetchAllWorkspaceNodes(client as any, {
      workspaceID: "ws-1",
      detail: false,
    });
    expect(result.items).toHaveLength(250);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/services/snapshots.test.ts`
Expected: FAIL — no safety cap exists yet, 300-item fetch succeeds instead of throwing.

- [ ] **Step 3: Add `MAX_IN_MEMORY_ITEMS` constant and cap to `fetchAllPaginated`**

In `src/services/cache/snapshots.ts`, add the constant after `DEFAULT_PAGE_SIZE` (line 11):

```typescript
const MAX_IN_MEMORY_ITEMS = 250;
```

Then in `fetchAllPaginated`, after `items.push(...page.data)` (line 77), add:

```typescript
    if (items.length > MAX_IN_MEMORY_ITEMS) {
      throw new Error(
        `Pagination exceeded ${MAX_IN_MEMORY_ITEMS} item safety limit. ` +
        `Use a cache-* tool for large collections.`
      );
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/services/snapshots.test.ts`
Expected: PASS

- [ ] **Step 5: Run full test suite to check for regressions**

Run: `npx vitest run`
Expected: All tests pass. The existing `get-environment-overview` multi-page test (5 items across 3 pages) is well under 250.

- [ ] **Step 6: Commit**

```bash
git add src/services/cache/snapshots.ts tests/services/snapshots.test.ts
git commit -m "feat: add 250-item safety cap to in-memory pagination"
```

---

### Task 2: Implement `streamAllPaginatedToDisk`

**Files:**
- Modify: `src/services/cache/snapshots.ts`
- Modify: `tests/services/snapshots.test.ts`

- [ ] **Step 1: Write the failing test for NDJSON streaming**

Add these imports to the top of `tests/services/snapshots.test.ts` (merge with existing imports):

```typescript
import { describe, it, expect, vi, afterEach } from "vitest";
import { mkdtempSync, readFileSync, rmSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  fetchAllWorkspaceNodes,
  streamAllPaginatedToDisk,
} from "../../src/services/cache/snapshots.js";
```

Then append this new `describe` block after the existing `fetchAllPaginatedToMemory safety cap` block:

```typescript
describe("streamAllPaginatedToDisk", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    vi.restoreAllMocks();
    for (const dir of tempDirs.splice(0, tempDirs.length)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  function createTempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), "coalesce-stream-test-"));
    tempDirs.push(dir);
    return dir;
  }

  it("writes items as NDJSON lines and creates meta file", async () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");

    const fetchPage = vi.fn()
      .mockResolvedValueOnce({
        data: [{ id: "a" }, { id: "b" }],
        next: "cursor-2",
      })
      .mockResolvedValueOnce({
        data: [{ id: "c" }],
      });

    const result = await streamAllPaginatedToDisk(fetchPage, {}, {}, {
      ndjsonPath,
      metaPath,
    });

    expect(result.totalItems).toBe(3);
    expect(result.pageCount).toBe(2);

    // Verify NDJSON file: 3 lines, each valid JSON
    const lines = readFileSync(ndjsonPath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(3);
    expect(JSON.parse(lines[0])).toEqual({ id: "a" });
    expect(JSON.parse(lines[1])).toEqual({ id: "b" });
    expect(JSON.parse(lines[2])).toEqual({ id: "c" });

    // Verify meta file
    const meta = JSON.parse(readFileSync(metaPath, "utf8"));
    expect(meta.totalItems).toBe(3);
    expect(meta.pageCount).toBe(2);
    expect(meta.pageSize).toBe(250);
    expect(meta.orderBy).toBe("id");
    expect(typeof meta.cachedAt).toBe("string");
  });

  it("does not write meta file if fetch fails mid-stream", async () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");

    const fetchPage = vi.fn()
      .mockResolvedValueOnce({
        data: [{ id: "a" }],
        next: "cursor-2",
      })
      .mockRejectedValueOnce(new Error("API failure"));

    await expect(
      streamAllPaginatedToDisk(fetchPage, {}, {}, { ndjsonPath, metaPath })
    ).rejects.toThrow("API failure");

    // NDJSON file may exist with partial data — that's fine
    // Meta file must NOT exist (signals incomplete)
    expect(existsSync(metaPath)).toBe(false);
  });

  it("applies itemTransform to each item before writing", async () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");

    const fetchPage = vi.fn().mockResolvedValueOnce({
      data: [{ id: "a", secret: "hidden" }, { id: "b", secret: "hidden" }],
    });

    const itemTransform = (item: unknown) => {
      const obj = item as Record<string, unknown>;
      const { secret, ...rest } = obj;
      return rest;
    };

    const result = await streamAllPaginatedToDisk(
      fetchPage, {}, {},
      { ndjsonPath, metaPath, itemTransform }
    );

    expect(result.totalItems).toBe(2);
    const lines = readFileSync(ndjsonPath, "utf8").trimEnd().split("\n");
    expect(JSON.parse(lines[0])).toEqual({ id: "a" });
    expect(JSON.parse(lines[1])).toEqual({ id: "b" });
  });

  it("detects repeated cursors", async () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");

    const fetchPage = vi.fn()
      .mockResolvedValueOnce({ data: [{ id: "a" }], next: "cursor-2" })
      .mockResolvedValueOnce({ data: [{ id: "b" }], next: "cursor-2" });

    await expect(
      streamAllPaginatedToDisk(fetchPage, {}, {}, { ndjsonPath, metaPath })
    ).rejects.toThrow("Pagination repeated cursor cursor-2");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/services/snapshots.test.ts`
Expected: FAIL — `streamAllPaginatedToDisk` does not exist yet.

- [ ] **Step 3: Implement `streamAllPaginatedToDisk`**

In `src/services/cache/snapshots.ts`, add this function after `fetchAllPaginated` (after line 98). Also add `appendFileSync` and `existsSync` to the `fs` import on line 1.

Update line 1 `fs` import:
```typescript
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
```

Update line 2 `path` import to add `dirname`:
```typescript
import { dirname, join } from "node:path";
```

Add new types and function:

```typescript
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/services/snapshots.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/services/cache/snapshots.ts tests/services/snapshots.test.ts
git commit -m "feat: add streamAllPaginatedToDisk with NDJSON output"
```

---

### Task 3: Wire cache functions to use streaming

**Files:**
- Modify: `src/services/cache/snapshots.ts:227-424` (the four `cache*` functions)
- Modify: `tests/tools/cache.test.ts`

- [ ] **Step 1: Update `cacheWorkspaceNodes` to use streaming**

Replace the body of `cacheWorkspaceNodes` (lines ~227-274) with:

```typescript
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
  const directory = ensureDirectory(baseDir, "data", "nodes");
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
```

Update `buildNodeCacheFileName` to use `.ndjson` extension:

```typescript
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
```

Update `buildRunsCacheFileName` similarly:

```typescript
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
```

- [ ] **Step 2: Update `cacheEnvironmentNodes` to use streaming**

Same pattern as workspace — use `streamAllPaginatedToDisk`, return `filePath` (ndjson) + `metaPath`.

```typescript
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
  const directory = ensureDirectory(baseDir, "data", "nodes");
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
```

- [ ] **Step 3: Update `cacheRuns` to use streaming with `sanitizeResponse` as `itemTransform`**

```typescript
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
  const directory = ensureDirectory(baseDir, "data", "runs");
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
```

- [ ] **Step 4: Update `cacheOrgUsers` to use streaming**

```typescript
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
  const directory = ensureDirectory(baseDir, "data", "users");
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
```

- [ ] **Step 5: Update cache tests**

Replace the entire test body in `tests/tools/cache.test.ts` with the following four tests. Each test now asserts `.ndjson` file paths, NDJSON line format, and `.meta.json` files:

```typescript
import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  cacheEnvironmentNodes,
  cacheOrgUsers,
  cacheRuns,
  cacheWorkspaceNodes,
} from "../../src/services/cache/snapshots.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

describe("cache snapshot tools", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    vi.restoreAllMocks();
    for (const directory of tempDirs.splice(0, tempDirs.length)) {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  function createTempDir(): string {
    const directory = mkdtempSync(join(tmpdir(), "coalesce-cache-test-"));
    tempDirs.push(directory);
    return directory;
  }

  it("caches all workspace nodes with detail enabled by default", async () => {
    const client = createMockClient();
    const baseDir = createTempDir();

    client.get.mockImplementation((_path: string, params?: Record<string, unknown>) => {
      if (!params?.startingFrom) {
        return Promise.resolve({
          data: [
            { id: "node-1", name: "STG_ORDERS", nodeType: "Stage", config: { truncateBefore: true } },
          ],
          next: "cursor-2",
        });
      }
      if (params.startingFrom === "cursor-2") {
        return Promise.resolve({
          data: [{ id: "node-2", name: "DIM_CUSTOMER", nodeType: "Dimension" }],
        });
      }
      throw new Error(`Unexpected cursor ${String(params.startingFrom)}`);
    });

    const result = await cacheWorkspaceNodes(client as any, { workspaceID: "ws-1" }, { baseDir });

    expect(result.filePath).toBe(join(baseDir, "data", "nodes", "workspace-ws-1-nodes.ndjson"));
    expect(result.metaPath).toBe(join(baseDir, "data", "nodes", "workspace-ws-1-nodes.meta.json"));
    expect(result.totalNodes).toBe(2);
    expect(client.get).toHaveBeenNthCalledWith(
      1,
      "/api/v1/workspaces/ws-1/nodes",
      { detail: true, limit: 250, orderBy: "id" }
    );
    expect(client.get).toHaveBeenNthCalledWith(
      2,
      "/api/v1/workspaces/ws-1/nodes",
      { detail: true, limit: 250, orderBy: "id", startingFrom: "cursor-2" }
    );

    // Verify NDJSON: one line per node
    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0])).toMatchObject({ id: "node-1", name: "STG_ORDERS" });
    expect(JSON.parse(lines[1])).toMatchObject({ id: "node-2", name: "DIM_CUSTOMER" });

    // Verify meta
    const meta = JSON.parse(readFileSync(result.metaPath, "utf8"));
    expect(meta.totalItems).toBe(2);
    expect(meta.pageCount).toBe(2);
    expect(typeof meta.cachedAt).toBe("string");
  });

  it("caches environment nodes with summary payloads when detail is false", async () => {
    const client = createMockClient();
    const baseDir = createTempDir();

    client.get.mockResolvedValue({
      data: [{ id: "env-node-1", name: "RAW_CUSTOMER", nodeType: "Stage" }],
    });

    const result = await cacheEnvironmentNodes(
      client as any,
      { environmentID: "env-1", detail: false },
      { baseDir }
    );

    expect(result.filePath).toBe(
      join(baseDir, "data", "nodes", "environment-env-1-nodes-summary.ndjson")
    );
    expect(result.metaPath).toBe(
      join(baseDir, "data", "nodes", "environment-env-1-nodes-summary.meta.json")
    );
    expect(result.detail).toBe(false);
    expect(client.get).toHaveBeenCalledWith("/api/v1/environments/env-1/nodes", {
      detail: false,
      limit: 250,
      orderBy: "id",
    });

    // Verify NDJSON
    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0])).toMatchObject({ id: "env-node-1", name: "RAW_CUSTOMER" });

    // Verify meta
    const meta = JSON.parse(readFileSync(result.metaPath, "utf8"));
    expect(meta.totalItems).toBe(1);
  });

  it("caches runs and strips userCredentials from each NDJSON line", async () => {
    const client = createMockClient();
    const baseDir = createTempDir();

    client.get.mockResolvedValue({
      data: [
        {
          id: "run-1",
          runStatus: "failed",
          userCredentials: { snowflakeUsername: "secret-user" },
        },
      ],
    });

    const result = await cacheRuns(
      client as any,
      { runStatus: "failed", detail: true },
      { baseDir }
    );

    expect(result.filePath).toBe(join(baseDir, "data", "runs", "runs-failed-detail.ndjson"));
    expect(result.metaPath).toBe(join(baseDir, "data", "runs", "runs-failed-detail.meta.json"));

    // Verify NDJSON line has no userCredentials (sanitizeResponse applied per-item)
    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(1);
    const parsed = JSON.parse(lines[0]);
    expect(parsed).toEqual({ id: "run-1", runStatus: "failed" });
    expect(parsed).not.toHaveProperty("userCredentials");

    // Verify meta
    const meta = JSON.parse(readFileSync(result.metaPath, "utf8"));
    expect(meta.totalItems).toBe(1);
  });

  it("caches organization users into data/users", async () => {
    const client = createMockClient();
    const baseDir = createTempDir();

    client.get.mockResolvedValue({
      data: [{ id: "user-1", name: "Alice" }, { id: "user-2", name: "Bob" }],
    });

    const result = await cacheOrgUsers(client as any, {}, { baseDir });

    expect(result.filePath).toBe(join(baseDir, "data", "users", "org-users.ndjson"));
    expect(result.metaPath).toBe(join(baseDir, "data", "users", "org-users.meta.json"));
    expect(result.totalUsers).toBe(2);

    // Verify NDJSON
    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0])).toMatchObject({ id: "user-1", name: "Alice" });
    expect(JSON.parse(lines[1])).toMatchObject({ id: "user-2", name: "Bob" });

    // Verify meta
    const meta = JSON.parse(readFileSync(result.metaPath, "utf8"));
    expect(meta.totalItems).toBe(2);
  });
});
```

- [ ] **Step 6: Run tests to verify everything passes**

Run: `npx vitest run`
Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/services/cache/snapshots.ts tests/tools/cache.test.ts
git commit -m "feat: switch cache tools to NDJSON streaming pagination"
```

---

### Task 4: Rename `fetchAllPaginated` and clean up exports

**Files:**
- Modify: `src/services/cache/snapshots.ts`

- [ ] **Step 1: Rename `fetchAllPaginated` to `fetchAllPaginatedToMemory`**

Rename the function at its definition and all internal call sites (`fetchAllWorkspaceNodes`, `fetchAllEnvironmentNodes`, `fetchAllRuns`, `fetchAllOrgUsers`). The function already has the safety cap from Task 1.

- [ ] **Step 2: Remove `writeSnapshotFile` function**

The `writeSnapshotFile` helper (lines ~106-117) is no longer called by any cache function. Delete it. Keep `ensureDirectory` — it is still used by all four cache functions to create the output directory before calling `streamAllPaginatedToDisk`.

- [ ] **Step 3: Run tests**

Run: `npx vitest run`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/services/cache/snapshots.ts
git commit -m "refactor: rename fetchAllPaginated to fetchAllPaginatedToMemory, remove dead code"
```

---

### Task 5: Add safety cap integration test for `get-environment-overview`

This is a test-after addition — the safety cap was implemented in Task 1. This test verifies the cap triggers correctly through the `getEnvironmentOverview` call path.

**Files:**
- Modify: `tests/workflows/get-environment-overview.test.ts`

- [ ] **Step 1: Add test for safety cap breach in environment overview**

Append to the existing describe block in `tests/workflows/get-environment-overview.test.ts`:

```typescript
  it("throws when node count exceeds the in-memory safety cap", async () => {
    const client = createMockClient();
    const envData = { id: "env-1", name: "Huge" };
    const hugeNodeList = Array.from({ length: 251 }, (_, i) => ({
      id: `node-${i}`,
      name: `NODE_${i}`,
    }));

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1") return Promise.resolve(envData);
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: hugeNodeList });
      }
      return Promise.resolve({});
    });

    await expect(
      getEnvironmentOverview(client as any, { environmentID: "env-1" })
    ).rejects.toThrow(/exceeded 250 item safety limit/);
  });
```

- [ ] **Step 2: Run test to verify it passes**

Run: `npx vitest run tests/workflows/get-environment-overview.test.ts`
Expected: PASS — the safety cap from Task 1 triggers.

- [ ] **Step 3: Run full suite**

Run: `npx vitest run`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/workflows/get-environment-overview.test.ts
git commit -m "test: add safety cap test for get-environment-overview"
```
