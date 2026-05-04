import { describe, it, expect, vi, afterEach } from "vitest";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  cacheEnvironmentNodes,
  cacheOrgUsers,
  cacheRuns,
  cacheWorkspaceNodes,
  fetchAllEnvironmentNodes,
  fetchAllRuns,
  fetchAllWorkspaceNodes,
  promoteSnapshotArtifacts,
  streamAllPaginatedToDisk,
  toNodeSummaries,
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

describe("fetchAllPaginatedToMemory", () => {
  it("collects multiple pages even when total item count exceeds 250", async () => {
    const client = createMockClient();
    const page1Items = Array.from({ length: 200 }, (_, i) => ({
      id: `node-${i}`,
      name: `NODE_${i}`,
      nodeType: "Stage",
    }));
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

    const result = await fetchAllWorkspaceNodes(client as any, {
      workspaceID: "ws-1",
      detail: false,
    });

    expect(result.items).toHaveLength(300);
    expect(result.pageCount).toBe(2);
    expect(result.pageSize).toBe(250);
    expect(result.orderBy).toBe("id");
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

    const lines = readFileSync(ndjsonPath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(3);
    expect(JSON.parse(lines[0])).toEqual({ id: "a" });
    expect(JSON.parse(lines[1])).toEqual({ id: "b" });
    expect(JSON.parse(lines[2])).toEqual({ id: "c" });

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

    expect(existsSync(ndjsonPath)).toBe(false);
    expect(existsSync(metaPath)).toBe(false);
  });

  it("preserves the previous snapshot when a replacement stream fails", async () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");

    mkdirSync(join(baseDir, "data"), { recursive: true });
    writeFileSync(ndjsonPath, '{"id":"existing"}\n', "utf8");
    writeFileSync(metaPath, '{"totalItems":1}\n', "utf8");

    const fetchPage = vi.fn()
      .mockResolvedValueOnce({
        data: [{ id: "a" }],
        next: "cursor-2",
      })
      .mockRejectedValueOnce(new Error("API failure"));

    await expect(
      streamAllPaginatedToDisk(fetchPage, {}, {}, { ndjsonPath, metaPath })
    ).rejects.toThrow("API failure");

    expect(readFileSync(ndjsonPath, "utf8")).toBe('{"id":"existing"}\n');
    expect(readFileSync(metaPath, "utf8")).toBe('{"totalItems":1}\n');
  });

  it("restores the previous snapshot pair if meta promotion fails after NDJSON promotion", () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");
    const tempNdjsonPath = join(baseDir, "data", "test.ndjson.tmp");
    const tempMetaPath = join(baseDir, "data", "test.meta.json.tmp");

    mkdirSync(join(baseDir, "data"), { recursive: true });
    writeFileSync(ndjsonPath, '{"id":"existing"}\n', "utf8");
    writeFileSync(metaPath, '{"totalItems":1}\n', "utf8");
    writeFileSync(tempNdjsonPath, '{"id":"fresh"}\n', "utf8");
    writeFileSync(tempMetaPath, '{"totalItems":2}\n', "utf8");

    const fsOps = {
      existsSync,
      renameSync: (from: string, to: string) => {
        if (from === tempMetaPath && to === metaPath) {
          throw new Error("meta promote failed");
        }
        return renameSync(from, to);
      },
      rmSync: (path: string, options?: { force?: boolean }) => rmSync(path, options),
    };

    expect(() =>
      promoteSnapshotArtifacts(tempNdjsonPath, ndjsonPath, tempMetaPath, metaPath, fsOps)
    ).toThrow("meta promote failed");

    expect(readFileSync(ndjsonPath, "utf8")).toBe('{"id":"existing"}\n');
    expect(readFileSync(metaPath, "utf8")).toBe('{"totalItems":1}\n');
  });

  it("removes the partially promoted ndjson on first-time-write meta rename failure", () => {
    // No prior ndjson/meta pair existed. The ndjson rename succeeds but the
    // meta rename fails. Without rollback, the new ndjson stays at its final
    // path and future readers see an orphaned ndjson with no matching meta —
    // a corrupt cache. Confirm the rollback deletes it.
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");
    const tempNdjsonPath = join(baseDir, "data", "test.ndjson.tmp");
    const tempMetaPath = join(baseDir, "data", "test.meta.json.tmp");

    mkdirSync(join(baseDir, "data"), { recursive: true });
    writeFileSync(tempNdjsonPath, '{"id":"fresh"}\n', "utf8");
    writeFileSync(tempMetaPath, '{"totalItems":1}\n', "utf8");

    const fsOps = {
      existsSync,
      renameSync: (from: string, to: string) => {
        if (from === tempMetaPath && to === metaPath) {
          throw new Error("meta promote failed");
        }
        return renameSync(from, to);
      },
      rmSync: (path: string, options?: { force?: boolean }) => rmSync(path, options),
    };

    expect(() =>
      promoteSnapshotArtifacts(tempNdjsonPath, ndjsonPath, tempMetaPath, metaPath, fsOps)
    ).toThrow("meta promote failed");

    expect(existsSync(ndjsonPath)).toBe(false);
    expect(existsSync(metaPath)).toBe(false);
  });

  it("leaves the cache empty when the first-time NDJSON rename fails", () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");
    const tempNdjsonPath = join(baseDir, "data", "test.ndjson.tmp");
    const tempMetaPath = join(baseDir, "data", "test.meta.json.tmp");

    mkdirSync(join(baseDir, "data"), { recursive: true });
    writeFileSync(tempNdjsonPath, '{"id":"fresh"}\n', "utf8");
    writeFileSync(tempMetaPath, '{"totalItems":1}\n', "utf8");

    const fsOps = {
      existsSync,
      renameSync: (from: string, to: string) => {
        if (from === tempNdjsonPath && to === ndjsonPath) {
          throw new Error("ndjson promote failed");
        }
        return renameSync(from, to);
      },
      rmSync: (path: string, options?: { force?: boolean }) => rmSync(path, options),
    };

    expect(() =>
      promoteSnapshotArtifacts(tempNdjsonPath, ndjsonPath, tempMetaPath, metaPath, fsOps)
    ).toThrow("ndjson promote failed");

    expect(existsSync(ndjsonPath)).toBe(false);
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

describe("toNodeSummaries", () => {
  it("returns name/nodeType pairs for well-formed entries", () => {
    expect(
      toNodeSummaries([
        { id: "n1", name: "STG_A", nodeType: "Stage" },
        { id: "n2", name: "DIM_B", nodeType: "Dimension" },
      ])
    ).toEqual([
      { name: "STG_A", nodeType: "Stage" },
      { name: "DIM_B", nodeType: "Dimension" },
    ]);
  });

  it("filters out entries missing name or nodeType", () => {
    expect(
      toNodeSummaries([
        { id: "n1", name: "STG_A", nodeType: "Stage" },
        { id: "n2", name: "no_type" },
        { id: "n3", nodeType: "Stage" },
        { id: "n4", name: "DIM_B", nodeType: "Dimension" },
      ])
    ).toEqual([
      { name: "STG_A", nodeType: "Stage" },
      { name: "DIM_B", nodeType: "Dimension" },
    ]);
  });

  it("filters out non-object entries (string, null, array)", () => {
    expect(
      toNodeSummaries([
        "raw-string",
        null,
        [{ name: "x", nodeType: "y" }],
        { name: "OK", nodeType: "Stage" },
      ])
    ).toEqual([{ name: "OK", nodeType: "Stage" }]);
  });

  it("rejects non-string name/nodeType (numeric ID, boolean type)", () => {
    expect(
      toNodeSummaries([
        { name: 42, nodeType: "Stage" },
        { name: "OK", nodeType: true },
        { name: "VALID", nodeType: "Stage" },
      ])
    ).toEqual([{ name: "VALID", nodeType: "Stage" }]);
  });

  it("returns an empty array when given an empty input", () => {
    expect(toNodeSummaries([])).toEqual([]);
  });
});

describe("fetchAllEnvironmentNodes", () => {
  it("paginates with environmentID and detail params", async () => {
    const client = createMockClient();
    client.get.mockImplementation((_path: string, params?: Record<string, unknown>) => {
      if (!params?.startingFrom) {
        return Promise.resolve({ data: [{ id: "e-1" }, { id: "e-2" }], next: "cur-2" });
      }
      return Promise.resolve({ data: [{ id: "e-3" }] });
    });

    const result = await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
      detail: true,
    });

    expect(result.items).toHaveLength(3);
    expect(result.pageCount).toBe(2);
    // detail=true forces an extended timeout — first call should pass it through
    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/environments/env-1/nodes",
      expect.objectContaining({ detail: true, limit: 250, orderBy: "id" }),
      expect.objectContaining({ timeoutMs: expect.any(Number) })
    );
  });

  it("rejects empty environmentID before any HTTP call", async () => {
    const client = createMockClient();
    await expect(
      fetchAllEnvironmentNodes(client as any, { environmentID: "" })
    ).rejects.toThrow(/environmentID/);
    expect(client.get).not.toHaveBeenCalled();
  });

  it("rejects environmentID containing path separators", async () => {
    const client = createMockClient();
    await expect(
      fetchAllEnvironmentNodes(client as any, { environmentID: "../escape" })
    ).rejects.toThrow(/path separators|control characters/);
    expect(client.get).not.toHaveBeenCalled();
  });
});

describe("fetchAllRuns", () => {
  it("forwards filter params (runType, runStatus, environmentID) to listRuns", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "r-1", runStatus: "completed" }] });

    await fetchAllRuns(client as any, {
      runType: "refresh",
      runStatus: "completed",
      environmentID: "env-9",
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/runs",
      expect.objectContaining({
        runType: "refresh",
        runStatus: "completed",
        environmentID: "env-9",
        limit: 250,
        orderBy: "id",
      })
    );
  });

  it("omits environmentID from query when not provided", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    await fetchAllRuns(client as any, {});

    const call = client.get.mock.calls[0];
    expect(call[1]).not.toHaveProperty("environmentID");
  });
});

// Cache* tools route through streamAllPaginatedToDisk with a real fs path. We
// give them a writable temp dir, mock the network layer, and assert both the
// returned metadata and the on-disk artifacts.
describe("cacheWorkspaceNodes (full pipeline to disk)", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    vi.restoreAllMocks();
    for (const dir of tempDirs.splice(0, tempDirs.length)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  function createTempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), "snapshots-cache-test-"));
    tempDirs.push(dir);
    return dir;
  }

  it("writes nodes.ndjson and meta to a workspace-scoped subdir", async () => {
    const baseDir = createTempDir();
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "n1", name: "STG_A", nodeType: "Stage" },
        { id: "n2", name: "DIM_B", nodeType: "Dimension" },
      ],
    });

    const result = await cacheWorkspaceNodes(
      client as any,
      { workspaceID: "ws-42", detail: true },
      { baseDir }
    );

    expect(result.workspaceID).toBe("ws-42");
    expect(result.detail).toBe(true);
    expect(result.totalNodes).toBe(2);
    expect(result.filePath).toMatch(/workspace-ws-42\/nodes\/nodes\.ndjson$/);
    expect(result.metaPath).toMatch(/workspace-ws-42\/nodes\/nodes\.meta\.json$/);
    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0])).toEqual({ id: "n1", name: "STG_A", nodeType: "Stage" });
  });

  it("uses nodes-summary.ndjson when detail=false", async () => {
    const baseDir = createTempDir();
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "n1" }] });

    const result = await cacheWorkspaceNodes(
      client as any,
      { workspaceID: "ws-1", detail: false },
      { baseDir }
    );

    expect(result.filePath).toMatch(/nodes-summary\.ndjson$/);
    expect(result.detail).toBe(false);
  });

  it("rejects empty workspaceID before any HTTP call", async () => {
    const baseDir = createTempDir();
    const client = createMockClient();
    await expect(
      cacheWorkspaceNodes(client as any, { workspaceID: "" }, { baseDir })
    ).rejects.toThrow(/workspaceID/);
    expect(client.get).not.toHaveBeenCalled();
  });
});

describe("cacheEnvironmentNodes (full pipeline to disk)", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    for (const dir of tempDirs.splice(0, tempDirs.length)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  function createTempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), "snapshots-cache-env-test-"));
    tempDirs.push(dir);
    return dir;
  }

  it("writes nodes.ndjson and meta to an environment-scoped subdir", async () => {
    const baseDir = createTempDir();
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [{ id: "en1", name: "DIM_C", nodeType: "Dimension" }],
    });

    const result = await cacheEnvironmentNodes(
      client as any,
      { environmentID: "env-7", detail: true },
      { baseDir }
    );

    expect(result.environmentID).toBe("env-7");
    expect(result.totalNodes).toBe(1);
    expect(result.filePath).toMatch(/environment-env-7\/nodes\/nodes\.ndjson$/);
    expect(JSON.parse(readFileSync(result.filePath, "utf8").trim())).toEqual({
      id: "en1",
      name: "DIM_C",
      nodeType: "Dimension",
    });
  });

  it("rejects empty environmentID before any HTTP call", async () => {
    const baseDir = createTempDir();
    const client = createMockClient();
    await expect(
      cacheEnvironmentNodes(client as any, { environmentID: "" }, { baseDir })
    ).rejects.toThrow(/environmentID/);
    expect(client.get).not.toHaveBeenCalled();
  });
});

describe("cacheRuns (security-critical: sanitizes credentials before disk write)", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    for (const dir of tempDirs.splice(0, tempDirs.length)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  function createTempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), "snapshots-cache-runs-test-"));
    tempDirs.push(dir);
    return dir;
  }

  // The itemTransform = sanitizeResponse applies SANITIZED_KEYS redaction on
  // each cached run — if anyone removes that transform, run snapshots would
  // start including userCredentials, accessTokens, snowflakePassword, etc.
  // This test fails loudly if the redaction is dropped.
  it("strips userCredentials, accessToken, snowflakeKeyPairKey, snowflakeKeyPairPass, snowflakePassword, gitToken from cached runs", async () => {
    const baseDir = createTempDir();
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        {
          id: "run-1",
          runStatus: "completed",
          runDetails: {
            userCredentials: { snowflakePassword: "PLAINTEXT-DO-NOT-LOG" },
            snowflakeKeyPairKey: "BEGIN-RSA-PRIVATE-KEY",
            accessToken: "secret-token",
          },
          gitToken: "ghp_REDACTME",
          metadata: { harmless: "ok" },
        },
      ],
    });

    const result = await cacheRuns(client as any, { detail: true }, { baseDir });
    const onDisk = readFileSync(result.filePath, "utf8").trim();
    const parsed = JSON.parse(onDisk) as Record<string, unknown>;

    expect(parsed.id).toBe("run-1");
    expect(parsed.runStatus).toBe("completed");
    expect(parsed.metadata).toEqual({ harmless: "ok" });
    // None of the SANITIZED_KEYS should appear anywhere in the serialized payload.
    for (const sensitive of [
      "userCredentials",
      "snowflakeKeyPairKey",
      "snowflakeKeyPairPass",
      "snowflakePassword",
      "gitToken",
      "accessToken",
      "PLAINTEXT-DO-NOT-LOG",
      "BEGIN-RSA-PRIVATE-KEY",
      "secret-token",
      "ghp_REDACTME",
    ]) {
      expect(onDisk).not.toContain(sensitive);
    }
  });

  it("uses runs-{filters}-summary.ndjson naming when filters and detail=false are passed", async () => {
    const baseDir = createTempDir();
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "run-2" }] });

    const result = await cacheRuns(
      client as any,
      { runType: "refresh", runStatus: "failed", environmentID: "env-3", detail: false },
      { baseDir }
    );

    expect(result.filePath).toMatch(/runs-env-env-3-refresh-failed-summary\.ndjson$/);
    expect(result.environmentID).toBe("env-3");
    expect(result.runType).toBe("refresh");
    expect(result.runStatus).toBe("failed");
  });

  it("rejects environmentID containing path separators", async () => {
    const baseDir = createTempDir();
    const client = createMockClient();
    await expect(
      cacheRuns(client as any, { environmentID: "..\\evil" }, { baseDir })
    ).rejects.toThrow(/path separators|control characters/);
  });
});

describe("cacheOrgUsers (full pipeline to disk)", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    for (const dir of tempDirs.splice(0, tempDirs.length)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("writes org-users.ndjson and meta in the users dir", async () => {
    const baseDir = mkdtempSync(join(tmpdir(), "snapshots-cache-users-test-"));
    tempDirs.push(baseDir);
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "u-1", email: "alice@example.com" },
        { id: "u-2", email: "bob@example.com" },
      ],
    });

    const result = await cacheOrgUsers(client as any, {}, { baseDir });

    expect(result.totalUsers).toBe(2);
    expect(result.filePath).toMatch(/users\/org-users\.ndjson$/);
    expect(result.metaPath).toMatch(/users\/org-users\.meta\.json$/);
    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(2);
  });

  it("respects custom pageSize and orderBy", async () => {
    const baseDir = mkdtempSync(join(tmpdir(), "snapshots-cache-users-test-"));
    tempDirs.push(baseDir);
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    await cacheOrgUsers(client as any, { pageSize: 50, orderBy: "name" }, { baseDir });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/users",
      expect.objectContaining({ limit: 50, orderBy: "name" })
    );
  });
});
