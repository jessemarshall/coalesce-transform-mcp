import { describe, it, expect, vi, afterEach } from "vitest";
import { mkdtempSync, readFileSync, rmSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  fetchAllWorkspaceNodes,
  streamAllPaginatedToDisk,
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
