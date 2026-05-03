import { describe, it, expect, vi, afterEach } from "vitest";
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import {
  fetchAllEnvironmentNodes,
  fetchAllRuns,
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

describe("pagination edge cases — fetchAllPaginatedToMemory", () => {
  it("handles empty first page with no next cursor", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    const result = await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
    });

    expect(result.items).toEqual([]);
    expect(result.pageCount).toBe(1);
  });

  it("handles empty first page followed by populated page", async () => {
    const client = createMockClient();
    client.get.mockImplementation((_path: string, params?: Record<string, unknown>) => {
      if (!params?.startingFrom) {
        return Promise.resolve({ data: [], next: "cursor-2" });
      }
      return Promise.resolve({ data: [{ id: "n1" }] });
    });

    const result = await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
    });

    expect(result.items).toHaveLength(1);
    expect(result.pageCount).toBe(2);
  });

  it("handles numeric next cursor (coerced to string)", async () => {
    const client = createMockClient();
    client.get.mockImplementation((_path: string, params?: Record<string, unknown>) => {
      if (!params?.startingFrom) {
        return Promise.resolve({ data: [{ id: "n1" }], next: 42 });
      }
      if (params.startingFrom === "42") {
        return Promise.resolve({ data: [{ id: "n2" }] });
      }
      return Promise.resolve({ data: [] });
    });

    const result = await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
    });

    expect(result.items).toHaveLength(2);
    expect(result.pageCount).toBe(2);
  });

  it("treats null next as end of pagination", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "n1" }], next: null });

    const result = await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
    });

    expect(result.items).toHaveLength(1);
    expect(result.pageCount).toBe(1);
  });

  it("treats empty-string next as end of pagination", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "n1" }], next: "" });

    const result = await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
    });

    expect(result.items).toHaveLength(1);
    expect(result.pageCount).toBe(1);
  });

  it("treats whitespace-only next as end of pagination", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "n1" }], next: "   " });

    const result = await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
    });

    expect(result.items).toHaveLength(1);
    expect(result.pageCount).toBe(1);
  });

  it("respects custom pageSize parameter", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "n1" }] });

    await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
      pageSize: 50,
    });

    expect(client.get).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ limit: 50 })
    );
  });

  it("clamps pageSize to minimum of 1", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
      pageSize: 0,
    });

    expect(client.get).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ limit: 1 })
    );
  });

  it("passes orderByDirection when specified", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    await fetchAllRuns(client as any, {
      environmentID: "env-1",
      orderBy: "id",
      orderByDirection: "desc",
    });

    expect(client.get).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ orderBy: "id", orderByDirection: "desc" })
    );
  });

  it("handles many pages without issues", async () => {
    const client = createMockClient();
    let pageNum = 0;
    const totalPages = 10;

    client.get.mockImplementation(() => {
      pageNum++;
      if (pageNum < totalPages) {
        return Promise.resolve({
          data: [{ id: `node-${pageNum}` }],
          next: `cursor-${pageNum + 1}`,
        });
      }
      return Promise.resolve({ data: [{ id: `node-${pageNum}` }] });
    });

    const result = await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
    });

    expect(result.items).toHaveLength(totalPages);
    expect(result.pageCount).toBe(totalPages);
  });

  it("throws on repeated cursor even across non-adjacent pages", async () => {
    const client = createMockClient();
    let callNum = 0;
    client.get.mockImplementation(() => {
      callNum++;
      if (callNum === 1) return Promise.resolve({ data: [{ id: "a" }], next: "cursor-A" });
      if (callNum === 2) return Promise.resolve({ data: [{ id: "b" }], next: "cursor-B" });
      // Third page returns cursor-A again
      return Promise.resolve({ data: [{ id: "c" }], next: "cursor-A" });
    });

    await expect(
      fetchAllEnvironmentNodes(client as any, { environmentID: "env-1" })
    ).rejects.toThrow("Pagination repeated cursor cursor-A");
  });

  it("throws when pagination exceeds MAX_PAGES (runaway loop guard)", async () => {
    // Defense-in-depth: an API that returns unique cursors indefinitely would
    // otherwise blow memory before any other check trips.
    const client = createMockClient();
    let callNum = 0;
    client.get.mockImplementation(() => {
      callNum++;
      // Always return a fresh unique cursor — the seen-cursors set never trips.
      return Promise.resolve({
        data: [{ id: `node-${callNum}` }],
        next: `cursor-${callNum}`,
      });
    });

    await expect(
      fetchAllEnvironmentNodes(client as any, { environmentID: "env-1" })
    ).rejects.toThrow(/Pagination exceeded 500 pages/);
  });

  it("handles response with missing data field (defaults to empty array)", async () => {
    const client = createMockClient();
    // Page 1 has no data field but has a next cursor; page 2 has neither — terminates pagination
    client.get
      .mockResolvedValueOnce({ next: "cursor-2" })
      .mockResolvedValueOnce({});

    const result = await fetchAllEnvironmentNodes(client as any, {
      environmentID: "env-1",
    });

    expect(result.items).toEqual([]);
    expect(result.pageCount).toBe(2);
  });
});

describe("pagination edge cases — streamAllPaginatedToDisk", () => {
  const tempDirs: string[] = [];

  afterEach(() => {
    vi.restoreAllMocks();
    for (const dir of tempDirs.splice(0, tempDirs.length)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  function createTempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), "coalesce-pagination-test-"));
    tempDirs.push(dir);
    return dir;
  }

  it("handles empty first page with no next cursor", async () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");

    const fetchPage = vi.fn().mockResolvedValueOnce({ data: [] });

    const result = await streamAllPaginatedToDisk(fetchPage, {}, {}, {
      ndjsonPath,
      metaPath,
    });

    expect(result.totalItems).toBe(0);
    expect(result.pageCount).toBe(1);
    expect(readFileSync(ndjsonPath, "utf8")).toBe("");
  });

  it("handles numeric next cursor in stream mode", async () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");

    const fetchPage = vi.fn()
      .mockResolvedValueOnce({ data: [{ id: "a" }], next: 99 })
      .mockResolvedValueOnce({ data: [{ id: "b" }] });

    const result = await streamAllPaginatedToDisk(fetchPage, {}, {}, {
      ndjsonPath,
      metaPath,
    });

    expect(result.totalItems).toBe(2);
    expect(result.pageCount).toBe(2);

    // Verify the cursor was passed correctly as string
    expect(fetchPage).toHaveBeenCalledWith(
      expect.objectContaining({ startingFrom: "99" })
    );
  });

  it("throws and cleans up temp files when streaming exceeds MAX_PAGES", async () => {
    const baseDir = createTempDir();
    const dataDir = join(baseDir, "data");
    const ndjsonPath = join(dataDir, "test.ndjson");
    const metaPath = join(dataDir, "test.meta.json");

    let callNum = 0;
    const fetchPage = vi.fn().mockImplementation(() => {
      callNum++;
      return Promise.resolve({
        data: [{ id: `n-${callNum}` }],
        next: `cursor-${callNum}`,
      });
    });

    await expect(
      streamAllPaginatedToDisk(fetchPage, {}, {}, { ndjsonPath, metaPath })
    ).rejects.toThrow(/Pagination exceeded 500 pages/);

    // No temp files should remain after cleanup. streamAllPaginatedToDisk
    // mkdir's the parent before the loop, so dataDir always exists here.
    const tempFiles = readdirSync(dataDir).filter((f) => f.includes(".tmp-"));
    expect(tempFiles).toHaveLength(0);
  });

  it("cleans up temp files when repeated cursor is detected", async () => {
    const baseDir = createTempDir();
    const dataDir = join(baseDir, "data");
    const ndjsonPath = join(dataDir, "test.ndjson");
    const metaPath = join(dataDir, "test.meta.json");

    // Pre-populate the final paths so we can verify they survive the error
    mkdirSync(dataDir, { recursive: true });
    writeFileSync(ndjsonPath, '{"id":"existing"}\n', "utf8");
    writeFileSync(metaPath, '{"totalItems":1}\n', "utf8");

    const fetchPage = vi.fn()
      .mockResolvedValueOnce({ data: [{ id: "a" }], next: "loop" })
      .mockResolvedValueOnce({ data: [{ id: "b" }], next: "loop" });

    await expect(
      streamAllPaginatedToDisk(fetchPage, {}, {}, { ndjsonPath, metaPath })
    ).rejects.toThrow("Pagination repeated cursor loop");

    // Previous snapshot pair must survive the failed stream
    expect(readFileSync(ndjsonPath, "utf8")).toBe('{"id":"existing"}\n');
    expect(readFileSync(metaPath, "utf8")).toBe('{"totalItems":1}\n');

    // No temp files should remain in the directory
    const remainingFiles = readdirSync(dataDir);
    const tempFiles = remainingFiles.filter((f) => f.includes(".tmp-"));
    expect(tempFiles).toHaveLength(0);
  });

  it("respects custom orderBy and orderByDirection", async () => {
    const baseDir = createTempDir();
    const ndjsonPath = join(baseDir, "data", "test.ndjson");
    const metaPath = join(baseDir, "data", "test.meta.json");

    const fetchPage = vi.fn().mockResolvedValueOnce({ data: [] });

    const result = await streamAllPaginatedToDisk(
      fetchPage, {}, { orderBy: "createdAt", orderByDirection: "desc" },
      { ndjsonPath, metaPath }
    );

    expect(result.orderBy).toBe("createdAt");
    expect(result.orderByDirection).toBe("desc");
    expect(fetchPage).toHaveBeenCalledWith(
      expect.objectContaining({ orderBy: "createdAt", orderByDirection: "desc" })
    );
  });
});
