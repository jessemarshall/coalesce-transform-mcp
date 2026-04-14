import { describe, it, expect, afterEach, vi, beforeEach } from "vitest";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { buildJsonToolResponse } from "../../src/coalesce/tool-response.js";
import { CACHE_DIR_NAME } from "../../src/cache-dir.js";

function createTempDir(): string {
  return mkdtempSync(join(tmpdir(), "coalesce-tool-response-cache-test-"));
}

function getAutoCacheDir(baseDir: string, workspaceID?: string): string {
  return join(baseDir, CACHE_DIR_NAME, workspaceID ?? "_global", "auto-cache");
}

/** Generate a payload that exceeds the given byte threshold when JSON-serialised. */
function largePayload(minBytes: number): unknown {
  return { data: "x".repeat(minBytes) };
}

describe("buildJsonToolResponse auto-cache behaviour", () => {
  const tempDirs: string[] = [];
  const originalEnv = process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES;

  function makeTempDir(): string {
    const dir = createTempDir();
    tempDirs.push(dir);
    return dir;
  }

  afterEach(() => {
    process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES = originalEnv;
    for (const dir of tempDirs.splice(0, tempDirs.length)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  // ─── Inline vs auto-cache threshold ──────────────────────────────────────

  it("returns inline response when payload is below the threshold", () => {
    const baseDir = makeTempDir();
    const result = buildJsonToolResponse("test_tool", { small: true }, {
      maxInlineBytes: 1024 * 1024,
      baseDir,
    });

    expect(result.structuredContent).toEqual({ small: true });
    expect(result.structuredContent).not.toHaveProperty("autoCached");
  });

  // ─── Workspace partitioning ──────────────────────────────────────────────

  it("writes auto-cache under the workspace bucket when workspaceID is provided", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "ws-42",
    });

    const files = readdirSync(getAutoCacheDir(baseDir, "ws-42")).filter((f) => f.endsWith(".json"));
    expect(files.length).toBe(1);
  });

  it("writes auto-cache under _global when no workspaceID is provided", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("list_workspaces", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
    });

    const files = readdirSync(getAutoCacheDir(baseDir)).filter((f) => f.endsWith(".json"));
    expect(files.length).toBe(1);
  });

  it("partitions auto-cache between workspaces without cross-contamination", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("tool_a", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "ws-a",
    });
    buildJsonToolResponse("tool_b", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "ws-b",
    });

    expect(readdirSync(getAutoCacheDir(baseDir, "ws-a")).filter((f) => f.endsWith(".json"))).toHaveLength(1);
    expect(readdirSync(getAutoCacheDir(baseDir, "ws-b")).filter((f) => f.endsWith(".json"))).toHaveLength(1);
  });

  it("auto-caches when payload exceeds maxInlineBytes", () => {
    const baseDir = makeTempDir();
    const result = buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
    });

    expect(result.structuredContent).toHaveProperty("autoCached", true);
    expect(result.structuredContent).toHaveProperty("toolName", "test_tool");
    expect(result.structuredContent).toHaveProperty("sizeBytes");
    expect(result.structuredContent).toHaveProperty("cachedAt");
    expect(result.structuredContent).toHaveProperty("message");
  });

  it("writes the full JSON response to disk when auto-caching", () => {
    const baseDir = makeTempDir();
    const payload = { items: [1, 2, 3], nested: { deep: true } };
    buildJsonToolResponse("test_tool", payload, {
      maxInlineBytes: 10,
      baseDir,
    });

    const autoCacheDir = getAutoCacheDir(baseDir);
    const files = readdirSync(autoCacheDir).filter((f) => f.endsWith(".json"));
    expect(files.length).toBeGreaterThanOrEqual(1);

    const cached = JSON.parse(readFileSync(join(autoCacheDir, files[0]), "utf8"));
    expect(cached).toEqual(payload);
  });

  it("includes a resource_link in content when auto-caching succeeds", () => {
    const baseDir = makeTempDir();
    const result = buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
    });

    const resourceLinks = result.content.filter((c) => c.type === "resource_link");
    expect(resourceLinks.length).toBe(1);
    expect((resourceLinks[0] as any).uri).toMatch(/^coalesce:\/\/cache\//);
  });

  // ─── Auto-cache write failure graceful fallback ──────────────────────────

  it("falls back to inline response when the cache directory is not writable", () => {
    // Use a path that can't be created (nested under a file, not a directory)
    const baseDir = makeTempDir();
    const blockingFile = join(baseDir, CACHE_DIR_NAME);
    writeFileSync(blockingFile, "not a directory");

    const result = buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
    });

    // Should fall back to inline — no autoCached flag
    expect(result.structuredContent).not.toHaveProperty("autoCached");
    // The data should still be present in the inline response
    const parsed = JSON.parse((result.content[0] as { type: "text"; text: string }).text);
    expect(parsed).toHaveProperty("data");
  });

  // ─── Environment variable override for cache threshold ───────────────────

  it("respects COALESCE_MCP_AUTO_CACHE_MAX_BYTES env var", () => {
    process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES = "50";
    const baseDir = makeTempDir();

    // Payload > 50 bytes but < default 32KB — should auto-cache with env override
    const result = buildJsonToolResponse("test_tool", largePayload(100), {
      baseDir,
    });

    expect(result.structuredContent).toHaveProperty("autoCached", true);
  });

  it("ignores invalid COALESCE_MCP_AUTO_CACHE_MAX_BYTES and uses default", () => {
    process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES = "not-a-number";
    const baseDir = makeTempDir();

    // Small payload should be inline with default 32KB threshold
    const result = buildJsonToolResponse("test_tool", { small: true }, {
      baseDir,
    });

    expect(result.structuredContent).not.toHaveProperty("autoCached");
  });

  it("ignores negative COALESCE_MCP_AUTO_CACHE_MAX_BYTES and uses default", () => {
    process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES = "-100";
    const baseDir = makeTempDir();

    const result = buildJsonToolResponse("test_tool", { small: true }, {
      baseDir,
    });

    expect(result.structuredContent).not.toHaveProperty("autoCached");
  });

  // ─── Stale file cleanup ──────────────────────────────────────────────────

  it("cleans up stale auto-cache files from previous sessions on write", () => {
    const baseDir = makeTempDir();
    const autoCacheDir = getAutoCacheDir(baseDir);
    mkdirSync(autoCacheDir, { recursive: true });

    // Create a "stale" file with an old timestamp prefix (before server start)
    const staleFile = "2020-01-01T00-00-00-000Z-old-tool-abc123.json";
    writeFileSync(join(autoCacheDir, staleFile), '{"stale": true}\n');

    // Trigger auto-cache, which should clean up the stale file
    buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
    });

    const remaining = readdirSync(autoCacheDir).filter((f) => f.endsWith(".json"));
    // The stale file should be gone; only the newly cached file should remain
    expect(remaining).not.toContain(staleFile);
    expect(remaining.length).toBe(1);
  });

  // ─── File name slugification ─────────────────────────────────────────────

  it("sanitizes tool names with special characters in cached file names", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("My Tool!@#$%", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
    });

    const autoCacheDir = getAutoCacheDir(baseDir);
    const files = readdirSync(autoCacheDir).filter((f) => f.endsWith(".json"));
    expect(files.length).toBe(1);
    // File name should contain slugified tool name (no special chars)
    expect(files[0]).toMatch(/my-tool/);
    expect(files[0]).not.toMatch(/[!@#$%]/);
  });

  it("handles empty tool name gracefully", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
    });

    const autoCacheDir = getAutoCacheDir(baseDir);
    const files = readdirSync(autoCacheDir).filter((f) => f.endsWith(".json"));
    expect(files.length).toBe(1);
    // Should fall back to "tool-response" when name slugifies to empty
    expect(files[0]).toMatch(/tool-response/);
  });

  // ─── Pagination coercion in auto-cached responses ────────────────────────

  it("coerces pagination fields before auto-caching", () => {
    const baseDir = makeTempDir();
    const payload = { data: "x".repeat(200), next: 42, total: null };
    buildJsonToolResponse("test_tool", payload, {
      maxInlineBytes: 50,
      baseDir,
    });

    const autoCacheDir = getAutoCacheDir(baseDir);
    const files = readdirSync(autoCacheDir).filter((f) => f.endsWith(".json"));
    const cached = JSON.parse(readFileSync(join(autoCacheDir, files[0]), "utf8"));
    // next should be coerced from number to string
    expect(cached.next).toBe("42");
    // null total should be removed
    expect(cached).not.toHaveProperty("total");
  });

  // ─── Cache path externalization ──────────────────────────────────────────

  it("externalizes file paths inside the cache dir as resource URIs", () => {
    const baseDir = makeTempDir();
    const cacheDir = join(baseDir, CACHE_DIR_NAME, "snapshots");
    mkdirSync(cacheDir, { recursive: true });
    const cachedFile = join(cacheDir, "test.json");
    writeFileSync(cachedFile, '{"cached": true}\n');

    const result = buildJsonToolResponse("test_tool", {
      snapshotPath: cachedFile,
    }, {
      maxInlineBytes: 1024 * 1024,
      baseDir,
    });

    // The path should be replaced with a URI
    expect(result.structuredContent).toHaveProperty("snapshotUri");
    expect((result.structuredContent as any).snapshotUri).toMatch(/^coalesce:\/\/cache\//);
    // Original "Path" key should be renamed to "Uri"
    expect(result.structuredContent).not.toHaveProperty("snapshotPath");
  });

  it("preserves file paths outside the cache dir unchanged", () => {
    const baseDir = makeTempDir();
    const outsidePath = "/tmp/not-in-cache/test.json";

    const result = buildJsonToolResponse("test_tool", {
      filePath: outsidePath,
    }, {
      maxInlineBytes: 1024 * 1024,
      baseDir,
    });

    // Path should remain unchanged since it's outside the cache dir
    expect(result.structuredContent).toHaveProperty("filePath", outsidePath);
  });

  it("adds resource_link entries for externalized cache paths", () => {
    const baseDir = makeTempDir();
    const cacheDir = join(baseDir, CACHE_DIR_NAME, "data");
    mkdirSync(cacheDir, { recursive: true });
    const cachedFile = join(cacheDir, "output.json");
    writeFileSync(cachedFile, '{"data": true}\n');

    const result = buildJsonToolResponse("test_tool", {
      outputPath: cachedFile,
    }, {
      maxInlineBytes: 1024 * 1024,
      baseDir,
    });

    const resourceLinks = result.content.filter((c) => c.type === "resource_link");
    expect(resourceLinks.length).toBe(1);
    expect((resourceLinks[0] as any).uri).toMatch(/^coalesce:\/\/cache\//);
  });

  // ─── Nested cache path externalization ───────────────────────────────────

  it("externalizes cache paths in nested objects and arrays", () => {
    const baseDir = makeTempDir();
    const cacheDir = join(baseDir, CACHE_DIR_NAME, "nodes");
    mkdirSync(cacheDir, { recursive: true });
    const file1 = join(cacheDir, "a.json");
    const file2 = join(cacheDir, "b.json");
    writeFileSync(file1, "{}");
    writeFileSync(file2, "{}");

    const result = buildJsonToolResponse("test_tool", {
      items: [
        { dataPath: file1 },
        { dataPath: file2 },
      ],
    }, {
      maxInlineBytes: 1024 * 1024,
      baseDir,
    });

    const structured = result.structuredContent as any;
    expect(structured.items[0].dataUri).toMatch(/^coalesce:\/\/cache\//);
    expect(structured.items[1].dataUri).toMatch(/^coalesce:\/\/cache\//);
    // resource_link entries should be created for each
    const resourceLinks = result.content.filter((c) => c.type === "resource_link");
    expect(resourceLinks.length).toBe(2);
  });
});
