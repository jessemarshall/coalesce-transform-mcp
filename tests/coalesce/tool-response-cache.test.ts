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
  const bucket = workspaceID ? `workspace-${workspaceID}` : "_global";
  return join(baseDir, CACHE_DIR_NAME, bucket, "auto-cache");
}

function getEnvAutoCacheDir(baseDir: string, environmentID: string): string {
  return join(baseDir, CACHE_DIR_NAME, `environment-${environmentID}`, "auto-cache");
}

/** Generate a payload that exceeds the given byte threshold when JSON-serialised. */
function largePayload(minBytes: number): unknown {
  return { data: "x".repeat(minBytes) };
}

describe("buildJsonToolResponse auto-cache behaviour", () => {
  const tempDirs: string[] = [];
  const originalEnv = process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES;
  const originalCacheDir = process.env.COALESCE_CACHE_DIR;

  function makeTempDir(): string {
    const dir = createTempDir();
    tempDirs.push(dir);
    return dir;
  }

  afterEach(() => {
    process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES = originalEnv;
    process.env.COALESCE_CACHE_DIR = originalCacheDir;
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

  it("uses COALESCE_CACHE_DIR when no baseDir is provided", () => {
    const cacheBaseDir = makeTempDir();
    process.env.COALESCE_CACHE_DIR = cacheBaseDir;

    buildJsonToolResponse("list_workspaces", largePayload(200), {
      maxInlineBytes: 50,
    });

    const files = readdirSync(getAutoCacheDir(cacheBaseDir)).filter((f) => f.endsWith(".json"));
    expect(files).toHaveLength(1);
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

  // ─── Workspace bucket sanitization (path traversal + collision defense) ──

  it("sanitizes path-traversal workspace IDs so the file stays under the cache root", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "../../etc",
    });

    const cacheRoot = join(baseDir, CACHE_DIR_NAME);
    const buckets = readdirSync(cacheRoot);
    // There must be exactly one bucket created, and it must sit directly under the cache root
    expect(buckets).toHaveLength(1);
    // The bucket name must not escape: no leading dots, no slashes
    expect(buckets[0]).not.toMatch(/^\.+$/);
    expect(buckets[0]).not.toMatch(/[/\\]/);
    // File must be addressable from within the sanitized bucket
    const files = readdirSync(join(cacheRoot, buckets[0], "auto-cache")).filter((f) =>
      f.endsWith(".json")
    );
    expect(files).toHaveLength(1);
  });

  it("sanitizes absolute-path workspace IDs into a safe segment", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "/abs/path",
    });

    const cacheRoot = join(baseDir, CACHE_DIR_NAME);
    const buckets = readdirSync(cacheRoot);
    expect(buckets).toHaveLength(1);
    expect(buckets[0]).not.toMatch(/^[-./]/);
    expect(buckets[0]).not.toMatch(/[/\\]/);
  });

  it("collapses workspace IDs that sanitize to empty into the _global bucket", () => {
    const baseDir = makeTempDir();
    // "!!!" → all chars replaced by "-" → leading/trailing "-" stripped → empty → _global
    buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "!!!",
    });

    const files = readdirSync(getAutoCacheDir(baseDir)).filter((f) => f.endsWith(".json"));
    expect(files).toHaveLength(1);
  });

  it("collapses '.' and '..' workspace IDs into the _global bucket", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("test_tool_a", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: ".",
    });
    buildJsonToolResponse("test_tool_b", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "..",
    });

    const cacheRoot = join(baseDir, CACHE_DIR_NAME);
    // Both responses must land in _global — not in any "." or ".." bucket
    expect(readdirSync(cacheRoot)).toEqual(["_global"]);
    const files = readdirSync(getAutoCacheDir(baseDir)).filter((f) => f.endsWith(".json"));
    expect(files).toHaveLength(2);
  });

  it("keeps a literal '_global' workspaceID distinct from the unscoped _global bucket", () => {
    const baseDir = makeTempDir();
    // With the `workspace-<id>` prefix, workspaceID="_global" becomes the
    // `workspace-_global/` bucket — structurally separate from the unscoped
    // `_global/` bucket. Prior behavior collapsed them; prefixing makes
    // collision impossible by construction.
    buildJsonToolResponse("workspace_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "_global",
    });
    buildJsonToolResponse("unscoped_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
    });

    const cacheRoot = join(baseDir, CACHE_DIR_NAME);
    expect(readdirSync(cacheRoot).sort()).toEqual(["_global", "workspace-_global"]);
    expect(readdirSync(getAutoCacheDir(baseDir)).filter((f) => f.endsWith(".json"))).toHaveLength(1);
    expect(readdirSync(getAutoCacheDir(baseDir, "_global")).filter((f) => f.endsWith(".json"))).toHaveLength(1);
  });

  // ─── Environment bucket ──────────────────────────────────────────────────

  it("writes auto-cache under environment-<id>/ when only environmentID is provided", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("run_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      environmentID: "env-7",
    });

    const files = readdirSync(getEnvAutoCacheDir(baseDir, "env-7")).filter((f) => f.endsWith(".json"));
    expect(files).toHaveLength(1);
  });

  it("prefers workspace scope over environment scope when both are provided", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("run_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "ws-1",
      environmentID: "env-1",
    });

    const cacheRoot = join(baseDir, CACHE_DIR_NAME);
    expect(readdirSync(cacheRoot)).toEqual(["workspace-ws-1"]);
  });

  it("replaces null bytes and control characters in workspace IDs", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: "ws\x00null\tbyte",
    });

    const cacheRoot = join(baseDir, CACHE_DIR_NAME);
    const buckets = readdirSync(cacheRoot);
    expect(buckets).toHaveLength(1);
    // No control characters should survive into the bucket name
    expect(buckets[0]).not.toMatch(/[\x00-\x1f]/);
    expect(buckets[0]).toMatch(/^[a-zA-Z0-9._-]+$/);
  });

  it("replaces null bytes and control characters in environment IDs", () => {
    const baseDir = makeTempDir();
    buildJsonToolResponse("run_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      environmentID: "env\x00null\tbyte",
    });

    const cacheRoot = join(baseDir, CACHE_DIR_NAME);
    const buckets = readdirSync(cacheRoot);
    expect(buckets).toHaveLength(1);
    expect(buckets[0]).toMatch(/^environment-/);
    expect(buckets[0]).not.toMatch(/[\x00-\x1f]/);
    expect(buckets[0]).toMatch(/^[a-zA-Z0-9._-]+$/);
  });

  it("preserves safe workspace IDs unchanged (alphanumerics, dots, dashes, underscores)", () => {
    const baseDir = makeTempDir();
    const safeID = "team-alpha.prod_2025";
    buildJsonToolResponse("test_tool", largePayload(200), {
      maxInlineBytes: 50,
      baseDir,
      workspaceID: safeID,
    });

    const files = readdirSync(getAutoCacheDir(baseDir, safeID)).filter((f) => f.endsWith(".json"));
    expect(files).toHaveLength(1);
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
