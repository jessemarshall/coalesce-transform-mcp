import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { defineCacheTools } from "../../src/mcp/cache.js";
import { CACHE_DIR_NAME } from "../../src/cache-dir.js";
import type { CoalesceClient } from "../../src/client.js";

type ToolEntry = [string, unknown, (...args: unknown[]) => Promise<unknown>];

function getHandler(tools: unknown[], name: string) {
  const entry = (tools as ToolEntry[]).find((t) => t[0] === name);
  if (!entry) throw new Error(`Tool "${name}" not registered`);
  return entry[2];
}

async function callHandler(handler: (...args: unknown[]) => Promise<unknown>, params: unknown) {
  const response = (await handler(params)) as {
    content: Array<{ type: string; text: string }>;
    structuredContent?: unknown;
    isError?: boolean;
  };
  return response;
}

function parseResponse(response: {
  content: Array<{ type: string; text: string }>;
  structuredContent?: unknown;
  isError?: boolean;
}) {
  const textEntry = response.content.find((c) => c.type === "text");
  if (!textEntry) throw new Error("No text content in response");
  return JSON.parse(textEntry.text);
}

function createMockServer(supportsElicitation: boolean) {
  return {
    server: {
      getClientCapabilities: vi.fn().mockReturnValue(
        supportsElicitation ? { elicitation: { form: true } } : {}
      ),
      elicitInput: vi.fn(),
    },
  } as unknown as McpServer;
}

function createMockClient(): CoalesceClient {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  } as unknown as CoalesceClient;
}

describe("defineCacheTools — clear_data_cache handler", () => {
  const tempDirs: string[] = [];
  const originalCacheDirEnv = process.env.COALESCE_CACHE_DIR;

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    for (const directory of tempDirs.splice(0, tempDirs.length)) {
      rmSync(directory, { recursive: true, force: true });
    }
    if (originalCacheDirEnv === undefined) {
      delete process.env.COALESCE_CACHE_DIR;
    } else {
      process.env.COALESCE_CACHE_DIR = originalCacheDirEnv;
    }
  });

  function createTempCacheBase(): string {
    const directory = mkdtempSync(join(tmpdir(), "coalesce-cache-handler-test-"));
    tempDirs.push(directory);
    process.env.COALESCE_CACHE_DIR = directory;
    return directory;
  }

  it("returns STOP_AND_CONFIRM without deleting when confirmation is missing and client lacks elicitation", async () => {
    const baseDir = createTempCacheBase();
    const cacheDir = join(baseDir, CACHE_DIR_NAME);
    mkdirSync(cacheDir, { recursive: true });
    writeFileSync(join(cacheDir, "sentinel.txt"), "x");

    const server = createMockServer(false);
    const tools = defineCacheTools(server, createMockClient());
    const handler = getHandler(tools, "clear_data_cache");

    const parsed = parseResponse(await callHandler(handler, {}));

    expect(parsed.executed).toBe(false);
    expect(typeof parsed.STOP_AND_CONFIRM).toBe("string");
    expect(parsed.STOP_AND_CONFIRM).toContain("clear_data_cache");
    expect(existsSync(cacheDir)).toBe(true);
    expect(existsSync(join(cacheDir, "sentinel.txt"))).toBe(true);
  });

  it("cancels via elicitation decline without deleting", async () => {
    const baseDir = createTempCacheBase();
    const cacheDir = join(baseDir, CACHE_DIR_NAME);
    mkdirSync(cacheDir, { recursive: true });
    writeFileSync(join(cacheDir, "sentinel.txt"), "x");

    const server = createMockServer(true);
    (server.server.elicitInput as ReturnType<typeof vi.fn>).mockResolvedValue({
      action: "accept",
      content: { confirmed: false },
    });

    const tools = defineCacheTools(server, createMockClient());
    const handler = getHandler(tools, "clear_data_cache");

    const parsed = parseResponse(await callHandler(handler, {}));

    expect(parsed.executed).toBe(false);
    expect(parsed.cancelled).toBe(true);
    expect(existsSync(cacheDir)).toBe(true);
    expect(existsSync(join(cacheDir, "sentinel.txt"))).toBe(true);
  });

  it("returns deleted=false when the cache directory does not exist", async () => {
    createTempCacheBase();
    const server = createMockServer(false);
    const tools = defineCacheTools(server, createMockClient());
    const handler = getHandler(tools, "clear_data_cache");

    const parsed = parseResponse(await callHandler(handler, { confirmed: true }));

    expect(parsed.deleted).toBe(false);
    expect(parsed.message).toContain("No cache directory found");
    expect(parsed.message).toContain(CACHE_DIR_NAME);
  });

  it("counts files recursively and removes the cache directory when confirmed", async () => {
    const baseDir = createTempCacheBase();
    const cacheDir = join(baseDir, CACHE_DIR_NAME);
    const nested = join(cacheDir, "workspace-ws-1", "nodes");
    mkdirSync(nested, { recursive: true });
    writeFileSync(join(cacheDir, "top.txt"), "hello");
    writeFileSync(join(nested, "a.ndjson"), "line one");
    writeFileSync(join(nested, "b.ndjson"), "another longer payload");

    const server = createMockServer(false);
    const tools = defineCacheTools(server, createMockClient());
    const handler = getHandler(tools, "clear_data_cache");

    const parsed = parseResponse(await callHandler(handler, { confirmed: true }));

    expect(parsed.deleted).toBe(true);
    expect(parsed.fileCount).toBe(3);
    expect(parsed.totalBytes).toBeGreaterThan(0);
    expect(typeof parsed.sizeMB).toBe("string");
    expect(parsed.sizeMB.endsWith(" MB")).toBe(true);
    expect(parsed.message).toContain("Deleted 3 files");
    expect(parsed.countWarning).toBeUndefined();
    expect(existsSync(cacheDir)).toBe(false);
  });

});
