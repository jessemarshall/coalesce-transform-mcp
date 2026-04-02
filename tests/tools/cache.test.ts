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

    expect(result.filePath).toBe(join(baseDir, "coalesce_transform_mcp_data_cache", "nodes", "workspace-ws-1-nodes.ndjson"));
    expect(result.metaPath).toBe(join(baseDir, "coalesce_transform_mcp_data_cache", "nodes", "workspace-ws-1-nodes.meta.json"));
    expect(result.totalNodes).toBe(2);
    expect(client.get).toHaveBeenNthCalledWith(
      1,
      "/api/v1/workspaces/ws-1/nodes",
      { detail: true, limit: 250, orderBy: "id" },
      { timeoutMs: 120_000 }
    );
    expect(client.get).toHaveBeenNthCalledWith(
      2,
      "/api/v1/workspaces/ws-1/nodes",
      { detail: true, limit: 250, orderBy: "id", startingFrom: "cursor-2" },
      { timeoutMs: 120_000 }
    );

    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0])).toMatchObject({ id: "node-1", name: "STG_ORDERS" });
    expect(JSON.parse(lines[1])).toMatchObject({ id: "node-2", name: "DIM_CUSTOMER" });

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
      join(baseDir, "coalesce_transform_mcp_data_cache", "nodes", "environment-env-1-nodes-summary.ndjson")
    );
    expect(result.metaPath).toBe(
      join(baseDir, "coalesce_transform_mcp_data_cache", "nodes", "environment-env-1-nodes-summary.meta.json")
    );
    expect(result.detail).toBe(false);
    expect(client.get).toHaveBeenCalledWith("/api/v1/environments/env-1/nodes", {
      detail: false,
      limit: 250,
      orderBy: "id",
    });

    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0])).toMatchObject({ id: "env-node-1", name: "RAW_CUSTOMER" });

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

    expect(result.filePath).toBe(join(baseDir, "coalesce_transform_mcp_data_cache", "runs", "runs-failed-detail.ndjson"));
    expect(result.metaPath).toBe(join(baseDir, "coalesce_transform_mcp_data_cache", "runs", "runs-failed-detail.meta.json"));

    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(1);
    const parsed = JSON.parse(lines[0]);
    expect(parsed).toEqual({ id: "run-1", runStatus: "failed" });
    expect(parsed).not.toHaveProperty("userCredentials");

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

    expect(result.filePath).toBe(join(baseDir, "coalesce_transform_mcp_data_cache", "users", "org-users.ndjson"));
    expect(result.metaPath).toBe(join(baseDir, "coalesce_transform_mcp_data_cache", "users", "org-users.meta.json"));
    expect(result.totalUsers).toBe(2);

    const lines = readFileSync(result.filePath, "utf8").trimEnd().split("\n");
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0])).toMatchObject({ id: "user-1", name: "Alice" });
    expect(JSON.parse(lines[1])).toMatchObject({ id: "user-2", name: "Bob" });

    const meta = JSON.parse(readFileSync(result.metaPath, "utf8"));
    expect(meta.totalItems).toBe(2);
  });
});
