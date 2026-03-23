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

    const result = await cacheWorkspaceNodes(
      client as any,
      { workspaceID: "ws-1" },
      { baseDir }
    );

    expect(result.filePath).toBe(join(baseDir, "data", "nodes", "workspace-ws-1-nodes.json"));
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

    const snapshot = JSON.parse(readFileSync(result.filePath, "utf8"));
    expect(snapshot).toMatchObject({
      scope: "workspace",
      workspaceID: "ws-1",
      detail: true,
      totalNodes: 2,
    });
    expect(snapshot.nodes).toHaveLength(2);
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
      join(baseDir, "data", "nodes", "environment-env-1-nodes-summary.json")
    );
    expect(result.detail).toBe(false);
    expect(client.get).toHaveBeenCalledWith("/api/v1/environments/env-1/nodes", {
      detail: false,
      limit: 250,
      orderBy: "id",
    });
  });

  it("caches runs and strips userCredentials from the saved snapshot", async () => {
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

    expect(result.filePath).toBe(join(baseDir, "data", "runs", "runs-failed-detail.json"));

    const snapshot = JSON.parse(readFileSync(result.filePath, "utf8"));
    expect(snapshot).toMatchObject({
      runStatus: "failed",
      detail: true,
      totalRuns: 1,
    });
    expect(snapshot.runs[0]).toEqual({
      id: "run-1",
      runStatus: "failed",
    });
  });

  it("caches organization users into data/users", async () => {
    const client = createMockClient();
    const baseDir = createTempDir();

    client.get.mockResolvedValue({
      data: [{ id: "user-1", name: "Alice" }, { id: "user-2", name: "Bob" }],
    });

    const result = await cacheOrgUsers(client as any, {}, { baseDir });

    expect(result.filePath).toBe(join(baseDir, "data", "users", "org-users.json"));
    expect(result.totalUsers).toBe(2);

    const snapshot = JSON.parse(readFileSync(result.filePath, "utf8"));
    expect(snapshot.users).toHaveLength(2);
  });
});
