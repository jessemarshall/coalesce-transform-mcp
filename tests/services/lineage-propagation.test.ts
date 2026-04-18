import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { existsSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { propagateColumnChange } from "../../src/services/lineage/lineage-propagation.js";
import type { LineageCacheEntry } from "../../src/services/lineage/lineage-cache.js";

// Mock dependencies
vi.mock("../../src/services/lineage/lineage-cache.js", () => ({
  invalidateLineageCache: vi.fn(),
}));

vi.mock("../../src/services/lineage/lineage-traversal.js", () => ({
  walkColumnLineage: vi.fn(),
}));

vi.mock("../../src/coalesce/api/nodes.js", () => ({
  setWorkspaceNode: vi.fn(),
}));

vi.mock("../../src/services/workspace/node-update-helpers.js", () => ({
  buildUpdatedWorkspaceNodeBody: vi.fn((_current, changes) => ({
    ...changes,
    merged: true,
  })),
}));

import { walkColumnLineage } from "../../src/services/lineage/lineage-traversal.js";
import { setWorkspaceNode } from "../../src/coalesce/api/nodes.js";
import { invalidateLineageCache } from "../../src/services/lineage/lineage-cache.js";

function buildFakeCache(
  nodes: Record<string, { name: string; nodeType: string; columns: Array<{ id: string; name: string }> }>
): LineageCacheEntry {
  const map = new Map<string, { name: string; nodeType: string; columns: Array<{ id: string; name: string }> }>();
  for (const [id, data] of Object.entries(nodes)) {
    map.set(id, data);
  }
  return { nodes: map, edges: new Map(), builtAt: Date.now() } as unknown as LineageCacheEntry;
}

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("propagateColumnChange", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("captures pre-mutation snapshot for each downstream node", async () => {
    const client = createMockClient();
    const cache = buildFakeCache({
      n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "order_id" }] },
      n2: { name: "STG_ORDERS", nodeType: "Stage", columns: [{ id: "c2", name: "order_id" }] },
    });

    vi.mocked(walkColumnLineage).mockReturnValue([
      { nodeID: "n2", nodeName: "STG_ORDERS", nodeType: "Stage", columnID: "c2", columnName: "order_id", direction: "downstream" as const, depth: 1 },
    ]);

    const nodeBody = {
      id: "n2",
      name: "STG_ORDERS",
      metadata: {
        columns: [{ id: "c2", columnID: "c2", name: "order_id", dataType: "VARCHAR" }],
      },
    };
    client.get.mockResolvedValue(nodeBody);
    vi.mocked(setWorkspaceNode).mockResolvedValue(undefined as never);

    const result = await propagateColumnChange(
      client as never,
      cache,
      "ws-1",
      "n1",
      "c1",
      { columnName: "renamed_order_id" },
    );

    // Inline snapshot should be a summary (no nodeBody — LLM-friendly)
    expect(result.preMutationSnapshot).toHaveLength(1);
    const snap = result.preMutationSnapshot[0];
    expect(snap.nodeID).toBe("n2");
    expect(snap.nodeName).toBe("STG_ORDERS");
    expect(snap.columnID).toBe("c2");
    expect(snap.previousColumnName).toBe("order_id");
    expect(snap.previousDataType).toBe("VARCHAR");
    expect(snap.capturedAt).toBeDefined();
    // nodeBody should NOT be in the inline snapshot (only on disk)
    expect(snap).not.toHaveProperty("nodeBody");

    // Write should still proceed
    expect(result.totalUpdated).toBe(1);
    expect(result.updatedNodes).toHaveLength(1);
  });

  it("captures snapshots even when writes fail (partial failure)", async () => {
    const client = createMockClient();
    const cache = buildFakeCache({
      n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "col_a" }] },
      n2: { name: "STG_A", nodeType: "Stage", columns: [{ id: "c2", name: "col_a" }] },
      n3: { name: "STG_B", nodeType: "Stage", columns: [{ id: "c3", name: "col_a" }] },
    });

    vi.mocked(walkColumnLineage).mockReturnValue([
      { nodeID: "n2", nodeName: "STG_A", nodeType: "Stage", columnID: "c2", columnName: "col_a", direction: "downstream" as const, depth: 1 },
      { nodeID: "n3", nodeName: "STG_B", nodeType: "Stage", columnID: "c3", columnName: "col_a", direction: "downstream" as const, depth: 2 },
    ]);

    client.get
      .mockResolvedValueOnce({
        id: "n2", name: "STG_A",
        metadata: { columns: [{ id: "c2", columnID: "c2", name: "col_a", dataType: "INT" }] },
      })
      .mockResolvedValueOnce({
        id: "n3", name: "STG_B",
        metadata: { columns: [{ id: "c3", columnID: "c3", name: "col_a", dataType: "INT" }] },
      });

    // First write succeeds, second fails, rollback succeeds
    vi.mocked(setWorkspaceNode)
      .mockResolvedValueOnce(undefined as never)
      .mockRejectedValueOnce(new Error("API timeout"))
      .mockResolvedValueOnce(undefined as never);

    const result = await propagateColumnChange(
      client as never,
      cache,
      "ws-1",
      "n1",
      "c1",
      { columnName: "renamed_col" },
    );

    // Both snapshots captured during Phase 1 (before any writes)
    expect(result.preMutationSnapshot).toHaveLength(2);
    expect(result.preMutationSnapshot[0].nodeID).toBe("n2");
    expect(result.preMutationSnapshot[1].nodeID).toBe("n3");

    // The successful write should be rolled back, so no downstream updates remain applied
    expect(result.partialFailure).toBeUndefined();
    expect(result.rolledBack).toBe(true);
    expect(result.totalUpdated).toBe(0);
    expect(result.updatedNodes).toEqual([]);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].message).toBe("API timeout");
    expect(setWorkspaceNode).toHaveBeenCalledTimes(3);
  });

  it("updates all affected columns on the same downstream node in a single write", async () => {
    const client = createMockClient();
    const cache = buildFakeCache({
      n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "order_id" }] },
      n2: {
        name: "STG_ORDERS",
        nodeType: "Stage",
        columns: [
          { id: "c2", name: "order_id" },
          { id: "c3", name: "order_id_copy" },
        ],
      },
    });

    vi.mocked(walkColumnLineage).mockReturnValue([
      { nodeID: "n2", nodeName: "STG_ORDERS", nodeType: "Stage", columnID: "c2", columnName: "order_id", direction: "downstream" as const, depth: 1 },
      { nodeID: "n2", nodeName: "STG_ORDERS", nodeType: "Stage", columnID: "c3", columnName: "order_id_copy", direction: "downstream" as const, depth: 1 },
    ]);

    client.get.mockResolvedValue({
      id: "n2",
      name: "STG_ORDERS",
      metadata: {
        columns: [
          { id: "c2", columnID: "c2", name: "order_id", dataType: "VARCHAR" },
          { id: "c3", columnID: "c3", name: "order_id_copy", dataType: "VARCHAR" },
        ],
      },
    });
    vi.mocked(setWorkspaceNode).mockResolvedValue(undefined as never);

    const result = await propagateColumnChange(
      client as never,
      cache,
      "ws-1",
      "n1",
      "c1",
      { columnName: "renamed_order_id" },
    );

    expect(result.totalUpdated).toBe(2);
    expect(result.errors).toEqual([]);
    expect(setWorkspaceNode).toHaveBeenCalledTimes(1);
    expect(vi.mocked(setWorkspaceNode).mock.calls[0]?.[1]).toEqual({
      workspaceID: "ws-1",
      nodeID: "n2",
      body: {
        metadata: {
          columns: [
            { id: "c2", columnID: "c2", name: "renamed_order_id", dataType: "VARCHAR" },
            { id: "c3", columnID: "c3", name: "renamed_order_id", dataType: "VARCHAR" },
          ],
        },
        merged: true,
      },
    });
  });

  it("reports partial failure only when rollback also fails", async () => {
    const client = createMockClient();
    const cache = buildFakeCache({
      n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "col_a" }] },
      n2: { name: "STG_A", nodeType: "Stage", columns: [{ id: "c2", name: "col_a" }] },
      n3: { name: "STG_B", nodeType: "Stage", columns: [{ id: "c3", name: "col_a" }] },
    });

    vi.mocked(walkColumnLineage).mockReturnValue([
      { nodeID: "n2", nodeName: "STG_A", nodeType: "Stage", columnID: "c2", columnName: "col_a", direction: "downstream" as const, depth: 1 },
      { nodeID: "n3", nodeName: "STG_B", nodeType: "Stage", columnID: "c3", columnName: "col_a", direction: "downstream" as const, depth: 2 },
    ]);

    client.get
      .mockResolvedValueOnce({
        id: "n2", name: "STG_A",
        metadata: { columns: [{ id: "c2", columnID: "c2", name: "col_a", dataType: "INT" }] },
      })
      .mockResolvedValueOnce({
        id: "n3", name: "STG_B",
        metadata: { columns: [{ id: "c3", columnID: "c3", name: "col_a", dataType: "INT" }] },
      });

    vi.mocked(setWorkspaceNode)
      .mockResolvedValueOnce(undefined as never)
      .mockRejectedValueOnce(new Error("API timeout"))
      .mockRejectedValueOnce(new Error("Rollback failed"));

    const result = await propagateColumnChange(
      client as never,
      cache,
      "ws-1",
      "n1",
      "c1",
      { columnName: "renamed_col" },
    );

    expect(result.partialFailure).toBe(true);
    expect(result.rolledBack).toBeUndefined();
    expect(result.totalUpdated).toBe(1);
    expect(result.updatedNodes.map((node) => node.nodeID)).toEqual(["n2"]);
    expect(result.errors.map((error) => error.message)).toEqual([
      "API timeout",
      "Rollback failed after propagation error: Rollback failed",
    ]);
  });

  it("returns empty snapshot when no downstream entries exist", async () => {
    const client = createMockClient();
    const cache = buildFakeCache({
      n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "col_a" }] },
    });

    vi.mocked(walkColumnLineage).mockReturnValue([]);

    const result = await propagateColumnChange(
      client as never,
      cache,
      "ws-1",
      "n1",
      "c1",
      { columnName: "renamed_col" },
    );

    expect(result.preMutationSnapshot).toHaveLength(0);
    expect(result.totalUpdated).toBe(0);
    expect(setWorkspaceNode).not.toHaveBeenCalled();
  });

  it("does not apply any writes when prepare phase fails for one of the downstream nodes", async () => {
    const client = createMockClient();
    const cache = buildFakeCache({
      n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "col_a" }] },
      n2: { name: "STG_A", nodeType: "Stage", columns: [{ id: "c2", name: "col_a" }] },
      n3: { name: "STG_B", nodeType: "Stage", columns: [{ id: "c3", name: "col_a" }] },
    });

    vi.mocked(walkColumnLineage).mockReturnValue([
      { nodeID: "n2", nodeName: "STG_A", nodeType: "Stage", columnID: "c2", columnName: "col_a", direction: "downstream" as const, depth: 1 },
      { nodeID: "n3", nodeName: "STG_B", nodeType: "Stage", columnID: "c3", columnName: "col_a", direction: "downstream" as const, depth: 2 },
    ]);

    client.get
      .mockResolvedValueOnce({
        id: "n2",
        name: "STG_A",
        metadata: { columns: [{ id: "c2", columnID: "c2", name: "col_a", dataType: "INT" }] },
      })
      .mockResolvedValueOnce({
        id: "n3",
        name: "STG_B",
        metadata: null,
      });

    const result = await propagateColumnChange(
      client as never,
      cache,
      "ws-1",
      "n1",
      "c1",
      { columnName: "renamed_col" },
    );

    expect(result.totalUpdated).toBe(0);
    expect(result.errors).toEqual([
      { nodeID: "n3", columnID: "c3", message: "Could not read node metadata" },
    ]);
    expect(result.skippedNodes?.map((node) => node.nodeID)).toEqual(["n2"]);
    expect(setWorkspaceNode).not.toHaveBeenCalled();
  });

  describe("disk snapshot persistence", () => {
    let testDir: string;

    beforeEach(() => {
      testDir = join(tmpdir(), `propagation-test-${Date.now()}`);
    });

    afterEach(() => {
      if (testDir && existsSync(testDir)) {
        rmSync(testDir, { recursive: true, force: true });
      }
    });

    it("writes snapshot to disk when baseDir is provided", async () => {
      const client = createMockClient();
      const cache = buildFakeCache({
        n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "order_id" }] },
        n2: { name: "STG_ORDERS", nodeType: "Stage", columns: [{ id: "c2", name: "order_id" }] },
      });

      vi.mocked(walkColumnLineage).mockReturnValue([
        { nodeID: "n2", nodeName: "STG_ORDERS", nodeType: "Stage", columnID: "c2", columnName: "order_id", direction: "downstream" as const, depth: 1 },
      ]);

      client.get.mockResolvedValue({
        id: "n2", name: "STG_ORDERS",
        metadata: { columns: [{ id: "c2", columnID: "c2", name: "order_id", dataType: "VARCHAR" }] },
      });
      vi.mocked(setWorkspaceNode).mockResolvedValue(undefined as never);

      const result = await propagateColumnChange(
        client as never,
        cache,
        "ws-1",
        "n1",
        "c1",
        { columnName: "renamed_order_id" },
        undefined,
        testDir,
      );

      expect(result.snapshotPath).toBeDefined();
      expect(result.snapshotPath).toContain("propagation-snapshots");
      expect(result.snapshotPath).toContain("propagation-ws-1-");
      expect(existsSync(result.snapshotPath!)).toBe(true);

      const diskData = JSON.parse(readFileSync(result.snapshotPath!, "utf-8"));
      expect(diskData.workspaceID).toBe("ws-1");
      expect(diskData.sourceNodeID).toBe("n1");
      expect(diskData.sourceColumnID).toBe("c1");
      expect(diskData.changes).toEqual({ columnName: "renamed_order_id" });
      // Disk snapshot has full entries with nodeBody for reversal
      expect(diskData.entries).toHaveLength(1);
      expect(diskData.entries[0].nodeID).toBe("n2");
      expect(diskData.entries[0].previousColumnName).toBe("order_id");
      expect(diskData.entries[0].nodeBody).toBeDefined();
      expect(diskData.entries[0].nodeBody.id).toBe("n2");
      expect(diskData.entries[0].nodeBody.metadata.columns).toHaveLength(1);
    });

    it("rejects workspaceID with path separators before writing snapshot", async () => {
      const client = createMockClient();
      const cache = buildFakeCache({
        n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "col_a" }] },
      });

      vi.mocked(walkColumnLineage).mockReturnValue([]);

      await expect(
        propagateColumnChange(
          client as never,
          cache,
          "../escape",
          "n1",
          "c1",
          { columnName: "renamed_col" },
          undefined,
          testDir,
        ),
      ).rejects.toThrow(/Invalid workspaceID/);

      // Directory must not exist — validatePathSegment should throw before any disk work
      const snapshotDir = join(testDir, ".coalesce-transform-mcp-cache", "propagation-snapshots");
      expect(existsSync(snapshotDir)).toBe(false);
    });

    it("uses the validated workspaceID in the snapshot filename", async () => {
      const client = createMockClient();
      const cache = buildFakeCache({
        n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "col_a" }] },
        n2: { name: "STG_A", nodeType: "Stage", columns: [{ id: "c2", name: "col_a" }] },
      });

      vi.mocked(walkColumnLineage).mockReturnValue([
        { nodeID: "n2", nodeName: "STG_A", nodeType: "Stage", columnID: "c2", columnName: "col_a", direction: "downstream" as const, depth: 1 },
      ]);

      client.get.mockResolvedValue({
        id: "n2", name: "STG_A",
        metadata: { columns: [{ id: "c2", columnID: "c2", name: "col_a", dataType: "VARCHAR" }] },
      });
      vi.mocked(setWorkspaceNode).mockResolvedValue(undefined as never);

      const result = await propagateColumnChange(
        client as never,
        cache,
        "ws-safe-id",
        "n1",
        "c1",
        { columnName: "renamed_col" },
        undefined,
        testDir,
      );

      expect(result.snapshotPath).toBeDefined();
      // The filename must come from safeWorkspaceID (validated), not a raw string that
      // could contain path-traversal characters in a future refactor.
      expect(result.snapshotPath).toMatch(/propagation-ws-safe-id-/);
    });

    it("does not fail propagation when disk write fails", async () => {
      const client = createMockClient();
      const cache = buildFakeCache({
        n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "col_a" }] },
        n2: { name: "STG_A", nodeType: "Stage", columns: [{ id: "c2", name: "col_a" }] },
      });

      vi.mocked(walkColumnLineage).mockReturnValue([
        { nodeID: "n2", nodeName: "STG_A", nodeType: "Stage", columnID: "c2", columnName: "col_a", direction: "downstream" as const, depth: 1 },
      ]);

      client.get.mockResolvedValue({
        id: "n2", name: "STG_A",
        metadata: { columns: [{ id: "c2", columnID: "c2", name: "col_a", dataType: "INT" }] },
      });
      vi.mocked(setWorkspaceNode).mockResolvedValue(undefined as never);

      // Use an invalid path that will fail mkdirSync
      const result = await propagateColumnChange(
        client as never,
        cache,
        "ws-1",
        "n1",
        "c1",
        { columnName: "renamed_col" },
        undefined,
        "/nonexistent/readonly/path",
      );

      // Propagation should still succeed even though disk write failed
      expect(result.snapshotPath).toBeUndefined();
      expect(result.totalUpdated).toBe(1);
      expect(result.preMutationSnapshot).toHaveLength(1);
    });
  });

  it("disk snapshot nodeBody is a deep clone not affected by subsequent mutations", async () => {
    const testDir = join(tmpdir(), `propagation-clone-test-${Date.now()}`);
    try {
      const client = createMockClient();
      const cache = buildFakeCache({
        n1: { name: "SRC_RAW", nodeType: "Source", columns: [{ id: "c1", name: "col_a" }] },
        n2: { name: "STG_A", nodeType: "Stage", columns: [{ id: "c2", name: "col_a" }] },
      });

      vi.mocked(walkColumnLineage).mockReturnValue([
        { nodeID: "n2", nodeName: "STG_A", nodeType: "Stage", columnID: "c2", columnName: "col_a", direction: "downstream" as const, depth: 1 },
      ]);

      const originalNode = {
        id: "n2", name: "STG_A",
        metadata: { columns: [{ id: "c2", columnID: "c2", name: "col_a", dataType: "VARCHAR" }] },
      };
      client.get.mockResolvedValue(originalNode);
      vi.mocked(setWorkspaceNode).mockResolvedValue(undefined as never);

      const result = await propagateColumnChange(
        client as never,
        cache,
        "ws-1",
        "n1",
        "c1",
        { columnName: "renamed_col" },
        undefined,
        testDir,
      );

      // Mutating the original object after snapshot should not affect disk data
      originalNode.metadata.columns[0].name = "MUTATED";

      const diskData = JSON.parse(readFileSync(result.snapshotPath!, "utf-8"));
      expect(diskData.entries[0].nodeBody.metadata.columns[0].name).toBe("col_a");
    } finally {
      if (existsSync(testDir)) rmSync(testDir, { recursive: true, force: true });
    }
  });
});
