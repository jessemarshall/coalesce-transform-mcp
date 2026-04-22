import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../src/coalesce/api/nodes.js", () => ({
  listWorkspaceNodes: vi.fn(),
}));

import { listWorkspaceNodes } from "../../src/coalesce/api/nodes.js";
import { listWorkspaceNodeTypes } from "../../src/services/workspace/mutations.js";
import {
  getCachedInventory,
  invalidateWorkspaceInventory,
  clearWorkspaceInventoryCache,
} from "../../src/services/cache/workspace-inventory.js";
import type { CoalesceClient } from "../../src/client.js";

const mockListWorkspaceNodes = vi.mocked(listWorkspaceNodes);

function makeClient(): CoalesceClient {
  return {} as CoalesceClient;
}

beforeEach(() => {
  vi.resetAllMocks();
  clearWorkspaceInventoryCache();
});

describe("workspace node-type inventory cache", () => {
  it("caches the result and only fetches once across calls", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", nodeType: "Stage" },
        { id: "n-2", nodeType: "Stage" },
        { id: "n-3", nodeType: "Dimension" },
      ],
    });

    const client = makeClient();
    const first = await listWorkspaceNodeTypes(client, { workspaceID: "ws-cache" });
    const second = await listWorkspaceNodeTypes(client, { workspaceID: "ws-cache" });

    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(1);
    expect(first).toEqual(second);
    expect(first.counts).toEqual({ Stage: 2, Dimension: 1 });
    expect(first.total).toBe(3);
  });

  it("coalesces concurrent callers into a single fetch", async () => {
    mockListWorkspaceNodes.mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      return {
        data: [{ id: "n-1", nodeType: "Stage" }],
      };
    });

    const client = makeClient();
    const results = await Promise.all([
      listWorkspaceNodeTypes(client, { workspaceID: "ws-concurrent" }),
      listWorkspaceNodeTypes(client, { workspaceID: "ws-concurrent" }),
      listWorkspaceNodeTypes(client, { workspaceID: "ws-concurrent" }),
    ]);

    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(1);
    expect(results[0]).toEqual(results[1]);
    expect(results[1]).toEqual(results[2]);
  });

  it("re-fetches after invalidation", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({ data: [{ id: "n-1", nodeType: "Stage" }] })
      .mockResolvedValueOnce({ data: [{ id: "n-2", nodeType: "View" }] });

    const client = makeClient();
    const first = await listWorkspaceNodeTypes(client, { workspaceID: "ws-invalidate" });
    invalidateWorkspaceInventory("ws-invalidate");
    const second = await listWorkspaceNodeTypes(client, { workspaceID: "ws-invalidate" });

    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(2);
    expect(first.counts).toEqual({ Stage: 1 });
    expect(second.counts).toEqual({ View: 1 });
  });

  it("does not share cache entries between different workspaces", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({ data: [{ id: "n-1", nodeType: "Stage" }] })
      .mockResolvedValueOnce({ data: [{ id: "n-2", nodeType: "Dimension" }] });

    const client = makeClient();
    const a = await listWorkspaceNodeTypes(client, { workspaceID: "ws-a" });
    const b = await listWorkspaceNodeTypes(client, { workspaceID: "ws-b" });

    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(2);
    expect(a.counts).toEqual({ Stage: 1 });
    expect(b.counts).toEqual({ Dimension: 1 });
  });

  it("getCachedInventory returns undefined for untouched workspaces", () => {
    expect(getCachedInventory("ws-untouched")).toBeUndefined();
  });

  it("does not overwrite the cache when invalidated mid-flight", async () => {
    let releaseFetch: (value: { data: unknown[] }) => void = () => {};
    const fetchPromise = new Promise<{ data: unknown[] }>((resolve) => {
      releaseFetch = resolve;
    });
    mockListWorkspaceNodes.mockImplementationOnce(() => fetchPromise);

    const client = makeClient();
    const loading = listWorkspaceNodeTypes(client, { workspaceID: "ws-race" });

    // Invalidate *during* the in-flight fetch.
    invalidateWorkspaceInventory("ws-race");

    releaseFetch({ data: [{ id: "n-1", nodeType: "Stage" }] });
    await loading;

    // The stale result must NOT have populated the cache.
    expect(getCachedInventory("ws-race")).toBeUndefined();
  });
});
