import { describe, it, expect, beforeEach, vi } from "vitest";

vi.mock("../../src/coalesce/api/nodes.js", () => ({
  listWorkspaceNodes: vi.fn(),
}));

import { listWorkspaceNodes } from "../../src/coalesce/api/nodes.js";
import {
  getWorkspaceNodeIndex,
  invalidateWorkspaceNodeIndex,
  clearWorkspaceNodeIndexCache,
} from "../../src/services/cache/workspace-node-index.js";
import type { CoalesceClient } from "../../src/client.js";

const mockListWorkspaceNodes = vi.mocked(listWorkspaceNodes);

function makeClient(): CoalesceClient {
  return {} as CoalesceClient;
}

beforeEach(() => {
  vi.resetAllMocks();
  clearWorkspaceNodeIndexCache();
});

describe("fetchWorkspaceNodeIndex - pagination and dedup", () => {
  it("extracts id/name/nodeType/locationName from a single page", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "DEV" },
        { id: "n-2", name: "DIM_USERS", nodeType: "Dimension" }, // no locationName
      ],
    });

    const entries = await getWorkspaceNodeIndex(makeClient(), "ws-1");
    expect(entries).toEqual([
      { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "DEV" },
      { id: "n-2", name: "DIM_USERS", nodeType: "Dimension", locationName: null },
    ]);
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(1);
  });

  it("defaults nodeType and locationName to null when non-string", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", name: "X", nodeType: 42, locationName: null },
      ],
    });

    const entries = await getWorkspaceNodeIndex(makeClient(), "ws-null");
    expect(entries[0].nodeType).toBeNull();
    expect(entries[0].locationName).toBeNull();
  });

  it("skips entries missing id or name", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", name: "KEEP" },
        { id: "n-2" }, // missing name
        { name: "NO_ID" }, // missing id
        "not-an-object",
        null,
      ],
    });

    const entries = await getWorkspaceNodeIndex(makeClient(), "ws-skip");
    expect(entries.map((e) => e.id)).toEqual(["n-1"]);
  });

  it("follows `next` cursors across multiple pages", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({
        data: [{ id: "n-1", name: "A" }],
        next: "cursor-2",
      })
      .mockResolvedValueOnce({
        data: [{ id: "n-2", name: "B" }],
        next: "cursor-3",
      })
      .mockResolvedValueOnce({
        data: [{ id: "n-3", name: "C" }],
      });

    const entries = await getWorkspaceNodeIndex(makeClient(), "ws-paginate");
    expect(entries.map((e) => e.id)).toEqual(["n-1", "n-2", "n-3"]);
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(3);

    // First call must not carry a startingFrom; subsequent calls must.
    expect(mockListWorkspaceNodes.mock.calls[0][1]).not.toHaveProperty("startingFrom");
    expect(mockListWorkspaceNodes.mock.calls[1][1]).toMatchObject({ startingFrom: "cursor-2" });
    expect(mockListWorkspaceNodes.mock.calls[2][1]).toMatchObject({ startingFrom: "cursor-3" });
  });

  it("coerces a numeric `next` cursor to a string", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({
        data: [{ id: "n-1", name: "A" }],
        next: 42,
      })
      .mockResolvedValueOnce({
        data: [{ id: "n-2", name: "B" }],
      });

    const entries = await getWorkspaceNodeIndex(makeClient(), "ws-numeric-cursor");
    expect(entries.map((e) => e.id)).toEqual(["n-1", "n-2"]);
    expect(mockListWorkspaceNodes.mock.calls[1][1]).toMatchObject({ startingFrom: "42" });
  });

  it("treats a whitespace-only `next` as end-of-pagination", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [{ id: "n-1", name: "A" }],
      next: "   ",
    });

    const entries = await getWorkspaceNodeIndex(makeClient(), "ws-blank-cursor");
    expect(entries.map((e) => e.id)).toEqual(["n-1"]);
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(1);
  });

  it("throws when a repeated cursor is returned (would infinite-loop without dedup)", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({
        data: [{ id: "n-1", name: "A" }],
        next: "cursor-same",
      })
      .mockResolvedValueOnce({
        data: [{ id: "n-2", name: "B" }],
        next: "cursor-same", // repeat — broken API
      });

    await expect(getWorkspaceNodeIndex(makeClient(), "ws-repeat")).rejects.toThrow(
      /repeated cursor cursor-same/
    );
  });

  it("throws when pagination exceeds MAX_PAGES (runaway loop guard)", async () => {
    // Simulate an API that always returns a fresh cursor. The MAX_PAGES guard
    // (500) keeps this from hanging the process indefinitely.
    let counter = 0;
    mockListWorkspaceNodes.mockImplementation(async () => ({
      data: [{ id: `n-${counter}`, name: `N${counter}` }],
      next: `cursor-${counter++}`,
    }));

    await expect(getWorkspaceNodeIndex(makeClient(), "ws-runaway")).rejects.toThrow(
      /exceeded 500 pages/
    );
  });

  it("throws when the response is not a plain object", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce("not an object" as unknown);

    await expect(getWorkspaceNodeIndex(makeClient(), "ws-bad")).rejects.toThrow(
      /not an object/
    );
  });

  it("returns an empty array when `data` is missing or non-array", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({ /* no data */ });

    const entries = await getWorkspaceNodeIndex(makeClient(), "ws-empty");
    expect(entries).toEqual([]);
  });
});

describe("getWorkspaceNodeIndex - cache behavior", () => {
  it("caches results across calls within the same workspace", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [{ id: "n-1", name: "A" }],
    });

    const client = makeClient();
    const first = await getWorkspaceNodeIndex(client, "ws-cache");
    const second = await getWorkspaceNodeIndex(client, "ws-cache");

    expect(first).toBe(second);
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(1);
  });

  it("coalesces concurrent callers into a single fetch", async () => {
    mockListWorkspaceNodes.mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      return { data: [{ id: "n-1", name: "A" }] };
    });

    const client = makeClient();
    const results = await Promise.all([
      getWorkspaceNodeIndex(client, "ws-concurrent"),
      getWorkspaceNodeIndex(client, "ws-concurrent"),
      getWorkspaceNodeIndex(client, "ws-concurrent"),
    ]);

    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(1);
    expect(results[0]).toBe(results[1]);
    expect(results[1]).toBe(results[2]);
  });

  it("re-fetches after invalidation", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({ data: [{ id: "n-1", name: "A" }] })
      .mockResolvedValueOnce({ data: [{ id: "n-2", name: "B" }] });

    const client = makeClient();
    const first = await getWorkspaceNodeIndex(client, "ws-inv");
    invalidateWorkspaceNodeIndex("ws-inv");
    const second = await getWorkspaceNodeIndex(client, "ws-inv");

    expect(first.map((e) => e.id)).toEqual(["n-1"]);
    expect(second.map((e) => e.id)).toEqual(["n-2"]);
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(2);
  });

  it("does not share cache entries between workspaces", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({ data: [{ id: "n-1", name: "A" }] })
      .mockResolvedValueOnce({ data: [{ id: "n-2", name: "B" }] });

    const client = makeClient();
    const a = await getWorkspaceNodeIndex(client, "ws-a");
    const b = await getWorkspaceNodeIndex(client, "ws-b");

    expect(a.map((e) => e.id)).toEqual(["n-1"]);
    expect(b.map((e) => e.id)).toEqual(["n-2"]);
  });

  it("does not cache the value when invalidated mid-flight", async () => {
    let release!: (value: unknown) => void;
    const pending = new Promise((resolve) => {
      release = resolve;
    });
    mockListWorkspaceNodes.mockImplementationOnce(() => pending);

    const client = makeClient();
    const loading = getWorkspaceNodeIndex(client, "ws-race");
    invalidateWorkspaceNodeIndex("ws-race");
    release({ data: [{ id: "n-1", name: "A" }] });
    await loading;

    // Next call must re-fetch since the stale fetch was discarded.
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [{ id: "n-2", name: "B" }],
    });
    const next = await getWorkspaceNodeIndex(client, "ws-race");
    expect(next.map((e) => e.id)).toEqual(["n-2"]);
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(2);
  });
});
