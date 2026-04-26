import { describe, it, expect, beforeEach, vi } from "vitest";

vi.mock("../../src/coalesce/api/nodes.js", () => ({
  listWorkspaceNodes: vi.fn(),
  getWorkspaceNode: vi.fn(),
}));

import { listWorkspaceNodes, getWorkspaceNode } from "../../src/coalesce/api/nodes.js";
import {
  getWorkspaceNodeDetailIndex,
  invalidateWorkspaceNodeDetailIndex,
  clearWorkspaceNodeDetailIndexCache,
  peekWorkspaceNodeDetail,
  populateWorkspaceNodeDetail,
  getCachedOrFetchWorkspaceNodeDetail,
} from "../../src/services/cache/workspace-node-detail-index.js";
import type { CoalesceClient } from "../../src/client.js";

const mockListWorkspaceNodes = vi.mocked(listWorkspaceNodes);
const mockGetWorkspaceNode = vi.mocked(getWorkspaceNode);

function makeClient(): CoalesceClient {
  return {} as CoalesceClient;
}

beforeEach(() => {
  vi.resetAllMocks();
  clearWorkspaceNodeDetailIndexCache();
});

describe("getWorkspaceNodeDetailIndex - pagination", () => {
  it("requests detail=true and indexes nodes by id", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", name: "A", metadata: { columns: [] } },
        { id: "n-2", name: "B", metadata: { columns: [{ name: "X" }] } },
      ],
    });

    const map = await getWorkspaceNodeDetailIndex(makeClient(), "ws-1");

    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(1);
    const params = mockListWorkspaceNodes.mock.calls[0][1];
    expect(params).toMatchObject({ workspaceID: "ws-1", detail: true });
    expect(map.get("n-1")?.name).toBe("A");
    expect(map.get("n-2")?.metadata).toMatchObject({ columns: [{ name: "X" }] });
  });

  it("skips entries missing a string id", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [
        { id: "n-1", name: "KEEP" },
        { id: 42, name: "BAD_ID_TYPE" },
        { name: "NO_ID" },
        "not-an-object",
        null,
      ],
    });

    const map = await getWorkspaceNodeDetailIndex(makeClient(), "ws-skip");
    expect([...map.keys()]).toEqual(["n-1"]);
  });

  it("follows next cursors across pages", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({ data: [{ id: "n-1", name: "A" }], next: "c2" })
      .mockResolvedValueOnce({ data: [{ id: "n-2", name: "B" }], next: "c3" })
      .mockResolvedValueOnce({ data: [{ id: "n-3", name: "C" }] });

    const map = await getWorkspaceNodeDetailIndex(makeClient(), "ws-paginate");
    expect([...map.keys()]).toEqual(["n-1", "n-2", "n-3"]);
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(3);
    expect(mockListWorkspaceNodes.mock.calls[0][1]).not.toHaveProperty("startingFrom");
    expect(mockListWorkspaceNodes.mock.calls[1][1]).toMatchObject({ startingFrom: "c2" });
    expect(mockListWorkspaceNodes.mock.calls[2][1]).toMatchObject({ startingFrom: "c3" });
  });

  it("rejects on repeated cursor (broken API guard)", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({ data: [{ id: "n-1", name: "A" }], next: "same" })
      .mockResolvedValueOnce({ data: [{ id: "n-2", name: "B" }], next: "same" });

    await expect(getWorkspaceNodeDetailIndex(makeClient(), "ws-rep")).rejects.toThrow(
      /repeated cursor same/
    );
  });

  it("rejects when list response is not an object", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce("nope" as unknown);

    await expect(getWorkspaceNodeDetailIndex(makeClient(), "ws-bad")).rejects.toThrow(
      /not an object/
    );
  });
});

describe("getWorkspaceNodeDetailIndex - cache behavior", () => {
  it("caches across calls within a workspace", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [{ id: "n-1", name: "A" }],
    });
    const client = makeClient();

    const first = await getWorkspaceNodeDetailIndex(client, "ws-cache");
    const second = await getWorkspaceNodeDetailIndex(client, "ws-cache");

    expect(first).toBe(second);
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(1);
  });

  it("re-fetches after invalidation", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({ data: [{ id: "n-1", name: "A" }] })
      .mockResolvedValueOnce({ data: [{ id: "n-2", name: "B" }] });
    const client = makeClient();

    await getWorkspaceNodeDetailIndex(client, "ws-inv");
    invalidateWorkspaceNodeDetailIndex("ws-inv");
    const second = await getWorkspaceNodeDetailIndex(client, "ws-inv");

    expect([...second.keys()]).toEqual(["n-2"]);
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(2);
  });
});

describe("peekWorkspaceNodeDetail / populateWorkspaceNodeDetail", () => {
  it("peek returns undefined when the workspace index is cold", () => {
    expect(peekWorkspaceNodeDetail("ws-cold", "n-1")).toBeUndefined();
  });

  it("populate is a no-op on a cold workspace (avoids implying a complete index)", async () => {
    populateWorkspaceNodeDetail("ws-cold", { id: "n-1", name: "A" });
    expect(peekWorkspaceNodeDetail("ws-cold", "n-1")).toBeUndefined();
  });

  it("populate write-throughs into a warm workspace", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [{ id: "n-1", name: "A" }],
    });
    await getWorkspaceNodeDetailIndex(makeClient(), "ws-warm");

    populateWorkspaceNodeDetail("ws-warm", { id: "n-2", name: "B" });
    expect(peekWorkspaceNodeDetail("ws-warm", "n-2")).toMatchObject({ name: "B" });
  });
});

describe("getCachedOrFetchWorkspaceNodeDetail", () => {
  it("returns cached body without a network call when warm", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [{ id: "n-1", name: "A", metadata: { columns: [] } }],
    });
    const client = makeClient();
    await getWorkspaceNodeDetailIndex(client, "ws-warm");

    const node = await getCachedOrFetchWorkspaceNodeDetail(client, "ws-warm", "n-1");
    expect(node).toMatchObject({ id: "n-1", name: "A" });
    expect(mockGetWorkspaceNode).not.toHaveBeenCalled();
  });

  it("falls back to getWorkspaceNode when cold", async () => {
    mockGetWorkspaceNode.mockResolvedValueOnce({ id: "n-9", name: "FRESH" });
    const client = makeClient();

    const node = await getCachedOrFetchWorkspaceNodeDetail(client, "ws-cold", "n-9");
    expect(node).toMatchObject({ id: "n-9", name: "FRESH" });
    expect(mockGetWorkspaceNode).toHaveBeenCalledWith(client, {
      workspaceID: "ws-cold",
      nodeID: "n-9",
    });
    expect(mockListWorkspaceNodes).not.toHaveBeenCalled();
  });

  it("write-throughs the fetched body so subsequent peeks hit when the workspace is warm", async () => {
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [{ id: "n-1", name: "A" }],
    });
    mockGetWorkspaceNode.mockResolvedValueOnce({ id: "n-2", name: "WRITTEN" });
    const client = makeClient();
    await getWorkspaceNodeDetailIndex(client, "ws-warm");

    await getCachedOrFetchWorkspaceNodeDetail(client, "ws-warm", "n-2");
    expect(peekWorkspaceNodeDetail("ws-warm", "n-2")).toMatchObject({ name: "WRITTEN" });
  });
});
