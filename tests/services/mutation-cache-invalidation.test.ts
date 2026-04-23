import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../src/coalesce/api/nodes.js", () => ({
  listWorkspaceNodes: vi.fn(),
  getWorkspaceNode: vi.fn(),
  setWorkspaceNode: vi.fn(),
  deleteWorkspaceNode: vi.fn(),
}));

import {
  listWorkspaceNodes,
  getWorkspaceNode,
  setWorkspaceNode,
  deleteWorkspaceNode as deleteWorkspaceNodeApi,
} from "../../src/coalesce/api/nodes.js";
import {
  deleteWorkspaceNode,
  updateWorkspaceNode,
  setWorkspaceNodeAndInvalidate,
  listWorkspaceNodeTypes,
} from "../../src/services/workspace/mutations.js";
import {
  getWorkspaceNodeIndex,
  invalidateWorkspaceNodeIndex,
} from "../../src/services/cache/workspace-node-index.js";
import type { CoalesceClient } from "../../src/client.js";

const mockListWorkspaceNodes = vi.mocked(listWorkspaceNodes);
const mockGetWorkspaceNode = vi.mocked(getWorkspaceNode);
const mockSetWorkspaceNode = vi.mocked(setWorkspaceNode);
const mockDeleteWorkspaceNodeApi = vi.mocked(deleteWorkspaceNodeApi);

function makeClient(): CoalesceClient {
  return {} as CoalesceClient;
}

beforeEach(() => {
  vi.resetAllMocks();
  mockSetWorkspaceNode.mockResolvedValue({ ok: true });
  mockDeleteWorkspaceNodeApi.mockResolvedValue({ ok: true });
});

describe("cache invalidation on mutations", () => {
  it("deleteWorkspaceNode invalidates both the inventory and node-index caches", async () => {
    let deleted = false;
    mockListWorkspaceNodes.mockImplementation(async () => {
      return deleted
        ? { data: [{ id: "n-1", name: "A", nodeType: "Stage", locationName: "RAW" }] }
        : {
            data: [
              { id: "n-1", name: "A", nodeType: "Stage", locationName: "RAW" },
              { id: "n-2", name: "B", nodeType: "Dimension", locationName: "EDM" },
            ],
          };
    });

    const client = makeClient();

    const typesBefore = await listWorkspaceNodeTypes(client, { workspaceID: "ws-1" });
    const indexBefore = await getWorkspaceNodeIndex(client, "ws-1");
    expect(typesBefore.counts).toEqual({ Stage: 1, Dimension: 1 });
    expect(indexBefore).toHaveLength(2);
    const callsAfterPriming = mockListWorkspaceNodes.mock.calls.length;

    deleted = true;
    await deleteWorkspaceNode(client, { workspaceID: "ws-1", nodeID: "n-2" });

    const typesAfter = await listWorkspaceNodeTypes(client, { workspaceID: "ws-1" });
    const indexAfter = await getWorkspaceNodeIndex(client, "ws-1");
    expect(typesAfter.counts).toEqual({ Stage: 1 });
    expect(indexAfter).toHaveLength(1);
    expect(mockListWorkspaceNodes.mock.calls.length).toBeGreaterThan(callsAfterPriming);
  });

  it("updateWorkspaceNode invalidates the node-index cache (rename case)", async () => {
    let renamed = false;
    mockListWorkspaceNodes.mockImplementation(async () => ({
      data: [
        {
          id: "n-1",
          name: renamed ? "NEW_NAME" : "OLD_NAME",
          nodeType: "Stage",
          locationName: "RAW",
        },
      ],
    }));
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-1",
      name: "OLD_NAME",
      metadata: {},
    });

    const client = makeClient();

    const indexBefore = await getWorkspaceNodeIndex(client, "ws-1");
    expect(indexBefore[0]?.name).toBe("OLD_NAME");

    renamed = true;
    await updateWorkspaceNode(client, {
      workspaceID: "ws-1",
      nodeID: "n-1",
      changes: { name: "NEW_NAME" },
    });

    const indexAfter = await getWorkspaceNodeIndex(client, "ws-1");
    expect(indexAfter[0]?.name).toBe("NEW_NAME");
  });

  it("node-index cache: mid-flight invalidation is not overwritten by the stale fetch", async () => {
    let releaseFetch: (v: { data: unknown[] }) => void = () => {};
    const pending = new Promise<{ data: unknown[] }>((resolve) => {
      releaseFetch = resolve;
    });
    mockListWorkspaceNodes.mockImplementationOnce(() => pending);

    const client = makeClient();
    const loading = getWorkspaceNodeIndex(client, "ws-race");

    invalidateWorkspaceNodeIndex("ws-race");

    releaseFetch({
      data: [{ id: "stale", name: "STALE", nodeType: "Stage", locationName: "RAW" }],
    });
    await loading;

    // Next read triggers a brand-new fetch rather than returning the stale one.
    mockListWorkspaceNodes.mockResolvedValueOnce({
      data: [{ id: "fresh", name: "FRESH", nodeType: "Stage", locationName: "RAW" }],
    });
    const after = await getWorkspaceNodeIndex(client, "ws-race");
    expect(after[0]?.id).toBe("fresh");
  });

  it("setWorkspaceNodeAndInvalidate invalidates both inventory and node-index caches", async () => {
    let changed = false;
    mockListWorkspaceNodes.mockImplementation(async () => ({
      data: [
        {
          id: "n-1",
          name: "A",
          nodeType: changed ? "Dimension" : "Stage",
          locationName: changed ? "EDM" : "RAW",
        },
      ],
    }));

    const client = makeClient();

    // Prime both caches
    const typesBefore = await listWorkspaceNodeTypes(client, { workspaceID: "ws-1" });
    const indexBefore = await getWorkspaceNodeIndex(client, "ws-1");
    expect(typesBefore.counts).toEqual({ Stage: 1 });
    expect(indexBefore[0]?.locationName).toBe("RAW");
    const callsAfterPriming = mockListWorkspaceNodes.mock.calls.length;

    changed = true;
    await setWorkspaceNodeAndInvalidate(client, {
      workspaceID: "ws-1",
      nodeID: "n-1",
      body: { nodeType: "Dimension", locationName: "EDM" },
    });

    // Both caches should be invalidated — fresh fetches pick up the change
    const typesAfter = await listWorkspaceNodeTypes(client, { workspaceID: "ws-1" });
    const indexAfter = await getWorkspaceNodeIndex(client, "ws-1");
    expect(typesAfter.counts).toEqual({ Dimension: 1 });
    expect(indexAfter[0]?.locationName).toBe("EDM");
    expect(mockListWorkspaceNodes.mock.calls.length).toBeGreaterThan(callsAfterPriming);
  });
});
