import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../src/services/config/intelligent.js", () => ({
  completeNodeConfiguration: vi.fn(async () => ({})),
}));

import {
  createWorkspaceNodeFromScratch,
  createWorkspaceNodeFromPredecessor,
} from "../../src/services/workspace/mutations.js";
import { CoalesceApiError } from "../../src/client.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("createWorkspaceNodeFromScratch — pre-flight duplicate detection", () => {
  it("returns the existing node with preExisting:true when a name-location match already exists", async () => {
    const client = createMockClient();
    const existing = {
      id: "n-existing",
      name: "RAMESH_COCO_TEST",
      locationName: "EDM",
      nodeType: "Stage",
    };

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-305/nodes") {
        return Promise.resolve({ data: [existing] });
      }
      if (path === "/api/v1/workspaces/ws-305/nodes/n-existing") {
        return Promise.resolve(existing);
      }
      return Promise.resolve({ data: [] });
    });

    const result = await createWorkspaceNodeFromScratch(client as any, {
      workspaceID: "ws-305",
      nodeType: "Stage",
      completionLevel: "created",
      name: "RAMESH_COCO_TEST",
      changes: { locationName: "EDM" },
    });

    expect(client.post).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      preExisting: true,
      node: { id: "n-existing" },
      nextSteps: expect.any(Array),
    });
    expect((result as { warning: string }).warning).toMatch(/already exists/i);
    expect((result as { nextSteps: string[] }).nextSteps.join(" ")).toMatch(
      /Verify the existing node/i
    );
  });

  it("creates normally when no duplicate is present", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-305/nodes") {
        return Promise.resolve({ data: [] });
      }
      if (path.startsWith("/api/v1/workspaces/ws-305/nodes/")) {
        return Promise.resolve({ id: "new-node", nodeType: "Stage", config: {} });
      }
      return Promise.resolve({ data: [] });
    });
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    await createWorkspaceNodeFromScratch(client as any, {
      workspaceID: "ws-305",
      nodeType: "Stage",
      completionLevel: "created",
    });

    expect(client.post).toHaveBeenCalled();
  });
});

describe("createWorkspaceNodeFromPredecessor — pre-flight duplicate detection", () => {
  it("short-circuits when name conflicts with an existing node", async () => {
    const client = createMockClient();
    const existing = {
      id: "n-existing",
      name: "DIM_CUSTOMER",
      locationName: "EDM",
      nodeType: "Dimension",
    };

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-305/nodes") {
        return Promise.resolve({ data: [existing] });
      }
      if (path === "/api/v1/workspaces/ws-305/nodes/n-existing") {
        return Promise.resolve(existing);
      }
      return Promise.resolve({ data: [] });
    });

    const result = await createWorkspaceNodeFromPredecessor(client as any, {
      workspaceID: "ws-305",
      nodeType: "Dimension",
      predecessorNodeIDs: ["src-1"],
      changes: { name: "DIM_CUSTOMER", locationName: "EDM" },
    });

    expect(client.post).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      preExisting: true,
      node: { id: "n-existing" },
    });
  });

  it("does NOT return preExisting when the existing node has a different nodeType — lets creation proceed", async () => {
    const client = createMockClient();
    const existing = {
      id: "n-existing",
      name: "CUSTOMER",
      locationName: "EDM",
      nodeType: "Stage",
    };

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-305/nodes") {
        return Promise.resolve({ data: [existing] });
      }
      if (path === "/api/v1/workspaces/ws-305/nodes/n-new") {
        return Promise.resolve({
          id: "n-new",
          nodeType: "Dimension",
          config: {},
          metadata: { sourceMapping: [{ dependencies: [{ nodeName: "src-1" }], list: [] }], columns: [{ name: "ID" }] },
        });
      }
      if (path === "/api/v1/workspaces/ws-305/nodes/src-1") {
        return Promise.resolve({ id: "src-1", name: "SRC", nodeType: "Source" });
      }
      return Promise.resolve({ data: [] });
    });
    client.post.mockResolvedValue({ id: "n-new", nodeType: "Dimension" });

    const result = await createWorkspaceNodeFromPredecessor(client as any, {
      workspaceID: "ws-305",
      nodeType: "Dimension",
      predecessorNodeIDs: ["src-1"],
      changes: { name: "CUSTOMER", locationName: "EDM" },
    });

    // Creation should have proceeded — not short-circuited as preExisting.
    expect(client.post).toHaveBeenCalled();
    expect((result as { preExisting?: boolean }).preExisting).toBeUndefined();
  });
});

describe("creation — unique-name recovery on race conditions", () => {
  it("falls through to recovery when the server returns 400 after the pre-flight passes", async () => {
    const client = createMockClient();
    // Pre-flight sees an empty index, then right before the POST another caller
    // creates the node. The POST (no name) succeeds, but the PUT with the name
    // returns the unique-name 400.
    let listCallCount = 0;
    const placedNode = {
      id: "n-placed",
      name: "RACE_NODE",
      locationName: "EDM",
      nodeType: "Stage",
    };

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-305/nodes") {
        listCallCount += 1;
        // First call (pre-flight): empty. Second call (recovery): populated.
        return listCallCount === 1
          ? Promise.resolve({ data: [] })
          : Promise.resolve({ data: [placedNode] });
      }
      if (path === "/api/v1/workspaces/ws-305/nodes/n-placed") {
        return Promise.resolve(placedNode);
      }
      if (path === "/api/v1/workspaces/ws-305/nodes/new-node") {
        return Promise.resolve({ id: "new-node", nodeType: "Stage", config: {} });
      }
      return Promise.resolve({ data: [] });
    });
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });
    client.put.mockRejectedValue(
      new CoalesceApiError(
        "Nodes assigned to the same Storage Location must have unique names.",
        400
      )
    );

    const result = await createWorkspaceNodeFromScratch(client as any, {
      workspaceID: "ws-305",
      nodeType: "Stage",
      completionLevel: "created",
      name: "RACE_NODE",
      changes: { locationName: "EDM" },
    });

    expect(result).toMatchObject({
      preExisting: true,
      node: { id: "n-placed" },
    });
    expect((result as { warning: string }).warning).toMatch(/timed out at the client/);
  });

  it("re-throws the original 400 when recovery cannot locate a matching node", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-305/nodes") {
        return Promise.resolve({ data: [] });
      }
      if (path === "/api/v1/workspaces/ws-305/nodes/new-node") {
        return Promise.resolve({ id: "new-node", nodeType: "Stage", config: {} });
      }
      return Promise.resolve({ data: [] });
    });
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });
    const original = new CoalesceApiError(
      "Nodes assigned to the same Storage Location must have unique names.",
      400
    );
    client.put.mockRejectedValue(original);

    await expect(
      createWorkspaceNodeFromScratch(client as any, {
        workspaceID: "ws-305",
        nodeType: "Stage",
        completionLevel: "created",
        name: "PHANTOM_NODE",
        changes: { locationName: "EDM" },
      })
    ).rejects.toBe(original);
  });
});
