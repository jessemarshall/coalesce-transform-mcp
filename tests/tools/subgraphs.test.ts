import { describe, it, expect, vi } from "vitest";
import {
  getWorkspaceSubgraph,
  createWorkspaceSubgraph,
  updateWorkspaceSubgraph,
  deleteWorkspaceSubgraph,
} from "../../src/coalesce/api/subgraphs.js";
import { CoalesceApiError } from "../../src/client.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({}),
    post: vi.fn().mockResolvedValue({}),
    put: vi.fn().mockResolvedValue({}),
    delete: vi.fn().mockResolvedValue({}),
  };
}

describe("Subgraph API", () => {
  it("getWorkspaceSubgraph calls GET /api/v1/workspaces/{workspaceID}/subgraphs/{subgraphID}", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "sg-1", name: "Staging" });

    const result = await getWorkspaceSubgraph(client as any, {
      workspaceID: "ws-1",
      subgraphID: "sg-1",
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/subgraphs/sg-1",
      {}
    );
    expect(result).toEqual({ id: "sg-1", name: "Staging" });
  });

  it("createWorkspaceSubgraph calls POST /api/v1/workspaces/{workspaceID}/subgraphs", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "sg-2" });

    const result = await createWorkspaceSubgraph(client as any, {
      workspaceID: "ws-1",
      name: "ETL Pipeline",
      steps: ["node-1", "node-2"],
    });

    expect(client.post).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/subgraphs",
      { name: "ETL Pipeline", steps: ["node-1", "node-2"] }
    );
    expect(result).toEqual({ id: "sg-2" });
  });

  it("updateWorkspaceSubgraph calls PUT /api/v1/workspaces/{workspaceID}/subgraphs/{subgraphID}", async () => {
    const client = createMockClient();
    client.put.mockResolvedValue({ id: "sg-1", name: "ETL v2" });

    const result = await updateWorkspaceSubgraph(client as any, {
      workspaceID: "ws-1",
      subgraphID: "sg-1",
      name: "ETL v2",
      steps: ["node-1", "node-3"],
    });

    expect(client.put).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/subgraphs/sg-1",
      { name: "ETL v2", steps: ["node-1", "node-3"] }
    );
    expect(result).toEqual({ id: "sg-1", name: "ETL v2" });
  });

  it("deleteWorkspaceSubgraph calls DELETE /api/v1/workspaces/{workspaceID}/subgraphs/{subgraphID}", async () => {
    const client = createMockClient();
    client.delete.mockResolvedValue({});

    await deleteWorkspaceSubgraph(client as any, {
      workspaceID: "ws-1",
      subgraphID: "sg-1",
    });

    expect(client.delete).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/subgraphs/sg-1"
    );
  });

  it("rejects path traversal in workspaceID", async () => {
    const client = createMockClient();

    await expect(
      getWorkspaceSubgraph(client as any, {
        workspaceID: "../escape",
        subgraphID: "sg-1",
      })
    ).rejects.toThrow("workspaceID");
  });

  it("rejects path traversal in subgraphID", async () => {
    const client = createMockClient();

    await expect(
      getWorkspaceSubgraph(client as any, {
        workspaceID: "ws-1",
        subgraphID: "../escape",
      })
    ).rejects.toThrow("subgraphID");
  });

  it("propagates CoalesceApiError from client", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Not found", 404));

    await expect(
      getWorkspaceSubgraph(client as any, {
        workspaceID: "ws-1",
        subgraphID: "bad",
      })
    ).rejects.toThrow("Not found");
  });
});
