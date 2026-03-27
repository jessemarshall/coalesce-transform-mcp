import { describe, it, expect, vi } from "vitest";
import { listWorkspaces, getWorkspace } from "../../src/coalesce/api/workspaces.js";
import { CoalesceApiError } from "../../src/client.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({}),
    post: vi.fn().mockResolvedValue({}),
    put: vi.fn().mockResolvedValue({}),
    delete: vi.fn().mockResolvedValue({}),
  };
}

describe("Workspace API", () => {
  it("listWorkspaces calls GET /api/v1/workspaces", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "ws-1", name: "Development" },
        { id: "ws-2", name: "Production" },
      ],
    });

    const result = await listWorkspaces(client as any);

    expect(client.get).toHaveBeenCalledWith("/api/v1/workspaces", {});
    expect(result).toEqual({
      data: [
        { id: "ws-1", name: "Development" },
        { id: "ws-2", name: "Production" },
      ],
    });
  });

  it("listWorkspaces passes pagination params", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    await listWorkspaces(client as any, { limit: 10, orderBy: "name" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/workspaces", {
      limit: 10,
      orderBy: "name",
    });
  });

  it("getWorkspace calls GET /api/v1/workspaces/{workspaceID}", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "ws-1", name: "Development" });

    const result = await getWorkspace(client as any, { workspaceID: "ws-1" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/workspaces/ws-1", {});
    expect(result).toEqual({ id: "ws-1", name: "Development" });
  });

  it("getWorkspace rejects path traversal in workspaceID", async () => {
    const client = createMockClient();

    await expect(
      getWorkspace(client as any, { workspaceID: "../escape" })
    ).rejects.toThrow("workspaceID");
  });

  it("propagates CoalesceApiError from client", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Unauthorized", 401));

    await expect(
      listWorkspaces(client as any)
    ).rejects.toThrow("Unauthorized");
  });
});
