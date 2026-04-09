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
  it("listWorkspaces calls projects endpoint with includeWorkspaces and flattens workspaces", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        {
          id: "proj-1",
          name: "Project 1",
          workspaces: [
            { id: "ws-1", name: "Development" },
            { id: "ws-2", name: "Production" },
          ],
        },
        {
          id: "proj-2",
          name: "Project 2",
          workspaces: [
            { id: "ws-3", name: "Staging" },
          ],
        },
      ],
    });

    const result = await listWorkspaces(client as any);

    expect(client.get).toHaveBeenCalledWith("/api/v1/projects", { includeWorkspaces: true });
    expect(result).toEqual({
      data: [
        { id: "ws-1", name: "Development", projectID: "proj-1" },
        { id: "ws-2", name: "Production", projectID: "proj-1" },
        { id: "ws-3", name: "Staging", projectID: "proj-2" },
      ],
    });
  });

  it("listWorkspaces handles projects with no workspaces", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "proj-1", name: "Empty Project" },
      ],
    });

    const result = await listWorkspaces(client as any);

    expect(result).toEqual({ data: [] });
  });

  it("listWorkspaces handles empty projects response", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    const result = await listWorkspaces(client as any);

    expect(result).toEqual({ data: [] });
  });

  it("listWorkspaces throws on unexpected response shape", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ items: [] });

    await expect(listWorkspaces(client as any)).rejects.toThrow(CoalesceApiError);
    await expect(listWorkspaces(client as any)).rejects.toThrow("missing or non-array");
  });

  it("listWorkspaces ignores non-array workspaces on a project", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        { id: "proj-1", workspaces: "not-an-array" },
        { id: "proj-2", workspaces: [{ id: "ws-1", name: "Valid" }] },
      ],
    });

    const result = await listWorkspaces(client as any);

    expect(result).toEqual({
      data: [{ id: "ws-1", name: "Valid", projectID: "proj-2" }],
    });
  });

  it("getWorkspace finds workspace by ID across projects", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        {
          id: "proj-1",
          workspaces: [
            { id: "ws-1", name: "Development" },
          ],
        },
        {
          id: "proj-2",
          workspaces: [
            { id: "ws-2", name: "Production" },
          ],
        },
      ],
    });

    const result = await getWorkspace(client as any, { workspaceID: "ws-2" });

    expect(result).toEqual({ id: "ws-2", name: "Production", projectID: "proj-2" });
  });

  it("getWorkspace throws CoalesceApiError with 404 when workspace not found", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      data: [
        {
          id: "proj-1",
          workspaces: [
            { id: "ws-1", name: "Development" },
          ],
        },
      ],
    });

    await expect(
      getWorkspace(client as any, { workspaceID: "ws-nonexistent" })
    ).rejects.toThrow(CoalesceApiError);
    await expect(
      getWorkspace(client as any, { workspaceID: "ws-nonexistent" })
    ).rejects.toThrow("Workspace not found: ws-nonexistent");
  });

  it("propagates API errors from the projects endpoint", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Unauthorized", 401));

    await expect(
      listWorkspaces(client as any)
    ).rejects.toThrow("Unauthorized");
  });
});
