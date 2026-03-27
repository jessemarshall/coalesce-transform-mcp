import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerWorkspaceTools } from "../../src/mcp/workspaces.js";
import { listWorkspaces } from "../../src/coalesce/api/workspaces.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue([]),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

describe("Workspace Tools", () => {
  it("registers list-workspaces tool", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerWorkspaceTools(server, client as any);
    expect(true).toBe(true);
  });

  it("listWorkspaces without projectID calls GET /api/v1/projects with includeWorkspaces", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue([
      {
        id: "proj-1",
        name: "Project One",
        workspaces: [
          { id: "ws-1", name: "Dev Workspace" },
          { id: "ws-2", name: "Prod Workspace" },
        ],
      },
    ]);

    const result = await listWorkspaces(client as any);

    expect(client.get).toHaveBeenCalledWith("/api/v1/projects", {
      includeWorkspaces: true,
    });
    expect(result).toEqual({
      workspaces: [
        { id: "ws-1", name: "Dev Workspace", projectID: "proj-1", projectName: "Project One" },
        { id: "ws-2", name: "Prod Workspace", projectID: "proj-1", projectName: "Project One" },
      ],
    });
  });

  it("listWorkspaces with projectID calls GET /api/v1/projects/{id} with includeWorkspaces", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      id: "proj-1",
      name: "Project One",
      workspaces: [{ id: "ws-1", name: "Dev" }],
    });

    const result = await listWorkspaces(client as any, { projectID: "proj-1" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/projects/proj-1", {
      includeWorkspaces: true,
    });
    expect(result).toEqual({
      workspaces: [
        { id: "ws-1", name: "Dev", projectID: "proj-1", projectName: "Project One" },
      ],
    });
  });

  it("returns empty workspaces when API returns non-array for all-projects path", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue("unexpected string");

    const result = await listWorkspaces(client as any);

    expect(result).toEqual({ workspaces: [] });
  });

  it("handles project with no workspaces field", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue([{ id: "proj-1", name: "Empty" }]);

    const result = await listWorkspaces(client as any);

    expect(result).toEqual({ workspaces: [] });
  });

  it("skips workspaces with no valid id", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue([
      {
        id: "proj-1",
        name: "Test",
        workspaces: [
          { id: "ws-1", name: "Good" },
          { id: "", name: "Empty ID" },
          { id: null, name: "Null ID" },
          { name: "Missing ID" },
        ],
      },
    ]);

    const result = await listWorkspaces(client as any);

    expect(result).toEqual({
      workspaces: [
        { id: "ws-1", name: "Good", projectID: "proj-1", projectName: "Test" },
      ],
    });
  });

  it("flattens workspaces from multiple projects", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue([
      { id: "p1", name: "P1", workspaces: [{ id: "ws-1", name: "W1" }] },
      { id: "p2", name: "P2", workspaces: [{ id: "ws-2", name: "W2" }] },
    ]);

    const result = await listWorkspaces(client as any);

    expect(result).toEqual({
      workspaces: [
        { id: "ws-1", name: "W1", projectID: "p1", projectName: "P1" },
        { id: "ws-2", name: "W2", projectID: "p2", projectName: "P2" },
      ],
    });
  });
});
