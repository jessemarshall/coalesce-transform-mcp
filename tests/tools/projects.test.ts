import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  listProjects,
  getProject,
  createProject,
  updateProject,
  deleteProject,
} from "../../src/coalesce/api/projects.js";
import { registerProjectTools } from "../../src/mcp/projects.js";
import { POSTMAN_PROJECTS_QUERY } from "../fixtures/postman-examples.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ ok: true }),
    put: vi.fn().mockResolvedValue({ ok: true }),
    patch: vi.fn().mockResolvedValue({ ok: true }),
    delete: vi.fn().mockResolvedValue({ message: "Operation completed successfully" }),
  };
}

describe("Project Tools", () => {
  it("registers all 5 project tools without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerProjectTools(server, client as any);
    expect(true).toBe(true);
  });

  it("listProjects calls GET /api/v1/projects", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "proj-1" }] });

    const result = await listProjects(client as any);

    expect(client.get).toHaveBeenCalledWith("/api/v1/projects", {});
    expect(result).toEqual({ data: [{ id: "proj-1" }] });
  });

  it("listProjects passes includeWorkspaces and includeJobs query params", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "proj-1", workspaces: [] }] });

    const result = await listProjects(client as any, POSTMAN_PROJECTS_QUERY);

    expect(client.get).toHaveBeenCalledWith("/api/v1/projects", POSTMAN_PROJECTS_QUERY);
    expect(result).toEqual({ data: [{ id: "proj-1", workspaces: [] }] });
  });

  it("getProject calls GET /api/v1/projects/{projectID}", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "proj-1", name: "My Project" });

    const result = await getProject(client as any, { projectID: "proj-1" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/projects/proj-1", {});
    expect(result).toEqual({ id: "proj-1", name: "My Project" });
  });

  it("getProject passes includeWorkspaces and includeJobs query params", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "proj-1", workspaces: [{ id: "ws-1" }] });

    const result = await getProject(client as any, {
      projectID: "proj-1",
      includeWorkspaces: POSTMAN_PROJECTS_QUERY.includeWorkspaces,
    });

    expect(client.get).toHaveBeenCalledWith("/api/v1/projects/proj-1", { includeWorkspaces: true });
    expect(result).toEqual({ id: "proj-1", workspaces: [{ id: "ws-1" }] });
  });

  it("createProject calls POST /api/v1/projects with body", async () => {
    const client = createMockClient();
    const body = { name: "New Project" };
    client.post.mockResolvedValue({ id: "proj-2", name: "New Project" });

    const result = await createProject(client as any, { body });

    expect(client.post).toHaveBeenCalledWith("/api/v1/projects", body);
    expect(result).toEqual({ id: "proj-2", name: "New Project" });
  });

  it("updateProject calls PATCH /api/v1/projects/{projectID} with body", async () => {
    const client = createMockClient();
    const body = { name: "Updated Project" };
    client.patch.mockResolvedValue({ id: "proj-1", name: "Updated Project" });

    const result = await updateProject(client as any, { projectID: "proj-1", body });

    expect(client.patch).toHaveBeenCalledWith("/api/v1/projects/proj-1", {}, body);
    expect(result).toEqual({ id: "proj-1", name: "Updated Project" });
  });

  it("deleteProject calls DELETE /api/v1/projects/{projectID}", async () => {
    const client = createMockClient();
    client.delete.mockResolvedValue({ message: "Operation completed successfully" });

    const result = await deleteProject(client as any, { projectID: "proj-1" });

    expect(client.delete).toHaveBeenCalledWith("/api/v1/projects/proj-1");
    expect(result).toEqual({ message: "Operation completed successfully" });
  });
});
