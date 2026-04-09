import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  POSTMAN_USER_ROLES_QUERY,
  POSTMAN_USERS_QUERY,
} from "../fixtures/postman-examples.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn(),
    put: vi.fn().mockResolvedValue({ ok: true }),
    delete: vi.fn().mockResolvedValue({ message: "Operation completed successfully" }),
  };
}

describe("User Tools", () => {
  it("registers all 8 user tools without throwing", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    const { defineUserTools } = await import("../../src/mcp/users.js");
    defineUserTools(server, client as any);
    expect(true).toBe(true);
  });

  it("listOrgUsers calls GET /api/v1/users", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "user-1", name: "Alice" }] });

    const { listOrgUsers } = await import("../../src/coalesce/api/users.js");
    const result = await listOrgUsers(client as any, POSTMAN_USERS_QUERY);

    expect(client.get).toHaveBeenCalledWith("/api/v1/users", POSTMAN_USERS_QUERY);
    expect(result).toEqual({ data: [{ id: "user-1", name: "Alice" }] });
  });

  it("getUserRoles calls GET /api/v2/userRoles/{userID} with optional scope filters", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ userID: "user-1", roles: [] });

    const { getUserRoles } = await import("../../src/coalesce/api/users.js");
    const result = await getUserRoles(client as any, {
      userID: "user-1",
      ...POSTMAN_USER_ROLES_QUERY,
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v2/userRoles/user-1",
      POSTMAN_USER_ROLES_QUERY
    );
    expect(result).toEqual({ userID: "user-1", roles: [] });
  });

  it("listUserRoles calls GET /api/v2/userRoles with optional scope filters", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ userID: "user-1" }] });

    const { listUserRoles } = await import("../../src/coalesce/api/users.js");
    const result = await listUserRoles(client as any, POSTMAN_USER_ROLES_QUERY);

    expect(client.get).toHaveBeenCalledWith(
      "/api/v2/userRoles",
      POSTMAN_USER_ROLES_QUERY
    );
    expect(result).toEqual({ data: [{ userID: "user-1" }] });
  });

  it("setOrgRole calls PUT /api/v2/userRoles/{userID}/organizationRole with body", async () => {
    const client = createMockClient();
    const body = { role: "admin" };
    client.put.mockResolvedValue({ ok: true });

    const { setOrgRole } = await import("../../src/coalesce/api/users.js");
    const result = await setOrgRole(client as any, { userID: "user-1", body });

    expect(client.put).toHaveBeenCalledWith(
      "/api/v2/userRoles/user-1/organizationRole",
      body
    );
    expect(result).toEqual({ ok: true });
  });

  it("setProjectRole calls PUT /api/v2/userRoles/{userID}/projects/{projectID} with body", async () => {
    const client = createMockClient();
    const body = { role: "editor" };
    client.put.mockResolvedValue({ ok: true });

    const { setProjectRole } = await import("../../src/coalesce/api/users.js");
    const result = await setProjectRole(client as any, {
      userID: "user-1",
      projectID: "proj-1",
      body,
    });

    expect(client.put).toHaveBeenCalledWith(
      "/api/v2/userRoles/user-1/projects/proj-1",
      body
    );
    expect(result).toEqual({ ok: true });
  });

  it("deleteProjectRole calls DELETE /api/v2/userRoles/{userID}/projects/{projectID}", async () => {
    const client = createMockClient();
    client.delete.mockResolvedValue({ message: "Operation completed successfully" });

    const { deleteProjectRole } = await import("../../src/coalesce/api/users.js");
    const result = await deleteProjectRole(client as any, {
      userID: "user-1",
      projectID: "proj-1",
    });

    expect(client.delete).toHaveBeenCalledWith(
      "/api/v2/userRoles/user-1/projects/proj-1"
    );
    expect(result).toEqual({ message: "Operation completed successfully" });
  });

  it("setEnvRole calls PUT /api/v2/userRoles/{userID}/environments/{environmentID} with body", async () => {
    const client = createMockClient();
    const body = { role: "viewer" };
    client.put.mockResolvedValue({ ok: true });

    const { setEnvRole } = await import("../../src/coalesce/api/users.js");
    const result = await setEnvRole(client as any, {
      userID: "user-1",
      environmentID: "env-1",
      body,
    });

    expect(client.put).toHaveBeenCalledWith(
      "/api/v2/userRoles/user-1/environments/env-1",
      body
    );
    expect(result).toEqual({ ok: true });
  });

  it("deleteEnvRole calls DELETE /api/v2/userRoles/{userID}/environments/{environmentID}", async () => {
    const client = createMockClient();
    client.delete.mockResolvedValue({ message: "Operation completed successfully" });

    const { deleteEnvRole } = await import("../../src/coalesce/api/users.js");
    const result = await deleteEnvRole(client as any, {
      userID: "user-1",
      environmentID: "env-1",
    });

    expect(client.delete).toHaveBeenCalledWith(
      "/api/v2/userRoles/user-1/environments/env-1"
    );
    expect(result).toEqual({ message: "Operation completed successfully" });
  });
});
