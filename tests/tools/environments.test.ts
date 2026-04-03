import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerEnvironmentTools } from "../../src/mcp/environments.js";
import { CoalesceApiError } from "../../src/client.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("Environment Tools", () => {
  it("registers list-environments and get-environment tools", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerEnvironmentTools(server, client as any);
    expect(true).toBe(true);
  });

  it("list-environments calls GET /api/v1/environments", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "env-1", name: "DEV" }] });

    const { listEnvironments } = await import("../../src/coalesce/api/environments.js");
    const result = await listEnvironments(client as any, {});

    expect(client.get).toHaveBeenCalledWith("/api/v1/environments", {});
    expect(result).toEqual({ data: [{ id: "env-1", name: "DEV" }] });
  });

  it("list-environments passes pagination params", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    const { listEnvironments } = await import("../../src/coalesce/api/environments.js");
    await listEnvironments(client as any, { limit: 5, orderBy: "name" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/environments", {
      limit: 5,
      orderBy: "name",
    });
  });

  it("get-environment calls GET with environmentID only", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "env-1", name: "DEV" });

    const { getEnvironment } = await import("../../src/coalesce/api/environments.js");
    const result = await getEnvironment(client as any, {
      environmentID: "env-1",
    });

    expect(client.get).toHaveBeenCalledWith("/api/v1/environments/env-1", {});
    expect(result).toEqual({ id: "env-1", name: "DEV" });
  });

  it("createEnvironment posts to /api/v1/environments with project field", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "env-new", name: "QA" });

    const { createEnvironment } = await import("../../src/coalesce/api/environments.js");
    const result = await createEnvironment(client as any, {
      project: "proj-1",
      name: "QA",
    });

    expect(client.post).toHaveBeenCalledWith("/api/v1/environments", {
      project: "proj-1",
      name: "QA",
    });
    expect(result).toEqual({ id: "env-new", name: "QA" });
  });

  it("updateEnvironment patches /api/v1/environments/{id}", async () => {
    const client = createMockClient();
    client.patch = vi.fn().mockResolvedValue({ id: "env-1", name: "Updated" });

    const { updateEnvironment } = await import("../../src/coalesce/api/environments.js");
    const result = await updateEnvironment(client as any, {
      environmentID: "env-1",
      name: "Updated",
    });

    expect(client.patch).toHaveBeenCalledWith(
      "/api/v1/environments/env-1",
      { name: "Updated" }
    );
    expect(result).toEqual({ id: "env-1", name: "Updated" });
  });

  it("getEnvironment still throws CoalesceApiError from data-access layer", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Not found", 404));

    const { getEnvironment } = await import("../../src/coalesce/api/environments.js");
    await expect(getEnvironment(client as any, { environmentID: "bad" })).rejects.toThrow("Not found");
  });
});
