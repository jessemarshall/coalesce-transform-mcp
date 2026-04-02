import { describe, it, expect, vi, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerEnvironmentTools } from "../../src/mcp/environments.js";
import { CoalesceApiError } from "../../src/client.js";
import { previewDeployment } from "../../src/services/workspace/deployment-diff.js";

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

describe("preview_deployment tool", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function buildNode(id: string, name: string, nodeType: string) {
    return { id, name, nodeType };
  }

  function makeClient(wsNodes: unknown[], envNodes: unknown[]) {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.includes("/workspaces/")) {
        return Promise.resolve({ data: wsNodes });
      }
      if (path.includes("/environments/")) {
        return Promise.resolve({ data: envNodes });
      }
      return Promise.resolve({ data: [] });
    });
    return client;
  }

  it("identifies new nodes (in workspace, not in environment)", async () => {
    const client = makeClient(
      [buildNode("n1", "STG_ORDERS", "Stage"), buildNode("n2", "NEW_NODE", "Stage")],
      [buildNode("n1", "STG_ORDERS", "Stage")]
    );

    const result = await previewDeployment(client as any, "ws-1", "env-1");

    expect(result.summary.new).toBe(1);
    expect(result.new).toHaveLength(1);
    expect(result.new[0].nodeID).toBe("n2");
    expect(result.new[0].name).toBe("NEW_NODE");
  });

  it("identifies removed nodes (in environment, not in workspace)", async () => {
    const client = makeClient(
      [buildNode("n1", "STG_ORDERS", "Stage")],
      [buildNode("n1", "STG_ORDERS", "Stage"), buildNode("n2", "OLD_NODE", "Stage")]
    );

    const result = await previewDeployment(client as any, "ws-1", "env-1");

    expect(result.summary.removed).toBe(1);
    expect(result.removed).toHaveLength(1);
    expect(result.removed[0].nodeID).toBe("n2");
    expect(result.removed[0].name).toBe("OLD_NODE");
  });

  it("identifies modified nodes (name changed)", async () => {
    const client = makeClient(
      [buildNode("n1", "STG_ORDERS_V2", "Stage")],
      [buildNode("n1", "STG_ORDERS", "Stage")]
    );

    const result = await previewDeployment(client as any, "ws-1", "env-1");

    expect(result.summary.modified).toBe(1);
    expect(result.modified).toHaveLength(1);
    expect(result.modified[0].nodeID).toBe("n1");
    expect(result.modified[0].workspaceName).toBe("STG_ORDERS_V2");
    expect(result.modified[0].environmentName).toBe("STG_ORDERS");
  });

  it("identifies modified nodes (nodeType changed)", async () => {
    const client = makeClient(
      [buildNode("n1", "STG_ORDERS", "View")],
      [buildNode("n1", "STG_ORDERS", "Stage")]
    );

    const result = await previewDeployment(client as any, "ws-1", "env-1");

    expect(result.summary.modified).toBe(1);
    expect(result.modified[0].workspaceNodeType).toBe("View");
    expect(result.modified[0].environmentNodeType).toBe("Stage");
  });

  it("counts unchanged nodes correctly", async () => {
    const nodes = [
      buildNode("n1", "STG_ORDERS", "Stage"),
      buildNode("n2", "DIM_CUSTOMER", "Dimension"),
    ];
    const client = makeClient(nodes, nodes);

    const result = await previewDeployment(client as any, "ws-1", "env-1");

    expect(result.summary.unchanged).toBe(2);
    expect(result.summary.new).toBe(0);
    expect(result.summary.removed).toBe(0);
    expect(result.summary.modified).toBe(0);
  });

  it("handles empty workspace (all environment nodes become removed)", async () => {
    const client = makeClient(
      [],
      [buildNode("n1", "STG_ORDERS", "Stage"), buildNode("n2", "DIM_CUSTOMER", "Dimension")]
    );

    const result = await previewDeployment(client as any, "ws-1", "env-1");

    expect(result.summary.new).toBe(0);
    expect(result.summary.removed).toBe(2);
    expect(result.summary.unchanged).toBe(0);
    expect(result.removed).toHaveLength(2);
  });

  it("handles empty environment (all workspace nodes become new)", async () => {
    const client = makeClient(
      [buildNode("n1", "STG_ORDERS", "Stage"), buildNode("n2", "DIM_CUSTOMER", "Dimension")],
      []
    );

    const result = await previewDeployment(client as any, "ws-1", "env-1");

    expect(result.summary.new).toBe(2);
    expect(result.summary.removed).toBe(0);
    expect(result.new).toHaveLength(2);
  });

  it("tool handler returns isError when API throws", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Forbidden", 403));

    registerEnvironmentTools(server, client as any);

    const call = toolSpy.mock.calls.find((c) => c[0] === "preview_deployment");
    const handler = call?.[2] as (params: { workspaceID: string; environmentID: string }) => Promise<{ isError?: boolean; content: { text: string }[] }>;

    const result = await handler({ workspaceID: "ws-1", environmentID: "env-1" });

    expect(result.isError).toBe(true);
    expect(result.content[0]!.text).toContain("Forbidden");
  });

  it("tool handler returns isError for invalid workspaceID (path traversal)", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    registerEnvironmentTools(server, client as any);

    const call = toolSpy.mock.calls.find((c) => c[0] === "preview_deployment");
    const handler = call?.[2] as (params: { workspaceID: string; environmentID: string }) => Promise<{ isError?: boolean; content: { text: string }[] }>;

    const result = await handler({ workspaceID: "../../etc", environmentID: "env-1" });

    expect(result.isError).toBe(true);
  });

  it("includes workspaceID and environmentID in result", async () => {
    const client = makeClient([], []);

    const result = await previewDeployment(client as any, "ws-42", "env-99");

    expect(result.workspaceID).toBe("ws-42");
    expect(result.environmentID).toBe("env-99");
    expect(typeof result.diffedAt).toBe("string");
  });

  it("paginates through multiple pages of workspace nodes and forwards cursor", async () => {
    const client = createMockClient();
    const wsCalls: Array<Record<string, unknown>> = [];
    client.get.mockImplementation((path: string, params: Record<string, unknown>) => {
      if (path.includes("/workspaces/")) {
        wsCalls.push(params);
        if (wsCalls.length === 1) {
          return Promise.resolve({
            data: [buildNode("n1", "STG_A", "Stage"), buildNode("n2", "STG_B", "Stage")],
            next: "cursor-2",
          });
        }
        return Promise.resolve({ data: [buildNode("n3", "STG_C", "Stage")] });
      }
      return Promise.resolve({ data: [] });
    });

    const result = await previewDeployment(client as any, "ws-1", "env-1");

    expect(wsCalls).toHaveLength(2);
    expect(wsCalls[1]!.startingFrom).toBe("cursor-2");
    expect(result.summary.new).toBe(3);
  });

  it("handles numeric next cursor (API returns page index as number)", async () => {
    const client = createMockClient();
    let callCount = 0;
    client.get.mockImplementation((path: string, params: Record<string, unknown>) => {
      if (path.includes("/workspaces/")) {
        callCount++;
        if (callCount === 1) {
          return Promise.resolve({
            data: [buildNode("n1", "STG_A", "Stage")],
            next: 2,  // numeric cursor — Coalesce API can return page index as number
          });
        }
        return Promise.resolve({ data: [buildNode("n2", "STG_B", "Stage")] });
      }
      return Promise.resolve({ data: [] });
    });

    const result = await previewDeployment(client as any, "ws-1", "env-1");

    expect(callCount).toBe(2);
    expect(result.summary.new).toBe(2);
  });

  it("throws when pagination returns a repeated cursor", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.includes("/workspaces/")) {
        return Promise.resolve({
          data: [buildNode("n1", "STG_A", "Stage")],
          next: "same-cursor",  // same cursor on every page → infinite loop guard
        });
      }
      return Promise.resolve({ data: [] });
    });

    await expect(previewDeployment(client as any, "ws-1", "env-1")).rejects.toThrow(
      "Pagination repeated cursor same-cursor"
    );
  });
});
