import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  getEnvironmentOverview,
  registerGetEnvironmentOverview,
} from "../../src/workflows/get-environment-overview.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ ok: true }),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("get-environment-overview workflow", () => {
  it("registers without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerGetEnvironmentOverview(server, client as any);
    expect(true).toBe(true);
  });

  it("returns all nodes from a single page", async () => {
    const client = createMockClient();
    const envData = { id: "env-1", name: "Production" };
    const nodesData = { data: [{ id: "node-1", name: "STG_ORDERS" }] };

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1") return Promise.resolve(envData);
      if (path === "/api/v1/environments/env-1/nodes") return Promise.resolve(nodesData);
      return Promise.resolve({});
    });

    const result = await getEnvironmentOverview(client as any, { environmentID: "env-1" });

    expect(result.environment).toEqual(envData);
    expect(result.nodes).toEqual([{ id: "node-1", name: "STG_ORDERS" }]);
  });

  it("auto-paginates through multiple pages", async () => {
    const client = createMockClient();
    const envData = { id: "env-1", name: "Production" };

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/environments/env-1") return Promise.resolve(envData);
      if (path === "/api/v1/environments/env-1/nodes") {
        if (!params || !params.startingFrom) {
          // First page
          return Promise.resolve({
            data: [{ id: "node-1" }, { id: "node-2" }],
            next: "cursor-page-2",
          });
        }
        if (params.startingFrom === "cursor-page-2") {
          // Second page
          return Promise.resolve({
            data: [{ id: "node-3" }, { id: "node-4" }],
            next: "cursor-page-3",
          });
        }
        if (params.startingFrom === "cursor-page-3") {
          // Last page — no next
          return Promise.resolve({
            data: [{ id: "node-5" }],
          });
        }
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentOverview(client as any, { environmentID: "env-1" });

    expect(result.nodes).toEqual([
      { id: "node-1" },
      { id: "node-2" },
      { id: "node-3" },
      { id: "node-4" },
      { id: "node-5" },
    ]);
    expect(client.get).toHaveBeenNthCalledWith(2, "/api/v1/environments/env-1/nodes", {
      limit: 250,
      orderBy: "id",
    });
    expect(client.get).toHaveBeenNthCalledWith(3, "/api/v1/environments/env-1/nodes", {
      limit: 250,
      orderBy: "id",
      startingFrom: "cursor-page-2",
    });
    expect(client.get).toHaveBeenNthCalledWith(4, "/api/v1/environments/env-1/nodes", {
      limit: 250,
      orderBy: "id",
      startingFrom: "cursor-page-3",
    });
    expect(client.get).toHaveBeenCalledTimes(4);
  });

  it("handles empty node list", async () => {
    const client = createMockClient();
    const envData = { id: "env-1", name: "Empty" };

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1") return Promise.resolve(envData);
      if (path === "/api/v1/environments/env-1/nodes") return Promise.resolve({ data: [] });
      return Promise.resolve({});
    });

    const result = await getEnvironmentOverview(client as any, { environmentID: "env-1" });

    expect(result.nodes).toEqual([]);
  });

  it("throws when environment node pagination repeats a cursor", async () => {
    const client = createMockClient();
    const envData = { id: "env-1", name: "Production" };

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/environments/env-1") return Promise.resolve(envData);
      if (path === "/api/v1/environments/env-1/nodes") {
        if (!params?.startingFrom) {
          return Promise.resolve({
            data: [{ id: "node-1" }],
            next: "cursor-page-2",
          });
        }
        if (params.startingFrom === "cursor-page-2") {
          return Promise.resolve({
            data: [{ id: "node-2" }],
            next: "cursor-page-2",
          });
        }
      }
      return Promise.resolve({});
    });

    await expect(
      getEnvironmentOverview(client as any, { environmentID: "env-1" })
    ).rejects.toThrow("Pagination repeated cursor cursor-page-2");
  });

  it("throws when node count exceeds the in-memory safety cap", async () => {
    const client = createMockClient();
    const envData = { id: "env-1", name: "Huge" };
    const hugeNodeList = Array.from({ length: 251 }, (_, i) => ({
      id: `node-${i}`,
      name: `NODE_${i}`,
    }));

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1") return Promise.resolve(envData);
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: hugeNodeList });
      }
      return Promise.resolve({});
    });

    await expect(
      getEnvironmentOverview(client as any, { environmentID: "env-1" })
    ).rejects.toThrow(/exceeded 250 item safety limit/);
  });
});
