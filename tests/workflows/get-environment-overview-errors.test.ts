import { describe, it, expect, vi } from "vitest";
import {
  getEnvironmentOverview,
} from "../../src/workflows/get-environment-overview.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ ok: true }),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("get-environment-overview error paths", () => {
  it("throws on empty environmentID", async () => {
    const client = createMockClient();
    await expect(
      getEnvironmentOverview(client as any, { environmentID: "" })
    ).rejects.toThrow("Invalid environmentID: must not be empty");
  });

  it("throws on environmentID with path traversal", async () => {
    const client = createMockClient();
    await expect(
      getEnvironmentOverview(client as any, { environmentID: "../../etc" })
    ).rejects.toThrow("Invalid environmentID");
  });

  it("throws on environmentID with URI delimiters", async () => {
    const client = createMockClient();
    await expect(
      getEnvironmentOverview(client as any, { environmentID: "env?id=1" })
    ).rejects.toThrow("Invalid environmentID");
  });

  it("propagates error when environment detail fetch fails", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new Error("403 Forbidden"));

    await expect(
      getEnvironmentOverview(client as any, { environmentID: "env-1" })
    ).rejects.toThrow("403 Forbidden");
  });

  it("propagates error when node pagination fetch fails on first page", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1") {
        return Promise.resolve({ id: "env-1", name: "Prod" });
      }
      if (path.includes("/nodes")) {
        return Promise.reject(new Error("500 Internal Server Error"));
      }
      return Promise.resolve({});
    });

    await expect(
      getEnvironmentOverview(client as any, { environmentID: "env-1" })
    ).rejects.toThrow("500 Internal Server Error");
  });

  it("propagates error when node pagination fails on second page", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/environments/env-1") {
        return Promise.resolve({ id: "env-1", name: "Prod" });
      }
      if (path.includes("/nodes")) {
        if (!params?.startingFrom) {
          return Promise.resolve({
            data: [{ id: "node-1" }],
            next: "cursor-2",
          });
        }
        return Promise.reject(new Error("Connection reset"));
      }
      return Promise.resolve({});
    });

    await expect(
      getEnvironmentOverview(client as any, { environmentID: "env-1" })
    ).rejects.toThrow("Connection reset");
  });

  it("returns empty nodes when response has no data field", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1") {
        return Promise.resolve({ id: "env-1", name: "Prod" });
      }
      if (path.includes("/nodes")) {
        // Response object has 'items' instead of 'data' — parseCollectionPage defaults to empty array
        return Promise.resolve({ items: [{ id: "1" }] });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentOverview(client as any, { environmentID: "env-1" });
    expect(result.nodes).toEqual([]);
  });

  it("throws when nodes response is not an object", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1") {
        return Promise.resolve({ id: "env-1", name: "Prod" });
      }
      if (path.includes("/nodes")) {
        return Promise.resolve("not-an-object");
      }
      return Promise.resolve({});
    });

    await expect(
      getEnvironmentOverview(client as any, { environmentID: "env-1" })
    ).rejects.toThrow("Paginated collection response was not an object");
  });

  it("handles null next cursor gracefully (treated as end of pagination)", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1") {
        return Promise.resolve({ id: "env-1", name: "Prod" });
      }
      if (path.includes("/nodes")) {
        return Promise.resolve({
          data: [{ id: "node-1" }],
          next: null,
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentOverview(client as any, { environmentID: "env-1" });
    expect(result.nodes).toEqual([{ id: "node-1" }]);
  });
});
