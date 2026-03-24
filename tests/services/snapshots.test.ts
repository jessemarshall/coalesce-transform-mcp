import { describe, it, expect, vi } from "vitest";
import {
  fetchAllWorkspaceNodes,
} from "../../src/services/cache/snapshots.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

describe("fetchAllPaginatedToMemory safety cap", () => {
  it("throws when item count exceeds 250", async () => {
    const client = createMockClient();
    const page1Items = Array.from({ length: 200 }, (_, i) => ({
      id: `node-${i}`,
      name: `NODE_${i}`,
      nodeType: "Stage",
    }));
    const page2Items = Array.from({ length: 100 }, (_, i) => ({
      id: `node-${200 + i}`,
      name: `NODE_${200 + i}`,
      nodeType: "Stage",
    }));

    client.get.mockImplementation((_path: string, params?: Record<string, unknown>) => {
      if (!params?.startingFrom) {
        return Promise.resolve({ data: page1Items, next: "cursor-2" });
      }
      return Promise.resolve({ data: page2Items });
    });

    await expect(
      fetchAllWorkspaceNodes(client as any, { workspaceID: "ws-1", detail: false })
    ).rejects.toThrow(
      /exceeded 250 item safety limit/
    );
  });

  it("succeeds when item count is within 250", async () => {
    const client = createMockClient();
    const items = Array.from({ length: 250 }, (_, i) => ({
      id: `node-${i}`,
      name: `NODE_${i}`,
      nodeType: "Stage",
    }));

    client.get.mockResolvedValue({ data: items });

    const result = await fetchAllWorkspaceNodes(client as any, {
      workspaceID: "ws-1",
      detail: false,
    });
    expect(result.items).toHaveLength(250);
  });
});
