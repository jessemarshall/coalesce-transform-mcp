import { describe, it, expect, vi } from "vitest";
import { updateWorkspaceNode } from "../../src/services/workspace/mutations.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ id: "new-node" }),
    put: vi.fn().mockResolvedValue({ id: "updated-node" }),
    delete: vi.fn(),
  };
}

describe("NodeType and Materialization Validation", () => {
  it("rejects View nodeType with table materializationType", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "TEST_NODE",
      nodeType: "View",
      materializationType: "view",
      metadata: {
        columns: [],
        sourceMapping: [],
      },
      config: {},
    });

    await expect(
      updateWorkspaceNode(client as any, {
        workspaceID: "ws-1",
        nodeID: "node-1",
        changes: {
          materializationType: "table",
        },
      })
    ).rejects.toThrow(
      /Invalid configuration: nodeType "View" cannot use materializationType "table"/
    );
  });

  it("allows View nodeType with view materializationType", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "TEST_NODE",
      nodeType: "View",
      materializationType: "view",
      metadata: {
        columns: [],
        sourceMapping: [],
      },
      config: {},
    });

    client.put.mockResolvedValue({
      id: "node-1",
      nodeType: "View",
      materializationType: "view",
    });

    await expect(
      updateWorkspaceNode(client as any, {
        workspaceID: "ws-1",
        nodeID: "node-1",
        changes: {
          description: "Updated description",
        },
      })
    ).resolves.toBeDefined();
  });

  it("allows Dimension nodeType with table materializationType", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "DIM_CUSTOMER",
      nodeType: "Dimension",
      materializationType: "view",
      metadata: {
        columns: [],
        sourceMapping: [],
      },
      config: {},
    });

    client.put.mockResolvedValue({
      id: "node-1",
      nodeType: "Dimension",
      materializationType: "table",
    });

    await expect(
      updateWorkspaceNode(client as any, {
        workspaceID: "ws-1",
        nodeID: "node-1",
        changes: {
          materializationType: "table",
        },
      })
    ).resolves.toBeDefined();
  });

  it("allows Fact nodeType with table materializationType", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "FACT_SALES",
      nodeType: "Fact",
      materializationType: "view",
      metadata: {
        columns: [],
        sourceMapping: [],
      },
      config: {},
    });

    client.put.mockResolvedValue({
      id: "node-1",
      nodeType: "Fact",
      materializationType: "table",
    });

    await expect(
      updateWorkspaceNode(client as any, {
        workspaceID: "ws-1",
        nodeID: "node-1",
        changes: {
          materializationType: "table",
        },
      })
    ).resolves.toBeDefined();
  });

  it("allows Stage nodeType with table materializationType", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "STG_CUSTOMER",
      nodeType: "Stage",
      materializationType: "view",
      metadata: {
        columns: [],
        sourceMapping: [],
      },
      config: {},
    });

    client.put.mockResolvedValue({
      id: "node-1",
      nodeType: "Stage",
      materializationType: "table",
    });

    await expect(
      updateWorkspaceNode(client as any, {
        workspaceID: "ws-1",
        nodeID: "node-1",
        changes: {
          materializationType: "table",
        },
      })
    ).resolves.toBeDefined();
  });

  it("provides actionable error message with suggestions", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "TEST_NODE",
      nodeType: "View",
      materializationType: "view",
      metadata: {
        columns: [],
        sourceMapping: [],
      },
      config: {},
    });

    try {
      await updateWorkspaceNode(client as any, {
        workspaceID: "ws-1",
        nodeID: "node-1",
        changes: {
          materializationType: "table",
        },
      });
      expect.fail("Should have thrown an error");
    } catch (error: any) {
      expect(error.message).toContain("Invalid configuration");
      expect(error.message).toContain('nodeType "View"');
      expect(error.message).toContain('materializationType "table"');
      expect(error.message).toContain("Dimension");
      expect(error.message).toContain("Fact");
      expect(error.message).toContain("Stage");
      expect(error.message).toContain("Work");
    }
  });
});
