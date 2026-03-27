import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { describe, it, expect, vi } from "vitest";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { resolveCacheResourceUri } from "../../src/cache-dir.js";
import {
  listEnvironmentNodes,
  listWorkspaceNodes,
  getEnvironmentNode,
  getWorkspaceNode,
  createWorkspaceNode,
  setWorkspaceNode,
} from "../../src/coalesce/api/nodes.js";
import {
  createWorkspaceNodeFromScratch,
  createWorkspaceNodeFromPredecessor,
  updateWorkspaceNode,
  replaceWorkspaceNodeColumns,
} from "../../src/services/workspace/mutations.js";
import { registerNodeTools } from "../../src/mcp/nodes.js";

// Mock completeNodeConfiguration so creation tests don't need full config completion setup
const mockConfigCompletion = {
  schemaSource: "corpus" as const,
  classification: { required: [], conditionalRequired: [], optionalWithDefaults: [], contextual: [], columnSelectors: [] },
  context: { hasMultipleSources: false, hasAggregates: false, hasTimestampColumns: false, hasType2Pattern: false, materializationType: "table" as const },
  appliedConfig: {},
  configChanges: { required: {}, contextual: {}, preserved: {}, defaults: {} },
  columnAttributeChanges: { applied: [], reasoning: [] },
  reasoning: [],
  detectedPatterns: { candidateColumns: [] },
};

vi.mock("../../src/services/config/intelligent.js", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../src/services/config/intelligent.js")>();
  return {
    ...orig,
    completeNodeConfiguration: vi.fn(async (client: any, params: { workspaceID: string; nodeID: string }) => {
      // Use the mock client's get to fetch the node, mirroring real behavior
      const node = await client.get(`/api/v1/workspaces/${params.workspaceID}/nodes/${params.nodeID}`);
      return { node, ...mockConfigCompletion };
    }),
  };
});

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ id: "new-node" }),
    put: vi.fn().mockResolvedValue({ id: "updated-node" }),
    delete: vi.fn(),
  };
}

describe("Node Tools", () => {
  it("registers all 14 node tools without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerNodeTools(server, client as any);
    expect(true).toBe(true);
  });

  it("analyze-workspace-patterns paginates through the full workspace node list", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    client.get.mockImplementation((_path: string, params?: Record<string, unknown>) => {
      if (!params?.startingFrom) {
        return Promise.resolve({
          data: [
            { id: "1", name: "STG_ORDERS", nodeType: "base-nodes:::Stage" },
            { id: "2", name: "INT_ORDERS", nodeType: "base-nodes:::View" },
          ],
          next: "cursor-2",
        });
      }

      if (params.startingFrom === "cursor-2") {
        return Promise.resolve({
          data: [{ id: "3", name: "FACT_ORDERS", nodeType: "Fact" }],
        });
      }

      throw new Error(`Unexpected cursor ${String(params.startingFrom)}`);
    });

    registerNodeTools(server, client as any);

    const analyzeToolCall = toolSpy.mock.calls.find(
      (call) => call[0] === "analyze_workspace_patterns"
    );
    const handler = analyzeToolCall?.[2] as
      | ((params: { workspaceID: string }) => Promise<{ content: { text: string }[] }>)
      | undefined;

    expect(typeof handler).toBe("function");

    const result = await handler!({ workspaceID: "ws-1" });
    const profile = JSON.parse(result.content[0]!.text);

    expect(client.get).toHaveBeenNthCalledWith(
      1,
      "/api/v1/workspaces/ws-1/nodes",
      { detail: false, limit: 250, orderBy: "id" }
    );
    expect(client.get).toHaveBeenNthCalledWith(
      2,
      "/api/v1/workspaces/ws-1/nodes",
      { detail: false, limit: 250, orderBy: "id", startingFrom: "cursor-2" }
    );
    expect(profile.nodeCount).toBe(3);
    expect(profile.packageAdoption.packages).toContain("base-nodes");
    expect(profile.recommendations.factType).toBe("Fact");
  });

  it("list-workspace-nodes auto-caches large responses", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "coalesce-auto-cache-node-tool-"));
    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(tempDir);
    const originalMaxBytes = process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES;
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES = "128";
    client.get.mockResolvedValue({
      data: [
        {
          id: "1",
          name: "STG_CUSTOMER",
          nodeType: "Stage",
          description: "x".repeat(1024),
        },
      ],
    });

    try {
      registerNodeTools(server, client as any);

      const listToolCall = toolSpy.mock.calls.find(
        (call) => call[0] === "list_workspace_nodes"
      );
      const handler = listToolCall?.[2] as
        | ((params: { workspaceID: string; detail?: boolean }) => Promise<{ content: { text: string }[] }>)
        | undefined;

      expect(typeof handler).toBe("function");

      const result = await handler!({ workspaceID: "ws-1", detail: true });
      const metadata = JSON.parse(result.content[0]!.text);

      expect(metadata).toMatchObject({
        autoCached: true,
        toolName: "list_workspace_nodes",
        resourceUri: expect.stringContaining("coalesce://cache/"),
      });

      const resolved = resolveCacheResourceUri(metadata.resourceUri, tempDir);
      expect(resolved).not.toBeNull();

      const cached = JSON.parse(readFileSync(resolved!.filePath, "utf8"));
      expect(cached).toEqual({
        data: [
          {
            id: "1",
            name: "STG_CUSTOMER",
            nodeType: "Stage",
            description: "x".repeat(1024),
          },
        ],
      });
      expect(result.content[1]).toMatchObject({
        type: "resource_link",
        uri: metadata.resourceUri,
      });
    } finally {
      if (originalMaxBytes === undefined) {
        delete process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES;
      } else {
        process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES = originalMaxBytes;
      }
      cwdSpy.mockRestore();
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("listEnvironmentNodes calls GET /api/v1/environments/{environmentID}/nodes", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "node-1" }] });

    const result = await listEnvironmentNodes(client as any, {
      environmentID: "env-1",
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/environments/env-1/nodes",
      {}
    );
    expect(result).toEqual({ data: [{ id: "node-1" }] });
  });

  it("listEnvironmentNodes passes pagination params", async () => {
    const client = createMockClient();

    await listEnvironmentNodes(client as any, {
      environmentID: "env-1",
      limit: 10,
      orderBy: "name",
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/environments/env-1/nodes",
      { limit: 10, orderBy: "name" }
    );
  });

  it("listWorkspaceNodes calls GET /api/v1/workspaces/{workspaceID}/nodes", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "node-2" }] });

    const result = await listWorkspaceNodes(client as any, {
      workspaceID: "ws-1",
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes",
      {}
    );
    expect(result).toEqual({ data: [{ id: "node-2" }] });
  });

  it("listWorkspaceNodes passes pagination params", async () => {
    const client = createMockClient();

    await listWorkspaceNodes(client as any, {
      workspaceID: "ws-1",
      limit: 5,
      startingFrom: "cursor-abc",
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes",
      { limit: 5, startingFrom: "cursor-abc" }
    );
  });

  it("getEnvironmentNode calls GET /api/v1/environments/{environmentID}/nodes/{nodeID}", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "node-1", name: "STG_CUSTOMERS" });

    const result = await getEnvironmentNode(client as any, {
      environmentID: "env-1",
      nodeID: "node-1",
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/environments/env-1/nodes/node-1",
      {}
    );
    expect(result).toEqual({ id: "node-1", name: "STG_CUSTOMERS" });
  });

  it("getWorkspaceNode calls GET /api/v1/workspaces/{workspaceID}/nodes/{nodeID}", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "node-2", name: "DIM_ORDERS" });

    const result = await getWorkspaceNode(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-2",
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes/node-2",
      {}
    );
    expect(result).toEqual({ id: "node-2", name: "DIM_ORDERS" });
  });

  it("createWorkspaceNode calls POST /api/v1/workspaces/{workspaceID}/nodes with nodeType and predecessors only", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    const result = await createWorkspaceNode(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      predecessorNodeIDs: ["node-a", "node-b"],
    });

    expect(client.post).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes",
      {
        nodeType: "Stage",
        predecessorNodeIDs: ["node-a", "node-b"],
      }
    );
    expect(result).toEqual({ id: "new-node", nodeType: "Stage" });
  });

  it("createWorkspaceNode works without optional fields", async () => {
    const client = createMockClient();

    await createWorkspaceNode(client as any, {
      workspaceID: "ws-1",
      nodeType: "Dimension",
    });

    expect(client.post).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes",
      { nodeType: "Dimension" }
    );
  });

  it("setWorkspaceNode calls PUT /api/v1/workspaces/{workspaceID}/nodes/{nodeID}", async () => {
    const client = createMockClient();
    client.put.mockResolvedValue({ id: "node-1", name: "UPDATED" });

    const result = await setWorkspaceNode(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      body: { name: "UPDATED", description: "Updated node" },
    });

    expect(client.put).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes/node-1",
      { name: "UPDATED", description: "Updated node" }
    );
    expect(result).toEqual({ id: "node-1", name: "UPDATED" });
  });

  it("updateWorkspaceNode fetches the current node, deep-merges object changes, and replaces arrays", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      id: "node-1",
      name: "STG_CUSTOMERS",
      description: "",
      config: {
        truncateBefore: true,
        preSQL: "",
        postSQL: "",
      },
      metadata: {
        columns: [{ name: "C1" }],
        sourceMapping: [{ name: "SRC" }],
      },
    });
    client.put.mockResolvedValue({ id: "node-1", name: "STG_CUSTOMERS" });

    const result = await updateWorkspaceNode(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      changes: {
        description: "Updated node",
        config: {
          truncateBefore: false,
        },
        metadata: {
          columns: [{ name: "C2" }],
        },
      },
    });

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes/node-1",
      {}
    );
    const putCall = client.put.mock.calls[0];
    expect(putCall[0]).toBe("/api/v1/workspaces/ws-1/nodes/node-1");
    const putBody = putCall[1];
    expect(putBody.id).toBe("node-1");
    expect(putBody.name).toBe("STG_CUSTOMERS");
    expect(putBody.table).toBe("STG_CUSTOMERS");
    expect(putBody.description).toBe("Updated node");
    expect(putBody.config).toEqual({
      truncateBefore: false,
      preSQL: "",
      postSQL: "",
    });
    expect(putBody.metadata.columns).toHaveLength(1);
    expect(putBody.metadata.columns[0].name).toBe("C2");
    expect(putBody.metadata.columns[0].nullable).toBe(true);
    expect(typeof putBody.metadata.columns[0].columnID).toBe("string");
    expect(putBody.metadata.sourceMapping).toEqual([{ name: "SRC" }]);
    expect(result).toEqual({ id: "node-1", name: "STG_CUSTOMERS" });
  });

  it("updateWorkspaceNode keeps metadata.sourceMapping names aligned when renaming a node", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      id: "node-1",
      name: "STG_NODE",
      metadata: {
        sourceMapping: [{ name: "STG_NODE", dependencies: [] }],
      },
    });
    client.put.mockResolvedValue({ id: "node-1", name: "RENAMED_NODE" });

    await updateWorkspaceNode(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      changes: {
        name: "RENAMED_NODE",
      },
    });

    expect(client.put).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes/node-1",
      {
        id: "node-1",
        name: "RENAMED_NODE",
        table: "RENAMED_NODE",
        metadata: {
          enabledColumnTestIDs: [],
          sourceMapping: [{ name: "RENAMED_NODE", dependencies: [] }],
        },
      }
    );
  });

  it("updateWorkspaceNode rejects SQL override fields in changes", async () => {
    const client = createMockClient();

    await expect(
      updateWorkspaceNode(client as any, {
        workspaceID: "ws-1",
        nodeID: "node-1",
        changes: {
          overrideSQL: true,
        },
      })
    ).rejects.toThrow(
      "update_workspace_node changes cannot set SQL override fields. Remove overrideSQL. SQL override is intentionally disallowed in this project."
    );

    expect(client.get).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it("updateWorkspaceNode does not rewrite unrelated sourceMapping names when renaming a node", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      id: "node-1",
      name: "STG_NODE",
      metadata: {
        sourceMapping: [
          { name: "LEFT_SOURCE", dependencies: [] },
          { name: "STG_NODE", dependencies: [] },
        ],
      },
    });
    client.put.mockResolvedValue({ id: "node-1", name: "RENAMED_NODE" });

    await updateWorkspaceNode(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      changes: {
        name: "RENAMED_NODE",
      },
    });

    expect(client.put).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes/node-1",
      {
        id: "node-1",
        name: "RENAMED_NODE",
        table: "RENAMED_NODE",
        metadata: {
          enabledColumnTestIDs: [],
          sourceMapping: [
            { name: "LEFT_SOURCE", dependencies: [] },
            { name: "RENAMED_NODE", dependencies: [] },
          ],
        },
      }
    );
  });

  it("createWorkspaceNodeFromScratch returns a created node when completionLevel is created", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      return Promise.resolve({
        id: "new-node",
        nodeType: "Stage",
        config: {},
      });
    });

    const result = await createWorkspaceNodeFromScratch(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      completionLevel: "created",
    });

    expect(client.post).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes",
      { nodeType: "Stage" }
    );
    expect(client.put).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      node: {
        id: "new-node",
        nodeType: "Stage",
        config: {},
      },
      validation: {
        requestedCompletionLevel: "created",
        completionSatisfied: true,
        nameRequired: false,
        nameSet: false,
        requestedName: null,
        requestedNameSatisfied: true,
        requestedLocationKeys: [],
        requestedLocationSatisfied: true,
        storageLocationsRequired: false,
        storageLocationCount: 0,
        storageLocationsSet: false,
        columnCount: 0,
        configPresent: true,
        configKeyCount: 0,
        requestedColumnCount: 0,
        requestedColumnNames: [],
        requestedColumnsSatisfied: false,
        requestedConfigKeys: [],
        requestedConfigSatisfied: true,
      },
      configCompletion: expect.objectContaining({ schemaSource: "corpus" }),
    });
  });

  it("createWorkspaceNodeFromScratch rejects SQL override fields before creating a node", async () => {
    const client = createMockClient();

    await expect(
      createWorkspaceNodeFromScratch(client as any, {
        workspaceID: "ws-1",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        metadata: {
          columns: [{ name: "CUSTOMER_ID" }],
        },
        changes: {
          override: {
            create: {
              enabled: true,
            },
          },
        },
      })
    ).rejects.toThrow(
      "create_workspace_node_from_scratch changes cannot set SQL override fields. Remove override. SQL override is intentionally disallowed in this project."
    );

    expect(client.post).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it("createWorkspaceNodeFromScratch defaults to configured and applies name, storageLocations, metadata, config, and extra changes", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    let getCallCount = 0;
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      getCallCount++;
      if (getCallCount === 1) {
        return Promise.resolve({
          id: "new-node",
          nodeType: "Stage",
          description: "",
          config: {
            truncateBefore: true,
          },
          metadata: {
            columns: [],
            sourceMapping: [{ name: "STG_NODE", dependencies: [] }],
          },
          storageLocations: [],
        });
      }
      return Promise.resolve({
        id: "new-node",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        description: "Scratch stage",
        config: {
          truncateBefore: false,
          postSQL: "",
        },
        metadata: {
          columns: [{ name: "CUSTOMER_ID" }],
          sourceMapping: [{ name: "STG_CUSTOMERS", dependencies: [] }],
        },
        storageLocations: [{ locationName: "DEV" }],
      });
    });
    client.put.mockResolvedValue({ id: "new-node" });

    const result = await createWorkspaceNodeFromScratch(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      name: "STG_CUSTOMERS",
      description: "Scratch stage",
      storageLocations: [{ locationName: "DEV" }],
      config: {
        truncateBefore: false,
      },
      metadata: {
        columns: [{ name: "CUSTOMER_ID" }],
      },
      changes: {
        config: {
          postSQL: "",
        },
      },
    });

    const putCall = client.put.mock.calls[0];
    expect(putCall[0]).toBe("/api/v1/workspaces/ws-1/nodes/new-node");
    const putBody = putCall[1];
    expect(putBody.id).toBe("new-node");
    expect(putBody.nodeType).toBe("Stage");
    expect(putBody.name).toBe("STG_CUSTOMERS");
    expect(putBody.table).toBe("STG_CUSTOMERS");
    expect(putBody.description).toBe("Scratch stage");
    expect(putBody.config).toEqual({ truncateBefore: false, postSQL: "" });
    expect(putBody.metadata.columns).toHaveLength(1);
    expect(putBody.metadata.columns[0].name).toBe("CUSTOMER_ID");
    expect(putBody.metadata.columns[0].nullable).toBe(true);
    expect(typeof putBody.metadata.columns[0].columnID).toBe("string");
    expect(putBody.metadata.sourceMapping).toEqual([{ name: "STG_CUSTOMERS", dependencies: [] }]);
    expect(putBody.storageLocations).toEqual([{ locationName: "DEV" }]);
    expect(result).toMatchObject({
      node: {
        id: "new-node",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        description: "Scratch stage",
        config: {
          truncateBefore: false,
          postSQL: "",
        },
        metadata: {
          columns: [{ name: "CUSTOMER_ID" }],
          sourceMapping: [{ name: "STG_CUSTOMERS", dependencies: [] }],
        },
        storageLocations: [{ locationName: "DEV" }],
      },
      validation: {
        requestedCompletionLevel: "configured",
        completionSatisfied: true,
        nameRequired: true,
        nameSet: true,
        requestedName: "STG_CUSTOMERS",
        requestedNameSatisfied: true,
        requestedLocationKeys: [],
        requestedLocationSatisfied: true,
        storageLocationsRequired: true,
        storageLocationCount: 1,
        storageLocationsSet: true,
        columnCount: 1,
        configPresent: true,
        configKeyCount: 2,
        requestedColumnCount: 1,
        requestedColumnNames: ["CUSTOMER_ID"],
        requestedColumnsSatisfied: true,
        requestedConfigKeys: ["postSQL", "truncateBefore"],
        requestedConfigSatisfied: true,
      },
      configCompletion: expect.objectContaining({ schemaSource: "corpus" }),
    });
  });

  it("createWorkspaceNodeFromScratch validates requested top-level location fields", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    let getCallCount = 0;
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      getCallCount++;
      if (getCallCount === 1) {
        return Promise.resolve({
          id: "new-node",
          nodeType: "Stage",
          database: "",
          schema: "",
          locationName: "",
          config: {
            truncateBefore: true,
          },
          metadata: {
            columns: [],
            sourceMapping: [{ name: "STG_NODE", dependencies: [] }],
          },
        });
      }
      return Promise.resolve({
        id: "new-node",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        database: "ANALYTICS",
        schema: "ETL_STAGE",
        locationName: "ETL_STAGE",
        config: {
          truncateBefore: false,
        },
        metadata: {
          columns: [{ name: "CUSTOMER_ID" }],
          sourceMapping: [{ name: "STG_CUSTOMERS", dependencies: [] }],
        },
      });
    });
    client.put.mockResolvedValue({ id: "new-node" });

    const result = await createWorkspaceNodeFromScratch(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      name: "STG_CUSTOMERS",
      metadata: {
        columns: [{ name: "CUSTOMER_ID" }],
      },
      config: {
        truncateBefore: false,
      },
      changes: {
        database: "ANALYTICS",
        schema: "ETL_STAGE",
        locationName: "ETL_STAGE",
      },
    });

    expect(result).toMatchObject({
      node: {
        id: "new-node",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        database: "ANALYTICS",
        schema: "ETL_STAGE",
        locationName: "ETL_STAGE",
        config: {
          truncateBefore: false,
        },
        metadata: {
          columns: [{ name: "CUSTOMER_ID" }],
          sourceMapping: [{ name: "STG_CUSTOMERS", dependencies: [] }],
        },
      },
      validation: {
        requestedCompletionLevel: "configured",
        completionSatisfied: true,
        nameRequired: true,
        nameSet: true,
        requestedName: "STG_CUSTOMERS",
        requestedNameSatisfied: true,
        requestedLocationKeys: ["database", "schema", "locationName"],
        requestedLocationSatisfied: true,
        storageLocationsRequired: false,
        storageLocationCount: 0,
        storageLocationsSet: false,
        columnCount: 1,
        configPresent: true,
        configKeyCount: 1,
        requestedColumnCount: 1,
        requestedColumnNames: ["CUSTOMER_ID"],
        requestedColumnsSatisfied: true,
        requestedConfigKeys: ["truncateBefore"],
        requestedConfigSatisfied: true,
      },
      configCompletion: expect.objectContaining({ schemaSource: "corpus" }),
    });
  });

  it("createWorkspaceNodeFromScratch satisfies named completion without storageLocations when none were requested", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    let getCallCount = 0;
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      getCallCount++;
      if (getCallCount === 1) {
        return Promise.resolve({
          id: "new-node",
          nodeType: "Stage",
          config: {},
          storageLocations: [],
          metadata: {
            sourceMapping: [{ name: "STG_NODE", dependencies: [] }],
          },
        });
      }
      return Promise.resolve({
        id: "new-node",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        config: {},
        storageLocations: [],
        metadata: {
          columns: [],
          sourceMapping: [{ name: "STG_CUSTOMERS", dependencies: [] }],
        },
      });
    });
    client.put.mockResolvedValue({ id: "new-node" });

    const result = await createWorkspaceNodeFromScratch(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      completionLevel: "named",
      name: "STG_CUSTOMERS",
    });

    expect(result).toMatchObject({
      node: {
        id: "new-node",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        config: {},
        storageLocations: [],
        metadata: {
          columns: [],
          sourceMapping: [{ name: "STG_CUSTOMERS", dependencies: [] }],
        },
      },
      validation: {
        requestedCompletionLevel: "named",
        completionSatisfied: true,
        nameRequired: true,
        nameSet: true,
        requestedName: "STG_CUSTOMERS",
        requestedNameSatisfied: true,
        requestedLocationKeys: [],
        requestedLocationSatisfied: true,
        storageLocationsRequired: false,
        storageLocationCount: 0,
        storageLocationsSet: false,
        columnCount: 0,
        configPresent: true,
        configKeyCount: 0,
        requestedColumnCount: 0,
        requestedColumnNames: [],
        requestedColumnsSatisfied: false,
        requestedConfigKeys: [],
        requestedConfigSatisfied: true,
      },
      configCompletion: expect.objectContaining({ schemaSource: "corpus" }),
    });
  });

  it("createWorkspaceNodeFromScratch returns a warning when the requested completion level is not satisfied", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    let getCallCount = 0;
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      getCallCount++;
      if (getCallCount === 1) {
        return Promise.resolve({
          id: "new-node",
          nodeType: "Stage",
          config: {},
          storageLocations: [],
          metadata: {
            sourceMapping: [{ name: "STG_NODE", dependencies: [] }],
          },
        });
      }
      return Promise.resolve({
        id: "new-node",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        config: {},
        storageLocations: [],
        metadata: {
          columns: [],
          sourceMapping: [{ name: "STG_CUSTOMERS", dependencies: [] }],
        },
      });
    });
    client.put.mockResolvedValue({ id: "new-node" });

    const result = await createWorkspaceNodeFromScratch(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      completionLevel: "named",
      name: "STG_CUSTOMERS",
      storageLocations: [{ locationName: "DEV" }],
    });

    expect(result).toMatchObject({
      node: {
        id: "new-node",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        config: {},
        storageLocations: [],
        metadata: {
          columns: [],
          sourceMapping: [{ name: "STG_CUSTOMERS", dependencies: [] }],
        },
      },
      validation: {
        requestedCompletionLevel: "named",
        completionSatisfied: false,
        nameRequired: true,
        nameSet: true,
        requestedName: "STG_CUSTOMERS",
        requestedNameSatisfied: true,
        requestedLocationKeys: [],
        requestedLocationSatisfied: true,
        storageLocationsRequired: true,
        storageLocationCount: 0,
        storageLocationsSet: false,
        columnCount: 0,
        configPresent: true,
        configKeyCount: 0,
        requestedColumnCount: 0,
        requestedColumnNames: [],
        requestedColumnsSatisfied: false,
        requestedConfigKeys: [],
        requestedConfigSatisfied: true,
      },
      warning:
        "Workspace node was created, but the requested scratch completion level was not fully satisfied. Review the node body and provide any missing name, storageLocations, metadata.columns, or config fields.",
    });
  });

  it("createWorkspaceNodeFromScratch refuses default configured creation when name and columns are missing", async () => {
    const client = createMockClient();

    await expect(
      createWorkspaceNodeFromScratch(client as any, {
        workspaceID: "ws-1",
        nodeType: "Stage",
      })
    ).rejects.toThrow(
      'Configured scratch node creation requires name and metadata.columns. Provide them explicitly or lower completionLevel to "named" or "created".'
    );

    expect(client.post).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it("createWorkspaceNodeFromScratch fails configured validation when requested columns do not persist", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    let getCallCount = 0;
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      getCallCount++;
      if (getCallCount === 1) {
        return Promise.resolve({
          id: "new-node",
          nodeType: "Stage",
          config: {
            truncateBefore: true,
          },
          metadata: {
            columns: [],
            sourceMapping: [{ name: "STG_NODE", dependencies: [] }],
          },
        });
      }
      return Promise.resolve({
        id: "new-node",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        config: {
          truncateBefore: false,
        },
        metadata: {
          columns: [{ name: "CUSTOMER_ID" }],
          sourceMapping: [{ name: "STG_CUSTOMERS", dependencies: [] }],
        },
      });
    });
    client.put.mockResolvedValue({ id: "new-node" });

    await expect(
      createWorkspaceNodeFromScratch(client as any, {
        workspaceID: "ws-1",
        nodeType: "Stage",
        name: "STG_CUSTOMERS",
        metadata: {
          columns: [{ name: "CUSTOMER_ID" }, { name: "CUSTOMER_NAME" }],
        },
        config: {
          truncateBefore: false,
        },
      })
    ).rejects.toThrow(
      "Workspace node new-node was created, but configured scratch validation failed. Check name, metadata.columns, and config values on the saved node body."
    );

    expect(client.post).toHaveBeenCalled();
    expect(client.put).toHaveBeenCalled();
  });

  it("createWorkspaceNodeFromPredecessor validates auto-populated columns and returns the created node", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/pred-1") {
        return Promise.resolve({
          id: "pred-1",
          name: "CUSTOMER",
          metadata: {
            columns: [{ name: "C1" }],
          },
        });
      }
      return Promise.resolve({
        id: "new-node",
        name: "STG_CUSTOMERS",
        metadata: {
          columns: [
            {
              name: "C1",
              sources: [
                {
                  columnReferences: [{ nodeID: "pred-1" }],
                },
              ],
            },
          ],
          sourceMapping: [
            {
              dependencies: [{ nodeName: "CUSTOMER" }],
            },
          ],
        },
      });
    });

    const result = await createWorkspaceNodeFromPredecessor(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      predecessorNodeIDs: ["pred-1"],
    });

    expect(client.post).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes",
      {
        nodeType: "Stage",
        predecessorNodeIDs: ["pred-1"],
      }
    );
    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes/pred-1",
      {}
    );
    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes/new-node",
      {}
    );
    expect(result).toMatchObject({
      node: {
        id: "new-node",
        name: "STG_CUSTOMERS",
        metadata: {
          columns: [
            {
              name: "C1",
              sources: [
                {
                  columnReferences: [{ nodeID: "pred-1" }],
                },
              ],
            },
          ],
          sourceMapping: [
            {
              dependencies: [{ nodeName: "CUSTOMER" }],
            },
          ],
        },
      },
      predecessors: [
        {
          nodeID: "pred-1",
          nodeName: "CUSTOMER",
          columnCount: 1,
          columnNames: ["C1"],
        },
      ],
      joinSuggestions: [],
      validation: {
        autoPopulatedColumns: true,
        allPredecessorsRepresented: true,
        columnCount: 1,
        dependencyCount: 1,
        dependencyNames: ["CUSTOMER"],
        predecessorNodeIDs: ["pred-1"],
        referencedPredecessorNodeIDs: ["pred-1"],
      },
      configCompletion: expect.objectContaining({ schemaSource: "corpus" }),
    });
  });

  it("createWorkspaceNodeFromPredecessor rejects SQL override fields before reading predecessors", async () => {
    const client = createMockClient();

    await expect(
      createWorkspaceNodeFromPredecessor(client as any, {
        workspaceID: "ws-1",
        nodeType: "Stage",
        predecessorNodeIDs: ["pred-1"],
        changes: {
          overrideSQL: true,
        },
      })
    ).rejects.toThrow(
      "create_workspace_node_from_predecessor changes cannot set SQL override fields. Remove overrideSQL. SQL override is intentionally disallowed in this project."
    );

    expect(client.get).not.toHaveBeenCalled();
    expect(client.post).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });

  it("createWorkspaceNodeFromPredecessor returns join suggestions and requires all predecessors for multi-source success", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/pred-1") {
        return Promise.resolve({
          id: "pred-1",
          name: "NATION",
          metadata: {
            columns: [{ name: "ID" }, { name: "LOADTIME" }, { name: "COUNTRY" }],
          },
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/pred-2") {
        return Promise.resolve({
          id: "pred-2",
          name: "CUSTOMER_LOYALTY",
          metadata: {
            columns: [{ name: "customer_id" }, { name: "loadtime" }, { name: "country" }],
          },
        });
      }
      return Promise.resolve({
        id: "new-node",
        name: "STG_JOINED",
        metadata: {
          columns: [
            {
              name: "ID",
              sources: [
                {
                  columnReferences: [{ nodeID: "pred-1" }],
                },
              ],
            },
            {
              name: "CUSTOMER_ID",
              sources: [
                {
                  columnReferences: [{ nodeID: "pred-2" }],
                },
              ],
            },
          ],
          sourceMapping: [
            {
              dependencies: [{ nodeName: "NATION" }, { nodeName: "CUSTOMER_LOYALTY" }],
            },
          ],
        },
      });
    });

    const result = await createWorkspaceNodeFromPredecessor(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      predecessorNodeIDs: ["pred-1", "pred-2"],
    });

    expect(result).toMatchObject({
      node: {
        id: "new-node",
        name: "STG_JOINED",
        metadata: {
          columns: [
            {
              name: "ID",
              sources: [
                {
                  columnReferences: [{ nodeID: "pred-1" }],
                },
              ],
            },
            {
              name: "CUSTOMER_ID",
              sources: [
                {
                  columnReferences: [{ nodeID: "pred-2" }],
                },
              ],
            },
          ],
          sourceMapping: [
            {
              dependencies: [{ nodeName: "NATION" }, { nodeName: "CUSTOMER_LOYALTY" }],
            },
          ],
        },
      },
      predecessors: [
        {
          nodeID: "pred-1",
          nodeName: "NATION",
          columnCount: 3,
          columnNames: ["ID", "LOADTIME", "COUNTRY"],
        },
        {
          nodeID: "pred-2",
          nodeName: "CUSTOMER_LOYALTY",
          columnCount: 3,
          columnNames: ["customer_id", "loadtime", "country"],
        },
      ],
      joinSuggestions: [
        {
          leftPredecessorNodeID: "pred-1",
          leftPredecessorName: "NATION",
          rightPredecessorNodeID: "pred-2",
          rightPredecessorName: "CUSTOMER_LOYALTY",
          commonColumns: [
            {
              normalizedName: "COUNTRY",
              leftColumnName: "COUNTRY",
              rightColumnName: "country",
            },
            {
              normalizedName: "LOADTIME",
              leftColumnName: "LOADTIME",
              rightColumnName: "loadtime",
            },
          ],
        },
      ],
      validation: {
        autoPopulatedColumns: true,
        allPredecessorsRepresented: true,
        columnCount: 2,
        dependencyCount: 2,
        dependencyNames: ["NATION", "CUSTOMER_LOYALTY"],
        predecessorNodeIDs: ["pred-1", "pred-2"],
        referencedPredecessorNodeIDs: ["pred-1", "pred-2"],
      },
      configCompletion: expect.objectContaining({ schemaSource: "corpus" }),
    });
  });

  it("createWorkspaceNodeFromPredecessor dedupes duplicate predecessor IDs for self-join creation", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/pred-1") {
        return Promise.resolve({
          id: "pred-1",
          name: "CUSTOMER",
          metadata: {
            columns: [{ name: "CUSTOMER_ID" }, { name: "CUSTOMER_NAME" }],
          },
        });
      }
      return Promise.resolve({
        id: "new-node",
        name: "STG_CUSTOMER",
        metadata: {
          columns: [
            {
              name: "CUSTOMER_ID",
              sources: [
                {
                  columnReferences: [{ nodeID: "pred-1" }],
                },
              ],
            },
          ],
          sourceMapping: [
            {
              dependencies: [{ nodeName: "CUSTOMER" }],
            },
          ],
        },
      });
    });

    const result = await createWorkspaceNodeFromPredecessor(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      predecessorNodeIDs: ["pred-1", "pred-1"],
    });

    expect(client.post).toHaveBeenCalledWith("/api/v1/workspaces/ws-1/nodes", {
      nodeType: "Stage",
      predecessorNodeIDs: ["pred-1"],
    });
    expect(result).toMatchObject({
      predecessors: [
        {
          nodeID: "pred-1",
          nodeName: "CUSTOMER",
          columnCount: 2,
          columnNames: ["CUSTOMER_ID", "CUSTOMER_NAME"],
        },
      ],
      validation: {
        autoPopulatedColumns: true,
        allPredecessorsRepresented: true,
        columnCount: 1,
        dependencyCount: 1,
        dependencyNames: ["CUSTOMER"],
        predecessorNodeIDs: ["pred-1"],
        referencedPredecessorNodeIDs: ["pred-1"],
      },
      configCompletion: expect.objectContaining({ schemaSource: "corpus" }),
    });
  });

  it("createWorkspaceNodeFromPredecessor returns a warning when a join node does not include all predecessors", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes") {
        return Promise.resolve({ data: [{ id: "existing-node", nodeType: "Stage" }] });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/pred-1") {
        return Promise.resolve({
          id: "pred-1",
          name: "LEFT_NODE",
          metadata: {
            columns: [{ name: "ID" }, { name: "COUNTRY" }],
          },
        });
      }
      if (path === "/api/v1/workspaces/ws-1/nodes/pred-2") {
        return Promise.resolve({
          id: "pred-2",
          name: "RIGHT_NODE",
          metadata: {
            columns: [{ name: "ID" }, { name: "COUNTRY" }],
          },
        });
      }
      return Promise.resolve({
        id: "new-node",
        name: "STG_JOINED",
        metadata: {
          columns: [
            {
              name: "ID",
              sources: [
                {
                  columnReferences: [{ nodeID: "pred-1" }],
                },
              ],
            },
          ],
          sourceMapping: [
            {
              dependencies: [{ nodeName: "LEFT_NODE" }, { nodeName: "RIGHT_NODE" }],
            },
          ],
        },
      });
    });

    const result = await createWorkspaceNodeFromPredecessor(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      predecessorNodeIDs: ["pred-1", "pred-2"],
      changes: { description: "Should not be applied" },
    });

    expect(client.put).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      node: {
        id: "new-node",
        name: "STG_JOINED",
        metadata: {
          columns: [
            {
              name: "ID",
              sources: [
                {
                  columnReferences: [{ nodeID: "pred-1" }],
                },
              ],
            },
          ],
          sourceMapping: [
            {
              dependencies: [{ nodeName: "LEFT_NODE" }, { nodeName: "RIGHT_NODE" }],
            },
          ],
        },
      },
      predecessors: [
        {
          nodeID: "pred-1",
          nodeName: "LEFT_NODE",
          columnCount: 2,
          columnNames: ["ID", "COUNTRY"],
        },
        {
          nodeID: "pred-2",
          nodeName: "RIGHT_NODE",
          columnCount: 2,
          columnNames: ["ID", "COUNTRY"],
        },
      ],
      joinSuggestions: [
        {
          leftPredecessorNodeID: "pred-1",
          leftPredecessorName: "LEFT_NODE",
          rightPredecessorNodeID: "pred-2",
          rightPredecessorName: "RIGHT_NODE",
          commonColumns: [
            {
              normalizedName: "COUNTRY",
              leftColumnName: "COUNTRY",
              rightColumnName: "COUNTRY",
            },
            {
              normalizedName: "ID",
              leftColumnName: "ID",
              rightColumnName: "ID",
            },
          ],
        },
      ],
      validation: {
        autoPopulatedColumns: false,
        allPredecessorsRepresented: false,
        columnCount: 1,
        dependencyCount: 2,
        dependencyNames: ["LEFT_NODE", "RIGHT_NODE"],
        predecessorNodeIDs: ["pred-1", "pred-2"],
        referencedPredecessorNodeIDs: ["pred-1"],
      },
      warning:
        "Workspace node was created from predecessor(s), but columns were not auto-populated from all requested predecessors. Review the suggested join columns and verify the node in Coalesce before proceeding.",
    });
  });

  it("replaceWorkspaceNodeColumns replaces all columns completely", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      id: "node-1",
      name: "FCT_TABLE",
      nodeType: "Fact",
      metadata: {
        columns: [
          { name: "OLD_COL_1" },
          { name: "OLD_COL_2" },
        ],
        sourceMapping: [{ name: "FCT_TABLE", dependencies: [] }],
      },
      config: { materializationType: "table" },
    });
    client.put.mockResolvedValue({ id: "node-1" });

    const result = await replaceWorkspaceNodeColumns(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      columns: [
        { name: "CUSTOMER_ID", transform: '"STG_ORDER"."CUSTOMER_ID"' },
        { name: "TOTAL_ORDERS", transform: 'COUNT(DISTINCT "STG_ORDER"."ORDER_ID")' },
      ],
    });

    const putCall = client.put.mock.calls[0];
    expect(putCall[0]).toBe("/api/v1/workspaces/ws-1/nodes/node-1");
    const putBody = putCall[1];
    expect(putBody.name).toBe("FCT_TABLE");
    expect(putBody.table).toBe("FCT_TABLE");
    expect(putBody.metadata.columns).toHaveLength(2);
    expect(putBody.metadata.columns[0].name).toBe("CUSTOMER_ID");
    expect(putBody.metadata.columns[0].nullable).toBe(true);
    expect(typeof putBody.metadata.columns[0].columnID).toBe("string");
    expect(putBody.metadata.columns[1].name).toBe("TOTAL_ORDERS");
    expect(typeof putBody.metadata.columns[1].columnID).toBe("string");
  });

  it("replaceWorkspaceNodeColumns applies additional changes", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      id: "node-1",
      name: "FCT_TABLE",
      nodeType: "Fact",
      metadata: {
        columns: [{ name: "OLD_COL" }],
        sourceMapping: [{ name: "FCT_TABLE", dependencies: [] }],
      },
      config: { materializationType: "table" },
      description: "Old description",
    });
    client.put.mockResolvedValue({ id: "node-1" });

    await replaceWorkspaceNodeColumns(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      columns: [
        { name: "CUSTOMER_ID", transform: '"STG_ORDER"."CUSTOMER_ID"' },
      ],
      additionalChanges: {
        description: "Customer purchase behavior metrics",
        config: {
          materializationType: "table",
          testsEnabled: true,
        },
      },
    });

    const putCall2 = client.put.mock.calls[0];
    const putBody2 = putCall2[1];
    expect(putBody2.name).toBe("FCT_TABLE");
    expect(putBody2.description).toBe("Customer purchase behavior metrics");
    expect(putBody2.config.testsEnabled).toBe(true);
    expect(putBody2.metadata.columns).toHaveLength(1);
    expect(putBody2.metadata.columns[0].name).toBe("CUSTOMER_ID");
    expect(typeof putBody2.metadata.columns[0].columnID).toBe("string");
  });

  it("replaceWorkspaceNodeColumns rejects SQL override in additionalChanges", async () => {
    const client = createMockClient();

    await expect(
      replaceWorkspaceNodeColumns(client as any, {
        workspaceID: "ws-1",
        nodeID: "node-1",
        columns: [{ name: "COL1" }],
        additionalChanges: {
          override: {
            create: { enabled: true },
          },
        },
      })
    ).rejects.toThrow(
      "replace_workspace_node_columns additionalChanges cannot set SQL override fields"
    );

    expect(client.get).not.toHaveBeenCalled();
    expect(client.put).not.toHaveBeenCalled();
  });
});
