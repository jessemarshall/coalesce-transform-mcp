import { describe, it, expect, vi } from "vitest";
import YAML from "yaml";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../../src/client.js";
import { defineRenderNodeTools } from "../../src/mcp/render-node.js";

type ToolEntry = [string, unknown, (params: unknown) => Promise<unknown>];

interface ToolResponse {
  content: Array<{ type: string; text: string }>;
  structuredContent?: unknown;
  isError?: boolean;
}

function getHandler(tools: ToolEntry[], name: string): ToolEntry[2] {
  const entry = tools.find((t) => t[0] === name);
  if (!entry) throw new Error(`Tool "${name}" not registered`);
  return entry[2];
}

function parseStructured(response: ToolResponse): Record<string, unknown> {
  if (response.structuredContent && typeof response.structuredContent === "object") {
    return response.structuredContent as Record<string, unknown>;
  }
  const textEntry = response.content.find((c) => c.type === "text");
  if (!textEntry) throw new Error("No text content in response");
  return JSON.parse(textEntry.text) as Record<string, unknown>;
}

function createServer(): McpServer {
  return {} as unknown as McpServer;
}

function createMockClient(): CoalesceClient {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  } as unknown as CoalesceClient;
}

// Minimal cloud-shape node used as the source for serialize tests.  The shape
// matches what `GET /api/v1/workspaces/:id/nodes/:id` returns (flat top-level
// fields, metadata.columns[].sources[].columnReferences[].nodeID/columnID).
function buildCloudNode(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: "node-1",
    name: "STG_CUSTOMER",
    description: "Stage customers",
    nodeType: "base-nodes:::Stage",
    locationName: "STG",
    database: "ANALYTICS",
    schema: "STAGING",
    config: { testsEnabled: true },
    metadata: {
      columns: [
        {
          name: "CUSTOMER_ID",
          dataType: "VARCHAR",
          nullable: false,
          columnID: "col-cust-id",
          sources: [
            {
              transform: "",
              columnReferences: [{ nodeID: "src-1", columnID: "src-1-cust-id" }],
            },
          ],
        },
      ],
      sourceMapping: [],
    },
    ...overrides,
  } as Record<string, unknown>;
}

describe("serialize_workspace_node_to_disk_yaml handler", () => {
  it("converts a cloud node into disk-shape YAML with a suggested filename", async () => {
    const client = createMockClient();
    const cloudNode = buildCloudNode();
    (client.get as ReturnType<typeof vi.fn>).mockResolvedValue(cloudNode);

    const tools = defineRenderNodeTools(createServer(), client) as unknown as ToolEntry[];
    const handler = getHandler(tools, "serialize_workspace_node_to_disk_yaml");

    const response = (await handler({ workspaceID: "ws-1", nodeID: "node-1" })) as ToolResponse;
    const body = parseStructured(response);

    expect(response.isError).toBeFalsy();
    expect(body.nodeName).toBe("STG_CUSTOMER");
    expect(body.locationName).toBe("STG");
    expect(body.suggestedFilename).toBe("STG-STG_CUSTOMER.yml");

    const diskNode = body.diskNode as Record<string, unknown>;
    expect(diskNode.fileVersion).toBe(1);
    expect(diskNode.id).toBe("node-1");
    expect(diskNode.name).toBe("STG_CUSTOMER");
    const operation = diskNode.operation as Record<string, unknown>;
    expect(operation.locationName).toBe("STG");
    expect(operation.nodeType).toBe("base-nodes:::Stage");

    // `yaml` field round-trips through YAML.parse back to the same disk shape.
    const yamlString = body.yaml as string;
    expect(yamlString).toContain("fileVersion: 1");
    expect(YAML.parse(yamlString)).toEqual(diskNode);
  });

  it("falls back to <name>.yml when the cloud node has no locationName", async () => {
    const client = createMockClient();
    const cloudNode = buildCloudNode();
    delete (cloudNode as Record<string, unknown>).locationName;
    (client.get as ReturnType<typeof vi.fn>).mockResolvedValue(cloudNode);

    const tools = defineRenderNodeTools(createServer(), client) as unknown as ToolEntry[];
    const handler = getHandler(tools, "serialize_workspace_node_to_disk_yaml");

    const response = (await handler({ workspaceID: "ws-1", nodeID: "node-1" })) as ToolResponse;
    const body = parseStructured(response);

    expect(response.isError).toBeFalsy();
    expect(body.suggestedFilename).toBe("STG_CUSTOMER.yml");
    expect(body.locationName).toBeUndefined();
  });

  it("returns an error when getWorkspaceNode resolves to a non-object", async () => {
    const client = createMockClient();
    (client.get as ReturnType<typeof vi.fn>).mockResolvedValue(null);

    const tools = defineRenderNodeTools(createServer(), client) as unknown as ToolEntry[];
    const handler = getHandler(tools, "serialize_workspace_node_to_disk_yaml");

    const response = (await handler({ workspaceID: "ws-1", nodeID: "missing" })) as ToolResponse;

    expect(response.isError).toBe(true);
    const body = parseStructured(response);
    expect(JSON.stringify(body)).toContain("expected object");
  });

  it("surfaces upstream API failures via handleToolError", async () => {
    const client = createMockClient();
    (client.get as ReturnType<typeof vi.fn>).mockRejectedValue(new Error("boom"));

    const tools = defineRenderNodeTools(createServer(), client) as unknown as ToolEntry[];
    const handler = getHandler(tools, "serialize_workspace_node_to_disk_yaml");

    const response = (await handler({ workspaceID: "ws-1", nodeID: "node-1" })) as ToolResponse;

    expect(response.isError).toBe(true);
    expect(JSON.stringify(parseStructured(response))).toContain("boom");
  });
});

describe("parse_disk_node_to_workspace_body handler", () => {
  // Build a minimal disk-shape doc by serializing through the cloud→disk
  // converter — guarantees the shape stays in sync if the bridge changes.
  async function buildDiskNode(): Promise<Record<string, unknown>> {
    const client = createMockClient();
    (client.get as ReturnType<typeof vi.fn>).mockResolvedValue(buildCloudNode());
    const tools = defineRenderNodeTools(createServer(), client) as unknown as ToolEntry[];
    const serialize = getHandler(tools, "serialize_workspace_node_to_disk_yaml");
    const response = (await serialize({ workspaceID: "ws-1", nodeID: "node-1" })) as ToolResponse;
    return parseStructured(response).diskNode as Record<string, unknown>;
  }

  it("converts a YAML string back into a cloud workspace body", async () => {
    const diskNode = await buildDiskNode();
    const yamlString = YAML.stringify(diskNode, { lineWidth: 0 });

    const tools = defineRenderNodeTools(createServer(), createMockClient()) as unknown as ToolEntry[];
    const handler = getHandler(tools, "parse_disk_node_to_workspace_body");

    const response = (await handler({ yaml: yamlString })) as ToolResponse;
    const body = parseStructured(response);

    expect(response.isError).toBeFalsy();
    expect(body.nodeID).toBe("node-1");
    expect(body.name).toBe("STG_CUSTOMER");
    const cloudBody = body.cloudBody as Record<string, unknown>;
    expect(cloudBody.id).toBe("node-1");
    expect(cloudBody.locationName).toBe("STG");
    // Disk wraps under operation:; the cloud shape should NOT carry operation.
    expect(cloudBody.operation).toBeUndefined();
  });

  it("accepts a pre-parsed diskNode without a yaml string", async () => {
    const diskNode = await buildDiskNode();

    const tools = defineRenderNodeTools(createServer(), createMockClient()) as unknown as ToolEntry[];
    const handler = getHandler(tools, "parse_disk_node_to_workspace_body");

    const response = (await handler({ diskNode })) as ToolResponse;
    const body = parseStructured(response);

    expect(response.isError).toBeFalsy();
    expect((body.cloudBody as Record<string, unknown>).id).toBe("node-1");
  });

  it("prefers diskNode over yaml when both are provided", async () => {
    const diskNode = await buildDiskNode();
    const aliasedDisk = { ...diskNode, name: "FROM_DISK_NODE" };
    const yamlOfDifferentNode = YAML.stringify(
      { ...diskNode, name: "FROM_YAML_STRING" },
      { lineWidth: 0 }
    );

    const tools = defineRenderNodeTools(createServer(), createMockClient()) as unknown as ToolEntry[];
    const handler = getHandler(tools, "parse_disk_node_to_workspace_body");

    const response = (await handler({
      diskNode: aliasedDisk,
      yaml: yamlOfDifferentNode,
    })) as ToolResponse;
    const body = parseStructured(response);

    expect((body.cloudBody as Record<string, unknown>).name).toBe("FROM_DISK_NODE");
  });

  it("returns an error when the YAML parses to a non-object", async () => {
    const tools = defineRenderNodeTools(createServer(), createMockClient()) as unknown as ToolEntry[];
    const handler = getHandler(tools, "parse_disk_node_to_workspace_body");

    const response = (await handler({ yaml: "- 1\n- 2\n" })) as ToolResponse;

    expect(response.isError).toBe(true);
    expect(JSON.stringify(parseStructured(response))).toContain("did not parse to an object");
  });

  it("returns an error when the YAML cannot be parsed at all", async () => {
    const tools = defineRenderNodeTools(createServer(), createMockClient()) as unknown as ToolEntry[];
    const handler = getHandler(tools, "parse_disk_node_to_workspace_body");

    // Unbalanced bracket — yaml library throws.
    const response = (await handler({ yaml: "name: [unbalanced" })) as ToolResponse;

    expect(response.isError).toBe(true);
  });
});
