import { describe, it, expect, vi, beforeEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerNodeTools } from "../../src/mcp/nodes.js";

// Mock the intelligent config module
vi.mock("../../src/services/config/intelligent.js", () => ({
  completeNodeConfiguration: vi.fn().mockResolvedValue({
    node: {
      id: "node-123",
      nodeType: "Dimension",
      config: {
        insertStrategy: "UNION",
        selectDistinct: false,
        truncateBefore: false,
      },
    },
    schemaSource: "corpus",
    classification: {
      required: [],
      conditionalRequired: [],
      optionalWithDefaults: ["insertStrategy", "selectDistinct", "truncateBefore"],
      contextual: [],
    },
    context: {
      hasMultipleSources: true,
      hasAggregates: true,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table",
    },
    appliedConfig: {
      insertStrategy: "UNION",
      selectDistinct: false,
      truncateBefore: false,
    },
    configChanges: {
      required: {},
      contextual: {
        insertStrategy: "UNION",
        selectDistinct: false,
        truncateBefore: false,
      },
      preserved: {},
    },
    reasoning: [
      "Multi-source node with aggregates suggests UNION to avoid duplicate aggregated rows",
      "Aggregates are incompatible with SELECT DISTINCT; suggests selectDistinct: false",
      "Table materialization suggests truncateBefore: false to preserve existing data",
    ],
    detectedPatterns: {
      candidateColumns: ["CUSTOMER_ID", "ORDER_ID"],
    },
  }),
}));

describe("complete-node-configuration tool", () => {
  it("registers complete-node-configuration tool", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const mockClient = {
      get: vi.fn(),
      post: vi.fn(),
      put: vi.fn(),
      delete: vi.fn(),
    };

    registerNodeTools(server, mockClient as any);

    const toolCall = toolSpy.mock.calls.find(
      (call) => call[0] === "coalesce_complete_node_configuration"
    );

    expect(toolCall).toBeDefined();
    expect(toolCall?.[1]?.description).toContain("intelligent configuration completion");
  });

  it("executes complete-node-configuration and returns structured result", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const mockClient = {
      get: vi.fn(),
      post: vi.fn(),
      put: vi.fn(),
      delete: vi.fn(),
    };

    registerNodeTools(server, mockClient as any);

    const toolCall = toolSpy.mock.calls.find(
      (call) => call[0] === "coalesce_complete_node_configuration"
    );
    const handler = toolCall?.[2] as
      | ((params: { workspaceID: string; nodeID: string; repoPath?: string }) => Promise<{ content: { text: string }[] }>)
      | undefined;

    expect(typeof handler).toBe("function");

    const result = await handler!({
      workspaceID: "ws-1",
      nodeID: "node-123",
    });

    expect(result.content).toBeDefined();
    expect(result.content[0].type).toBe("text");

    const response = JSON.parse((result.content[0] as any).text);
    expect(response).toHaveProperty("node");
    expect(response).toHaveProperty("schemaSource");
    expect(response).toHaveProperty("classification");
    expect(response).toHaveProperty("context");
    expect(response).toHaveProperty("appliedConfig");
    expect(response).toHaveProperty("configChanges");
    expect(response).toHaveProperty("reasoning");
    expect(response).toHaveProperty("detectedPatterns");

    // Verify reasoning is included
    expect(response.reasoning).toContain(
      "Multi-source node with aggregates suggests UNION to avoid duplicate aggregated rows"
    );

    // Verify config was applied
    expect(response.appliedConfig.insertStrategy).toBe("UNION");
  });
});
