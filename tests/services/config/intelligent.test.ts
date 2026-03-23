import { describe, it, expect, vi, beforeEach } from "vitest";
import { completeNodeConfiguration } from "../../../src/services/config/intelligent.js";

// Mock the schema resolver
vi.mock("../../../src/services/config/schema-resolver.js", () => ({
  resolveNodeTypeSchema: vi.fn().mockResolvedValue({
    source: "corpus",
    schema: {
      config: [
        {
          groupName: "General Options",
          items: [
            { attributeName: "insertStrategy", type: "string", isRequired: false, default: "UNION ALL" },
            { attributeName: "selectDistinct", type: "boolean", isRequired: false, default: false },
            { attributeName: "truncateBefore", type: "boolean", isRequired: false, default: false },
          ],
        },
      ],
    },
  }),
}));

describe("completeNodeConfiguration - Basic Structure", () => {
  it("returns complete result structure with schema, context, and classification", async () => {
    const mockNode = {
      id: "node-123",
      nodeType: "Dimension",
      config: {},
      metadata: {
        columns: [
          { name: "CUSTOMER_ID", transform: "CUSTOMER_ID" },
          { name: "CUSTOMER_NAME", transform: "CUSTOMER_NAME" },
        ],
        sourceMapping: [],
      },
    };

    const mockClient = {
      get: vi.fn().mockResolvedValue(mockNode),
      put: vi.fn().mockResolvedValue({
        ...mockNode,
        config: {
          insertStrategy: "UNION ALL",
          selectDistinct: false,
          truncateBefore: false,
        },
      }),
    } as any;

    const result = await completeNodeConfiguration(mockClient, {
      workspaceID: "ws-1",
      nodeID: "node-123",
    });

    // Verify structure
    expect(result).toHaveProperty("node");
    expect(result).toHaveProperty("schemaSource");
    expect(result).toHaveProperty("classification");
    expect(result).toHaveProperty("context");
    expect(result).toHaveProperty("appliedConfig");
    expect(result).toHaveProperty("reasoning");
    expect(result).toHaveProperty("detectedPatterns");

    // Verify classification structure
    expect(result.classification).toHaveProperty("required");
    expect(result.classification).toHaveProperty("conditionalRequired");
    expect(result.classification).toHaveProperty("optionalWithDefaults");
    expect(result.classification).toHaveProperty("contextual");

    // Verify context structure
    expect(result.context).toHaveProperty("hasMultipleSources");
    expect(result.context).toHaveProperty("hasAggregates");
    expect(result.context).toHaveProperty("hasTimestampColumns");
    expect(result.context).toHaveProperty("hasType2Pattern");
    expect(result.context).toHaveProperty("materializationType");

    // Verify detected patterns
    expect(result.detectedPatterns).toHaveProperty("candidateColumns");
    expect(result.detectedPatterns.candidateColumns).toContain("CUSTOMER_ID");

    // Verify reasoning is an array
    expect(Array.isArray(result.reasoning)).toBe(true);
  });

  it("applies config changes and tracks required/contextual/preserved fields", async () => {
    const mockClient = {
      get: vi.fn().mockResolvedValue({
        id: "node-123",
        nodeType: "Dimension",
        config: {
          existingField: "preserved-value",
        },
        metadata: {
          columns: [
            { name: "CUSTOMER_ID", transform: "CUSTOMER_ID" },
            { name: "ORDER_COUNT", transform: "COUNT(ORDER_ID)" },
          ],
          sourceMapping: [
            {
              dependencies: [
                { nodeName: "source1", locationName: "WORK" },
                { nodeName: "source2", locationName: "WORK" },
              ],
            },
          ], // Multiple sources for insertStrategy
        },
      }),
      put: vi.fn().mockResolvedValue({
        id: "node-123",
        nodeType: "Dimension",
        config: {
          existingField: "preserved-value",
          insertStrategy: "UNION",
          selectDistinct: false,
          truncateBefore: false,
        },
        metadata: {
          columns: [
            { name: "CUSTOMER_ID", transform: "CUSTOMER_ID" },
            { name: "ORDER_COUNT", transform: "COUNT(ORDER_ID)" },
          ],
          sourceMapping: [
            {
              dependencies: [
                { nodeName: "source1", locationName: "WORK" },
                { nodeName: "source2", locationName: "WORK" },
              ],
            },
          ],
        },
      }),
    } as any;

    const result = await completeNodeConfiguration(mockClient, {
      workspaceID: "ws-1",
      nodeID: "node-123",
    });

    // Verify config was applied
    expect(mockClient.put).toHaveBeenCalled();

    // Verify configChanges structure
    expect(result.configChanges).toHaveProperty("required");
    expect(result.configChanges).toHaveProperty("contextual");
    expect(result.configChanges).toHaveProperty("preserved");

    // Verify contextual changes include rule suggestions
    expect(result.configChanges.contextual).toHaveProperty("selectDistinct");
    expect(result.configChanges.contextual.selectDistinct).toBe(false);

    // Verify appliedConfig includes all changes
    expect(result.appliedConfig).toMatchObject({
      insertStrategy: "UNION", // UNION because has aggregates
      selectDistinct: false,
      truncateBefore: false,
    });
  });
});
