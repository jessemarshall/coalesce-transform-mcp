import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { defineNodeTypeCorpusTools } from "../../src/mcp/node-type-corpus.js";
import * as corpusLoader from "../../src/services/corpus/loader.js";
import type { CoalesceClient } from "../../src/client.js";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type {
  NodeTypeCorpusSnapshot,
  NodeTypeCorpusVariant,
} from "../../src/services/corpus/loader.js";

type ToolEntry = [string, unknown, (...args: unknown[]) => Promise<unknown>];

function getHandler(tools: unknown[], name: string) {
  const entry = (tools as ToolEntry[]).find((t) => t[0] === name);
  if (!entry) throw new Error(`Tool "${name}" not registered`);
  return entry[2];
}

async function callHandler(handler: (...args: unknown[]) => Promise<unknown>, params: unknown) {
  const response = (await handler(params)) as {
    content: Array<{ type: string; text: string }>;
    structuredContent?: unknown;
    isError?: boolean;
  };
  return response;
}

function parseResponse(response: {
  content: Array<{ type: string; text: string }>;
  structuredContent?: unknown;
  isError?: boolean;
}) {
  const textEntry = response.content.find((c) => c.type === "text");
  if (!textEntry) throw new Error("No text content in response");
  return JSON.parse(textEntry.text);
}

function makeSupportedVariant(overrides: Partial<NodeTypeCorpusVariant> = {}): NodeTypeCorpusVariant {
  return {
    variantKey: "Stage:::abc123",
    normalizedFamily: "Stage",
    packageNames: ["@coalesce/core"],
    occurrenceCount: 3,
    occurrences: [],
    definitionHash: "hash-def",
    createHash: "hash-create",
    runHash: "hash-run",
    primitiveSignature: ["tabular", "materializationSelector"],
    controlSignature: ["textbox"],
    unsupportedPrimitives: [],
    supportStatus: "supported",
    definitionSummary: {
      capitalized: "Stage",
      short: "STG",
      plural: "Stages",
      tagColor: "#abcdef",
      deployStrategy: "recreate",
      configGroupCount: 1,
      configItemCount: 1,
    },
    outerDefinition: {
      fileVersion: 1,
      id: "stage-node-id",
      isDisabled: false,
      name: "Stage",
      type: "Stage",
    },
    nodeMetadataSpec: "",
    nodeDefinition: {
      capitalized: "Stage",
      short: "STG",
      plural: "Stages",
      tagColor: "#abcdef",
      deployStrategy: "recreate",
      config: [
        {
          groupName: "General",
          items: [
            {
              type: "textbox",
              displayName: "Node Name",
              attributeName: "nodeName",
              default: "",
            },
          ],
        },
      ],
    },
    parseError: null,
    ...overrides,
  };
}

function makeSnapshot(variants: NodeTypeCorpusVariant[]): NodeTypeCorpusSnapshot {
  return {
    generatedAt: "2026-04-17T00:00:00Z",
    sourceRoot: "/mock/source",
    packageCount: 2,
    definitionCount: variants.length,
    uniqueVariantCount: variants.length,
    uniqueNormalizedFamilyCount: new Set(variants.map((v) => v.normalizedFamily)).size,
    supportedVariantCount: variants.filter((v) => v.supportStatus === "supported").length,
    partialVariantCount: variants.filter((v) => v.supportStatus === "partial").length,
    parseErrorVariantCount: variants.filter((v) => v.supportStatus === "parse_error").length,
    variants,
  };
}

function makeMockClient(getImpl: (path: string) => Promise<unknown> = async () => ({})) {
  return {
    get: vi.fn(getImpl),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  } as unknown as CoalesceClient;
}

const mockServer = {} as McpServer;

describe("defineNodeTypeCorpusTools — search_node_type_variants", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns summary plus all variants when no filters are given", async () => {
    const variants = [
      makeSupportedVariant({ variantKey: "Stage:::1", normalizedFamily: "Stage" }),
      makeSupportedVariant({ variantKey: "View:::2", normalizedFamily: "View" }),
    ];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(tools, "search_node_type_variants");
    const result = parseResponse(await callHandler(handler, {}));

    expect(result.summary.uniqueVariantCount).toBe(2);
    expect(result.summary.topFamilies).toEqual(
      expect.arrayContaining([
        { normalizedFamily: "Stage", variantCount: 1 },
        { normalizedFamily: "View", variantCount: 1 },
      ])
    );
    expect(result.matchedCount).toBe(2);
    expect(result.returnedCount).toBe(2);
    expect(result.matches.map((m: { variantKey: string }) => m.variantKey)).toEqual([
      "Stage:::1",
      "View:::2",
    ]);
  });

  it("filters by normalizedFamily case-insensitively", async () => {
    const variants = [
      makeSupportedVariant({ variantKey: "Stage:::1", normalizedFamily: "Stage" }),
      makeSupportedVariant({ variantKey: "View:::2", normalizedFamily: "View" }),
    ];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(tools, "search_node_type_variants");
    const result = parseResponse(await callHandler(handler, { normalizedFamily: "stage" }));

    expect(result.matchedCount).toBe(1);
    expect(result.matches[0].variantKey).toBe("Stage:::1");
  });

  it("filters by supportStatus", async () => {
    const variants = [
      makeSupportedVariant({ variantKey: "Stage:::1", supportStatus: "supported" }),
      makeSupportedVariant({
        variantKey: "Custom:::2",
        supportStatus: "partial",
        unsupportedPrimitives: ["unknownThing"],
      }),
    ];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(tools, "search_node_type_variants");
    const result = parseResponse(await callHandler(handler, { supportStatus: "partial" }));

    expect(result.matchedCount).toBe(1);
    expect(result.matches[0].variantKey).toBe("Custom:::2");
  });

  it("clamps limit to max 200 and min 1", async () => {
    const variants = Array.from({ length: 5 }, (_, i) =>
      makeSupportedVariant({ variantKey: `Stage:::${i}`, normalizedFamily: "Stage" })
    );
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(tools, "search_node_type_variants");

    const resWith2 = parseResponse(await callHandler(handler, { limit: 2 }));
    expect(resWith2.returnedCount).toBe(2);
    expect(resWith2.matches).toHaveLength(2);
    expect(resWith2.matchedCount).toBe(5);
  });

  it("filters overrideSQLToggle controls out of returned primitive signature", async () => {
    const variants = [
      makeSupportedVariant({
        variantKey: "Stage:::1",
        primitiveSignature: ["tabular", "overrideSQLToggle", "materializationSelector"],
      }),
    ];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(tools, "search_node_type_variants");
    const result = parseResponse(await callHandler(handler, {}));

    expect(result.matches[0].primitiveSignature).not.toContain("overrideSQLToggle");
    expect(result.matches[0].primitiveSignature).toEqual(["tabular", "materializationSelector"]);
  });
});

describe("defineNodeTypeCorpusTools — get_node_type_variant", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns the variant with its sanitized nodeMetadataSpec rendered to YAML", async () => {
    const variants = [makeSupportedVariant({ variantKey: "Stage:::1" })];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(tools, "get_node_type_variant");
    const result = parseResponse(await callHandler(handler, { variantKey: "Stage:::1" }));

    expect(result.variantKey).toBe("Stage:::1");
    expect(result.nodeDefinition).toBeDefined();
    expect(typeof result.nodeMetadataSpec).toBe("string");
    expect(result.nodeMetadataSpec.length).toBeGreaterThan(0);
    expect(Array.isArray(result.warnings)).toBe(true);
  });

  it("returns an error response when variantKey is not found", async () => {
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot([]));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(tools, "get_node_type_variant");
    const response = await callHandler(handler, { variantKey: "DoesNotExist:::x" });

    expect(response.isError).toBe(true);
    const text = response.content.map((c) => c.text).join("\n");
    expect(text).toContain("DoesNotExist:::x");
  });

  it("still returns variant metadata when nodeDefinition is null (parse_error)", async () => {
    const variants = [
      makeSupportedVariant({
        variantKey: "Broken:::1",
        supportStatus: "parse_error",
        nodeDefinition: null,
        parseError: "bad yaml",
      }),
    ];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(tools, "get_node_type_variant");
    const result = parseResponse(await callHandler(handler, { variantKey: "Broken:::1" }));

    expect(result.variantKey).toBe("Broken:::1");
    expect(result.nodeDefinition).toBeNull();
    expect(result.parseError).toBe("bad yaml");
  });
});

describe("defineNodeTypeCorpusTools — generate_set_workspace_node_template_from_variant", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("generates a template from a supported variant", async () => {
    const variants = [makeSupportedVariant({ variantKey: "Stage:::1" })];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const result = parseResponse(
      await callHandler(handler, {
        variantKey: "Stage:::1",
        nodeName: "my_stage",
      })
    );

    expect(result.variant.variantKey).toBe("Stage:::1");
    expect(result.setWorkspaceNodeBodyTemplate).toBeDefined();
    expect(typeof result.setWorkspaceNodeBodyTemplateYaml).toBe("string");
    expect(result.setWorkspaceNodeBodyTemplateYaml.length).toBeGreaterThan(0);
    expect(Array.isArray(result.warnings)).toBe(true);
    expect(result.comparison).toBeUndefined();
  });

  it("rejects a partial variant when allowPartial is not set", async () => {
    const variants = [
      makeSupportedVariant({
        variantKey: "Custom:::partial",
        supportStatus: "partial",
        unsupportedPrimitives: ["exoticControl"],
      }),
    ];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const response = await callHandler(handler, { variantKey: "Custom:::partial" });

    expect(response.isError).toBe(true);
    const text = response.content.map((c) => c.text).join("\n");
    expect(text).toContain("partially supported");
    expect(text).toContain("allowPartial=true");
  });

  it("accepts a partial variant when allowPartial=true and emits a best-effort warning", async () => {
    const variants = [
      makeSupportedVariant({
        variantKey: "Custom:::partial",
        supportStatus: "partial",
        unsupportedPrimitives: ["exoticControl"],
      }),
    ];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const result = parseResponse(
      await callHandler(handler, {
        variantKey: "Custom:::partial",
        allowPartial: true,
      })
    );

    expect(result.variant.variantKey).toBe("Custom:::partial");
    expect(
      result.warnings.some((w: string) => w.includes("Best-effort template only"))
    ).toBe(true);
    expect(
      result.warnings.some((w: string) => w.includes("exoticControl"))
    ).toBe(true);
  });

  it("rejects workspaceID without nodeID", async () => {
    const variants = [makeSupportedVariant({ variantKey: "Stage:::1" })];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const response = await callHandler(handler, {
      variantKey: "Stage:::1",
      workspaceID: "ws-1",
    });

    expect(response.isError).toBe(true);
    const text = response.content.map((c) => c.text).join("\n");
    expect(text).toContain("workspaceID and nodeID must be provided together");
  });

  it("rejects nodeID without workspaceID", async () => {
    const variants = [makeSupportedVariant({ variantKey: "Stage:::1" })];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const response = await callHandler(handler, {
      variantKey: "Stage:::1",
      nodeID: "node-1",
    });

    expect(response.isError).toBe(true);
    const text = response.content.map((c) => c.text).join("\n");
    expect(text).toContain("workspaceID and nodeID must be provided together");
    expect(mockServer).toBeDefined();
  });

  it("deduplicates warnings when generator emits a message for a supported variant", async () => {
    const variants = [
      makeSupportedVariant({
        variantKey: "Stage:::withDupWarnings",
        nodeDefinition: {
          capitalized: "Stage",
          short: "STG",
          plural: "Stages",
          tagColor: "#abcdef",
          deployStrategy: "recreate",
          config: [
            {
              groupName: "General",
              items: [
                {
                  type: "textbox",
                  displayName: "Node Name",
                  attributeName: "nodeName",
                  default: "",
                },
                {
                  type: "unknownPrimitive",
                  displayName: "Weird",
                  attributeName: "weird",
                },
                {
                  type: "unknownPrimitive",
                  displayName: "Weird2",
                  attributeName: "weird2",
                },
              ],
            },
          ],
        },
      }),
    ];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const result = parseResponse(
      await callHandler(handler, { variantKey: "Stage:::withDupWarnings" })
    );

    expect(Array.isArray(result.warnings)).toBe(true);
    const uniqueWarnings = new Set(result.warnings);
    expect(uniqueWarnings.size).toBe(result.warnings.length);
  });

  it("surfaces the defensive error when sanitization produces a non-object definition", async () => {
    const variants = [makeSupportedVariant({ variantKey: "Stage:::broken" })];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));
    const sqlOverridePolicyModule = await import(
      "../../src/services/policies/sql-override.js"
    );
    vi.spyOn(sqlOverridePolicyModule, "sanitizeNodeDefinitionSqlOverridePolicy").mockReturnValue({
      nodeDefinition: null as unknown as Record<string, unknown>,
      warnings: [],
    });

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const response = await callHandler(handler, { variantKey: "Stage:::broken" });

    expect(response.isError).toBe(true);
    const text = response.content.map((c) => c.text).join("\n");
    expect(text).toContain("Sanitized node definition was not an object");
  });

  it("rejects a parse_error variant with an actionable error", async () => {
    const variants = [
      makeSupportedVariant({
        variantKey: "Broken:::1",
        supportStatus: "parse_error",
        nodeDefinition: null,
        parseError: "yaml parse error at line 3",
      }),
    ];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const tools = defineNodeTypeCorpusTools(mockServer, makeMockClient());
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const response = await callHandler(handler, { variantKey: "Broken:::1" });

    expect(response.isError).toBe(true);
    const text = response.content.map((c) => c.text).join("\n");
    expect(text).toContain("could not be parsed");
    expect(text).toContain("yaml parse error at line 3");
  });

  it("performs a live comparison when workspaceID and nodeID are both provided", async () => {
    const variants = [makeSupportedVariant({ variantKey: "Stage:::1" })];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const mockClient = makeMockClient(async () => ({
      id: "node-1",
      name: "existing_stage",
      nodeType: "Stage",
      config: {},
    }));

    const tools = defineNodeTypeCorpusTools(mockServer, mockClient);
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const result = parseResponse(
      await callHandler(handler, {
        variantKey: "Stage:::1",
        workspaceID: "ws-1",
        nodeID: "node-1",
      })
    );

    expect(result.comparison).toBeDefined();
    expect(result.comparison.workspaceID).toBe("ws-1");
    expect(result.comparison.nodeID).toBe("node-1");
    expect(result.comparison.result).toBeDefined();
    expect(mockClient.get).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/nodes/node-1",
      {}
    );
  });

  it("surfaces client errors from the live comparison fetch", async () => {
    const variants = [makeSupportedVariant({ variantKey: "Stage:::1" })];
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue(makeSnapshot(variants));

    const mockClient = makeMockClient(async () => {
      throw new Error("404 Not Found");
    });

    const tools = defineNodeTypeCorpusTools(mockServer, mockClient);
    const handler = getHandler(
      tools,
      "generate_set_workspace_node_template_from_variant"
    );
    const response = await callHandler(handler, {
      variantKey: "Stage:::1",
      workspaceID: "ws-1",
      nodeID: "node-missing",
    });

    expect(response.isError).toBe(true);
    const text = response.content.map((c) => c.text).join("\n");
    expect(text).toContain("404 Not Found");
  });
});
