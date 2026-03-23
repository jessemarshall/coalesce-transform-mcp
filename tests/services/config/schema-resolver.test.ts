import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { resolve } from "node:path";
import { resolveNodeTypeSchema } from "../../../src/services/config/schema-resolver.js";
import * as corpusLoader from "../../../src/services/corpus/loader.js";

const fixtureRepoPath = resolve("tests/fixtures/repo-backed-coalesce");

describe("resolveNodeTypeSchema", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("resolves schema from repo when repoPath provided", async () => {
    const result = await resolveNodeTypeSchema("DataVault:::33", fixtureRepoPath);

    expect(result.source).toBe("repo");
    expect(result.schema.config).toBeDefined();
    expect(result.schema.config[0].items).toBeDefined();
  });

  it("resolves Stage schema from repo", async () => {
    const result = await resolveNodeTypeSchema("Stage:::Stage", fixtureRepoPath);

    expect(result.source).toBe("repo");
    expect(result.schema.config).toBeDefined();
  });

  it("falls back to corpus when repo resolution fails", async () => {
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue({
      generatedAt: "2024-01-01",
      sourceRoot: "/mock",
      packageCount: 1,
      definitionCount: 1,
      uniqueVariantCount: 1,
      uniqueNormalizedFamilyCount: 1,
      supportedVariantCount: 1,
      partialVariantCount: 0,
      parseErrorVariantCount: 0,
      variants: [{
        variantKey: "work-v1",
        normalizedFamily: "work",
        packageNames: ["test-package"],
        occurrenceCount: 1,
        occurrences: [],
        definitionHash: "hash1",
        createHash: "hash2",
        runHash: "hash3",
        primitiveSignature: [],
        controlSignature: [],
        unsupportedPrimitives: [],
        supportStatus: "supported",
        definitionSummary: {
          capitalized: "Work",
          short: "WRK",
          plural: "Works",
          tagColor: "#2EB67D",
          deployStrategy: null,
          configGroupCount: 1,
          configItemCount: 2,
        },
        outerDefinition: {
          fileVersion: 1,
          id: "Work",
          isDisabled: false,
          name: "Work",
          type: "Work",
        },
        nodeMetadataSpec: "",
        nodeDefinition: {
          config: [{
            groupName: "Options",
            items: [
              { type: "materializationSelector", default: "table" },
              { attributeName: "selectDistinct", type: "toggleButton", default: false },
            ],
          }],
        },
        parseError: null,
      }],
    });

    const result = await resolveNodeTypeSchema("work");

    expect(result.source).toBe("corpus");
    expect(result.schema.config).toBeDefined();
  });

  it("normalizes node type families when searching corpus", async () => {
    vi.spyOn(corpusLoader, "loadNodeTypeCorpusSnapshot").mockReturnValue({
      generatedAt: "2024-01-01",
      sourceRoot: "/mock",
      packageCount: 1,
      definitionCount: 1,
      uniqueVariantCount: 1,
      uniqueNormalizedFamilyCount: 1,
      supportedVariantCount: 1,
      partialVariantCount: 0,
      parseErrorVariantCount: 0,
      variants: [{
        variantKey: "customwork-v1",
        normalizedFamily: "customwork",
        packageNames: ["test-package"],
        occurrenceCount: 1,
        occurrences: [],
        definitionHash: "hash1",
        createHash: "hash2",
        runHash: "hash3",
        primitiveSignature: [],
        controlSignature: [],
        unsupportedPrimitives: [],
        supportStatus: "supported",
        definitionSummary: {
          capitalized: "Custom Work",
          short: "CWRK",
          plural: "Custom Works",
          tagColor: "#E01E5A",
          deployStrategy: null,
          configGroupCount: 1,
          configItemCount: 1,
        },
        outerDefinition: {
          fileVersion: 1,
          id: "CustomWork",
          isDisabled: false,
          name: "Custom Work",
          type: "CustomWork",
        },
        nodeMetadataSpec: "",
        nodeDefinition: {
          config: [{
            groupName: "Options",
            items: [
              { type: "materializationSelector", default: "view" },
            ],
          }],
        },
        parseError: null,
      }],
    });

    const result = await resolveNodeTypeSchema("Custom Work");

    expect(result.source).toBe("corpus");
    expect(result.schema.config).toBeDefined();
  });
});
