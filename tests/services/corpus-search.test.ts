import { describe, it, expect } from "vitest";
import {
  summarizeNodeTypeCorpus,
  buildVariantSummary,
  searchNodeTypeCorpusVariants,
  getNodeTypeCorpusVariant,
} from "../../src/services/corpus/search.js";
import type {
  NodeTypeCorpusSnapshot,
  NodeTypeCorpusVariant,
} from "../../src/services/corpus/loader.js";

// --- Helpers ---

function makeVariant(overrides: Partial<NodeTypeCorpusVariant> = {}): NodeTypeCorpusVariant {
  return {
    variantKey: "Stage-Stage",
    normalizedFamily: "Stage",
    packageNames: ["default-package"],
    occurrenceCount: 1,
    occurrences: [],
    definitionHash: "abc123",
    createHash: "def456",
    runHash: "ghi789",
    primitiveSignature: ["create", "run"],
    controlSignature: [],
    unsupportedPrimitives: [],
    supportStatus: "supported",
    definitionSummary: {
      capitalized: "Stage",
      short: "STG",
      plural: "Stages",
      tagColor: "#0000FF",
      deployStrategy: "CREATE OR REPLACE",
      configGroupCount: 1,
      configItemCount: 5,
    },
    outerDefinition: {
      fileVersion: 1,
      id: "Stage",
      isDisabled: false,
      name: "Stage",
      type: "Stage",
    },
    nodeMetadataSpec: "config:\n  - groupName: Options",
    nodeDefinition: { config: [] },
    parseError: null,
    ...overrides,
  };
}

function makeSnapshot(variants: NodeTypeCorpusVariant[]): NodeTypeCorpusSnapshot {
  return {
    generatedAt: "2026-04-20T00:00:00.000Z",
    packageCount: 3,
    definitionCount: variants.length,
    uniqueVariantCount: variants.length,
    uniqueNormalizedFamilyCount: new Set(variants.map((v) => v.normalizedFamily)).size,
    supportedVariantCount: variants.filter((v) => v.supportStatus === "supported").length,
    partialVariantCount: variants.filter((v) => v.supportStatus === "partial").length,
    parseErrorVariantCount: variants.filter((v) => v.supportStatus === "parse_error").length,
    variants,
  };
}

// --- Tests ---

describe("summarizeNodeTypeCorpus", () => {
  it("returns snapshot metadata and top families sorted by count descending", () => {
    const variants = [
      makeVariant({ variantKey: "Stage-1", normalizedFamily: "Stage" }),
      makeVariant({ variantKey: "Stage-2", normalizedFamily: "Stage" }),
      makeVariant({ variantKey: "Dim-1", normalizedFamily: "Dimension" }),
      makeVariant({ variantKey: "Fact-1", normalizedFamily: "Fact" }),
      makeVariant({ variantKey: "Fact-2", normalizedFamily: "Fact" }),
      makeVariant({ variantKey: "Fact-3", normalizedFamily: "Fact" }),
    ];
    const snapshot = makeSnapshot(variants);
    const summary = summarizeNodeTypeCorpus(snapshot);

    expect(summary.generatedAt).toBe(snapshot.generatedAt);
    expect(summary.uniqueVariantCount).toBe(6);
    expect(summary.topFamilies).toHaveLength(3);
    // Fact: 3, Stage: 2, Dimension: 1
    expect(summary.topFamilies[0]).toEqual({ normalizedFamily: "Fact", variantCount: 3 });
    expect(summary.topFamilies[1]).toEqual({ normalizedFamily: "Stage", variantCount: 2 });
    expect(summary.topFamilies[2]).toEqual({ normalizedFamily: "Dimension", variantCount: 1 });
  });

  it("breaks ties in family counts by alphabetical order", () => {
    const variants = [
      makeVariant({ variantKey: "B-1", normalizedFamily: "Bravo" }),
      makeVariant({ variantKey: "A-1", normalizedFamily: "Alpha" }),
    ];
    const snapshot = makeSnapshot(variants);
    const summary = summarizeNodeTypeCorpus(snapshot);

    // Both have count=1, so alphabetical: Alpha before Bravo
    expect(summary.topFamilies[0].normalizedFamily).toBe("Alpha");
    expect(summary.topFamilies[1].normalizedFamily).toBe("Bravo");
  });

  it("limits top families to 20", () => {
    const variants = Array.from({ length: 25 }, (_, i) =>
      makeVariant({ variantKey: `V-${i}`, normalizedFamily: `Family-${String(i).padStart(2, "0")}` })
    );
    const snapshot = makeSnapshot(variants);
    const summary = summarizeNodeTypeCorpus(snapshot);

    expect(summary.topFamilies).toHaveLength(20);
  });

  it("handles empty snapshot", () => {
    const snapshot = makeSnapshot([]);
    const summary = summarizeNodeTypeCorpus(snapshot);

    expect(summary.topFamilies).toHaveLength(0);
    expect(summary.uniqueVariantCount).toBe(0);
  });
});

describe("buildVariantSummary", () => {
  it("returns the expected subset of variant fields", () => {
    const variant = makeVariant({
      primitiveSignature: ["create", "run", "overrideSQLToggle"],
      controlSignature: ["overrideSQLToggle"],
      unsupportedPrimitives: ["overrideSQLToggle"],
    });
    const summary = buildVariantSummary(variant);

    expect(summary.variantKey).toBe(variant.variantKey);
    expect(summary.normalizedFamily).toBe(variant.normalizedFamily);
    expect(summary.packageNames).toEqual(variant.packageNames);
    expect(summary.occurrenceCount).toBe(variant.occurrenceCount);
    expect(summary.supportStatus).toBe(variant.supportStatus);
    expect(summary.definitionSummary).toEqual(variant.definitionSummary);
  });

  it("filters overrideSQLToggle from primitive/control/unsupported signatures", () => {
    const variant = makeVariant({
      primitiveSignature: ["create", "overrideSQLToggle", "run"],
      controlSignature: ["overrideSQLToggle"],
      unsupportedPrimitives: ["overrideSQLToggle", "customPrimitive"],
    });
    const summary = buildVariantSummary(variant);

    expect(summary.primitiveSignature).toEqual(["create", "run"]);
    expect(summary.controlSignature).toEqual([]);
    expect(summary.unsupportedPrimitives).toEqual(["customPrimitive"]);
  });
});

describe("searchNodeTypeCorpusVariants", () => {
  const stageVariant = makeVariant({
    variantKey: "Stage-Stage",
    normalizedFamily: "Stage",
    packageNames: ["default-package"],
    supportStatus: "supported",
    primitiveSignature: ["create", "run"],
  });
  const dimVariant = makeVariant({
    variantKey: "Dimension-Dim",
    normalizedFamily: "Dimension",
    packageNames: ["analytics-package"],
    supportStatus: "supported",
    primitiveSignature: ["create", "run", "merge"],
  });
  const factVariant = makeVariant({
    variantKey: "Fact-Fact",
    normalizedFamily: "Fact",
    packageNames: ["analytics-package"],
    supportStatus: "partial",
    primitiveSignature: ["create"],
  });
  const errorVariant = makeVariant({
    variantKey: "Broken-X",
    normalizedFamily: "Broken",
    packageNames: ["broken-package"],
    supportStatus: "parse_error",
    primitiveSignature: [],
  });
  const snapshot = makeSnapshot([stageVariant, dimVariant, factVariant, errorVariant]);

  it("returns all variants when no filters are provided", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, {});
    expect(result.matchedCount).toBe(4);
    expect(result.returnedCount).toBe(4);
  });

  it("filters by normalizedFamily (case-insensitive)", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, { normalizedFamily: "stage" });
    expect(result.matchedCount).toBe(1);
    expect(result.matches[0].variantKey).toBe("Stage-Stage");
  });

  it("filters by packageName (case-insensitive)", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, { packageName: "Analytics-Package" });
    expect(result.matchedCount).toBe(2);
    expect(result.matches.map((m) => m.variantKey).sort()).toEqual(["Dimension-Dim", "Fact-Fact"]);
  });

  it("filters by primitive (case-insensitive)", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, { primitive: "MERGE" });
    expect(result.matchedCount).toBe(1);
    expect(result.matches[0].variantKey).toBe("Dimension-Dim");
  });

  it("filters by supportStatus", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, { supportStatus: "partial" });
    expect(result.matchedCount).toBe(1);
    expect(result.matches[0].variantKey).toBe("Fact-Fact");
  });

  it("combines multiple filters with AND logic", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, {
      packageName: "analytics-package",
      supportStatus: "supported",
    });
    expect(result.matchedCount).toBe(1);
    expect(result.matches[0].variantKey).toBe("Dimension-Dim");
  });

  it("returns empty when no variants match", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, { normalizedFamily: "nonexistent" });
    expect(result.matchedCount).toBe(0);
    expect(result.returnedCount).toBe(0);
    expect(result.matches).toEqual([]);
  });

  it("respects limit parameter", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, { limit: 2 });
    expect(result.matchedCount).toBe(4);
    expect(result.returnedCount).toBe(2);
    expect(result.matches).toHaveLength(2);
  });

  it("clamps limit to minimum of 1", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, { limit: 0 });
    expect(result.returnedCount).toBe(1);
  });

  it("clamps limit to maximum of 200", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, { limit: 500 });
    expect(result.returnedCount).toBe(4); // all 4 variants returned (under 200)
  });

  it("defaults limit to 25 when not specified", () => {
    const manyVariants = Array.from({ length: 30 }, (_, i) =>
      makeVariant({ variantKey: `V-${i}`, normalizedFamily: "Stage" })
    );
    const bigSnapshot = makeSnapshot(manyVariants);
    const result = searchNodeTypeCorpusVariants(bigSnapshot, {});
    expect(result.matchedCount).toBe(30);
    expect(result.returnedCount).toBe(25);
  });

  it("trims whitespace from filter values", () => {
    const result = searchNodeTypeCorpusVariants(snapshot, { normalizedFamily: "  Stage  " });
    expect(result.matchedCount).toBe(1);
    expect(result.matches[0].variantKey).toBe("Stage-Stage");
  });
});

describe("getNodeTypeCorpusVariant", () => {
  const variant = makeVariant({ variantKey: "Stage-Stage" });
  const snapshot = makeSnapshot([variant]);

  it("returns the variant matching the variantKey", () => {
    const result = getNodeTypeCorpusVariant(snapshot, "Stage-Stage");
    expect(result.variantKey).toBe("Stage-Stage");
    expect(result.normalizedFamily).toBe("Stage");
  });

  it("throws when variantKey is not found", () => {
    expect(() => getNodeTypeCorpusVariant(snapshot, "nonexistent")).toThrow(
      "No node type corpus variant found for variantKey nonexistent"
    );
  });
});
