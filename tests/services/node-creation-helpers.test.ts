import { describe, it, expect } from "vitest";
import {
  suggestNamingConvention,
  shouldSuggestNamingConvention,
  buildFamilyMaterializationGuidance,
  buildPostCreationNextSteps,
} from "../../src/services/workspace/node-creation.js";
import type { JoinSuggestion } from "../../src/services/workspace/join-helpers.js";

// ---------------------------------------------------------------------------
// suggestNamingConvention
// ---------------------------------------------------------------------------
// Naming-convention regex matches below check only the family prefix, not the
// full convention string. This keeps the tests robust to incidental edits in
// the human-readable example portion (e.g., adding/removing example node names)
// while still catching prefix changes that would actually break agent output.
describe("suggestNamingConvention", () => {
  it("returns the stage convention for stage", () => {
    expect(suggestNamingConvention("stage")).toMatch(/STG_/);
  });

  it("returns the dimension convention for dimension", () => {
    expect(suggestNamingConvention("dimension")).toMatch(/DIM_/);
  });

  it("returns the fact convention for fact", () => {
    const convention = suggestNamingConvention("fact");
    expect(convention).toMatch(/FACT_|FCT_/);
  });

  it("returns the view convention for view", () => {
    expect(suggestNamingConvention("view")).toMatch(/V_|INT_/);
  });

  it("returns the work convention for work", () => {
    expect(suggestNamingConvention("work")).toMatch(/INT_|WRK_/);
  });

  it("returns the hub convention for hub", () => {
    expect(suggestNamingConvention("hub")).toMatch(/HUB_/);
  });

  it("returns the satellite convention for satellite", () => {
    expect(suggestNamingConvention("satellite")).toMatch(/SAT_/);
  });

  it("returns the link convention for link", () => {
    expect(suggestNamingConvention("link")).toMatch(/LNK_/);
  });

  it("falls back to a generic message for unknown families", () => {
    expect(suggestNamingConvention("unknown-family")).toBe(
      "Use a descriptive, layer-appropriate name"
    );
  });

  it("falls back to a generic message for empty string", () => {
    expect(suggestNamingConvention("")).toBe(
      "Use a descriptive, layer-appropriate name"
    );
  });
});

// ---------------------------------------------------------------------------
// shouldSuggestNamingConvention
// ---------------------------------------------------------------------------
describe("shouldSuggestNamingConvention", () => {
  it("returns true for empty current name", () => {
    expect(shouldSuggestNamingConvention("", "Stage")).toBe(true);
  });

  it("returns true when current name equals the node type (placeholder default)", () => {
    expect(shouldSuggestNamingConvention("Stage", "Stage")).toBe(true);
    expect(shouldSuggestNamingConvention("Dimension", "Dimension")).toBe(true);
  });

  it("returns true for COA UI placeholder pattern (TYPE_NN)", () => {
    expect(shouldSuggestNamingConvention("STAGE_42", "Stage")).toBe(true);
    expect(shouldSuggestNamingConvention("DIMENSION_7", "Dimension")).toBe(true);
    expect(shouldSuggestNamingConvention("FACT_1", "Fact")).toBe(true);
  });

  it("returns false for an intentional name", () => {
    expect(shouldSuggestNamingConvention("STG_CUSTOMERS", "Stage")).toBe(false);
    expect(shouldSuggestNamingConvention("DIM_PRODUCT", "Dimension")).toBe(false);
    expect(shouldSuggestNamingConvention("FACT_SALES", "Fact")).toBe(false);
  });

  it("returns false for a name with mixed casing or non-placeholder shape", () => {
    expect(shouldSuggestNamingConvention("MyNode", "Stage")).toBe(false);
    expect(shouldSuggestNamingConvention("stg_customers", "Stage")).toBe(false);
    expect(shouldSuggestNamingConvention("STG_CUSTOMERS_V2", "Stage")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// buildFamilyMaterializationGuidance
// ---------------------------------------------------------------------------
describe("buildFamilyMaterializationGuidance", () => {
  it("returns no steps for non-fact/dimension families", () => {
    expect(buildFamilyMaterializationGuidance("stage", 0)).toEqual([]);
    expect(buildFamilyMaterializationGuidance("view", 1)).toEqual([]);
    expect(buildFamilyMaterializationGuidance("work", 2)).toEqual([]);
    expect(buildFamilyMaterializationGuidance("", 0)).toEqual([]);
  });

  it("emits a single materialization step for fact with <= 1 predecessors", () => {
    const steps = buildFamilyMaterializationGuidance("fact", 0);
    expect(steps).toHaveLength(1);
    expect(steps[0]).toMatch(/Verify materialization.*Fact/);

    const stepsOnePred = buildFamilyMaterializationGuidance("fact", 1);
    expect(stepsOnePred).toHaveLength(1);
    expect(stepsOnePred[0]).toMatch(/Verify materialization.*Fact/);
  });

  it("adds grain-definition guidance for multi-predecessor fact nodes", () => {
    const steps = buildFamilyMaterializationGuidance("fact", 2);
    expect(steps).toHaveLength(2);
    expect(steps[0]).toMatch(/Verify materialization.*Fact/);
    expect(steps[1]).toMatch(/define the grain/);
  });

  it("emits materialization + business-key guidance for dimension regardless of predecessor count", () => {
    for (const count of [0, 1, 5]) {
      const steps = buildFamilyMaterializationGuidance("dimension", count);
      expect(steps).toHaveLength(2);
      expect(steps[0]).toMatch(/Verify materialization.*Dimension/);
      expect(steps[1]).toMatch(/business key/);
    }
  });

  it("does not emit grain guidance for dimension nodes (grain is fact-only)", () => {
    const steps = buildFamilyMaterializationGuidance("dimension", 5);
    expect(steps.some((s) => /define the grain/.test(s))).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// buildPostCreationNextSteps
// ---------------------------------------------------------------------------
describe("buildPostCreationNextSteps", () => {
  function joinSuggestion(commonColumnCount: number): JoinSuggestion {
    return {
      leftPredecessorNodeID: "n1",
      leftPredecessorName: "LEFT",
      rightPredecessorNodeID: "n2",
      rightPredecessorName: "RIGHT",
      commonColumns: Array.from({ length: commonColumnCount }, (_, i) => ({
        normalizedName: `COL_${i}`,
        leftColumnName: `COL_${i}`,
        rightColumnName: `COL_${i}`,
      })),
    };
  }

  it("includes a verification step in every result", () => {
    const steps = buildPostCreationNextSteps(0, "Stage", [], { name: "MY_NODE" });
    expect(steps).toContainEqual(expect.stringContaining("get_workspace_node"));
  });

  it("suggests a naming convention when name is empty", () => {
    const steps = buildPostCreationNextSteps(1, "Stage", [], { name: "" });
    const namingStep = steps.find((s) => s.startsWith("Name this node"));
    expect(namingStep).toBeDefined();
    expect(namingStep).toMatch(/STG_/);
  });

  it("suggests a naming convention when name equals the node type", () => {
    const steps = buildPostCreationNextSteps(1, "Stage", [], { name: "Stage" });
    expect(steps.some((s) => s.startsWith("Name this node"))).toBe(true);
  });

  it("suggests a naming convention when name matches placeholder pattern", () => {
    const steps = buildPostCreationNextSteps(1, "Stage", [], { name: "STAGE_42" });
    expect(steps.some((s) => s.startsWith("Name this node"))).toBe(true);
  });

  it("does not suggest a naming convention when the name looks intentional", () => {
    const steps = buildPostCreationNextSteps(1, "Stage", [], {
      name: "STG_CUSTOMERS",
    });
    expect(steps.some((s) => s.startsWith("Name this node"))).toBe(false);
  });

  it("does not suggest a naming convention when name is missing entirely (no string)", () => {
    // missing-name path takes the same branch as empty-string
    const steps = buildPostCreationNextSteps(1, "Stage", [], {});
    expect(steps.some((s) => s.startsWith("Name this node"))).toBe(true);
  });

  it("emits the REQUIRED join step for multi-predecessor nodes", () => {
    const steps = buildPostCreationNextSteps(2, "Stage", [joinSuggestion(0)], {
      name: "STG_X",
    });
    expect(steps.some((s) => s.startsWith("REQUIRED: Set up the join condition"))).toBe(true);
  });

  it("warns when no common columns exist between predecessors", () => {
    const steps = buildPostCreationNextSteps(2, "Stage", [joinSuggestion(0)], {
      name: "STG_X",
    });
    expect(steps.some((s) => s.startsWith("WARNING: No common columns"))).toBe(true);
  });

  it("emits a verify-join-columns step when common columns exist", () => {
    const steps = buildPostCreationNextSteps(2, "Stage", [joinSuggestion(2)], {
      name: "STG_X",
    });
    expect(steps.some((s) => s.startsWith("Verify join columns"))).toBe(true);
    expect(steps.some((s) => s.startsWith("WARNING: No common columns"))).toBe(false);
  });

  it("does not emit join steps for single-predecessor nodes", () => {
    const steps = buildPostCreationNextSteps(1, "Stage", [], { name: "STG_X" });
    expect(steps.some((s) => s.startsWith("REQUIRED: Set up the join"))).toBe(false);
    expect(steps.some((s) => s.startsWith("WARNING: No common columns"))).toBe(false);
  });

  it("emits review-auto-populated-columns guidance for single-predecessor nodes", () => {
    const steps = buildPostCreationNextSteps(1, "Stage", [], { name: "STG_X" });
    expect(steps.some((s) => s.startsWith("Review auto-populated columns"))).toBe(true);
  });

  it("does not emit single-predecessor guidance for multi-predecessor nodes", () => {
    const steps = buildPostCreationNextSteps(2, "Stage", [joinSuggestion(1)], {
      name: "STG_X",
    });
    expect(steps.some((s) => s.startsWith("Review auto-populated columns"))).toBe(false);
  });

  it("adds materialization guidance for fact nodes", () => {
    const steps = buildPostCreationNextSteps(1, "Fact", [], { name: "FACT_SALES" });
    const matStep = steps.find((s) => s.startsWith("Verify materialization"));
    expect(matStep).toBeDefined();
    expect(matStep).toMatch(/Fact/);
  });

  it("adds grain-definition guidance for multi-predecessor fact nodes", () => {
    const steps = buildPostCreationNextSteps(2, "Fact", [joinSuggestion(1)], {
      name: "FACT_SALES",
    });
    expect(steps.some((s) => s.startsWith("For fact tables: define the grain"))).toBe(true);
  });

  it("does not add grain guidance for single-predecessor fact nodes", () => {
    const steps = buildPostCreationNextSteps(1, "Fact", [], { name: "FACT_SALES" });
    expect(steps.some((s) => s.startsWith("For fact tables: define the grain"))).toBe(false);
  });

  it("adds materialization + business-key guidance for dimension nodes", () => {
    const steps = buildPostCreationNextSteps(1, "Dimension", [], { name: "DIM_CUSTOMER" });
    expect(steps.some((s) => s.startsWith("Verify materialization"))).toBe(true);
    expect(steps.some((s) => s.startsWith("For dimensions:"))).toBe(true);
  });

  it("does not add fact/dimension guidance for stage nodes", () => {
    const steps = buildPostCreationNextSteps(1, "Stage", [], { name: "STG_X" });
    expect(steps.some((s) => s.startsWith("Verify materialization"))).toBe(false);
    expect(steps.some((s) => s.startsWith("For dimensions:"))).toBe(false);
    expect(steps.some((s) => s.startsWith("For fact tables:"))).toBe(false);
  });

  it("falls back to the generic naming-convention message for an unknown-family node type", () => {
    // Source nodes resolve to an unknown family in inferFamily — exercises the
    // path where suggestNamingConvention's family lookup misses and the
    // generic fallback string is used.
    const steps = buildPostCreationNextSteps(0, "Source", [], { name: "" });
    const namingStep = steps.find((s) => s.startsWith("Name this node"));
    expect(namingStep).toBeDefined();
    expect(namingStep).toContain("Use a descriptive, layer-appropriate name");
    // Source is not fact/dimension — no materialization guidance should appear.
    expect(steps.some((s) => s.startsWith("Verify materialization"))).toBe(false);
  });

  it("returns steps in the documented section order", () => {
    // Asserts the actual ordering of the section blocks (naming -> join ->
    // family materialization -> verification) so that any reordering of the
    // emit blocks in buildPostCreationNextSteps is caught. Substring matches
    // keep the test robust to copy edits inside each block.
    const steps = buildPostCreationNextSteps(2, "Fact", [joinSuggestion(2)], {
      name: "",
    });
    const namingIdx = steps.findIndex((s) => s.startsWith("Name this node"));
    const requiredJoinIdx = steps.findIndex((s) => s.startsWith("REQUIRED: Set up the join"));
    const verifyJoinIdx = steps.findIndex((s) => s.startsWith("Verify join columns"));
    const materializationIdx = steps.findIndex((s) => s.startsWith("Verify materialization"));
    const grainIdx = steps.findIndex((s) => s.startsWith("For fact tables: define the grain"));
    const verifyNodeIdx = steps.findIndex((s) =>
      s.startsWith("Verify the node:")
    );

    expect(namingIdx).toBeGreaterThanOrEqual(0);
    expect(requiredJoinIdx).toBeGreaterThan(namingIdx);
    expect(verifyJoinIdx).toBeGreaterThan(requiredJoinIdx);
    expect(materializationIdx).toBeGreaterThan(verifyJoinIdx);
    expect(grainIdx).toBeGreaterThan(materializationIdx);
    expect(verifyNodeIdx).toBe(steps.length - 1); // verification is always last
  });
});
