import { describe, it, expect } from "vitest";
import {
  suggestNamingConvention,
  buildPostCreationNextSteps,
} from "../../src/services/workspace/node-creation.js";
import type { JoinSuggestion } from "../../src/services/workspace/join-helpers.js";

// ---------------------------------------------------------------------------
// suggestNamingConvention
// ---------------------------------------------------------------------------
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

  it("returns steps in a stable, deterministic order", () => {
    const steps1 = buildPostCreationNextSteps(2, "Fact", [joinSuggestion(2)], {
      name: "FACT_SALES",
    });
    const steps2 = buildPostCreationNextSteps(2, "Fact", [joinSuggestion(2)], {
      name: "FACT_SALES",
    });
    expect(steps1).toEqual(steps2);
  });
});
