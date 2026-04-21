import { describe, it, expect } from "vitest";
import {
  buildUseCaseContext,
  scoreCandidate,
  challengeCandidate,
  rankCandidates,
} from "../../../src/services/pipelines/node-type-scoring.js";
import type {
  InternalPipelineNodeTypeCandidate,
  PipelineNodeTypeSelectionContext,
} from "../../../src/services/pipelines/node-type-selection.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeCandidate(
  overrides: Partial<InternalPipelineNodeTypeCandidate> = {}
): InternalPipelineNodeTypeCandidate {
  return {
    nodeType: "base:::Stage",
    displayName: "Stage",
    shortName: "STG",
    family: "stage",
    usageCount: 0,
    workspaceUsageCount: 0,
    observedInWorkspace: true,
    autoExecutable: true,
    semanticSignals: [],
    missingDefaultFields: [],
    templateWarnings: [],
    score: 0,
    reasons: [],
    source: "workspace",
    ...overrides,
  };
}

function makeContext(
  overrides: Partial<PipelineNodeTypeSelectionContext> = {}
): PipelineNodeTypeSelectionContext {
  return {
    sourceCount: 1,
    ...overrides,
  };
}

// =========================================================================
// buildUseCaseContext
// =========================================================================

describe("buildUseCaseContext", () => {
  describe("general-purpose (default)", () => {
    it("returns general-purpose when no specialized signals", () => {
      const result = buildUseCaseContext(makeContext({ goal: "stage raw data" }));
      expect(result.category).toBe("general-purpose");
      expect(result.desiredFamilies).toContain("stage");
      expect(result.desiredFamilies).toContain("work");
    });

    it("prefers work over stage when workspace has more work nodes", () => {
      const result = buildUseCaseContext(
        makeContext({
          workspaceNodeTypeCounts: {
            "Work": 10,
            "Stage": 2,
          },
        })
      );
      expect(result.category).toBe("general-purpose");
      expect(result.desiredFamilies[0]).toBe("work");
    });

    it("prefers stage over work when workspace has more stage nodes", () => {
      const result = buildUseCaseContext(
        makeContext({
          workspaceNodeTypeCounts: {
            "Stage": 10,
            "Work": 2,
          },
        })
      );
      expect(result.category).toBe("general-purpose");
      expect(result.desiredFamilies[0]).toBe("stage");
    });

    it("includes view in general-purpose families", () => {
      const result = buildUseCaseContext(makeContext());
      expect(result.desiredFamilies).toContain("view");
    });
  });

  describe("dimensional modeling", () => {
    it("detects 'dimensional model' in goal", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "build a dimensional model for sales" })
      );
      expect(result.category).toBe("dimensional");
      expect(result.desiredFamilies).toContain("dimension");
      expect(result.desiredFamilies).toContain("fact");
      expect(result.dimensionalModeling).toBe(true);
    });

    it("detects 'star schema' in goal", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "create a star schema" })
      );
      expect(result.category).toBe("dimensional");
      expect(result.dimensionalModeling).toBe(true);
    });

    it("detects 'snowflake schema' in goal", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "build snowflake schema" })
      );
      expect(result.category).toBe("dimensional");
    });
  });

  describe("data vault", () => {
    it("detects 'data vault' intent", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "build a data vault model" })
      );
      expect(result.category).toBe("data-vault");
      expect(result.desiredFamilies).toEqual(
        expect.arrayContaining(["hub", "satellite", "link"])
      );
    });

    it("data vault takes precedence over dimensional when both present", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "data vault with dimensional model" })
      );
      expect(result.category).toBe("data-vault");
    });
  });

  describe("persistent stage", () => {
    it("detects 'persistent stage' intent", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "create a persistent stage for CDC" })
      );
      expect(result.category).toBe("persistent");
      expect(result.desiredFamilies).toContain("persistent-stage");
    });

    it("detects 'cdc' intent", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "implement CDC tracking" })
      );
      expect(result.category).toBe("persistent");
    });

    it("detects 'change tracking' intent", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "add change tracking to orders" })
      );
      expect(result.category).toBe("persistent");
    });
  });

  describe("view", () => {
    it("detects 'view' intent without join/groupby", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "create a view for quick access" })
      );
      expect(result.category).toBe("view");
      expect(result.desiredFamilies).toContain("view");
    });

    it("skips explicit view intent when hasJoin but still matches via strong signals", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "create a view", hasJoin: true })
      );
      // hasJoin blocks the explicit viewIntent check (line 77), but "view" in the goal
      // still matches view's strongSignals regex in the fallback loop — so category is "view"
      expect(result.category).toBe("view");
    });

    it("falls through to general-purpose when hasJoin and no view signal in text", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "join two tables with no materialization", hasJoin: true })
      );
      // "no materialization" triggers viewIntent, but hasJoin blocks the explicit check.
      // "no materialization" does NOT match view's strongSignals regex, so it falls
      // through to general-purpose.
      expect(result.category).toBe("general-purpose");
    });

    it("detects 'no materialization' intent", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "use no materialization for this" })
      );
      expect(result.category).toBe("view");
    });

    it("detects 'virtual table' intent", () => {
      const result = buildUseCaseContext(
        makeContext({ goal: "set up a virtual table" })
      );
      expect(result.category).toBe("view");
    });
  });

  describe("strong signals from target name", () => {
    it("detects dim_ prefix in target name as dimensional", () => {
      const result = buildUseCaseContext(
        makeContext({ targetName: "dim_customer" })
      );
      expect(result.category).toBe("dimensional");
    });

    it("detects fct_ prefix in target name as dimensional", () => {
      const result = buildUseCaseContext(
        makeContext({ targetName: "fct_sales" })
      );
      expect(result.category).toBe("dimensional");
    });

    it("detects hub_ prefix in target name as data-vault", () => {
      const result = buildUseCaseContext(
        makeContext({ targetName: "hub_customer" })
      );
      expect(result.category).toBe("data-vault");
    });
  });

  describe("empty/undefined context fields", () => {
    it("handles all optional fields undefined with sourceCount 0", () => {
      const result = buildUseCaseContext({
        sourceCount: 0,
        goal: undefined,
        targetName: undefined,
        workspaceNodeTypeCounts: undefined,
      } as PipelineNodeTypeSelectionContext);
      expect(result.category).toBe("general-purpose");
      expect(result.multiSource).toBe(false);
      expect(result.dimensionalModeling).toBe(false);
      expect(result.desiredFamilies.length).toBeGreaterThan(0);
    });

    it("falls back to empty counts when workspaceNodeTypeCounts is undefined", () => {
      const result = buildUseCaseContext(makeContext({
        workspaceNodeTypeCounts: undefined,
      }));
      // With no usage data, stage is preferred over work (default ordering)
      expect(result.category).toBe("general-purpose");
      expect(result.desiredFamilies[0]).toBe("stage");
    });
  });

  describe("multiSource flag", () => {
    it("sets multiSource true when sourceCount > 1", () => {
      const result = buildUseCaseContext(makeContext({ sourceCount: 3 }));
      expect(result.multiSource).toBe(true);
    });

    it("sets multiSource false when sourceCount is 1", () => {
      const result = buildUseCaseContext(makeContext({ sourceCount: 1 }));
      expect(result.multiSource).toBe(false);
    });
  });
});

// =========================================================================
// scoreCandidate
// =========================================================================

describe("scoreCandidate", () => {
  describe("explicit node type override", () => {
    it("gives massive bonus for exact match", () => {
      const candidate = makeCandidate({ nodeType: "base:::Stage" });
      const context = makeContext({ explicitNodeType: "base:::Stage" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.score).toBeGreaterThan(900);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/explicit targetNodeType override/),
        ])
      );
    });

    it("gives large bonus for ID match", () => {
      const candidate = makeCandidate({ nodeType: "pkg:::Stage" });
      const context = makeContext({ explicitNodeType: "other:::Stage" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/explicit targetNodeType ID/),
        ])
      );
    });

    it("penalizes candidates that don't match explicit type", () => {
      const candidate = makeCandidate({ nodeType: "base:::View", family: "view" });
      const context = makeContext({ explicitNodeType: "base:::Stage" });
      const scored = scoreCandidate(candidate, context);
      // Should have a penalty applied
      expect(scored.score).toBeLessThan(candidate.score);
    });
  });

  describe("family scoring — general-purpose", () => {
    it("gives high score to stage and work families for general-purpose context", () => {
      const stage = makeCandidate({ family: "stage" });
      const work = makeCandidate({ nodeType: "base:::Work", family: "work" });
      const view = makeCandidate({ nodeType: "base:::View", family: "view" });
      const context = makeContext({ goal: "transform data" });
      const scoredStage = scoreCandidate(stage, context);
      const scoredWork = scoreCandidate(work, context);
      const scoredView = scoreCandidate(view, context);
      // Stage and work should both score well above view for general-purpose
      expect(scoredStage.score).toBeGreaterThan(scoredView.score);
      expect(scoredWork.score).toBeGreaterThan(scoredView.score);
    });

    it("gives workspace bonus to observed stage/work candidates", () => {
      const withWorkspace = makeCandidate({
        family: "stage",
        observedInWorkspace: true,
      });
      const without = makeCandidate({
        family: "stage",
        observedInWorkspace: false,
      });
      const context = makeContext();
      const scoredWith = scoreCandidate(withWorkspace, context);
      const scoredWithout = scoreCandidate(without, context);
      expect(scoredWith.score).toBeGreaterThan(scoredWithout.score);
    });

    it("gives lower score to view for general-purpose context", () => {
      const stage = makeCandidate({ family: "stage" });
      const view = makeCandidate({
        nodeType: "base:::View",
        family: "view",
      });
      const context = makeContext();
      const scoredStage = scoreCandidate(stage, context);
      const scoredView = scoreCandidate(view, context);
      expect(scoredStage.score).toBeGreaterThan(scoredView.score);
    });

    it("gives much lower score to dimension than stage for general-purpose context", () => {
      const dimension = makeCandidate({
        nodeType: "base:::Dimension",
        family: "dimension",
        autoExecutable: false,
        semanticSignals: ["business_key"],
      });
      const stage = makeCandidate({ family: "stage" });
      const context = makeContext({ goal: "basic staging" });
      const scoredDimension = scoreCandidate(dimension, context);
      const scoredStage = scoreCandidate(stage, context);
      // Dimension should score far below stage for general-purpose
      expect(scoredDimension.score).toBeLessThan(scoredStage.score);
    });
  });

  describe("family scoring — specialized categories", () => {
    it("gives higher score to dimension than stage when dimensional modeling detected", () => {
      const dimension = makeCandidate({
        nodeType: "base:::Dimension",
        family: "dimension",
        autoExecutable: false,
      });
      const stage = makeCandidate({ family: "stage" });
      const context = makeContext({ goal: "build a dimensional model" });
      const scoredDimension = scoreCandidate(dimension, context);
      const scoredStage = scoreCandidate(stage, context);
      expect(scoredDimension.score).toBeGreaterThan(scoredStage.score);
    });

    it("gives fallback score to stage when dimensional category active", () => {
      const candidate = makeCandidate({ family: "stage" });
      const context = makeContext({ goal: "build dimensional model" });
      const scored = scoreCandidate(candidate, context);
      // Stage as fallback for dimensional should get some score but not top
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/fallback/),
        ])
      );
    });
  });

  describe("multi-source bonus", () => {
    it("adds bonus for auto-executable family with multiple sources", () => {
      const candidate = makeCandidate({ family: "stage", autoExecutable: true });
      const context = makeContext({ sourceCount: 3 });
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/multisource/),
        ])
      );
    });
  });

  describe("dimensional modeling bonus", () => {
    it("adds bonus for dimension family with dimensional modeling intent", () => {
      const candidate = makeCandidate({
        nodeType: "base:::Dimension",
        family: "dimension",
        autoExecutable: false,
      });
      const context = makeContext({ goal: "dimensional model" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/dimensional modeling/i),
        ])
      );
    });

    it("adds bonus for fact family with dimensional modeling intent", () => {
      const candidate = makeCandidate({
        nodeType: "base:::Fact",
        family: "fact",
        autoExecutable: false,
      });
      const context = makeContext({ goal: "star schema for sales" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/dimensional modeling/i),
        ])
      );
    });
  });

  describe("anti-signal penalty", () => {
    it("penalizes persistent-stage when context says 'staging layer'", () => {
      const candidate = makeCandidate({
        nodeType: "base:::PersistentStage",
        family: "persistent-stage",
        autoExecutable: false,
      });
      const context = makeContext({ goal: "staging layer for raw data" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/not a persistent-stage use case/),
        ])
      );
    });

    it("penalizes dimension when context says 'intermediate transform'", () => {
      const candidate = makeCandidate({
        nodeType: "base:::Dimension",
        family: "dimension",
        autoExecutable: false,
      });
      const context = makeContext({ goal: "intermediate transform step" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/not a dimension use case/),
        ])
      );
    });
  });

  describe("semantic config penalty", () => {
    it("penalizes types requiring semantic config without dimensional intent", () => {
      const candidate = makeCandidate({
        nodeType: "base:::Dimension",
        family: "dimension",
        autoExecutable: false,
      });
      const context = makeContext({ goal: "process raw data" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/requires semantic config/),
        ])
      );
    });

    it("skips penalty when dimensional modeling intent is present", () => {
      const candidate = makeCandidate({
        nodeType: "base:::Dimension",
        family: "dimension",
        autoExecutable: false,
      });
      const context = makeContext({ goal: "build dimensional model" });
      const scored = scoreCandidate(candidate, context);
      const semanticPenaltyReasons = scored.reasons.filter((r) =>
        /requires semantic config.*no dimensional/i.test(r)
      );
      expect(semanticPenaltyReasons).toHaveLength(0);
    });
  });

  describe("specialized pattern penalty", () => {
    it("sets -Infinity for dynamic table without dynamic table context", () => {
      const candidate = makeCandidate({
        nodeType: "base:::DynamicTable",
        displayName: "Dynamic Table Stage",
        family: "stage",
      });
      const context = makeContext({ goal: "stage raw data" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.score).toBe(-Infinity);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/not applicable.*Dynamic Table/),
        ])
      );
    });

    it("gives bonus for dynamic table when context requests it", () => {
      const candidate = makeCandidate({
        nodeType: "DynamicTableStage",
        displayName: "Dynamic Table Stage",
        family: "stage",
      });
      const context = makeContext({ goal: "create a dynamic table with auto-refresh" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.score).toBeGreaterThan(0);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/explicitly requests Dynamic Table/),
        ])
      );
    });
  });

  describe("data vault package exclusion", () => {
    it("excludes data vault package types when no data vault intent", () => {
      const candidate = makeCandidate({
        nodeType: "data-vault:::SomeType",
        family: "stage",
        packageAlias: "data-vault-nodes",
        source: "workspace",
      });
      const context = makeContext({ goal: "stage raw data" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.score).toBe(-Infinity);
    });

    it("allows data vault package types when data vault intent present", () => {
      const candidate = makeCandidate({
        nodeType: "data-vault:::Hub",
        family: "hub",
        packageAlias: "data-vault-nodes",
        source: "workspace",
      });
      const context = makeContext({ goal: "build a data vault model" });
      const scored = scoreCandidate(candidate, context);
      expect(scored.score).toBeGreaterThan(-Infinity);
    });
  });

  describe("non-base package exclusion for general-purpose", () => {
    it("excludes non-base repo types not observed in workspace", () => {
      const candidate = makeCandidate({
        source: "repo",
        packageAlias: "custom-nodes",
        observedInWorkspace: false,
      });
      const context = makeContext();
      const scored = scoreCandidate(candidate, context);
      expect(scored.score).toBe(-Infinity);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/excluded.*non-base package/),
        ])
      );
    });

    it("allows non-base repo types when observed in workspace", () => {
      const candidate = makeCandidate({
        source: "repo",
        packageAlias: "custom-nodes",
        observedInWorkspace: true,
      });
      const context = makeContext();
      const scored = scoreCandidate(candidate, context);
      expect(scored.score).toBeGreaterThan(-Infinity);
    });

    it("gives bonus to base node type package", () => {
      const candidate = makeCandidate({
        source: "repo",
        packageAlias: "base-node-types",
        observedInWorkspace: false,
      });
      const context = makeContext();
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/base node type package/),
        ])
      );
    });
  });

  describe("copy-of penalty", () => {
    it("penalizes 'Copy of' display names", () => {
      const candidate = makeCandidate({
        displayName: "Copy of Stage",
      });
      const context = makeContext();
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/cloned type/),
        ])
      );
    });

    it("does not penalize 'Copy of' when explicit node type matches", () => {
      const candidate = makeCandidate({
        nodeType: "base:::CopyOfStage",
        displayName: "Copy of Stage",
      });
      const context = makeContext({ explicitNodeType: "base:::CopyOfStage" });
      const scored = scoreCandidate(candidate, context);
      const clonePenalties = scored.reasons.filter((r) => /cloned type/i.test(r));
      expect(clonePenalties).toHaveLength(0);
    });
  });

  describe("unknown family penalty", () => {
    it("penalizes unknown family without explicit override", () => {
      const candidate = makeCandidate({
        nodeType: "CustomUnknownType",
        family: "unknown",
        autoExecutable: false,
        source: "workspace",
      });
      const context = makeContext();
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/unknown node type family/),
        ])
      );
    });
  });

  describe("auto-executable bonus/penalty", () => {
    it("gives bonus to auto-executable candidates", () => {
      const candidate = makeCandidate({ autoExecutable: true });
      const context = makeContext();
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/supports template-based automatic creation/),
        ])
      );
    });

    it("penalizes non-auto-executable candidates", () => {
      const candidate = makeCandidate({ autoExecutable: false });
      const context = makeContext();
      const scored = scoreCandidate(candidate, context);
      expect(scored.reasons).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/needs extra semantic configuration/),
        ])
      );
    });
  });

  describe("missing default fields penalty", () => {
    it("penalizes per missing default field", () => {
      const withMissing = makeCandidate({
        missingDefaultFields: ["business_key", "scd_type", "grain"],
      });
      const withoutMissing = makeCandidate({
        missingDefaultFields: [],
      });
      const context = makeContext();
      const scoredWith = scoreCandidate(withMissing, context);
      const scoredWithout = scoreCandidate(withoutMissing, context);
      expect(scoredWith.score).toBeLessThan(scoredWithout.score);
    });
  });

  describe("semantic signals penalty", () => {
    it("penalizes per semantic signal", () => {
      const withSignals = makeCandidate({
        semanticSignals: ["business_key", "surrogate_key"],
      });
      const withoutSignals = makeCandidate({
        semanticSignals: [],
      });
      const context = makeContext();
      const scoredWith = scoreCandidate(withSignals, context);
      const scoredWithout = scoreCandidate(withoutSignals, context);
      expect(scoredWith.score).toBeLessThan(scoredWithout.score);
    });
  });

  describe("template warnings penalty", () => {
    it("penalizes candidates with template warnings", () => {
      const withWarnings = makeCandidate({
        templateWarnings: ["field X does not map cleanly", "type Y unknown"],
      });
      const withoutWarnings = makeCandidate({
        templateWarnings: [],
      });
      const context = makeContext();
      const scoredWith = scoreCandidate(withWarnings, context);
      const scoredWithout = scoreCandidate(withoutWarnings, context);
      expect(scoredWith.score).toBeLessThan(scoredWithout.score);
    });

    it("caps template warning penalty at 3 warnings", () => {
      const threeWarnings = makeCandidate({
        templateWarnings: ["w1", "w2", "w3"],
      });
      const sixWarnings = makeCandidate({
        templateWarnings: ["w1", "w2", "w3", "w4", "w5", "w6"],
      });
      const context = makeContext();
      const scoredThree = scoreCandidate(threeWarnings, context);
      const scoredSix = scoreCandidate(sixWarnings, context);
      // Both should have the same penalty since it's capped at 3
      expect(scoredThree.score).toBe(scoredSix.score);
    });
  });
});

// =========================================================================
// challengeCandidate
// =========================================================================

describe("challengeCandidate", () => {
  it("challenges on anti-signal match", () => {
    const candidate = makeCandidate({
      family: "persistent-stage",
    });
    const challenges = challengeCandidate(
      candidate,
      makeContext(),
      "simple staging layer"
    );
    expect(challenges).toEqual(
      expect.arrayContaining([
        expect.stringMatching(/anti-signal matched/),
      ])
    );
  });

  it("challenges on specialized pattern penalty", () => {
    const candidate = makeCandidate({
      nodeType: "base:::DynamicTableStage",
      displayName: "Dynamic Table Stage",
      family: "stage",
    });
    const challenges = challengeCandidate(
      candidate,
      makeContext(),
      "stage raw data"
    );
    expect(challenges).toEqual(
      expect.arrayContaining([
        expect.stringMatching(/Dynamic Table/),
      ])
    );
  });

  it("challenges on semantic config without dimensional intent", () => {
    const candidate = makeCandidate({
      family: "dimension",
    });
    const challenges = challengeCandidate(
      candidate,
      makeContext(),
      "process raw data"
    );
    expect(challenges).toEqual(
      expect.arrayContaining([
        expect.stringMatching(/requires semantic config/),
      ])
    );
  });

  it("does not challenge semantic config when dimensional intent present", () => {
    const candidate = makeCandidate({
      family: "dimension",
    });
    const challenges = challengeCandidate(
      candidate,
      makeContext(),
      "build a dimensional model"
    );
    const semanticChallenges = challenges.filter((c) =>
      /requires semantic config/i.test(c)
    );
    expect(semanticChallenges).toHaveLength(0);
  });

  it("challenges when another family has a strong signal", () => {
    const candidate = makeCandidate({
      family: "view",
    });
    const challenges = challengeCandidate(
      candidate,
      makeContext(),
      "persistent stage with CDC tracking"
    );
    expect(challenges).toEqual(
      expect.arrayContaining([
        expect.stringMatching(/strong signal for persistent-stage/),
      ])
    );
  });

  it("does not cross-challenge between stage and work", () => {
    const candidate = makeCandidate({
      family: "stage",
    });
    // "work" and "transform" are strong signals for work, but stage/work don't challenge each other
    const challenges = challengeCandidate(
      candidate,
      makeContext(),
      "work table for intermediate transform"
    );
    const crossChallenges = challenges.filter((c) =>
      /strong signal for work but candidate is stage/i.test(c)
    );
    expect(crossChallenges).toHaveLength(0);
  });

  describe("doNotUseWhen challenges", () => {
    it("challenges dimension for CTE decomposition context", () => {
      const candidate = makeCandidate({
        family: "dimension",
      });
      const challenges = challengeCandidate(
        candidate,
        makeContext(),
        "CTE decomposition of the staging pipeline"
      );
      expect(challenges).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/do not use dimension for CTE decomposition/),
        ])
      );
    });

    it("challenges persistent-stage for batch ETL context", () => {
      const candidate = makeCandidate({
        family: "persistent-stage",
      });
      const challenges = challengeCandidate(
        candidate,
        makeContext(),
        "batch ETL pipeline with truncate and insert"
      );
      expect(challenges).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/do not use persistent-stage for batch ETL/),
        ])
      );
    });

    it("challenges persistent-stage for general-purpose context", () => {
      const candidate = makeCandidate({
        family: "persistent-stage",
      });
      const challenges = challengeCandidate(
        candidate,
        makeContext(),
        "general purpose staging of simple data"
      );
      expect(challenges).toEqual(
        expect.arrayContaining([
          expect.stringMatching(/do not use persistent-stage for general-purpose/),
        ])
      );
    });

    it("skips doNotUseWhen for short anti-patterns (length <= 10)", () => {
      // The guard `antiLower.length > 10` filters out short entries
      const candidate = makeCandidate({ family: "unknown" });
      const challenges = challengeCandidate(
        candidate,
        makeContext(),
        "cte decomposition batch etl general purpose"
      );
      // "unknown" family has doNotUseWhen: ["A known family matches the use case"]
      // which is > 10 chars but doesn't match any of the three regex branches
      const doNotUseChallenges = challenges.filter((c) => /do not use/i.test(c));
      expect(doNotUseChallenges).toHaveLength(0);
    });
  });

  it("challenges 'Copy of' display names", () => {
    const candidate = makeCandidate({
      displayName: "Copy of Work",
    });
    const challenges = challengeCandidate(
      candidate,
      makeContext(),
      "transform data"
    );
    expect(challenges).toEqual(
      expect.arrayContaining([
        expect.stringMatching(/cloned type/),
      ])
    );
  });

  it("challenges specialized package for general-purpose context", () => {
    const candidate = makeCandidate({
      packageAlias: "custom-advanced-nodes",
      observedInWorkspace: false,
      source: "repo",
    });
    const challenges = challengeCandidate(
      candidate,
      makeContext(),
      "general staging transform"
    );
    expect(challenges).toEqual(
      expect.arrayContaining([
        expect.stringMatching(/specialized package/),
      ])
    );
  });

  it("returns empty array when no challenges apply", () => {
    const candidate = makeCandidate({
      family: "stage",
      displayName: "Stage",
    });
    const challenges = challengeCandidate(
      candidate,
      makeContext(),
      "stage raw data"
    );
    // Stage for "stage raw data" should pass with no challenges
    expect(challenges).toHaveLength(0);
  });
});

// =========================================================================
// rankCandidates
// =========================================================================

describe("rankCandidates", () => {
  it("sorts candidates by score descending", () => {
    const candidates = [
      makeCandidate({ nodeType: "low", score: 10 }),
      makeCandidate({ nodeType: "high", score: 200 }),
      makeCandidate({ nodeType: "mid", score: 100 }),
    ];
    const ranked = rankCandidates(candidates);
    expect(ranked[0]!.nodeType).toBe("high");
    expect(ranked[1]!.nodeType).toBe("mid");
    expect(ranked[2]!.nodeType).toBe("low");
  });

  it("does not mutate the original array", () => {
    const candidates = [
      makeCandidate({ nodeType: "a", score: 10 }),
      makeCandidate({ nodeType: "b", score: 50 }),
    ];
    const ranked = rankCandidates(candidates);
    expect(candidates[0]!.nodeType).toBe("a");
    expect(ranked[0]!.nodeType).toBe("b");
  });

  it("handles -Infinity scores at the bottom", () => {
    const candidates = [
      makeCandidate({ nodeType: "excluded", score: -Infinity }),
      makeCandidate({ nodeType: "normal", score: 50 }),
      makeCandidate({ nodeType: "low", score: 5 }),
    ];
    const ranked = rankCandidates(candidates);
    expect(ranked[0]!.nodeType).toBe("normal");
    expect(ranked[ranked.length - 1]!.nodeType).toBe("excluded");
  });

  it("handles empty array", () => {
    expect(rankCandidates([])).toEqual([]);
  });
});

// =========================================================================
// Integration: scoreCandidate + rankCandidates
// =========================================================================

describe("scoring integration", () => {
  it("ranks stage above dimension for general-purpose context", () => {
    const stage = makeCandidate({ nodeType: "base:::Stage", family: "stage" });
    const dimension = makeCandidate({
      nodeType: "base:::Dimension",
      family: "dimension",
      autoExecutable: false,
      semanticSignals: ["business_key"],
    });
    const context = makeContext({ goal: "process raw data" });
    const scored = [stage, dimension].map((c) => scoreCandidate(c, context));
    const ranked = rankCandidates(scored);
    expect(ranked[0]!.family).toBe("stage");
  });

  it("ranks dimension above stage for dimensional modeling context", () => {
    const stage = makeCandidate({ nodeType: "base:::Stage", family: "stage" });
    const dimension = makeCandidate({
      nodeType: "base:::Dimension",
      family: "dimension",
      autoExecutable: false,
    });
    const context = makeContext({ goal: "build dimensional model for customers" });
    const scored = [stage, dimension].map((c) => scoreCandidate(c, context));
    const ranked = rankCandidates(scored);
    expect(ranked[0]!.family).toBe("dimension");
  });

  it("ranks hub above stage for data vault context", () => {
    const stage = makeCandidate({ nodeType: "base:::Stage", family: "stage" });
    const hub = makeCandidate({
      nodeType: "dv:::Hub",
      family: "hub",
      autoExecutable: false,
      packageAlias: "data-vault-nodes",
    });
    const context = makeContext({ goal: "build a data vault" });
    const scored = [stage, hub].map((c) => scoreCandidate(c, context));
    const ranked = rankCandidates(scored);
    expect(ranked[0]!.family).toBe("hub");
  });

  it("deduplicates reasons in scored output", () => {
    const candidate = makeCandidate({
      reasons: ["already present"],
    });
    const context = makeContext();
    const scored = scoreCandidate(candidate, context);
    const uniqueReasons = new Set(scored.reasons);
    expect(scored.reasons.length).toBe(uniqueReasons.size);
  });
});
