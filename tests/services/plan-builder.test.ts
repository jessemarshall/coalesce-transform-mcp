import { describe, expect, it } from "vitest";
import {
  buildSelectItemsFromSourceNode,
  expandWildcardSelectItems,
  buildDefaultNodePrefix,
  buildDefaultNodeName,
  buildPlanFromSql,
} from "../../src/services/pipelines/plan-builder.js";
import type {
  PlannedSelectItem,
  ParsedSqlSourceRef,
  ResolvedSqlRef,
  SqlParseResult,
} from "../../src/services/pipelines/planning-types.js";
import type {
  PipelineNodeTypeSelection,
  PipelineNodeTypeFamily,
} from "../../src/services/pipelines/node-type-selection.js";

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

function makeSelectItem(overrides: Partial<PlannedSelectItem> = {}): PlannedSelectItem {
  return {
    expression: "c.ID",
    outputName: "ID",
    sourceNodeAlias: "c",
    sourceNodeName: "STG_CUSTOMER",
    sourceNodeID: "node-cust",
    sourceColumnName: "ID",
    kind: "column",
    supported: true,
    ...overrides,
  };
}

function makeRef(overrides: Partial<ParsedSqlSourceRef> = {}): ParsedSqlSourceRef {
  return {
    locationName: "STAGING",
    nodeName: "STG_CUSTOMER",
    alias: "c",
    nodeID: "node-cust",
    sourceStyle: "table_name",
    locationCandidates: ["STAGING"],
    relationStart: 0,
    relationEnd: 0,
    ...overrides,
  };
}

function makeResolvedRef(overrides: Partial<ResolvedSqlRef> = {}): ResolvedSqlRef {
  return {
    locationName: "STAGING",
    nodeName: "STG_CUSTOMER",
    alias: "c",
    nodeID: "node-cust",
    ...overrides,
  };
}

function makeSelection(
  overrides: Partial<PipelineNodeTypeSelection> = {}
): PipelineNodeTypeSelection {
  return {
    strategy: "repo-ranked",
    selectedNodeType: "Stage",
    selectedDisplayName: "Stage",
    selectedShortName: "STG",
    selectedFamily: "stage",
    confidence: "high",
    autoExecutable: true,
    supportedNodeTypes: ["Stage"],
    repoPath: null,
    resolvedRepoPath: null,
    repoWarnings: [],
    workspaceObservedNodeTypes: ["Stage"],
    consideredNodeTypes: [],
    ...overrides,
  } as PipelineNodeTypeSelection;
}

function makeSelectedNodeType(
  overrides: Partial<Parameters<typeof buildPlanFromSql>[0]["selectedNodeType"] & object> = {}
) {
  return {
    nodeType: "Stage",
    displayName: "Stage",
    shortName: "STG",
    family: "stage" as PipelineNodeTypeFamily,
    autoExecutable: true,
    semanticSignals: [],
    missingDefaultFields: [],
    templateWarnings: [],
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// buildSelectItemsFromSourceNode — expands a node's columns into select items
// ---------------------------------------------------------------------------

describe("buildSelectItemsFromSourceNode", () => {
  it("emits one supported column select item per column", () => {
    const node = {
      metadata: {
        columns: [
          { name: "ID" },
          { name: "NAME" },
          { name: "EMAIL" },
        ],
      },
    };
    const items = buildSelectItemsFromSourceNode("node-1", "STG_USER", node);

    expect(items).toHaveLength(3);
    expect(items[0]).toMatchObject({
      expression: "STG_USER.ID",
      outputName: "ID",
      sourceNodeAlias: "STG_USER",
      sourceNodeName: "STG_USER",
      sourceNodeID: "node-1",
      sourceColumnName: "ID",
      kind: "column",
      supported: true,
    });
    expect(items.map((i) => i.outputName)).toEqual(["ID", "NAME", "EMAIL"]);
  });

  it("returns empty array when node has no columns", () => {
    expect(buildSelectItemsFromSourceNode("node-1", "X", {})).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// expandWildcardSelectItems — expands * into concrete columns using predecessors
// ---------------------------------------------------------------------------

describe("expandWildcardSelectItems", () => {
  it("passes non-wildcard items through unchanged", () => {
    const item = makeSelectItem();
    const result = expandWildcardSelectItems([item], [makeResolvedRef()], {});
    expect(result).toHaveLength(1);
    expect(result[0]).toBe(item);
  });

  it("expands a wildcard using the matching predecessor node", () => {
    const wildcard = makeSelectItem({
      expression: "c.*",
      outputName: null,
      sourceColumnName: "*",
      sourceNodeID: "node-cust",
    });
    const predecessorNodes = {
      "node-cust": {
        metadata: {
          columns: [{ name: "ID" }, { name: "NAME" }],
        },
      },
    };
    const result = expandWildcardSelectItems(
      [wildcard],
      [makeResolvedRef()],
      predecessorNodes
    );

    expect(result).toHaveLength(2);
    expect(result[0]).toMatchObject({
      expression: "c.ID",
      outputName: "ID",
      sourceColumnName: "ID",
      sourceNodeID: "node-cust",
      supported: true,
    });
    expect(result[1]!.outputName).toBe("NAME");
  });

  it("marks wildcard as unsupported when the predecessor node body is missing", () => {
    const wildcard = makeSelectItem({
      sourceColumnName: "*",
      sourceNodeID: "node-cust",
    });
    const result = expandWildcardSelectItems(
      [wildcard],
      [makeResolvedRef()],
      {}
    );

    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({
      supported: false,
      reason: expect.stringMatching(/predecessor body was not available/i),
    });
  });

  it("marks wildcard as unsupported when predecessor has no columns", () => {
    const wildcard = makeSelectItem({
      sourceColumnName: "*",
      sourceNodeID: "node-cust",
    });
    const result = expandWildcardSelectItems(
      [wildcard],
      [makeResolvedRef()],
      { "node-cust": { metadata: { columns: [] } } }
    );

    expect(result[0]).toMatchObject({
      supported: false,
      reason: expect.stringMatching(/no columns to expand/i),
    });
  });

  it("marks wildcard as unsupported when the ref cannot be resolved", () => {
    const wildcard = makeSelectItem({
      sourceColumnName: "*",
      sourceNodeID: null,
      sourceNodeAlias: "bogus",
    });
    const result = expandWildcardSelectItems([wildcard], [], {});
    expect(result[0]).toMatchObject({
      supported: false,
      reason: expect.stringMatching(/could not be resolved/i),
    });
  });

  it("falls back to matching by alias when sourceNodeID is missing", () => {
    const wildcard = makeSelectItem({
      expression: "c.*",
      outputName: null,
      sourceColumnName: "*",
      sourceNodeID: null,
      sourceNodeAlias: "c",
    });
    const ref = makeResolvedRef({ alias: "c", nodeID: "node-cust" });
    const predecessorNodes = {
      "node-cust": {
        metadata: {
          columns: [{ name: "ID" }],
        },
      },
    };
    const result = expandWildcardSelectItems([wildcard], [ref], predecessorNodes);
    expect(result).toHaveLength(1);
    expect(result[0]!.sourceColumnName).toBe("ID");
  });
});

// ---------------------------------------------------------------------------
// buildDefaultNodePrefix / buildDefaultNodeName — name conventions
// ---------------------------------------------------------------------------

describe("buildDefaultNodePrefix", () => {
  it("uses shortName when provided", () => {
    expect(buildDefaultNodePrefix("stage", "customShort")).toBe("CUSTOMSHORT");
  });

  it("normalises non-alphanumeric chars in shortName to underscores", () => {
    expect(buildDefaultNodePrefix("stage", "my-short name")).toBe(
      "MY_SHORT_NAME"
    );
  });

  it.each([
    ["stage" as const, "STG"],
    ["persistent-stage" as const, "PSTG"],
    ["view" as const, "VW"],
    ["work" as const, "WRK"],
    ["dimension" as const, "DIM"],
    ["fact" as const, "FACT"],
    ["hub" as const, "HUB"],
    ["satellite" as const, "SAT"],
    ["link" as const, "LNK"],
  ])("maps family %s to prefix %s", (family, prefix) => {
    expect(buildDefaultNodePrefix(family, null)).toBe(prefix);
  });

  it("falls back to NODE for unknown family", () => {
    expect(buildDefaultNodePrefix(null, null)).toBe("NODE");
    expect(buildDefaultNodePrefix(undefined, null)).toBe("NODE");
    expect(buildDefaultNodePrefix("unknown", null)).toBe("NODE");
  });
});

describe("buildDefaultNodeName", () => {
  it("returns the explicit target name when provided", () => {
    expect(
      buildDefaultNodeName("my_target", [makeResolvedRef()], "stage", null)
    ).toBe("my_target");
  });

  it("returns '<prefix>_NEW_PIPELINE' when no refs are provided", () => {
    expect(buildDefaultNodeName(undefined, [], "stage", null)).toBe(
      "STG_NEW_PIPELINE"
    );
  });

  it("strips common prefixes from the first ref's node name", () => {
    const ref = makeResolvedRef({ nodeName: "SRC_ORDERS" });
    expect(buildDefaultNodeName(undefined, [ref], "stage", null)).toBe(
      "STG_ORDERS"
    );
  });

  it("uppercases the combined name and collapses consecutive underscores", () => {
    const ref = makeResolvedRef({ nodeName: "work_data" });
    // 'work_' is a stripped prefix, leaving 'data' → STG_DATA
    expect(buildDefaultNodeName(undefined, [ref], "stage", null)).toBe(
      "STG_DATA"
    );
  });

  it("treats blank targetName as missing and falls back to the default pattern", () => {
    // Blank targetName → trim-empty → skip the targetName branch and derive
    // the name from prefix + first ref's nodeName (STG_CUSTOMER stripped to
    // CUSTOMER, prefixed with STG → STG_CUSTOMER).
    expect(
      buildDefaultNodeName("   ", [makeResolvedRef()], "stage", null)
    ).toBe("STG_CUSTOMER");
  });
});

// ---------------------------------------------------------------------------
// buildPlanFromSql — the orchestrator
// ---------------------------------------------------------------------------

describe("buildPlanFromSql", () => {
  const baseParams = (overrides: Partial<Parameters<typeof buildPlanFromSql>[0]> = {}) => ({
    workspaceID: "ws-1",
    sql: "SELECT c.ID FROM STG_CUSTOMER c",
    nodeTypeSelection: makeSelection(),
    selectedNodeType: makeSelectedNodeType(),
    ...overrides,
  });

  const baseParseResult = (overrides: Partial<SqlParseResult> = {}): SqlParseResult => ({
    refs: [makeRef()],
    selectItems: [makeSelectItem()],
    warnings: [],
    ...overrides,
  });

  it("builds a ready plan with one node, expanded select items, and predecessor wiring", () => {
    const plan = buildPlanFromSql(
      baseParams(),
      baseParseResult(),
      {},
      [],
      []
    );

    expect(plan.status).toBe("ready");
    expect(plan.nodes).toHaveLength(1);
    expect(plan.nodes[0]).toMatchObject({
      planNodeID: "node-1",
      nodeType: "Stage",
      nodeTypeFamily: "stage",
      predecessorNodeIDs: ["node-cust"],
      predecessorNodeNames: ["STG_CUSTOMER"],
      outputColumnNames: ["ID"],
      requiresFullSetNode: true,
    });
    expect(plan.openQuestions).toEqual([]);
    expect(plan.warnings).toEqual([]);
  });

  it("falls back to Stage when no selectedNodeType is provided", () => {
    const plan = buildPlanFromSql(
      baseParams({ selectedNodeType: null }),
      baseParseResult(),
      {},
      [],
      []
    );

    expect(plan.nodes[0]!.nodeType).toBe("Stage");
    expect(plan.warnings.some((w) => /fell back to Stage/.test(w))).toBe(true);
  });

  it("uses targetNodeType as fallback when selectedNodeType is null", () => {
    const plan = buildPlanFromSql(
      baseParams({ selectedNodeType: null, targetNodeType: "View" }),
      baseParseResult(),
      {},
      [],
      []
    );

    expect(plan.nodes[0]!.nodeType).toBe("View");
  });

  it("marks plan as needs_clarification when autoExecutable is false", () => {
    const plan = buildPlanFromSql(
      baseParams({
        selectedNodeType: makeSelectedNodeType({
          autoExecutable: false,
          semanticSignals: ["sourceMapping"],
          missingDefaultFields: ["businessKey"],
        }),
      }),
      baseParseResult(),
      {},
      [],
      []
    );

    expect(plan.status).toBe("needs_clarification");
    expect(
      plan.warnings.some((w) =>
        /needs additional semantic configuration/i.test(w)
      )
    ).toBe(true);
    expect(
      plan.openQuestions.some((q) => /Confirm the required configuration/.test(q))
    ).toBe(true);
    expect(
      plan.openQuestions.some((q) =>
        /Provide values for .* config fields without defaults/.test(q)
      )
    ).toBe(true);
  });

  it("marks plan as needs_clarification when there are unsupported select items", () => {
    const plan = buildPlanFromSql(
      baseParams(),
      baseParseResult({
        selectItems: [
          makeSelectItem({
            supported: false,
            reason: "Unqualified columns are only supported when exactly one predecessor ref is present.",
          }),
        ],
      }),
      {},
      [],
      []
    );

    expect(plan.status).toBe("needs_clarification");
    expect(plan.warnings.some((w) => /Unqualified columns/.test(w))).toBe(true);
  });

  it("adds an open question when no SELECT/FROM was parseable", () => {
    const plan = buildPlanFromSql(
      baseParams(),
      baseParseResult({
        selectItems: [],
        warnings: [
          "Could not find a top-level SELECT ... FROM clause in the SQL.",
        ],
      }),
      {},
      [],
      []
    );

    expect(plan.status).toBe("needs_clarification");
    expect(
      plan.openQuestions.some((q) =>
        /Provide a top-level SELECT \.\.\. FROM query/.test(q)
      )
    ).toBe(true);
  });

  it("adds an open question when no supported projected columns exist", () => {
    const plan = buildPlanFromSql(
      baseParams(),
      baseParseResult({
        selectItems: [
          makeSelectItem({ supported: true, outputName: null }),
        ],
      }),
      {},
      [],
      []
    );

    expect(plan.status).toBe("needs_clarification");
    expect(
      plan.openQuestions.some((q) =>
        /at least one supported projected column/.test(q)
      )
    ).toBe(true);
  });

  it("populates sourceRefs from parseResult", () => {
    const plan = buildPlanFromSql(
      baseParams(),
      baseParseResult({
        refs: [
          makeRef({ nodeName: "CUSTOMERS", alias: "c" }),
          makeRef({
            nodeName: "ORDERS",
            alias: "o",
            nodeID: "node-orders",
          }),
        ],
        selectItems: [makeSelectItem()],
      }),
      {},
      [],
      []
    );

    expect(plan.nodes[0]!.sourceRefs).toHaveLength(2);
    expect(plan.nodes[0]!.sourceRefs[0]).toMatchObject({
      nodeName: "CUSTOMERS",
      alias: "c",
    });
    expect(plan.nodes[0]!.predecessorNodeIDs).toEqual([
      "node-cust",
      "node-orders",
    ]);
  });

  it("deduplicates predecessor IDs when the same ref appears twice", () => {
    const ref = makeRef();
    const plan = buildPlanFromSql(
      baseParams(),
      baseParseResult({ refs: [ref, ref] }),
      {},
      [],
      []
    );
    // Two refs with the same nodeID → deduped to one
    expect(plan.nodes[0]!.predecessorNodeIDs).toEqual(["node-cust"]);
  });

  it("builds a join condition from the SQL FROM clause with ref() syntax", () => {
    const plan = buildPlanFromSql(
      baseParams({ sql: "SELECT c.ID FROM STG_CUSTOMER c" }),
      baseParseResult({
        refs: [
          makeRef({
            nodeName: "STG_CUSTOMER",
            alias: "c",
            // relationStart/End point at STG_CUSTOMER in the FROM clause
            // extractFromClause returns "FROM STG_CUSTOMER c" — the slice
            // covers the table name.
            relationStart: "FROM ".length,
            relationEnd: "FROM STG_CUSTOMER".length,
          }),
        ],
      }),
      {},
      [],
      []
    );

    expect(plan.nodes[0]!.joinCondition).toMatch(
      /FROM \{\{ ref\('STAGING', 'STG_CUSTOMER'\) \}\}/
    );
  });

  it("propagates parseResult warnings and locally-provided warnings onto the plan", () => {
    const plan = buildPlanFromSql(
      baseParams(),
      baseParseResult({ warnings: ["parse-warn"] }),
      {},
      [],
      ["local-warn"]
    );

    expect(plan.warnings).toEqual(
      expect.arrayContaining(["parse-warn", "local-warn"])
    );
  });

  it("deep-clones configOverrides so the plan can be mutated safely", () => {
    const overrides = { preSQL: "SET foo = bar;" };
    const plan = buildPlanFromSql(
      baseParams({ configOverrides: overrides }),
      baseParseResult(),
      {},
      [],
      []
    );

    expect(plan.nodes[0]!.configOverrides).toEqual(overrides);
    expect(plan.nodes[0]!.configOverrides).not.toBe(overrides);
  });

  it("defaults configOverrides to an empty object when not provided", () => {
    const plan = buildPlanFromSql(baseParams(), baseParseResult(), {}, [], []);
    expect(plan.nodes[0]!.configOverrides).toEqual({});
  });

  it("exposes templateDefaults on the node when the selection provides them", () => {
    const templateDefaults = {
      inferredTopLevelFields: { description: "auto" },
      inferredConfig: { multisource: true },
    };
    const plan = buildPlanFromSql(
      baseParams({
        selectedNodeType: makeSelectedNodeType({ templateDefaults }),
      }),
      baseParseResult(),
      {},
      [],
      []
    );

    expect(plan.nodes[0]!.templateDefaults).toEqual(templateDefaults);
  });

  it("uses the provided targetName when given (respects casing)", () => {
    const plan = buildPlanFromSql(
      baseParams({ targetName: "MixedCase_Name" }),
      baseParseResult(),
      {},
      [],
      []
    );

    expect(plan.nodes[0]!.name).toBe("MixedCase_Name");
  });

  it("uses supportedNodeTypes from the selection when non-empty", () => {
    const plan = buildPlanFromSql(
      baseParams({
        nodeTypeSelection: makeSelection({
          supportedNodeTypes: ["Stage", "View", "Work"],
        }),
      }),
      baseParseResult(),
      {},
      [],
      []
    );

    expect(plan.supportedNodeTypes).toEqual(["Stage", "View", "Work"]);
  });

  it("falls back to [nodeType] when selection has no supportedNodeTypes", () => {
    const plan = buildPlanFromSql(
      baseParams({
        nodeTypeSelection: makeSelection({ supportedNodeTypes: [] }),
      }),
      baseParseResult(),
      {},
      [],
      []
    );

    expect(plan.supportedNodeTypes).toEqual(["Stage"]);
  });
});
