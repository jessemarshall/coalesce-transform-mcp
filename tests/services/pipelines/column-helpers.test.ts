import { describe, it, expect } from "vitest";
import {
  getNodeColumnArray,
  getColumnSourceNodeIDs,
  findMatchingBaseColumn,
  renameSourceMappingEntries,
  buildStageSourceMappingFromPlan,
  getColumnNamesFromNode,
} from "../../../src/services/pipelines/column-helpers.js";
import type {
  PlannedSelectItem,
  PlannedPipelineNode,
} from "../../../src/services/pipelines/planning-types.js";

function makeSelectItem(
  overrides: Partial<PlannedSelectItem> = {}
): PlannedSelectItem {
  return {
    expression: overrides.expression ?? "ORDER_ID",
    outputName: overrides.outputName ?? "ORDER_ID",
    sourceNodeAlias: overrides.sourceNodeAlias ?? null,
    sourceNodeName: overrides.sourceNodeName ?? null,
    sourceNodeID: overrides.sourceNodeID ?? null,
    sourceColumnName: overrides.sourceColumnName ?? null,
    kind: overrides.kind ?? "column",
    supported: overrides.supported ?? true,
    ...(overrides.reason !== undefined ? { reason: overrides.reason } : {}),
  };
}

function makePlannedNode(
  overrides: Partial<PlannedPipelineNode> = {}
): PlannedPipelineNode {
  return {
    planNodeID: overrides.planNodeID ?? "plan-1",
    name: overrides.name ?? "STG_ORDERS",
    nodeType: overrides.nodeType ?? "Stage",
    nodeTypeFamily: overrides.nodeTypeFamily ?? null,
    predecessorNodeIDs: overrides.predecessorNodeIDs ?? [],
    predecessorPlanNodeIDs: overrides.predecessorPlanNodeIDs ?? [],
    predecessorNodeNames: overrides.predecessorNodeNames ?? [],
    description: overrides.description ?? null,
    sql: overrides.sql ?? null,
    selectItems: overrides.selectItems ?? [],
    outputColumnNames: overrides.outputColumnNames ?? [],
    configOverrides: overrides.configOverrides ?? {},
    sourceRefs: overrides.sourceRefs ?? [],
    joinCondition: overrides.joinCondition ?? null,
    location: overrides.location ?? {},
    requiresFullSetNode: overrides.requiresFullSetNode ?? false,
  };
}

describe("getNodeColumnArray", () => {
  it("returns metadata.columns when present and an array", () => {
    const node = {
      metadata: {
        columns: [
          { name: "A", dataType: "VARCHAR" },
          { name: "B", dataType: "NUMBER" },
        ],
      },
    };
    expect(getNodeColumnArray(node)).toHaveLength(2);
    expect(getNodeColumnArray(node)[0]).toEqual({
      name: "A",
      dataType: "VARCHAR",
    });
  });

  it("returns [] when metadata is missing", () => {
    expect(getNodeColumnArray({})).toEqual([]);
  });

  it("returns [] when metadata.columns is missing", () => {
    expect(getNodeColumnArray({ metadata: {} })).toEqual([]);
  });

  it("returns [] when metadata.columns is not an array", () => {
    expect(getNodeColumnArray({ metadata: { columns: "not-an-array" } })).toEqual([]);
  });

  it("returns [] when metadata is not a plain object (e.g. array)", () => {
    expect(getNodeColumnArray({ metadata: [] as unknown as Record<string, unknown> })).toEqual([]);
  });

  it("filters non-object entries from metadata.columns", () => {
    const node = {
      metadata: {
        columns: [
          { name: "A" },
          "not-an-object",
          null,
          { name: "B" },
        ],
      },
    };
    expect(getNodeColumnArray(node)).toEqual([{ name: "A" }, { name: "B" }]);
  });
});

describe("getColumnSourceNodeIDs", () => {
  it("returns [] when sources is missing", () => {
    expect(getColumnSourceNodeIDs({})).toEqual([]);
  });

  it("returns [] when sources is not an array", () => {
    expect(getColumnSourceNodeIDs({ sources: "x" })).toEqual([]);
  });

  it("collects unique nodeIDs from columnReferences", () => {
    const column = {
      sources: [
        {
          columnReferences: [
            { nodeID: "n1", columnID: "c1" },
            { nodeID: "n2", columnID: "c2" },
          ],
        },
        {
          columnReferences: [
            { nodeID: "n1", columnID: "c3" }, // duplicate nodeID
          ],
        },
      ],
    };
    expect(getColumnSourceNodeIDs(column).sort()).toEqual(["n1", "n2"]);
  });

  it("skips refs with non-string nodeID and non-object refs", () => {
    const column = {
      sources: [
        {
          columnReferences: [
            { nodeID: 123 },
            "not-an-object",
            { columnID: "c1" }, // missing nodeID
            { nodeID: "n1" },
          ],
        },
      ],
    };
    expect(getColumnSourceNodeIDs(column)).toEqual(["n1"]);
  });

  it("skips source entries that are not plain objects", () => {
    const column = {
      sources: ["not-object", null, { columnReferences: [{ nodeID: "n1" }] }],
    };
    expect(getColumnSourceNodeIDs(column)).toEqual(["n1"]);
  });

  it("skips source entries whose columnReferences is not an array", () => {
    const column = {
      sources: [{ columnReferences: "x" }, { columnReferences: [{ nodeID: "n1" }] }],
    };
    expect(getColumnSourceNodeIDs(column)).toEqual(["n1"]);
  });
});

describe("findMatchingBaseColumn", () => {
  const baseNode = {
    metadata: {
      columns: [
        {
          name: "ORDER_ID",
          dataType: "NUMBER",
          sources: [
            { columnReferences: [{ nodeID: "n_src", columnID: "c1" }] },
          ],
        },
        {
          name: "CUSTOMER_NAME",
          dataType: "VARCHAR",
          sources: [
            { columnReferences: [{ nodeID: "n_other", columnID: "c2" }] },
          ],
        },
        {
          name: "TOTAL",
          dataType: "DECIMAL",
          sources: [],
        },
      ],
    },
  };

  it("returns null when sourceColumnName is not provided on the select item", () => {
    expect(
      findMatchingBaseColumn(
        baseNode,
        makeSelectItem({ sourceColumnName: null })
      )
    ).toBeNull();
  });

  it("matches by normalized identifier (case-insensitive, quote-stripped)", () => {
    const result = findMatchingBaseColumn(
      baseNode,
      makeSelectItem({
        sourceColumnName: '"order_id"',
        sourceNodeID: "n_src",
      })
    );
    expect(result).toMatchObject({ name: "ORDER_ID", dataType: "NUMBER" });
  });

  it("requires sourceNodeID match when one is supplied", () => {
    expect(
      findMatchingBaseColumn(
        baseNode,
        makeSelectItem({
          sourceColumnName: "ORDER_ID",
          sourceNodeID: "n_other",
        })
      )
    ).toBeNull();
  });

  it("returns the column even with no sourceNodeID restriction", () => {
    const result = findMatchingBaseColumn(
      baseNode,
      makeSelectItem({ sourceColumnName: "ORDER_ID" })
    );
    expect(result).toMatchObject({ name: "ORDER_ID" });
  });

  it("returns a deep clone — caller mutations do not leak into the source node", () => {
    const result = findMatchingBaseColumn(
      baseNode,
      makeSelectItem({ sourceColumnName: "ORDER_ID" })
    );
    expect(result).not.toBeNull();
    (result as Record<string, unknown>).name = "MUTATED";
    const original = baseNode.metadata.columns[0]!;
    expect(original.name).toBe("ORDER_ID");
  });

  it("returns null when there are no metadata columns at all", () => {
    expect(
      findMatchingBaseColumn(
        { metadata: { columns: [] } },
        makeSelectItem({ sourceColumnName: "ORDER_ID" })
      )
    ).toBeNull();
  });

  it("returns null when no column name matches", () => {
    expect(
      findMatchingBaseColumn(
        baseNode,
        makeSelectItem({ sourceColumnName: "DOES_NOT_EXIST" })
      )
    ).toBeNull();
  });

  it("skips columns with non-string names", () => {
    const node = {
      metadata: {
        columns: [
          { name: 42, sources: [] },
          { name: "ORDER_ID", sources: [] },
        ],
      },
    };
    const result = findMatchingBaseColumn(
      node,
      makeSelectItem({ sourceColumnName: "ORDER_ID" })
    );
    expect(result).toMatchObject({ name: "ORDER_ID" });
  });
});

describe("renameSourceMappingEntries", () => {
  it("renames entries whose name matches the previous node name", () => {
    const node = {
      name: "OLD_NAME",
      metadata: {
        sourceMapping: [
          { name: "OLD_NAME", dependencies: [] },
          { name: "OTHER", dependencies: [] },
        ],
      },
    };
    const result = renameSourceMappingEntries(node, "NEW_NAME");
    const entries = (result.metadata as Record<string, unknown>).sourceMapping as Array<{ name: string }>;
    expect(entries[0]!.name).toBe("NEW_NAME");
    expect(entries[1]!.name).toBe("OTHER");
  });

  it("renames the single unnamed mapping when the node has no current name", () => {
    const node = {
      // no name field at all
      metadata: {
        sourceMapping: [{ aliases: {} }],
      },
    };
    const result = renameSourceMappingEntries(node, "NEW_NAME");
    const entries = (result.metadata as Record<string, unknown>).sourceMapping as Array<{ name: string }>;
    expect(entries[0]!.name).toBe("NEW_NAME");
  });

  it("does NOT rename multiple unnamed mappings (ambiguous)", () => {
    const node = {
      metadata: {
        sourceMapping: [
          { aliases: {} },
          { aliases: { other: "n_x" } },
        ],
      },
    };
    const result = renameSourceMappingEntries(node, "NEW_NAME");
    const entries = (result.metadata as Record<string, unknown>).sourceMapping as Array<{ name?: string }>;
    expect(entries[0]!.name).toBeUndefined();
    expect(entries[1]!.name).toBeUndefined();
  });

  it("returns the node unchanged when metadata is missing", () => {
    const node = { name: "X" };
    expect(renameSourceMappingEntries(node, "NEW")).toBe(node);
  });

  it("returns the node unchanged when sourceMapping is missing", () => {
    const node = { name: "X", metadata: {} };
    expect(renameSourceMappingEntries(node, "NEW")).toBe(node);
  });

  it("returns the node unchanged when sourceMapping is not an array", () => {
    const node = { name: "X", metadata: { sourceMapping: "x" } };
    expect(renameSourceMappingEntries(node, "NEW")).toBe(node);
  });

  it("preserves non-object entries verbatim", () => {
    const node = {
      name: "OLD",
      metadata: {
        sourceMapping: [{ name: "OLD" }, "weird-string-entry"],
      },
    };
    const result = renameSourceMappingEntries(node, "NEW");
    const entries = (result.metadata as Record<string, unknown>).sourceMapping as unknown[];
    expect((entries[0] as Record<string, unknown>).name).toBe("NEW");
    expect(entries[1]).toBe("weird-string-entry");
  });

  it("treats a whitespace-only previous name as missing (single-mapping rule applies)", () => {
    const node = {
      name: "   ",
      metadata: { sourceMapping: [{ aliases: {} }] },
    };
    const result = renameSourceMappingEntries(node, "NEW");
    const entries = (result.metadata as Record<string, unknown>).sourceMapping as Array<{ name: string }>;
    expect(entries[0]!.name).toBe("NEW");
  });
});

describe("buildStageSourceMappingFromPlan", () => {
  it("emits one mapping with name, dependencies, aliases, customSQL, and join scaffolding", () => {
    const plan = makePlannedNode({
      name: "STG_ORDERS",
      sourceRefs: [
        {
          locationName: "RAW",
          nodeName: "ORDERS",
          alias: null,
          nodeID: "n_orders",
        },
      ],
    });
    const [mapping] = buildStageSourceMappingFromPlan({}, plan);
    expect(mapping).toMatchObject({
      name: "STG_ORDERS",
      dependencies: [{ locationName: "RAW", nodeName: "ORDERS" }],
      aliases: {}, // single-source with no alias → no aliases entries
      customSQL: { customSQL: "" },
      join: { joinCondition: "" },
      noLinkRefs: [],
    });
  });

  it("populates aliases for multi-source plans", () => {
    const plan = makePlannedNode({
      sourceRefs: [
        { locationName: "RAW", nodeName: "ORDERS", alias: null, nodeID: "n1" },
        { locationName: "RAW", nodeName: "CUSTOMERS", alias: null, nodeID: "n2" },
      ],
    });
    const [mapping] = buildStageSourceMappingFromPlan({}, plan);
    expect((mapping as { aliases: Record<string, string> }).aliases).toEqual({
      ORDERS: "n1",
      CUSTOMERS: "n2",
    });
  });

  it("uses explicit alias when supplied even for single-source plans", () => {
    const plan = makePlannedNode({
      sourceRefs: [
        { locationName: "RAW", nodeName: "ORDERS", alias: "O", nodeID: "n1" },
      ],
    });
    const [mapping] = buildStageSourceMappingFromPlan({}, plan);
    expect((mapping as { aliases: Record<string, string> }).aliases).toEqual({
      O: "n1",
    });
  });

  it("skips aliases when the source ref has no nodeID", () => {
    const plan = makePlannedNode({
      sourceRefs: [
        { locationName: "RAW", nodeName: "ORDERS", alias: null, nodeID: null },
      ],
    });
    const [mapping] = buildStageSourceMappingFromPlan({}, plan);
    expect((mapping as { aliases: Record<string, string> }).aliases).toEqual({});
    // dependency should still appear (driven by nodeName/locationName, not nodeID)
    expect((mapping as { dependencies: unknown[] }).dependencies).toEqual([
      { locationName: "RAW", nodeName: "ORDERS" },
    ]);
  });

  it("preserves the existing join condition and customSQL block from the current node", () => {
    const currentNode = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: "EXISTING_JOIN" },
            customSQL: { customSQL: "EXISTING_SQL", extraField: "keep-me" },
            noLinkRefs: ["preserved"],
          },
        ],
      },
    };
    const plan = makePlannedNode({
      sourceRefs: [
        { locationName: "RAW", nodeName: "ORDERS", alias: null, nodeID: "n1" },
      ],
      joinCondition: null, // null → empty string in output
    });
    const [mapping] = buildStageSourceMappingFromPlan(currentNode, plan);
    expect((mapping as { join: { joinCondition: string } }).join.joinCondition).toBe("");
    expect((mapping as { customSQL: { customSQL: string; extraField?: string } }).customSQL).toEqual({
      customSQL: "",
      extraField: "keep-me",
    });
    expect((mapping as { noLinkRefs: unknown[] }).noLinkRefs).toEqual(["preserved"]);
  });

  it("uses the plan's joinCondition when provided", () => {
    const plan = makePlannedNode({
      sourceRefs: [
        { locationName: "RAW", nodeName: "ORDERS", alias: null, nodeID: "n1" },
      ],
      joinCondition: "a.id = b.id",
    });
    const [mapping] = buildStageSourceMappingFromPlan({}, plan);
    expect((mapping as { join: { joinCondition: string } }).join.joinCondition).toBe(
      "a.id = b.id"
    );
  });

  it("dedupes dependencies that differ only in nodeID", () => {
    const plan = makePlannedNode({
      sourceRefs: [
        { locationName: "RAW", nodeName: "ORDERS", alias: "A", nodeID: "n1" },
        { locationName: "RAW", nodeName: "ORDERS", alias: "B", nodeID: "n1b" },
      ],
    });
    const [mapping] = buildStageSourceMappingFromPlan({}, plan);
    // dependencies dedup on (locationName, nodeName) — both refs collapse to one
    expect((mapping as { dependencies: unknown[] }).dependencies).toEqual([
      { locationName: "RAW", nodeName: "ORDERS" },
    ]);
  });
});

describe("getColumnNamesFromNode (re-export of getNodeColumnNames)", () => {
  it("returns the names from metadata.columns", () => {
    const node = {
      metadata: { columns: [{ name: "A" }, { name: "B" }, { other: "skipped" }] },
    };
    expect(getColumnNamesFromNode(node)).toEqual(["A", "B"]);
  });

  it("returns [] when metadata is missing", () => {
    expect(getColumnNamesFromNode({})).toEqual([]);
  });
});
