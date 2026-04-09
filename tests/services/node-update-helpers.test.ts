import { describe, it, expect, vi } from "vitest";

vi.mock("node:crypto", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:crypto")>();
  let counter = 0;
  return {
    ...actual,
    randomUUID: vi.fn(() => `00000000-0000-0000-0000-${String(++counter).padStart(12, "0")}`),
  };
});

import {
  mergeWorkspaceNodeChanges,
  buildUpdatedWorkspaceNodeBody,
  syncNodeNameIntoMetadataSourceMapping,
  reconcileExternalSchema,
  assertNotSourceNodeType,
} from "../../src/services/workspace/node-update-helpers.js";

// ---------------------------------------------------------------------------
// mergeWorkspaceNodeChanges
// ---------------------------------------------------------------------------
describe("mergeWorkspaceNodeChanges", () => {
  it("merges top-level string fields", () => {
    const current = { name: "OLD", description: "old desc", nodeType: "Stage" };
    const changes = { name: "NEW", description: "new desc" };
    const result = mergeWorkspaceNodeChanges(current, changes);
    expect(result).toEqual({ name: "NEW", description: "new desc", nodeType: "Stage" });
  });

  it("replaces arrays rather than appending", () => {
    const current = { metadata: { columns: [{ name: "A" }, { name: "B" }] } };
    const changes = { metadata: { columns: [{ name: "X" }] } };
    const result = mergeWorkspaceNodeChanges(current, changes) as any;
    expect(result.metadata.columns).toEqual([{ name: "X" }]);
  });

  it("deep merges nested objects", () => {
    const current = { metadata: { sourceMapping: [{ name: "SRC" }], extra: "keep" } };
    const changes = { metadata: { sourceMapping: [{ name: "NEW" }] } };
    const result = mergeWorkspaceNodeChanges(current, changes) as any;
    expect(result.metadata.sourceMapping).toEqual([{ name: "NEW" }]);
    expect(result.metadata.extra).toBe("keep");
  });

  it("returns changes directly when current is not an object", () => {
    expect(mergeWorkspaceNodeChanges("string", { a: 1 })).toEqual({ a: 1 });
    expect(mergeWorkspaceNodeChanges(null, { a: 1 })).toEqual({ a: 1 });
    expect(mergeWorkspaceNodeChanges(42, { a: 1 })).toEqual({ a: 1 });
  });

  it("handles undefined values in changes", () => {
    const current = { name: "A", description: "B" };
    const changes = { description: undefined };
    const result = mergeWorkspaceNodeChanges(current, changes) as any;
    expect(result.description).toBeUndefined();
    expect(result.name).toBe("A");
  });

  it("returns changes when changes is an array", () => {
    const result = mergeWorkspaceNodeChanges({ a: 1 }, [1, 2, 3]);
    expect(result).toEqual([1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// syncNodeNameIntoMetadataSourceMapping
// ---------------------------------------------------------------------------
describe("syncNodeNameIntoMetadataSourceMapping", () => {
  it("renames matching sourceMapping entry when node name changes", () => {
    const current: Record<string, unknown> = {
      name: "OLD_NODE",
      metadata: { sourceMapping: [{ name: "OLD_NODE", alias: "x" }] },
    };
    const merged: Record<string, unknown> = {
      name: "NEW_NODE",
      metadata: { sourceMapping: [{ name: "OLD_NODE", alias: "x" }] },
    };
    const changes = { name: "NEW_NODE" };
    const result = syncNodeNameIntoMetadataSourceMapping(current, merged, changes);
    const meta = result.metadata as any;
    expect(meta.sourceMapping[0].name).toBe("NEW_NODE");
    expect(meta.sourceMapping[0].alias).toBe("x");
  });

  it("skips when changes include explicit sourceMapping", () => {
    const current: Record<string, unknown> = {
      name: "OLD",
      metadata: { sourceMapping: [{ name: "OLD" }] },
    };
    const merged: Record<string, unknown> = {
      name: "NEW",
      metadata: { sourceMapping: [{ name: "CUSTOM" }] },
    };
    const changes = { name: "NEW", metadata: { sourceMapping: [{ name: "CUSTOM" }] } };
    const result = syncNodeNameIntoMetadataSourceMapping(current, merged, changes);
    const meta = result.metadata as any;
    expect(meta.sourceMapping[0].name).toBe("CUSTOM");
  });

  it("updates single unnamed mapping when no previous name", () => {
    const current: Record<string, unknown> = {
      name: "",
      metadata: { sourceMapping: [{ name: "SOMETHING", alias: "a" }] },
    };
    const merged: Record<string, unknown> = {
      name: "BRAND_NEW",
      metadata: { sourceMapping: [{ name: "SOMETHING", alias: "a" }] },
    };
    const changes = { name: "BRAND_NEW" };
    const result = syncNodeNameIntoMetadataSourceMapping(current, merged, changes);
    const meta = result.metadata as any;
    expect(meta.sourceMapping[0].name).toBe("BRAND_NEW");
  });

  it("does nothing when name is not in changes", () => {
    const current: Record<string, unknown> = {
      name: "NODE",
      metadata: { sourceMapping: [{ name: "NODE" }] },
    };
    const merged: Record<string, unknown> = {
      name: "NODE",
      metadata: { sourceMapping: [{ name: "NODE" }] },
    };
    const changes = { description: "updated desc" };
    const result = syncNodeNameIntoMetadataSourceMapping(current, merged, changes);
    expect(result).toBe(merged); // returns same reference
  });

  it("handles missing sourceMapping gracefully", () => {
    const current: Record<string, unknown> = { name: "OLD" };
    const merged: Record<string, unknown> = { name: "NEW" };
    const changes = { name: "NEW" };
    const result = syncNodeNameIntoMetadataSourceMapping(current, merged, changes);
    expect(result).toBe(merged);
  });

  it("handles empty sourceMapping array", () => {
    const current: Record<string, unknown> = {
      name: "OLD",
      metadata: { sourceMapping: [] },
    };
    const merged: Record<string, unknown> = {
      name: "NEW",
      metadata: { sourceMapping: [] },
    };
    const changes = { name: "NEW" };
    const result = syncNodeNameIntoMetadataSourceMapping(current, merged, changes);
    const meta = result.metadata as any;
    expect(meta.sourceMapping).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// buildUpdatedWorkspaceNodeBody
// ---------------------------------------------------------------------------
describe("buildUpdatedWorkspaceNodeBody", () => {
  const baseNode = () => ({
    name: "MY_NODE",
    nodeType: "Stage",
    materializationType: "table",
    table: "MY_NODE",
    overrideSQL: "SELECT 1",
    metadata: {
      columns: [
        {
          name: "ID",
          columnID: "col-1",
          dataType: "NUMBER",
          nullable: false,
          description: "Primary key",
          sources: [{ transform: "", columnReferences: [] }],
          columnReference: { columnCounter: 1 },
          placement: { x: 0, y: 0 },
        },
      ],
      sourceMapping: [{ name: "MY_NODE" }],
      enabledColumnTestIDs: ["test-1"],
      appliedNodeTests: ["should-be-stripped"],
      cteString: "should-be-stripped",
    },
  });

  it("performs basic merge with name change", () => {
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), { name: "RENAMED" });
    expect(result.name).toBe("RENAMED");
    expect(result.nodeType).toBe("Stage");
  });

  it("strips invalid metadata fields (appliedNodeTests, cteString)", () => {
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), { name: "X" });
    const meta = result.metadata as any;
    expect(meta.appliedNodeTests).toBeUndefined();
    expect(meta.cteString).toBeUndefined();
    expect(meta.columns).toBeDefined();
    expect(meta.sourceMapping).toBeDefined();
    expect(meta.enabledColumnTestIDs).toBeDefined();
  });

  it("preserves table field from current node", () => {
    const current = baseNode();
    const result = buildUpdatedWorkspaceNodeBody(current, { name: "X" });
    expect(result.table).toBe("MY_NODE");
  });

  it("falls back to node name when table is missing", () => {
    const current = baseNode();
    delete (current as any).table;
    const result = buildUpdatedWorkspaceNodeBody(current, { name: "FALLBACK_NAME" });
    expect(result.table).toBe("FALLBACK_NAME");
  });

  it("preserves overrideSQL from current node", () => {
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), { name: "X" });
    expect(result.overrideSQL).toBe("SELECT 1");
  });

  it("preserves column linkage (columnID, sources, columnReference, placement)", () => {
    const changes = {
      metadata: {
        columns: [{ name: "ID", dataType: "INTEGER" }],
      },
    };
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), changes);
    const cols = (result.metadata as any).columns;
    expect(cols[0].columnID).toBe("col-1");
    expect(cols[0].sources).toBeDefined();
    expect(cols[0].columnReference).toEqual({ columnCounter: 1 });
    expect(cols[0].placement).toEqual({ x: 0, y: 0 });
  });

  it("generates UUID for new columns without columnID", () => {
    const changes = {
      metadata: {
        columns: [{ name: "BRAND_NEW_COL", dataType: "VARCHAR" }],
      },
    };
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), changes);
    const cols = (result.metadata as any).columns;
    const newCol = cols.find((c: any) => c.name === "BRAND_NEW_COL");
    expect(newCol.columnID).toMatch(/^00000000-/);
  });

  it("sets column defaults (nullable=true, description='', dataType='VARCHAR')", () => {
    const changes = {
      metadata: {
        columns: [{ name: "BARE_COL" }],
      },
    };
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), changes);
    const cols = (result.metadata as any).columns;
    const col = cols.find((c: any) => c.name === "BARE_COL");
    expect(col.nullable).toBe(true);
    expect(col.description).toBe("");
    expect(col.dataType).toBe("VARCHAR");
  });

  it("strips passthrough transforms", () => {
    const changes = {
      metadata: {
        columns: [{ name: "ID", dataType: "NUMBER", transform: '"SRC"."ID"' }],
      },
    };
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), changes);
    const cols = (result.metadata as any).columns;
    expect(cols[0].transform).toBeUndefined();
  });

  it("strips invalid column properties (primaryKey, foreignKey, unique, index)", () => {
    const changes = {
      metadata: {
        columns: [
          {
            name: "ID",
            dataType: "NUMBER",
            primaryKey: true,
            foreignKey: "other.id",
            unique: true,
            index: true,
          },
        ],
      },
    };
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), changes);
    const cols = (result.metadata as any).columns;
    expect(cols[0].primaryKey).toBeUndefined();
    expect(cols[0].foreignKey).toBeUndefined();
    expect(cols[0].unique).toBeUndefined();
    expect(cols[0].index).toBeUndefined();
  });

  it("throws for View + table materializationType", () => {
    const current = baseNode();
    current.nodeType = "View";
    current.materializationType = "view";
    expect(() =>
      buildUpdatedWorkspaceNodeBody(current, { materializationType: "table" })
    ).toThrow(/View.*cannot use materializationType.*table/);
  });

  it("throws when current is not an object", () => {
    expect(() => buildUpdatedWorkspaceNodeBody("not-an-object" as any, {})).toThrow(
      "Workspace node response was not an object"
    );
    expect(() => buildUpdatedWorkspaceNodeBody(null as any, {})).toThrow(
      "Workspace node response was not an object"
    );
  });

  it("preserves enabledColumnTestIDs from current node", () => {
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), { name: "X" });
    const meta = result.metadata as any;
    expect(meta.enabledColumnTestIDs).toEqual(["test-1"]);
  });

  it("cleans backslash-escaped quotes from transforms", () => {
    const changes = {
      metadata: {
        columns: [{ name: "CALC", dataType: "VARCHAR", transform: 'UPPER(\\"SRC\\".\\"COL\\")' }],
      },
    };
    const result = buildUpdatedWorkspaceNodeBody(baseNode(), changes);
    const cols = (result.metadata as any).columns;
    const calc = cols.find((c: any) => c.name === "CALC");
    expect(calc.transform).not.toContain("\\");
  });
});

// ---------------------------------------------------------------------------
// reconcileExternalSchema
// ---------------------------------------------------------------------------
describe("reconcileExternalSchema", () => {
  it("matches columns case-insensitively", () => {
    const auto = [
      { name: "id", columnID: "c1", dataType: "NUMBER", sources: [{ transform: "" }] },
    ];
    const target = [{ name: "ID", dataType: "INTEGER" }];
    const { columns, reconciliation } = reconcileExternalSchema(auto, target);
    expect(columns).toHaveLength(1);
    expect(columns[0].name).toBe("ID");
    expect(columns[0].dataType).toBe("INTEGER");
    expect(reconciliation.matched).toHaveLength(1);
  });

  it("preserves source linkage from existing columns", () => {
    const auto = [
      {
        name: "COL_A",
        columnID: "c1",
        dataType: "VARCHAR",
        sources: [{ transform: "src", columnReferences: [] }],
        columnReference: { counter: 1 },
      },
    ];
    const target = [{ name: "COL_A", dataType: "VARCHAR" }];
    const { columns } = reconcileExternalSchema(auto, target);
    expect(columns[0].columnID).toBe("c1");
    expect(columns[0].sources).toBeDefined();
    expect(columns[0].columnReference).toBeDefined();
  });

  it("adds new columns with needsTransform flag", () => {
    const auto = [{ name: "EXISTING", columnID: "c1", dataType: "VARCHAR" }];
    const target = [
      { name: "EXISTING", dataType: "VARCHAR" },
      { name: "BRAND_NEW", dataType: "INTEGER" },
    ];
    const { columns, reconciliation } = reconcileExternalSchema(auto, target);
    expect(columns).toHaveLength(2);
    expect(reconciliation.added).toHaveLength(1);
    expect(reconciliation.added[0].name).toBe("BRAND_NEW");
    expect(reconciliation.added[0].needsTransform).toBe(true);
  });

  it("marks needsTransform=false when transform is provided", () => {
    const auto: Record<string, unknown>[] = [];
    const target = [{ name: "CALC", dataType: "NUMBER", transform: "A + B" }];
    const { reconciliation } = reconcileExternalSchema(auto, target);
    expect(reconciliation.added[0].needsTransform).toBe(false);
  });

  it("drops predecessor columns not in target", () => {
    const auto = [
      { name: "KEEP", columnID: "c1", dataType: "VARCHAR" },
      { name: "DROP_ME", columnID: "c2", dataType: "VARCHAR" },
    ];
    const target = [{ name: "KEEP", dataType: "VARCHAR" }];
    const { columns, reconciliation } = reconcileExternalSchema(auto, target);
    expect(columns).toHaveLength(1);
    expect(reconciliation.dropped).toHaveLength(1);
    expect(reconciliation.dropped[0].name).toBe("DROP_ME");
  });

  it("detects type changes", () => {
    const auto = [{ name: "COL", columnID: "c1", dataType: "VARCHAR" }];
    const target = [{ name: "COL", dataType: "NUMBER" }];
    const { reconciliation } = reconcileExternalSchema(auto, target);
    expect(reconciliation.typeChanges).toHaveLength(1);
    expect(reconciliation.typeChanges[0]).toEqual({ name: "COL", from: "VARCHAR", to: "NUMBER" });
  });

  it("handles empty inputs", () => {
    const { columns, reconciliation } = reconcileExternalSchema([], []);
    expect(columns).toEqual([]);
    expect(reconciliation.matched).toEqual([]);
    expect(reconciliation.added).toEqual([]);
    expect(reconciliation.dropped).toEqual([]);
    expect(reconciliation.typeChanges).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// assertNotSourceNodeType
// ---------------------------------------------------------------------------
describe("assertNotSourceNodeType", () => {
  it("throws for 'Source'", () => {
    expect(() => assertNotSourceNodeType("Source")).toThrow(/Cannot create a node with nodeType "Source"/);
  });

  it("throws for 'source' (lowercase)", () => {
    expect(() => assertNotSourceNodeType("source")).toThrow(/Cannot create a node/);
  });

  it("throws for package-prefixed 'Package:::Source'", () => {
    expect(() => assertNotSourceNodeType("SomePackage:::Source")).toThrow(/Cannot create a node/);
  });

  it("allows 'Stage'", () => {
    expect(() => assertNotSourceNodeType("Stage")).not.toThrow();
  });

  it("allows 'Work'", () => {
    expect(() => assertNotSourceNodeType("Work")).not.toThrow();
  });

  it("allows 'Dimension'", () => {
    expect(() => assertNotSourceNodeType("Dimension")).not.toThrow();
  });

  it("allows 'View'", () => {
    expect(() => assertNotSourceNodeType("View")).not.toThrow();
  });

  it("allows 'Fact'", () => {
    expect(() => assertNotSourceNodeType("Fact")).not.toThrow();
  });
});
