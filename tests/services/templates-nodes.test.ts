import { describe, it, expect } from "vitest";
import {
  buildSetWorkspaceNodeTemplateFromDefinition,
  compareGeneratedTemplateToWorkspaceNode,
  renderYaml,
} from "../../src/services/templates/nodes.js";

function buildDefinitionWithItems(
  items: Array<Record<string, unknown>>,
  extras: Record<string, unknown> = {}
): Record<string, unknown> {
  return {
    capitalized: "Stage",
    short: "STG",
    plural: "Stages",
    tagColor: "#FFAA00",
    config: [
      {
        groupName: "Options",
        items,
      },
    ],
    ...extras,
  };
}

describe("buildSetWorkspaceNodeTemplateFromDefinition", () => {
  it("summarizes the definition and counts groups/items", () => {
    const definition = buildDefinitionWithItems([
      { type: "textBox", attributeName: "alias" },
      { type: "toggleButton", attributeName: "enableX" },
    ]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);

    expect(result.definitionSummary).toMatchObject({
      capitalized: "Stage",
      short: "STG",
      plural: "Stages",
      tagColor: "#FFAA00",
      configGroupCount: 1,
      configItemCount: 2,
    });
  });

  it("infers defaults for built-in selectors and toggles", () => {
    const definition = buildDefinitionWithItems([
      { type: "materializationSelector", options: [{ value: "view" }, { value: "table" }] },
      { type: "multisourceToggle" },
      { type: "toggleButton", attributeName: "enableLogging" },
      { type: "textBox", attributeName: "alias" },
    ]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);

    expect(result.inferredTopLevelFields).toMatchObject({
      materializationType: "view",
      isMultisource: false,
    });
    expect(result.inferredConfig).toMatchObject({
      enableLogging: false,
      alias: "",
    });
  });

  it("uses explicit default value when present", () => {
    const definition = buildDefinitionWithItems([
      { type: "textBox", attributeName: "alias", default: "MY_ALIAS" },
    ]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);
    expect(result.inferredConfig).toEqual({ alias: "MY_ALIAS" });
  });

  it("emits both top-level and config mapping when selector/toggle has attributeName", () => {
    const definition = buildDefinitionWithItems([
      {
        type: "multisourceToggle",
        attributeName: "isMultisourceCustom",
      },
    ]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);

    const targetPaths = result.fieldMappings.map((m) => m.targetPath);
    expect(targetPaths).toEqual(["isMultisource", "config.isMultisourceCustom"]);

    expect(result.inferredTopLevelFields).toEqual({ isMultisource: false });
    expect(result.inferredConfig).toEqual({ isMultisourceCustom: false });
  });

  it("records columnSelector items as informational and skips top-level setByPath", () => {
    const definition = buildDefinitionWithItems([
      { type: "columnSelector", attributeName: "isBusinessKey" },
    ]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);

    const mapping = result.fieldMappings[0];
    expect(mapping.targetPath).toBe("columns[].isBusinessKey");
    expect(mapping.itemType).toBe("columnSelector");

    expect(result.inferredTopLevelFields).toEqual({});
    expect(result.inferredConfig).toEqual({});
    expect(result.setWorkspaceNodeBodyTemplate).not.toHaveProperty("columns[]");
    expect(result.setWorkspaceNodeBodyTemplate.config).toEqual({});
  });

  it("warns when columnSelector has no attributeName", () => {
    const definition = buildDefinitionWithItems([{ type: "columnSelector" }]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);

    expect(result.warnings.some((w) => w.includes("does not map cleanly"))).toBe(true);
    expect(result.fieldMappings[0].targetPath).toBeNull();
  });

  it("warns when generic input lacks both attributeName and a built-in target", () => {
    const definition = buildDefinitionWithItems([{ type: "textBox" }]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);

    expect(result.warnings.some((w) => w.includes("does not map cleanly"))).toBe(true);
  });

  it("warns when an inferred default is undefined", () => {
    const definition = buildDefinitionWithItems([
      { type: "dropdownSelector", attributeName: "mode" },
    ]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);

    expect(
      result.warnings.some((w) => w.includes("config.mode") && w.includes("no inferred default"))
    ).toBe(true);
    expect(result.inferredConfig).toEqual({});
  });

  it("uses provided options for nodeName, nodeType, locationName, database, schema", () => {
    const definition = buildDefinitionWithItems([{ type: "textBox", attributeName: "alias" }]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition, {
      nodeName: "MY_NODE",
      nodeType: "Custom",
      locationName: "PROD",
      database: "DB",
      schema: "PUBLIC",
    });

    expect(result.setWorkspaceNodeBodyTemplate).toMatchObject({
      name: "MY_NODE",
      nodeType: "Custom",
      locationName: "PROD",
      database: "DB",
      schema: "PUBLIC",
    });
  });

  it("falls back to a name derived from short/capitalized when not provided", () => {
    const definition = buildDefinitionWithItems([], {
      capitalized: "Persistent",
      short: "PRS",
    });

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);
    expect(result.setWorkspaceNodeBodyTemplate.name).toBe("PRS_NODE");
  });

  it("includes baseline empty metadata arrays in the body template", () => {
    const definition = buildDefinitionWithItems([]);
    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);

    expect(result.setWorkspaceNodeBodyTemplate.metadata).toEqual({
      columns: [],
      sourceMapping: [],
      cteString: "",
      appliedNodeTests: [],
      enabledColumnTestIDs: [],
    });
  });

  it("strips overrideSQLToggle items via the SQL override policy sanitizer", () => {
    const definition = buildDefinitionWithItems([
      { type: "overrideSQLToggle", attributeName: "useOverride" },
      { type: "textBox", attributeName: "alias" },
    ]);

    const result = buildSetWorkspaceNodeTemplateFromDefinition(definition);

    expect(
      result.fieldMappings.some((m) => m.itemType === "overrideSQLToggle")
    ).toBe(false);
    expect(result.warnings.some((w) => w.includes("SQL override"))).toBe(true);
  });

  it("emits usageGuidance covering columnSelector handling", () => {
    const result = buildSetWorkspaceNodeTemplateFromDefinition(
      buildDefinitionWithItems([])
    );

    expect(
      result.usageGuidance.some(
        (line) => line.includes("columnSelector") && line.includes("metadata.columns")
      )
    ).toBe(true);
  });
});

describe("compareGeneratedTemplateToWorkspaceNode", () => {
  it("returns matched/mismatched/missing counts for top-level fields", () => {
    const generated = buildSetWorkspaceNodeTemplateFromDefinition(
      buildDefinitionWithItems([
        { type: "textBox", attributeName: "alias", default: "DEFAULT_ALIAS" },
        { type: "toggleButton", attributeName: "enableX", default: false },
        { type: "textBox", attributeName: "missing", default: "X" },
      ])
    );

    const workspaceNode = {
      config: {
        alias: "DEFAULT_ALIAS",
        enableX: true,
      },
    };

    const result = compareGeneratedTemplateToWorkspaceNode(generated, workspaceNode);

    expect(result.checkedFieldCount).toBe(3);
    expect(result.matchedFieldCount).toBe(1);
    expect(result.mismatchedFieldCount).toBe(1);
    expect(result.missingFieldCount).toBe(1);

    const byPath = Object.fromEntries(result.fields.map((f) => [f.targetPath, f]));
    expect(byPath["config.alias"].status).toBe("matched");
    expect(byPath["config.enableX"].status).toBe("mismatched");
    expect(byPath["config.missing"].status).toBe("missing");
  });

  it("excludes columns[] paths from the comparison entirely (read-side [] guard)", () => {
    const generated = buildSetWorkspaceNodeTemplateFromDefinition(
      buildDefinitionWithItems([
        { type: "columnSelector", attributeName: "isBusinessKey" },
        { type: "textBox", attributeName: "alias", default: "X" },
      ])
    );

    const workspaceNode = {
      config: { alias: "X" },
      metadata: {
        columns: [{ name: "ID", isBusinessKey: true }],
      },
    };

    const result = compareGeneratedTemplateToWorkspaceNode(generated, workspaceNode);

    expect(result.checkedFieldCount).toBe(1);
    expect(result.fields.every((f) => !f.targetPath.includes("[]"))).toBe(true);
    expect(result.fields[0].targetPath).toBe("config.alias");
  });

  it("treats objects with different key order as matched (order-insensitive deep equal)", () => {
    const generated = buildSetWorkspaceNodeTemplateFromDefinition(
      buildDefinitionWithItems([
        {
          type: "textBox",
          attributeName: "settings",
          default: { a: 1, b: 2, nested: { x: "x", y: "y" } },
        },
      ])
    );

    const workspaceNode = {
      config: {
        settings: { nested: { y: "y", x: "x" }, b: 2, a: 1 },
      },
    };

    const result = compareGeneratedTemplateToWorkspaceNode(generated, workspaceNode);

    expect(result.matchedFieldCount).toBe(1);
    expect(result.mismatchedFieldCount).toBe(0);
    expect(result.fields[0].status).toBe("matched");
  });

  it("treats arrays with different element order as mismatched", () => {
    const generated = buildSetWorkspaceNodeTemplateFromDefinition(
      buildDefinitionWithItems([
        { type: "textBox", attributeName: "tags", default: ["a", "b"] },
      ])
    );

    const workspaceNode = { config: { tags: ["b", "a"] } };

    const result = compareGeneratedTemplateToWorkspaceNode(generated, workspaceNode);
    expect(result.fields[0].status).toBe("mismatched");
  });

  it("flags both an absent key and an explicit-undefined value as missing", () => {
    const generated = buildSetWorkspaceNodeTemplateFromDefinition(
      buildDefinitionWithItems([
        { type: "textBox", attributeName: "alias", default: "A" },
      ])
    );

    const absent = compareGeneratedTemplateToWorkspaceNode(generated, { config: {} });
    expect(absent.fields[0].status).toBe("missing");
    expect(absent.fields[0].actualValue).toBeUndefined();

    const explicitUndefined = compareGeneratedTemplateToWorkspaceNode(generated, {
      config: { alias: undefined },
    });
    expect(explicitUndefined.fields[0].status).toBe("missing");
    expect(explicitUndefined.fields[0].actualValue).toBeUndefined();
  });

  it("checks both mappings independently when a built-in selector also has attributeName", () => {
    const generated = buildSetWorkspaceNodeTemplateFromDefinition(
      buildDefinitionWithItems([
        {
          type: "materializationSelector",
          attributeName: "matCustom",
          options: [{ value: "view" }, { value: "table" }],
        },
      ])
    );

    const workspaceNode = {
      materializationType: "view",
      config: { matCustom: "table" },
    };

    const result = compareGeneratedTemplateToWorkspaceNode(generated, workspaceNode);

    expect(result.checkedFieldCount).toBe(2);
    const byPath = Object.fromEntries(result.fields.map((f) => [f.targetPath, f]));
    expect(byPath["materializationType"].status).toBe("matched");
    expect(byPath["config.matCustom"].status).toBe("mismatched");
  });

  it("returns an empty result when the generated template has no field mappings", () => {
    const generated = buildSetWorkspaceNodeTemplateFromDefinition(
      buildDefinitionWithItems([])
    );

    const result = compareGeneratedTemplateToWorkspaceNode(generated, { config: {} });

    expect(result.checkedFieldCount).toBe(0);
    expect(result.fields).toEqual([]);
  });
});

describe("renderYaml", () => {
  it("renders scalars, arrays and objects deterministically", () => {
    const yaml = renderYaml({
      name: "MY_NODE",
      enabled: true,
      tags: ["a", "b"],
      nested: { count: 1 },
      empties: { obj: {}, arr: [] },
    });

    expect(yaml).toBe(
      [
        "name: MY_NODE",
        "enabled: true",
        "tags:",
        "  - a",
        "  - b",
        "nested:",
        "  count: 1",
        "empties:",
        "  obj:",
        "    {}",
        "  arr:",
        "    []",
        "",
      ].join("\n")
    );
  });

  it("quotes strings that contain unsafe characters", () => {
    const yaml = renderYaml({ description: "hello world" });
    expect(yaml).toBe('description: "hello world"\n');
  });

  it("renders empty string and null sentinels", () => {
    const yaml = renderYaml({ blank: "", missing: null });
    expect(yaml).toBe('blank: ""\nmissing: null\n');
  });

  it("renders arrays of objects with hyphen prefix", () => {
    const yaml = renderYaml([{ name: "A" }, { name: "B" }]);
    expect(yaml).toBe(
      ["-", "  name: A", "-", "  name: B", ""].join("\n")
    );
  });
});
