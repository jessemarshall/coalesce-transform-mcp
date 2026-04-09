import { describe, it, expect } from "vitest";
import { classifyConfigFields } from "../../../src/services/config/field-classifier.js";

describe("classifyConfigFields", () => {
  it("classifies required fields with isRequired: true", () => {
    const config = [
      {
        groupName: "Storage",
        items: [
          {
            attributeName: "database",
            type: "textBox",
            isRequired: true,
          },
          {
            attributeName: "schema",
            type: "textBox",
            isRequired: true,
          },
        ],
      },
    ];

    const result = classifyConfigFields(config);

    expect(result.required).toEqual(["database", "schema"]);
    expect(result.conditionalRequired).toEqual([]);
    expect(result.optionalWithDefaults).toEqual([]);
    expect(result.contextual).toEqual([]);
  });

  it("skips items without attributeName", () => {
    const config = [
      {
        groupName: "Options",
        items: [
          {
            type: "materializationSelector",
            default: "table",
          },
          {
            attributeName: "database",
            type: "textBox",
            isRequired: true,
          },
        ],
      },
    ];

    const result = classifyConfigFields(config);

    expect(result.required).toEqual(["database"]);
  });

  it("classifies all four field types correctly", () => {
    const config = [
      {
        groupName: "Storage",
        items: [
          {
            attributeName: "database",
            type: "textBox",
            isRequired: true,
          },
          {
            attributeName: "schema",
            type: "textBox",
            isRequired: "{% if node.materializationType == 'table' %} true {% endif %}",
          },
          {
            attributeName: "truncateBefore",
            type: "toggleButton",
            default: false,
          },
          {
            attributeName: "clusterKeyExpressions",
            type: "textBox",
          },
        ],
      },
    ];

    const result = classifyConfigFields(config);

    expect(result.required).toEqual(["database"]);
    expect(result.conditionalRequired).toEqual(["schema"]);
    expect(result.optionalWithDefaults).toEqual(["truncateBefore"]);
    expect(result.contextual).toEqual(["clusterKeyExpressions"]);
    expect(result.columnSelectors).toEqual([]);
  });

  it("separates columnSelector items into columnSelectors array", () => {
    const config = [
      {
        groupName: "Options",
        items: [
          {
            attributeName: "truncateBefore",
            type: "toggleButton",
            default: true,
          },
          {
            attributeName: "isBusinessKey",
            type: "columnSelector",
            displayName: "Business Key",
            isRequired: true,
          },
          {
            attributeName: "isChangeTracking",
            type: "columnSelector",
            displayName: "Change Tracking",
          },
        ],
      },
    ];

    const result = classifyConfigFields(config);

    expect(result.optionalWithDefaults).toEqual(["truncateBefore"]);
    expect(result.columnSelectors).toEqual([
      { attributeName: "isBusinessKey", displayName: "Business Key", isRequired: true },
      { attributeName: "isChangeTracking", displayName: "Change Tracking", isRequired: false },
    ]);
    // columnSelectors should NOT appear in other classifications
    expect(result.required).toEqual([]);
    expect(result.contextual).toEqual([]);
  });
});
