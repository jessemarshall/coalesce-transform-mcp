import { describe, it, expect } from "vitest";
import {
  filterSqlOverrideControls,
  sanitizeNodeDefinitionSqlOverridePolicy,
  assertNoSqlOverridePayload,
} from "../../src/services/policies/sql-override.js";

describe("filterSqlOverrideControls", () => {
  it("removes overrideSQLToggle from the array", () => {
    expect(filterSqlOverrideControls(["text", "overrideSQLToggle", "toggle"])).toEqual([
      "text",
      "toggle",
    ]);
  });

  it("returns the same array when no overrideSQLToggle present", () => {
    expect(filterSqlOverrideControls(["text", "toggle"])).toEqual(["text", "toggle"]);
  });

  it("returns empty array from empty input", () => {
    expect(filterSqlOverrideControls([])).toEqual([]);
  });
});

describe("sanitizeNodeDefinitionSqlOverridePolicy", () => {
  it("removes overrideSQLToggle config items", () => {
    const def = {
      config: [
        {
          groupName: "Options",
          items: [
            { type: "overrideSQLToggle", name: "toggle1" },
            { type: "text", name: "keepMe" },
          ],
        },
      ],
    };
    const result = sanitizeNodeDefinitionSqlOverridePolicy(def);
    expect(result.warnings).toHaveLength(1);
    expect(result.warnings[0]).toContain("Removed 1 SQL override control");
    const items = (result.nodeDefinition.config as any[])[0].items;
    expect(items).toHaveLength(1);
    expect(items[0].name).toBe("keepMe");
  });

  it("removes empty config groups after filtering", () => {
    const def = {
      config: [
        {
          groupName: "SQLOnly",
          items: [{ type: "overrideSQLToggle", name: "toggle1" }],
        },
      ],
    };
    const result = sanitizeNodeDefinitionSqlOverridePolicy(def);
    expect(result.nodeDefinition.config).toEqual([]);
    expect(result.warnings).toHaveLength(1);
  });

  it("rewrites node.override.* references in enableIf/disableIf expressions", () => {
    const def = {
      config: [
        {
          groupName: "Options",
          items: [
            {
              type: "text",
              name: "field1",
              enableIf: "node.override.sqlEnabled",
            },
          ],
        },
      ],
    };
    const result = sanitizeNodeDefinitionSqlOverridePolicy(def);
    expect(result.warnings).toHaveLength(1);
    expect(result.warnings[0]).toContain("Rewrote 1 conditional expression");
    const items = (result.nodeDefinition.config as any[])[0].items;
    expect(items[0].enableIf).toBe("false");
  });

  it("does not mutate the input", () => {
    const def = {
      config: [
        {
          groupName: "Options",
          items: [{ type: "overrideSQLToggle", name: "toggle1" }],
        },
      ],
    };
    const originalJson = JSON.stringify(def);
    sanitizeNodeDefinitionSqlOverridePolicy(def);
    expect(JSON.stringify(def)).toBe(originalJson);
  });

  it("returns no warnings when definition has no SQL override content", () => {
    const def = {
      config: [
        {
          groupName: "Options",
          items: [{ type: "text", name: "field1" }],
        },
      ],
    };
    const result = sanitizeNodeDefinitionSqlOverridePolicy(def);
    expect(result.warnings).toEqual([]);
  });
});

describe("assertNoSqlOverridePayload", () => {
  it("does nothing for clean payloads", () => {
    expect(() =>
      assertNoSqlOverridePayload({ name: "NODE_A", config: {} }, "test")
    ).not.toThrow();
  });

  it("throws when overrideSQL is present at top level", () => {
    expect(() =>
      assertNoSqlOverridePayload({ overrideSQL: true }, "test_context")
    ).toThrow("test_context cannot set SQL override fields");
  });

  it("throws when override is present at top level", () => {
    expect(() =>
      assertNoSqlOverridePayload({ override: {} }, "test_context")
    ).toThrow("test_context cannot set SQL override fields");
  });

  it("throws when overrideSQL is nested in metadata", () => {
    expect(() =>
      assertNoSqlOverridePayload(
        { metadata: { overrideSQL: "SELECT 1" } },
        "test_context"
      )
    ).toThrow("test_context cannot set SQL override fields");
  });

  it("lists all offending paths in the error", () => {
    try {
      assertNoSqlOverridePayload(
        { overrideSQL: true, nested: { override: {} } },
        "ctx"
      );
      expect.fail("should have thrown");
    } catch (error: any) {
      expect(error.message).toContain("overrideSQL");
      expect(error.message).toContain("nested.override");
    }
  });

  it("handles arrays with nested override fields", () => {
    expect(() =>
      assertNoSqlOverridePayload(
        { items: [{ overrideSQL: true }] },
        "test_context"
      )
    ).toThrow("items[0].overrideSQL");
  });

  it("does not throw for null/undefined/primitive values", () => {
    expect(() => assertNoSqlOverridePayload(null, "ctx")).not.toThrow();
    expect(() => assertNoSqlOverridePayload(undefined, "ctx")).not.toThrow();
    expect(() => assertNoSqlOverridePayload("string", "ctx")).not.toThrow();
    expect(() => assertNoSqlOverridePayload(42, "ctx")).not.toThrow();
  });
});
