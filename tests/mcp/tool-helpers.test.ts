import { describe, it, expect } from "vitest";
import { extractEntityName, extractCacheScope } from "../../src/mcp/tool-helpers.js";

describe("extractEntityName", () => {
  it("returns name when present and non-empty", () => {
    expect(extractEntityName({ name: "Foo" })).toBe("Foo");
  });

  it("falls back to label when name is empty", () => {
    expect(extractEntityName({ name: "", label: "Bar" })).toBe("Bar");
  });

  it("falls back to displayName when name and label are empty", () => {
    expect(extractEntityName({ name: "", label: "", displayName: "Baz" })).toBe("Baz");
  });

  it("falls back to label when name is missing", () => {
    expect(extractEntityName({ label: "Only Label" })).toBe("Only Label");
  });

  it("falls back to displayName when name and label are missing", () => {
    expect(extractEntityName({ displayName: "Only Display" })).toBe("Only Display");
  });

  it("prefers name over label and displayName", () => {
    expect(extractEntityName({ name: "N", label: "L", displayName: "D" })).toBe("N");
  });

  it("prefers label over displayName when name is missing", () => {
    expect(extractEntityName({ label: "L", displayName: "D" })).toBe("L");
  });

  it("ignores non-string name values and falls through", () => {
    expect(extractEntityName({ name: 42, label: "FromLabel" })).toBe("FromLabel");
  });

  it("ignores null name and falls through", () => {
    expect(extractEntityName({ name: null, label: "FromLabel" })).toBe("FromLabel");
  });

  it("returns undefined when all candidates are empty strings", () => {
    expect(extractEntityName({ name: "", label: "", displayName: "" })).toBeUndefined();
  });

  it("returns undefined when no candidate keys exist", () => {
    expect(extractEntityName({ id: "abc" })).toBeUndefined();
  });

  it("returns undefined for null input", () => {
    expect(extractEntityName(null)).toBeUndefined();
  });

  it("returns undefined for undefined input", () => {
    expect(extractEntityName(undefined)).toBeUndefined();
  });

  it("returns undefined for string input", () => {
    expect(extractEntityName("just a string")).toBeUndefined();
  });

  it("returns undefined for number input", () => {
    expect(extractEntityName(123)).toBeUndefined();
  });

  it("returns undefined for array input", () => {
    // Arrays are objects in JS, but have no name/label/displayName fields.
    expect(extractEntityName(["x", "y"])).toBeUndefined();
  });
});

describe("extractCacheScope", () => {
  it("reads top-level workspaceID (workspace-scoped tools)", () => {
    expect(extractCacheScope({ workspaceID: "ws-1", nodeID: "n-1" })).toEqual({
      workspaceID: "ws-1",
    });
  });

  it("reads top-level environmentID (environment-scoped tools)", () => {
    expect(extractCacheScope({ environmentID: "env-9", nodeID: "n-1" })).toEqual({
      environmentID: "env-9",
    });
  });

  it("reads runDetails.workspaceID for run-task inputs", () => {
    expect(
      extractCacheScope({ runDetails: { workspaceID: "ws-2", runID: "r-1" } })
    ).toEqual({ workspaceID: "ws-2" });
  });

  it("reads runDetails.environmentID for run-task inputs", () => {
    expect(
      extractCacheScope({ runDetails: { environmentID: "env-2", runID: "r-1" } })
    ).toEqual({ environmentID: "env-2" });
  });

  it("prefers top-level workspaceID over runDetails", () => {
    expect(
      extractCacheScope({
        workspaceID: "ws-top",
        runDetails: { workspaceID: "ws-nested", environmentID: "env-nested" },
      })
    ).toEqual({ workspaceID: "ws-top" });
  });

  it("prefers workspaceID over environmentID at the same level", () => {
    expect(extractCacheScope({ workspaceID: "ws-1", environmentID: "env-1" })).toEqual({
      workspaceID: "ws-1",
    });
    expect(
      extractCacheScope({
        runDetails: { workspaceID: "ws-rd", environmentID: "env-rd" },
      })
    ).toEqual({ workspaceID: "ws-rd" });
  });

  it("returns empty for tenant-level tools (no workspace or environment)", () => {
    expect(extractCacheScope({ limit: 25 })).toEqual({});
    expect(extractCacheScope({})).toEqual({});
  });

  it("returns empty for non-object inputs", () => {
    expect(extractCacheScope(null)).toEqual({});
    expect(extractCacheScope(undefined)).toEqual({});
    expect(extractCacheScope("string")).toEqual({});
    expect(extractCacheScope(["array"])).toEqual({});
  });

  it("ignores non-string ID fields", () => {
    expect(extractCacheScope({ workspaceID: 42 })).toEqual({});
    expect(extractCacheScope({ environmentID: null })).toEqual({});
    expect(extractCacheScope({ workspaceID: "" })).toEqual({});
  });
});
