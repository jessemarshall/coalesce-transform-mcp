import { describe, it, expect } from "vitest";
import { extractEntityName } from "../../src/mcp/tool-helpers.js";

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
