import { describe, it, expect } from "vitest";
import {
  extractNodeArray,
  isPassthroughTransform,
  cloneValue,
} from "../../src/services/shared/node-helpers.js";

describe("extractNodeArray", () => {
  it("extracts from a bare array", () => {
    const result = extractNodeArray([{ id: "1" }, { id: "2" }]);
    expect(result).toHaveLength(2);
    expect(result[0].id).toBe("1");
  });

  it("extracts from a { data: [...] } wrapper", () => {
    const result = extractNodeArray({ data: [{ id: "1" }] });
    expect(result).toHaveLength(1);
  });

  it("filters out non-object entries", () => {
    const result = extractNodeArray([{ id: "1" }, "not-an-object", 42, null]);
    expect(result).toHaveLength(1);
  });

  it("returns empty array for null/undefined/primitives", () => {
    expect(extractNodeArray(null)).toEqual([]);
    expect(extractNodeArray(undefined)).toEqual([]);
    expect(extractNodeArray("string")).toEqual([]);
    expect(extractNodeArray(42)).toEqual([]);
  });

  it("returns empty array for object without data array", () => {
    expect(extractNodeArray({ other: "field" })).toEqual([]);
  });
});

describe("isPassthroughTransform", () => {
  it("treats empty string as passthrough", () => {
    expect(isPassthroughTransform("", "COL")).toBe(true);
    expect(isPassthroughTransform("   ", "COL")).toBe(true);
  });

  it("treats bare column name as passthrough", () => {
    expect(isPassthroughTransform("MY_COL", "MY_COL")).toBe(true);
    expect(isPassthroughTransform("my_col", "MY_COL")).toBe(true);
  });

  it("treats quoted bare name as passthrough", () => {
    expect(isPassthroughTransform('"MY_COL"', "MY_COL")).toBe(true);
  });

  it('treats "ALIAS"."COL" as passthrough', () => {
    expect(isPassthroughTransform('"SRC_TABLE"."MY_COL"', "MY_COL")).toBe(true);
  });

  it("treats {{ ref(...) }}.\"COL\" as passthrough", () => {
    expect(
      isPassthroughTransform(
        "{{ ref('LOC', 'SRC') }}.\"MY_COL\"",
        "MY_COL"
      )
    ).toBe(true);
  });

  it("does NOT treat actual transforms as passthrough", () => {
    expect(isPassthroughTransform("UPPER(MY_COL)", "MY_COL")).toBe(false);
    expect(isPassthroughTransform("CONCAT(A, B)", "MY_COL")).toBe(false);
    expect(isPassthroughTransform("1 + 1", "MY_COL")).toBe(false);
  });

  it("is case-insensitive", () => {
    expect(isPassthroughTransform("my_col", "MY_COL")).toBe(true);
    expect(isPassthroughTransform("MY_COL", "my_col")).toBe(true);
  });
});

describe("cloneValue", () => {
  it("deep-clones an object", () => {
    const original = { a: { b: [1, 2, 3] } };
    const cloned = cloneValue(original);
    expect(cloned).toEqual(original);
    expect(cloned).not.toBe(original);
    expect(cloned.a).not.toBe(original.a);
  });

  it("deep-clones an array", () => {
    const original = [{ id: 1 }, { id: 2 }];
    const cloned = cloneValue(original);
    expect(cloned).toEqual(original);
    expect(cloned[0]).not.toBe(original[0]);
  });

  it("handles primitives", () => {
    expect(cloneValue(42)).toBe(42);
    expect(cloneValue("hello")).toBe("hello");
    expect(cloneValue(null)).toBe(null);
    expect(cloneValue(true)).toBe(true);
  });
});
