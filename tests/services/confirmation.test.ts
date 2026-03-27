import { describe, it, expect } from "vitest";
import {
  sortJsonValue,
  buildPlanConfirmationToken,
} from "../../src/services/pipelines/confirmation.js";

describe("sortJsonValue", () => {
  it("sorts object keys alphabetically", () => {
    const result = sortJsonValue({ z: 1, a: 2, m: 3 });
    expect(Object.keys(result as Record<string, unknown>)).toEqual(["a", "m", "z"]);
  });

  it("sorts nested objects recursively", () => {
    const result = sortJsonValue({ b: { d: 1, c: 2 }, a: 3 }) as any;
    expect(Object.keys(result)).toEqual(["a", "b"]);
    expect(Object.keys(result.b)).toEqual(["c", "d"]);
  });

  it("preserves array order while sorting objects inside", () => {
    const result = sortJsonValue([{ z: 1, a: 2 }, { b: 3 }]) as any[];
    expect(Object.keys(result[0])).toEqual(["a", "z"]);
    expect(result[1]).toEqual({ b: 3 });
  });

  it("returns primitives unchanged", () => {
    expect(sortJsonValue(42)).toBe(42);
    expect(sortJsonValue("hello")).toBe("hello");
    expect(sortJsonValue(null)).toBe(null);
    expect(sortJsonValue(true)).toBe(true);
  });

  it("strips undefined values from objects", () => {
    const result = sortJsonValue({ a: 1, b: undefined, c: 3 });
    expect(result).toEqual({ a: 1, c: 3 });
  });
});

describe("buildPlanConfirmationToken", () => {
  it("returns a 16-character hex string", () => {
    const token = buildPlanConfirmationToken({ nodes: [] });
    expect(token).toMatch(/^[0-9a-f]{16}$/);
  });

  it("returns the same token for structurally identical plans", () => {
    const plan1 = { nodes: [{ id: "1", name: "A" }], status: "ready" };
    const plan2 = { nodes: [{ id: "1", name: "A" }], status: "ready" };
    expect(buildPlanConfirmationToken(plan1)).toBe(
      buildPlanConfirmationToken(plan2)
    );
  });

  it("returns the same token regardless of key order", () => {
    const plan1 = { status: "ready", nodes: [] };
    const plan2 = { nodes: [], status: "ready" };
    expect(buildPlanConfirmationToken(plan1)).toBe(
      buildPlanConfirmationToken(plan2)
    );
  });

  it("returns different tokens for different plans", () => {
    const plan1 = { nodes: [{ id: "1" }] };
    const plan2 = { nodes: [{ id: "2" }] };
    expect(buildPlanConfirmationToken(plan1)).not.toBe(
      buildPlanConfirmationToken(plan2)
    );
  });
});
