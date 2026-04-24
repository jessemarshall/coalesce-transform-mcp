import { describe, it, expect } from "vitest";
import { RunIDSchema } from "../../src/coalesce/run-schemas.js";

describe("RunIDSchema", () => {
  it("accepts a numeric string run ID", () => {
    expect(RunIDSchema.safeParse("401").success).toBe(true);
    expect(RunIDSchema.safeParse("0").success).toBe(true);
    expect(RunIDSchema.safeParse("1").success).toBe(true);
    expect(RunIDSchema.safeParse("999999").success).toBe(true);
  });

  it("rejects empty string", () => {
    const result = RunIDSchema.safeParse("");
    expect(result.success).toBe(false);
  });

  it("rejects non-numeric strings", () => {
    const cases = [
      "abc",
      "42abc",
      "abc42",
      "42 OR 1=1",
      "../../etc/passwd",
    ];
    for (const value of cases) {
      const result = RunIDSchema.safeParse(value);
      expect(result.success, `expected rejection of "${value}"`).toBe(false);
    }
  });

  it("rejects UUID-shaped strings (the common mistake the description warns about)", () => {
    const result = RunIDSchema.safeParse("3f2504e0-4f89-11d3-9a0c-0305e82c3301");
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues[0]?.message).toContain("UUID");
    }
  });

  it("rejects negative or decimal numeric strings", () => {
    expect(RunIDSchema.safeParse("-1").success).toBe(false);
    expect(RunIDSchema.safeParse("1.5").success).toBe(false);
  });

  it("rejects non-string inputs", () => {
    expect(RunIDSchema.safeParse(42).success).toBe(false);
    expect(RunIDSchema.safeParse(null).success).toBe(false);
    expect(RunIDSchema.safeParse(undefined).success).toBe(false);
  });
});
