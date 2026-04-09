import { describe, it, expect } from "vitest";
import { clampWithWarning } from "../src/constants.js";

describe("clampWithWarning", () => {
  it("returns value unchanged when within range", () => {
    const result = clampWithWarning(10, 5, 20, "pollInterval");
    expect(result).toEqual({ value: 10 });
    expect(result.warning).toBeUndefined();
  });

  it("clamps value below minimum and returns warning", () => {
    const result = clampWithWarning(2, 5, 20, "pollInterval");
    expect(result.value).toBe(5);
    expect(result.warning).toBe(
      "pollInterval 2 is below the minimum (5); using 5 instead."
    );
  });

  it("clamps value above maximum and returns warning", () => {
    const result = clampWithWarning(50, 5, 20, "timeout");
    expect(result.value).toBe(20);
    expect(result.warning).toBe(
      "timeout 50 exceeds the maximum (20); using 20 instead."
    );
  });

  it("returns value at exact minimum boundary without warning", () => {
    const result = clampWithWarning(5, 5, 20, "pollInterval");
    expect(result).toEqual({ value: 5 });
    expect(result.warning).toBeUndefined();
  });

  it("returns value at exact maximum boundary without warning", () => {
    const result = clampWithWarning(20, 5, 20, "pollInterval");
    expect(result).toEqual({ value: 20 });
    expect(result.warning).toBeUndefined();
  });
});
