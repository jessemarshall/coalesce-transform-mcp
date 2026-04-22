import { describe, it, expect } from "vitest";
import { CoalesceApiError } from "../src/client.js";
import {
  isPlainObject,
  rethrowNonRecoverableApiError,
  rethrowNonRecoverableOrServerError,
  uniqueInOrder,
  sanitizeForFilename,
} from "../src/utils.js";

describe("isPlainObject", () => {
  it("returns true for a plain object literal", () => {
    expect(isPlainObject({ a: 1 })).toBe(true);
  });

  it("returns true for an empty object", () => {
    expect(isPlainObject({})).toBe(true);
  });

  it("returns false for null", () => {
    expect(isPlainObject(null)).toBe(false);
  });

  it("returns false for an array", () => {
    expect(isPlainObject([1, 2, 3])).toBe(false);
  });

  it("returns false for an empty array", () => {
    expect(isPlainObject([])).toBe(false);
  });

  it("returns false for a string", () => {
    expect(isPlainObject("hello")).toBe(false);
  });

  it("returns false for a number", () => {
    expect(isPlainObject(42)).toBe(false);
  });

  it("returns false for undefined", () => {
    expect(isPlainObject(undefined)).toBe(false);
  });

  it("returns false for a boolean", () => {
    expect(isPlainObject(true)).toBe(false);
  });

  it("returns true for nested objects", () => {
    expect(isPlainObject({ nested: { deep: true } })).toBe(true);
  });

  it("returns true for object with array values", () => {
    expect(isPlainObject({ items: [1, 2] })).toBe(true);
  });
});

describe("rethrowNonRecoverableApiError", () => {
  it("rethrows 401 CoalesceApiError", () => {
    const error = new CoalesceApiError("Unauthorized", 401);
    expect(() => rethrowNonRecoverableApiError(error)).toThrow(error);
  });

  it("rethrows 403 CoalesceApiError", () => {
    const error = new CoalesceApiError("Forbidden", 403);
    expect(() => rethrowNonRecoverableApiError(error)).toThrow(error);
  });

  it("rethrows 503 CoalesceApiError", () => {
    const error = new CoalesceApiError("Service unavailable", 503);
    expect(() => rethrowNonRecoverableApiError(error)).toThrow(error);
  });

  it("does not rethrow 400 CoalesceApiError", () => {
    const error = new CoalesceApiError("Bad request", 400);
    expect(() => rethrowNonRecoverableApiError(error)).not.toThrow();
  });

  it("does not rethrow 404 CoalesceApiError", () => {
    const error = new CoalesceApiError("Not found", 404);
    expect(() => rethrowNonRecoverableApiError(error)).not.toThrow();
  });

  it("does not rethrow 500 CoalesceApiError", () => {
    const error = new CoalesceApiError("Internal server error", 500);
    expect(() => rethrowNonRecoverableApiError(error)).not.toThrow();
  });

  it("does not rethrow generic Error", () => {
    const error = new Error("something broke");
    expect(() => rethrowNonRecoverableApiError(error)).not.toThrow();
  });

  it("does not rethrow non-Error values", () => {
    expect(() => rethrowNonRecoverableApiError("string error")).not.toThrow();
    expect(() => rethrowNonRecoverableApiError(null)).not.toThrow();
    expect(() => rethrowNonRecoverableApiError(undefined)).not.toThrow();
    expect(() => rethrowNonRecoverableApiError(42)).not.toThrow();
  });
});

describe("rethrowNonRecoverableOrServerError", () => {
  it("rethrows 401 CoalesceApiError", () => {
    const error = new CoalesceApiError("Unauthorized", 401);
    expect(() => rethrowNonRecoverableOrServerError(error)).toThrow(error);
  });

  it("rethrows 403 CoalesceApiError", () => {
    const error = new CoalesceApiError("Forbidden", 403);
    expect(() => rethrowNonRecoverableOrServerError(error)).toThrow(error);
  });

  it("rethrows 500 CoalesceApiError (unlike the base variant)", () => {
    const error = new CoalesceApiError("Internal server error", 500);
    expect(() => rethrowNonRecoverableOrServerError(error)).toThrow(error);
  });

  it("rethrows 503 CoalesceApiError", () => {
    const error = new CoalesceApiError("Service unavailable", 503);
    expect(() => rethrowNonRecoverableOrServerError(error)).toThrow(error);
  });

  it("does not rethrow 400 CoalesceApiError", () => {
    const error = new CoalesceApiError("Bad request", 400);
    expect(() => rethrowNonRecoverableOrServerError(error)).not.toThrow();
  });

  it("does not rethrow 404 CoalesceApiError", () => {
    const error = new CoalesceApiError("Not found", 404);
    expect(() => rethrowNonRecoverableOrServerError(error)).not.toThrow();
  });

  it("does not rethrow 502 CoalesceApiError", () => {
    const error = new CoalesceApiError("Bad gateway", 502);
    expect(() => rethrowNonRecoverableOrServerError(error)).not.toThrow();
  });

  it("does not rethrow generic Error", () => {
    const error = new Error("something broke");
    expect(() => rethrowNonRecoverableOrServerError(error)).not.toThrow();
  });

  it("does not rethrow non-Error values", () => {
    expect(() => rethrowNonRecoverableOrServerError("string")).not.toThrow();
    expect(() => rethrowNonRecoverableOrServerError(null)).not.toThrow();
  });
});

describe("uniqueInOrder", () => {
  it("removes duplicates while preserving order", () => {
    expect(uniqueInOrder([3, 1, 2, 1, 3])).toEqual([3, 1, 2]);
  });

  it("returns empty array for empty input", () => {
    expect(uniqueInOrder([])).toEqual([]);
  });

  it("returns same array when all elements are unique", () => {
    expect(uniqueInOrder([1, 2, 3])).toEqual([1, 2, 3]);
  });

  it("handles single-element array", () => {
    expect(uniqueInOrder([42])).toEqual([42]);
  });

  it("handles all-duplicate array", () => {
    expect(uniqueInOrder(["a", "a", "a"])).toEqual(["a"]);
  });

  it("preserves first occurrence position", () => {
    expect(uniqueInOrder(["b", "a", "c", "a", "b"])).toEqual(["b", "a", "c"]);
  });

  it("works with string values", () => {
    expect(uniqueInOrder(["foo", "bar", "foo", "baz", "bar"])).toEqual([
      "foo",
      "bar",
      "baz",
    ]);
  });
});

describe("sanitizeForFilename", () => {
  it("passes through alphanumeric strings unchanged", () => {
    expect(sanitizeForFilename("abc123")).toBe("abc123");
  });

  it("passes through hyphens and underscores", () => {
    expect(sanitizeForFilename("my-file_name")).toBe("my-file_name");
  });

  it("replaces spaces with underscores", () => {
    expect(sanitizeForFilename("hello world")).toBe("hello_world");
  });

  it("replaces path separators", () => {
    expect(sanitizeForFilename("path/to\\file")).toBe("path_to_file");
  });

  it("replaces special characters", () => {
    expect(sanitizeForFilename("file@#$%!.txt")).toBe("file______txt");
  });

  it("handles empty string", () => {
    expect(sanitizeForFilename("")).toBe("");
  });

  it("replaces dots", () => {
    expect(sanitizeForFilename("workspace.v2")).toBe("workspace_v2");
  });

  it("handles UUID-like workspace IDs", () => {
    expect(sanitizeForFilename("a1b2c3d4-e5f6-7890-abcd-ef1234567890")).toBe(
      "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    );
  });
});
