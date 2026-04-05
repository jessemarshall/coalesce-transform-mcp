import { describe, it, expect } from "vitest";
import { handleToolError, buildJsonToolResponse } from "../../src/coalesce/tool-response.js";
import { CoalesceApiError } from "../../src/client.js";

describe("handleToolError serialization", () => {
  it("serializes a CoalesceApiError with status and message", () => {
    const error = new CoalesceApiError("Not Found", 404);
    const result = handleToolError(error);

    expect(result.isError).toBe(true);
    expect(result.content).toHaveLength(1);
    expect(result.content[0].text).toBe("Not Found");
    expect(result.structuredContent.error).toEqual({
      message: "Not Found",
      status: 404,
    });
  });

  it("serializes a CoalesceApiError with detail", () => {
    const error = new CoalesceApiError("Validation failed", 422, {
      fields: ["environmentID"],
      reason: "must be a valid UUID",
    });
    const result = handleToolError(error);

    expect(result.isError).toBe(true);
    expect(result.content[0].text).toBe("Validation failed");
    expect(result.structuredContent.error).toEqual({
      message: "Validation failed",
      status: 422,
      detail: {
        fields: ["environmentID"],
        reason: "must be a valid UUID",
      },
    });
  });

  it("serializes a CoalesceApiError with string detail", () => {
    const error = new CoalesceApiError("Bad Request", 400, "Invalid JSON body");
    const result = handleToolError(error);

    expect(result.structuredContent.error).toEqual({
      message: "Bad Request",
      status: 400,
      detail: "Invalid JSON body",
    });
  });

  it("serializes a CoalesceApiError with undefined detail (omits detail key)", () => {
    const error = new CoalesceApiError("Server Error", 500);
    const result = handleToolError(error);

    expect(result.structuredContent.error).toEqual({
      message: "Server Error",
      status: 500,
    });
    expect("detail" in result.structuredContent.error).toBe(false);
  });

  it("serializes a regular Error", () => {
    const error = new Error("Something went wrong");
    const result = handleToolError(error);

    expect(result.isError).toBe(true);
    expect(result.content[0].text).toBe("Something went wrong");
    expect(result.structuredContent.error).toEqual({
      message: "Something went wrong",
    });
  });

  it("serializes a TypeError", () => {
    const error = new TypeError("Cannot read properties of undefined");
    const result = handleToolError(error);

    expect(result.isError).toBe(true);
    expect(result.content[0].text).toBe("Cannot read properties of undefined");
    expect(result.structuredContent.error).toEqual({
      message: "Cannot read properties of undefined",
    });
  });

  it("serializes a string thrown as error", () => {
    const result = handleToolError("raw string error");

    expect(result.isError).toBe(true);
    expect(result.content[0].text).toBe("raw string error");
    expect(result.structuredContent.error).toEqual({
      message: "raw string error",
    });
  });

  it("serializes a number thrown as error", () => {
    const result = handleToolError(42);

    expect(result.isError).toBe(true);
    expect(result.content[0].text).toBe("42");
    expect(result.structuredContent.error).toEqual({
      message: "42",
    });
  });

  it("serializes null thrown as error", () => {
    const result = handleToolError(null);

    expect(result.isError).toBe(true);
    expect(result.content[0].text).toBe("null");
    expect(result.structuredContent.error).toEqual({
      message: "null",
    });
  });

  it("serializes undefined thrown as error", () => {
    const result = handleToolError(undefined);

    expect(result.isError).toBe(true);
    expect(result.content[0].text).toBe("undefined");
    expect(result.structuredContent.error).toEqual({
      message: "undefined",
    });
  });

  it("always returns exactly one text content item", () => {
    const cases = [
      new CoalesceApiError("api", 500),
      new Error("standard"),
      "string",
      null,
    ];

    for (const error of cases) {
      const result = handleToolError(error);
      expect(result.content).toHaveLength(1);
      expect(result.content[0].type).toBe("text");
    }
  });
});

describe("buildJsonToolResponse edge cases", () => {
  it("returns structured content for a simple object", () => {
    const result = buildJsonToolResponse("test_tool", { key: "value" }, {
      maxInlineBytes: 1024 * 1024, // Force inline
    });

    expect(result.structuredContent).toEqual({ key: "value" });
    expect(result.content[0]).toEqual({
      type: "text",
      text: JSON.stringify({ key: "value" }, null, 2),
    });
  });

  it("wraps non-object result in { value: ... }", () => {
    const result = buildJsonToolResponse("test_tool", "just a string", {
      maxInlineBytes: 1024 * 1024,
    });

    expect(result.structuredContent).toEqual({ value: "just a string" });
  });

  it("wraps null result in { value: null }", () => {
    const result = buildJsonToolResponse("test_tool", null, {
      maxInlineBytes: 1024 * 1024,
    });

    expect(result.structuredContent).toEqual({ value: null });
  });

  it("wraps array result in { value: [...] }", () => {
    const result = buildJsonToolResponse("test_tool", [1, 2, 3], {
      maxInlineBytes: 1024 * 1024,
    });

    expect(result.structuredContent).toEqual({ value: [1, 2, 3] });
  });

  it("coerces numeric next cursor to string", () => {
    const result = buildJsonToolResponse("test_tool", {
      data: [{ id: "1" }],
      next: 42,
    }, { maxInlineBytes: 1024 * 1024 });

    expect(result.structuredContent).toEqual({
      data: [{ id: "1" }],
      next: "42",
    });
  });

  it("removes null next cursor from response", () => {
    const result = buildJsonToolResponse("test_tool", {
      data: [{ id: "1" }],
      next: null,
    }, { maxInlineBytes: 1024 * 1024 });

    expect(result.structuredContent).not.toHaveProperty("next");
  });

  it("removes null total from response", () => {
    const result = buildJsonToolResponse("test_tool", {
      data: [],
      total: null,
    }, { maxInlineBytes: 1024 * 1024 });

    expect(result.structuredContent).not.toHaveProperty("total");
  });

  it("preserves valid string next cursor", () => {
    const result = buildJsonToolResponse("test_tool", {
      data: [],
      next: "cursor-abc",
    }, { maxInlineBytes: 1024 * 1024 });

    expect(result.structuredContent).toHaveProperty("next", "cursor-abc");
  });

  it("auto-caches response when it exceeds maxInlineBytes", () => {
    const largeData = { items: Array.from({ length: 100 }, (_, i) => ({ id: `item-${i}`, data: "x".repeat(100) })) };

    const result = buildJsonToolResponse("test_tool", largeData, {
      maxInlineBytes: 100, // Very small threshold to force caching
    });

    expect(result.structuredContent).toHaveProperty("autoCached", true);
    expect(result.structuredContent).toHaveProperty("toolName", "test_tool");
    expect(result.structuredContent).toHaveProperty("message");
  });
});
