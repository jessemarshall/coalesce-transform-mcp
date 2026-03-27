import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  remainingTimeMs,
  serializeResultsError,
  throwIfAborted,
  sleepWithAbort,
  createWorkflowProgressReporter,
} from "../../src/workflows/progress.js";
import { CoalesceApiError } from "../../src/client.js";

describe("remainingTimeMs", () => {
  it("returns positive remaining time", () => {
    const startedAt = Date.now() - 1000;
    const result = remainingTimeMs(startedAt, 5000);
    // Should be approximately 4000ms (5000 - 1000)
    expect(result).toBeGreaterThan(3900);
    expect(result).toBeLessThanOrEqual(4100);
  });

  it("returns 0 when timeout has elapsed", () => {
    const startedAt = Date.now() - 10000;
    expect(remainingTimeMs(startedAt, 5000)).toBe(0);
  });

  it("never returns negative", () => {
    const startedAt = Date.now() - 100000;
    expect(remainingTimeMs(startedAt, 1000)).toBe(0);
  });
});

describe("serializeResultsError", () => {
  it("serializes CoalesceApiError with status and detail", () => {
    const error = new CoalesceApiError("Not found", 404, { key: "val" });
    const result = serializeResultsError(error);
    expect(result).toEqual({
      message: "Not found",
      status: 404,
      detail: { key: "val" },
    });
  });

  it("serializes CoalesceApiError without detail", () => {
    const error = new CoalesceApiError("Server error", 500);
    const result = serializeResultsError(error);
    expect(result).toEqual({ message: "Server error", status: 500 });
    expect(result).not.toHaveProperty("detail");
  });

  it("serializes regular Error", () => {
    const error = new Error("Something broke");
    expect(serializeResultsError(error)).toEqual({
      message: "Something broke",
    });
  });

  it("serializes unknown values with default message", () => {
    expect(serializeResultsError("string error")).toEqual({
      message: "Unable to fetch run results",
      detail: "string error",
    });
    expect(serializeResultsError(42)).toEqual({
      message: "Unable to fetch run results",
      detail: 42,
    });
  });
});

describe("throwIfAborted", () => {
  it("does nothing when signal is undefined", () => {
    expect(() => throwIfAborted(undefined)).not.toThrow();
  });

  it("does nothing when signal is not aborted", () => {
    const controller = new AbortController();
    expect(() => throwIfAborted(controller.signal)).not.toThrow();
  });

  it("throws AbortError when signal is aborted", () => {
    const controller = new AbortController();
    controller.abort();
    expect(() => throwIfAborted(controller.signal)).toThrow("Request was cancelled");
  });
});

describe("sleepWithAbort", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("resolves after the specified delay without signal", async () => {
    const promise = sleepWithAbort(100);
    vi.advanceTimersByTime(100);
    await expect(promise).resolves.toBeUndefined();
  });

  it("resolves after the specified delay with non-aborted signal", async () => {
    const controller = new AbortController();
    const promise = sleepWithAbort(100, controller.signal);
    vi.advanceTimersByTime(100);
    await expect(promise).resolves.toBeUndefined();
  });

  it("rejects immediately if signal is already aborted", async () => {
    const controller = new AbortController();
    controller.abort();
    await expect(sleepWithAbort(100, controller.signal)).rejects.toThrow(
      "Request was cancelled"
    );
  });

  it("rejects when signal is aborted during sleep", async () => {
    const controller = new AbortController();
    const promise = sleepWithAbort(1000, controller.signal);
    vi.advanceTimersByTime(50);
    controller.abort();
    await expect(promise).rejects.toThrow("Request was cancelled");
  });
});

describe("createWorkflowProgressReporter", () => {
  it("returns undefined when no progressToken", () => {
    const reporter = createWorkflowProgressReporter({
      sendNotification: vi.fn(),
    });
    expect(reporter).toBeUndefined();
  });

  it("returns undefined when no sendNotification", () => {
    const reporter = createWorkflowProgressReporter({
      _meta: { progressToken: "tok-1" },
    });
    expect(reporter).toBeUndefined();
  });

  it("returns undefined when extra is undefined", () => {
    expect(createWorkflowProgressReporter(undefined)).toBeUndefined();
  });

  it("returns a function when both token and sender are present", () => {
    const reporter = createWorkflowProgressReporter({
      _meta: { progressToken: "tok-1" },
      sendNotification: vi.fn(),
    });
    expect(typeof reporter).toBe("function");
  });

  it("sends notification with incremented progress counter", async () => {
    const sendNotification = vi.fn().mockResolvedValue(undefined);
    const reporter = createWorkflowProgressReporter({
      _meta: { progressToken: "tok-1" },
      sendNotification,
    })!;

    await reporter("Step 1", 3);
    expect(sendNotification).toHaveBeenCalledWith({
      method: "notifications/progress",
      params: {
        progressToken: "tok-1",
        progress: 1,
        total: 3,
        message: "Step 1",
      },
    });

    await reporter("Step 2", 3);
    expect(sendNotification).toHaveBeenCalledWith(
      expect.objectContaining({
        params: expect.objectContaining({ progress: 2 }),
      })
    );
  });

  it("does not include total when undefined", async () => {
    const sendNotification = vi.fn().mockResolvedValue(undefined);
    const reporter = createWorkflowProgressReporter({
      _meta: { progressToken: "tok-1" },
      sendNotification,
    })!;

    await reporter("msg");
    const call = sendNotification.mock.calls[0][0];
    expect(call.params).not.toHaveProperty("total");
  });

  it("swallows notification errors without throwing", async () => {
    const sendNotification = vi.fn().mockRejectedValue(new Error("network fail"));
    const reporter = createWorkflowProgressReporter({
      _meta: { progressToken: "tok-1" },
      sendNotification,
    })!;

    // Should not throw
    await expect(reporter("msg")).resolves.toBeUndefined();
  });
});
