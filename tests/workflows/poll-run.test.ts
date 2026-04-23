import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { CoalesceApiError } from "../../src/client.js";
import { pollRunToCompletion, type PollRunOptions } from "../../src/workflows/poll-run.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

function buildOptions(
  overrides: Partial<PollRunOptions> & { client: ReturnType<typeof createMockClient> }
): PollRunOptions {
  return {
    runCounter: 100,
    label: "run",
    pollIntervalMs: 100,
    timeoutMs: 5000,
    startedAt: Date.now(),
    ...overrides,
  };
}

describe("pollRunToCompletion", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns status and results when run completes successfully", async () => {
    const client = createMockClient();
    const statusResponse = { runCounter: 100, runStatus: "completed" };
    const resultsResponse = { nodes: [{ nodeID: "n1", status: "passed" }] };

    client.get
      .mockResolvedValueOnce(statusResponse) // runStatus
      .mockResolvedValueOnce(resultsResponse); // results

    const opts = buildOptions({ client });
    const promise = pollRunToCompletion(opts);
    await vi.advanceTimersByTimeAsync(opts.pollIntervalMs);

    const result = await promise;
    expect(result).toEqual({ status: statusResponse, results: resultsResponse });
  });

  it("returns status and results when run fails (terminal status)", async () => {
    const client = createMockClient();
    const statusResponse = { runCounter: 100, runStatus: "failed" };
    const resultsResponse = { nodes: [{ nodeID: "n1", status: "failed", error: "SQL error" }] };

    client.get
      .mockResolvedValueOnce(statusResponse)
      .mockResolvedValueOnce(resultsResponse);

    const opts = buildOptions({ client });
    const promise = pollRunToCompletion(opts);
    await vi.advanceTimersByTimeAsync(opts.pollIntervalMs);

    const result = await promise;
    expect(result).toEqual({ status: statusResponse, results: resultsResponse });
  });

  it("polls multiple times until terminal status", async () => {
    const client = createMockClient();

    client.get
      .mockResolvedValueOnce({ runCounter: 100, runStatus: "running" })
      .mockResolvedValueOnce({ runCounter: 100, runStatus: "running" })
      .mockResolvedValueOnce({ runCounter: 100, runStatus: "completed" })
      .mockResolvedValueOnce({ nodes: [] }); // results

    const opts = buildOptions({ client, pollIntervalMs: 100 });
    const promise = pollRunToCompletion(opts);

    // Advance through three poll cycles
    await vi.advanceTimersByTimeAsync(100);
    await vi.advanceTimersByTimeAsync(100);
    await vi.advanceTimersByTimeAsync(100);

    const result = await promise;
    expect(result).toEqual({
      status: { runCounter: 100, runStatus: "completed" },
      results: { nodes: [] },
    });
    // 3 status calls + 1 results call
    expect(client.get).toHaveBeenCalledTimes(4);
  });

  it("returns timedOut when timeout expires before terminal status", async () => {
    const client = createMockClient();

    // Always return running
    client.get.mockResolvedValue({ runCounter: 100, runStatus: "running" });

    const now = Date.now();
    const opts = buildOptions({
      client,
      startedAt: now,
      timeoutMs: 250,
      pollIntervalMs: 100,
    });
    const promise = pollRunToCompletion(opts);

    // Advance past timeout
    await vi.advanceTimersByTimeAsync(300);

    const result = (await promise) as Record<string, unknown>;
    expect(result.timedOut).toBe(true);
    expect(result.results).toBeNull();
    expect(result.status).toBeDefined();
  });

  it("retries on 408 status check timeout and continues polling", async () => {
    const client = createMockClient();
    const progressMessages: string[] = [];

    client.get
      .mockRejectedValueOnce(new CoalesceApiError("Timeout", 408))
      .mockResolvedValueOnce({ runCounter: 100, runStatus: "completed" })
      .mockResolvedValueOnce({ nodes: [] });

    const opts = buildOptions({
      client,
      pollIntervalMs: 100,
      reportProgress: async (msg) => { progressMessages.push(msg); },
    });
    const promise = pollRunToCompletion(opts);

    await vi.advanceTimersByTimeAsync(100);
    await vi.advanceTimersByTimeAsync(100);

    const result = await promise;
    expect(result).toEqual({
      status: { runCounter: 100, runStatus: "completed" },
      results: { nodes: [] },
    });
    expect(progressMessages.some((m) => m.includes("timed out"))).toBe(true);
  });

  it("throws on non-408 API errors during polling", async () => {
    const client = createMockClient();

    client.get.mockRejectedValueOnce(new CoalesceApiError("Server Error", 500));

    const opts = buildOptions({ client, pollIntervalMs: 100 });
    const promise = pollRunToCompletion(opts);

    // Attach rejection handler before advancing timers to avoid unhandled rejection
    const expectation = expect(promise).rejects.toThrow("Server Error");
    await vi.advanceTimersByTimeAsync(100);
    await expectation;
  });

  it("returns incomplete with resultsError when results fetch fails", async () => {
    const client = createMockClient();

    client.get
      .mockResolvedValueOnce({ runCounter: 100, runStatus: "completed" })
      .mockRejectedValueOnce(new CoalesceApiError("Not Found", 404));

    const opts = buildOptions({ client, pollIntervalMs: 100 });
    const promise = pollRunToCompletion(opts);

    await vi.advanceTimersByTimeAsync(100);

    const result = (await promise) as Record<string, unknown>;
    expect(result.incomplete).toBe(true);
    expect(result.results).toBeNull();
    expect(result.resultsError).toMatchObject({ message: "Not Found", status: 404 });
  });

  it("sends progress notifications with poll count and status", async () => {
    const client = createMockClient();
    const progressMessages: string[] = [];

    client.get
      .mockResolvedValueOnce({ runCounter: 100, runStatus: "running" })
      .mockResolvedValueOnce({ runCounter: 100, runStatus: "completed" })
      .mockResolvedValueOnce({ nodes: [] });

    const opts = buildOptions({
      client,
      pollIntervalMs: 100,
      reportProgress: async (msg) => { progressMessages.push(msg); },
    });
    const promise = pollRunToCompletion(opts);

    await vi.advanceTimersByTimeAsync(100);
    await vi.advanceTimersByTimeAsync(100);

    await promise;

    expect(progressMessages).toEqual(
      expect.arrayContaining([
        expect.stringContaining("Status check 1"),
        expect.stringContaining("Status check 2"),
        expect.stringContaining("terminal status completed"),
        expect.stringContaining("Fetched results"),
      ])
    );
  });

  it("handles canceled run status as terminal", async () => {
    const client = createMockClient();

    client.get
      .mockResolvedValueOnce({ runCounter: 100, runStatus: "canceled" })
      .mockResolvedValueOnce({ nodes: [] });

    const opts = buildOptions({ client, pollIntervalMs: 100 });
    const promise = pollRunToCompletion(opts);

    await vi.advanceTimersByTimeAsync(100);

    const result = await promise;
    expect(result).toEqual({
      status: { runCounter: 100, runStatus: "canceled" },
      results: { nodes: [] },
    });
  });

  it("throws on unexpected (unknown) run status", async () => {
    const client = createMockClient();

    client.get.mockResolvedValueOnce({ runCounter: 100, runStatus: "exploded" });

    const opts = buildOptions({ client, pollIntervalMs: 100 });
    const promise = pollRunToCompletion(opts);

    const expectation = expect(promise).rejects.toThrow("unexpected runStatus 'exploded'");
    await vi.advanceTimersByTimeAsync(100);
    await expectation;
  });

  it("respects abort signal during polling", async () => {
    const client = createMockClient();
    const controller = new AbortController();

    client.get.mockResolvedValue({ runCounter: 100, runStatus: "running" });

    const opts = buildOptions({
      client,
      pollIntervalMs: 100,
      signal: controller.signal,
    });
    const promise = pollRunToCompletion(opts);

    const expectation = expect(promise).rejects.toThrow("cancelled");
    // Abort before poll completes
    controller.abort();
    await vi.advanceTimersByTimeAsync(100);
    await expectation;
  });

  it("returns timedOut when polling budget is exhausted during sleep", async () => {
    const client = createMockClient();

    const now = Date.now();
    // Only 50ms of budget remains; the poll sleep (clamped to 50ms)
    // consumes it all, so the loop breaks before ever calling client.get.
    const opts = buildOptions({
      client,
      startedAt: now - 4950,
      timeoutMs: 5000,
      pollIntervalMs: 100,
    });
    const promise = pollRunToCompletion(opts);

    await vi.advanceTimersByTimeAsync(100);

    const result = (await promise) as Record<string, unknown>;
    expect(result.timedOut).toBe(true);
    expect(result.results).toBeNull();
    expect(result.status).toBeNull();
    // client.get should never have been called — budget ran out before the fetch
    expect(client.get).not.toHaveBeenCalled();
  });

  it("handles non-string runStatus by throwing", async () => {
    const client = createMockClient();

    client.get.mockResolvedValueOnce({ runCounter: 100, runStatus: 42 });

    const opts = buildOptions({ client, pollIntervalMs: 100 });
    const promise = pollRunToCompletion(opts);

    const expectation = expect(promise).rejects.toThrow("non-string runStatus");
    await vi.advanceTimersByTimeAsync(100);
    await expectation;
  });

  it("uses label in progress messages", async () => {
    const client = createMockClient();
    const progressMessages: string[] = [];

    client.get
      .mockResolvedValueOnce({ runCounter: 42, runStatus: "completed" })
      .mockResolvedValueOnce({ nodes: [] });

    const opts = buildOptions({
      client,
      runCounter: 42,
      label: "retry",
      pollIntervalMs: 100,
      reportProgress: async (msg) => { progressMessages.push(msg); },
    });
    const promise = pollRunToCompletion(opts);

    await vi.advanceTimersByTimeAsync(100);
    await promise;

    expect(progressMessages.some((m) => m.includes("retry 42"))).toBe(true);
  });
});
