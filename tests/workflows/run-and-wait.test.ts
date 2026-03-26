import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { writeFileSync, unlinkSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { CoalesceApiError } from "../../src/client.js";
import {
  runAndWait,
  registerRunAndWait,
} from "../../src/workflows/run-and-wait.js";
import {
  POSTMAN_RUN_STATUS_RESPONSE,
  POSTMAN_START_RUN_RESPONSE,
} from "../fixtures/postman-examples.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue(POSTMAN_START_RUN_RESPONSE),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("run-and-wait workflow", () => {
  const originalEnv = process.env;
  const tempDir = join(tmpdir(), "coalesce-raw-test-" + process.pid);
  const keyFilePath = join(tempDir, "test-key.pem");

  beforeEach(() => {
    vi.useFakeTimers();
    mkdirSync(tempDir, { recursive: true });
    writeFileSync(keyFilePath, "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----");
    process.env = {
      ...originalEnv,
      SNOWFLAKE_USERNAME: "user",
      SNOWFLAKE_KEY_PAIR_KEY: keyFilePath,
      SNOWFLAKE_WAREHOUSE: "wh",
      SNOWFLAKE_ROLE: "role",
    };
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = originalEnv;
    try { unlinkSync(keyFilePath); } catch { /* ignore */ }
  });

  it("registers without throwing", () => {
    vi.useRealTimers(); // registration doesn't need fake timers
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerRunAndWait(server, client as any);
    expect(true).toBe(true);
  });

  it("sends MCP progress notifications while waiting for the run", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "tool");
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") {
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: "completed",
        });
      }
      if (path === "/api/v1/runs/0/results") {
        return Promise.resolve({ results: [] });
      }
      return Promise.resolve({});
    });

    registerRunAndWait(server, client as any);

    const toolCall = toolSpy.mock.calls.find((call) => call[0] === "run-and-wait");
    const handler = toolCall?.[4] as
      | ((params: {
          runDetails: { environmentID: string; jobID: string };
          pollInterval?: number;
        }, extra?: {
          signal?: AbortSignal;
          _meta?: { progressToken?: string | number };
          sendNotification?: (notification: {
            method: "notifications/progress";
            params: {
              progressToken: string | number;
              progress: number;
              total?: number;
              message?: string;
            };
          }) => Promise<void>;
        }) => Promise<unknown>)
      | undefined;

    expect(typeof handler).toBe("function");

    const sendNotification = vi.fn().mockResolvedValue(undefined);
    const promise = handler!(
      {
        runDetails: { environmentID: "env-1", jobID: "job-1" },
        pollInterval: 5,
      },
      {
        signal: new AbortController().signal,
        _meta: { progressToken: "progress-1" },
        sendNotification,
      }
    );

    await vi.advanceTimersByTimeAsync(5_000);
    await promise;

    const notifications = sendNotification.mock.calls.map(
      ([notification]) => notification
    );

    expect(notifications).toEqual([
      expect.objectContaining({
        method: "notifications/progress",
        params: expect.objectContaining({
          progressToken: "progress-1",
          progress: 1,
          message: expect.stringContaining("Started run 0"),
        }),
      }),
      expect.objectContaining({
        method: "notifications/progress",
        params: expect.objectContaining({
          progressToken: "progress-1",
          progress: 2,
          message: expect.stringContaining("Status check 1 for run 0: completed"),
        }),
      }),
      expect.objectContaining({
        method: "notifications/progress",
        params: expect.objectContaining({
          progressToken: "progress-1",
          progress: 3,
          message: expect.stringContaining("Run 0 reached terminal status completed"),
        }),
      }),
      expect.objectContaining({
        method: "notifications/progress",
        params: expect.objectContaining({
          progressToken: "progress-1",
          progress: 4,
          message: expect.stringContaining("Fetched results for run 0"),
        }),
      }),
    ]);
  });

  it("calls POST /scheduler/startRun then polls GET /scheduler/runStatus", async () => {
    const client = createMockClient();
    const params = {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
    };

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") {
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: "completed",
        });
      }
      if (path === "/api/v1/runs/0/results") {
        return Promise.resolve({ results: [] });
      }
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, { ...params, pollInterval: 10 });
    await vi.advanceTimersByTimeAsync(10_000);
    const result = await promise;

    expect(client.post).toHaveBeenCalledWith("/scheduler/startRun", {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      userCredentials: {
        snowflakeUsername: "user",
        snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
        snowflakeWarehouse: "wh",
        snowflakeRole: "role",
        snowflakeAuthType: "KeyPair",
      },
    }, undefined, expect.objectContaining({ timeoutMs: expect.any(Number) }));
    expect(client.get).toHaveBeenCalledWith("/scheduler/runStatus", {
      runCounter: 0,
    }, expect.objectContaining({ timeoutMs: expect.any(Number) }));
    expect(result).toBeDefined();
  });

  it("returns { status, results } when run completes with status 'completed'", async () => {
    const client = createMockClient();
    const statusData = {
      ...POSTMAN_RUN_STATUS_RESPONSE,
      runStatus: "completed",
    };
    const resultsData = { results: [{ nodeID: "n1", status: "success" }] };

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") return Promise.resolve(statusData);
      if (path === "/api/v1/runs/0/results")
        return Promise.resolve(resultsData);
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 10,
    });
    await vi.advanceTimersByTimeAsync(10_000);
    const result = await promise;

    expect(result).toEqual({ status: statusData, results: resultsData });
  });

  it("fetches results via GET /api/v1/runs/{runID}/results on completion", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus")
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: "completed",
        });
      if (path === "/api/v1/runs/0/results")
        return Promise.resolve({ data: "results-here" });
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 5,
    });
    await vi.advanceTimersByTimeAsync(5_000);
    await promise;

    expect(client.get).toHaveBeenCalledWith(
      "/api/v1/runs/0/results",
      undefined,
      expect.objectContaining({ timeoutMs: expect.any(Number) })
    );
  });

  it("surfaces resultsError and incomplete when results fetch fails", async () => {
    const client = createMockClient();
    const statusData = {
      ...POSTMAN_RUN_STATUS_RESPONSE,
      runStatus: "completed",
    };

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") {
        return Promise.resolve(statusData);
      }
      if (path === "/api/v1/runs/0/results") {
        return Promise.reject(
          new CoalesceApiError("Results endpoint unavailable", 503, {
            endpoint: "/api/v1/runs/0/results",
          })
        );
      }
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 5,
    });
    await vi.advanceTimersByTimeAsync(5_000);
    const result = await promise;

    expect(result).toEqual({
      status: statusData,
      results: null,
      resultsError: {
        message: "Results endpoint unavailable",
        status: 503,
        detail: {
          endpoint: "/api/v1/runs/0/results",
        },
      },
      incomplete: true,
    });
  });

  it("handles immediate completion (first poll returns done)", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus")
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: "completed",
        });
      if (path === "/api/v1/runs/0/results")
        return Promise.resolve({ done: true });
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 5,
    });
    await vi.advanceTimersByTimeAsync(5_000);
    const result = await promise;

    expect(result).toEqual({
      status: {
        ...POSTMAN_RUN_STATUS_RESPONSE,
        runStatus: "completed",
      },
      results: { done: true },
    });
    // Only one poll needed
    expect(
      client.get.mock.calls.filter(
        (c: string[]) => c[0] === "/scheduler/runStatus"
      ).length
    ).toBe(1);
  });

  it("returns timedOut when timeout is reached", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus")
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: "running",
        });
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 5,
      timeout: 30, // minimum allowed timeout
    });

    // Advance past the timeout (polls at 5s, 10s, 15s, 20s, 25s, 30s)
    await vi.advanceTimersByTimeAsync(35_000);
    const result = (await promise) as { status: unknown; results: unknown; timedOut: boolean };

    expect(result.timedOut).toBe(true);
    expect(result.results).toBeNull();
  });

  it("polls multiple times until status is completed", async () => {
    const client = createMockClient();
    let pollCount = 0;

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") {
        pollCount++;
        if (pollCount < 3) {
          return Promise.resolve({
            ...POSTMAN_RUN_STATUS_RESPONSE,
            runStatus: "running",
          });
        }
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: "completed",
        });
      }
      if (path === "/api/v1/runs/0/results")
        return Promise.resolve({ data: "final" });
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 5,
    });

    // Advance through 3 poll intervals
    await vi.advanceTimersByTimeAsync(5_000); // poll 1: running
    await vi.advanceTimersByTimeAsync(5_000); // poll 2: running
    await vi.advanceTimersByTimeAsync(5_000); // poll 3: completed
    const result = await promise;

    expect(pollCount).toBe(3);
    expect(result).toEqual({
      status: {
        ...POSTMAN_RUN_STATUS_RESPONSE,
        runStatus: "completed",
      },
      results: { data: "final" },
    });
  });

  it("treats waitingToRun as a valid non-terminal status", async () => {
    const client = createMockClient();
    let pollCount = 0;

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") {
        pollCount++;
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: pollCount === 1 ? "waitingToRun" : "completed",
        });
      }
      if (path === "/api/v1/runs/0/results") {
        return Promise.resolve({ data: "final" });
      }
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 5,
    });

    await vi.advanceTimersByTimeAsync(10_000);
    const result = await promise;

    expect(pollCount).toBe(2);
    expect(result).toEqual({
      status: {
        ...POSTMAN_RUN_STATUS_RESPONSE,
        runStatus: "completed",
      },
      results: { data: "final" },
    });
  });

  it("throws when runStatus falls outside the documented status set", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") {
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: "mystery_status",
        });
      }
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 5,
    });
    const rejection = expect(promise).rejects.toThrow(
      "Run 0 returned unexpected runStatus 'mystery_status'. Expected one of: waitingToRun, running, completed, failed, canceled."
    );

    await vi.advanceTimersByTimeAsync(5_000);

    await rejection;
  });

  it("keeps polling after a timed-out status request while time remains", async () => {
    const client = createMockClient();
    let pollCount = 0;

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") {
        pollCount++;
        if (pollCount === 1) {
          return Promise.reject(
            new CoalesceApiError("Coalesce API request timed out after 25000ms", 408)
          );
        }
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: "completed",
        });
      }
      if (path === "/api/v1/runs/0/results") {
        return Promise.resolve({ data: "final" });
      }
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 5,
      timeout: 30,
    });

    await vi.advanceTimersByTimeAsync(5_000);
    await vi.advanceTimersByTimeAsync(5_000);
    const result = await promise;

    expect(pollCount).toBe(2);
    expect(client.get).toHaveBeenNthCalledWith(
      1,
      "/scheduler/runStatus",
      { runCounter: 0 },
      { timeoutMs: 25_000 }
    );
    expect(result).toEqual({
      status: {
        ...POSTMAN_RUN_STATUS_RESPONSE,
        runStatus: "completed",
      },
      results: { data: "final" },
    });
  });

  it("surfaces incomplete when the workflow deadline is reached before results fetch", async () => {
    const client = createMockClient();
    const statusData = {
      ...POSTMAN_RUN_STATUS_RESPONSE,
      runStatus: "completed",
    };

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") {
        vi.setSystemTime(Date.now() + 1_000);
        return Promise.resolve(statusData);
      }
      return Promise.resolve({});
    });

    const promise = runAndWait(client as any, {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      pollInterval: 29,
      timeout: 30,
    });
    await vi.advanceTimersByTimeAsync(29_000);
    const result = await promise;

    expect(result).toEqual({
      status: statusData,
      results: null,
      resultsError: {
        message: "Workflow timeout reached before run results could be fetched",
        status: 408,
      },
      incomplete: true,
    });
  });
});
