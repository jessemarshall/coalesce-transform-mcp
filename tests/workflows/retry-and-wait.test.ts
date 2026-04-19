import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { writeFileSync, unlinkSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { CoalesceApiError } from "../../src/client.js";
import {
  retryAndWait,
  registerRetryAndWait,
  extractResultScope,
} from "../../src/workflows/retry-and-wait.js";
import {
  POSTMAN_RERUN_RESPONSE,
  POSTMAN_RUN_STATUS_RESPONSE,
} from "../fixtures/postman-examples.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue(POSTMAN_RERUN_RESPONSE),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("retry-and-wait workflow", () => {
  const originalEnv = process.env;
  const tempDir = join(tmpdir(), "coalesce-retry-test-" + process.pid);
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
    vi.useRealTimers();
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerRetryAndWait(server, client as any);
    expect(true).toBe(true);
  });

  it("sends MCP progress notifications while waiting for the retry run", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
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

    registerRetryAndWait(server, client as any);

    const toolCall = toolSpy.mock.calls.find((call) => call[0] === "retry_and_wait");
    const handler = toolCall?.[2] as
      | ((params: {
          runDetails: { runID: string };
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
        runDetails: { runID: "0" },
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
          message: expect.stringContaining("Started retry run 0"),
        }),
      }),
      expect.objectContaining({
        method: "notifications/progress",
        params: expect.objectContaining({
          progressToken: "progress-1",
          progress: 2,
          message: expect.stringContaining("Status check 1 for retry run 0: completed"),
        }),
      }),
      expect.objectContaining({
        method: "notifications/progress",
        params: expect.objectContaining({
          progressToken: "progress-1",
          progress: 3,
          message: expect.stringContaining("Retry run 0 reached terminal status completed"),
        }),
      }),
      expect.objectContaining({
        method: "notifications/progress",
        params: expect.objectContaining({
          progressToken: "progress-1",
          progress: 4,
          message: expect.stringContaining("Fetched results for retry run 0"),
        }),
      }),
    ]);
  });

  it("calls POST /scheduler/rerun with credentials then polls GET /scheduler/runStatus", async () => {
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

    const promise = retryAndWait(client as any, {
      runDetails: { runID: "0" },
      pollInterval: 10,
    });
    await vi.advanceTimersByTimeAsync(10_000);
    const result = await promise;

    expect(client.post).toHaveBeenCalledWith("/scheduler/rerun", {
      runDetails: { runID: "0" },
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

  it("aborts an in-flight status request when the workflow signal is cancelled", async () => {
    const client = createMockClient();
    const controller = new AbortController();

    client.get.mockImplementation((path: string, _params: unknown, options?: { signal?: AbortSignal }) => {
      if (path === "/scheduler/runStatus") {
        return new Promise((_, reject) => {
          options?.signal?.addEventListener("abort", () => {
            const error = new Error("Request was cancelled");
            error.name = "AbortError";
            reject(error);
          });
        });
      }
      return Promise.resolve({});
    });

    const promise = retryAndWait(client as any, {
      runDetails: { runID: "0" },
      pollInterval: 5,
    }, {
      signal: controller.signal,
    });

    await vi.advanceTimersByTimeAsync(5_000);
    expect(client.get).toHaveBeenCalledWith(
      "/scheduler/runStatus",
      { runCounter: 0 },
      expect.objectContaining({ signal: controller.signal })
    );

    controller.abort();

    await expect(promise).rejects.toThrow("Request was cancelled");
  });

  it("returns { status, results } when run completes", async () => {
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

    const promise = retryAndWait(client as any, {
      runDetails: { runID: "0" },
      pollInterval: 10,
    });
    await vi.advanceTimersByTimeAsync(10_000);
    const result = await promise;

    expect(result).toEqual({ status: statusData, results: resultsData });
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

    const promise = retryAndWait(client as any, {
      runDetails: { runID: "0" },
      pollInterval: 5,
      timeout: 30, // minimum allowed timeout
    });

    await vi.advanceTimersByTimeAsync(35_000);
    const result = (await promise) as { status: unknown; results: unknown; timedOut: boolean };

    expect(result.timedOut).toBe(true);
    expect(result.results).toBeNull();
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

    const promise = retryAndWait(client as any, {
      runDetails: { runID: "0" },
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

    const promise = retryAndWait(client as any, {
      runDetails: { runID: "0" },
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

  it("throws when retry runStatus falls outside the documented status set", async () => {
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

    const promise = retryAndWait(client as any, {
      runDetails: { runID: "0" },
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

    const promise = retryAndWait(client as any, {
      runDetails: { runID: "0" },
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

    const promise = retryAndWait(client as any, {
      runDetails: { runID: "0" },
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

  it("throws a descriptive error when rerun response lacks a numeric runCounter", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ runCounter: "not-a-number" });

    await expect(
      retryAndWait(client as any, { runDetails: { runID: "0" } })
    ).rejects.toThrow("rerun response did not include a numeric runCounter (got string)");
  });
});

describe("extractResultScope", () => {
  it("reads workspaceID from the nested run-status payload", () => {
    expect(
      extractResultScope({ status: { workspaceID: "ws-42", runStatus: "completed" }, results: [] })
    ).toEqual({ workspaceID: "ws-42" });
  });

  it("reads environmentID from the nested run-status payload", () => {
    expect(
      extractResultScope({ status: { environmentID: "env-9", runStatus: "completed" }, results: [] })
    ).toEqual({ environmentID: "env-9" });
  });

  it("reads workspaceID from the top level when the result is already flat", () => {
    expect(extractResultScope({ workspaceID: "ws-1" })).toEqual({ workspaceID: "ws-1" });
  });

  it("prefers workspaceID over environmentID when both are present", () => {
    expect(
      extractResultScope({ status: { workspaceID: "ws-1", environmentID: "env-1" } })
    ).toEqual({ workspaceID: "ws-1" });
  });

  it("returns empty when no recognizable IDs are found", () => {
    expect(extractResultScope({ status: { runStatus: "completed" }, results: [] })).toEqual({});
    expect(extractResultScope(null)).toEqual({});
    expect(extractResultScope("not an object")).toEqual({});
  });
});
