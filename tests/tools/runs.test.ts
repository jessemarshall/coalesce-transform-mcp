import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { writeFileSync, unlinkSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  listRuns,
  getRun,
  getRunResults,
  startRun,
  runStatus,
  retryRun,
  cancelRun,
} from "../../src/coalesce/api/runs.js";
import { registerRunTools } from "../../src/mcp/runs.js";
import {
  POSTMAN_CANCEL_RUN_BODY,
  POSTMAN_RERUN_RESPONSE,
  POSTMAN_RUN_STATUS_RESPONSE,
  POSTMAN_RUN_DETAILS_RESPONSE,
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

describe("Run Tools", () => {
  const originalEnv = process.env;
  const tempDir = join(tmpdir(), "coalesce-runs-test-" + process.pid);
  const keyFilePath = join(tempDir, "test-key.pem");

  beforeEach(() => {
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
    process.env = originalEnv;
    try { unlinkSync(keyFilePath); } catch { /* ignore */ }
  });

  it("registers all 7 run tools without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerRunTools(server, client as any);
    expect(true).toBe(true);
  });

  it("sanitizes userCredentials from run read tool output", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/runs") {
        return Promise.resolve({ data: [POSTMAN_RUN_DETAILS_RESPONSE] });
      }
      if (path === "/api/v1/runs/0") {
        return Promise.resolve(POSTMAN_RUN_DETAILS_RESPONSE);
      }
      if (path === "/api/v1/runs/0/results") {
        return Promise.resolve({
          results: [
            {
              nodeID: "n1",
              status: "success",
              userCredentials: { snowflakeUsername: "secret-user" },
            },
          ],
        });
      }
      if (path === "/scheduler/runStatus") {
        return Promise.resolve({
          runCounter: 0,
          runStatus: "completed",
          userCredentials: { snowflakeUsername: "secret-user" },
        });
      }
      return Promise.resolve({});
    });

    registerRunTools(server, client as any);

    const listRunsHandler = toolSpy.mock.calls.find((call) => call[0] === "list_runs")?.[2] as
      | ((params: Record<string, unknown>) => Promise<{ content: { text: string }[] }>)
      | undefined;
    const getRunHandler = toolSpy.mock.calls.find((call) => call[0] === "get_run")?.[2] as
      | ((params: { runID: string }) => Promise<{ content: { text: string }[] }>)
      | undefined;
    const getRunResultsHandler = toolSpy.mock.calls.find(
      (call) => call[0] === "get_run_results"
    )?.[2] as
      | ((params: { runID: string }) => Promise<{ content: { text: string }[] }>)
      | undefined;
    const runStatusHandler = toolSpy.mock.calls.find((call) => call[0] === "run_status")?.[2] as
      | ((params: { runCounter: number }) => Promise<{ content: { text: string }[] }>)
      | undefined;

    expect(typeof listRunsHandler).toBe("function");
    expect(typeof getRunHandler).toBe("function");
    expect(typeof getRunResultsHandler).toBe("function");
    expect(typeof runStatusHandler).toBe("function");

    const listRunsResult = await listRunsHandler!({});
    const getRunResult = await getRunHandler!({ runID: "0" });
    const getRunResultsResult = await getRunResultsHandler!({ runID: "0" });
    const runStatusResult = await runStatusHandler!({ runCounter: 0 });

    expect(listRunsResult.content[0]?.text).not.toContain("userCredentials");
    expect(getRunResult.content[0]?.text).not.toContain("userCredentials");
    expect(getRunResultsResult.content[0]?.text).not.toContain("userCredentials");
    expect(runStatusResult.content[0]?.text).not.toContain("userCredentials");
  });

  it("listRuns calls GET /api/v1/runs", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "run-1" }] });

    const result = await listRuns(client as any, {});

    expect(client.get).toHaveBeenCalledWith("/api/v1/runs", {});
    expect(result).toEqual({ data: [{ id: "run-1" }] });
  });

  it("listRuns passes pagination params", async () => {
    const client = createMockClient();

    await listRuns(client as any, { limit: 10, orderBy: "startedAt" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/runs", {
      limit: 10,
      orderBy: "startedAt",
    });
  });

  it("getRun calls GET /api/v1/runs/{runID}", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue(POSTMAN_RUN_DETAILS_RESPONSE);

    const result = await getRun(client as any, { runID: "0" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/runs/0", {});
    expect(result).toEqual(POSTMAN_RUN_DETAILS_RESPONSE);
  });

  it("getRun ignores unsupported query params and only requests by runID", async () => {
    const client = createMockClient();

    await getRun(client as any, { runID: "0" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/runs/0", {});
  });

  it("getRunResults calls GET /api/v1/runs/{runID}/results", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ results: [{ nodeID: "n1", status: "success" }] });

    const result = await getRunResults(client as any, { runID: "0" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/runs/0/results", {});
    expect(result).toEqual({ results: [{ nodeID: "n1", status: "success" }] });
  });

  it("startRun calls POST /scheduler/startRun with body including env var credentials", async () => {
    const client = createMockClient();
    const params = {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
    };
    client.post.mockResolvedValue(POSTMAN_START_RUN_RESPONSE);

    const result = await startRun(client as any, params);

    expect(client.post).toHaveBeenCalledWith("/scheduler/startRun", {
      runDetails: { environmentID: "env-1", jobID: "job-1" },
      userCredentials: {
        snowflakeUsername: "user",
        snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
        snowflakeWarehouse: "wh",
        snowflakeRole: "role",
        snowflakeAuthType: "KeyPair",
      },
    });
    expect(result).toEqual(POSTMAN_START_RUN_RESPONSE);
  });

  it("runStatus calls GET /scheduler/runStatus with runCounter query param", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      ...POSTMAN_RUN_STATUS_RESPONSE,
      runStatus: "running",
    });

    const result = await runStatus(client as any, { runCounter: 42 });

    expect(client.get).toHaveBeenCalledWith("/scheduler/runStatus", {
      runCounter: 42,
    });
    expect(result).toEqual({
      ...POSTMAN_RUN_STATUS_RESPONSE,
      runStatus: "running",
    });
  });

  it("retryRun calls POST /scheduler/rerun with typed params and env var credentials", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue(POSTMAN_RERUN_RESPONSE);

    const result = await retryRun(client as any, {
      runDetails: { runID: "0" },
    });

    expect(client.post).toHaveBeenCalledWith("/scheduler/rerun", {
      runDetails: { runID: "0" },
      userCredentials: {
        snowflakeUsername: "user",
        snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
        snowflakeWarehouse: "wh",
        snowflakeRole: "role",
        snowflakeAuthType: "KeyPair",
      },
    });
    expect(result).toEqual(POSTMAN_RERUN_RESPONSE);
  });

  it("cancelRun calls POST /scheduler/cancelRun with typed params", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ message: "Run cancelled" });

    const result = await cancelRun(client as any, {
      runID: "42",
      orgID: "org-1",
      environmentID: "env-1",
    });

    expect(client.post).toHaveBeenCalledWith("/scheduler/cancelRun", {
      runID: "42",
      orgID: "org-1",
      environmentID: "env-1",
    });
    expect(result).toEqual({ message: "Run cancelled" });
  });

  it("cancelRun falls back to COALESCE_ORG_ID when orgID is omitted", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ message: "Run cancelled" });
    process.env.COALESCE_ORG_ID = "org-from-env";

    const result = await cancelRun(client as any, {
      runID: "42",
      environmentID: "env-1",
    });

    expect(client.post).toHaveBeenCalledWith("/scheduler/cancelRun", {
      runID: "42",
      orgID: "org-from-env",
      environmentID: "env-1",
    });
    expect(result).toEqual({ message: "Run cancelled" });
  });

  it("cancelRun throws when neither orgID nor COALESCE_ORG_ID is provided", async () => {
    const client = createMockClient();
    delete process.env.COALESCE_ORG_ID;

    await expect(
      cancelRun(client as any, {
        runID: "42",
        environmentID: "env-1",
      })
    ).rejects.toThrow("COALESCE_ORG_ID");
  });

  it("tracks the documented cancelRun body shape from the Postman example", () => {
    expect(Object.keys(POSTMAN_CANCEL_RUN_BODY)).toEqual([
      "runID",
      "orgID",
      "environmentID",
    ]);
  });
});
