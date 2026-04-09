import { describe, it, expect, vi, afterEach } from "vitest";
import { diagnoseRunFailure } from "../../src/services/runs/diagnostics.js";
import { CoalesceApiError } from "../../src/client.js";
import { createMockClient } from "../helpers/fixtures.js";

function mockRunAndResults(
  client: ReturnType<typeof createMockClient>,
  run: Record<string, unknown>,
  results: unknown
) {
  client.get.mockImplementation((path: string) => {
    if (path.endsWith("/results")) return Promise.resolve(results);
    return Promise.resolve(run);
  });
}

const BASE_RUN = {
  id: "100",
  runStatus: "failed",
  runType: "deploy",
  runStartTime: "2026-03-27T10:00:00Z",
  runEndTime: "2026-03-27T10:05:00Z",
  runDetails: {
    environmentID: "env-1",
    nodesInRun: "3",
  },
};

describe("diagnoseRunFailure", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("diagnoses a successful run with no failures", async () => {
    const client = createMockClient();
    mockRunAndResults(client, { ...BASE_RUN, runStatus: "completed" }, {
      data: [
        { nodeID: "n1", nodeName: "STG_CUSTOMERS", status: "completed" },
        { nodeID: "n2", nodeName: "STG_ORDERS", status: "completed" },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.runStatus).toBe("completed");
    expect(result.summary.totalNodes).toBe(2);
    expect(result.summary.succeeded).toBe(2);
    expect(result.summary.failed).toBe(0);
    expect(result.failures).toHaveLength(0);
    expect(result.recommendations).toContain(
      "This run completed successfully with no failures."
    );
  });

  it("classifies SQL syntax errors", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          nodeName: "STG_CUSTOMERS",
          nodeType: "Stage",
          status: "failed",
          errorMessage: "SQL compilation error: syntax error line 5 at position 10",
        },
        { nodeID: "n2", nodeName: "STG_ORDERS", status: "completed" },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.summary.failed).toBe(1);
    expect(result.summary.succeeded).toBe(1);
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]!.category).toBe("sql_error");
    expect(result.failures[0]!.nodeName).toBe("STG_CUSTOMERS");
    expect(result.failures[0]!.suggestedFixes.length).toBeGreaterThan(0);
  });

  it("classifies permission errors", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          nodeName: "DIM_PRODUCTS",
          status: "failed",
          errorMessage: "Insufficient privileges to operate on table 'RAW.PRODUCTS'",
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.failures[0]!.category).toBe("permission_error");
    expect(result.recommendations.some((r) => r.includes("permission"))).toBe(true);
  });

  it("classifies missing object errors", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          nodeName: "FCT_ORDERS",
          status: "failed",
          errorMessage: "Object 'RAW.STG_ORDERS' does not exist or not authorized",
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.failures[0]!.category).toBe("reference_error");
  });

  it("classifies data type errors", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          nodeName: "STG_PAYMENTS",
          status: "failed",
          errorMessage: "Numeric value 'abc' is not recognized",
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.failures[0]!.category).toBe("data_type_error");
  });

  it("classifies timeout errors", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          nodeName: "AGG_REVENUE",
          status: "failed",
          errorMessage: "Statement reached its statement or warehouse timeout and was canceled",
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.failures[0]!.category).toBe("timeout");
  });

  it("classifies configuration errors", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          nodeName: "DIM_DATE",
          status: "failed",
          errorMessage: "Duplicate column name 'DATE_KEY' in target table",
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.failures[0]!.category).toBe("configuration_error");
  });

  it("handles unknown error patterns", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          nodeName: "MYSTERY_NODE",
          status: "failed",
          errorMessage: "Something completely unexpected happened",
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.failures[0]!.category).toBe("unknown");
    expect(result.recommendations.some((r) => r.includes("unclassified"))).toBe(true);
  });

  it("handles multiple failures with different categories", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          nodeName: "NODE_A",
          status: "failed",
          errorMessage: "syntax error at position 10",
        },
        {
          nodeID: "n2",
          nodeName: "NODE_B",
          status: "failed",
          errorMessage: "Insufficient privileges on warehouse 'COMPUTE_WH'",
        },
        {
          nodeID: "n3",
          nodeName: "NODE_C",
          status: "completed",
        },
        {
          nodeID: "n4",
          nodeName: "NODE_D",
          status: "skipped",
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.summary.totalNodes).toBe(4);
    expect(result.summary.failed).toBe(2);
    expect(result.summary.succeeded).toBe(1);
    expect(result.summary.skipped).toBe(1);
    expect(result.failures).toHaveLength(2);
    expect(result.failures[0]!.category).toBe("sql_error");
    expect(result.failures[1]!.category).toBe("permission_error");
  });

  it("handles run marked failed with no node-level failures", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, { data: [] });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.runStatus).toBe("failed");
    expect(result.summary.failed).toBe(0);
    expect(result.recommendations.some((r) => r.includes("system-level failure"))).toBe(true);
  });

  it("handles canceled run", async () => {
    const client = createMockClient();
    mockRunAndResults(client, { ...BASE_RUN, runStatus: "canceled" }, { data: [] });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.runStatus).toBe("canceled");
    expect(result.recommendations.some((r) => r.includes("canceled"))).toBe(true);
  });

  it("warns when results fetch fails but still returns diagnosis", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.endsWith("/results")) {
        return Promise.reject(new Error("Connection reset"));
      }
      return Promise.resolve(BASE_RUN);
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.runStatus).toBe("failed");
    expect(result.warnings.some((w) => w.includes("Could not fetch run results"))).toBe(true);
    expect(result.summary.totalNodes).toBe(0);
  });

  it("re-throws auth errors (401/403)", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Unauthorized", 401));

    await expect(
      diagnoseRunFailure(client as any, { runID: "100" })
    ).rejects.toThrow("Unauthorized");
  });

  it("extracts run metadata correctly", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, { data: [] });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.runID).toBe("100");
    expect(result.runType).toBe("deploy");
    expect(result.environmentID).toBe("env-1");
    expect(result.startTime).toBe("2026-03-27T10:00:00Z");
    expect(result.endTime).toBe("2026-03-27T10:05:00Z");
    expect(result.analyzedAt).toBeTruthy();
  });

  it("handles results in { results: [...] } format", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      results: [
        { nodeID: "n1", status: "failed", error: "syntax error" },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.summary.failed).toBe(1);
    expect(result.failures[0]!.category).toBe("sql_error");
  });

  it("handles results as bare array", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, [
      { nodeID: "n1", status: "completed" },
    ]);

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.summary.totalNodes).toBe(1);
    expect(result.summary.succeeded).toBe(1);
  });

  it("extracts error from nested error object", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          nodeName: "NODE_A",
          status: "failed",
          error: { message: "Object 'MISSING_TABLE' does not exist" },
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.failures[0]!.errorMessage).toBe(
      "Object 'MISSING_TABLE' does not exist"
    );
    expect(result.failures[0]!.category).toBe("reference_error");
  });

  it("recommends retry for transient failures", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          status: "failed",
          errorMessage: "Warehouse 'COMPUTE_WH' is suspended or does not exist",
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.recommendations.some((r) => r.includes("retry_run"))).toBe(true);
  });

  it("flags high failure ratio", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        { nodeID: "n1", status: "failed", errorMessage: "access denied" },
        { nodeID: "n2", status: "failed", errorMessage: "access denied" },
        { nodeID: "n3", status: "failed", errorMessage: "access denied" },
        { nodeID: "n4", status: "completed" },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.recommendations.some((r) => r.includes("Over half"))).toBe(true);
  });

  it("handles node results with no error message", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        { nodeID: "n1", nodeName: "BROKEN_NODE", status: "failed" },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.failures[0]!.category).toBe("unknown");
    expect(result.failures[0]!.errorMessage).toBeNull();
    expect(result.failures[0]!.suggestedFixes).toContain(
      "No error message available. Check the Coalesce UI for details."
    );
  });

  it("includes run-level error in warnings", async () => {
    const client = createMockClient();
    mockRunAndResults(
      client,
      { ...BASE_RUN, error: "Authentication failed for account" },
      { data: [] }
    );

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.warnings.some((w) => w.includes("Authentication failed"))).toBe(true);
  });

  it("classifies warehouse suspended as permission error", async () => {
    const client = createMockClient();
    mockRunAndResults(client, BASE_RUN, {
      data: [
        {
          nodeID: "n1",
          status: "failed",
          errorMessage: "Warehouse 'ANALYTICS_WH' is suspended",
        },
      ],
    });

    const result = await diagnoseRunFailure(client as any, { runID: "100" });

    expect(result.failures[0]!.category).toBe("permission_error");
  });
});
