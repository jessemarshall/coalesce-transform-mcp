import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { CoalesceApiError, type CoalesceClient } from "../client.js";
import {
  RerunParams,
  buildJsonToolResponse,
  buildRerunBody,
  WRITE_ANNOTATIONS,
  sanitizeResponse,
  handleToolError,
} from "../coalesce/types.js";

function remainingTimeMs(startedAt: number, totalTimeoutMs: number): number {
  return Math.max(0, totalTimeoutMs - (Date.now() - startedAt));
}

function serializeResultsError(error: unknown): Record<string, unknown> {
  if (error instanceof CoalesceApiError) {
    return {
      message: error.message,
      status: error.status,
      ...(error.detail !== undefined ? { detail: error.detail } : {}),
    };
  }
  if (error instanceof Error) {
    return { message: error.message };
  }
  return { message: "Unable to fetch run results", detail: error };
}

export async function retryAndWait(
  client: CoalesceClient,
  params: z.infer<typeof RerunParams> & {
    pollInterval?: number;
    timeout?: number;
  }
): Promise<unknown> {
  const pollInterval = Math.max(5, Math.min(params.pollInterval ?? 10, 300)) * 1000;
  const timeout = Math.max(30, Math.min(params.timeout ?? 1800, 3600)) * 1000;
  const startedAt = Date.now();

  // Retry the run — response is { runCounter: number }
  const body = buildRerunBody(params);
  const rerunResult = (await client.post("/scheduler/rerun", body, undefined, {
    timeoutMs: remainingTimeMs(startedAt, timeout),
  })) as Record<string, unknown>;
  if (typeof rerunResult.runCounter !== "number") {
    throw new Error(
      `rerun response did not include a numeric runCounter (got ${typeof rerunResult.runCounter})`
    );
  }
  const runCounter: number = rerunResult.runCounter;

  // Poll for status
  let lastStatus: unknown = null;
  while (remainingTimeMs(startedAt, timeout) > 0) {
    const nextPollDelay = Math.min(pollInterval, remainingTimeMs(startedAt, timeout));
    await new Promise((resolve) => setTimeout(resolve, nextPollDelay));

    const statusTimeoutMs = remainingTimeMs(startedAt, timeout);
    if (statusTimeoutMs <= 0) {
      break;
    }

    let status: Record<string, unknown>;
    try {
      status = (await client.get(
        "/scheduler/runStatus",
        {
          runCounter,
        },
        { timeoutMs: statusTimeoutMs }
      )) as Record<string, unknown>;
    } catch (error) {
      if (error instanceof CoalesceApiError && error.status === 408) {
        continue;
      }
      throw error;
    }
    lastStatus = status;

    const runStatus = status.runStatus;
    if (runStatus === "completed" || runStatus === "failed" || runStatus === "canceled") {
      // Fetch run results — runCounter is the numeric run ID
      const resultsTimeoutMs = remainingTimeMs(startedAt, timeout);
      if (resultsTimeoutMs <= 0) {
        return {
          status,
          results: null,
          resultsError: {
            message: "Workflow timeout reached before run results could be fetched",
            status: 408,
          },
          incomplete: true,
        };
      }

      try {
        const results = await client.get(
          `/api/v1/runs/${runCounter}/results`,
          undefined,
          { timeoutMs: resultsTimeoutMs }
        );
        return { status, results };
      } catch (error) {
        return {
          status,
          results: null,
          resultsError: serializeResultsError(error),
          incomplete: true,
        };
      }
    }
  }

  // Timeout — return last known status
  let finalStatus = lastStatus;
  const finalStatusTimeoutMs = remainingTimeMs(startedAt, timeout);
  if (finalStatusTimeoutMs > 0) {
    try {
      finalStatus = await client.get(
        "/scheduler/runStatus",
        {
          runCounter: runCounter as number,
        },
        { timeoutMs: finalStatusTimeoutMs }
      );
    } catch (error) {
      if (!(error instanceof CoalesceApiError && error.status === 408)) {
        throw error;
      }
    }
  }
  return { status: finalStatus, results: null, timedOut: true };
}

export function registerRetryAndWait(server: McpServer, client: CoalesceClient): void {
  server.tool(
    "retry-and-wait",
    "Retry a failed Coalesce run and wait for it to complete. Requires the runID from the original run. " +
    "Requires Snowflake Key Pair auth; credentials are read from environment variables. Polls run status until finished or timeout.",
    RerunParams.extend({
      pollInterval: z.number().optional().describe("Seconds between status checks (default: 10, min: 5, max: 300)"),
      timeout: z.number().optional().describe("Max seconds to wait (default: 1800, min: 30, max: 3600)"),
    }).shape,
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await retryAndWait(client, params);
        return buildJsonToolResponse("retry-and-wait", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
