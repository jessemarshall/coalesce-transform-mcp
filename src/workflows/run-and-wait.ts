import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { CoalesceApiError, type CoalesceClient } from "../client.js";
import {
  StartRunParams,
  buildJsonToolResponse,
  buildStartRunBody,
  WRITE_ANNOTATIONS,
  sanitizeResponse,
  handleToolError,
} from "../coalesce/types.js";
import {
  createWorkflowProgressReporter,
  sleepWithAbort,
  throwIfAborted,
  type WorkflowProgressExtra,
  type WorkflowProgressReporter,
} from "./progress.js";

const TERMINAL_RUN_STATUSES = new Set(["completed", "failed", "canceled"]);
const KNOWN_RUN_STATUSES = new Set(["completed", "failed", "canceled", "running", "waitingToRun"]);
const MAX_CONSECUTIVE_UNRECOGNIZED = 5;

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

export async function runAndWait(
  client: CoalesceClient,
  params: z.infer<typeof StartRunParams> & {
    pollInterval?: number;
    timeout?: number;
  },
  options: {
    signal?: AbortSignal;
    reportProgress?: WorkflowProgressReporter;
  } = {}
): Promise<unknown> {
  const pollInterval = Math.max(5, Math.min(params.pollInterval ?? 10, 300)) * 1000;
  const timeout = Math.max(30, Math.min(params.timeout ?? 1800, 3600)) * 1000;
  const startedAt = Date.now();
  const { signal, reportProgress } = options;

  throwIfAborted(signal);

  // Start the run — response is { runCounter: number }
  const body = buildStartRunBody(params);
  const startResult = (await client.post("/scheduler/startRun", body, undefined, {
    timeoutMs: remainingTimeMs(startedAt, timeout),
  })) as Record<string, unknown>;
  if (typeof startResult.runCounter !== "number") {
    throw new Error(
      `startRun response did not include a numeric runCounter (got ${typeof startResult.runCounter})`
    );
  }
  const runCounter: number = startResult.runCounter;
  await reportProgress?.(
    `Started run ${runCounter}. Polling every ${pollInterval / 1000}s for up to ${timeout / 1000}s.`
  );

  // Poll for status
  let lastStatus: unknown = null;
  let pollCount = 0;
  let consecutiveUnrecognized = 0;
  while (remainingTimeMs(startedAt, timeout) > 0) {
    const nextPollDelay = Math.min(pollInterval, remainingTimeMs(startedAt, timeout));
    await sleepWithAbort(nextPollDelay, signal);

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
        pollCount += 1;
        await reportProgress?.(
          `Status check ${pollCount} for run ${runCounter} timed out. Retrying while time remains.`
        );
        continue;
      }
      throw error;
    }
    lastStatus = status;
    pollCount += 1;

    const runStatus = status.runStatus;
    if (typeof runStatus !== "string") {
      throw new Error(
        `runStatus response missing a string runStatus field (got ${typeof runStatus})`
      );
    }

    if (!KNOWN_RUN_STATUSES.has(runStatus)) {
      consecutiveUnrecognized += 1;
      await reportProgress?.(
        `Status check ${pollCount} for run ${runCounter}: unrecognized status "${runStatus}" (${consecutiveUnrecognized}/${MAX_CONSECUTIVE_UNRECOGNIZED}).`
      );
      if (consecutiveUnrecognized >= MAX_CONSECUTIVE_UNRECOGNIZED) {
        throw new Error(
          `Run ${runCounter} returned unrecognized status "${runStatus}" ${MAX_CONSECUTIVE_UNRECOGNIZED} consecutive times. Aborting to avoid an infinite poll loop.`
        );
      }
      continue;
    }
    consecutiveUnrecognized = 0;

    await reportProgress?.(
      `Status check ${pollCount} for run ${runCounter}: ${runStatus}.`
    );
    if (TERMINAL_RUN_STATUSES.has(runStatus)) {
      // Fetch run results — runCounter is the numeric run ID
      await reportProgress?.(
        `Run ${runCounter} reached terminal status ${runStatus}. Fetching results.`
      );
      const resultsTimeoutMs = remainingTimeMs(startedAt, timeout);
      if (resultsTimeoutMs <= 0) {
        await reportProgress?.(
          `Workflow deadline reached before results could be fetched for run ${runCounter}.`
        );
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
        await reportProgress?.(`Fetched results for run ${runCounter}.`);
        return { status, results };
      } catch (error) {
        const serializedError = serializeResultsError(error);
        await reportProgress?.(
          `Run ${runCounter} finished, but fetching results failed: ${serializedError.message}.`
        );
        return {
          status,
          results: null,
          resultsError: serializedError,
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
          runCounter,
        },
        { timeoutMs: finalStatusTimeoutMs }
      );
    } catch (error) {
      if (!(error instanceof CoalesceApiError && error.status === 408)) {
        throw error;
      }
    }
  }
  await reportProgress?.(
    `Timed out waiting for run ${runCounter}. Returning the last known status.`
  );
  return { status: finalStatus, results: null, timedOut: true };
}

export function registerRunAndWait(server: McpServer, client: CoalesceClient): void {
  server.tool(
    "run-and-wait",
    "Start a Coalesce run and wait for completion. Requires a numeric environmentID and optionally a jobID (not job name). " +
    "If the user provides a job name instead of an ID, ask them for the job ID. " +
    "If the user doesn't know the environment ID, use list-environments to look it up by name. " +
    "Requires Snowflake Key Pair auth; credentials are read from environment variables. Polls run status until finished or timeout.",
    StartRunParams.extend({
      pollInterval: z.number().optional().describe("Seconds between status checks (default: 10, min: 5, max: 300)"),
      timeout: z.number().optional().describe("Max seconds to wait (default: 1800, min: 30, max: 3600)"),
    }).shape,
    WRITE_ANNOTATIONS,
    async (params, extra) => {
      try {
        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );
        const result = await runAndWait(client, params, {
          signal: extra?.signal,
          reportProgress: progressReporter,
        });
        return buildJsonToolResponse("run-and-wait", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
