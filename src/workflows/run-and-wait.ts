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
  getToolOutputSchema,
} from "../coalesce/types.js";
import {
  createWorkflowProgressReporter,
  remainingTimeMs,
  serializeResultsError,
  sleepWithAbort,
  throwIfAborted,
  type WorkflowProgressExtra,
  type WorkflowProgressReporter,
} from "./progress.js";
import {
  formatRunStatusForMessage,
  isTerminalRunStatus,
  validateRunStatus,
} from "./run-status.js";

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
    await reportProgress?.(
      `Status check ${pollCount} for run ${runCounter}: ${formatRunStatusForMessage(runStatus)}.`
    );
    const validatedRunStatus = validateRunStatus(runCounter, runStatus);
    if (isTerminalRunStatus(validatedRunStatus)) {
      // Fetch run results — runCounter is the numeric run ID
      await reportProgress?.(
        `Run ${runCounter} reached terminal status ${validatedRunStatus}. Fetching results.`
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
  if (finalStatus && typeof finalStatus === "object") {
    validateRunStatus(runCounter, (finalStatus as Record<string, unknown>).runStatus);
  }
  await reportProgress?.(
    `Timed out waiting for run ${runCounter}. Returning the last known status.`
  );
  return { status: finalStatus, results: null, timedOut: true };
}

export function registerRunAndWait(server: McpServer, client: CoalesceClient): void {
  server.registerTool(
    "run_and_wait",
    {
      title: "Run and Wait",
      description:
        "Start a Coalesce refresh run and poll until completion or timeout. This is the preferred tool when the user wants an end-to-end run outcome in a single call.\n\nRequires Snowflake Key Pair auth via environment variables. If the user provides a job name instead of an ID, look it up with list_environment_jobs first.\n\nArgs:\n  - runDetails.environmentID (string, required): Target environment\n  - runDetails.jobID (string, optional): Specific job to run\n  - runDetails.includeNodesSelector (string, optional): Node filter\n  - runDetails.excludeNodesSelector (string, optional): Node exclusion\n  - runDetails.parallelism (number, optional): Max parallel nodes\n  - runDetails.forceIgnoreWorkspaceStatus (boolean, optional): Force run\n  - confirmRunAllNodes (boolean): Required when no job/node scope\n  - parameters (object, optional): Runtime parameters\n  - pollInterval (number, optional): Seconds between checks (default: 10, range: 5–300)\n  - timeout (number, optional): Max wait seconds (default: 1800, range: 30–3600)\n\nReturns:\n  { status, results, resultsError?, incomplete?, timedOut? }\n\nInspect timedOut, incomplete, and resultsError fields before continuing.",
      inputSchema: StartRunParams.extend({
        pollInterval: z.number().optional().describe("Seconds between status checks (default: 10, min: 5, max: 300)"),
        timeout: z.number().optional().describe("Max seconds to wait (default: 1800, min: 30, max: 3600)"),
      }),
      outputSchema: getToolOutputSchema("run_and_wait"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params, extra) => {
      try {
        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );
        const result = await runAndWait(client, params, {
          signal: extra?.signal,
          reportProgress: progressReporter,
        });
        return buildJsonToolResponse("run_and_wait", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
