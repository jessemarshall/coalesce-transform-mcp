import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  RerunParams,
  buildJsonToolResponse,
  buildRerunBody,
  WRITE_ANNOTATIONS,
  sanitizeResponse,
  handleToolError,
  getToolOutputSchema,
} from "../coalesce/types.js";
import {
  createWorkflowProgressReporter,
  remainingTimeMs,
  throwIfAborted,
  type WorkflowProgressExtra,
  type WorkflowProgressReporter,
} from "./progress.js";
import { pollRunToCompletion } from "./poll-run.js";
import {
  POLL_INTERVAL_MIN_S,
  POLL_INTERVAL_DEFAULT_S,
  POLL_INTERVAL_MAX_S,
  WORKFLOW_TIMEOUT_MIN_S,
  WORKFLOW_TIMEOUT_DEFAULT_S,
  WORKFLOW_TIMEOUT_MAX_S,
} from "../constants.js";

export async function retryAndWait(
  client: CoalesceClient,
  params: z.infer<typeof RerunParams> & {
    pollInterval?: number;
    timeout?: number;
  },
  options: {
    signal?: AbortSignal;
    reportProgress?: WorkflowProgressReporter;
  } = {}
): Promise<unknown> {
  const pollIntervalMs =
    Math.max(POLL_INTERVAL_MIN_S, Math.min(params.pollInterval ?? POLL_INTERVAL_DEFAULT_S, POLL_INTERVAL_MAX_S)) * 1000;
  const timeoutMs =
    Math.max(WORKFLOW_TIMEOUT_MIN_S, Math.min(params.timeout ?? WORKFLOW_TIMEOUT_DEFAULT_S, WORKFLOW_TIMEOUT_MAX_S)) * 1000;
  const startedAt = Date.now();
  const { signal, reportProgress } = options;

  throwIfAborted(signal);

  // Retry the run — response is { runCounter: number }
  const body = buildRerunBody(params);
  const rerunResult = (await client.post("/scheduler/rerun", body, undefined, {
    timeoutMs: remainingTimeMs(startedAt, timeoutMs),
    signal,
  })) as Record<string, unknown>;
  if (typeof rerunResult.runCounter !== "number") {
    throw new Error(
      `rerun response did not include a numeric runCounter (got ${typeof rerunResult.runCounter})`
    );
  }
  const runCounter: number = rerunResult.runCounter;
  await reportProgress?.(
    `Started retry run ${runCounter}. Polling every ${pollIntervalMs / 1000}s for up to ${timeoutMs / 1000}s.`
  );

  return pollRunToCompletion({
    client,
    runCounter,
    label: "retry run",
    pollIntervalMs,
    timeoutMs,
    startedAt,
    signal,
    reportProgress,
  });
}

export function registerRetryAndWait(server: McpServer, client: CoalesceClient): void {
  server.registerTool(
    "retry_and_wait",
    {
      title: "Retry and Wait",
      description:
        "Retry a failed Coalesce run and poll until completion or timeout. Preferred tool for immediate reruns of failed runs with a single call.\n\nRequires Snowflake Key Pair auth via environment variables.\n\nArgs:\n  - runDetails.runID (string, required): The run ID to retry\n  - runDetails.forceIgnoreWorkspaceStatus (boolean, optional): Force retry\n  - parameters (object, optional): Runtime parameters\n  - pollInterval (number, optional): Seconds between checks (default: 10)\n  - timeout (number, optional): Max wait seconds (default: 1800)\n\nReturns:\n  { status, results, resultsError?, incomplete?, timedOut? }\n\nInspect timedOut, incomplete, and resultsError fields before continuing.",
      inputSchema: RerunParams.extend({
        pollInterval: z.number().optional().describe("Seconds between status checks (default: 10, min: 5, max: 300)"),
        timeout: z.number().optional().describe("Max seconds to wait (default: 1800, min: 30, max: 3600)"),
      }),
      outputSchema: getToolOutputSchema("retry_and_wait"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params, extra) => {
      try {
        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );
        const result = await retryAndWait(client, params, {
          signal: extra?.signal,
          reportProgress: progressReporter,
        });
        return buildJsonToolResponse("retry_and_wait", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
