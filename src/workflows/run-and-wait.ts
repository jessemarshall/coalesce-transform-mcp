import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
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
  clampWithWarning,
} from "../constants.js";

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
  const pollClamped = clampWithWarning(
    params.pollInterval ?? POLL_INTERVAL_DEFAULT_S,
    POLL_INTERVAL_MIN_S,
    POLL_INTERVAL_MAX_S,
    "pollInterval"
  );
  const timeoutClamped = clampWithWarning(
    params.timeout ?? WORKFLOW_TIMEOUT_DEFAULT_S,
    WORKFLOW_TIMEOUT_MIN_S,
    WORKFLOW_TIMEOUT_MAX_S,
    "timeout"
  );
  const pollIntervalMs = pollClamped.value * 1000;
  const timeoutMs = timeoutClamped.value * 1000;
  const startedAt = Date.now();
  const { signal, reportProgress } = options;

  throwIfAborted(signal);

  // Surface clamping warnings so the caller knows values were adjusted.
  // Falls back to stderr when reportProgress is not wired up (e.g. programmatic callers).
  for (const w of [pollClamped.warning, timeoutClamped.warning]) {
    if (w) {
      if (reportProgress) await reportProgress(w);
      else console.warn(`[run_and_wait] ${w}`);
    }
  }

  // Start the run — response is { runCounter: number }
  const body = buildStartRunBody(params);
  const startResult = (await client.post("/scheduler/startRun", body, undefined, {
    timeoutMs: remainingTimeMs(startedAt, timeoutMs),
    signal,
  })) as Record<string, unknown>;
  if (typeof startResult.runCounter !== "number") {
    throw new Error(
      `startRun response did not include a numeric runCounter (got ${typeof startResult.runCounter})`
    );
  }
  const runCounter: number = startResult.runCounter;
  await reportProgress?.(
    `Started run ${runCounter}. Polling every ${pollClamped.value}s for up to ${timeoutClamped.value}s.`
  );

  return pollRunToCompletion({
    client,
    runCounter,
    label: "run",
    pollIntervalMs,
    timeoutMs,
    startedAt,
    signal,
    reportProgress,
  });
}

export function registerRunAndWait(server: McpServer, client: CoalesceClient): void {
  server.registerTool(
    "run_and_wait",
    {
      title: "Run and Wait",
      description:
        "Start a Coalesce refresh run and poll until completion or timeout. This is the preferred tool when the user wants an end-to-end run outcome in a single call.\n\nRequires Snowflake auth via environment variables (Key Pair or PAT). Provide exactly one of runDetails.environmentID or runDetails.workspaceID. If the user provides a job name instead of an ID, look it up with list_environment_jobs first.\n\nArgs:\n  - runDetails.environmentID (string, optional): Target deployed environment (numeric ID as string)\n  - runDetails.workspaceID (string, optional): Target workspace for a development run (numeric ID as string)\n  - runDetails.jobID (string, optional): Specific job to run\n  - runDetails.includeNodesSelector (string, optional): Node filter\n  - runDetails.excludeNodesSelector (string, optional): Node exclusion\n  - runDetails.parallelism (number, optional): Max parallel nodes\n  - runDetails.forceIgnoreWorkspaceStatus (boolean, optional): Force run\n  - confirmRunAllNodes (boolean): Required when no job/node scope\n  - parameters (object, optional): Runtime parameters\n  - pollInterval (number, optional): Seconds between checks (default: 10, range: 5–300)\n  - timeout (number, optional): Max wait seconds (default: 1800, range: 30–3600)\n\nReturns:\n  { status, results, resultsError?, incomplete?, timedOut? }\n\nInspect timedOut, incomplete, and resultsError fields before continuing.",
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
        return buildJsonToolResponse("run_and_wait", sanitizeResponse(result), {
          workspaceID:
            params.runDetails?.workspaceID ?? params.runDetails?.environmentID,
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
