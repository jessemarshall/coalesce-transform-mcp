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
  clampWithWarning,
} from "../constants.js";

/**
 * Best-effort extraction of workspace or environment scope from a run-status
 * result so the auto-cache can partition by workspace/environment even when
 * the tool input doesn't carry those IDs (retry_and_wait takes only runID).
 *
 * `pollRunToCompletion` returns `{ status, results, ... }` where `status` is
 * the raw `/scheduler/runStatus` payload — that's where the workspace and
 * environment IDs live. Top-level reads cover callers that hand us a flatter
 * shape.
 *
 * Falls through to an empty object when neither ID is present; the auto-cache
 * then lands in `_global`, matching the pre-existing behaviour.
 */
export function extractResultScope(value: unknown): {
  workspaceID?: string;
  environmentID?: string;
} {
  const candidates: Record<string, unknown>[] = [];
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    candidates.push(record);
    // `pollRunToCompletion` nests the run-status payload under `status`; probe
    // that too so retry_and_wait / run_and_wait results get their IDs seen.
    const status = record.status;
    if (status && typeof status === "object" && !Array.isArray(status)) {
      candidates.push(status as Record<string, unknown>);
    }
  }

  for (const candidate of candidates) {
    const ws = candidate.workspaceID;
    if (typeof ws === "string" && ws.length > 0) return { workspaceID: ws };
    const env = candidate.environmentID;
    if (typeof env === "string" && env.length > 0) return { environmentID: env };
  }
  return {};
}

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
      else console.warn(`[retry_and_wait] ${w}`);
    }
  }

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
    `Started retry run ${runCounter}. Polling every ${pollClamped.value}s for up to ${timeoutClamped.value}s.`
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
        "Retry a failed Coalesce run and poll until completion or timeout. Preferred tool for immediate reruns of failed runs with a single call.\n\nRequires Snowflake auth via environment variables (Key Pair or PAT).\n\nArgs:\n  - runDetails.runID (string, required): The run ID to retry\n  - runDetails.forceIgnoreWorkspaceStatus (boolean, optional): Force retry\n  - parameters (object, optional): Runtime parameters\n  - pollInterval (number, optional): Seconds between checks (default: 10)\n  - timeout (number, optional): Max wait seconds (default: 1800)\n\nReturns:\n  { status, results, resultsError?, incomplete?, timedOut? }\n\nInspect timedOut, incomplete, and resultsError fields before continuing.",
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
        const sanitized = sanitizeResponse(result);
        return buildJsonToolResponse("retry_and_wait", sanitized, extractResultScope(sanitized));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
