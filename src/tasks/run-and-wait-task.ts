import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  StartRunParams,
  buildJsonToolResponse,
  WRITE_ANNOTATIONS,
  sanitizeResponse,
  getToolOutputSchema,
} from "../coalesce/types.js";
import { runAndWait } from "../workflows/run-and-wait.js";

const RunAndWaitTaskInput = StartRunParams.extend({
  pollInterval: z
    .number()
    .optional()
    .describe("Seconds between status checks (default: 10, min: 5, max: 300)"),
  timeout: z
    .number()
    .optional()
    .describe("Max seconds to wait (default: 1800, min: 30, max: 3600)"),
});

type RunAndWaitParams = z.infer<typeof RunAndWaitTaskInput>;

export function registerRunAndWaitTask(
  server: McpServer,
  client: CoalesceClient
): void {
  server.experimental.tasks.registerToolTask(
    "run_and_wait",
    {
      title: "Run and Wait",
      description:
        "Start a Coalesce refresh run and poll until completion or timeout. Returns a task that can be polled for status.\n\nRequires Snowflake auth via environment variables (Key Pair or PAT). Provide exactly one of runDetails.environmentID or runDetails.workspaceID. If the user provides a job name instead of an ID, look it up with list_environment_jobs first.\n\nArgs:\n  - runDetails.environmentID (string, optional): Target deployed environment (numeric ID as string)\n  - runDetails.workspaceID (string, optional): Target workspace for a development run (numeric ID as string)\n  - runDetails.jobID (string, optional): Specific job to run\n  - runDetails.includeNodesSelector (string, optional): Node filter\n  - runDetails.excludeNodesSelector (string, optional): Node exclusion\n  - runDetails.parallelism (number, optional): Max parallel nodes\n  - runDetails.forceIgnoreWorkspaceStatus (boolean, optional): Force run\n  - confirmRunAllNodes (boolean): Required when no job/node scope\n  - parameters (object, optional): Runtime parameters\n  - pollInterval (number, optional): Seconds between checks (default: 10, range: 5–300)\n  - timeout (number, optional): Max wait seconds (default: 1800, range: 30–3600)\n\nReturns:\n  { status, results, resultsError?, incomplete?, timedOut? }\n\nInspect timedOut, incomplete, and resultsError fields before continuing.",
      inputSchema: RunAndWaitTaskInput,
      outputSchema: getToolOutputSchema("run_and_wait"),
      annotations: WRITE_ANNOTATIONS,
      execution: { taskSupport: "optional" },
    },
    {
      async createTask(rawParams, extra) {
        const params = rawParams as RunAndWaitParams;
        const task = await extra.taskStore.createTask({
          ttl: extra.taskRequestedTtl,
          pollInterval: (params.pollInterval ?? 10) * 1000,
        });

        (async () => {
          try {
            const result = await runAndWait(client, params, {
              signal: extra.signal,
            });
            const sanitized = sanitizeResponse(result);
            const response = buildJsonToolResponse("run_and_wait", sanitized, {
              workspaceID: params.runDetails?.workspaceID,
              environmentID: params.runDetails?.environmentID,
            });
            await extra.taskStore.storeTaskResult(
              task.taskId,
              "completed",
              response
            );
          } catch (error) {
            const message =
              error instanceof Error ? error.message : String(error);
            await extra.taskStore.storeTaskResult(task.taskId, "failed", {
              content: [{ type: "text", text: `run_and_wait failed: ${message}` }],
              isError: true,
            });
          }
        })();

        return { task };
      },

      async getTask(_params, extra) {
        const task = await extra.taskStore.getTask(extra.taskId);
        if (!task) {
          throw new Error(`Task ${extra.taskId} not found`);
        }
        return task;
      },

      async getTaskResult(_params, extra) {
        const result = await extra.taskStore.getTaskResult(extra.taskId);
        return result as ReturnType<typeof buildJsonToolResponse>;
      },
    }
  );
}
