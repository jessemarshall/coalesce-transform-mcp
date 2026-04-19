import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  StartRunParams,
  type StartRunInput,
  buildJsonToolResponse,
  WRITE_ANNOTATIONS,
  sanitizeResponse,
  getToolOutputSchema,
} from "../coalesce/types.js";
import { startRun } from "../coalesce/api/runs.js";

export function registerStartRunTask(
  server: McpServer,
  client: CoalesceClient
): void {
  server.experimental.tasks.registerToolTask(
    "start_run",
    {
      title: "Start Run",
      description:
        "Start a new Coalesce refresh run. Returns a task that resolves once the run has been submitted.\n\nRequires Snowflake auth — credentials read from environment variables (Key Pair: SNOWFLAKE_KEY_PAIR_KEY, or PAT: SNOWFLAKE_PAT, plus SNOWFLAKE_USERNAME, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE).\n\nProvide exactly one of runDetails.environmentID or runDetails.workspaceID. If the user provides a job name, look up the ID with list_environment_jobs first.\n\nArgs:\n  - runDetails.environmentID (string, optional): Target deployed environment (numeric ID as string)\n  - runDetails.workspaceID (string, optional): Target workspace for a development run (numeric ID as string)\n  - runDetails.jobID (string, optional): Specific job to run\n  - runDetails.includeNodesSelector (string, optional): Node filter for ad-hoc runs\n  - runDetails.excludeNodesSelector (string, optional): Node exclusion filter\n  - runDetails.parallelism (number, optional): Max parallel nodes (default: 16)\n  - runDetails.forceIgnoreWorkspaceStatus (boolean, optional): Allow run even if last deploy failed\n  - confirmRunAllNodes (boolean): Required when no job/node scope is provided\n  - parameters (object, optional): Key-value runtime parameters\n\nReturns:\n  { runCounter: number, runStatus: string, message: string }\n\nPrefer run_and_wait when you need the final outcome in a single call.",
      inputSchema: StartRunParams,
      outputSchema: getToolOutputSchema("start_run"),
      annotations: WRITE_ANNOTATIONS,
      execution: { taskSupport: "optional" },
    },
    {
      async createTask(rawParams, extra) {
        const params = rawParams as StartRunInput;
        const task = await extra.taskStore.createTask({
          ttl: extra.taskRequestedTtl,
        });

        (async () => {
          try {
            const result = await startRun(client, params);
            const sanitized = sanitizeResponse(result);
            const response = buildJsonToolResponse("start_run", sanitized, {
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
              content: [{ type: "text", text: `start_run failed: ${message}` }],
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
