import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  RerunParams,
  buildJsonToolResponse,
  WRITE_ANNOTATIONS,
  sanitizeResponse,
  getToolOutputSchema,
} from "../coalesce/types.js";
import { retryAndWait, extractResultScope } from "../workflows/retry-and-wait.js";

const RetryAndWaitTaskInput = RerunParams.extend({
  pollInterval: z
    .number()
    .optional()
    .describe("Seconds between status checks (default: 10, min: 5, max: 300)"),
  timeout: z
    .number()
    .optional()
    .describe("Max seconds to wait (default: 1800, min: 30, max: 3600)"),
});

type RetryAndWaitParams = z.infer<typeof RetryAndWaitTaskInput>;

export function registerRetryAndWaitTask(
  server: McpServer,
  client: CoalesceClient
): void {
  server.experimental.tasks.registerToolTask(
    "retry_and_wait",
    {
      title: "Retry and Wait",
      description:
        "Retry a failed Coalesce run and poll until completion or timeout. Returns a task that can be polled for status.\n\nRequires Snowflake auth via environment variables (Key Pair or PAT).\n\nArgs:\n  - runDetails.runID (string, required): The run ID to retry\n  - runDetails.forceIgnoreWorkspaceStatus (boolean, optional): Force retry\n  - parameters (object, optional): Runtime parameters\n  - pollInterval (number, optional): Seconds between checks (default: 10)\n  - timeout (number, optional): Max wait seconds (default: 1800)\n\nReturns:\n  { status, results, resultsError?, incomplete?, timedOut? }\n\nInspect timedOut, incomplete, and resultsError fields before continuing.",
      inputSchema: RetryAndWaitTaskInput,
      outputSchema: getToolOutputSchema("retry_and_wait"),
      annotations: WRITE_ANNOTATIONS,
      execution: { taskSupport: "optional" },
    },
    {
      async createTask(rawParams, extra) {
        const params = rawParams as RetryAndWaitParams;
        const task = await extra.taskStore.createTask({
          ttl: extra.taskRequestedTtl,
          pollInterval: (params.pollInterval ?? 10) * 1000,
        });

        (async () => {
          try {
            const result = await retryAndWait(client, params, {
              signal: extra.signal,
            });
            const sanitized = sanitizeResponse(result);
            const response = buildJsonToolResponse("retry_and_wait", sanitized, extractResultScope(sanitized));
            await extra.taskStore.storeTaskResult(
              task.taskId,
              "completed",
              response
            );
          } catch (error) {
            const message =
              error instanceof Error ? error.message : String(error);
            await extra.taskStore.storeTaskResult(task.taskId, "failed", {
              content: [{ type: "text", text: `retry_and_wait failed: ${message}` }],
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
