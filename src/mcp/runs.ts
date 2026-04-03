import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listRuns,
  getRun,
  getRunResults,
  startRun,
  runStatus,
  retryRun,
  cancelRun,
} from "../coalesce/api/runs.js";
import { diagnoseRunFailure } from "../services/runs/diagnostics.js";
import {
  PaginationParams,
  StartRunParams,
  RerunParams,
  buildJsonToolResponse,
  handleToolError,
  sanitizeResponse,
  getToolOutputSchema,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";
import { requireDestructiveConfirmation } from "../services/shared/elicitation.js";

export function registerRunTools(
  server: McpServer,
  client: CoalesceClient,
  options?: { skipStartRun?: boolean }
): void {
  server.registerTool(
    "list_runs",
    {
      title: "List Runs",
      description:
        "List Coalesce runs with optional filters for type, status, and environment.\n\nArgs:\n  - runType ('deploy'|'refresh', optional): Filter by type\n  - runStatus ('completed'|'failed'|'canceled'|'running'|'waitingToRun', optional): Filter by status\n  - environmentID (string, optional): Filter by environment\n  - detail (boolean, optional): Include full run details\n  - limit, startingFrom, orderBy, orderByDirection: Pagination controls\n\nReturns:\n  { data: Run[], next?: string, total?: number }",
      inputSchema: PaginationParams.extend({
        runType: z.enum(["deploy", "refresh"]).optional().describe("Filter by run type"),
        runStatus: z.enum(["completed", "failed", "canceled", "running", "waitingToRun"]).optional().describe("Filter by run status"),
        environmentID: z.string().optional().describe("Filter by environment ID"),
        detail: z.boolean().optional().describe("Include full run details in response"),
      }),
      outputSchema: getToolOutputSchema("list_runs"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listRuns(client, params);
        return buildJsonToolResponse("list_runs", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_run",
    {
      title: "Get Run",
      description:
        "Get details of a specific Coalesce run.\n\nArgs:\n  - runID (string, required): Numeric run ID (integer, e.g. '401'). Use the runCounter value from start_run or run_status — not the UUID from run URLs.\n\nReturns:\n  Full run object with status, timing, node results, and configuration.",
      inputSchema: z.object({
        runID: z.string().describe("The numeric run ID (integer, e.g. '401'). Use the runCounter value from start_run or run_status responses — not the UUID from run URLs."),
      }),
      outputSchema: getToolOutputSchema("get_run"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getRun(client, params);
        return buildJsonToolResponse("get_run", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_run_results",
    {
      title: "Get Run Results",
      description:
        "Get the execution results of a specific Coalesce run.\n\nArgs:\n  - runID (string, required): Numeric run ID (integer). Use runCounter, not the UUID.\n\nReturns:\n  Run results including per-node execution status, row counts, and errors.",
      inputSchema: z.object({
        runID: z.string().describe("The numeric run ID (integer, e.g. '401'). Use the runCounter value from start_run or run_status responses — not the UUID from run URLs."),
      }),
      outputSchema: getToolOutputSchema("get_run_results"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getRunResults(client, params);
        return buildJsonToolResponse("get_run_results", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  if (!options?.skipStartRun) {
    server.registerTool(
      "start_run",
      {
        title: "Start Run",
        description:
          "Start a new Coalesce refresh run. Requires Snowflake Key Pair auth — credentials are read from SNOWFLAKE_USERNAME, SNOWFLAKE_KEY_PAIR_KEY, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE environment variables.\n\nRequires a numeric environmentID and optionally a jobID. If the user provides a job name, look up the ID with list_environment_jobs first.\n\nArgs:\n  - runDetails.environmentID (string, required): Target environment\n  - runDetails.jobID (string, optional): Specific job to run\n  - runDetails.includeNodesSelector (string, optional): Node filter for ad-hoc runs\n  - runDetails.excludeNodesSelector (string, optional): Node exclusion filter\n  - runDetails.parallelism (number, optional): Max parallel nodes (default: 16)\n  - runDetails.forceIgnoreWorkspaceStatus (boolean, optional): Allow run even if last deploy failed\n  - confirmRunAllNodes (boolean): Required when no job/node scope is provided\n  - parameters (object, optional): Key-value runtime parameters\n\nReturns:\n  { runCounter: number, runStatus: string, message: string }\n\nPrefer run_and_wait when you need the final outcome in a single call.",
        inputSchema: StartRunParams,
        outputSchema: getToolOutputSchema("start_run"),
        annotations: WRITE_ANNOTATIONS,
      },
      async (params) => {
        try {
          const result = await startRun(client, params);
          return buildJsonToolResponse("start_run", sanitizeResponse(result));
        } catch (error) {
          return handleToolError(error);
        }
      }
    );
  }

  server.registerTool(
    "run_status",
    {
      title: "Run Status",
      description:
        "Get the current status of a Coalesce run by run counter.\n\nTerminal statuses: completed, failed, canceled. Non-terminal: waitingToRun, running.\n\nArgs:\n  - runCounter (number, required): The numeric run counter\n\nReturns:\n  { runCounter, runStatus, message }",
      inputSchema: z.object({
        runCounter: z.number().describe("The run counter number"),
      }),
      outputSchema: getToolOutputSchema("run_status"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await runStatus(client, params);
        return buildJsonToolResponse("run_status", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "retry_run",
    {
      title: "Retry Run",
      description:
        "Retry a failed Coalesce run. Requires Snowflake Key Pair auth via environment variables.\n\nArgs:\n  - runDetails.runID (string, required): The run ID to retry\n  - runDetails.forceIgnoreWorkspaceStatus (boolean, optional): Force retry even if deploy failed\n  - parameters (object, optional): Runtime parameters\n\nReturns:\n  { runCounter, runStatus, message }\n\nPrefer retry_and_wait when you need the final outcome in a single call.",
      inputSchema: RerunParams,
      outputSchema: getToolOutputSchema("retry_run"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await retryRun(client, params);
        return buildJsonToolResponse("retry_run", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "diagnose_run_failure",
    {
      title: "Diagnose Run Failure",
      description:
        "Diagnose a failed Coalesce run. Fetches run metadata and per-node results, classifies each failure " +
        "(SQL error, missing object, permission issue, data type mismatch, timeout, configuration error), " +
        "and returns actionable fix suggestions.\n\n" +
        "Use this when a run has failed and the user wants to understand what went wrong and how to fix it. " +
        "Works best with completed (failed) runs — for in-progress runs, use run_status instead.\n\n" +
        "Returns: run summary, per-node failure diagnosis with error classification, and prioritized recommendations.",
      inputSchema: z.object({
        runID: z.string().describe(
          "The numeric run ID (integer, e.g. '401'). Use the runCounter value from start_run or run_status responses — not the UUID from run URLs."
        ),
      }),
      outputSchema: getToolOutputSchema("diagnose_run_failure"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await diagnoseRunFailure(client, params);
        return buildJsonToolResponse("diagnose_run_failure", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "cancel_run",
    {
      title: "Cancel Run",
      description:
        "Cancel an in-progress Coalesce run. Destructive — the run will be terminated immediately. Canceling a running pipeline mid-execution can leave data in an inconsistent state (partial loads, half-transformed tables). There is no 'undo cancel'.\n\nArgs:\n  - runID (string, required): Numeric run ID to cancel\n  - environmentID (string, required): Environment the run belongs to\n  - orgID (string, optional): Organization ID. Falls back to COALESCE_ORG_ID env var.\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms cancellation\n\nReturns:\n  Confirmation with updated run status.",
      inputSchema: z.object({
        runID: z.string().describe("The numeric run ID (integer) of the run to cancel"),
        orgID: z
          .string()
          .optional()
          .describe("The organization ID. Optional if COALESCE_ORG_ID is set."),
        environmentID: z.string().describe("The environment ID the run belongs to"),
        confirmed: z
          .boolean()
          .optional()
          .describe("Set to true after the user explicitly confirms the cancellation."),
      }),
      outputSchema: getToolOutputSchema("cancel_run"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const approvalResponse = await requireDestructiveConfirmation(
          server,
          "cancel_run",
          `This will cancel run "${params.runID}" in environment "${params.environmentID}". The run will be terminated immediately and cannot be resumed — data may be left in an inconsistent state.`,
          params.confirmed,
        );
        if (approvalResponse) return approvalResponse;

        const result = await cancelRun(client, params);
        return buildJsonToolResponse("cancel_run", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
