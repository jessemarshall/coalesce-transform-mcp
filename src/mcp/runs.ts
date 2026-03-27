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
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerRunTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.tool(
    "list-runs",
    "List all Coalesce runs. Supports filtering by runType, runStatus, and environmentID.",
    PaginationParams.extend({
      runType: z.enum(["deploy", "refresh"]).optional().describe("Filter by run type"),
      runStatus: z.enum(["completed", "failed", "canceled", "running", "waitingToRun"]).optional().describe("Filter by run status"),
      environmentID: z.string().optional().describe("Filter by environment ID"),
      detail: z.boolean().optional().describe("Include full run details in response"),
    }).shape,
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listRuns(client, params);
        return buildJsonToolResponse("list-runs", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-run",
    "Get details of a specific Coalesce run",
    {
      runID: z.string().describe("The numeric run ID (integer, e.g. '401'). Use the runCounter value from start-run or run-status responses — not the UUID from run URLs."),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getRun(client, params);
        return buildJsonToolResponse("get-run", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-run-results",
    "Get the results of a specific Coalesce run",
    {
      runID: z.string().describe("The numeric run ID (integer, e.g. '401'). Use the runCounter value from start-run or run-status responses — not the UUID from run URLs."),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getRunResults(client, params);
        return buildJsonToolResponse("get-run-results", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "start-run",
    "Start a new Coalesce run. Requires a numeric environmentID and optionally a jobID (not job name). " +
    "If the user provides a job name instead of an ID, ask them for the job ID. " +
    "If the user doesn't know the environment ID, use list-environments to look it up by name. " +
    "Requires Snowflake Key Pair auth; credentials are read from environment variables.",
    StartRunParams.shape,
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await startRun(client, params);
        return buildJsonToolResponse("start-run", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "run-status",
    "Get the status of a Coalesce run by run counter",
    {
      runCounter: z.number().describe("The run counter number"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await runStatus(client, params);
        return buildJsonToolResponse("run-status", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "retry-run",
    "Retry a failed Coalesce run. Requires the runID from the original run. " +
    "Requires Snowflake Key Pair auth; credentials are read from environment variables.",
    RerunParams.shape,
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await retryRun(client, params);
        return buildJsonToolResponse("retry-run", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "diagnose-run-failure",
    "Diagnose a failed Coalesce run. Fetches run metadata and per-node results, classifies each failure " +
      "(SQL error, missing object, permission issue, data type mismatch, timeout, configuration error), " +
      "and returns actionable fix suggestions.\n\n" +
      "Use this when a run has failed and the user wants to understand what went wrong and how to fix it. " +
      "Works best with completed (failed) runs — for in-progress runs, use run-status instead.\n\n" +
      "Returns: run summary, per-node failure diagnosis with error classification, and prioritized recommendations.",
    {
      runID: z.string().describe(
        "The numeric run ID (integer, e.g. '401'). Use the runCounter value from start-run or run-status responses — not the UUID from run URLs."
      ),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await diagnoseRunFailure(client, params);
        return buildJsonToolResponse("diagnose-run-failure", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "cancel-run",
    "Cancel an in-progress Coalesce run. Requires the runID and environmentID. " +
    "Provide orgID explicitly, or omit it to use COALESCE_ORG_ID from the environment. " +
    "If the user does not know the orgID, they can find it in Coalesce Support Information.",
    {
      runID: z.string().describe("The numeric run ID (integer) of the run to cancel"),
      orgID: z
        .string()
        .optional()
        .describe("The organization ID. Optional if COALESCE_ORG_ID is set."),
      environmentID: z.string().describe("The environment ID the run belongs to"),
    },
    DESTRUCTIVE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await cancelRun(client, params);
        return buildJsonToolResponse("cancel-run", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
