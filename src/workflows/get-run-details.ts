import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { CoalesceApiError } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  buildJsonToolResponse,
  sanitizeResponse,
  validatePathSegment,
  handleToolError,
  getToolOutputSchema,
  type JsonToolError,
} from "../coalesce/types.js";

export async function getRunDetails(
  client: CoalesceClient,
  params: { runID: string }
): Promise<{ run: unknown; results: unknown; resultsError?: JsonToolError }> {
  const validRunID = validatePathSegment(params.runID, "runID");

  let run: unknown;
  let results: unknown = null;
  let resultsError: JsonToolError | undefined;

  const runPromise = client.get(`/api/v1/runs/${validRunID}`);
  const resultsPromise = client.get(`/api/v1/runs/${validRunID}/results`);

  [run] = await Promise.all([
    runPromise,
    resultsPromise.then(
      (data) => { results = data; },
      (error) => { resultsError = serializeResultsError(error); }
    ),
  ]);

  return resultsError !== undefined
    ? { run, results: null, resultsError }
    : { run, results };
}

function serializeResultsError(error: unknown): JsonToolError {
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

export function registerGetRunDetails(server: McpServer, client: CoalesceClient): void {
  server.registerTool(
    "coalesce_get_run_details",
    {
      title: "Get Run Details",
      description:
        "Get run metadata and execution results in a single call. Combines coalesce_get_run and coalesce_get_run_results.\n\nArgs:\n  - runID (string, required): Numeric run ID (integer). Use runCounter, not the UUID.\n\nReturns:\n  { run: RunObject, results: ResultsObject, resultsError?: ErrorObject }",
      inputSchema: z.object({
        runID: z.string().describe("The numeric run ID (integer, e.g. '401'). Use the runCounter value from coalesce_start_run or coalesce_run_status responses — not the UUID from run URLs."),
      }),
      outputSchema: getToolOutputSchema("coalesce_get_run_details"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getRunDetails(client, params);
        return buildJsonToolResponse("coalesce_get_run_details", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
