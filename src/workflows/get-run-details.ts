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
} from "../coalesce/types.js";

type ResultsError = { message: string; status?: number; detail?: unknown };

export async function getRunDetails(
  client: CoalesceClient,
  params: { runID: string }
): Promise<{ run: unknown; results: unknown; resultsError?: ResultsError }> {
  const validRunID = validatePathSegment(params.runID, "runID");

  let run: unknown;
  let results: unknown = null;
  let resultsError: ResultsError | undefined;

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

function serializeResultsError(error: unknown): ResultsError {
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
  server.tool(
    "get-run-details",
    "Get run metadata and results in a single call",
    { runID: z.string().describe("The numeric run ID (integer, e.g. '401'). Use the runCounter value from start-run or run-status responses — not the UUID from run URLs.") },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getRunDetails(client, params);
        return buildJsonToolResponse("get-run-details", sanitizeResponse(result));
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
