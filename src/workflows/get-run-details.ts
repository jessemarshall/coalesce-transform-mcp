import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  buildJsonToolResponse,
  sanitizeResponse,
  validatePathSegment,
  handleToolError,
} from "../coalesce/types.js";

export async function getRunDetails(
  client: CoalesceClient,
  params: { runID: string }
): Promise<{ run: unknown; results: unknown; resultsError?: string }> {
  const validRunID = validatePathSegment(params.runID, "runID");

  let run: unknown;
  let results: unknown = null;
  let resultsError: string | undefined;

  const runPromise = client.get(`/api/v1/runs/${validRunID}`);
  const resultsPromise = client.get(`/api/v1/runs/${validRunID}/results`);

  [run] = await Promise.all([
    runPromise,
    resultsPromise.then(
      (data) => { results = data; },
      (error) => { resultsError = error instanceof Error ? error.message : String(error); }
    ),
  ]);

  return resultsError !== undefined
    ? { run, results: null, resultsError }
    : { run, results };
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
