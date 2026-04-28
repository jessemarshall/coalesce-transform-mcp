import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  validatePathSegment,
  type JsonToolError,
  type ToolDefinition,
} from "../coalesce/types.js";
import { serializeResultsError } from "./progress.js";
import { defineSimpleTool } from "../mcp/tool-helpers.js";

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

export function defineGetRunDetails(_server: McpServer, client: CoalesceClient): ToolDefinition[] {
  return [
    defineSimpleTool(client, "get_run_details", {
      title: "Get Run Details",
      description:
        "Get run metadata and execution results in a single call. Combines get_run and get_run_results.\n\nArgs:\n  - runID (string, required): Numeric run ID (integer). Use runCounter, not the UUID.\n\nReturns:\n  { run: RunObject, results: ResultsObject, resultsError?: ErrorObject }",
      inputSchema: z.object({
        runID: z.string().min(1, "runID must not be empty").describe("The numeric run ID (integer, e.g. '401'). Use the runCounter value from start_run or run_status responses — not the UUID from run URLs."),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
      sanitize: true,
    }, getRunDetails),
  ];
}
