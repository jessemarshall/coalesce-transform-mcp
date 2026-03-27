import { execFileSync } from "node:child_process";
import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  READ_ONLY_ANNOTATIONS,
} from "../coalesce/types.js";
import {
  isCortexAvailable,
  askCortex,
  listConnections,
  searchObjects,
} from "../services/cortex/executor.js";
import {
  parseTableReference,
  extractTableReference,
  searchCoalesceForTable,
} from "../services/exploration/search.js";

/**
 * Checks if the cortex CLI binary exists in PATH at registration time.
 * Uses `cortex --version` (cross-platform) instead of `which` (unix-only).
 */
export function isCortexInstalled(): boolean {
  try {
    execFileSync("cortex", ["--version"], { timeout: 5_000, stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

const CORTEX_INSTALL_HINT =
  "Install Cortex Code to enable Snowflake exploration: curl -LsS https://ai.snowflake.com/static/cc-scripts/install.sh | sh";

export function registerExplorationTools(
  server: McpServer,
  client: CoalesceClient
): void {
  if (!isCortexInstalled()) {
    console.error(
      "[coalesce-transform-mcp] Cortex Code CLI not found — Snowflake exploration tools will not be registered. " +
        CORTEX_INSTALL_HINT
    );
    return;
  }

  server.registerTool(
    "explore_data_source",
    {
      title: "Explore Data Source",
      description:
        "Find information about a table, column, or data source. Searches Coalesce workspaces first — if not found, queries Snowflake directly via Cortex Code CLI.\n\nUse this when questions arise about data or tables that may or may not be managed in Coalesce. The tool automatically falls back to Snowflake exploration when the object isn't in Coalesce.\n\nArgs:\n  - question (string, required): Question about a table, column, schema, or data — e.g. 'what columns does RAW.CUSTOMERS have?', 'show me sample data from ANALYTICS.REVENUE', 'what tables exist in the RAW database'\n  - workspaceID (string, optional): Coalesce workspace to search. Omit to search all accessible workspaces\n  - connection (string, optional): Snowflake connection name for Cortex Code (e.g. 'dev', 'prod'). Uses active connection if omitted\n\nReturns:\n  { source, coalesceResult?, snowflakeResult?, cortexAvailable, installHint? }",
      inputSchema: z.object({
        question: z
          .string()
          .describe(
            "Question about a table, column, schema, or data — e.g. 'what columns does RAW.CUSTOMERS have?', 'show me sample data from ANALYTICS.REVENUE', 'what tables exist in the RAW database'"
          ),
        workspaceID: z
          .string()
          .optional()
          .describe(
            "Coalesce workspace to search. Omit to search all accessible workspaces."
          ),
        connection: z
          .string()
          .optional()
          .describe(
            "Snowflake connection name for Cortex Code (e.g. 'dev', 'prod'). Uses active connection if omitted."
          ),
      }),
      outputSchema: getToolOutputSchema("explore_data_source"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const { question, workspaceID, connection } = params;

        // Try to extract a table reference from the question
        const tableRefStr = extractTableReference(question);
        const tableRef = tableRefStr
          ? parseTableReference(tableRefStr)
          : null;

        // Search Coalesce if we have a table reference
        let coalesceResult = null;
        if (tableRef) {
          coalesceResult = await searchCoalesceForTable(
            client,
            tableRef,
            workspaceID
          );
        }

        // If found in Coalesce, return early
        if (coalesceResult?.found) {
          return buildJsonToolResponse("explore_data_source", {
            source: "coalesce",
            coalesceResult: {
              found: true,
              matches: coalesceResult.matches,
              searchedWorkspaces: coalesceResult.searchedWorkspaces,
              ...(coalesceResult.skippedWorkspaces.length > 0
                ? { skippedWorkspaces: coalesceResult.skippedWorkspaces }
                : {}),
            },
            cortexAvailable: await isCortexAvailable(),
          });
        }

        // Defense-in-depth: cortex was present at registration but may have been
        // uninstalled during server lifetime. The runtime check gracefully degrades.
        const cortexAvailable = await isCortexAvailable();
        if (!cortexAvailable) {
          return buildJsonToolResponse("explore_data_source", {
            source: "not_found",
            coalesceResult: coalesceResult
              ? {
                  found: false,
                  matches: [],
                  searchedWorkspaces: coalesceResult.searchedWorkspaces,
                  ...(coalesceResult.skippedWorkspaces.length > 0
                    ? { skippedWorkspaces: coalesceResult.skippedWorkspaces }
                    : {}),
                }
              : null,
            cortexAvailable: false,
            installHint: CORTEX_INSTALL_HINT,
          });
        }

        // Delegate to Cortex Code
        const cortexResult = await askCortex(question, { connection });

        return buildJsonToolResponse("explore_data_source", {
          source: "snowflake",
          coalesceResult: coalesceResult
            ? {
                found: false,
                matches: [],
                searchedWorkspaces: coalesceResult.searchedWorkspaces,
              }
            : null,
          snowflakeResult: {
            answer: cortexResult.answer,
            connection: connection ?? "active",
          },
          cortexAvailable: true,
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "query_snowflake",
    {
      title: "Query Snowflake",
      description:
        "Ask any question about Snowflake — data, schemas, permissions, performance, Cortex AI, etc. Delegates directly to Cortex Code CLI, which handles SQL generation and execution.\n\nSkips Coalesce search — use explore_data_source if you want to check Coalesce first.\n\nRequires Cortex Code CLI to be installed (https://ai.snowflake.com).\n\nArgs:\n  - question (string, required): Any question about Snowflake data, schemas, permissions, etc.\n  - connection (string, optional): Snowflake connection name for Cortex Code\n\nReturns:\n  { answer, connection, cortexAvailable }",
      inputSchema: z.object({
        question: z
          .string()
          .describe(
            "Any question about Snowflake — data, schemas, permissions, performance, Cortex AI, etc."
          ),
        connection: z
          .string()
          .optional()
          .describe(
            "Snowflake connection name for Cortex Code (e.g. 'dev', 'prod'). Uses active connection if omitted."
          ),
      }),
      outputSchema: getToolOutputSchema("query_snowflake"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const { question, connection } = params;

        const cortexAvailable = await isCortexAvailable();
        if (!cortexAvailable) {
          return buildJsonToolResponse("query_snowflake", {
            cortexAvailable: false,
            installHint: CORTEX_INSTALL_HINT,
          });
        }

        const result = await askCortex(question, { connection });

        return buildJsonToolResponse("query_snowflake", {
          answer: result.answer,
          connection: connection ?? "active",
          cortexAvailable: true,
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "search_snowflake_objects",
    {
      title: "Search Snowflake Objects",
      description:
        "Search for tables, views, schemas, databases, and other objects in Snowflake. Uses Cortex Code's semantic object search.\n\nArgs:\n  - query (string, required): Search query — e.g. 'customer tables', 'revenue views', 'RAW database'\n  - connection (string, optional): Snowflake connection name\n  - maxResults (number, optional): Max results (default 10)\n  - types (string, optional): Comma-separated object types to filter (e.g. 'TABLE,VIEW')\n\nReturns:\n  { results, connection, cortexAvailable }",
      inputSchema: z.object({
        query: z.string().describe("Search query for Snowflake objects"),
        connection: z
          .string()
          .optional()
          .describe("Snowflake connection name"),
        maxResults: z
          .number()
          .optional()
          .default(10)
          .describe("Max results to return (default 10)"),
        types: z
          .string()
          .optional()
          .describe(
            "Comma-separated object types to filter (e.g. 'TABLE,VIEW')"
          ),
      }),
      outputSchema: getToolOutputSchema("search_snowflake_objects"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const { query, connection, maxResults, types } = params;

        const cortexAvailable = await isCortexAvailable();
        if (!cortexAvailable) {
          return buildJsonToolResponse("search_snowflake_objects", {
            cortexAvailable: false,
            installHint: CORTEX_INSTALL_HINT,
          });
        }

        const result = await searchObjects(query, {
          connection,
          maxResults,
          types,
        });

        return buildJsonToolResponse("search_snowflake_objects", {
          results: result.results,
          connection: connection ?? "active",
          cortexAvailable: true,
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "list_snowflake_connections",
    {
      title: "List Snowflake Connections",
      description:
        "List available Snowflake connections configured in Cortex Code.\n\nShows all connections with their account, user, and role. Indicates which connection is currently active.\n\nRequires Cortex Code CLI to be installed.\n\nReturns:\n  { activeConnection, connections, cortexAvailable }",
      inputSchema: z.object({}),
      outputSchema: getToolOutputSchema("list_snowflake_connections"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async () => {
      try {
        const cortexAvailable = await isCortexAvailable();
        if (!cortexAvailable) {
          return buildJsonToolResponse("list_snowflake_connections", {
            cortexAvailable: false,
            installHint: CORTEX_INSTALL_HINT,
          });
        }

        const connections = await listConnections();

        return buildJsonToolResponse("list_snowflake_connections", {
          ...connections,
          cortexAvailable: true,
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
