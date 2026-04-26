import { rmSync, existsSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  DESTRUCTIVE_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  type ToolDefinition,
} from "../coalesce/types.js";
import { requireDestructiveConfirmation } from "../services/shared/elicitation.js";
import { getCacheDir, CACHE_DIR_NAME } from "../cache-dir.js";
import {
  cacheEnvironmentNodes,
  cacheOrgUsers,
  cacheRuns,
  cacheWorkspaceNodes,
} from "../services/cache/snapshots.js";
import { defineSimpleTool } from "./tool-helpers.js";
import { DOCUMENTED_RUN_STATUSES } from "../constants.js";

const SnapshotPaginationShape = {
  pageSize: z
    .number()
    .int()
    .positive()
    .max(500)
    .optional()
    .describe("Optional API page size used while collecting the full snapshot. Defaults to 250."),
  orderBy: z
    .string()
    .optional()
    .describe("Optional sort field used for paginated collection. Defaults to id."),
  orderByDirection: z
    .enum(["asc", "desc"])
    .optional()
    .describe("Optional sort direction used for paginated collection."),
} as const;

export function defineCacheTools(
  server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  defineSimpleTool(client, "cache_workspace_nodes", {
    title: "Cache Workspace Nodes",
    description:
      "Fetch and cache all workspace nodes to disk for efficient repeated access. Useful when working with large workspaces where inline responses exceed the auto-cache threshold.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - detail (boolean, optional): Fetch expanded node details. Defaults to true\n  - pageSize (number, optional): API page size for collection. Defaults to 250, max 500\n  - orderBy (string, optional): Sort field for paginated collection. Defaults to id\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n\nReturns:\n  Cache metadata with resourceUri (coalesce://cache/...) for accessing the cached data via MCP resource read.",
    inputSchema: z.object({
      workspaceID: z.string().min(1, "workspaceID must not be empty").describe("The workspace ID"),
      detail: z
        .boolean()
        .optional()
        .describe("When true, fetches expanded node details. Defaults to true."),
      ...SnapshotPaginationShape,
    }),
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
  }, cacheWorkspaceNodes),

  defineSimpleTool(client, "cache_environment_nodes", {
    title: "Cache Environment Nodes",
    description:
      "Fetch and cache all environment nodes to disk for efficient repeated access.\n\nArgs:\n  - environmentID (string, required): The environment ID\n  - detail (boolean, optional): Fetch expanded node details. Defaults to true\n  - pageSize (number, optional): API page size for collection. Defaults to 250, max 500\n  - orderBy (string, optional): Sort field for paginated collection. Defaults to id\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n\nReturns:\n  Cache metadata with resourceUri (coalesce://cache/...) for accessing the cached data via MCP resource read.",
    inputSchema: z.object({
      environmentID: z.string().min(1, "environmentID must not be empty").describe("The environment ID"),
      detail: z
        .boolean()
        .optional()
        .describe("When true, fetches expanded node details. Defaults to true."),
      ...SnapshotPaginationShape,
    }),
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
  }, cacheEnvironmentNodes),

  defineSimpleTool(client, "cache_runs", {
    title: "Cache Runs",
    description:
      "Fetch and cache runs to disk with optional filters for efficient repeated access.\n\nArgs:\n  - runType (enum, optional): Filter by 'deploy' or 'refresh'\n  - runStatus (enum, optional): Filter by 'completed' | 'failed' | 'canceled' | 'running' | 'waitingToRun'\n  - environmentID (string, optional): Filter by environment ID\n  - detail (boolean, optional): Fetch expanded run details. Defaults to false\n  - pageSize (number, optional): API page size for collection. Defaults to 250, max 500\n  - orderBy (string, optional): Sort field for paginated collection. Defaults to id\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n\nReturns:\n  Cache metadata with resourceUri (coalesce://cache/...) for accessing the cached data via MCP resource read.",
    inputSchema: z.object({
      runType: z.enum(["deploy", "refresh"]).optional().describe("Optional run type filter"),
      runStatus: z
        .enum(DOCUMENTED_RUN_STATUSES)
        .optional()
        .describe("Optional run status filter"),
      environmentID: z.string().optional().describe("Optional environment ID filter"),
      detail: z
        .boolean()
        .optional()
        .describe("When true, fetches expanded run details. Defaults to false."),
      ...SnapshotPaginationShape,
    }),
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
  }, cacheRuns),

  defineSimpleTool(client, "cache_org_users", {
    title: "Cache Org Users",
    description:
      "Fetch and cache all organization users to disk for efficient repeated access.\n\nArgs:\n  - pageSize (number, optional): API page size for collection. Defaults to 250, max 500\n  - orderBy (string, optional): Sort field for paginated collection. Defaults to id\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n\nReturns:\n  Cache metadata with resourceUri (coalesce://cache/...) for accessing the cached data via MCP resource read.",
    inputSchema: z.object({
      ...SnapshotPaginationShape,
    }),
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
  }, cacheOrgUsers),

  // clear_data_cache has complex inline logic (file counting + rmSync)
  // that doesn't fit the simple helper pattern.
  // NOTE: The inline handler requires `server` for elicitation — do NOT rename to `_server`.
  ["clear_data_cache",
    {
      title: "Clear Data Cache",
      description:
        "Clear the MCP server's local data cache. Removes all cached artifacts from disk.\n\nArgs:\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms cache clearing\n\nReturns:\n  { deleted: boolean, fileCount: number, totalBytes: number, message: string }",
      inputSchema: z.object({
        confirmed: z
          .boolean()
          .optional()
          .describe("Set to true after the user explicitly confirms the cache clearing."),
      }),
      outputSchema: getToolOutputSchema("clear_data_cache"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const approvalResponse = await requireDestructiveConfirmation(
          server,
          "clear_data_cache",
          "This will delete all cached data from disk. Cached snapshots will need to be re-fetched.",
          params.confirmed,
        );
        if (approvalResponse) return approvalResponse;

        const cacheDir = getCacheDir();
        if (!existsSync(cacheDir)) {
          return buildJsonToolResponse("clear_data_cache", {
            deleted: false,
            message: `No cache directory found at ${CACHE_DIR_NAME}/`,
          });
        }

        // Count files and measure size before deleting
        let fileCount = 0;
        let totalBytes = 0;
        let countErrors = 0;
        function countFiles(dir: string): void {
          try {
            for (const entry of readdirSync(dir, { withFileTypes: true })) {
              const entryPath = join(dir, entry.name);
              if (entry.isDirectory()) {
                countFiles(entryPath);
              } else if (entry.isFile()) {
                fileCount++;
                try {
                  totalBytes += statSync(entryPath).size;
                } catch {
                  countErrors++;
                }
              }
            }
          } catch {
            countErrors++;
          }
        }
        countFiles(cacheDir);

        rmSync(cacheDir, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });

        const sizeMB = (totalBytes / (1024 * 1024)).toFixed(2);
        return buildJsonToolResponse("clear_data_cache", {
          deleted: true,
          fileCount,
          totalBytes,
          sizeMB: `${sizeMB} MB`,
          message: `Deleted ${fileCount} files (${sizeMB} MB) from ${CACHE_DIR_NAME}/`,
          ...(countErrors > 0 ? { countWarning: `${countErrors} file(s)/dir(s) could not be measured — reported totals may be approximate` } : {}),
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  ] as ToolDefinition,
  ];
}
