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
} from "../coalesce/types.js";
import { getCacheDir, CACHE_DIR_NAME } from "../cache-dir.js";
import {
  cacheEnvironmentNodes,
  cacheOrgUsers,
  cacheRuns,
  cacheWorkspaceNodes,
} from "../services/cache/snapshots.js";

const SnapshotPaginationShape = {
  pageSize: z
    .number()
    .int()
    .positive()
    .max(1000)
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

export function registerCacheTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "coalesce_cache_workspace_nodes",
    {
      title: "Cache Workspace Nodes",
      description:
        "Fetch and cache all workspace nodes to disk for efficient repeated access. Useful when working with large workspaces where inline responses exceed the auto-cache threshold.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - detail (boolean, optional): Fetch expanded node details. Defaults to true\n  - pageSize (number, optional): API page size for collection. Defaults to 250, max 1000\n  - orderBy (string, optional): Sort field for paginated collection. Defaults to id\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n\nReturns:\n  Cache metadata with resourceUri (coalesce://cache/...) for accessing the cached data via MCP resource read.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        detail: z
          .boolean()
          .optional()
          .describe("When true, fetches expanded node details. Defaults to true."),
        ...SnapshotPaginationShape,
      }),
      outputSchema: getToolOutputSchema("coalesce_cache_workspace_nodes"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await cacheWorkspaceNodes(client, params);
        return buildJsonToolResponse("coalesce_cache_workspace_nodes", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_cache_environment_nodes",
    {
      title: "Cache Environment Nodes",
      description:
        "Fetch and cache all environment nodes to disk for efficient repeated access.\n\nArgs:\n  - environmentID (string, required): The environment ID\n  - detail (boolean, optional): Fetch expanded node details. Defaults to true\n  - pageSize (number, optional): API page size for collection. Defaults to 250, max 1000\n  - orderBy (string, optional): Sort field for paginated collection. Defaults to id\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n\nReturns:\n  Cache metadata with resourceUri (coalesce://cache/...) for accessing the cached data via MCP resource read.",
      inputSchema: z.object({
        environmentID: z.string().describe("The environment ID"),
        detail: z
          .boolean()
          .optional()
          .describe("When true, fetches expanded node details. Defaults to true."),
        ...SnapshotPaginationShape,
      }),
      outputSchema: getToolOutputSchema("coalesce_cache_environment_nodes"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await cacheEnvironmentNodes(client, params);
        return buildJsonToolResponse("coalesce_cache_environment_nodes", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_cache_runs",
    {
      title: "Cache Runs",
      description:
        "Fetch and cache runs to disk with optional filters for efficient repeated access.\n\nArgs:\n  - runType (enum, optional): Filter by 'deploy' or 'refresh'\n  - runStatus (enum, optional): Filter by 'completed' | 'failed' | 'canceled' | 'running' | 'waitingToRun'\n  - environmentID (string, optional): Filter by environment ID\n  - detail (boolean, optional): Fetch expanded run details. Defaults to false\n  - pageSize (number, optional): API page size for collection. Defaults to 250, max 1000\n  - orderBy (string, optional): Sort field for paginated collection. Defaults to id\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n\nReturns:\n  Cache metadata with resourceUri (coalesce://cache/...) for accessing the cached data via MCP resource read.",
      inputSchema: z.object({
        runType: z.enum(["deploy", "refresh"]).optional().describe("Optional run type filter"),
        runStatus: z
          .enum(["completed", "failed", "canceled", "running", "waitingToRun"])
          .optional()
          .describe("Optional run status filter"),
        environmentID: z.string().optional().describe("Optional environment ID filter"),
        detail: z
          .boolean()
          .optional()
          .describe("When true, fetches expanded run details. Defaults to false."),
        ...SnapshotPaginationShape,
      }),
      outputSchema: getToolOutputSchema("coalesce_cache_runs"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await cacheRuns(client, params);
        return buildJsonToolResponse("coalesce_cache_runs", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_cache_org_users",
    {
      title: "Cache Org Users",
      description:
        "Fetch and cache all organization users to disk for efficient repeated access.\n\nArgs:\n  - pageSize (number, optional): API page size for collection. Defaults to 250, max 1000\n  - orderBy (string, optional): Sort field for paginated collection. Defaults to id\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n\nReturns:\n  Cache metadata with resourceUri (coalesce://cache/...) for accessing the cached data via MCP resource read.",
      inputSchema: z.object({
        ...SnapshotPaginationShape,
      }),
      outputSchema: getToolOutputSchema("coalesce_cache_org_users"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await cacheOrgUsers(client, params);
        return buildJsonToolResponse("coalesce_cache_org_users", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_clear_data_cache",
    {
      title: "Clear Data Cache",
      description:
        "Clear the MCP server's local data cache. Removes all cached artifacts from disk.\n\nReturns:\n  { deleted: boolean, fileCount: number, totalBytes: number, message: string }",
      inputSchema: z.object({}),
      outputSchema: getToolOutputSchema("coalesce_clear_data_cache"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async () => {
      try {
        const cacheDir = getCacheDir();
        if (!existsSync(cacheDir)) {
          return buildJsonToolResponse("coalesce_clear_data_cache", {
            deleted: false,
            message: `No cache directory found at ${CACHE_DIR_NAME}/`,
          });
        }

        // Count files and measure size before deleting
        let fileCount = 0;
        let totalBytes = 0;
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
                  // skip unreadable files
                }
              }
            }
          } catch {
            // skip unreadable dirs
          }
        }
        countFiles(cacheDir);

        rmSync(cacheDir, { recursive: true, force: true });

        const sizeMB = (totalBytes / (1024 * 1024)).toFixed(2);
        return buildJsonToolResponse("coalesce_clear_data_cache", {
          deleted: true,
          fileCount,
          totalBytes,
          sizeMB: `${sizeMB} MB`,
          message: `Deleted ${fileCount} files (${sizeMB} MB) from ${CACHE_DIR_NAME}/`,
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
