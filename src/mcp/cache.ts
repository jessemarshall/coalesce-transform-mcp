import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  IDEMPOTENT_WRITE_ANNOTATIONS,
  buildJsonToolResponse,
  handleToolError,
} from "../coalesce/types.js";
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
  server.tool(
    "cache-workspace-nodes",
    "Fetch every page of workspace nodes from the Coalesce API, save the collected snapshot to data/nodes, and return only cache metadata. Use this when the full node list would be too large for chat context or will be reused across multiple steps.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      detail: z
        .boolean()
        .optional()
        .describe("When true, fetches expanded node details. Defaults to true."),
      ...SnapshotPaginationShape,
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await cacheWorkspaceNodes(client, params);
        return buildJsonToolResponse("cache-workspace-nodes", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "cache-environment-nodes",
    "Fetch every page of environment nodes from the Coalesce API, save the collected snapshot to data/nodes, and return only cache metadata. Use this when the full node list would be too large for chat context or will be reused across multiple steps.",
    {
      environmentID: z.string().describe("The environment ID"),
      detail: z
        .boolean()
        .optional()
        .describe("When true, fetches expanded node details. Defaults to true."),
      ...SnapshotPaginationShape,
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await cacheEnvironmentNodes(client, params);
        return buildJsonToolResponse("cache-environment-nodes", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "cache-runs",
    "Fetch every page of list-runs results from the Coalesce API, save the collected snapshot to data/runs, and return only cache metadata. Use this when the run list would be too large for chat context or should be preserved outside the conversation.",
    {
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
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await cacheRuns(client, params);
        return buildJsonToolResponse("cache-runs", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "cache-org-users",
    "Fetch every page of organization users from the Coalesce API, save the collected snapshot to data/users, and return only cache metadata. Use this when the user list would be too large for chat context or should be preserved outside the conversation.",
    SnapshotPaginationShape,
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await cacheOrgUsers(client, params);
        return buildJsonToolResponse("cache-org-users", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
