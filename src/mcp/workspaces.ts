import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { listWorkspaces, getWorkspace } from "../coalesce/api/workspaces.js";
import {
  PaginationParams,
  buildJsonToolResponse,
  handleToolError,
  READ_ONLY_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerWorkspaceTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.tool(
    "list-workspaces",
    "List all Coalesce workspaces. Returns workspace IDs needed for node, job, and subgraph tools. Prefer this over list-projects with includeWorkspaces when you only need workspace-level data.",
    PaginationParams.shape,
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listWorkspaces(client, params);
        return buildJsonToolResponse("list-workspaces", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-workspace",
    "Get details of a specific Coalesce workspace.",
    {
      workspaceID: z.string().describe("The workspace ID"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getWorkspace(client, params);
        return buildJsonToolResponse("get-workspace", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
