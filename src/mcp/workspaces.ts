import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { listWorkspaces, getWorkspace } from "../coalesce/api/workspaces.js";
import {
  PaginationParams,
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  READ_ONLY_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerWorkspaceTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "coalesce_list_workspaces",
    {
      title: "List Workspaces",
      description:
        "List all Coalesce workspaces with optional pagination.\n\nReturns workspace IDs needed by node, job, and subgraph tools. Prefer this over coalesce_list_projects with includeWorkspaces when you only need workspace-level data.\n\nArgs:\n  - limit (number, optional): Max results per page\n  - startingFrom (string, optional): Pagination cursor\n  - orderBy (string, optional): Sort field\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n\nReturns:\n  { data: Workspace[], next?: string, total?: number }",
      inputSchema: PaginationParams,
      outputSchema: getToolOutputSchema("coalesce_list_workspaces"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listWorkspaces(client, params);
        return buildJsonToolResponse("coalesce_list_workspaces", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_get_workspace",
    {
      title: "Get Workspace",
      description:
        "Get details of a specific Coalesce workspace by ID.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n\nReturns:\n  Full workspace object with ID, name, project association, and settings.",
      inputSchema: {
        workspaceID: z.string().describe("The workspace ID"),
      },
      outputSchema: getToolOutputSchema("coalesce_get_workspace"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getWorkspace(client, params);
        return buildJsonToolResponse("coalesce_get_workspace", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
