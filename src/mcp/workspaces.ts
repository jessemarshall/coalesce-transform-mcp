import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { listWorkspaces, getWorkspace } from "../coalesce/api/workspaces.js";
import {
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
    "list_workspaces",
    {
      title: "List Workspaces",
      description:
        "List all Coalesce workspaces.\n\nReturns workspace IDs needed by node, job, and subgraph tools. Each workspace includes its projectID.\n\nReturns:\n  { data: Workspace[] }",
      inputSchema: z.object({}),
      outputSchema: getToolOutputSchema("list_workspaces"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async () => {
      try {
        const result = await listWorkspaces(client);
        return buildJsonToolResponse("list_workspaces", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_workspace",
    {
      title: "Get Workspace",
      description:
        "Get details of a specific Coalesce workspace by ID.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n\nReturns:\n  Full workspace object with ID, name, project association, and settings.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
      }),
      outputSchema: getToolOutputSchema("get_workspace"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getWorkspace(client, params);
        return buildJsonToolResponse("get_workspace", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
