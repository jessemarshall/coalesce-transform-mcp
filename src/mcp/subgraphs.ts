import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listWorkspaceSubgraphs,
  getWorkspaceSubgraph,
  createWorkspaceSubgraph,
  updateWorkspaceSubgraph,
  deleteWorkspaceSubgraph,
} from "../coalesce/api/subgraphs.js";
import {
  PaginationParams,
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";
import { requireDestructiveConfirmation } from "../services/shared/elicitation.js";

export function registerSubgraphTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "list_workspace_subgraphs",
    {
      title: "List Workspace Subgraphs",
      description:
        "List all subgraphs in a Coalesce workspace. Use this to discover subgraph IDs.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - limit, startingFrom, orderBy, orderByDirection: Pagination\n\nReturns:\n  { data: Subgraph[], next?: string, total?: number }",
      inputSchema: PaginationParams.extend({
        workspaceID: z.string().describe("The workspace ID"),
      }),
      outputSchema: getToolOutputSchema("list_workspace_subgraphs"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listWorkspaceSubgraphs(client, params);
        return buildJsonToolResponse("list_workspace_subgraphs", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_workspace_subgraph",
    {
      title: "Get Workspace Subgraph",
      description:
        "Get details of a specific subgraph.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - subgraphID (string, required): The subgraph ID\n\nReturns:\n  Subgraph object with name and node steps.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        subgraphID: z.string().describe("The subgraph ID"),
      }),
      outputSchema: getToolOutputSchema("get_workspace_subgraph"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getWorkspaceSubgraph(client, params);
        return buildJsonToolResponse("get_workspace_subgraph", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "create_workspace_subgraph",
    {
      title: "Create Workspace Subgraph",
      description:
        "Create a subgraph in a Coalesce workspace. Subgraphs group nodes visually.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - name (string, required): Subgraph name\n  - steps (string[], required): Array of node IDs to include\n\nReturns:\n  Created subgraph with assigned ID.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        name: z.string().describe("Name for the subgraph"),
        steps: z
          .array(z.string())
          .describe("Array of node IDs to include in the subgraph"),
      }),
      outputSchema: getToolOutputSchema("create_workspace_subgraph"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await createWorkspaceSubgraph(client, params);
        return buildJsonToolResponse("create_workspace_subgraph", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "update_workspace_subgraph",
    {
      title: "Update Workspace Subgraph",
      description:
        "Update a subgraph's name and member nodes. Replaces the entire steps array.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - subgraphID (string, required): The subgraph ID\n  - name (string, required): Updated name\n  - steps (string[], required): Updated node IDs\n\nReturns:\n  Updated subgraph object.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        subgraphID: z.string().describe("The subgraph ID to update"),
        name: z.string().describe("Updated name for the subgraph"),
        steps: z
          .array(z.string())
          .describe("Updated array of node IDs to include in the subgraph"),
      }),
      outputSchema: getToolOutputSchema("update_workspace_subgraph"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await updateWorkspaceSubgraph(client, params);
        return buildJsonToolResponse("update_workspace_subgraph", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "delete_workspace_subgraph",
    {
      title: "Delete Workspace Subgraph",
      description:
        "Delete a subgraph from a workspace. Destructive — the subgraph is removed but its member nodes are NOT deleted.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - subgraphID (string, required): The subgraph ID\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms deletion\n\nReturns:\n  Confirmation message.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        subgraphID: z.string().describe("The subgraph ID to delete"),
        confirmed: z
          .boolean()
          .optional()
          .describe("Set to true after the user explicitly confirms the deletion."),
      }),
      outputSchema: getToolOutputSchema("delete_workspace_subgraph"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const approvalResponse = await requireDestructiveConfirmation(
          server,
          "delete_workspace_subgraph",
          `This will permanently delete subgraph "${params.subgraphID}" from workspace "${params.workspaceID}". Member nodes will NOT be deleted.`,
          params.confirmed,
        );
        if (approvalResponse) return approvalResponse;

        const result = await deleteWorkspaceSubgraph(client, params);
        return buildJsonToolResponse("delete_workspace_subgraph", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
