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
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerSubgraphTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.tool(
    "list-workspace-subgraphs",
    "List all subgraphs in a Coalesce workspace. Use this to discover subgraph IDs for get, update, or delete operations.",
    PaginationParams.extend({
      workspaceID: z.string().describe("The workspace ID"),
    }).shape,
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listWorkspaceSubgraphs(client, params);
        return buildJsonToolResponse("list-workspace-subgraphs", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-workspace-subgraph",
    "Get details of a specific subgraph in a Coalesce workspace.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      subgraphID: z.string().describe("The subgraph ID"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getWorkspaceSubgraph(client, params);
        return buildJsonToolResponse("get-workspace-subgraph", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "create-workspace-subgraph",
    "Create a subgraph in a Coalesce workspace. A subgraph groups nodes together visually. The steps array contains node IDs to include in the subgraph.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      name: z.string().describe("Name for the subgraph"),
      steps: z
        .array(z.string())
        .describe("Array of node IDs to include in the subgraph"),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await createWorkspaceSubgraph(client, params);
        return buildJsonToolResponse("create-workspace-subgraph", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "update-workspace-subgraph",
    "Update a subgraph in a Coalesce workspace. Replaces the subgraph's name and steps (node IDs).",
    {
      workspaceID: z.string().describe("The workspace ID"),
      subgraphID: z.string().describe("The subgraph ID to update"),
      name: z.string().describe("Updated name for the subgraph"),
      steps: z
        .array(z.string())
        .describe("Updated array of node IDs to include in the subgraph"),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await updateWorkspaceSubgraph(client, params);
        return buildJsonToolResponse("update-workspace-subgraph", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "delete-workspace-subgraph",
    "Delete a subgraph from a Coalesce workspace. This is a destructive operation — the subgraph will be permanently removed. The nodes within it are NOT deleted.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      subgraphID: z.string().describe("The subgraph ID to delete"),
    },
    DESTRUCTIVE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await deleteWorkspaceSubgraph(client, params);
        return buildJsonToolResponse("delete-workspace-subgraph", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
