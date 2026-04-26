import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { listWorkspaces, getWorkspace } from "../coalesce/api/workspaces.js";
import { READ_ONLY_ANNOTATIONS, type ToolDefinition } from "../coalesce/types.js";
import { defineSimpleTool } from "./tool-helpers.js";

export function defineWorkspaceTools(
  _server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  defineSimpleTool(client, "list_workspaces", {
    title: "List Workspaces",
    description:
      "List all Coalesce workspaces.\n\nReturns workspace IDs needed by node, job, and subgraph tools. Each workspace includes its projectID.\n\nReturns:\n  { data: Workspace[] }",
    inputSchema: z.object({}),
    annotations: READ_ONLY_ANNOTATIONS,
  }, listWorkspaces),

  defineSimpleTool(client, "get_workspace", {
    title: "Get Workspace",
    description:
      "Get details of a specific Coalesce workspace by ID.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n\nReturns:\n  Full workspace object with ID, name, project association, and settings.",
    inputSchema: z.object({
      workspaceID: z.string().min(1, "workspaceID must not be empty").describe("The workspace ID"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, getWorkspace),
  ];
}
