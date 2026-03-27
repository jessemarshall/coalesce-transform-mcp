import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { listWorkspaces } from "../coalesce/api/workspaces.js";
import {
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
    "List all workspaces across projects, returning workspace IDs needed by workspace-node, subgraph, and job tools. Optionally filter by projectID.",
    {
      projectID: z
        .string()
        .optional()
        .describe("Optional project ID to filter workspaces to a single project"),
    },
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
}
