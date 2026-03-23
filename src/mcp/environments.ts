import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listEnvironments,
  getEnvironment,
  createEnvironment,
  deleteEnvironment,
} from "../coalesce/api/environments.js";
import {
  PaginationParams,
  buildJsonToolResponse,
  handleToolError,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerEnvironmentTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.tool(
    "list-environments",
    "List all available Coalesce environments",
    PaginationParams.extend({
      detail: z.boolean().optional().describe("When true, returns expanded environment info"),
    }).shape,
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listEnvironments(client, params);
        return buildJsonToolResponse("list-environments", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-environment",
    "Get details of a specific Coalesce environment",
    {
      environmentID: z.string().describe("The environment ID"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getEnvironment(client, params);
        return buildJsonToolResponse("get-environment", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "create-environment",
    "Create a new Coalesce environment within a project.",
    {
      project: z.string().describe("The project ID to create the environment in"),
      name: z.string().describe("Name for the new environment"),
      oauthEnabled: z.boolean().optional().describe("Whether OAuth is enabled. Defaults to false."),
      devEnv: z.boolean().optional().describe("Defaults to false."),
      connectionAccount: z.string().optional().describe("Optional connection account identifier"),
      runTimeParameters: z.record(z.unknown()).optional().describe("Optional runtime parameters"),
      tagColors: z
        .object({
          backgroundColor: z.string().optional(),
          textColor: z.string().optional(),
        })
        .optional()
        .describe("Optional tag colors for the environment"),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await createEnvironment(client, params);
        return buildJsonToolResponse("create-environment", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "delete-environment",
    "Delete a Coalesce environment. This is a destructive operation — the environment and its configuration will be permanently removed.",
    {
      environmentID: z.string().describe("The environment ID to delete"),
    },
    DESTRUCTIVE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await deleteEnvironment(client, params);
        return buildJsonToolResponse("delete-environment", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
