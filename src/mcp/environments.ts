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
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool, defineDestructiveTool } from "./tool-helpers.js";

export function defineEnvironmentTools(
  server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  defineSimpleTool(client, "list_environments", {
    title: "List Environments",
    description:
      "List all available Coalesce environments with optional pagination.\n\nReturns environment IDs, names, and configuration. Use this to discover environment IDs needed by run, node, and job tools.\n\nArgs:\n  - limit (number, optional): Max results per page\n  - startingFrom (string, optional): Pagination cursor from previous response\n  - orderBy (string, optional): Sort field (requires startingFrom)\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n  - detail (boolean, optional): Include expanded environment configuration\n\nReturns:\n  { data: Environment[], next?: string, total?: number }\n\nUse get_environment for a single environment by ID.",
    inputSchema: PaginationParams.extend({
      detail: z.boolean().optional().describe("When true, returns expanded environment info"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, listEnvironments),

  defineSimpleTool(client, "get_environment", {
    title: "Get Environment",
    description:
      "Get details of a specific Coalesce environment by ID.\n\nReturns full environment configuration including connection details, runtime parameters, and tag colors.\n\nArgs:\n  - environmentID (string, required): The environment ID\n\nReturns:\n  Full environment object with ID, name, project, connection settings, and configuration.",
    inputSchema: z.object({
      environmentID: z.string().describe("The environment ID"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, getEnvironment),

  defineSimpleTool(client, "create_environment", {
    title: "Create Environment",
    description:
      "Create a new Coalesce environment within a project.\n\nArgs:\n  - projectID (string, required): The project to create the environment in\n  - name (string, required): Name for the new environment\n  - oauthEnabled (boolean, optional): Enable OAuth (default: false)\n  - devEnv (boolean, optional): Mark as development environment (default: false)\n  - connectionAccount (string, optional): Connection account identifier\n  - runTimeParameters (object, optional): Runtime parameters for the environment\n  - tagColors (object, optional): UI tag colors { backgroundColor, textColor }\n\nReturns:\n  Created environment object with assigned ID.",
    inputSchema: z.object({
      projectID: z.string().describe("The project ID to create the environment in"),
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
    }),
    annotations: WRITE_ANNOTATIONS,
  }, (client, params) => {
    const { projectID, ...rest } = params;
    return createEnvironment(client, { project: projectID, oauthEnabled: false, ...rest });
  }),

  defineDestructiveTool(server, client, "delete_environment", {
    title: "Delete Environment",
    description:
      "Permanently delete a Coalesce environment. This is destructive and cannot be undone.\n\nArgs:\n  - environmentID (string, required): The environment ID to delete\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms deletion\n\nReturns:\n  Confirmation message.",
    inputSchema: z.object({
      environmentID: z.string().describe("The environment ID to delete"),
      confirmed: z
        .boolean()
        .optional()
        .describe("Set to true after the user explicitly confirms the deletion."),
    }),
    annotations: DESTRUCTIVE_ANNOTATIONS,
    confirmMessage: (params) => `This will permanently delete environment "${params.environmentID}". This cannot be undone.`,
  }, deleteEnvironment),
  ];
}
