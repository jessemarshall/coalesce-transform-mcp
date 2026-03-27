import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listEnvironments,
  getEnvironment,
  createEnvironment,
  updateEnvironment,
  deleteEnvironment,
} from "../coalesce/api/environments.js";
import {
  PaginationParams,
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerEnvironmentTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "list_environments",
    {
      title: "List Environments",
      description:
        "List all available Coalesce environments with optional pagination.\n\nReturns environment IDs, names, and configuration. Use this to discover environment IDs needed by run, node, and job tools.\n\nArgs:\n  - limit (number, optional): Max results per page\n  - startingFrom (string, optional): Pagination cursor from previous response\n  - orderBy (string, optional): Sort field (requires startingFrom)\n  - orderByDirection ('asc'|'desc', optional): Sort direction\n  - detail (boolean, optional): Include expanded environment configuration\n\nReturns:\n  { data: Environment[], next?: string, total?: number }\n\nUse get_environment for a single environment by ID.",
      inputSchema: PaginationParams.extend({
        detail: z.boolean().optional().describe("When true, returns expanded environment info"),
      }),
      outputSchema: getToolOutputSchema("list_environments"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listEnvironments(client, params);
        return buildJsonToolResponse("list_environments", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_environment",
    {
      title: "Get Environment",
      description:
        "Get details of a specific Coalesce environment by ID.\n\nReturns full environment configuration including connection details, runtime parameters, and tag colors.\n\nArgs:\n  - environmentID (string, required): The environment ID\n\nReturns:\n  Full environment object with ID, name, project, connection settings, and configuration.",
      inputSchema: z.object({
        environmentID: z.string().describe("The environment ID"),
      }),
      outputSchema: getToolOutputSchema("get_environment"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getEnvironment(client, params);
        return buildJsonToolResponse("get_environment", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "create_environment",
    {
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
      outputSchema: getToolOutputSchema("create_environment"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        // Coalesce API expects `project` in the body
        const { projectID, ...rest } = params;
        const result = await createEnvironment(client, { project: projectID, ...rest });
        return buildJsonToolResponse("create_environment", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "update_environment",
    {
      title: "Update Environment",
      description:
        "Update an existing Coalesce environment. Partial update — only provided fields are changed.\n\nArgs:\n  - environmentID (string, required): The environment ID to update\n  - name (string, optional): Updated environment name\n  - oauthEnabled (boolean, optional): Toggle OAuth\n  - connectionAccount (string, optional): Updated connection account\n  - runTimeParameters (object, optional): Updated runtime parameters\n  - tagColors (object, optional): Updated UI tag colors\n\nReturns:\n  Updated environment object.",
      inputSchema: z.object({
        environmentID: z.string().describe("The environment ID to update"),
        name: z.string().optional().describe("Updated name for the environment"),
        oauthEnabled: z.boolean().optional().describe("Whether OAuth is enabled"),
        devEnv: z.boolean().optional().describe("Whether this is a dev environment"),
        connectionAccount: z.string().optional().describe("Connection account identifier"),
        runTimeParameters: z.record(z.unknown()).optional().describe("Runtime parameters"),
        tagColors: z
          .object({
            backgroundColor: z.string().optional(),
            textColor: z.string().optional(),
          })
          .optional()
          .describe("Tag colors for the environment"),
      }),
      outputSchema: getToolOutputSchema("update_environment"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await updateEnvironment(client, params);
        return buildJsonToolResponse("update_environment", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "delete_environment",
    {
      title: "Delete Environment",
      description:
        "Permanently delete a Coalesce environment. This is destructive and cannot be undone.\n\nArgs:\n  - environmentID (string, required): The environment ID to delete\n\nReturns:\n  Confirmation message.",
      inputSchema: z.object({
        environmentID: z.string().describe("The environment ID to delete"),
      }),
      outputSchema: getToolOutputSchema("delete_environment"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await deleteEnvironment(client, params);
        return buildJsonToolResponse("delete_environment", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
