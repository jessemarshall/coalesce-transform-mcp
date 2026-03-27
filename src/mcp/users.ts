import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listOrgUsers,
  getUserRoles,
  listUserRoles,
  setOrgRole,
  setProjectRole,
  deleteProjectRole,
  setEnvRole,
  deleteEnvRole,
} from "../coalesce/api/users.js";
import {
  PaginationParams,
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  READ_ONLY_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerUserTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "list_org_users",
    {
      title: "List Org Users",
      description:
        "List all users in the Coalesce organization.\n\nArgs:\n  - limit, startingFrom, orderBy, orderByDirection: Pagination controls\n\nReturns:\n  { data: User[], next?: string, total?: number }",
      inputSchema: PaginationParams,
      outputSchema: getToolOutputSchema("list_org_users"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listOrgUsers(client, params);
        return buildJsonToolResponse("list_org_users", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_user_roles",
    {
      title: "Get User Roles",
      description:
        "Get roles assigned to a specific user, optionally scoped to a project or environment.\n\nArgs:\n  - userID (string, required): The user ID\n  - projectID (string, optional): Scope to a specific project\n  - environmentID (string, optional): Scope to a specific environment\n\nReturns:\n  Role assignments for the user.",
      inputSchema: z.object({
        userID: z.string().describe("The user ID"),
        projectID: z.string().optional().describe("Optional project scope filter"),
        environmentID: z.string().optional().describe("Optional environment scope filter"),
      }),
      outputSchema: getToolOutputSchema("get_user_roles"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getUserRoles(client, params);
        return buildJsonToolResponse("get_user_roles", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "list_user_roles",
    {
      title: "List User Roles",
      description:
        "List roles for all users in the organization, optionally scoped.\n\nArgs:\n  - projectID (string, optional): Scope to a project\n  - environmentID (string, optional): Scope to an environment\n\nReturns:\n  { data: RoleAssignment[], next?: string, total?: number }",
      inputSchema: z.object({
        projectID: z.string().optional().describe("Optional project scope filter"),
        environmentID: z.string().optional().describe("Optional environment scope filter"),
      }),
      outputSchema: getToolOutputSchema("list_user_roles"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listUserRoles(client, params);
        return buildJsonToolResponse("list_user_roles", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "set_org_role",
    {
      title: "Set Org Role",
      description:
        "Set the organization-level role for a user. Idempotent — safe to call multiple times.\n\nArgs:\n  - userID (string, required): The user ID\n  - role (string, required): Role to assign (e.g., 'admin', 'member', 'viewer')\n\nReturns:\n  Updated role assignment.",
      inputSchema: z.object({
        userID: z.string().describe("The user ID"),
        role: z.string().describe("The organization role to assign (e.g., 'admin', 'member', 'viewer')"),
      }),
      outputSchema: getToolOutputSchema("set_org_role"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const { userID, ...body } = params;
        const result = await setOrgRole(client, { userID, body });
        return buildJsonToolResponse("set_org_role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "set_project_role",
    {
      title: "Set Project Role",
      description:
        "Set a user's role for a specific project. Idempotent.\n\nArgs:\n  - userID (string, required): The user ID\n  - projectID (string, required): The project ID\n  - role (string, required): Role to assign (e.g., 'admin', 'developer', 'viewer')\n\nReturns:\n  Updated role assignment.",
      inputSchema: z.object({
        userID: z.string().describe("The user ID"),
        projectID: z.string().describe("The project ID"),
        role: z.string().describe("The project role to assign (e.g., 'admin', 'developer', 'viewer')"),
      }),
      outputSchema: getToolOutputSchema("set_project_role"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const { userID, projectID, ...body } = params;
        const result = await setProjectRole(client, { userID, projectID, body });
        return buildJsonToolResponse("set_project_role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "delete_project_role",
    {
      title: "Delete Project Role",
      description:
        "Remove a user's role from a specific project. Destructive — the user will lose project access.\n\nArgs:\n  - userID (string, required): The user ID\n  - projectID (string, required): The project ID\n\nReturns:\n  Confirmation message.",
      inputSchema: z.object({
        userID: z.string().describe("The user ID"),
        projectID: z.string().describe("The project ID"),
      }),
      outputSchema: getToolOutputSchema("delete_project_role"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await deleteProjectRole(client, params);
        return buildJsonToolResponse("delete_project_role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "set_env_role",
    {
      title: "Set Env Role",
      description:
        "Set a user's role for a specific environment. Idempotent.\n\nArgs:\n  - userID (string, required): The user ID\n  - environmentID (string, required): The environment ID\n  - role (string, required): Role to assign\n\nReturns:\n  Updated role assignment.",
      inputSchema: z.object({
        userID: z.string().describe("The user ID"),
        environmentID: z.string().describe("The environment ID"),
        role: z.string().describe("The environment role to assign (e.g., 'admin', 'developer', 'viewer')"),
      }),
      outputSchema: getToolOutputSchema("set_env_role"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const { userID, environmentID, ...body } = params;
        const result = await setEnvRole(client, { userID, environmentID, body });
        return buildJsonToolResponse("set_env_role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "delete_env_role",
    {
      title: "Delete Env Role",
      description:
        "Remove a user's role from a specific environment. Destructive.\n\nArgs:\n  - userID (string, required): The user ID\n  - environmentID (string, required): The environment ID\n\nReturns:\n  Confirmation message.",
      inputSchema: z.object({
        userID: z.string().describe("The user ID"),
        environmentID: z.string().describe("The environment ID"),
      }),
      outputSchema: getToolOutputSchema("delete_env_role"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await deleteEnvRole(client, params);
        return buildJsonToolResponse("delete_env_role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
