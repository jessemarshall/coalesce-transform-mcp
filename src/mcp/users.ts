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
  READ_ONLY_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool, defineDestructiveTool } from "./tool-helpers.js";

function summarizeRole(roles: unknown): string | undefined {
  if (!roles || typeof roles !== "object") return undefined;
  const r = roles as Record<string, unknown>;
  const labelKeys = ["projectRole", "environmentRole", "orgRole", "role"];
  for (const key of labelKeys) {
    const value = r[key];
    if (typeof value === "string" && value.length > 0) return value;
  }
  return undefined;
}

export function defineUserTools(
  server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  defineSimpleTool(client, "list_org_users", {
    title: "List Org Users",
    description:
      "List all users in the Coalesce organization.\n\nArgs:\n  - limit, startingFrom, orderBy, orderByDirection: Pagination controls\n\nReturns:\n  { data: User[], next?: string, total?: number }",
    inputSchema: PaginationParams,
    annotations: READ_ONLY_ANNOTATIONS,
  }, listOrgUsers),

  defineSimpleTool(client, "get_user_roles", {
    title: "Get User Roles",
    description:
      "Get roles assigned to a specific user, optionally scoped to a project or environment.\n\nArgs:\n  - userID (string, required): The user ID\n  - projectID (string, optional): Scope to a specific project\n  - environmentID (string, optional): Scope to a specific environment\n\nReturns:\n  Role assignments for the user.",
    inputSchema: z.object({
      userID: z.string().min(1, "userID must not be empty").describe("The user ID"),
      projectID: z.string().min(1, "projectID must not be empty when provided").optional().describe("Optional project scope filter"),
      environmentID: z.string().min(1, "environmentID must not be empty when provided").optional().describe("Optional environment scope filter"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, getUserRoles),

  defineSimpleTool(client, "list_user_roles", {
    title: "List User Roles",
    description:
      "List roles for all users in the organization, optionally scoped.\n\nArgs:\n  - projectID (string, optional): Scope to a project\n  - environmentID (string, optional): Scope to an environment\n\nReturns:\n  { data: RoleAssignment[], next?: string, total?: number }",
    inputSchema: z.object({
      projectID: z.string().optional().describe("Optional project scope filter"),
      environmentID: z.string().optional().describe("Optional environment scope filter"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, listUserRoles),

  defineSimpleTool(client, "set_org_role", {
    title: "Set Org Role",
    description:
      "Set the organization-level role for a user. Idempotent — safe to call multiple times.\n\nArgs:\n  - userID (string, required): The user ID\n  - role (string, required): Role to assign (e.g., 'admin', 'member', 'viewer')\n\nReturns:\n  Updated role assignment.",
    inputSchema: z.object({
      userID: z.string().min(1, "userID must not be empty").describe("The user ID"),
      role: z.string().min(1, "role must not be empty").describe("The organization role to assign (e.g., 'admin', 'member', 'viewer')"),
    }),
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
  }, (client, params) => {
    const { userID, ...body } = params;
    return setOrgRole(client, { userID, body });
  }),

  defineSimpleTool(client, "set_project_role", {
    title: "Set Project Role",
    description:
      "Set a user's role for a specific project. Idempotent.\n\nArgs:\n  - userID (string, required): The user ID\n  - projectID (string, required): The project ID\n  - role (string, required): Role to assign (e.g., 'admin', 'developer', 'viewer')\n\nReturns:\n  Updated role assignment.",
    inputSchema: z.object({
      userID: z.string().min(1, "userID must not be empty").describe("The user ID"),
      projectID: z.string().min(1, "projectID must not be empty").describe("The project ID"),
      role: z.string().min(1, "role must not be empty").describe("The project role to assign (e.g., 'admin', 'developer', 'viewer')"),
    }),
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
  }, (client, params) => {
    const { userID, projectID, role } = params;
    return setProjectRole(client, { userID, projectID, body: { projectRole: role } });
  }),

  defineDestructiveTool(server, client, "delete_project_role", {
    title: "Delete Project Role",
    description:
      "Remove a user's role from a specific project. Destructive — the user will lose project access immediately.\n\nArgs:\n  - userID (string, required): The user ID\n  - projectID (string, required): The project ID\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms the role removal\n\nReturns:\n  Confirmation message.",
    inputSchema: z.object({
      userID: z.string().min(1, "userID must not be empty").describe("The user ID"),
      projectID: z.string().min(1, "projectID must not be empty").describe("The project ID"),
      confirmed: z
        .boolean()
        .optional()
        .describe("Set to true after the user explicitly confirms the role removal."),
    }),
    annotations: DESTRUCTIVE_ANNOTATIONS,
    resolve: async (client, params) => {
      const roles = await getUserRoles(client, {
        userID: params.userID,
        projectID: params.projectID,
      });
      const roleLabel = summarizeRole(roles);
      if (!roleLabel) {
        throw new Error(
          `no project role found for user "${params.userID}" on project "${params.projectID}" — nothing to remove`
        );
      }
      return {
        primary: {
          type: "project_role",
          id: `${params.userID}@${params.projectID}`,
          name: `${roleLabel} (user=${params.userID}, project=${params.projectID})`,
        },
      };
    },
    confirmMessage: (params, preview) => {
      const label = preview?.primary.name ?? `user "${params.userID}" on project "${params.projectID}"`;
      return `This will remove the project role for ${label}. The user will lose project access immediately.`;
    },
  }, deleteProjectRole),

  defineSimpleTool(client, "set_env_role", {
    title: "Set Env Role",
    description:
      "Set a user's role for a specific environment. Idempotent.\n\nArgs:\n  - userID (string, required): The user ID\n  - environmentID (string, required): The environment ID\n  - role (string, required): Role to assign\n\nReturns:\n  Updated role assignment.",
    inputSchema: z.object({
      userID: z.string().min(1, "userID must not be empty").describe("The user ID"),
      environmentID: z.string().min(1, "environmentID must not be empty").describe("The environment ID"),
      role: z.string().min(1, "role must not be empty").describe("The environment role to assign (e.g., 'admin', 'developer', 'viewer')"),
    }),
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
  }, (client, params) => {
    const { userID, environmentID, role } = params;
    return setEnvRole(client, { userID, environmentID, body: { environmentRole: role } });
  }),

  defineDestructiveTool(server, client, "delete_env_role", {
    title: "Delete Env Role",
    description:
      "Remove a user's role from a specific environment. Destructive — the user will lose environment access immediately.\n\nArgs:\n  - userID (string, required): The user ID\n  - environmentID (string, required): The environment ID\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms the role removal\n\nReturns:\n  Confirmation message.",
    inputSchema: z.object({
      userID: z.string().min(1, "userID must not be empty").describe("The user ID"),
      environmentID: z.string().min(1, "environmentID must not be empty").describe("The environment ID"),
      confirmed: z
        .boolean()
        .optional()
        .describe("Set to true after the user explicitly confirms the role removal."),
    }),
    annotations: DESTRUCTIVE_ANNOTATIONS,
    resolve: async (client, params) => {
      const roles = await getUserRoles(client, {
        userID: params.userID,
        environmentID: params.environmentID,
      });
      const roleLabel = summarizeRole(roles);
      if (!roleLabel) {
        throw new Error(
          `no environment role found for user "${params.userID}" on environment "${params.environmentID}" — nothing to remove`
        );
      }
      return {
        primary: {
          type: "environment_role",
          id: `${params.userID}@${params.environmentID}`,
          name: `${roleLabel} (user=${params.userID}, environment=${params.environmentID})`,
        },
      };
    },
    confirmMessage: (params, preview) => {
      const label = preview?.primary.name ?? `user "${params.userID}" on environment "${params.environmentID}"`;
      return `This will remove the environment role for ${label}. The user will lose environment access immediately.`;
    },
  }, deleteEnvRole),
  ];
}
