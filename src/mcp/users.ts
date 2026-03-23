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
  READ_ONLY_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerUserTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.tool(
    "list-org-users",
    "List all users in the Coalesce organization",
    PaginationParams.shape,
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listOrgUsers(client, params);
        return buildJsonToolResponse("list-org-users", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-user-roles",
    "Get roles assigned to a specific user",
    {
      userID: z.string().describe("The user ID"),
      projectID: z.string().optional().describe("Optional project scope filter"),
      environmentID: z.string().optional().describe("Optional environment scope filter"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getUserRoles(client, params);
        return buildJsonToolResponse("get-user-roles", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "list-user-roles",
    "List roles for all users in the organization",
    {
      projectID: z.string().optional().describe("Optional project scope filter"),
      environmentID: z.string().optional().describe("Optional environment scope filter"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listUserRoles(client, params);
        return buildJsonToolResponse("list-user-roles", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "set-org-role",
    "Set the organization-level role for a user",
    {
      userID: z.string().describe("The user ID"),
      body: z
        .record(z.unknown())
        .describe("The organization role assignment body"),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await setOrgRole(client, params);
        return buildJsonToolResponse("set-org-role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "set-project-role",
    "Set a user's role for a specific project",
    {
      userID: z.string().describe("The user ID"),
      projectID: z.string().describe("The project ID"),
      body: z
        .record(z.unknown())
        .describe("The project role assignment body"),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await setProjectRole(client, params);
        return buildJsonToolResponse("set-project-role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "delete-project-role",
    "Remove a user's role from a specific project",
    {
      userID: z.string().describe("The user ID"),
      projectID: z.string().describe("The project ID"),
    },
    DESTRUCTIVE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await deleteProjectRole(client, params);
        return buildJsonToolResponse("delete-project-role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "set-env-role",
    "Set a user's role for a specific environment",
    {
      userID: z.string().describe("The user ID"),
      environmentID: z.string().describe("The environment ID"),
      body: z
        .record(z.unknown())
        .describe("The environment role assignment body"),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await setEnvRole(client, params);
        return buildJsonToolResponse("set-env-role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "delete-env-role",
    "Remove a user's role from a specific environment",
    {
      userID: z.string().describe("The user ID"),
      environmentID: z.string().describe("The environment ID"),
    },
    DESTRUCTIVE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await deleteEnvRole(client, params);
        return buildJsonToolResponse("delete-env-role", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
