import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listGitAccounts,
  getGitAccount,
  createGitAccount,
  updateGitAccount,
  deleteGitAccount,
} from "../coalesce/api/git-accounts.js";
import {
  buildJsonToolResponse,
  handleToolError,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerGitAccountTools(
  server: McpServer,
  client: CoalesceClient
): void {
  const accountOwnerParam = z.string().optional().describe("User ID of the account owner (org admins can manage other users' accounts)");

  server.tool(
    "list-git-accounts",
    "List all Coalesce git accounts",
    {
      accountOwner: accountOwnerParam,
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listGitAccounts(client, params);
        return buildJsonToolResponse("list-git-accounts", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-git-account",
    "Get details of a specific Coalesce git account",
    {
      gitAccountID: z.string().describe("The git account ID"),
      accountOwner: accountOwnerParam,
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getGitAccount(client, params);
        return buildJsonToolResponse("get-git-account", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "create-git-account",
    "Create a new Coalesce git account",
    {
      body: z
        .record(z.unknown())
        .describe("The git account creation request body"),
      accountOwner: accountOwnerParam,
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await createGitAccount(client, params);
        return buildJsonToolResponse("create-git-account", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "update-git-account",
    "Update an existing Coalesce git account (partial update — only provided fields are changed)",
    {
      gitAccountID: z.string().describe("The git account ID"),
      body: z
        .record(z.unknown())
        .describe("The git account update request body (partial — only include fields to change)"),
      accountOwner: accountOwnerParam,
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await updateGitAccount(client, params);
        return buildJsonToolResponse("update-git-account", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "delete-git-account",
    "Delete a Coalesce git account",
    {
      gitAccountID: z.string().describe("The git account ID"),
      accountOwner: accountOwnerParam,
    },
    DESTRUCTIVE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await deleteGitAccount(client, params);
        return buildJsonToolResponse("delete-git-account", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
