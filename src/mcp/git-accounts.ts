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
  getToolOutputSchema,
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

  server.registerTool(
    "coalesce_list_git_accounts",
    {
      title: "List Git Accounts",
      description:
        "List all Git accounts configured in Coalesce.\n\nArgs:\n  - accountOwner (string, optional): User ID of the account owner. Org admins can manage other users' accounts\n\nReturns:\n  { data: GitAccount[], next?: string, total?: number }",
      inputSchema: z.object({
        accountOwner: accountOwnerParam,
      }),
      outputSchema: getToolOutputSchema("coalesce_list_git_accounts"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listGitAccounts(client, params);
        return buildJsonToolResponse("coalesce_list_git_accounts", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_get_git_account",
    {
      title: "Get Git Account",
      description:
        "Get details of a specific Git account.\n\nArgs:\n  - gitAccountID (string, required): The Git account ID\n  - accountOwner (string, optional): User ID of the account owner\n\nReturns:\n  Git account object with connection details.",
      inputSchema: z.object({
        gitAccountID: z.string().describe("The git account ID"),
        accountOwner: accountOwnerParam,
      }),
      outputSchema: getToolOutputSchema("coalesce_get_git_account"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getGitAccount(client, params);
        return buildJsonToolResponse("coalesce_get_git_account", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_create_git_account",
    {
      title: "Create Git Account",
      description:
        "Create a new Git account in Coalesce.\n\nArgs:\n  - name (string, required): Account name\n  - provider (enum, optional): 'github' | 'gitlab' | 'bitbucket' | 'azureDevOps'\n  - accessToken (string, optional): Personal access token for the git provider\n  - accountOwner (string, optional): User ID of the account owner\n\nReturns:\n  Created Git account with assigned ID.",
      inputSchema: z.object({
        name: z.string().describe("Name for the git account"),
        provider: z.enum(["github", "gitlab", "bitbucket", "azureDevOps"]).optional().describe("Git provider type"),
        accessToken: z.string().optional().describe("Personal access token for the git provider"),
        accountOwner: accountOwnerParam,
      }),
      outputSchema: getToolOutputSchema("coalesce_create_git_account"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const { accountOwner, ...body } = params;
        const result = await createGitAccount(client, { body, accountOwner });
        return buildJsonToolResponse("coalesce_create_git_account", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_update_git_account",
    {
      title: "Update Git Account",
      description:
        "Update an existing Git account. Partial update — only provided fields are changed.\n\nArgs:\n  - gitAccountID (string, required): The account ID\n  - name (string, optional): Updated name\n  - provider (enum, optional): 'github' | 'gitlab' | 'bitbucket' | 'azureDevOps'\n  - accessToken (string, optional): Updated personal access token\n  - accountOwner (string, optional): User ID of the account owner\n\nReturns:\n  Updated Git account object.",
      inputSchema: z.object({
        gitAccountID: z.string().describe("The git account ID"),
        name: z.string().optional().describe("Updated name for the git account"),
        provider: z.enum(["github", "gitlab", "bitbucket", "azureDevOps"]).optional().describe("Git provider type"),
        accessToken: z.string().optional().describe("Updated personal access token"),
        accountOwner: accountOwnerParam,
      }),
      outputSchema: getToolOutputSchema("coalesce_update_git_account"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const { gitAccountID, accountOwner, ...body } = params;
        const result = await updateGitAccount(client, { gitAccountID, body, accountOwner });
        return buildJsonToolResponse("coalesce_update_git_account", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_delete_git_account",
    {
      title: "Delete Git Account",
      description:
        "Permanently delete a Git account. Destructive and cannot be undone.\n\nArgs:\n  - gitAccountID (string, required): The account ID\n  - accountOwner (string, optional): User ID of the account owner\n\nReturns:\n  Confirmation message.",
      inputSchema: z.object({
        gitAccountID: z.string().describe("The git account ID"),
        accountOwner: accountOwnerParam,
      }),
      outputSchema: getToolOutputSchema("coalesce_delete_git_account"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await deleteGitAccount(client, params);
        return buildJsonToolResponse("coalesce_delete_git_account", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
