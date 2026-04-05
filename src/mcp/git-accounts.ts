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
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";
import { registerSimpleTool, registerDestructiveTool } from "./tool-helpers.js";

export function registerGitAccountTools(
  server: McpServer,
  client: CoalesceClient
): void {
  const accountOwnerParam = z.string().optional().describe("User ID of the account owner (org admins can manage other users' accounts)");

  registerSimpleTool(server, client, "list_git_accounts", {
    title: "List Git Accounts",
    description:
      "List all Git accounts configured in Coalesce.\n\nArgs:\n  - accountOwner (string, optional): User ID of the account owner. Org admins can manage other users' accounts\n\nReturns:\n  { data: GitAccount[], next?: string, total?: number }",
    inputSchema: z.object({
      accountOwner: accountOwnerParam,
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, listGitAccounts);

  registerSimpleTool(server, client, "get_git_account", {
    title: "Get Git Account",
    description:
      "Get details of a specific Git account.\n\nArgs:\n  - gitAccountID (string, required): The Git account ID\n  - accountOwner (string, optional): User ID of the account owner\n\nReturns:\n  Git account object with connection details.",
    inputSchema: z.object({
      gitAccountID: z.string().describe("The git account ID"),
      accountOwner: accountOwnerParam,
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, getGitAccount);

  registerSimpleTool(server, client, "create_git_account", {
    title: "Create Git Account",
    description:
      "Create a new Git account in Coalesce.\n\nArgs:\n  - name (string, required): Account name\n  - gitAuthorName (string, required): Author name for git commits\n  - gitAuthorEmail (string, required): Email address for git commits\n  - provider (enum, optional): 'github' | 'gitlab' | 'bitbucket' | 'azureDevOps'\n  - accessToken (string, optional): Personal access token for the git provider\n  - accountOwner (string, optional): User ID of the account owner\n\nReturns:\n  Created Git account with assigned ID.",
    inputSchema: z.object({
      name: z.string().describe("Name for the git account"),
      gitAuthorName: z.string().describe("Author name used for git commits"),
      gitAuthorEmail: z.string().describe("Email address used for git commits"),
      provider: z.enum(["github", "gitlab", "bitbucket", "azureDevOps"]).optional().describe("Git provider type"),
      accessToken: z.string().optional().describe("Personal access token for the git provider"),
      accountOwner: accountOwnerParam,
    }),
    annotations: WRITE_ANNOTATIONS,
  }, (client, params) => {
    const { accountOwner, name, ...rest } = params;
    return createGitAccount(client, { body: { gitAccountName: name, ...rest }, accountOwner });
  });

  registerSimpleTool(server, client, "update_git_account", {
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
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
  }, (client, params) => {
    const { gitAccountID, accountOwner, ...body } = params;
    return updateGitAccount(client, { gitAccountID, body, accountOwner });
  });

  registerDestructiveTool(server, client, "delete_git_account", {
    title: "Delete Git Account",
    description:
      "Permanently delete a Git account. Destructive and cannot be undone — if this is the only git account linked to a project, it breaks the CI/CD connection.\n\nArgs:\n  - gitAccountID (string, required): The account ID\n  - accountOwner (string, optional): User ID of the account owner\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms deletion\n\nReturns:\n  Confirmation message.",
    inputSchema: z.object({
      gitAccountID: z.string().describe("The git account ID"),
      accountOwner: accountOwnerParam,
      confirmed: z
        .boolean()
        .optional()
        .describe("Set to true after the user explicitly confirms the deletion."),
    }),
    annotations: DESTRUCTIVE_ANNOTATIONS,
    confirmMessage: (params) => `This will permanently delete git account "${params.gitAccountID}". This cannot be undone.`,
  }, deleteGitAccount);
}
