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
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool, defineDestructiveTool, extractEntityName } from "./tool-helpers.js";

export function defineGitAccountTools(
  server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  const accountOwnerParam = z.string().optional().describe("User ID of the account owner (org admins can manage other users' accounts)");

  return [
  defineSimpleTool(client, "list_git_accounts", {
    title: "List Git Accounts",
    description:
      "List all Git accounts configured in Coalesce.\n\nArgs:\n  - accountOwner (string, optional): User ID of the account owner. Org admins can manage other users' accounts\n\nReturns:\n  { data: GitAccount[], next?: string, total?: number }",
    inputSchema: z.object({
      accountOwner: accountOwnerParam,
    }),
    annotations: READ_ONLY_ANNOTATIONS,
    sanitize: true,
  }, listGitAccounts),

  defineSimpleTool(client, "get_git_account", {
    title: "Get Git Account",
    description:
      "Get details of a specific Git account.\n\nArgs:\n  - gitAccountID (string, required): The Git account ID\n  - accountOwner (string, optional): User ID of the account owner\n\nReturns:\n  Git account object with connection details.",
    inputSchema: z.object({
      gitAccountID: z.string().describe("The git account ID"),
      accountOwner: accountOwnerParam,
    }),
    annotations: READ_ONLY_ANNOTATIONS,
    sanitize: true,
  }, getGitAccount),

  defineSimpleTool(client, "create_git_account", {
    title: "Create Git Account",
    description:
      "Create a new Git account in Coalesce.\n\nArgs:\n  - name (string, required): Account name\n  - gitUsername (string, required): Git username for authentication\n  - gitAuthorName (string, required): Author name for git commits\n  - gitAuthorEmail (string, required): Email address for git commits\n  - gitToken (string, required): Personal access token for the git provider\n  - provider (enum, optional): 'github' | 'gitlab' | 'bitbucket' | 'azureDevOps'\n  - accountOwner (string, optional): User ID of the account owner\n\nReturns:\n  Created Git account with assigned ID.",
    inputSchema: z.object({
      name: z.string().describe("Name for the git account"),
      gitUsername: z.string().describe("Git username for authentication"),
      gitAuthorName: z.string().describe("Author name used for git commits"),
      gitAuthorEmail: z.string().describe("Email address used for git commits"),
      gitToken: z.string().describe("Personal access token or authentication token for the git provider"),
      provider: z.enum(["github", "gitlab", "bitbucket", "azureDevOps"]).optional().describe("Git provider type"),
      accountOwner: accountOwnerParam,
    }),
    annotations: WRITE_ANNOTATIONS,
    sanitize: true,
  }, (client, params) => {
    const { accountOwner, name, ...rest } = params;
    return createGitAccount(client, { body: { gitAccountName: name, ...rest }, accountOwner });
  }),

  defineSimpleTool(client, "update_git_account", {
    title: "Update Git Account",
    description:
      "Update an existing Git account. Partial update — only provided fields are changed.\n\nArgs:\n  - gitAccountID (string, required): The account ID\n  - name (string, optional): Updated name\n  - gitUsername (string, optional): Updated git username\n  - gitAuthorName (string, optional): Updated author name for git commits\n  - gitAuthorEmail (string, optional): Updated email for git commits\n  - gitToken (string, optional): Updated personal access token\n  - provider (enum, optional): 'github' | 'gitlab' | 'bitbucket' | 'azureDevOps'\n  - accountOwner (string, optional): User ID of the account owner\n\nReturns:\n  Updated Git account object.",
    inputSchema: z.object({
      gitAccountID: z.string().describe("The git account ID"),
      name: z.string().optional().describe("Updated name for the git account"),
      gitUsername: z.string().optional().describe("Updated git username for authentication"),
      gitAuthorName: z.string().optional().describe("Updated author name for git commits"),
      gitAuthorEmail: z.string().optional().describe("Updated email for git commits"),
      gitToken: z.string().optional().describe("Updated personal access token for the git provider"),
      provider: z.enum(["github", "gitlab", "bitbucket", "azureDevOps"]).optional().describe("Git provider type"),
      accountOwner: accountOwnerParam,
    }),
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    sanitize: true,
  }, (client, params) => {
    const { gitAccountID, accountOwner, name, ...body } = params;
    const patchBody = name ? { gitAccountName: name, ...body } : body;
    return updateGitAccount(client, { gitAccountID, body: patchBody, accountOwner });
  }),

  defineDestructiveTool(server, client, "delete_git_account", {
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
    resolve: async (client, params) => {
      const account = await getGitAccount(client, {
        gitAccountID: params.gitAccountID,
        accountOwner: params.accountOwner,
      });
      const extracted = extractEntityName(account)
        ?? (typeof (account as { gitAccountName?: unknown })?.gitAccountName === "string"
          ? (account as { gitAccountName: string }).gitAccountName
          : undefined);
      return {
        primary: {
          type: "git_account",
          id: params.gitAccountID,
          name: extracted,
        },
      };
    },
    confirmMessage: (params, preview) => {
      const label = preview?.primary.name
        ? `"${preview.primary.name}" (${params.gitAccountID})`
        : `"${params.gitAccountID}"`;
      return `This will permanently delete git account ${label}. This cannot be undone.`;
    },
  }, deleteGitAccount),
  ];
}
