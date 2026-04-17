import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listWorkspaceSubgraphs,
  getWorkspaceSubgraph,
} from "../coalesce/api/subgraphs.js";
import {
  createSubgraphWithCache,
  updateSubgraphResolved,
  deleteSubgraphByID,
} from "../services/subgraphs/operations.js";
import { resolveSubgraphByName } from "../services/subgraphs/resolve.js";
import {
  PaginationParams,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool, defineDestructiveTool, extractEntityName } from "./tool-helpers.js";

export function defineSubgraphTools(
  server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  defineSimpleTool(client, "list_workspace_subgraphs", {
    title: "List Workspace Subgraphs",
    description:
      "List all subgraphs in a Coalesce workspace. Use this to discover subgraph IDs.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - limit, startingFrom, orderBy, orderByDirection: Pagination\n\nReturns:\n  { data: Subgraph[], next?: string, total?: number }",
    inputSchema: PaginationParams.extend({
      workspaceID: z.string().describe("The workspace ID"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, listWorkspaceSubgraphs),

  defineSimpleTool(client, "get_workspace_subgraph", {
    title: "Get Workspace Subgraph",
    description:
      "Get details of a specific subgraph.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - subgraphID (string, required): The subgraph ID\n\nReturns:\n  Subgraph object with name and node steps.",
    inputSchema: z.object({
      workspaceID: z.string().describe("The workspace ID"),
      subgraphID: z.string().describe("The subgraph ID"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, getWorkspaceSubgraph),

  defineSimpleTool(client, "create_workspace_subgraph", {
    title: "Create Workspace Subgraph",
    description:
      "Create a subgraph in a Coalesce workspace. Subgraphs group nodes visually. The assigned UUID is cached locally so future edits can reference the subgraph by name.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - name (string, required): Subgraph name\n  - steps (string[], required): Array of node IDs to include\n\nReturns:\n  { subgraphID, subgraph, cached, message } — subgraphID is the UUID; cache it if you need to edit this subgraph later.",
    inputSchema: z.object({
      workspaceID: z.string().describe("The workspace ID"),
      name: z.string().describe("Name for the subgraph"),
      steps: z
        .array(z.string())
        .describe("Array of node IDs to include in the subgraph"),
    }),
    annotations: WRITE_ANNOTATIONS,
  }, createSubgraphWithCache),

  defineSimpleTool(client, "update_workspace_subgraph", {
    title: "Update Workspace Subgraph",
    description:
      "Update a subgraph's name and member nodes. Replaces the entire steps array. Pass either subgraphID (fastest) or subgraphName — if only the name is given, the ID is resolved from (1) the local UUID cache, (2) {repoPath}/subgraphs/*.yml, (3) the workspace subgraph list.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - subgraphID (string, optional): The subgraph ID. Preferred when known.\n  - subgraphName (string, optional): The subgraph name. Used to resolve the ID when subgraphID is absent.\n  - repoPath (string, optional): Coalesce repo path for subgraph YAML lookup.\n  - name (string, required): Updated name\n  - steps (string[], required): Updated node IDs\n\nReturns:\n  { subgraphID, subgraph, resolvedFrom } where resolvedFrom is \"input\" | \"cache\" | \"repo\" | \"workspace\".",
    inputSchema: z.object({
      workspaceID: z.string().describe("The workspace ID"),
      subgraphID: z
        .string()
        .optional()
        .describe("The subgraph ID. Preferred when known — fastest path."),
      subgraphName: z
        .string()
        .optional()
        .describe(
          "The subgraph name. Used to resolve the ID from the local cache, repo subgraphs/ folder, or workspace list when subgraphID is absent."
        ),
      repoPath: z
        .string()
        .optional()
        .describe(
          "Optional Coalesce repo path for subgraph YAML lookup. Falls back to COALESCE_REPO_PATH or the coa profile."
        ),
      name: z.string().describe("Updated name for the subgraph"),
      steps: z
        .array(z.string())
        .describe("Updated array of node IDs to include in the subgraph"),
    }),
    annotations: WRITE_ANNOTATIONS,
  }, updateSubgraphResolved),

  defineDestructiveTool(server, client, "delete_workspace_subgraph", {
    title: "Delete Workspace Subgraph",
    description:
      "Delete a subgraph from a workspace. Destructive — the subgraph is removed but its member nodes are NOT deleted. Pass either subgraphID (preferred) or subgraphName with optional repoPath.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - subgraphID (string, optional): The subgraph ID. Preferred when known.\n  - subgraphName (string, optional): The subgraph name. Resolved via cache \u2192 repo \u2192 workspace when subgraphID is absent.\n  - repoPath (string, optional): Coalesce repo path for subgraph YAML lookup.\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms deletion\n\nReturns:\n  Confirmation message.",
    inputSchema: z.object({
      workspaceID: z.string().describe("The workspace ID"),
      subgraphID: z
        .string()
        .optional()
        .describe("The subgraph ID to delete. Preferred when known."),
      subgraphName: z
        .string()
        .optional()
        .describe(
          "The subgraph name. Resolved via cache \u2192 repo \u2192 workspace list when subgraphID is absent."
        ),
      repoPath: z
        .string()
        .optional()
        .describe(
          "Optional Coalesce repo path for subgraph YAML lookup. Falls back to COALESCE_REPO_PATH or the coa profile."
        ),
      confirmed: z
        .boolean()
        .optional()
        .describe("Set to true after the user explicitly confirms the deletion."),
    }),
    annotations: DESTRUCTIVE_ANNOTATIONS,
    resolve: async (client, params) => {
      if (!params.subgraphID && !params.subgraphName) {
        throw new Error("Either subgraphID or subgraphName is required.");
      }
      if (!params.subgraphID) {
        const resolved = await resolveSubgraphByName(client, {
          workspaceID: params.workspaceID,
          name: params.subgraphName!,
          repoPath: params.repoPath,
        });
        // Mutate params so the downstream apiFunc skips a second resolve.
        // defineDestructiveTool passes the same params object to both callbacks.
        params.subgraphID = resolved.id;
      }
      const id = params.subgraphID!;
      const subgraph = await getWorkspaceSubgraph(client, {
        workspaceID: params.workspaceID,
        subgraphID: id,
      });
      const steps = (subgraph as { steps?: unknown })?.steps;
      const memberIDs = Array.isArray(steps) ? steps.filter((s) => typeof s === "string") : [];
      return {
        primary: {
          type: "workspace_subgraph",
          id,
          name: extractEntityName(subgraph),
        },
        affected: memberIDs.map((memberID) => ({
          type: "member_node",
          id: String(memberID),
          note: "reference removed; node itself is not deleted",
        })),
        context: { workspaceID: params.workspaceID, memberCount: memberIDs.length },
      };
    },
    confirmMessage: (params, preview) => {
      const resolvedID = preview?.primary.id ?? params.subgraphID ?? params.subgraphName ?? "unknown";
      const label = preview?.primary.name
        ? `"${preview.primary.name}" (${resolvedID})`
        : `"${resolvedID}"`;
      return `This will permanently delete subgraph ${label} from workspace "${params.workspaceID}". Member nodes will NOT be deleted.`;
    },
  }, async (client, params) =>
    deleteSubgraphByID(client, {
      workspaceID: params.workspaceID,
      subgraphID: params.subgraphID!,
    })
  ),
  ];
}
