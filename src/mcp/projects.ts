import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listProjects,
  getProject,
  createProject,
  updateProject,
  deleteProject,
} from "../coalesce/api/projects.js";
import {
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool, defineDestructiveTool, extractEntityName } from "./tool-helpers.js";

export function defineProjectTools(
  server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  defineSimpleTool(client, "list_projects", {
    title: "List Projects",
    description:
      "List all Coalesce projects.\n\nArgs:\n  - includeWorkspaces (boolean, optional): Include nested workspace data\n  - includeJobs (boolean, optional): Include nested job data for all workspaces\n\nReturns:\n  { data: Project[], next?: string, total?: number }",
    inputSchema: z.object({
      includeWorkspaces: z.boolean().optional().describe("Include nested workspace data with workspace IDs"),
      includeJobs: z.boolean().optional().describe("Include nested job data for all workspaces"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, listProjects),

  defineSimpleTool(client, "get_project", {
    title: "Get Project",
    description:
      "Get details of a specific Coalesce project.\n\nArgs:\n  - projectID (string, required): The project ID\n  - includeWorkspaces (boolean, optional): Include nested workspace data\n  - includeJobs (boolean, optional): Include nested job data\n\nReturns:\n  Full project object with ID, name, description, git configuration.",
    inputSchema: z.object({
      projectID: z.string().describe("The project ID"),
      includeWorkspaces: z.boolean().optional().describe("Include nested workspace data with workspace IDs"),
      includeJobs: z.boolean().optional().describe("Include nested job data for all workspaces"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, getProject),

  defineSimpleTool(client, "create_project", {
    title: "Create Project",
    description:
      "Create a new Coalesce project.\n\nArgs:\n  - name (string, required): Project name\n  - platformKind (enum, required): Target platform — 'snowflake', 'databricks', 'starburst', or 'spark'\n  - description (string, optional): Project description\n  - gitAccountID (string, optional): Git account to link\n  - gitRepo (string, optional): Git repository URL\n  - gitBranch (string, optional): Default git branch\n\nReturns:\n  Created project object with assigned ID.",
    inputSchema: z.object({
      name: z.string().describe("Name for the new project"),
      platformKind: z.enum(["snowflake", "databricks", "starburst", "spark"]).describe("Target platform for the project"),
      description: z.string().optional().describe("Optional project description"),
      gitAccountID: z.string().optional().describe("Git account ID to link to the project"),
      gitRepo: z.string().optional().describe("Git repository URL"),
      gitBranch: z.string().optional().describe("Default git branch name"),
    }),
    annotations: WRITE_ANNOTATIONS,
  }, (client, params) => createProject(client, { body: params })),

  defineSimpleTool(client, "update_project", {
    title: "Update Project",
    description:
      "Update an existing Coalesce project. Partial update — only provided fields are changed.\n\nArgs:\n  - projectID (string, required): The project ID\n  - name (string, optional): Updated name\n  - description (string, optional): Updated description\n  - gitAccountID, gitRepo, gitBranch (string, optional): Updated git settings\n  - includeWorkspaces, includeJobs (boolean, optional): Expand response\n\nReturns:\n  Updated project object.",
    inputSchema: z.object({
      projectID: z.string().describe("The project ID"),
      name: z.string().optional().describe("Updated project name"),
      description: z.string().optional().describe("Updated project description"),
      gitAccountID: z.string().optional().describe("Git account ID to link to the project"),
      gitRepo: z.string().optional().describe("Git repository URL"),
      gitBranch: z.string().optional().describe("Default git branch name"),
      includeWorkspaces: z.boolean().optional().describe("Include nested workspace data in the response"),
      includeJobs: z.boolean().optional().describe("Include nested job data in the response"),
    }),
    annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
  }, (client, params) => {
    const { projectID, includeWorkspaces, includeJobs, ...body } = params;
    return updateProject(client, { projectID, body, includeWorkspaces, includeJobs });
  }),

  defineDestructiveTool(server, client, "delete_project", {
    title: "Delete Project",
    description:
      "Permanently delete a Coalesce project. This is destructive and cannot be undone.\n\nArgs:\n  - projectID (string, required): The project ID\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms deletion\n\nReturns:\n  Confirmation message.",
    inputSchema: z.object({
      projectID: z.string().describe("The project ID"),
      confirmed: z
        .boolean()
        .optional()
        .describe("Set to true after the user explicitly confirms the deletion."),
    }),
    annotations: DESTRUCTIVE_ANNOTATIONS,
    resolve: async (client, params) => {
      const project = await getProject(client, {
        projectID: params.projectID,
        includeWorkspaces: true,
      });
      const workspaces = (project as { workspaces?: unknown })?.workspaces;
      const workspaceList = Array.isArray(workspaces) ? workspaces : [];
      return {
        primary: {
          type: "project",
          id: params.projectID,
          name: extractEntityName(project),
        },
        affected: workspaceList.map((ws) => {
          const w = ws as { id?: unknown; name?: unknown };
          return {
            type: "workspace",
            id: String(w.id ?? ""),
            name: typeof w.name === "string" ? w.name : undefined,
            note: "will be deleted with the project",
          };
        }),
        context: { workspaceCount: workspaceList.length },
      };
    },
    confirmMessage: (params, preview) => {
      const label = preview?.primary.name
        ? `"${preview.primary.name}" (${params.projectID})`
        : `"${params.projectID}"`;
      const wsCount = preview?.affected?.length ?? 0;
      const wsNote = wsCount > 0 ? ` and its ${wsCount} workspace${wsCount === 1 ? "" : "s"}` : "";
      return `This will permanently delete project ${label}${wsNote}. This cannot be undone.`;
    },
  }, deleteProject),
  ];
}
