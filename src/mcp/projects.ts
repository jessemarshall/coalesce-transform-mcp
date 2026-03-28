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
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerProjectTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "list_projects",
    {
      title: "List Projects",
      description:
        "List all Coalesce projects.\n\nArgs:\n  - includeWorkspaces (boolean, optional): Include nested workspace data\n  - includeJobs (boolean, optional): Include nested job data for all workspaces\n\nReturns:\n  { data: Project[], next?: string, total?: number }",
      inputSchema: z.object({
        includeWorkspaces: z.boolean().optional().describe("Include nested workspace data with workspace IDs"),
        includeJobs: z.boolean().optional().describe("Include nested job data for all workspaces"),
      }),
      outputSchema: getToolOutputSchema("list_projects"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listProjects(client, params);
        return buildJsonToolResponse("list_projects", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_project",
    {
      title: "Get Project",
      description:
        "Get details of a specific Coalesce project.\n\nArgs:\n  - projectID (string, required): The project ID\n  - includeWorkspaces (boolean, optional): Include nested workspace data\n  - includeJobs (boolean, optional): Include nested job data\n\nReturns:\n  Full project object with ID, name, description, git configuration.",
      inputSchema: z.object({
        projectID: z.string().describe("The project ID"),
        includeWorkspaces: z.boolean().optional().describe("Include nested workspace data with workspace IDs"),
        includeJobs: z.boolean().optional().describe("Include nested job data for all workspaces"),
      }),
      outputSchema: getToolOutputSchema("get_project"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getProject(client, params);
        return buildJsonToolResponse("get_project", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "create_project",
    {
      title: "Create Project",
      description:
        "Create a new Coalesce project.\n\nArgs:\n  - name (string, required): Project name\n  - description (string, optional): Project description\n  - gitAccountID (string, optional): Git account to link\n  - gitRepo (string, optional): Git repository URL\n  - gitBranch (string, optional): Default git branch\n\nReturns:\n  Created project object with assigned ID.",
      inputSchema: z.object({
        name: z.string().describe("Name for the new project"),
        description: z.string().optional().describe("Optional project description"),
        gitAccountID: z.string().optional().describe("Git account ID to link to the project"),
        gitRepo: z.string().optional().describe("Git repository URL"),
        gitBranch: z.string().optional().describe("Default git branch name"),
      }),
      outputSchema: getToolOutputSchema("create_project"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await createProject(client, { body: params });
        return buildJsonToolResponse("create_project", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "update_project",
    {
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
      outputSchema: getToolOutputSchema("update_project"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const { projectID, includeWorkspaces, includeJobs, ...body } = params;
        const result = await updateProject(client, { projectID, body, includeWorkspaces, includeJobs });
        return buildJsonToolResponse("update_project", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "delete_project",
    {
      title: "Delete Project",
      description:
        "Permanently delete a Coalesce project. This is destructive and cannot be undone.\n\nArgs:\n  - projectID (string, required): The project ID\n\nReturns:\n  Confirmation message.",
      inputSchema: z.object({
        projectID: z.string().describe("The project ID"),
      }),
      outputSchema: getToolOutputSchema("delete_project"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await deleteProject(client, params);
        return buildJsonToolResponse("delete_project", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
