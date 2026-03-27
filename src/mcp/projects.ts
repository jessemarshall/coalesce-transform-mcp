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
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerProjectTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.tool(
    "list-projects",
    "List all Coalesce projects. For workspace IDs, prefer list-workspaces instead of includeWorkspaces.",
    {
      includeWorkspaces: z.boolean().optional().describe("Include nested workspace data with workspace IDs"),
      includeJobs: z.boolean().optional().describe("Include nested job data for all workspaces"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listProjects(client, params);
        return buildJsonToolResponse("list-projects", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-project",
    "Get details of a specific Coalesce project. For workspace IDs, prefer list-workspaces instead of includeWorkspaces.",
    {
      projectID: z.string().describe("The project ID"),
      includeWorkspaces: z.boolean().optional().describe("Include nested workspace data with workspace IDs"),
      includeJobs: z.boolean().optional().describe("Include nested job data for all workspaces"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getProject(client, params);
        return buildJsonToolResponse("get-project", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "create-project",
    "Create a new Coalesce project",
    {
      name: z.string().describe("Name for the new project"),
      description: z.string().optional().describe("Optional project description"),
      gitAccountID: z.string().optional().describe("Git account ID to link to the project"),
      gitRepo: z.string().optional().describe("Git repository URL"),
      gitBranch: z.string().optional().describe("Default git branch name"),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await createProject(client, { body: params });
        return buildJsonToolResponse("create-project", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "update-project",
    "Update an existing Coalesce project (partial update — only provided fields are changed)",
    {
      projectID: z.string().describe("The project ID"),
      name: z.string().optional().describe("Updated project name"),
      description: z.string().optional().describe("Updated project description"),
      gitAccountID: z.string().optional().describe("Git account ID to link to the project"),
      gitRepo: z.string().optional().describe("Git repository URL"),
      gitBranch: z.string().optional().describe("Default git branch name"),
      includeWorkspaces: z.boolean().optional().describe("Include nested workspace data in the response"),
      includeJobs: z.boolean().optional().describe("Include nested job data in the response"),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const { projectID, includeWorkspaces, includeJobs, ...body } = params;
        const result = await updateProject(client, { projectID, body, includeWorkspaces, includeJobs });
        return buildJsonToolResponse("update-project", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "delete-project",
    "Delete a Coalesce project",
    {
      projectID: z.string().describe("The project ID"),
    },
    DESTRUCTIVE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await deleteProject(client, params);
        return buildJsonToolResponse("delete-project", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
