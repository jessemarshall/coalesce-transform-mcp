import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listEnvironmentJobs,
  createWorkspaceJob,
  getEnvironmentJob,
  updateWorkspaceJob,
  deleteWorkspaceJob,
} from "../coalesce/api/jobs.js";
import {
  PaginationParams,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool, defineDestructiveTool } from "./tool-helpers.js";

export function defineJobTools(
  server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  defineSimpleTool(client, "list_environment_jobs", {
    title: "List Environment Jobs",
    description:
      "List all jobs in a Coalesce environment. Jobs define which nodes to run together.\n\nArgs:\n  - environmentID (string, required): The environment ID\n  - limit, startingFrom, orderBy, orderByDirection: Pagination\n\nReturns:\n  { data: Job[], next?: string, total?: number }",
    inputSchema: PaginationParams.extend({
      environmentID: z.string().describe("The environment ID"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, listEnvironmentJobs),

  defineSimpleTool(client, "create_workspace_job", {
    title: "Create Workspace Job",
    description:
      "Create a new job in a Coalesce workspace. Jobs define which nodes to run together.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - name (string, required): Job name\n  - includeSelector (string, required): Node selector. Format: '{ location: LOC name: NAME } OR { location: LOC name: NAME }'. Use empty string to include nothing\n  - excludeSelector (string, required): Node exclusion selector. Same format as includeSelector. Use empty string to exclude nothing\n\nReturns:\n  Created job with assigned ID.",
    inputSchema: z.object({
      workspaceID: z.string().describe("The workspace ID"),
      name: z.string().describe("Name for the job"),
      includeSelector: z
        .string()
        .describe("Node selector string. Format: '{ location: LOC name: NAME } OR { location: LOC name: NAME }'. Use empty string to include nothing."),
      excludeSelector: z
        .string()
        .describe("Node exclusion selector string. Same format as includeSelector. Use empty string to exclude nothing."),
    }),
    annotations: WRITE_ANNOTATIONS,
  }, createWorkspaceJob),

  defineSimpleTool(client, "get_environment_job", {
    title: "Get Environment Job",
    description:
      "Get details of a specific job in an environment.\n\nArgs:\n  - environmentID (string, required): The environment ID\n  - jobID (string, required): The job ID\n\nReturns:\n  Job object with name, node list, schedule, and configuration.",
    inputSchema: z.object({
      environmentID: z.string().describe("The environment ID"),
      jobID: z.string().describe("The job ID"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, getEnvironmentJob),

  defineSimpleTool(client, "update_workspace_job", {
    title: "Update Workspace Job",
    description:
      "Update an existing workspace job.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - jobID (string, required): The job ID\n  - name (string, required): Updated job name\n  - includeSelector (string, required): Node selector. Format: '{ location: LOC name: NAME } OR { location: LOC name: NAME }'\n  - excludeSelector (string, required): Node exclusion selector. Same format as includeSelector\n\nReturns:\n  Updated job object.",
    inputSchema: z.object({
      workspaceID: z.string().describe("The workspace ID"),
      jobID: z.string().describe("The job ID to update"),
      name: z.string().describe("Updated name for the job"),
      includeSelector: z
        .string()
        .describe("Node selector string. Format: '{ location: LOC name: NAME } OR { location: LOC name: NAME }'."),
      excludeSelector: z
        .string()
        .describe("Node exclusion selector string. Same format as includeSelector. Use empty string to exclude nothing."),
    }),
    annotations: WRITE_ANNOTATIONS,
  }, updateWorkspaceJob),

  defineDestructiveTool(server, client, "delete_workspace_job", {
    title: "Delete Workspace Job",
    description:
      "Delete a job from a workspace. Destructive — jobs define which nodes run together; deleting the wrong one breaks scheduled pipelines.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - jobID (string, required): The job ID\n  - confirmed (boolean, optional): Set to true after the user explicitly confirms deletion\n\nReturns:\n  Confirmation message.",
    inputSchema: z.object({
      workspaceID: z.string().describe("The workspace ID"),
      jobID: z.string().describe("The job ID to delete"),
      confirmed: z
        .boolean()
        .optional()
        .describe("Set to true after the user explicitly confirms the deletion."),
    }),
    annotations: DESTRUCTIVE_ANNOTATIONS,
    confirmMessage: (params) => `This will permanently delete job "${params.jobID}" from workspace "${params.workspaceID}". This cannot be undone.`,
  }, deleteWorkspaceJob),
  ];
}
