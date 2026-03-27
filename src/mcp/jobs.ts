import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listWorkspaceJobs,
  listEnvironmentJobs,
  createWorkspaceJob,
  getEnvironmentJob,
  updateWorkspaceJob,
  deleteWorkspaceJob,
} from "../coalesce/api/jobs.js";
import {
  PaginationParams,
  buildJsonToolResponse,
  handleToolError,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";

export function registerJobTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.tool(
    "list-jobs",
    "List all jobs in a Coalesce environment. For workspace-scoped listing, use list-workspace-jobs instead.",
    PaginationParams.extend({
      environmentID: z.string().describe("The environment ID"),
    }).shape,
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listEnvironmentJobs(client, params);
        return buildJsonToolResponse("list-jobs", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "list-workspace-jobs",
    "List all jobs in a Coalesce workspace. Use this to discover job IDs for update-workspace-job or delete-workspace-job.",
    PaginationParams.extend({
      workspaceID: z.string().describe("The workspace ID"),
    }).shape,
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listWorkspaceJobs(client, params);
        return buildJsonToolResponse("list-workspace-jobs", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "create-workspace-job",
    "Create a job in a Coalesce workspace. A job defines which nodes to include/exclude when running.\n\nThe includeSelector and excludeSelector use the format:\n  { location: LOCATION_NAME name: NODE_NAME } OR { location: LOCATION_NAME name: NODE_NAME }\n\nUse locationName from node details (e.g., 'ETL_STAGE', 'ANALYTICS'), NOT database.schema.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      name: z.string().describe("Name for the job"),
      includeSelector: z
        .string()
        .describe("Node selector string. Format: '{ location: LOC name: NAME } OR { location: LOC name: NAME }'. Use empty string to include nothing."),
      excludeSelector: z
        .string()
        .describe("Node exclusion selector string. Same format as includeSelector. Use empty string to exclude nothing."),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await createWorkspaceJob(client, params);
        return buildJsonToolResponse("create-workspace-job", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-job",
    "Get details of a specific job. Jobs are read via the environment endpoint.",
    {
      environmentID: z.string().describe("The environment ID"),
      jobID: z.string().describe("The job ID"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getEnvironmentJob(client, params);
        return buildJsonToolResponse("get-job", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "update-workspace-job",
    "Update a job in a Coalesce workspace. Replaces the job's name, includeSelector, and excludeSelector.\n\nThe includeSelector and excludeSelector use the format:\n  { location: LOCATION_NAME name: NODE_NAME } OR { location: LOCATION_NAME name: NODE_NAME }\n\nUse locationName from node details (e.g., 'ETL_STAGE', 'ANALYTICS'), NOT database.schema.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      jobID: z.string().describe("The job ID to update"),
      name: z.string().describe("Updated name for the job"),
      includeSelector: z
        .string()
        .describe("Node selector string. Format: '{ location: LOC name: NAME } OR { location: LOC name: NAME }'."),
      excludeSelector: z
        .string()
        .describe("Node exclusion selector string. Same format as includeSelector. Use empty string to exclude nothing."),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await updateWorkspaceJob(client, params);
        return buildJsonToolResponse("update-workspace-job", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "delete-workspace-job",
    "Delete a job from a Coalesce workspace. This is a destructive operation — the job definition will be permanently removed.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      jobID: z.string().describe("The job ID to delete"),
    },
    DESTRUCTIVE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await deleteWorkspaceJob(client, params);
        return buildJsonToolResponse("delete-workspace-job", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
