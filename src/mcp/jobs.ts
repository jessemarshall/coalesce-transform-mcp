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
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../coalesce/types.js";
import { requireDestructiveConfirmation } from "../services/shared/elicitation.js";

export function registerJobTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "list_environment_jobs",
    {
      title: "List Environment Jobs",
      description:
        "List all jobs in a Coalesce environment. Jobs define which nodes to run together. Discovers jobs by scanning sequential IDs.\n\nArgs:\n  - environmentID (string, required): The environment ID\n  - limit (number, optional): Maximum number of jobs to return\n\nReturns:\n  { data: Job[] }",
      inputSchema: z.object({
        environmentID: z.string().describe("The environment ID"),
        limit: z.number().optional().describe("Maximum number of jobs to return"),
      }),
      outputSchema: getToolOutputSchema("list_environment_jobs"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listEnvironmentJobs(client, params);
        return buildJsonToolResponse("list_environment_jobs", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "create_workspace_job",
    {
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
      outputSchema: getToolOutputSchema("create_workspace_job"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await createWorkspaceJob(client, params);
        return buildJsonToolResponse("create_workspace_job", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_environment_job",
    {
      title: "Get Environment Job",
      description:
        "Get details of a specific job in an environment.\n\nArgs:\n  - environmentID (string, required): The environment ID\n  - jobID (string, required): The job ID\n\nReturns:\n  Job object with name, node list, schedule, and configuration.",
      inputSchema: z.object({
        environmentID: z.string().describe("The environment ID"),
        jobID: z.string().describe("The job ID"),
      }),
      outputSchema: getToolOutputSchema("get_environment_job"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getEnvironmentJob(client, params);
        return buildJsonToolResponse("get_environment_job", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "update_workspace_job",
    {
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
      outputSchema: getToolOutputSchema("update_workspace_job"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await updateWorkspaceJob(client, params);
        return buildJsonToolResponse("update_workspace_job", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "delete_workspace_job",
    {
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
      outputSchema: getToolOutputSchema("delete_workspace_job"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const approvalResponse = await requireDestructiveConfirmation(
          server,
          "delete_workspace_job",
          `This will permanently delete job "${params.jobID}" from workspace "${params.workspaceID}". This cannot be undone.`,
          params.confirmed,
        );
        if (approvalResponse) return approvalResponse;

        const result = await deleteWorkspaceJob(client, params);
        return buildJsonToolResponse("delete_workspace_job", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
