import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listEnvironmentJobs,
  createWorkspaceJob,
  getEnvironmentJob,
  getWorkspaceJob,
  updateWorkspaceJob,
  deleteWorkspaceJob,
} from "../coalesce/api/jobs.js";
import { listJobNodes } from "../services/jobs/resolve.js";
import {
  PaginationParams,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool, defineDestructiveTool, extractEntityName } from "./tool-helpers.js";

export function defineJobTools(
  server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  defineSimpleTool(client, "list_job_nodes", {
    title: "List Job Nodes (grouped by subgraph)",
    description:
      "Resolve a workspace job's selectors into concrete workspace nodes, grouped by subgraph. Composes getWorkspaceJob + listWorkspaceNodes + the local repo's subgraphs/ folder to evaluate the includeSelector/excludeSelector DSL (supported clauses: { subgraph: NAME } and { location: LOC name: NAME }, joined by OR).\n\nSubgraph resolution note: the public Coalesce API has no subgraph list endpoint. `{ subgraph: NAME }` terms can only be resolved when `repoPath` is set (or COALESCE_REPO_PATH). Without a repo, such terms land in `unresolved` and a warning is added to `summary.warnings`.\n\nArgs:\n  - workspaceID (string, required): The workspace that owns the job\n  - jobID (string, optional): The job ID. Preferred when known.\n  - jobName (string, optional): The job name. Resolved against workspace jobs when jobID is absent.\n  - repoPath (string, optional): Coalesce repo path for subgraph YAML lookup. Falls back to COALESCE_REPO_PATH or the coa profile.\n\nReturns:\n  {\n    job: { id, name, includeSelector, excludeSelector },\n    summary: { totalNodes, subgraphCount, unattachedCount, unresolvedCount, warnings },\n    nodesBySubgraph: [{ subgraphID, subgraphName, nodes: [{ id, name, location, nodeType }] }],\n    unattached: [...],                       // nodes matched by the job but not in any subgraph\n    unresolved: [{ term, reason }]           // selector terms that matched nothing (stale selectors or missing repoPath)\n  }",
    inputSchema: z
      .object({
        workspaceID: z.string().min(1, "workspaceID must not be empty").describe("The workspace that owns the job"),
        jobID: z.string().min(1, "jobID must not be empty when provided").optional().describe("The job ID. Preferred when known."),
        jobName: z
          .string()
          .min(1, "jobName must not be empty when provided")
          .optional()
          .describe("The job name — resolved via listWorkspaceJobs when jobID is absent."),
        repoPath: z
          .string()
          .optional()
          .describe(
            "Optional Coalesce repo path for subgraph YAML lookup. Required to resolve `{ subgraph: NAME }` selector terms. Falls back to COALESCE_REPO_PATH or the coa profile."
          ),
      })
      .refine((v) => Boolean(v.jobID) || Boolean(v.jobName), {
        message: "Either jobID or jobName is required",
      }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, async (client, params) => listJobNodes(client, params)),

  defineSimpleTool(client, "list_environment_jobs", {
    title: "List Environment Jobs",
    description:
      "List all jobs deployed to a Coalesce environment. Jobs define which nodes run together — pair this with list_environment_nodes when planning a refresh, or with create_workspace_job when authoring new schedules.\n\nDifferent from listing workspace-side jobs: this returns the jobs *currently deployed* to the target environment (what the scheduler will execute), not the in-flight workspace edits that may not be deployed yet.\n\nArgs:\n  - environmentID (string, required): The environment ID\n  - limit, startingFrom, orderBy, orderByDirection: Pagination\n\nReturns:\n  { data: Job[], next?: string, total?: number }",
    inputSchema: PaginationParams.extend({
      environmentID: z.string().min(1, "environmentID must not be empty").describe("The environment ID"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, listEnvironmentJobs),

  defineSimpleTool(client, "create_workspace_job", {
    title: "Create Workspace Job",
    description:
      "Create a new job in a Coalesce workspace. Jobs define which nodes to run together.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - name (string, required): Job name\n  - includeSelector (string, required): Node selector. Format: '{ location: LOC name: NAME } OR { location: LOC name: NAME }'. Use empty string to include nothing\n  - excludeSelector (string, required): Node exclusion selector. Same format as includeSelector. Use empty string to exclude nothing\n\nReturns:\n  Created job with assigned ID.",
    inputSchema: z.object({
      workspaceID: z.string().min(1, "workspaceID must not be empty").describe("The workspace ID"),
      name: z.string().min(1, "name must not be empty").describe("Name for the job"),
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
      environmentID: z.string().min(1, "environmentID must not be empty").describe("The environment ID"),
      jobID: z.string().min(1, "jobID must not be empty").describe("The job ID"),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, getEnvironmentJob),

  defineSimpleTool(client, "update_workspace_job", {
    title: "Update Workspace Job",
    description:
      "Update an existing workspace job.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - jobID (string, required): The job ID\n  - name (string, required): Updated job name\n  - includeSelector (string, required): Node selector. Format: '{ location: LOC name: NAME } OR { location: LOC name: NAME }'\n  - excludeSelector (string, required): Node exclusion selector. Same format as includeSelector\n\nReturns:\n  Updated job object.",
    inputSchema: z.object({
      workspaceID: z.string().min(1, "workspaceID must not be empty").describe("The workspace ID"),
      jobID: z.string().min(1, "jobID must not be empty").describe("The job ID to update"),
      name: z.string().min(1, "name must not be empty").describe("Updated name for the job"),
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
      workspaceID: z.string().min(1, "workspaceID must not be empty").describe("The workspace ID"),
      jobID: z.string().min(1, "jobID must not be empty").describe("The job ID to delete"),
      confirmed: z
        .boolean()
        .optional()
        .describe("Set to true after the user explicitly confirms the deletion."),
    }),
    annotations: DESTRUCTIVE_ANNOTATIONS,
    resolve: async (client, params) => {
      const job = await getWorkspaceJob(client, {
        workspaceID: params.workspaceID,
        jobID: params.jobID,
      });
      return {
        primary: {
          type: "workspace_job",
          id: params.jobID,
          name: extractEntityName(job),
        },
        context: { workspaceID: params.workspaceID },
      };
    },
    confirmMessage: (params, preview) => {
      const label = preview?.primary.name
        ? `"${preview.primary.name}" (${params.jobID})`
        : `"${params.jobID}"`;
      return `This will permanently delete job ${label} from workspace "${params.workspaceID}". This cannot be undone.`;
    },
  }, deleteWorkspaceJob),
  ];
}
