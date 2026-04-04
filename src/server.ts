import { createRequire } from "node:module";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "./client.js";
import { registerEnvironmentTools } from "./mcp/environments.js";
import { registerNodeTools } from "./mcp/nodes.js";
import { registerPipelineTools } from "./mcp/pipelines.js";
import { registerRunTools } from "./mcp/runs.js";
import { registerProjectTools } from "./mcp/projects.js";
import { registerGitAccountTools } from "./mcp/git-accounts.js";
import { registerUserTools } from "./mcp/users.js";
import { registerNodeTypeCorpusTools } from "./mcp/node-type-corpus.js";
import { registerRepoNodeTypeTools } from "./mcp/repo-node-types.js";
import { registerJobTools } from "./mcp/jobs.js";
import { registerSubgraphTools } from "./mcp/subgraphs.js";
import { registerWorkspaceTools } from "./mcp/workspaces.js";
import { registerCacheTools } from "./mcp/cache.js";
import { registerWorkshopTools } from "./mcp/workshop.js";
import { registerLineageTools } from "./mcp/lineage.js";
import { registerSkillTools } from "./mcp/skills.js";

import { registerGetRunDetails } from "./workflows/get-run-details.js";
import { registerGetEnvironmentOverview } from "./workflows/get-environment-overview.js";
import { registerGetEnvironmentHealth } from "./workflows/get-environment-health.js";
import { registerResources } from "./resources/index.js";
import { registerPrompts } from "./prompts/index.js";
import {
  registerTaskTools,
  InMemoryTaskStore,
  InMemoryTaskMessageQueue,
} from "./tasks/index.js";

const require = createRequire(import.meta.url);
const { version } = require("../package.json") as { version: string };

// Tools that write to local disk only (not the Coalesce API) — safe in read-only mode.
const LOCAL_ONLY_WRITE_TOOLS = new Set([
  "cache_workspace_nodes",
  "cache_environment_nodes",
  "cache_runs",
  "cache_org_users",
  "personalize_skills",
]);

export function isReadOnlyMode(): boolean {
  return process.env.COALESCE_MCP_READ_ONLY === "true";
}

function isWriteTool(
  name: string,
  metadata: { annotations?: { readOnlyHint?: boolean } }
): boolean {
  if (metadata.annotations?.readOnlyHint === true) return false;
  if (LOCAL_ONLY_WRITE_TOOLS.has(name)) return false;
  return true;
}

function applyReadOnlyFilter(server: McpServer): void {
  const originalRegisterTool = server.registerTool.bind(server);
  server.registerTool = ((name: string, metadata: any, handler: any) => {
    if (isWriteTool(name, metadata)) return;
    originalRegisterTool(name, metadata, handler);
  }) as typeof server.registerTool;

  const originalRegisterToolTask =
    server.experimental.tasks.registerToolTask.bind(
      server.experimental.tasks
    );
  server.experimental.tasks.registerToolTask = ((
    name: string,
    metadata: any,
    impl: any
  ) => {
    if (isWriteTool(name, metadata)) return;
    originalRegisterToolTask(name, metadata, impl);
  }) as typeof server.experimental.tasks.registerToolTask;
}

export const SERVER_NAME = "coalesce-transform-mcp";
export const SERVER_VERSION = version;
export const SERVER_INSTRUCTIONS = `\
This server manages Coalesce node definitions, pipelines, and workspace configuration — not live warehouse data. \
For data questions (tables, schemas, row counts, sample data), use a Snowflake-capable tool if available.

TOOL CATEGORIES:

Discovery (start here for any task):
  list_workspaces, list_environments, list_projects — resolve IDs before any operation
  list_workspace_nodes, list_environment_nodes — browse node inventories
  get_workspace_node, get_environment_node — full node detail by ID
  analyze_workspace_patterns — compact workspace profile for large workspaces
  list_environment_jobs — resolve job IDs for runs

Pipeline building (always plan first):
  plan_pipeline → create_pipeline_from_plan or create_pipeline_from_sql
  create_workspace_node_from_predecessor — create individual nodes with predecessors
  create_workspace_node_from_scratch — create nodes with no upstream
  create_node_from_external_schema — match columns to an external table schema
  RULE: Never skip plan_pipeline. Never guess node types.

Node editing:
  update_workspace_node, set_workspace_node — modify node body fields
  replace_workspace_node_columns — replace full column set
  convert_join_to_aggregation, apply_join_condition — join operations
  complete_node_configuration — auto-fill config from repo node type definition

Execution:
  run_and_wait, retry_and_wait — preferred: end-to-end run outcome in one call
  start_run, run_status, cancel_run — manual run lifecycle
  get_run_details — run metadata plus results
  get_environment_overview, get_environment_health — environment-level status
  diagnose_run_failure — root-cause analysis for failed runs

Node type discovery:
  list_workspace_node_types — types currently in workspace
  search_node_type_variants — broader type search
  list_repo_node_types, get_repo_node_type_definition — repo-backed type inspection
  generate_set_workspace_node_template — scaffold node body from type definition

Lineage:
  get_upstream_nodes, get_downstream_nodes — graph traversal
  analyze_impact — downstream impact counts for a node or column change
  propagate_column_change — apply column changes downstream (destructive, confirm first)
  NOTE: First lineage call per workspace is slow (builds full cache).

Caching (for large datasets):
  cache_workspace_nodes, cache_environment_nodes, cache_runs, cache_org_users
  Results returned as coalesce://cache/... resource URIs — read the resource, not inline JSON.

Users and admin:
  list_org_users — user discovery
  list_git_accounts — git integration

Customization:
  personalize_skills — export bundled skill files to a local directory for customization

TYPICAL WORKFLOWS:

Explore a workspace: list_workspaces → list_workspace_nodes → get_workspace_node
Build a pipeline: plan_pipeline → (user approval) → create_pipeline_from_plan → verify each node
Edit a node: get_workspace_node → update_workspace_node or replace_workspace_node_columns → verify
Run a job: list_environments → list_environment_jobs → run_and_wait → inspect results
Diagnose a failure: list_runs or get_run_details → diagnose_run_failure

RULES:
- Resolve IDs before mutating (list_workspaces, list_environments, list_environment_jobs)
- Always plan_pipeline before creating pipeline nodes; wait for explicit user approval
- Inspect warning, validation, resultsError, incomplete, timedOut, and cleanupFailures fields before continuing
- Read coalesce://context/* resources for deep guidance on specific topics`;

export function registerServerSurface(server: McpServer, client: CoalesceClient): void {
  if (isReadOnlyMode()) applyReadOnlyFilter(server);

  registerEnvironmentTools(server, client);
  registerNodeTools(server, client);
  registerPipelineTools(server, client);
  registerRunTools(server, client, { skipStartRun: true });
  registerProjectTools(server, client);
  registerGitAccountTools(server, client);
  registerUserTools(server, client);
  registerNodeTypeCorpusTools(server, client);
  registerRepoNodeTypeTools(server, client);
  registerJobTools(server, client);
  registerSubgraphTools(server, client);
  registerWorkspaceTools(server, client);
  registerCacheTools(server, client);
  registerWorkshopTools(server, client);
  registerLineageTools(server, client);
  registerSkillTools(server, client);

  registerGetRunDetails(server, client);
  registerGetEnvironmentOverview(server, client);
  registerGetEnvironmentHealth(server, client);
  registerTaskTools(server, client);
  registerResources(server);
  registerPrompts(server);
}

export function createCoalesceMcpServer(client: CoalesceClient): McpServer {
  const taskStore = new InMemoryTaskStore();
  const taskMessageQueue = new InMemoryTaskMessageQueue();

  const server = new McpServer(
    {
      name: SERVER_NAME,
      version: SERVER_VERSION,
    },
    {
      instructions: SERVER_INSTRUCTIONS,
      capabilities: {
        tasks: {
          requests: {
            tools: {
              call: {},
            },
          },
        },
      },
      taskStore,
      taskMessageQueue,
    }
  );

  registerServerSurface(server, client);
  return server;
}
