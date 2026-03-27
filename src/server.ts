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
import { registerRunAndWait } from "./workflows/run-and-wait.js";
import { registerRetryAndWait } from "./workflows/retry-and-wait.js";
import { registerGetRunDetails } from "./workflows/get-run-details.js";
import { registerGetEnvironmentOverview } from "./workflows/get-environment-overview.js";
import { registerResources } from "./resources/index.js";
import { registerPrompts } from "./prompts/index.js";
import { ensureJsonToolOutputSchemas } from "./coalesce/types.js";

const require = createRequire(import.meta.url);
const { version } = require("../package.json") as { version: string };

export const SERVER_NAME = "coalesce-transform-mcp";
export const SERVER_VERSION = version;
export const SERVER_INSTRUCTIONS = [
  "Resolve IDs before mutating projects, workspaces, environments, jobs, runs, or nodes.",
  "Always use plan-pipeline before creating pipeline nodes, and wait for explicit user approval before calling creation tools.",
  "Inspect warning, validation, resultsError, incomplete, timedOut, and cleanupFailures fields before continuing.",
  "Prefer run-and-wait or retry-and-wait when the user wants an end-to-end run outcome in one call.",
  "Large payloads may be exposed through coalesce://cache resource URIs; read the resource rather than assuming inline JSON.",
].join("\n");

export function registerServerSurface(server: McpServer, client: CoalesceClient): void {
  ensureJsonToolOutputSchemas(server);
  registerEnvironmentTools(server, client);
  registerNodeTools(server, client);
  registerPipelineTools(server, client);
  registerRunTools(server, client);
  registerProjectTools(server, client);
  registerGitAccountTools(server, client);
  registerUserTools(server, client);
  registerNodeTypeCorpusTools(server, client);
  registerRepoNodeTypeTools(server, client);
  registerJobTools(server, client);
  registerSubgraphTools(server, client);
  registerWorkspaceTools(server, client);
  registerCacheTools(server, client);
  registerRunAndWait(server, client);
  registerRetryAndWait(server, client);
  registerGetRunDetails(server, client);
  registerGetEnvironmentOverview(server, client);
  registerResources(server);
  registerPrompts(server);
}

export function createCoalesceMcpServer(client: CoalesceClient): McpServer {
  const server = new McpServer(
    {
      name: SERVER_NAME,
      version: SERVER_VERSION,
    },
    {
      instructions: SERVER_INSTRUCTIONS,
    }
  );

  registerServerSurface(server, client);
  return server;
}
