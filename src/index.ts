#!/usr/bin/env node
import { createRequire } from "node:module";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { validateConfig, createClient } from "./client.js";
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
import { registerCacheTools } from "./mcp/cache.js";
import { registerRunAndWait } from "./workflows/run-and-wait.js";
import { registerRetryAndWait } from "./workflows/retry-and-wait.js";
import { registerGetRunDetails } from "./workflows/get-run-details.js";
import { registerGetEnvironmentOverview } from "./workflows/get-environment-overview.js";
import { registerResources } from "./resources/index.js";

const require = createRequire(import.meta.url);
const { version } = require("../package.json") as { version: string };

const config = validateConfig();
const client = createClient(config);

const server = new McpServer({
  name: "coalesce-transform-mcp",
  version,
});

// Register all tools
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
registerCacheTools(server, client);
registerRunAndWait(server, client);
registerRetryAndWait(server, client);
registerGetRunDetails(server, client);
registerGetEnvironmentOverview(server, client);

// Register resources
registerResources(server);

// Start server
const transport = new StdioServerTransport();
await server.connect(transport);
