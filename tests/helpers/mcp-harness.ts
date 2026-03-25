import { createRequire } from "node:module";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { registerEnvironmentTools } from "../../src/mcp/environments.js";
import { registerNodeTools } from "../../src/mcp/nodes.js";
import { registerPipelineTools } from "../../src/mcp/pipelines.js";
import { registerRunTools } from "../../src/mcp/runs.js";
import { registerProjectTools } from "../../src/mcp/projects.js";
import { registerGitAccountTools } from "../../src/mcp/git-accounts.js";
import { registerUserTools } from "../../src/mcp/users.js";
import { registerNodeTypeCorpusTools } from "../../src/mcp/node-type-corpus.js";
import { registerRepoNodeTypeTools } from "../../src/mcp/repo-node-types.js";
import { registerJobTools } from "../../src/mcp/jobs.js";
import { registerSubgraphTools } from "../../src/mcp/subgraphs.js";
import { registerCacheTools } from "../../src/mcp/cache.js";
import { registerRunAndWait } from "../../src/workflows/run-and-wait.js";
import { registerRetryAndWait } from "../../src/workflows/retry-and-wait.js";
import { registerGetRunDetails } from "../../src/workflows/get-run-details.js";
import { registerGetEnvironmentOverview } from "../../src/workflows/get-environment-overview.js";
import { registerResources } from "../../src/resources/index.js";

const require = createRequire(import.meta.url);
const { version } = require("../../package.json") as { version: string };

class LoopbackTransport implements Transport {
  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: Transport["onmessage"];
  sessionId?: string;
  setProtocolVersion?: (version: string) => void;
  peer?: LoopbackTransport;

  constructor(sessionId?: string) {
    this.sessionId = sessionId;
  }

  async start(): Promise<void> {
    // No-op; both peers already exist in-memory.
  }

  async send(message: Parameters<NonNullable<Transport["onmessage"]>>[0]): Promise<void> {
    const peer = this.peer;
    if (!peer) {
      throw new Error("Loopback transport peer is not connected");
    }

    queueMicrotask(() => {
      try {
        peer.onmessage?.(structuredClone(message));
      } catch (error) {
        const normalized = error instanceof Error ? error : new Error(String(error));
        this.onerror?.(normalized);
        peer.onerror?.(normalized);
      }
    });
  }

  async close(): Promise<void> {
    this.onclose?.();
  }
}

export function createMockApiClient(overrides: Partial<{
  get: (...args: unknown[]) => Promise<unknown>;
  post: (...args: unknown[]) => Promise<unknown>;
  put: (...args: unknown[]) => Promise<unknown>;
  patch: (...args: unknown[]) => Promise<unknown>;
  delete: (...args: unknown[]) => Promise<unknown>;
}> = {}) {
  return {
    get: async () => ({ data: [] }),
    post: async () => ({ ok: true }),
    put: async () => ({ ok: true }),
    patch: async () => ({ ok: true }),
    delete: async () => ({ ok: true }),
    ...overrides,
  };
}

export async function createConnectedMcpHarness(apiClient: ReturnType<typeof createMockApiClient>) {
  const server = new McpServer({
    name: "coalesce-transform-mcp",
    version,
  });

  registerEnvironmentTools(server, apiClient as never);
  registerNodeTools(server, apiClient as never);
  registerPipelineTools(server, apiClient as never);
  registerRunTools(server, apiClient as never);
  registerProjectTools(server, apiClient as never);
  registerGitAccountTools(server, apiClient as never);
  registerUserTools(server, apiClient as never);
  registerNodeTypeCorpusTools(server, apiClient as never);
  registerRepoNodeTypeTools(server, apiClient as never);
  registerJobTools(server, apiClient as never);
  registerSubgraphTools(server, apiClient as never);
  registerCacheTools(server, apiClient as never);
  registerRunAndWait(server, apiClient as never);
  registerRetryAndWait(server, apiClient as never);
  registerGetRunDetails(server, apiClient as never);
  registerGetEnvironmentOverview(server, apiClient as never);
  registerResources(server);

  const serverTransport = new LoopbackTransport();
  const clientTransport = new LoopbackTransport();
  serverTransport.peer = clientTransport;
  clientTransport.peer = serverTransport;

  const client = new Client(
    {
      name: "coalesce-transform-mcp-test-client",
      version: "0.0.1",
    },
    { capabilities: {} }
  );

  await server.connect(serverTransport);
  await client.connect(clientTransport);

  return {
    client,
    server,
    async close() {
      await Promise.allSettled([client.close(), server.close()]);
    },
  };
}
