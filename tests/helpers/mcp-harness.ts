import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { createCoalesceMcpServer, SERVER_NAME } from "../../src/server.js";

export class LoopbackTransport implements Transport {
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
  const server = createCoalesceMcpServer(apiClient as never);

  const serverTransport = new LoopbackTransport();
  const clientTransport = new LoopbackTransport();
  serverTransport.peer = clientTransport;
  clientTransport.peer = serverTransport;

  const client = new Client(
    {
      name: `${SERVER_NAME}-test-client`,
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
