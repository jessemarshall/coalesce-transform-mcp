#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type Server as HttpServer,
  type ServerResponse,
} from "node:http";
import { fileURLToPath } from "node:url";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { createClient, validateConfig, type CoalesceClient } from "./client.js";
import { createCoalesceMcpServer } from "./server.js";

type RequestHandler = (
  req: IncomingMessage,
  res: ServerResponse
) => Promise<void> | void;

type HttpSession = {
  server: ReturnType<typeof createCoalesceMcpServer>;
  transport: StreamableHTTPServerTransport;
};

export type CoalesceHttpServerOptions = {
  host?: string;
  port?: number;
  path?: string;
  allowedHosts?: string[];
};

export type StartedCoalesceHttpServer = {
  host: string;
  port: number;
  path: string;
  server: HttpServer;
  close: () => Promise<void>;
};

function getHttpHost(options?: CoalesceHttpServerOptions): string {
  return options?.host ?? process.env.COALESCE_MCP_HTTP_HOST ?? "127.0.0.1";
}

function getHttpPort(options?: CoalesceHttpServerOptions): number {
  const raw = options?.port ?? process.env.COALESCE_MCP_HTTP_PORT;
  if (typeof raw === "number") {
    return raw;
  }
  if (typeof raw === "string" && raw.length > 0) {
    const parsed = Number.parseInt(raw, 10);
    if (Number.isFinite(parsed) && parsed >= 0) {
      return parsed;
    }
  }
  return 3333;
}

function getHttpPath(options?: CoalesceHttpServerOptions): string {
  const raw = options?.path ?? process.env.COALESCE_MCP_HTTP_PATH ?? "/mcp";
  return raw.startsWith("/") ? raw : `/${raw}`;
}

function getAllowedHosts(options?: CoalesceHttpServerOptions): string[] | undefined {
  if (options?.allowedHosts) {
    return options.allowedHosts;
  }
  const raw = process.env.COALESCE_MCP_HTTP_ALLOWED_HOSTS;
  if (!raw) {
    return undefined;
  }
  const hosts = raw
    .split(",")
    .map((host) => host.trim())
    .filter((host) => host.length > 0);
  return hosts.length > 0 ? hosts : undefined;
}

function getHeaderValue(header: string | string[] | undefined): string | undefined {
  return Array.isArray(header) ? header[0] : header;
}

function hostHeaderAllowed(
  req: IncomingMessage,
  host: string,
  allowedHosts?: string[]
): boolean {
  const header = getHeaderValue(req.headers.host);
  if (!header) {
    return true;
  }

  const actualHost = header.toLowerCase();
  const accepted = new Set<string>(
    (allowedHosts ?? [])
      .map((entry) => entry.toLowerCase())
      .concat(
        host === "127.0.0.1" || host === "localhost" || host === "::1"
          ? ["127.0.0.1", "localhost", "[::1]"]
          : []
      )
  );

  if (accepted.size === 0) {
    return true;
  }

  return Array.from(accepted).some((candidate) => {
    return actualHost === candidate || actualHost.startsWith(`${candidate}:`);
  });
}

function writeJson(res: ServerResponse, status: number, body: unknown): void {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(body));
}

async function readJsonBody(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  if (chunks.length === 0) {
    return undefined;
  }

  const body = Buffer.concat(chunks).toString("utf8");
  if (body.trim().length === 0) {
    return undefined;
  }

  return JSON.parse(body);
}

function createMethodNotAllowedHandler(allowed: string): RequestHandler {
  return (_req, res) => {
    res.statusCode = 405;
    res.setHeader("Allow", allowed);
    res.end("Method Not Allowed");
  };
}

export function createCoalesceHttpRequestHandler(
  client: CoalesceClient,
  options: CoalesceHttpServerOptions = {}
): {
  host: string;
  path: string;
  handler: RequestHandler;
  close: () => Promise<void>;
} {
  const host = getHttpHost(options);
  const path = getHttpPath(options);
  const allowedHosts = getAllowedHosts(options);
  const sessions = new Map<string, HttpSession>();

  async function closeSession(sessionId: string): Promise<void> {
    const session = sessions.get(sessionId);
    if (!session) {
      return;
    }
    sessions.delete(sessionId);
    await Promise.allSettled([session.transport.close(), session.server.close()]);
  }

  async function close(): Promise<void> {
    await Promise.allSettled(Array.from(sessions.keys()).map((sessionId) => closeSession(sessionId)));
  }

  const postHandler: RequestHandler = async (req, res) => {
    let parsedBody: unknown;
    try {
      parsedBody = await readJsonBody(req);
    } catch {
      writeJson(res, 400, {
        jsonrpc: "2.0",
        error: {
          code: -32700,
          message: "Invalid JSON request body",
        },
        id: null,
      });
      return;
    }

    const sessionId = getHeaderValue(req.headers["mcp-session-id"]);

    try {
      if (sessionId && sessions.has(sessionId)) {
        await sessions.get(sessionId)!.transport.handleRequest(req, res, parsedBody);
        return;
      }

      if (!sessionId && isInitializeRequest(parsedBody)) {
        const server = createCoalesceMcpServer(client);
        const transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          onsessioninitialized: (newSessionId) => {
            sessions.set(newSessionId, { server, transport });
          },
        });

        transport.onclose = () => {
          const activeSessionId = transport.sessionId;
          if (!activeSessionId) {
            return;
          }
          void closeSession(activeSessionId);
        };

        await server.connect(transport);
        await transport.handleRequest(req, res, parsedBody);
        return;
      }

      writeJson(res, 400, {
        jsonrpc: "2.0",
        error: {
          code: -32000,
          message: "Bad Request: No valid session ID provided",
        },
        id: null,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Internal server error";
      if (!res.headersSent) {
        writeJson(res, 500, {
          jsonrpc: "2.0",
          error: {
            code: -32603,
            message,
          },
          id: null,
        });
      }
    }
  };

  const getHandler: RequestHandler = async (req, res) => {
    const sessionId = getHeaderValue(req.headers["mcp-session-id"]);
    if (!sessionId || !sessions.has(sessionId)) {
      res.statusCode = 400;
      res.end("Invalid or missing session ID");
      return;
    }

    await sessions.get(sessionId)!.transport.handleRequest(req, res);
  };

  const deleteHandler: RequestHandler = async (req, res) => {
    const sessionId = getHeaderValue(req.headers["mcp-session-id"]);
    if (!sessionId || !sessions.has(sessionId)) {
      res.statusCode = 400;
      res.end("Invalid or missing session ID");
      return;
    }

    await sessions.get(sessionId)!.transport.handleRequest(req, res);
  };

  const methodNotAllowed = {
    GET: createMethodNotAllowedHandler("POST"),
    POST: createMethodNotAllowedHandler("GET, POST, DELETE"),
    DELETE: createMethodNotAllowedHandler("POST"),
  };

  const handler: RequestHandler = async (req, res) => {
    if (!hostHeaderAllowed(req, host, allowedHosts)) {
      res.statusCode = 403;
      res.end("Forbidden host header");
      return;
    }

    const requestUrl = new URL(req.url ?? "/", `http://${getHeaderValue(req.headers.host) ?? host}`);
    if (requestUrl.pathname !== path) {
      res.statusCode = 404;
      res.end("Not Found");
      return;
    }

    if (req.method === "POST") {
      await postHandler(req, res);
      return;
    }
    if (req.method === "GET") {
      await getHandler(req, res);
      return;
    }
    if (req.method === "DELETE") {
      await deleteHandler(req, res);
      return;
    }

    const deny =
      req.method === "HEAD" || req.method === "OPTIONS"
        ? createMethodNotAllowedHandler("GET, POST, DELETE")
        : methodNotAllowed.POST;
    await deny(req, res);
  };

  return {
    host,
    path,
    handler,
    close,
  };
}

export async function startCoalesceHttpServer(
  client: CoalesceClient,
  options: CoalesceHttpServerOptions = {}
): Promise<StartedCoalesceHttpServer> {
  const host = getHttpHost(options);
  const port = getHttpPort(options);
  const { path, handler, close } = createCoalesceHttpRequestHandler(client, {
    ...options,
    host,
  });

  const server = createServer((req, res) => {
    void handler(req, res);
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => {
      server.off("error", reject);
      resolve();
    });
  });

  return {
    host,
    port: (server.address() as { port: number }).port,
    path,
    server,
    async close() {
      await close();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }
          resolve();
        });
      });
    },
  };
}

async function main(): Promise<void> {
  const client = createClient(validateConfig());
  const server = await startCoalesceHttpServer(client);
  const allowedHosts = getAllowedHosts();

  if ((server.host === "0.0.0.0" || server.host === "::") && !allowedHosts) {
    // eslint-disable-next-line no-console
    console.warn(
      "Warning: HTTP transport is bound to all interfaces without COALESCE_MCP_HTTP_ALLOWED_HOSTS."
    );
  }

  process.on("SIGINT", () => {
    void server.close().finally(() => process.exit(0));
  });
  process.on("SIGTERM", () => {
    void server.close().finally(() => process.exit(0));
  });

  // eslint-disable-next-line no-console
  console.log(`Coalesce MCP Streamable HTTP server listening on http://${server.host}:${server.port}${server.path}`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void main().catch((error) => {
    // eslint-disable-next-line no-console
    console.error(error);
    process.exit(1);
  });
}
