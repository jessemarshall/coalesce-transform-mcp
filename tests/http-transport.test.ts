import { afterEach, describe, expect, it, vi } from "vitest";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { startCoalesceHttpServer } from "../src/http.js";
import { createMockApiClient } from "./helpers/mcp-harness.js";

const disposers: Array<() => Promise<void>> = [];

afterEach(async () => {
  while (disposers.length > 0) {
    const dispose = disposers.pop();
    if (!dispose) {
      continue;
    }
    await dispose();
  }
});

describe("MCP Streamable HTTP Transport", () => {
  it("serves instructions, prompts, resources, and tools over HTTP", async () => {
    const server = await startCoalesceHttpServer(
      createMockApiClient({
        get: vi.fn(async (path: unknown) => {
          if (path === "/api/v1/runs") {
            return {
              data: [{ id: "run-1", status: "completed" }],
            };
          }
          return { data: [] };
        }),
      }) as never,
      {
        host: "127.0.0.1",
        port: 0,
        path: "/mcp",
      }
    );
    disposers.push(async () => server.close());

    const client = new Client(
      {
        name: "coalesce-transform-mcp-http-test-client",
        version: "0.0.1",
      },
      { capabilities: {} }
    );
    const transport = new StreamableHTTPClientTransport(
      new URL(`http://${server.host}:${server.port}${server.path}`)
    );
    disposers.push(async () => {
      await Promise.allSettled([client.close(), transport.close()]);
    });

    await client.connect(transport);

    expect(client.getInstructions()).toContain("Resolve IDs before mutating");

    const prompts = await client.listPrompts();
    expect(prompts.prompts.map((prompt) => prompt.name)).toContain(
      "coalesce-start-here"
    );

    const startHerePrompt = await client.getPrompt({
      name: "coalesce-start-here",
    });
    expect(startHerePrompt.messages).toEqual([
      expect.objectContaining({
        role: "user",
        content: expect.objectContaining({
          type: "text",
          text: expect.stringContaining("Start with discovery before mutation"),
        }),
      }),
    ]);

    const resources = await client.listResources();
    expect(resources.resources).toContainEqual(
      expect.objectContaining({
        uri: "coalesce://context/overview",
      })
    );

    const listRuns = await client.callTool({
      name: "list-runs",
      arguments: {},
    });
    expect(listRuns.isError).toBeUndefined();
    expect(listRuns.structuredContent).toEqual({
      data: [{ id: "run-1", status: "completed" }],
    });
  });
});
