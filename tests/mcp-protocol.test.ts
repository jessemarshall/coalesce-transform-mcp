import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { buildCacheResourceUri } from "../src/cache-dir.js";
import {
  createConnectedMcpHarness,
  createMockApiClient,
} from "./helpers/mcp-harness.js";

const tempDirs: string[] = [];

afterEach(() => {
  vi.restoreAllMocks();
  for (const directory of tempDirs.splice(0, tempDirs.length)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("MCP Protocol Surface", () => {
  it("initializes and lists tool metadata over a real MCP client/transport", async () => {
    const harness = await createConnectedMcpHarness(createMockApiClient());

    try {
      expect(harness.client.getServerVersion()).toEqual({
        name: "coalesce-transform-mcp",
        version: expect.any(String),
      });
      expect(harness.client.getInstructions()).toContain("plan-pipeline");

      const capabilities = harness.client.getServerCapabilities();
      expect(capabilities?.tools).toBeDefined();
      expect(capabilities?.resources).toBeDefined();
      expect(capabilities?.prompts).toBeDefined();

      const result = await harness.client.listTools();
      expect(result.tools.length).toBeGreaterThan(70);

      const toolNames = result.tools.map((tool) => tool.name);
      expect(toolNames).toContain("set-workspace-node");
      expect(toolNames).toContain("replace-workspace-node-columns");
      expect(toolNames).toContain("create-pipeline-from-plan");
      expect(toolNames).toContain("clear_coalesce_transform_mcp_data_cache");

      const setTool = result.tools.find((tool) => tool.name === "set-workspace-node");
      expect(setTool?.annotations).toMatchObject({
        readOnlyHint: false,
        idempotentHint: true,
        destructiveHint: false,
      });
      expect(setTool?.outputSchema).toMatchObject({
        type: "object",
      });
      expect(setTool?.inputSchema).toMatchObject({
        type: "object",
        properties: {
          body: expect.objectContaining({
            type: "object",
            properties: expect.objectContaining({
              config: expect.objectContaining({ type: "object" }),
              metadata: expect.objectContaining({ type: "object" }),
            }),
          }),
        },
      });

      const replaceColumnsTool = result.tools.find(
        (tool) => tool.name === "replace-workspace-node-columns"
      );
      expect(replaceColumnsTool?.inputSchema).toMatchObject({
        type: "object",
        properties: {
          columns: expect.objectContaining({
            type: "array",
            items: expect.objectContaining({ type: "object" }),
          }),
        },
      });

      const createFromPlanTool = result.tools.find(
        (tool) => tool.name === "create-pipeline-from-plan"
      );
      expect(createFromPlanTool?.inputSchema).toMatchObject({
        type: "object",
        properties: {
          plan: expect.objectContaining({
            type: "object",
            properties: expect.objectContaining({
              nodes: expect.objectContaining({ type: "array" }),
            }),
          }),
        },
      });

      const clearCacheTool = result.tools.find(
        (tool) => tool.name === "clear_coalesce_transform_mcp_data_cache"
      );
      expect(clearCacheTool?.annotations).toMatchObject({
        destructiveHint: true,
      });

      const listRunsTool = result.tools.find((tool) => tool.name === "list-runs");
      expect(listRunsTool?.outputSchema).toMatchObject({
        type: "object",
      });

      const prompts = await harness.client.listPrompts();
      const promptNames = prompts.prompts.map((prompt) => prompt.name);
      expect(promptNames).toEqual(
        expect.arrayContaining([
          "coalesce-start-here",
          "safe-pipeline-planning",
          "run-operations-guide",
          "large-result-handling",
        ])
      );

      const planningPrompt = await harness.client.getPrompt({
        name: "safe-pipeline-planning",
      });
      expect(planningPrompt.messages).toEqual([
        expect.objectContaining({
          role: "user",
          content: expect.objectContaining({
            type: "text",
            text: expect.stringContaining("Always call plan-pipeline"),
          }),
        }),
      ]);
    } finally {
      await harness.close();
    }
  });

  it("lists fixed resources, exposes the cache template, and reads resources over MCP", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "coalesce-mcp-resource-protocol-"));
    tempDirs.push(tempDir);

    const cacheFilePath = join(
      tempDir,
      "coalesce_transform_mcp_data_cache",
      "auto-cache",
      "cached-response.json"
    );
    mkdirSync(join(tempDir, "coalesce_transform_mcp_data_cache", "auto-cache"), {
      recursive: true,
    });
    writeFileSync(cacheFilePath, JSON.stringify({ ok: true }, null, 2), "utf8");

    vi.spyOn(process, "cwd").mockReturnValue(tempDir);

    const harness = await createConnectedMcpHarness(createMockApiClient());

    try {
      const resources = await harness.client.listResources();
      expect(resources.resources.length).toBeGreaterThan(15);
      expect(resources.resources).toContainEqual(
        expect.objectContaining({
          uri: "coalesce://context/overview",
          name: "Coalesce Overview",
          mimeType: "text/markdown",
        })
      );

      const templates = await harness.client.listResourceTemplates();
      expect(templates.resourceTemplates).toContainEqual(
        expect.objectContaining({
          name: "Coalesce Cache Artifact",
          uriTemplate: "coalesce://cache/{cacheKey}",
        })
      );

      const overview = await harness.client.readResource({
        uri: "coalesce://context/overview",
      });
      expect(overview.contents).toEqual([
        expect.objectContaining({
          uri: "coalesce://context/overview",
          mimeType: "text/markdown",
          text: expect.stringContaining("Coalesce"),
        }),
      ]);

      const nodeOperations = await harness.client.readResource({
        uri: "coalesce://context/node-operations",
      });
      expect(nodeOperations.contents).toEqual([
        expect.objectContaining({
          uri: "coalesce://context/node-operations",
          mimeType: "text/markdown",
          text: expect.stringContaining(
            "Pass the user's exact SQL unchanged to `plan-pipeline` or `create-pipeline-from-sql`"
          ),
        }),
      ]);
      expect(nodeOperations.contents[0]?.text).not.toContain(
        "Rewrite with `{{ ref('LOCATION', 'NODE') }}` syntax, preserving original aliases"
      );

      const cacheUri = buildCacheResourceUri(cacheFilePath, tempDir);
      expect(cacheUri).toBeTruthy();

      const cached = await harness.client.readResource({
        uri: cacheUri!,
      });
      expect(cached.contents).toEqual([
        {
          uri: cacheUri,
          mimeType: "application/json",
          text: JSON.stringify({ ok: true }, null, 2),
        },
      ]);
    } finally {
      await harness.close();
    }
  });

  it("calls tools and returns MCP-native validation errors over the protocol", async () => {
    const harness = await createConnectedMcpHarness(
      createMockApiClient({
        get: vi.fn(async (path: unknown) => {
          if (path === "/api/v1/runs") {
            return {
              data: [
                {
                  id: "run-1",
                  status: "completed",
                  userCredentials: { snowflakeUsername: "secret-user" },
                },
              ],
            };
          }
          return { data: [] };
        }),
      })
    );

    try {
      const listRunsResult = await harness.client.callTool({
        name: "list-runs",
        arguments: {},
      });

      expect(listRunsResult.isError).toBeUndefined();
      expect(listRunsResult.content[0]).toMatchObject({
        type: "text",
      });
      expect((listRunsResult.content[0] as { text: string }).text).not.toContain(
        "userCredentials"
      );
      expect(listRunsResult.structuredContent).toEqual({
        data: [{ id: "run-1", status: "completed" }],
      });

      const invalidSetNodeResult = await harness.client.callTool({
        name: "set-workspace-node",
        arguments: {
          workspaceID: "ws-1",
          nodeID: "node-1",
          body: "invalid-body",
        },
      });

      expect(invalidSetNodeResult).toMatchObject({
        isError: true,
        content: [
          {
            type: "text",
            text: expect.stringContaining("Input validation error"),
          },
        ],
      });
    } finally {
      await harness.close();
    }
  });
});
