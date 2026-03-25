import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { buildCacheResourceUri } from "../src/cache-dir.js";
import { registerResources } from "../src/resources/index.js";

describe("Resources", () => {
  let server: McpServer;
  let resourceSpy: ReturnType<typeof vi.spyOn>;
  const tempDirs: string[] = [];

  beforeEach(() => {
    server = new McpServer({ name: "test", version: "0.0.1" });
    resourceSpy = vi.spyOn(server, "resource");
  });

  afterEach(() => {
    vi.restoreAllMocks();
    for (const directory of tempDirs.splice(0, tempDirs.length)) {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("registers fixed context resources plus the cache resource template", () => {
    registerResources(server);

    expect(resourceSpy).toHaveBeenCalledTimes(20);
    const fixedResourceCalls = resourceSpy.mock.calls
      .filter((call) => typeof call[1] === "string")
      .map((call) => ({
      name: call[0],
      uri: call[1],
      }));
    expect(fixedResourceCalls).toEqual([
      {
        name: "Coalesce Overview",
        uri: "coalesce://context/overview",
      },
      {
        name: "SQL Platform Selection",
        uri: "coalesce://context/sql-platform-selection",
      },
      {
        name: "SQL Rules: Snowflake",
        uri: "coalesce://context/sql-snowflake",
      },
      {
        name: "SQL Rules: Databricks",
        uri: "coalesce://context/sql-databricks",
      },
      {
        name: "SQL Rules: BigQuery",
        uri: "coalesce://context/sql-bigquery",
      },
      {
        name: "Data Engineering Principles",
        uri: "coalesce://context/data-engineering-principles",
      },
      {
        name: "Storage Locations and References",
        uri: "coalesce://context/storage-mappings",
      },
      {
        name: "Tool Usage Patterns",
        uri: "coalesce://context/tool-usage",
      },
      {
        name: "ID Discovery",
        uri: "coalesce://context/id-discovery",
      },
      {
        name: "Node Creation Decision Tree",
        uri: "coalesce://context/node-creation-decision-tree",
      },
      {
        name: "Node Payloads",
        uri: "coalesce://context/node-payloads",
      },
      {
        name: "Hydrated Metadata",
        uri: "coalesce://context/hydrated-metadata",
      },
      {
        name: "Run Operations",
        uri: "coalesce://context/run-operations",
      },
      {
        name: "Node Type Corpus",
        uri: "coalesce://context/node-type-corpus",
      },
      {
        name: "Aggregation Patterns",
        uri: "coalesce://context/aggregation-patterns",
      },
      {
        name: "Intelligent Node Configuration",
        uri: "coalesce://context/intelligent-node-configuration",
      },
      {
        name: "Pipeline Workflows",
        uri: "coalesce://context/pipeline-workflows",
      },
      {
        name: "Node Operations",
        uri: "coalesce://context/node-operations",
      },
      {
        name: "Node Type Selection Guide",
        uri: "coalesce://context/node-type-selection-guide",
      },
    ]);

    const cacheTemplateCall = resourceSpy.mock.calls.find(
      (call) => typeof call[1] !== "string"
    );
    expect(cacheTemplateCall?.[0]).toBe("Coalesce Cache Artifact");
  });

  it("returns markdown content for a registered resource", async () => {
    registerResources(server);

    const overviewCall = resourceSpy.mock.calls.find(
      (call) => call[1] === "coalesce://context/overview"
    );

    expect(overviewCall).toBeDefined();
    const readCallback = overviewCall?.[3];
    expect(typeof readCallback).toBe("function");

    const result = await readCallback?.(
      new URL("coalesce://context/overview"),
      {} as never
    );

    expect(result).toEqual({
      contents: [
        expect.objectContaining({
          uri: "coalesce://context/overview",
          mimeType: "text/markdown",
          text: expect.stringContaining("Coalesce"),
        }),
      ],
    });
  });

  it("can read every registered resource callback", async () => {
    registerResources(server);

    for (const call of resourceSpy.mock.calls.filter(
      (entry) => typeof entry[1] === "string"
    )) {
      const uri = call[1] as string;
      const readCallback = call[3];

      expect(typeof readCallback).toBe("function");

      const result = await readCallback(new URL(uri), {} as never);
      expect(result).toEqual({
        contents: [
          expect.objectContaining({
            uri,
            mimeType: "text/markdown",
            text: expect.any(String),
          }),
        ],
      });
      expect(result.contents[0]?.text.length).toBeGreaterThan(0);
    }
  });

  it("exposes cached files through the cache resource template", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "coalesce-resource-cache-"));
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

    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(tempDir);
    registerResources(server);

    const cacheTemplateCall = resourceSpy.mock.calls.find(
      (call) => typeof call[1] !== "string"
    );
    expect(cacheTemplateCall).toBeDefined();

    const template = cacheTemplateCall?.[1];
    const readCallback = cacheTemplateCall?.[3];
    expect(template).toBeDefined();
    expect(typeof readCallback).toBe("function");

    const cacheUri = buildCacheResourceUri(cacheFilePath, tempDir);
    expect(cacheUri).toBeTruthy();

    const listResult = await template.listCallback({} as never);
    expect(listResult.resources).toContainEqual(
      expect.objectContaining({
        uri: cacheUri,
        mimeType: "application/json",
      })
    );

    const readResult = await readCallback?.(new URL(cacheUri!), {} as never, {} as never);
    expect(readResult).toEqual({
      contents: [
        {
          uri: cacheUri,
          mimeType: "application/json",
          text: JSON.stringify({ ok: true }, null, 2),
        },
      ],
    });

    cwdSpy.mockRestore();
  });
});
