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

  it("does not list or read orphaned snapshot files without matching metadata", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "coalesce-resource-orphan-"));
    tempDirs.push(tempDir);

    const orphanNdjsonPath = join(
      tempDir,
      "coalesce_transform_mcp_data_cache",
      "nodes",
      "workspace-ws-1-nodes.ndjson"
    );
    mkdirSync(join(tempDir, "coalesce_transform_mcp_data_cache", "nodes"), {
      recursive: true,
    });
    writeFileSync(orphanNdjsonPath, '{"id":"partial"}\n', "utf8");

    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(tempDir);
    registerResources(server);

    const cacheTemplateCall = resourceSpy.mock.calls.find(
      (call) => typeof call[1] !== "string"
    );
    const template = cacheTemplateCall?.[1];
    const readCallback = cacheTemplateCall?.[3];
    const orphanUri = buildCacheResourceUri(orphanNdjsonPath, tempDir);

    expect(orphanUri).toBeTruthy();

    const listResult = await template.listCallback({} as never);
    expect(listResult.resources).not.toContainEqual(
      expect.objectContaining({
        uri: orphanUri,
      })
    );

    await expect(
      readCallback?.(new URL(orphanUri!), {} as never, {} as never)
    ).rejects.toThrow(`Unknown cache resource: ${orphanUri}`);

    cwdSpy.mockRestore();
  });

  it("does not list or read orphaned snapshot metadata without matching NDJSON", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "coalesce-resource-orphan-meta-"));
    tempDirs.push(tempDir);

    const orphanMetaPath = join(
      tempDir,
      "coalesce_transform_mcp_data_cache",
      "nodes",
      "workspace-ws-1-nodes.meta.json"
    );
    mkdirSync(join(tempDir, "coalesce_transform_mcp_data_cache", "nodes"), {
      recursive: true,
    });
    writeFileSync(orphanMetaPath, JSON.stringify({ totalItems: 1 }, null, 2), "utf8");

    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(tempDir);
    registerResources(server);

    const cacheTemplateCall = resourceSpy.mock.calls.find(
      (call) => typeof call[1] !== "string"
    );
    const template = cacheTemplateCall?.[1];
    const readCallback = cacheTemplateCall?.[3];
    const orphanUri = buildCacheResourceUri(orphanMetaPath, tempDir);

    expect(orphanUri).toBeTruthy();

    const listResult = await template.listCallback({} as never);
    expect(listResult.resources).not.toContainEqual(
      expect.objectContaining({
        uri: orphanUri,
      })
    );

    await expect(
      readCallback?.(new URL(orphanUri!), {} as never, {} as never)
    ).rejects.toThrow(`Unknown cache resource: ${orphanUri}`);

    cwdSpy.mockRestore();
  });

  it("does not list or read temporary cache files", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "coalesce-resource-temp-"));
    tempDirs.push(tempDir);

    const tempNdjsonPath = join(
      tempDir,
      "coalesce_transform_mcp_data_cache",
      "nodes",
      "workspace-ws-1-nodes.ndjson.tmp-123"
    );
    mkdirSync(join(tempDir, "coalesce_transform_mcp_data_cache", "nodes"), {
      recursive: true,
    });
    writeFileSync(tempNdjsonPath, '{"id":"partial"}\n', "utf8");

    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(tempDir);
    registerResources(server);

    const cacheTemplateCall = resourceSpy.mock.calls.find(
      (call) => typeof call[1] !== "string"
    );
    const template = cacheTemplateCall?.[1];
    const readCallback = cacheTemplateCall?.[3];
    const tempUri = buildCacheResourceUri(tempNdjsonPath, tempDir);

    expect(tempUri).toBeTruthy();

    const listResult = await template.listCallback({} as never);
    expect(listResult.resources).not.toContainEqual(
      expect.objectContaining({
        uri: tempUri,
      })
    );

    await expect(
      readCallback?.(new URL(tempUri!), {} as never, {} as never)
    ).rejects.toThrow(`Unknown cache resource: ${tempUri}`);

    cwdSpy.mockRestore();
  });

  it("does not list or read backup cache files", async () => {
    const tempDir = mkdtempSync(join(tmpdir(), "coalesce-resource-backup-"));
    tempDirs.push(tempDir);

    const backupNdjsonPath = join(
      tempDir,
      "coalesce_transform_mcp_data_cache",
      "nodes",
      "workspace-ws-1-nodes.ndjson.bak-123"
    );
    mkdirSync(join(tempDir, "coalesce_transform_mcp_data_cache", "nodes"), {
      recursive: true,
    });
    writeFileSync(backupNdjsonPath, '{"id":"backup"}\n', "utf8");

    const cwdSpy = vi.spyOn(process, "cwd").mockReturnValue(tempDir);
    registerResources(server);

    const cacheTemplateCall = resourceSpy.mock.calls.find(
      (call) => typeof call[1] !== "string"
    );
    const template = cacheTemplateCall?.[1];
    const readCallback = cacheTemplateCall?.[3];
    const backupUri = buildCacheResourceUri(backupNdjsonPath, tempDir);

    expect(backupUri).toBeTruthy();

    const listResult = await template.listCallback({} as never);
    expect(listResult.resources).not.toContainEqual(
      expect.objectContaining({
        uri: backupUri,
      })
    );

    await expect(
      readCallback?.(new URL(backupUri!), {} as never, {} as never)
    ).rejects.toThrow(`Unknown cache resource: ${backupUri}`);

    cwdSpy.mockRestore();
  });
});
