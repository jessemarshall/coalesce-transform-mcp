import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { buildCacheResourceUri } from "../src/cache-dir.js";
import { registerResources, resetSkillsState } from "../src/resources/index.js";

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

    expect(resourceSpy).toHaveBeenCalledTimes(24);
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
      {
        name: "Intent Pipeline Guide",
        uri: "coalesce://context/intent-pipeline-guide",
      },
      {
        name: "Run Diagnostics Guide",
        uri: "coalesce://context/run-diagnostics-guide",
      },
      {
        name: "Pipeline Review Guide",
        uri: "coalesce://context/pipeline-review-guide",
      },
      {
        name: "Pipeline Workshop Guide",
        uri: "coalesce://context/pipeline-workshop-guide",
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

describe("Skills directory", () => {
  let server: McpServer;
  let resourceSpy: ReturnType<typeof vi.spyOn>;
  const tempDirs: string[] = [];
  const ORIGINAL_SKILLS_DIR = process.env.COALESCE_MCP_SKILLS_DIR;

  beforeEach(() => {
    server = new McpServer({ name: "test", version: "0.0.1" });
    resourceSpy = vi.spyOn(server, "resource");
    resetSkillsState();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    if (ORIGINAL_SKILLS_DIR === undefined) {
      delete process.env.COALESCE_MCP_SKILLS_DIR;
    } else {
      process.env.COALESCE_MCP_SKILLS_DIR = ORIGINAL_SKILLS_DIR;
    }
    for (const directory of tempDirs.splice(0, tempDirs.length)) {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  function makeSkillsDir(): string {
    const dir = mkdtempSync(join(tmpdir(), "coalesce-skills-"));
    tempDirs.push(dir);
    process.env.COALESCE_MCP_SKILLS_DIR = dir;
    return dir;
  }

  function getResourceCallback(uri: string) {
    return resourceSpy.mock.calls.find((call) => call[1] === uri)?.[3] as
      | ((uri: URL, extra: never) => Promise<{ contents: { uri: string; mimeType: string; text: string }[] }>)
      | undefined;
  }

  it("seeds coalesce_skills.* and user_skills.* files on first resource read", async () => {
    const dir = makeSkillsDir();
    registerResources(server);

    const callback = getResourceCallback("coalesce://context/overview");
    expect(callback).toBeDefined();
    await callback!(new URL("coalesce://context/overview"), {} as never);

    // Verify coalesce_skills files exist
    expect(existsSync(join(dir, "coalesce_skills.overview.md"))).toBe(true);
    expect(existsSync(join(dir, "coalesce_skills.sql-snowflake.md"))).toBe(true);
    expect(existsSync(join(dir, "coalesce_skills.node-payloads.md"))).toBe(true);

    // Verify user_skills files exist
    expect(existsSync(join(dir, "user_skills.overview.md"))).toBe(true);
    expect(existsSync(join(dir, "user_skills.sql-snowflake.md"))).toBe(true);

    // Verify user_skills stubs start with STUB marker and contain instructions
    const userStub = readFileSync(join(dir, "user_skills.overview.md"), "utf-8");
    expect(userStub.startsWith("<!-- STUB -->")).toBe(true);
    expect(userStub).toContain("<!-- OVERRIDE -->");
    expect(userStub).toContain("User customization file");
  });

  it("serves only default content when user file is the seeded stub", async () => {
    const dir = makeSkillsDir();
    registerResources(server);

    const callback = getResourceCallback("coalesce://context/overview");
    const result = await callback!(new URL("coalesce://context/overview"), {} as never);

    // The seeded stub should NOT trigger augmentation — only default content is served
    const defaultContent = readFileSync(join(dir, "coalesce_skills.overview.md"), "utf-8");
    expect(result.contents[0].text).toBe(defaultContent);
    expect(result.contents[0].text).not.toContain("<!-- STUB -->");
    expect(result.contents[0].text).not.toContain("User customization file");
  });

  it("does not overwrite existing files during seeding", async () => {
    const dir = makeSkillsDir();
    const customContent = "# My custom overview";
    writeFileSync(join(dir, "coalesce_skills.overview.md"), customContent, "utf-8");

    registerResources(server);
    const callback = getResourceCallback("coalesce://context/overview");
    await callback!(new URL("coalesce://context/overview"), {} as never);

    expect(readFileSync(join(dir, "coalesce_skills.overview.md"), "utf-8")).toBe(customContent);
  });

  it("serves only user content when override marker is present", async () => {
    const dir = makeSkillsDir();
    writeFileSync(join(dir, "coalesce_skills.overview.md"), "Default content", "utf-8");
    writeFileSync(join(dir, "user_skills.overview.md"), "<!-- OVERRIDE -->\nMy custom override", "utf-8");

    registerResources(server);
    const callback = getResourceCallback("coalesce://context/overview");
    const result = await callback!(new URL("coalesce://context/overview"), {} as never);

    expect(result.contents[0].text).toBe("<!-- OVERRIDE -->\nMy custom override");
  });

  it("concatenates default + user content when no override marker", async () => {
    const dir = makeSkillsDir();
    writeFileSync(join(dir, "coalesce_skills.overview.md"), "Default content", "utf-8");
    writeFileSync(join(dir, "user_skills.overview.md"), "Extra user notes", "utf-8");

    registerResources(server);
    const callback = getResourceCallback("coalesce://context/overview");
    const result = await callback!(new URL("coalesce://context/overview"), {} as never);

    expect(result.contents[0].text).toBe("Default content\n\nExtra user notes");
  });

  it("serves default content when user file is empty", async () => {
    const dir = makeSkillsDir();
    writeFileSync(join(dir, "coalesce_skills.overview.md"), "Default content", "utf-8");
    writeFileSync(join(dir, "user_skills.overview.md"), "   \n  ", "utf-8");

    registerResources(server);
    const callback = getResourceCallback("coalesce://context/overview");
    const result = await callback!(new URL("coalesce://context/overview"), {} as never);

    expect(result.contents[0].text).toBe("Default content");
  });

  it("serves default content when user file is missing", async () => {
    const dir = makeSkillsDir();
    writeFileSync(join(dir, "coalesce_skills.overview.md"), "Default content", "utf-8");

    registerResources(server);
    const callback = getResourceCallback("coalesce://context/overview");

    // Trigger seeding (creates user_skills stub), then delete the user file
    await callback!(new URL("coalesce://context/overview"), {} as never);
    rmSync(join(dir, "user_skills.overview.md"), { force: true });

    const result = await callback!(new URL("coalesce://context/overview"), {} as never);
    expect(result.contents[0].text).toBe("Default content");
  });

  it("serves empty string when both files are deleted", async () => {
    const dir = makeSkillsDir();
    // Don't create any files, and mark as initialized to skip seeding
    mkdirSync(dir, { recursive: true });

    registerResources(server);

    // Trigger seeding first (creates files), then delete them
    const callback = getResourceCallback("coalesce://context/overview");
    await callback!(new URL("coalesce://context/overview"), {} as never);

    rmSync(join(dir, "coalesce_skills.overview.md"), { force: true });
    rmSync(join(dir, "user_skills.overview.md"), { force: true });

    // Read again — should return empty
    const result = await callback!(new URL("coalesce://context/overview"), {} as never);
    expect(result.contents[0].text).toBe("");
  });

  it("handles BOM prefix in user file with override marker", async () => {
    const dir = makeSkillsDir();
    writeFileSync(join(dir, "coalesce_skills.overview.md"), "Default content", "utf-8");
    // BOM + override marker (common with Windows Notepad)
    writeFileSync(join(dir, "user_skills.overview.md"), "\uFEFF<!-- OVERRIDE -->\nBOM override", "utf-8");

    registerResources(server);
    const callback = getResourceCallback("coalesce://context/overview");
    const result = await callback!(new URL("coalesce://context/overview"), {} as never);

    // BOM should be stripped, override marker should be detected
    expect(result.contents[0].text).toBe("<!-- OVERRIDE -->\nBOM override");
  });

  it("allows disabling a resource via empty override", async () => {
    const dir = makeSkillsDir();
    writeFileSync(join(dir, "coalesce_skills.overview.md"), "Default content", "utf-8");
    writeFileSync(join(dir, "user_skills.overview.md"), "<!-- OVERRIDE -->", "utf-8");

    registerResources(server);
    const callback = getResourceCallback("coalesce://context/overview");
    const result = await callback!(new URL("coalesce://context/overview"), {} as never);

    // Override with just the marker — effectively disables the resource
    expect(result.contents[0].text).toBe("<!-- OVERRIDE -->");
  });
});
