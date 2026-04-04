import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerSkillTools } from "../../src/mcp/skills.js";
import { RESOURCE_FILES, resetSkillsState } from "../../src/resources/index.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

describe("personalize_skills tool", () => {
  let server: McpServer;
  let toolSpy: ReturnType<typeof vi.spyOn>;
  const tempDirs: string[] = [];

  beforeEach(() => {
    server = new McpServer({ name: "test", version: "0.0.1" });
    toolSpy = vi.spyOn(server, "registerTool");
    resetSkillsState();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    for (const directory of tempDirs.splice(0, tempDirs.length)) {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  function createTempDir(): string {
    const directory = mkdtempSync(join(tmpdir(), "coalesce-skills-test-"));
    tempDirs.push(directory);
    return directory;
  }

  function getToolHandler() {
    const client = createMockClient();
    registerSkillTools(server, client as any);
    const call = toolSpy.mock.calls.find(
      (c: unknown[]) => c[0] === "personalize_skills"
    );
    expect(call).toBeDefined();
    return call![2] as (params: { directory?: string }) => Promise<any>;
  }

  it("registers the personalize_skills tool", () => {
    const client = createMockClient();
    registerSkillTools(server, client as any);

    expect(toolSpy).toHaveBeenCalledTimes(1);
    expect(toolSpy.mock.calls[0][0]).toBe("personalize_skills");
  });

  it("seeds all skill files into a new directory", async () => {
    const dir = createTempDir();
    const handler = getToolHandler();

    const result = await handler({ directory: dir });

    expect(result.isError).toBeUndefined();
    expect(result.structuredContent.directory).toBe(dir);
    expect(result.structuredContent.totalSkills).toBe(
      Object.keys(RESOURCE_FILES).length
    );
    expect(result.structuredContent.created.length).toBe(
      Object.keys(RESOURCE_FILES).length
    );
    expect(result.structuredContent.alreadyExisted.length).toBe(0);

    // Verify files exist on disk
    expect(existsSync(join(dir, "coalesce_skills.overview.md"))).toBe(true);
    expect(existsSync(join(dir, "user_skills.overview.md"))).toBe(true);
  });

  it("reports already-existing skills without overwriting them", async () => {
    const dir = createTempDir();
    const handler = getToolHandler();

    // Pre-create one skill pair
    const customContent = "# My custom overview";
    writeFileSync(join(dir, "coalesce_skills.overview.md"), customContent, "utf-8");
    writeFileSync(join(dir, "user_skills.overview.md"), "My overrides", "utf-8");

    const result = await handler({ directory: dir });

    expect(result.structuredContent.alreadyExisted).toContain("overview");
    expect(result.structuredContent.created).not.toContain("overview");

    // Verify original content was not overwritten
    expect(readFileSync(join(dir, "coalesce_skills.overview.md"), "utf-8")).toBe(
      customContent
    );
    expect(readFileSync(join(dir, "user_skills.overview.md"), "utf-8")).toBe(
      "My overrides"
    );
  });

  it("is idempotent — running twice produces the same result", async () => {
    const dir = createTempDir();
    const handler = getToolHandler();

    const first = await handler({ directory: dir });
    resetSkillsState();
    const second = await handler({ directory: dir });

    expect(first.structuredContent.created.length).toBe(
      Object.keys(RESOURCE_FILES).length
    );
    expect(second.structuredContent.alreadyExisted.length).toBe(
      Object.keys(RESOURCE_FILES).length
    );
    expect(second.structuredContent.created.length).toBe(0);
  });

  it("includes config hint when COALESCE_MCP_SKILLS_DIR is not set", async () => {
    const dir = createTempDir();
    const original = process.env.COALESCE_MCP_SKILLS_DIR;
    delete process.env.COALESCE_MCP_SKILLS_DIR;

    try {
      const handler = getToolHandler();
      const result = await handler({ directory: dir });

      expect(result.structuredContent.configHint).toContain(
        "COALESCE_MCP_SKILLS_DIR"
      );
      expect(result.structuredContent.configHint).toContain(dir);
    } finally {
      if (original !== undefined) {
        process.env.COALESCE_MCP_SKILLS_DIR = original;
      }
    }
  });

  it("omits config hint when COALESCE_MCP_SKILLS_DIR matches", async () => {
    const dir = createTempDir();
    const original = process.env.COALESCE_MCP_SKILLS_DIR;
    process.env.COALESCE_MCP_SKILLS_DIR = dir;

    try {
      const handler = getToolHandler();
      const result = await handler({ directory: dir });

      expect(result.structuredContent.configHint).toBeNull();
    } finally {
      if (original !== undefined) {
        process.env.COALESCE_MCP_SKILLS_DIR = original;
      } else {
        delete process.env.COALESCE_MCP_SKILLS_DIR;
      }
    }
  });

  it("includes customization instructions in text output", async () => {
    const dir = createTempDir();
    const handler = getToolHandler();
    const result = await handler({ directory: dir });

    const text = result.content[0].text;
    expect(text).toContain("How to customize:");
    expect(text).toContain("<!-- OVERRIDE -->");
    expect(text).toContain("user_skills");
  });

  it("returns error for unwritable directory", async () => {
    const handler = getToolHandler();
    const result = await handler({ directory: "/proc/nonexistent/path" });

    expect(result.isError).toBe(true);
    expect(result.content[0].text).toContain("Failed to personalize skills");
  });
});
