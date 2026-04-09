import { afterEach, describe, expect, it, vi } from "vitest";
import { cpSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  parseRepo,
  resolveRepoNodeType,
} from "../../src/services/repo/parser.js";
import {
  generateSetWorkspaceNodeTemplate,
  getRepoNodeTypeDefinition,
  listRepoNodeTypes,
  listRepoPackages,
} from "../../src/mcp/repo-node-types.js";

const fixtureRepoPath = resolve("tests/fixtures/repo-backed-coalesce");

function makeTempRepoCopy(): string {
  const tempDir = mkdtempSync(join(tmpdir(), "coalesce-repo-fixture-"));
  cpSync(fixtureRepoPath, tempDir, { recursive: true });
  return tempDir;
}

describe("repo-parser", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("parses package aliases from package name and counts operation.sqlType usage", () => {
    const parsed = parseRepo(fixtureRepoPath);
    const packageRecord = parsed.packages.find(
      (entry) => entry.alias === "package-alpha"
    );

    expect(parsed.summary.resolvedRepoPath).toBe(fixtureRepoPath);
    expect(parsed.usageCounts).toMatchObject({
      Stage: 1,
      "package-alpha:::65": 2,
      "package-alpha:::66": 1,
      "65": 1,
      "package-beta:::33": 1,
    });
    expect(packageRecord).toMatchObject({
      alias: "package-alpha",
      aliasSource: "name",
      packageID: "@example/package-alpha",
      releaseID: "release-package-alpha-1",
      enabledNodeTypeIDs: ["65", "66"],
      resolvedDefinitionIDs: ["65"],
      missingDefinitionIDs: ["66"],
      usageByNodeTypeID: {
        "65": 2,
        "66": 1,
      },
      usageCount: 3,
    });
    expect(packageRecord?.packageVariables).toContain("feature_flag");
    expect(parsed.summary.warnings).toContainEqual(
      expect.stringContaining("Multiple committed nodeTypes share id 500")
    );
  });

  it("resolves exact direct and package-backed identifiers", () => {
    const parsed = parseRepo(fixtureRepoPath);

    const direct = resolveRepoNodeType(parsed, "65");
    const packageBacked = resolveRepoNodeType(parsed, "package-alpha:::65");

    expect(direct).toMatchObject({
      resolutionKind: "direct",
      resolvedNodeType: "65",
      usageCount: 1,
    });
    expect(direct.nodeTypeRecord.outerDefinition.id).toBe("65");

    expect(packageBacked).toMatchObject({
      resolutionKind: "package",
      resolvedNodeType: "package-alpha:::65",
      packageAlias: "package-alpha",
      usageCount: 2,
    });
    expect(packageBacked.nodeTypeRecord.outerDefinition.id).toBe("65");
  });

  it("errors on ambiguous direct identifiers instead of guessing", () => {
    const parsed = parseRepo(fixtureRepoPath);

    expect(() => resolveRepoNodeType(parsed, "500")).toThrow(
      /Multiple committed nodeTypes definitions share outer id 500/u
    );
  });

  it("treats missing packages and nodes as warnings but requires nodeTypes", () => {
    const missingPackagesRepo = makeTempRepoCopy();
    const missingNodesRepo = makeTempRepoCopy();
    const missingNodeTypesRepo = makeTempRepoCopy();

    rmSync(join(missingPackagesRepo, "packages"), { recursive: true, force: true });
    rmSync(join(missingNodesRepo, "nodes"), { recursive: true, force: true });
    rmSync(join(missingNodeTypesRepo, "nodeTypes"), {
      recursive: true,
      force: true,
    });

    const parsedWithoutPackages = parseRepo(missingPackagesRepo);
    const parsedWithoutNodes = parseRepo(missingNodesRepo);

    expect(parsedWithoutPackages.summary.warnings).toContainEqual(
      expect.stringContaining("missing packages/")
    );
    expect(parsedWithoutNodes.summary.warnings).toContainEqual(
      expect.stringContaining("missing nodes/")
    );
    expect(() => parseRepo(missingNodeTypesRepo)).toThrow(
      /missing nodeTypes\/ subdirectory/u
    );

    rmSync(missingPackagesRepo, { recursive: true, force: true });
    rmSync(missingNodesRepo, { recursive: true, force: true });
    rmSync(missingNodeTypesRepo, { recursive: true, force: true });
  });
});

describe("repo-backed tools", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete process.env.COALESCE_REPO_PATH;
  });

  it("lists repo packages with coverage and usage summaries", () => {
    const result = listRepoPackages({ repoPath: fixtureRepoPath });
    const packageRecord = result.packages.find(
      (entry) => entry.alias === "package-alpha"
    );

    expect(result.summary.resolvedRepoPath).toBe(fixtureRepoPath);
    expect(packageRecord).toMatchObject({
      alias: "package-alpha",
      resolvedDefinitionIDs: ["65"],
      missingDefinitionIDs: ["66"],
      usageCount: 3,
      packageVariables: expect.stringContaining("feature_flag"),
    });
  });

  it("uses COALESCE_REPO_PATH as a fallback for repo package discovery", () => {
    process.env.COALESCE_REPO_PATH = fixtureRepoPath;

    const result = listRepoPackages({});

    expect(result.summary.resolvedRepoPath).toBe(fixtureRepoPath);
  });

  it("lists resolvable repo node types sorted by usage count and identifier", () => {
    const result = listRepoNodeTypes({ repoPath: fixtureRepoPath });
    const identifiers = result.nodeTypes.map((entry) => entry.nodeType);

    expect(identifiers[0]).toBe("package-alpha:::65");
    expect(identifiers).toContain("Stage");
    expect(identifiers).toContain("65");
    expect(identifiers).toContain("package-beta:::33");
    expect(identifiers).not.toContain("package-alpha:::66");
    expect(identifiers).not.toContain("500");
  });

  it("filters repo node types by package alias and in-use flag", () => {
    const result = listRepoNodeTypes({
      repoPath: fixtureRepoPath,
      packageAlias: "package-alpha",
      inUseOnly: true,
    });

    expect(result.nodeTypes).toEqual([
      expect.objectContaining({
        nodeType: "package-alpha:::65",
        packageAlias: "package-alpha",
        usageCount: 2,
      }),
    ]);
  });

  it("returns one resolved repo node type definition with metadata and usage", () => {
    const result = getRepoNodeTypeDefinition({
      repoPath: fixtureRepoPath,
      nodeType: "package-alpha:::65",
    });

    expect(result).toMatchObject({
      resolvedRepoPath: fixtureRepoPath,
      requestedNodeType: "package-alpha:::65",
      resolvedNodeType: "package-alpha:::65",
      resolution: {
        resolutionKind: "package",
        packageAlias: "package-alpha",
        usageCount: 2,
      },
      outerDefinition: {
        id: "65",
        name: "Custom Work",
      },
      usageSummary: {
        exactNodeType: "package-alpha:::65",
        usageCount: 2,
      },
    });
    expect(result.nodeMetadataSpecYaml).toContain('capitalized: "Custom Work"');
    expect(result.nodeDefinition).toMatchObject({
      short: "CWRK",
    });
  });

  it("errors when neither repoPath nor COALESCE_REPO_PATH is provided", () => {
    expect(() => listRepoPackages({})).toThrow(
      "repoPath is required for repo-backed tools. Provide repoPath explicitly or set COALESCE_REPO_PATH."
    );
  });

  it("builds a raw template and can compare it to a live workspace node", async () => {
    const mockClient = {
      get: vi.fn().mockResolvedValue({
        name: "RAW_STAGE",
        nodeType: "Stage",
        materializationType: "table",
      }),
    };

    const result = await generateSetWorkspaceNodeTemplate(mockClient as any, {
      definition: {
        capitalized: "Stage",
        short: "STG",
        plural: "Stages",
        tagColor: "#2EB67D",
        config: [
          {
            groupName: "Options",
            items: [
              {
                type: "materializationSelector",
                default: "table",
                options: ["table", "view"],
              },
            ],
          },
        ],
      },
      nodeName: "RAW_STAGE",
      workspaceID: "1",
      nodeID: "2",
    });

    expect(result).toMatchObject({
      definitionSource: {
        mode: "raw",
      },
      setWorkspaceNodeBodyTemplate: {
        name: "RAW_STAGE",
        nodeType: "Stage",
        materializationType: "table",
      },
      comparison: {
        workspaceID: "1",
        nodeID: "2",
      },
    });
    expect(mockClient.get).toHaveBeenCalledWith(
      "/api/v1/workspaces/1/nodes/2",
      {}
    );
  });

  it("removes SQL override controls from raw template generation", async () => {
    const mockClient = {
      get: vi.fn(),
    };

    const result = await generateSetWorkspaceNodeTemplate(mockClient as any, {
      definition: {
        capitalized: "Work",
        short: "WRK",
        plural: "Works",
        tagColor: "#2EB67D",
        config: [
          {
            groupName: "Options",
            items: [
              {
                type: "materializationSelector",
                default: "table",
                options: ["table", "view"],
              },
              {
                type: "overrideSQLToggle",
                enableIf:
                  "{% if node.materializationType == 'view' %} true {% else %} false {% endif %}",
              },
              {
                displayName: "Distinct",
                attributeName: "selectDistinct",
                type: "toggleButton",
                default: false,
                enableIf:
                  "{% if config.groupByAll or (node.materializationType == 'view' and node.override.create.enabled) %} false {% else %} true {% endif %}",
              },
            ],
          },
        ],
      },
    });

    expect(JSON.stringify(result.nodeDefinition)).not.toContain("overrideSQLToggle");
    expect(JSON.stringify(result.nodeDefinition)).not.toContain(
      "node.override.create.enabled"
    );
    expect(result.setWorkspaceNodeBodyTemplate).not.toHaveProperty("overrideSQL");
    expect(result.fieldMappings).toEqual(
      expect.not.arrayContaining([
        expect.objectContaining({
          targetPath: "overrideSQL",
        }),
      ])
    );
    expect(result.usageGuidance).toContain(
      "Do not add overrideSQL or override.* fields; SQL override is intentionally disallowed in this project."
    );
    expect(result.warnings).toEqual(
      expect.arrayContaining([
        expect.stringContaining("SQL override control"),
      ])
    );
  });

  it("builds a repo-backed template and preserves the exact package-backed nodeType", async () => {
    const mockClient = {
      get: vi.fn().mockResolvedValue({
        name: "PACKAGE_ALPHA_NODE_1",
        nodeType: "package-alpha:::65",
        config: {
          strategy: "APPEND",
        },
      }),
    };

    const result = await generateSetWorkspaceNodeTemplate(mockClient as any, {
      repoPath: fixtureRepoPath,
      nodeType: "package-alpha:::65",
      nodeName: "PACKAGE_ALPHA_NODE_1",
      workspaceID: "10",
      nodeID: "20",
    });

    expect(result).toMatchObject({
      definitionSource: {
        mode: "repo",
      },
      resolvedRepoPath: fixtureRepoPath,
      requestedNodeType: "package-alpha:::65",
      resolvedNodeType: "package-alpha:::65",
      setWorkspaceNodeBodyTemplate: {
        name: "PACKAGE_ALPHA_NODE_1",
        nodeType: "package-alpha:::65",
        config: {
          strategy: "APPEND",
        },
      },
      comparison: {
        workspaceID: "10",
        nodeID: "20",
      },
    });
  });

  it("uses COALESCE_REPO_PATH fallback for repo-backed template generation", async () => {
    process.env.COALESCE_REPO_PATH = fixtureRepoPath;
    const mockClient = {
      get: vi.fn().mockResolvedValue({
        name: "PACKAGE_ALPHA_NODE_1",
        nodeType: "package-alpha:::65",
        config: {
          strategy: "APPEND",
        },
      }),
    };

    const result = await generateSetWorkspaceNodeTemplate(mockClient as any, {
      nodeType: "package-alpha:::65",
      nodeName: "PACKAGE_ALPHA_NODE_1",
      workspaceID: "10",
      nodeID: "20",
    });

    expect(result).toMatchObject({
      definitionSource: {
        mode: "repo",
      },
      repoPath: fixtureRepoPath,
      resolvedRepoPath: fixtureRepoPath,
      requestedNodeType: "package-alpha:::65",
      resolvedNodeType: "package-alpha:::65",
    });
  });

  it("fails explicitly when a package alias exists but no committed definition is present", async () => {
    const mockClient = {
      get: vi.fn(),
    };

    await expect(
      generateSetWorkspaceNodeTemplate(mockClient as any, {
        repoPath: fixtureRepoPath,
        nodeType: "package-alpha:::66",
      })
    ).rejects.toThrow(/corpus tools/u);
  });
});
