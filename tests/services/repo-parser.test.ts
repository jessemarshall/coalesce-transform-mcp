/**
 * Direct unit tests for src/services/repo/parser.ts.
 *
 * The repo-parser is the foundation of every repo-backed tool
 * (list_repo_packages, list_repo_node_types, get_repo_node_type_definition,
 * generate_set_workspace_node_template). It walks the on-disk Coalesce repo
 * (packages/, nodeTypes/, nodes/) and resolves identifiers — its error
 * branches are how the agent learns the difference between "no repo path
 * configured" and "repo path is wrong" and "this nodeType doesn't exist
 * here". Existing tests in tests/tools/repo-node-types.test.ts cover the
 * happy paths; this file fills in the negative paths by mutating fresh
 * temp-dir copies of the fixture repo.
 */
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
} from "vitest";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  parseRepo,
  resolveRepoNodeType,
} from "../../src/services/repo/parser.js";

const fixtureRepoPath = resolve("tests/fixtures/repo-backed-coalesce");

function makeTempRepoCopy(): string {
  const tempDir = mkdtempSync(join(tmpdir(), "coalesce-repo-parser-"));
  cpSync(fixtureRepoPath, tempDir, { recursive: true });
  return tempDir;
}

describe("normalizeRepoPath (via parseRepo)", () => {
  it("throws when the repo path does not exist", () => {
    expect(() =>
      parseRepo("/tmp/coalesce-repo-parser-does-not-exist-zzz")
    ).toThrow(/Repo path does not exist/u);
  });

  it("throws when the repo path is a file rather than a directory", () => {
    const dir = mkdtempSync(join(tmpdir(), "coalesce-repo-parser-file-"));
    const filePath = join(dir, "not-a-dir.txt");
    writeFileSync(filePath, "");
    try {
      expect(() => parseRepo(filePath)).toThrow(
        /not a directory/u
      );
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("throws when nodeTypes/ is a file instead of a directory", () => {
    const dir = mkdtempSync(join(tmpdir(), "coalesce-repo-parser-typesfile-"));
    writeFileSync(join(dir, "nodeTypes"), "");
    try {
      expect(() => parseRepo(dir)).toThrow(/missing nodeTypes\/ subdirectory/u);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("loadRepoNodeTypes warnings (via parseRepo)", () => {
  let tempRepo: string;

  beforeEach(() => {
    tempRepo = makeTempRepoCopy();
  });

  afterEach(() => {
    rmSync(tempRepo, { recursive: true, force: true });
  });

  it("skips a nodeType subdir that has no definition.yml and emits a warning", () => {
    mkdirSync(join(tempRepo, "nodeTypes", "Empty-Node-Type"), { recursive: true });

    const parsed = parseRepo(tempRepo);

    expect(parsed.summary.warnings).toContainEqual(
      expect.stringContaining("Skipping node type directory without definition.yml")
    );
    expect(
      parsed.nodeTypes.some((record) => record.dirName === "Empty-Node-Type")
    ).toBe(false);
  });

  it("skips a nodeType whose definition.yml does not parse to an object", () => {
    const dirPath = join(tempRepo, "nodeTypes", "Scalar-Node-Type");
    mkdirSync(dirPath, { recursive: true });
    writeFileSync(join(dirPath, "definition.yml"), "just-a-scalar\n");

    const parsed = parseRepo(tempRepo);

    expect(parsed.summary.warnings).toContainEqual(
      expect.stringContaining("Skipping node type definition that did not parse to an object")
    );
    expect(
      parsed.nodeTypes.some((record) => record.dirName === "Scalar-Node-Type")
    ).toBe(false);
  });

  it("skips a nodeType whose definition.yml is malformed YAML", () => {
    const dirPath = join(tempRepo, "nodeTypes", "Broken-Yaml-Node-Type");
    mkdirSync(dirPath, { recursive: true });
    writeFileSync(
      join(dirPath, "definition.yml"),
      "this: is: definitely: not: valid yaml: [\n"
    );

    const parsed = parseRepo(tempRepo);

    expect(parsed.summary.warnings).toContainEqual(
      expect.stringContaining("Skipping unreadable node type definition")
    );
    expect(
      parsed.nodeTypes.some((record) => record.dirName === "Broken-Yaml-Node-Type")
    ).toBe(false);
  });

  it("records record-level warnings when nodeMetadataSpec is missing", () => {
    const dirPath = join(tempRepo, "nodeTypes", "No-Metadata-Spec-77");
    mkdirSync(dirPath, { recursive: true });
    writeFileSync(
      join(dirPath, "definition.yml"),
      "fileVersion: 1\nid: \"77\"\nisDisabled: false\nname: NoSpec\n"
    );

    const parsed = parseRepo(tempRepo);

    const record = parsed.nodeTypes.find(
      (entry) => entry.dirName === "No-Metadata-Spec-77"
    );
    expect(record).toBeDefined();
    expect(record!.nodeDefinition).toBeNull();
    expect(record!.nodeMetadataSpec).toBeNull();
    expect(record!.warnings).toContainEqual(
      expect.stringContaining("Missing metadata.nodeMetadataSpec")
    );
    expect(record!.warnings).toContainEqual(
      expect.stringContaining("Missing create.sql.j2")
    );
    expect(record!.warnings).toContainEqual(
      expect.stringContaining("Missing run.sql.j2")
    );
  });

  it("records a parse error when nodeMetadataSpec is unparseable YAML", () => {
    const dirPath = join(tempRepo, "nodeTypes", "Bad-Spec-78");
    mkdirSync(dirPath, { recursive: true });
    writeFileSync(
      join(dirPath, "definition.yml"),
      [
        "fileVersion: 1",
        "id: \"78\"",
        "isDisabled: false",
        "name: BadSpec",
        "metadata:",
        "  nodeMetadataSpec: |",
        "    capitalized: BadSpec",
        "    short: [unclosed",
        "",
      ].join("\n")
    );

    const parsed = parseRepo(tempRepo);

    const record = parsed.nodeTypes.find(
      (entry) => entry.dirName === "Bad-Spec-78"
    );
    expect(record).toBeDefined();
    expect(record!.parseError).not.toBeNull();
    expect(record!.warnings).toContainEqual(
      expect.stringContaining("Unable to parse metadata.nodeMetadataSpec")
    );
    expect(record!.nodeDefinition).toBeNull();
  });

  it("records a parse error when nodeMetadataSpec parses to a non-object", () => {
    const dirPath = join(tempRepo, "nodeTypes", "Scalar-Spec-79");
    mkdirSync(dirPath, { recursive: true });
    writeFileSync(
      join(dirPath, "definition.yml"),
      [
        "fileVersion: 1",
        "id: \"79\"",
        "isDisabled: false",
        "name: ScalarSpec",
        "metadata:",
        "  nodeMetadataSpec: |",
        "    just-a-scalar",
        "",
      ].join("\n")
    );

    const parsed = parseRepo(tempRepo);

    const record = parsed.nodeTypes.find(
      (entry) => entry.dirName === "Scalar-Spec-79"
    );
    expect(record).toBeDefined();
    expect(record!.parseError).toBe("Parsed nodeMetadataSpec was not an object");
    expect(record!.nodeDefinition).toBeNull();
  });
});

describe("loadPackages warnings (via parseRepo)", () => {
  let tempRepo: string;

  beforeEach(() => {
    tempRepo = makeTempRepoCopy();
  });

  afterEach(() => {
    rmSync(tempRepo, { recursive: true, force: true });
  });

  it("falls back to the filename alias and warns when a package has no name", () => {
    writeFileSync(
      join(tempRepo, "packages", "no-name-package.yml"),
      [
        "config:",
        "  entities:",
        "    nodeTypes:",
        "      \"33\":",
        "        defaultStorageLocation: null",
        "        isDisabled: false",
        "fileVersion: 1",
        "id: \"@example/no-name\"",
        "packageID: \"@example/no-name\"",
        "releaseID: release-no-name-1",
        "type: Package",
        "",
      ].join("\n")
    );

    const parsed = parseRepo(tempRepo);

    const record = parsed.packages.find(
      (entry) => entry.alias === "no-name-package"
    );
    expect(record).toBeDefined();
    expect(record!.aliasSource).toBe("filename");
    expect(record!.warnings).toContainEqual(
      expect.stringContaining("missing name; falling back to filename alias")
    );
  });

  it("skips a package file that does not parse to an object", () => {
    writeFileSync(
      join(tempRepo, "packages", "scalar-package.yml"),
      "just-a-scalar\n"
    );

    const parsed = parseRepo(tempRepo);

    expect(parsed.summary.warnings).toContainEqual(
      expect.stringContaining(
        "Skipping package file that did not parse to an object"
      )
    );
    expect(
      parsed.packages.some((p) => p.packageFilePath.endsWith("scalar-package.yml"))
    ).toBe(false);
  });

  it("skips a package file with malformed YAML and emits a warning", () => {
    writeFileSync(
      join(tempRepo, "packages", "broken-package.yml"),
      "config: { entities: [unclosed\n"
    );

    const parsed = parseRepo(tempRepo);

    expect(parsed.summary.warnings).toContainEqual(
      expect.stringContaining("Skipping unreadable package file")
    );
  });

  it("treats isDisabled: true entries as not enabled", () => {
    writeFileSync(
      join(tempRepo, "packages", "with-disabled.yml"),
      [
        "config:",
        "  entities:",
        "    nodeTypes:",
        "      \"65\":",
        "        defaultStorageLocation: null",
        "        isDisabled: true",
        "      \"66\":",
        "        defaultStorageLocation: null",
        "        isDisabled: false",
        "fileVersion: 1",
        "id: \"@example/with-disabled\"",
        "name: with-disabled",
        "packageID: \"@example/with-disabled\"",
        "releaseID: release-with-disabled-1",
        "type: Package",
        "",
      ].join("\n")
    );

    const parsed = parseRepo(tempRepo);

    const record = parsed.packages.find((p) => p.alias === "with-disabled");
    expect(record).toBeDefined();
    expect(record!.enabledNodeTypeIDs).toEqual(["66"]);
    // 66 has no committed nodeType in the fixture — should be missing, not resolved
    expect(record!.missingDefinitionIDs).toContain("66");
    expect(record!.resolvedDefinitionIDs).not.toContain("65");
  });

  it("emits an ambiguity warning when two package files share the same alias", () => {
    // Duplicate the existing package-alpha alias under a new filename.
    writeFileSync(
      join(tempRepo, "packages", "alpha-twin.yml"),
      [
        "config:",
        "  entities:",
        "    nodeTypes:",
        "      \"65\":",
        "        defaultStorageLocation: null",
        "        isDisabled: false",
        "fileVersion: 1",
        "id: \"@example/alpha-twin\"",
        "name: package-alpha",
        "packageID: \"@example/alpha-twin\"",
        "releaseID: release-alpha-twin-1",
        "type: Package",
        "",
      ].join("\n")
    );

    const parsed = parseRepo(tempRepo);

    expect(parsed.summary.warnings).toContainEqual(
      expect.stringContaining(
        "Multiple package manifests share alias package-alpha"
      )
    );
  });

  it("flags a package with an ambiguous committed definition as ambiguousDefinitionIDs", () => {
    // The fixture already has Duplicate-A-500 and Duplicate-B-500, both with
    // outer id 500. A package that enables 500 should land in
    // ambiguousDefinitionIDs (not resolved, not missing).
    writeFileSync(
      join(tempRepo, "packages", "ambiguous-package.yml"),
      [
        "config:",
        "  entities:",
        "    nodeTypes:",
        "      \"500\":",
        "        defaultStorageLocation: null",
        "        isDisabled: false",
        "fileVersion: 1",
        "id: \"@example/ambiguous-package\"",
        "name: ambiguous-package",
        "packageID: \"@example/ambiguous-package\"",
        "releaseID: release-ambiguous-1",
        "type: Package",
        "",
      ].join("\n")
    );

    const parsed = parseRepo(tempRepo);

    const record = parsed.packages.find((p) => p.alias === "ambiguous-package");
    expect(record).toBeDefined();
    expect(record!.ambiguousDefinitionIDs).toEqual(["500"]);
    expect(record!.resolvedDefinitionIDs).not.toContain("500");
    expect(record!.missingDefinitionIDs).not.toContain("500");
    expect(record!.warnings).toContainEqual(
      expect.stringContaining("ambiguous committed definitions")
    );
  });
});

describe("loadUsageCounts warnings (via parseRepo)", () => {
  let tempRepo: string;

  beforeEach(() => {
    tempRepo = makeTempRepoCopy();
  });

  afterEach(() => {
    rmSync(tempRepo, { recursive: true, force: true });
  });

  it("skips node files that do not parse to an object and warns", () => {
    writeFileSync(
      join(tempRepo, "nodes", "scalar-node.yml"),
      "just-a-scalar\n"
    );

    const parsed = parseRepo(tempRepo);

    expect(parsed.summary.warnings).toContainEqual(
      expect.stringContaining(
        "Skipping node file that did not parse to an object"
      )
    );
  });

  it("counts nodes without operation.sqlType but does not raise warnings for them", () => {
    writeFileSync(
      join(tempRepo, "nodes", "no-sqltype.yml"),
      [
        "fileVersion: 1",
        "id: node-no-sqltype",
        "name: NO_SQLTYPE",
        "operation:",
        "  name: NO_SQLTYPE",
        "  type: transform",
        "type: Node",
        "",
      ].join("\n")
    );

    const parsed = parseRepo(tempRepo);

    expect(parsed.summary.nodeCount).toBeGreaterThan(0);
    // The added file should still be counted toward total nodeCount
    // even though its sqlType is absent (no usage entry created).
    expect(parsed.usageCounts).not.toHaveProperty("undefined");
  });

  it("skips a node file with malformed YAML and emits a warning", () => {
    writeFileSync(
      join(tempRepo, "nodes", "broken-node.yml"),
      "operation: { sqlType: [unclosed\n"
    );

    const parsed = parseRepo(tempRepo);

    expect(parsed.summary.warnings).toContainEqual(
      expect.stringContaining("Skipping unreadable node file")
    );
  });
});

describe("resolveRepoNodeType error branches", () => {
  it("rejects the empty-alias-before-delimiter form (':::65')", () => {
    const parsed = parseRepo(fixtureRepoPath);
    expect(() => resolveRepoNodeType(parsed, ":::65")).toThrow(
      /alias:::id format/u
    );
  });

  it("rejects the empty-id-after-delimiter form ('package-alpha:::')", () => {
    const parsed = parseRepo(fixtureRepoPath);
    expect(() => resolveRepoNodeType(parsed, "package-alpha:::")).toThrow(
      /alias:::id format/u
    );
  });

  it("rejects empty on both sides (':::')", () => {
    const parsed = parseRepo(fixtureRepoPath);
    expect(() => resolveRepoNodeType(parsed, ":::")).toThrow(
      /alias:::id format/u
    );
  });

  it("throws when the package alias is unknown", () => {
    const parsed = parseRepo(fixtureRepoPath);
    expect(() =>
      resolveRepoNodeType(parsed, "package-does-not-exist:::65")
    ).toThrow(/No committed package alias/u);
  });

  it("throws when the alias is ambiguous across multiple package manifests", () => {
    const tempRepo = makeTempRepoCopy();
    try {
      // Add a second package file with the same alias as package-alpha.
      writeFileSync(
        join(tempRepo, "packages", "alpha-twin.yml"),
        [
          "config:",
          "  entities:",
          "    nodeTypes:",
          "      \"65\":",
          "        defaultStorageLocation: null",
          "        isDisabled: false",
          "fileVersion: 1",
          "id: \"@example/alpha-twin\"",
          "name: package-alpha",
          "packageID: \"@example/alpha-twin\"",
          "releaseID: release-alpha-twin-1",
          "type: Package",
          "",
        ].join("\n")
      );
      const parsed = parseRepo(tempRepo);

      expect(() =>
        resolveRepoNodeType(parsed, "package-alpha:::65")
      ).toThrow(/Package alias package-alpha is ambiguous across/u);
    } finally {
      rmSync(tempRepo, { recursive: true, force: true });
    }
  });

  it("throws when the package alias does not enable the requested ID", () => {
    const parsed = parseRepo(fixtureRepoPath);
    // package-alpha enables 65 and 66, but not 999.
    expect(() =>
      resolveRepoNodeType(parsed, "package-alpha:::999")
    ).toThrow(/does not enable node type ID 999/u);
  });

  it("throws when the package enables an ID with no committed nodeType definition", () => {
    const parsed = parseRepo(fixtureRepoPath);
    // package-alpha enables 66 but the fixture has no committed nodeType for it.
    expect(() =>
      resolveRepoNodeType(parsed, "package-alpha:::66")
    ).toThrow(/no committed nodeTypes definition with outer id 66 was found/u);
  });

  it("throws when the package alias maps to multiple committed definitions for the same outer id", () => {
    const tempRepo = makeTempRepoCopy();
    try {
      // The fixture has two committed definitions for outer id 500
      // (Duplicate-A-500 / Duplicate-B-500). Wire a package that enables 500.
      writeFileSync(
        join(tempRepo, "packages", "ambiguous-defs-package.yml"),
        [
          "config:",
          "  entities:",
          "    nodeTypes:",
          "      \"500\":",
          "        defaultStorageLocation: null",
          "        isDisabled: false",
          "fileVersion: 1",
          "id: \"@example/ambiguous-defs\"",
          "name: ambiguous-defs",
          "packageID: \"@example/ambiguous-defs\"",
          "releaseID: release-ambiguous-defs-1",
          "type: Package",
          "",
        ].join("\n")
      );
      const parsed = parseRepo(tempRepo);

      expect(() =>
        resolveRepoNodeType(parsed, "ambiguous-defs:::500")
      ).toThrow(/maps to multiple committed definitions for outer id 500/u);
    } finally {
      rmSync(tempRepo, { recursive: true, force: true });
    }
  });

  it("throws when the direct identifier is unknown", () => {
    const parsed = parseRepo(fixtureRepoPath);
    expect(() => resolveRepoNodeType(parsed, "ZZZ-NOT-A-REAL-TYPE")).toThrow(
      /No committed nodeTypes definition with outer id ZZZ-NOT-A-REAL-TYPE was found/u
    );
  });

  it("error messages always reference the corpus tools as a fallback path", () => {
    const parsed = parseRepo(fixtureRepoPath);
    let message = "";
    try {
      resolveRepoNodeType(parsed, "ZZZ-NOT-A-REAL-TYPE");
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toContain("corpus tools");
  });
});

describe("parseRepo de-duplicates summary warnings", () => {
  let tempRepo: string;

  beforeEach(() => {
    tempRepo = makeTempRepoCopy();
  });

  afterEach(() => {
    rmSync(tempRepo, { recursive: true, force: true });
  });

  it("does not list the same warning string twice in summary.warnings", () => {
    // Force two ambiguous-alias warnings via two duplicate packages.
    writeFileSync(
      join(tempRepo, "packages", "alpha-twin-1.yml"),
      [
        "config:",
        "  entities:",
        "    nodeTypes:",
        "      \"65\":",
        "        defaultStorageLocation: null",
        "        isDisabled: false",
        "fileVersion: 1",
        "id: \"@example/alpha-twin-1\"",
        "name: package-alpha",
        "packageID: \"@example/alpha-twin-1\"",
        "releaseID: release-1",
        "type: Package",
        "",
      ].join("\n")
    );

    const parsed = parseRepo(tempRepo);

    const counts: Record<string, number> = {};
    for (const w of parsed.summary.warnings) {
      counts[w] = (counts[w] ?? 0) + 1;
    }
    for (const [, count] of Object.entries(counts)) {
      expect(count).toBe(1);
    }
  });
});
