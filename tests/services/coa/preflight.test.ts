import { describe, it, expect, beforeEach, afterEach } from "vitest";
import {
  chmodSync,
  mkdtempSync,
  mkdirSync,
  readdirSync,
  writeFileSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  runPreflight,
  CoaPreflightError,
  detectV2Artifacts,
  summarizePreflight,
  pathExists,
  readLocationNames,
} from "../../../src/services/coa/preflight.js";

let projectDir: string;

beforeEach(() => {
  projectDir = mkdtempSync(join(tmpdir(), "coa-preflight-test-"));
  writeFileSync(join(projectDir, "data.yml"), "fileVersion: 3\n");
  mkdirSync(join(projectDir, "nodes"));
});

afterEach(() => {
  rmSync(projectDir, { recursive: true, force: true });
});

function withSql(path: string, content: string): void {
  const fullPath = join(projectDir, "nodes", path);
  mkdirSync(join(fullPath, ".."), { recursive: true });
  writeFileSync(fullPath, content);
}

describe("runPreflight - data.yml", () => {
  it("passes cleanly when fileVersion is 3", () => {
    const report = runPreflight(projectDir);
    expect(report.errors).toEqual([]);
    expect(report.warnings).toEqual([]);
  });

  it("warns when fileVersion is missing", () => {
    writeFileSync(join(projectDir, "data.yml"), "# no fileversion\n");
    const report = runPreflight(projectDir);
    expect(report.errors).toEqual([]);
    expect(report.warnings.map((w) => w.code)).toContain("DATA_YML_NO_FILEVERSION");
  });

  it("warns when fileVersion is not 3", () => {
    writeFileSync(join(projectDir, "data.yml"), "fileVersion: 1\n");
    const report = runPreflight(projectDir);
    expect(report.errors).toEqual([]);
    expect(report.warnings.map((w) => w.code)).toContain("DATA_YML_UNEXPECTED_FILEVERSION");
  });
});

describe("runPreflight - workspaces.yml", () => {
  it("passes when requireWorkspacesYml=false and file is missing (default)", () => {
    const report = runPreflight(projectDir);
    expect(report.errors).toEqual([]);
  });

  it("errors when requireWorkspacesYml=true and file is missing", () => {
    const report = runPreflight(projectDir, { requireWorkspacesYml: true });
    expect(report.errors.map((e) => e.code)).toContain("WORKSPACES_YML_MISSING");
  });

  it("passes when requireWorkspacesYml=true and workspaces.yml exists", () => {
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "[default]\nsomething: here\n"
    );
    const report = runPreflight(projectDir, { requireWorkspacesYml: true });
    expect(report.errors).toEqual([]);
  });
});

describe("runPreflight - workspaces.yml shape", () => {
  it("passes a well-shaped workspaces.yml without warnings", () => {
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "dev:\n  connection: snowflake\n  locations:\n    SRC_A:\n      database: DEV\n      schema: SRC_A\n"
    );
    const report = runPreflight(projectDir);
    const codes = report.warnings.map((w) => w.code);
    expect(codes).not.toContain("WORKSPACES_YML_NESTED_WRAPPER");
    expect(codes).not.toContain("WORKSPACES_YML_WRONG_LOCATIONS_KEY");
    expect(codes).not.toContain("WORKSPACES_YML_MISSING_CONNECTION");
  });

  it("warns on nested `workspaces:` wrapper", () => {
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "workspaces:\n  dev:\n    connection: snowflake\n"
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("WORKSPACES_YML_NESTED_WRAPPER");
  });

  it("warns on `storageLocations` instead of `locations`", () => {
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "dev:\n  connection: snowflake\n  storageLocations:\n    SRC_A:\n      database: DEV\n      schema: SRC_A\n"
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("WORKSPACES_YML_WRONG_LOCATIONS_KEY");
  });

  it("warns on unexpected `fileVersion`", () => {
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "fileVersion: 3\ndev:\n  connection: snowflake\n"
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("WORKSPACES_YML_UNEXPECTED_FILEVERSION");
  });

  it("warns when a workspace is missing `connection`", () => {
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "dev:\n  locations:\n    SRC_A:\n      database: DEV\n      schema: SRC_A\n"
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("WORKSPACES_YML_MISSING_CONNECTION");
  });

  it("warns when YAML fails to parse", () => {
    writeFileSync(join(projectDir, "workspaces.yml"), "dev:\n  connection: [unclosed\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("WORKSPACES_YML_PARSE_FAILED");
  });
});

describe("runPreflight - workspaces.yml gitignore", () => {
  beforeEach(() => {
    // Pretend the project is a git repo so gitignore is evaluated.
    mkdirSync(join(projectDir, ".git"));
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "dev:\n  connection: snowflake\n"
    );
  });

  it("warns when .gitignore is missing in a git repo", () => {
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("WORKSPACES_YML_NOT_GITIGNORED");
  });

  it("warns when .gitignore exists but does not match workspaces.yml", () => {
    writeFileSync(join(projectDir, ".gitignore"), "node_modules/\n*.log\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("WORKSPACES_YML_NOT_GITIGNORED");
  });

  it("does not warn when workspaces.yml is listed in .gitignore", () => {
    writeFileSync(join(projectDir, ".gitignore"), "workspaces.yml\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain("WORKSPACES_YML_NOT_GITIGNORED");
  });

  it("does not warn when a broader pattern covers it (*.yml)", () => {
    writeFileSync(join(projectDir, ".gitignore"), "*.yml\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain("WORKSPACES_YML_NOT_GITIGNORED");
  });
});

describe("runPreflight - locations.yml shape", () => {
  it("warns when locations.yml fails to parse", () => {
    writeFileSync(join(projectDir, "locations.yml"), "SRC_A:\n  type: [unclosed\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("LOCATIONS_YML_PARSE_FAILED");
  });

  it("warns when locations.yml parses to a non-object", () => {
    writeFileSync(join(projectDir, "locations.yml"), "- just\n- a\n- list\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("LOCATIONS_YML_INVALID_SHAPE");
  });

  it("does not warn when locations.yml is missing", () => {
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain("LOCATIONS_YML_PARSE_FAILED");
    expect(report.warnings.map((w) => w.code)).not.toContain("LOCATIONS_YML_INVALID_SHAPE");
  });
});

describe("runPreflight - read-failure warnings on unreadable files", () => {
  // POSIX chmod 0 makes the file unreadable so readFileSync throws EACCES.
  // Skipped on Windows (chmod no-op) and when running as root (root bypasses DAC).
  const canTestFsPermissionDenial =
    process.platform !== "win32" && process.getuid?.() !== 0;

  it.skipIf(!canTestFsPermissionDenial)(
    "emits LOCATIONS_YML_READ_FAILED when locations.yml exists but cannot be read",
    () => {
      const path = join(projectDir, "locations.yml");
      writeFileSync(path, "SRC_A:\n  type: snowflake\n");
      chmodSync(path, 0o000);
      try {
        const report = runPreflight(projectDir);
        expect(report.warnings.map((w) => w.code)).toContain("LOCATIONS_YML_READ_FAILED");
      } finally {
        chmodSync(path, 0o600);
      }
    }
  );

  it.skipIf(!canTestFsPermissionDenial)(
    "emits WORKSPACES_YML_READ_FAILED when workspaces.yml exists but cannot be read",
    () => {
      const path = join(projectDir, "workspaces.yml");
      writeFileSync(path, "dev:\n  connection: snowflake\n");
      chmodSync(path, 0o000);
      try {
        const report = runPreflight(projectDir);
        expect(report.warnings.map((w) => w.code)).toContain("WORKSPACES_YML_READ_FAILED");
      } finally {
        chmodSync(path, 0o600);
      }
    }
  );

  it.skipIf(!canTestFsPermissionDenial)(
    "emits GITIGNORE_READ_FAILED when .gitignore exists but cannot be read",
    () => {
      mkdirSync(join(projectDir, ".git"));
      writeFileSync(join(projectDir, "workspaces.yml"), "dev:\n  connection: snowflake\n");
      const gitignorePath = join(projectDir, ".gitignore");
      writeFileSync(gitignorePath, "node_modules/\n");
      chmodSync(gitignorePath, 0o000);
      try {
        const report = runPreflight(projectDir);
        expect(report.warnings.map((w) => w.code)).toContain("GITIGNORE_READ_FAILED");
      } finally {
        chmodSync(gitignorePath, 0o600);
      }
    }
  );
});

describe("runPreflight - workspaces.yml cross-reference with locations.yml", () => {
  it("warns when a location key is not declared in locations.yml", () => {
    writeFileSync(
      join(projectDir, "locations.yml"),
      "SRC_KNOWN:\n  type: snowflake\n"
    );
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "dev:\n  connection: snowflake\n  locations:\n    SRC_UNKNOWN:\n      database: D\n      schema: S\n"
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("WORKSPACES_YML_UNKNOWN_LOCATION");
  });

  it("does not warn when all location keys match", () => {
    writeFileSync(
      join(projectDir, "locations.yml"),
      "SRC_A:\n  type: snowflake\nSRC_B:\n  type: snowflake\n"
    );
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "dev:\n  connection: snowflake\n  locations:\n    SRC_A:\n      database: D\n      schema: S\n    SRC_B:\n      database: D\n      schema: S\n"
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain("WORKSPACES_YML_UNKNOWN_LOCATION");
  });

  it("skips the cross-reference silently when locations.yml is missing or malformed", () => {
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "dev:\n  connection: snowflake\n  locations:\n    SRC_ANY:\n      database: D\n      schema: S\n"
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain("WORKSPACES_YML_UNKNOWN_LOCATION");
  });
});

describe("CoaPreflightError setup hint", () => {
  it("appends /coalesce-setup hint when WORKSPACES_YML_MISSING is in the report", () => {
    const report = runPreflight(projectDir, { requireWorkspacesYml: true });
    const err = new CoaPreflightError(report);
    expect(err.message).toContain("/coalesce-setup");
  });

  it("does not append the hint for non-setup errors", () => {
    withSql("X.sql", `SELECT * FROM {{ ref("SRC","A") }}\n`);
    const report = runPreflight(projectDir);
    const err = new CoaPreflightError(report);
    expect(err.message).not.toContain("/coalesce-setup");
  });
});

describe("runPreflight - SQL double-quoted ref", () => {
  it("errors on .sql files using double-quoted ref()", () => {
    withSql(
      "TARGET-STG.sql",
      `@id("x")\nSELECT * FROM {{ ref("SRC", "ORDERS") }}\n`
    );
    const report = runPreflight(projectDir);
    expect(report.errors.map((e) => e.code)).toContain("SQL_DOUBLE_QUOTED_REF");
    expect(report.errors[0].path).toContain("TARGET-STG.sql");
  });

  it("does not flag single-quoted ref()", () => {
    withSql(
      "TARGET-STG.sql",
      `@id("x")\nSELECT * FROM {{ ref('SRC', 'ORDERS') }}\n`
    );
    const report = runPreflight(projectDir);
    expect(report.errors.map((e) => e.code)).not.toContain("SQL_DOUBLE_QUOTED_REF");
  });

  it("walks nested node directories", () => {
    withSql(
      "sub/dir/NESTED-STG.sql",
      `SELECT * FROM {{ ref("A","B") }}\n`
    );
    const report = runPreflight(projectDir);
    expect(report.errors.map((e) => e.code)).toContain("SQL_DOUBLE_QUOTED_REF");
  });
});

describe("runPreflight - literal UNION ALL", () => {
  it("warns on literal UNION ALL outside comments", () => {
    withSql(
      "TARGET-U.sql",
      `SELECT 1\nUNION ALL\nSELECT 2\n`
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("SQL_LITERAL_UNION_ALL");
  });

  it("does not warn when UNION ALL appears only in a line comment", () => {
    withSql(
      "TARGET-NO-U.sql",
      `SELECT 1 -- previously had UNION ALL here\n`
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain("SQL_LITERAL_UNION_ALL");
  });

  it("does not warn when UNION ALL appears only in a block comment", () => {
    withSql(
      "TARGET-BLK.sql",
      `SELECT 1 /* UNION ALL disabled */\n`
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain("SQL_LITERAL_UNION_ALL");
  });
});

describe("runPreflight - selector validation", () => {
  it("errors on the `{ A || B }` footgun", () => {
    const report = runPreflight(projectDir, {
      selectors: ["{ A || B }"],
    });
    expect(report.errors.map((e) => e.code)).toContain("SELECTOR_COMBINED_OR");
  });

  it("does not flag the valid `{ A } || { B }` form", () => {
    const report = runPreflight(projectDir, {
      selectors: ["{ A } || { B }"],
    });
    expect(report.errors.map((e) => e.code)).not.toContain("SELECTOR_COMBINED_OR");
  });

  it("ignores empty/undefined selectors", () => {
    const report = runPreflight(projectDir, {
      selectors: [undefined, "", "   "],
    });
    expect(report.errors).toEqual([]);
  });
});

describe("runPreflight - scan truncation", () => {
  it("emits a PREFLIGHT_SCAN_TRUNCATED warning when more than the cap of .sql files exist", () => {
    // Cap is 500; create 501 clean SQL files. We don't assert which ones get scanned —
    // only that the warning fires so the agent knows the scan is partial.
    for (let i = 0; i < 501; i++) {
      withSql(`STAGE-${i.toString().padStart(4, "0")}.sql`, `-- clean\nSELECT 1\n`);
    }
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("PREFLIGHT_SCAN_TRUNCATED");
  });

  it("does not emit the truncation warning for typical project sizes", () => {
    withSql("a.sql", "SELECT 1");
    withSql("b.sql", "SELECT 2");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain("PREFLIGHT_SCAN_TRUNCATED");
  });
});

describe("detectV2Artifacts + V2_ALPHA_DETECTED", () => {
  function withV2NodeType(name: string, version: number): void {
    const dir = join(projectDir, "nodeTypes", name);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "definition.yml"), `fileVersion: ${version}\nid: x\n`);
  }

  function codesOf(issues: ReturnType<typeof detectV2Artifacts>): string[] {
    return issues.map((i) => i.code);
  }

  it("returns an empty array when neither .sql nodes nor fileVersion: 2 node types exist", () => {
    expect(detectV2Artifacts(projectDir)).toEqual([]);
  });

  it("returns a warning when .sql nodes exist", () => {
    withSql("TARGET-STG.sql", "SELECT 1");
    const issues = detectV2Artifacts(projectDir);
    expect(codesOf(issues)).toEqual(["V2_ALPHA_DETECTED"]);
    expect(issues[0].message).toContain("1 `.sql` node");
  });

  it("returns a warning when fileVersion: 2 node types exist", () => {
    withV2NodeType("Stage-abc", 2);
    const issues = detectV2Artifacts(projectDir);
    expect(codesOf(issues)).toEqual(["V2_ALPHA_DETECTED"]);
    expect(issues[0].message).toContain("1 `fileVersion: 2` node type");
  });

  it("ignores fileVersion: 1 node types", () => {
    withV2NodeType("Stage-v1", 1);
    expect(detectV2Artifacts(projectDir)).toEqual([]);
  });

  it("reports both counts when both are present", () => {
    withV2NodeType("Stage-abc", 2);
    withSql("A.sql", "SELECT 1");
    withSql("B.sql", "SELECT 2");
    const issues = detectV2Artifacts(projectDir);
    const alpha = issues.find((i) => i.code === "V2_ALPHA_DETECTED");
    expect(alpha?.message).toContain("1 `fileVersion: 2` node type");
    expect(alpha?.message).toContain("2 `.sql` nodes");
    expect(alpha?.message).toContain("coalesce://context/sql-node-v2-policy");
  });

  it("is surfaced through runPreflight warnings", () => {
    withSql("TARGET-STG.sql", "SELECT 1");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain("V2_ALPHA_DETECTED");
  });

  it("is not surfaced for pure V1 projects", () => {
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain("V2_ALPHA_DETECTED");
  });

  // When the `.sql` node scan hits MAX_SQL_FILES_SCANNED, `detectV2Artifacts`
  // must treat that as a scan failure, not a clean V1 count — otherwise large
  // projects bypass the V2 hard guard silently.
  it("fires V2_SCAN_FAILED when the .sql node scan hits the file cap", () => {
    for (let i = 0; i < 501; i++) {
      withSql(`STAGE-${i.toString().padStart(4, "0")}.sql`, "SELECT 1");
    }
    const issues = detectV2Artifacts(projectDir);
    expect(codesOf(issues)).toContain("V2_SCAN_FAILED");
  });

  // POSIX `chmod 0` removes read/exec on the dir so readdir throws EACCES.
  // Skipped on Windows (chmod no-op) and when running as root (root bypasses
  // POSIX DAC). Using `it.skipIf` so vitest reports this as skipped, not
  // silently "passed" when the environment can't exercise the branch.
  const canTestFsPermissionDenial =
    process.platform !== "win32" && process.getuid?.() !== 0;

  it.skipIf(!canTestFsPermissionDenial)(
    "emits V2_SCAN_FAILED when a nested nodes/ dir cannot be read",
    () => {
      const locked = join(projectDir, "nodes", "locked");
      mkdirSync(locked);
      chmodSync(locked, 0o000);
      try {
        // Environmental sanity check: if chmod 0 doesn't actually deny readdir
        // (e.g., overlayfs in some containers), don't assert — the branch
        // we're testing is unreachable in this environment.
        let canReach = false;
        try {
          readdirSync(locked);
          canReach = true;
        } catch {
          /* expected */
        }
        if (canReach) return;
        const issues = detectV2Artifacts(projectDir);
        expect(issues.map((i) => i.code)).toContain("V2_SCAN_FAILED");
        const scan = issues.find((i) => i.code === "V2_SCAN_FAILED");
        expect(scan?.message).toContain("readdir failed");
      } finally {
        chmodSync(locked, 0o700);
      }
    }
  );
});

describe("runPreflight - workspaces.yml non-object shapes", () => {
  it("warns when workspaces.yml parses to a YAML list", () => {
    writeFileSync(join(projectDir, "workspaces.yml"), "- one\n- two\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain(
      "WORKSPACES_YML_INVALID_SHAPE"
    );
  });

  it("warns when workspaces.yml parses to a scalar", () => {
    writeFileSync(join(projectDir, "workspaces.yml"), "just-a-string\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain(
      "WORKSPACES_YML_INVALID_SHAPE"
    );
  });

  it("does not emit unknown-location warning when the workspace has no locations block", () => {
    writeFileSync(
      join(projectDir, "locations.yml"),
      "SRC_KNOWN:\n  type: snowflake\n"
    );
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "dev:\n  connection: snowflake\n"
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain(
      "WORKSPACES_YML_UNKNOWN_LOCATION"
    );
  });
});

describe("runPreflight - .gitignore ignore-hint heuristic", () => {
  beforeEach(() => {
    mkdirSync(join(projectDir, ".git"));
    writeFileSync(
      join(projectDir, "workspaces.yml"),
      "dev:\n  connection: snowflake\n"
    );
  });

  it("recognizes a bare `*` as covering workspaces.yml", () => {
    writeFileSync(join(projectDir, ".gitignore"), "*\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain(
      "WORKSPACES_YML_NOT_GITIGNORED"
    );
  });

  it("recognizes `*.yaml` as covering workspaces.yml", () => {
    writeFileSync(
      join(projectDir, "workspaces.yaml"),
      "dev:\n  connection: snowflake\n"
    );
    rmSync(join(projectDir, "workspaces.yml"));
    writeFileSync(join(projectDir, ".gitignore"), "*.yaml\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain(
      "WORKSPACES_YML_NOT_GITIGNORED"
    );
  });

  it("recognizes the `workspaces.*` wildcard pattern", () => {
    writeFileSync(join(projectDir, ".gitignore"), "workspaces.*\n");
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain(
      "WORKSPACES_YML_NOT_GITIGNORED"
    );
  });

  it("ignores comments and negation lines", () => {
    // A `!workspaces.yml` negation means "force include" — the heuristic must
    // not treat it as coverage.
    writeFileSync(
      join(projectDir, ".gitignore"),
      "# workspaces.yml\n!workspaces.yml\nnode_modules/\n"
    );
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).toContain(
      "WORKSPACES_YML_NOT_GITIGNORED"
    );
  });

  it("is silent when the project is not a git repo and .gitignore is missing", () => {
    rmSync(join(projectDir, ".git"), { recursive: true, force: true });
    const report = runPreflight(projectDir);
    expect(report.warnings.map((w) => w.code)).not.toContain(
      "WORKSPACES_YML_NOT_GITIGNORED"
    );
  });
});

describe("readLocationNames", () => {
  it("returns [] when locations.yml is missing", () => {
    expect(readLocationNames(projectDir)).toEqual([]);
  });

  it("returns [] when locations.yml is malformed", () => {
    writeFileSync(join(projectDir, "locations.yml"), "[unclosed\n");
    expect(readLocationNames(projectDir)).toEqual([]);
  });

  it("reads top-level keys (flat shape)", () => {
    writeFileSync(
      join(projectDir, "locations.yml"),
      "SRC_A:\n  type: snowflake\nSRC_B:\n  type: snowflake\n"
    );
    expect(readLocationNames(projectDir).sort()).toEqual(["SRC_A", "SRC_B"]);
  });

  it("reads keys under a nested `locations:` wrapper", () => {
    writeFileSync(
      join(projectDir, "locations.yml"),
      "locations:\n  SRC_A:\n    type: snowflake\n  SRC_B:\n    type: snowflake\n"
    );
    expect(readLocationNames(projectDir).sort()).toEqual(["SRC_A", "SRC_B"]);
  });

  it("filters out the `fileVersion` key", () => {
    writeFileSync(
      join(projectDir, "locations.yml"),
      "fileVersion: 1\nSRC_A:\n  type: snowflake\n"
    );
    expect(readLocationNames(projectDir)).toEqual(["SRC_A"]);
  });
});

describe("pathExists", () => {
  it("returns true for an existing file", () => {
    const path = join(projectDir, "data.yml");
    expect(pathExists(path)).toBe(true);
  });

  it("returns false for a directory (file-only check)", () => {
    const path = join(projectDir, "nodes");
    expect(pathExists(path)).toBe(false);
  });

  it("returns false for a missing path", () => {
    expect(pathExists(join(projectDir, "does-not-exist"))).toBe(false);
  });
});

describe("CoaPreflightError + summarizePreflight", () => {
  it("summarizes both errors and warnings", () => {
    writeFileSync(join(projectDir, "data.yml"), "fileVersion: 1\n");
    withSql(
      "TARGET-BAD.sql",
      `SELECT * FROM {{ ref("A","B") }}\n`
    );
    const report = runPreflight(projectDir, { requireWorkspacesYml: true });
    const text = summarizePreflight(report);
    expect(text).toContain("ERROR");
    expect(text).toContain("WARN");
    expect(text).toContain("SQL_DOUBLE_QUOTED_REF");
    expect(text).toContain("WORKSPACES_YML_MISSING");
    expect(text).toContain("DATA_YML_UNEXPECTED_FILEVERSION");
  });

  it("CoaPreflightError carries the full report", () => {
    const report = runPreflight(projectDir, { selectors: ["{ A || B }"] });
    const err = new CoaPreflightError(report);
    expect(err.report).toBe(report);
    expect(err.name).toBe("CoaPreflightError");
    expect(err.message).toContain("SELECTOR_COMBINED_OR");
  });
});
