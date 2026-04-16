import { describe, it, expect, beforeEach, afterEach } from "vitest";
import {
  mkdtempSync,
  mkdirSync,
  writeFileSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  runPreflight,
  CoaPreflightError,
  summarizePreflight,
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
