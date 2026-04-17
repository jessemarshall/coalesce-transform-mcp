import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  validateProjectPath,
  InvalidCoaProjectPathError,
} from "../../../src/services/coa/project.js";

let tmpRoot: string;

beforeAll(() => {
  tmpRoot = mkdtempSync(join(tmpdir(), "coa-project-test-"));
});

afterAll(() => {
  rmSync(tmpRoot, { recursive: true, force: true });
});

describe("validateProjectPath", () => {
  it("accepts a directory containing data.yml and returns the absolute path", () => {
    const projectDir = join(tmpRoot, "good-project");
    mkdirSync(projectDir);
    writeFileSync(join(projectDir, "data.yml"), "fileVersion: 3\n");
    expect(validateProjectPath(projectDir)).toBe(projectDir);
  });

  it("throws when projectPath is empty", () => {
    expect(() => validateProjectPath("")).toThrow(InvalidCoaProjectPathError);
    expect(() => validateProjectPath("")).toThrow(/required/);
  });

  it("throws when the path does not exist", () => {
    expect(() => validateProjectPath(join(tmpRoot, "nope"))).toThrow(/does not exist/);
  });

  it("throws when the path is a file, not a directory", () => {
    const filePath = join(tmpRoot, "a-file.txt");
    writeFileSync(filePath, "x");
    expect(() => validateProjectPath(filePath)).toThrow(/not a directory/);
  });

  it("throws when data.yml is missing", () => {
    const emptyDir = join(tmpRoot, "empty");
    mkdirSync(emptyDir);
    expect(() => validateProjectPath(emptyDir)).toThrow(/missing data\.yml/);
  });

  it("resolves relative paths against cwd", () => {
    const projectDir = join(tmpRoot, "relative-project");
    mkdirSync(projectDir);
    writeFileSync(join(projectDir, "data.yml"), "fileVersion: 3\n");
    // macOS /tmp is a symlink to /private/tmp — normalize both sides through realpath.
    const expected = realpathSync(projectDir);
    const prevCwd = process.cwd();
    process.chdir(realpathSync(tmpRoot));
    try {
      expect(validateProjectPath("relative-project")).toBe(expected);
    } finally {
      process.chdir(prevCwd);
    }
  });
});
