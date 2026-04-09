import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import {
  resolveRepoPathInput,
  resolveOptionalRepoPathInput,
} from "../../src/services/repo/path.js";

const fixtureRepoPath = resolve("tests/fixtures/repo-backed-coalesce");

describe("resolveRepoPathInput validation", () => {
  const originalEnv = process.env;
  const tempDirs: string[] = [];

  afterEach(() => {
    process.env = originalEnv;
    for (const dir of tempDirs.splice(0, tempDirs.length)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  function createTempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), "coalesce-repo-path-test-"));
    tempDirs.push(dir);
    return dir;
  }

  it("returns resolved path for a valid Coalesce repo", () => {
    const result = resolveRepoPathInput(fixtureRepoPath);
    expect(result).toBe(resolve(fixtureRepoPath));
  });

  it("rejects a path that does not exist", () => {
    expect(() => resolveRepoPathInput("/nonexistent/path")).toThrow(
      "repoPath does not exist"
    );
  });

  it("rejects a file instead of a directory", () => {
    const dir = createTempDir();
    const filePath = join(dir, "not-a-dir.txt");
    writeFileSync(filePath, "hello");
    expect(() => resolveRepoPathInput(filePath)).toThrow(
      "repoPath is not a directory"
    );
  });

  it("rejects a directory without nodeTypes/", () => {
    const dir = createTempDir();
    expect(() => resolveRepoPathInput(dir)).toThrow(
      "missing nodeTypes/ subdirectory"
    );
  });

  it("rejects /etc as not a valid Coalesce repo", () => {
    expect(() => resolveRepoPathInput("/etc")).toThrow(
      "missing nodeTypes/ subdirectory"
    );
  });

  it("rejects path traversal attempts without leaking the resolved path", () => {
    try {
      resolveRepoPathInput("../../etc");
      expect.unreachable("should have thrown");
    } catch (e: unknown) {
      const message = (e as Error).message;
      // Should fail validation (no nodeTypes/ or doesn't exist)
      expect(message).toMatch(/does not exist|missing nodeTypes/);
      // Must not contain the resolved absolute path
      expect(message).not.toContain(resolve("../../etc"));
    }
  });

  it("does not expose filesystem paths in error messages", () => {
    const dir = createTempDir();
    try {
      resolveRepoPathInput(dir);
      expect.unreachable("should have thrown");
    } catch (e: unknown) {
      const message = (e as Error).message;
      expect(message).not.toContain(dir);
    }
  });

  it("returns the configured env var path in optional mode without validating it", () => {
    const dir = createTempDir();
    process.env = { ...originalEnv, COALESCE_REPO_PATH: dir };
    expect(resolveOptionalRepoPathInput()).toBe(dir);
  });

  it("validates env var path that is a valid repo", () => {
    process.env = { ...originalEnv, COALESCE_REPO_PATH: fixtureRepoPath };
    const result = resolveOptionalRepoPathInput();
    expect(result).toBe(resolve(fixtureRepoPath));
  });

  it("accepts a directory that has nodeTypes/ even without nodes/ or packages/", () => {
    const dir = createTempDir();
    mkdirSync(join(dir, "nodeTypes"));
    const result = resolveRepoPathInput(dir);
    // realpathSync may resolve symlinks (e.g. /var → /private/var on macOS)
    expect(result).toContain("coalesce-repo-path-test-");
    expect(typeof result).toBe("string");
  });
});
