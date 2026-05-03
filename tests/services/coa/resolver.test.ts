import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  resolveCoaBinary,
  resetCoaBinaryCache,
  CoaNotFoundError,
  isExecutableFile,
} from "../../../src/services/coa/resolver.js";

describe("resolveCoaBinary (integration with bundled @coalescesoftware/coa)", () => {
  beforeEach(() => {
    resetCoaBinaryCache();
  });

  it("resolves the bundled coa.js from node_modules", () => {
    const resolved = resolveCoaBinary();
    expect(resolved.source).toBe("bundled");
    expect(resolved.binaryPath).toMatch(/@coalescesoftware[\/\\]coa[\/\\]coa\.js$/);
    // Version may be null on machines where the probe fails (e.g., missing Python runtime),
    // but when the probe succeeds the string must look like a version tag.
    if (resolved.version !== null) {
      expect(resolved.version.length).toBeGreaterThan(0);
    }
  });

  it("caches the result across calls", () => {
    const first = resolveCoaBinary();
    const second = resolveCoaBinary();
    expect(second).toBe(first);
  });

  // Each cold resolve probes the bundled coa.js for its version, which can
  // take ~2-3s; doing it twice in one test pushes past vitest's default 5s
  // timeout under full-suite load. Bump the budget so this isn't flaky.
  it("resets via resetCoaBinaryCache", () => {
    const first = resolveCoaBinary();
    resetCoaBinaryCache();
    const second = resolveCoaBinary();
    expect(second).not.toBe(first);
    expect(second.binaryPath).toBe(first.binaryPath);
  }, 15000);
});

describe("CoaNotFoundError", () => {
  it("has the expected name", () => {
    const err = new CoaNotFoundError("missing");
    expect(err.name).toBe("CoaNotFoundError");
    expect(err.message).toBe("missing");
    expect(err).toBeInstanceOf(Error);
  });
});

describe("isExecutableFile", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "coalesce-resolver-test-"));
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("returns false for a path that does not exist", () => {
    expect(isExecutableFile(join(tempDir, "nonexistent"))).toBe(false);
  });

  it("returns false for a directory even when one named `coa` shadows PATH", () => {
    const dirCandidate = join(tempDir, "coa");
    mkdirSync(dirCandidate);
    expect(isExecutableFile(dirCandidate)).toBe(false);
  });

  // chmod is meaningful on POSIX only; Windows treats files as executable
  // based on PATHEXT, which the helper short-circuits past.
  it.skipIf(process.platform === "win32")(
    "returns false for a regular file without the executable bit",
    () => {
      const fileCandidate = join(tempDir, "coa");
      writeFileSync(fileCandidate, "#!/bin/sh\necho hello\n", "utf8");
      chmodSync(fileCandidate, 0o644);
      expect(isExecutableFile(fileCandidate)).toBe(false);
    }
  );

  it.skipIf(process.platform === "win32")(
    "returns true for a regular file with the executable bit set",
    () => {
      const fileCandidate = join(tempDir, "coa");
      writeFileSync(fileCandidate, "#!/bin/sh\necho hello\n", "utf8");
      chmodSync(fileCandidate, 0o755);
      expect(isExecutableFile(fileCandidate)).toBe(true);
    }
  );
});
