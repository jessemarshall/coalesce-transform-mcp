import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync, existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  COA_DESCRIBE_TOPICS,
  fetchDescribeTopic,
  resetCoaDescribeMemoryCache,
  isCoaDescribeTopic,
  CoaDescribeError,
  applyCoalesceCorrections,
  SQL_FORMAT_DBT_SOURCE_BANNER,
} from "../../../src/services/coa/describe.js";
import type { RunCoaResult } from "../../../src/services/coa/runner.js";

let cacheDir: string;

beforeEach(() => {
  cacheDir = mkdtempSync(join(tmpdir(), "coa-describe-cache-"));
  resetCoaDescribeMemoryCache();
});

afterEach(() => {
  rmSync(cacheDir, { recursive: true, force: true });
});

function fakeRun(
  canned: Partial<RunCoaResult>
): {
  calls: Array<{ args: string[] }>;
  runCoaFn: (args: string[], options?: unknown) => Promise<RunCoaResult>;
} {
  const calls: Array<{ args: string[] }> = [];
  const runCoaFn = async (args: string[]) => {
    calls.push({ args });
    return {
      exitCode: 0,
      timedOut: false,
      stdout: "",
      stderr: "",
      ...canned,
    } satisfies RunCoaResult;
  };
  return { calls, runCoaFn };
}

describe("isCoaDescribeTopic", () => {
  it("accepts known topics", () => {
    for (const topic of COA_DESCRIBE_TOPICS) {
      expect(isCoaDescribeTopic(topic)).toBe(true);
    }
  });

  it("rejects unknown topics", () => {
    expect(isCoaDescribeTopic("not-a-real-topic")).toBe(false);
  });
});

describe("fetchDescribeTopic", () => {
  it("shells out with --no-color describe <topic> on a cold fetch", async () => {
    const { calls, runCoaFn } = fakeRun({ stdout: "# selectors topic" });
    const result = await fetchDescribeTopic("selectors", {
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });
    expect(calls[0].args).toEqual(["--no-color", "describe", "selectors"]);
    expect(result.source).toBe("coa");
    expect(result.content).toBe("# selectors topic");
    expect(result.coaVersion).toBe("7.0.0");
  });

  it("writes to disk cache after a COA fetch", async () => {
    const { runCoaFn } = fakeRun({ stdout: "content-v1" });
    await fetchDescribeTopic("selectors", {
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });
    const diskPath = join(cacheDir, "7.0.0", "selectors.md");
    expect(existsSync(diskPath)).toBe(true);
    expect(readFileSync(diskPath, "utf8")).toBe("content-v1");
  });

  it("returns from in-memory cache on a subsequent call", async () => {
    const { calls, runCoaFn } = fakeRun({ stdout: "cached" });
    await fetchDescribeTopic("selectors", {
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });
    const second = await fetchDescribeTopic("selectors", {
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });
    expect(second.source).toBe("memory");
    expect(second.content).toBe("cached");
    expect(calls).toHaveLength(1);
  });

  it("returns from disk cache when memory is cold", async () => {
    // Pre-populate the disk cache.
    const versionDir = join(cacheDir, "7.0.0");
    mkdirSync(versionDir, { recursive: true });
    writeFileSync(join(versionDir, "selectors.md"), "from-disk", "utf8");

    const { calls, runCoaFn } = fakeRun({ stdout: "stale-coa" });
    const result = await fetchDescribeTopic("selectors", {
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });
    expect(result.source).toBe("disk");
    expect(result.content).toBe("from-disk");
    expect(calls).toHaveLength(0);
  });

  it("bypasses both caches when refresh: true", async () => {
    // Prime the caches with a stale entry.
    const priming = fakeRun({ stdout: "stale" });
    await fetchDescribeTopic("selectors", {
      runCoaFn: priming.runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });

    const { calls, runCoaFn } = fakeRun({ stdout: "fresh" });
    const result = await fetchDescribeTopic("selectors", {
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
      refresh: true,
    });
    expect(result.source).toBe("coa");
    expect(result.content).toBe("fresh");
    expect(calls).toHaveLength(1);
  });

  it("partitions cache by version — v7.0.0 content is invisible to v7.0.1", async () => {
    const primingA = fakeRun({ stdout: "v7.0.0 content" });
    await fetchDescribeTopic("selectors", {
      runCoaFn: primingA.runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });

    const { calls, runCoaFn } = fakeRun({ stdout: "v7.0.1 content" });
    const result = await fetchDescribeTopic("selectors", {
      runCoaFn,
      getVersion: () => "7.0.1",
      cacheBaseDir: cacheDir,
    });
    expect(result.content).toBe("v7.0.1 content");
    expect(calls).toHaveLength(1);
  });

  it("passes subtopic through as an additional positional arg", async () => {
    const { calls, runCoaFn } = fakeRun({ stdout: "deep-dive" });
    await fetchDescribeTopic("command", {
      subtopic: "create",
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });
    expect(calls[0].args).toEqual([
      "--no-color",
      "describe",
      "command",
      "create",
    ]);
  });

  it("caches subtopic requests separately from bare-topic requests", async () => {
    const bare = fakeRun({ stdout: "bare" });
    await fetchDescribeTopic("command", {
      runCoaFn: bare.runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });

    const { calls, runCoaFn } = fakeRun({ stdout: "sub" });
    const result = await fetchDescribeTopic("command", {
      subtopic: "create",
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });
    expect(result.content).toBe("sub");
    expect(calls).toHaveLength(1);
  });

  it("throws CoaDescribeError on non-zero exit", async () => {
    const { runCoaFn } = fakeRun({
      exitCode: 2,
      stderr: "unknown topic",
    });
    await expect(
      fetchDescribeTopic("bogus", {
        runCoaFn,
        getVersion: () => "7.0.0",
        cacheBaseDir: cacheDir,
      })
    ).rejects.toBeInstanceOf(CoaDescribeError);
  });

  it("throws CoaDescribeError on timeout", async () => {
    const { runCoaFn } = fakeRun({
      exitCode: null,
      timedOut: true,
    });
    await expect(
      fetchDescribeTopic("selectors", {
        runCoaFn,
        getVersion: () => "7.0.0",
        cacheBaseDir: cacheDir,
      })
    ).rejects.toThrow(/timed out/);
  });

  it("skips disk cache when version is null but still returns content and caches in memory", async () => {
    const { calls, runCoaFn } = fakeRun({ stdout: "versionless" });
    const first = await fetchDescribeTopic("selectors", {
      runCoaFn,
      getVersion: () => null,
      cacheBaseDir: cacheDir,
    });
    expect(first.source).toBe("coa");
    expect(first.coaVersion).toBeNull();
    // Nothing should have been written under cacheDir since version is unknown.
    expect(existsSync(join(cacheDir, "unknown"))).toBe(false);

    const second = await fetchDescribeTopic("selectors", {
      runCoaFn,
      getVersion: () => null,
      cacheBaseDir: cacheDir,
    });
    expect(second.source).toBe("memory");
    expect(calls).toHaveLength(1);
  });
});

describe("applyCoalesceCorrections - sql-format dbt source() leak", () => {
  it("prepends the correction banner when sql-format content mentions source(", () => {
    const raw = [
      "# SQL format",
      "Use `source('raw', 'orders')` to reference external tables.",
    ].join("\n");
    const out = applyCoalesceCorrections("sql-format", raw);
    expect(out.startsWith(SQL_FORMAT_DBT_SOURCE_BANNER)).toBe(true);
    expect(out).toContain("Coalesce uses `ref()`, not dbt's `source()`");
    // Original content is preserved verbatim after the banner.
    expect(out).toContain(raw);
  });

  it("is a no-op when sql-format content does not mention source(", () => {
    const raw = "# SQL format\nUse `ref('SRC', 'ORDERS')`.";
    expect(applyCoalesceCorrections("sql-format", raw)).toBe(raw);
  });

  it("does not touch other topics even if they mention source(", () => {
    const raw = "# Concepts\nThe source() macro comes from dbt, not Coalesce.";
    expect(applyCoalesceCorrections("concepts", raw)).toBe(raw);
  });

  it("matches case-insensitively so `Source(` still triggers the banner", () => {
    const raw = "# SQL format\nUse `Source('raw', 'orders')` — CLI capitalisation variant.";
    const out = applyCoalesceCorrections("sql-format", raw);
    expect(out.startsWith(SQL_FORMAT_DBT_SOURCE_BANNER)).toBe(true);
  });

  it("is applied through fetchDescribeTopic on the coa path", async () => {
    const { runCoaFn } = fakeRun({
      stdout: "# sql-format\nCall `source('raw', 'orders')` to ...",
    });
    const result = await fetchDescribeTopic("sql-format", {
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });
    expect(result.content.startsWith(SQL_FORMAT_DBT_SOURCE_BANNER)).toBe(true);
  });

  it("disk cache stores the raw CLI content — the banner is applied at read time", async () => {
    const { runCoaFn } = fakeRun({
      stdout: "# sql-format\nCall `source('raw', 'orders')` ...",
    });
    await fetchDescribeTopic("sql-format", {
      runCoaFn,
      getVersion: () => "7.0.0",
      cacheBaseDir: cacheDir,
    });
    const diskPath = join(cacheDir, "7.0.0", "sql-format.md");
    const onDisk = readFileSync(diskPath, "utf8");
    expect(onDisk.startsWith(SQL_FORMAT_DBT_SOURCE_BANNER)).toBe(false);
    expect(onDisk).toContain("source('raw', 'orders')");
  });
});
