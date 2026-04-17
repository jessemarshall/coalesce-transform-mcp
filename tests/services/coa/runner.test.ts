import { describe, it, expect, vi, afterEach } from "vitest";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { runCoa } from "../../../src/services/coa/runner.js";
import type { ResolvedCoaBinary } from "../../../src/services/coa/resolver.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fakeCoaPath = join(__dirname, "fixtures", "fake-coa.js");

function fakeBundled(): ResolvedCoaBinary {
  return {
    binaryPath: fakeCoaPath,
    source: "bundled",
    version: "test-fake",
  };
}

describe("runCoa", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("passes args through to the child process", async () => {
    const result = await runCoa(["--echo-args-json", "extra", "--flag"], {
      resolve: fakeBundled,
    });
    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual([
      "--echo-args-json",
      "extra",
      "--flag",
    ]);
  });

  it("captures stdout and stderr separately", async () => {
    const result = await runCoa(
      ["--print=out-ok", "--stderr=err-warn", "--exit=0"],
      { resolve: fakeBundled }
    );
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe("out-ok");
    expect(result.stderr).toBe("err-warn");
  });

  it("returns non-zero exit codes without throwing", async () => {
    const result = await runCoa(["--stderr=boom", "--exit=2"], {
      resolve: fakeBundled,
    });
    expect(result.exitCode).toBe(2);
    expect(result.stderr).toBe("boom");
    expect(result.timedOut).toBe(false);
  });

  it("parses stdout JSON when parseJson: true", async () => {
    const result = await runCoa(["--print=" + JSON.stringify({ ok: 1 })], {
      resolve: fakeBundled,
      parseJson: true,
    });
    expect(result.json).toEqual({ ok: 1 });
    expect(result.jsonParseError).toBeUndefined();
  });

  it("reports parse errors when parseJson: true and stdout is not JSON", async () => {
    const result = await runCoa(["--print=not json"], {
      resolve: fakeBundled,
      parseJson: true,
    });
    expect(result.json).toBeUndefined();
    expect(result.jsonParseError).toBeTruthy();
  });

  it("reports an empty-stdout parse error when parseJson: true and nothing was printed", async () => {
    const result = await runCoa([], {
      resolve: fakeBundled,
      parseJson: true,
    });
    expect(result.json).toBeUndefined();
    expect(result.jsonParseError).toBe("stdout was empty");
  });

  it("strips ambient COALESCE_* env vars from the child process", async () => {
    vi.stubEnv("COALESCE_API_TOKEN", "should-be-stripped");
    vi.stubEnv("COALESCE_CACHE_DIR", "should-also-be-stripped");
    const result = await runCoa(["--echo-env-json=COALESCE_"], {
      resolve: fakeBundled,
    });
    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout)).toEqual({});
  });

  it("preserves explicit COALESCE_* overrides even when ambient vars are stripped", async () => {
    vi.stubEnv("COALESCE_API_TOKEN", "ambient-will-be-stripped");
    const result = await runCoa(["--echo-env-json=COALESCE_"], {
      resolve: fakeBundled,
      env: { COALESCE_API_TOKEN: "explicit-override-kept" },
    });
    expect(JSON.parse(result.stdout)).toEqual({
      COALESCE_API_TOKEN: "explicit-override-kept",
    });
  });

  it("applies user-provided env overrides", async () => {
    const result = await runCoa(["--echo-env-json=COA_TEST_"], {
      resolve: fakeBundled,
      env: { COA_TEST_VAR: "hello" },
    });
    expect(JSON.parse(result.stdout)).toEqual({ COA_TEST_VAR: "hello" });
  });

  it("times out long-running commands and marks timedOut", async () => {
    const result = await runCoa(["--sleep=5000", "--exit=0"], {
      resolve: fakeBundled,
      timeoutMs: 200,
    });
    expect(result.timedOut).toBe(true);
    // On SIGTERM the exit code is null (signal-terminated).
    expect(result.exitCode).not.toBe(0);
  });

  it("respects the cwd option", async () => {
    const result = await runCoa(
      ["--print=cwd-ok", "--exit=0"],
      { resolve: fakeBundled, cwd: __dirname }
    );
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe("cwd-ok");
  });
});
