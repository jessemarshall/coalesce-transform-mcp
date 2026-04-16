import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from "vitest";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  coaDoctorHandler,
  coaValidateHandler,
  coaListProjectNodesHandler,
  coaDryRunCreateHandler,
  coaDryRunRunHandler,
  coaListEnvironmentsHandler,
  coaListEnvironmentNodesHandler,
  coaListRunsHandler,
  coaDescribeHandler,
  coaCreateHandler,
  coaRunHandler,
  coaPlanHandler,
  coaDeployHandler,
  coaRefreshHandler,
} from "../../src/mcp/coa.js";
import { CoaPreflightError } from "../../src/services/coa/preflight.js";
import { resetCoaDescribeMemoryCache } from "../../src/services/coa/describe.js";
import type { RunCoaResult } from "../../src/services/coa/runner.js";

type SpawnSpy = {
  calls: Array<{ args: string[]; cwd?: string; parseJson?: boolean }>;
};

/** Build a fake runCoa that records calls and returns a canned result. */
function fakeRunCoa(canned: Partial<RunCoaResult> = {}): {
  spy: SpawnSpy;
  run: (args: string[], options?: any) => Promise<RunCoaResult>;
} {
  const spy: SpawnSpy = { calls: [] };
  const run = async (args: string[], options: any = {}) => {
    spy.calls.push({ args, cwd: options.cwd, parseJson: options.parseJson });
    return {
      exitCode: 0,
      timedOut: false,
      stdout: "",
      stderr: "",
      ...canned,
    } satisfies RunCoaResult;
  };
  return { spy, run };
}

let tmpProject: string;

beforeAll(() => {
  tmpProject = mkdtempSync(join(tmpdir(), "coa-mcp-test-"));
  mkdirSync(tmpProject, { recursive: true });
  writeFileSync(join(tmpProject, "data.yml"), "fileVersion: 3\n");
  writeFileSync(join(tmpProject, "workspaces.yml"), "[default]\nx: y\n");
});

afterAll(() => {
  rmSync(tmpProject, { recursive: true, force: true });
});

describe("coa_doctor handler", () => {
  it("passes --json doctor --dir <project> with parseJson", async () => {
    const { spy, run } = fakeRunCoa({
      stdout: '{"ok":true}',
    });
    const result = await coaDoctorHandler({ projectPath: tmpProject }, run);
    expect(spy.calls).toHaveLength(1);
    expect(spy.calls[0].args).toEqual(["--json", "doctor", "--dir", tmpProject]);
    expect(spy.calls[0].cwd).toBe(tmpProject);
    expect(spy.calls[0].parseJson).toBe(true);
    expect(result.command).toContain("coa --json doctor --dir");
    expect(result.exitCode).toBe(0);
  });

  it("appends --workspace when provided", async () => {
    const { spy, run } = fakeRunCoa();
    await coaDoctorHandler(
      { projectPath: tmpProject, workspace: "prod" },
      run
    );
    expect(spy.calls[0].args).toContain("--workspace");
    expect(spy.calls[0].args).toContain("prod");
  });

  it("throws before spawning when projectPath is invalid", async () => {
    const { spy, run } = fakeRunCoa();
    await expect(
      coaDoctorHandler({ projectPath: "/does/not/exist-12345" }, run)
    ).rejects.toThrow(/does not exist/);
    expect(spy.calls).toHaveLength(0);
  });
});

describe("coa_validate handler", () => {
  it("passes --include and --exclude selectors through", async () => {
    const { spy, run } = fakeRunCoa();
    await coaValidateHandler(
      {
        projectPath: tmpProject,
        include: "{ STG_ORDERS }",
        exclude: "{ location: \"TEMP\" }",
      },
      run
    );
    const args = spy.calls[0].args;
    expect(args).toEqual(
      expect.arrayContaining([
        "--json",
        "validate",
        "--include",
        "{ STG_ORDERS }",
        "--exclude",
        '{ location: "TEMP" }',
      ])
    );
  });
});

describe("coa_list_project_nodes handler", () => {
  it("uses `create --list-nodes`", async () => {
    const { spy, run } = fakeRunCoa();
    await coaListProjectNodesHandler({ projectPath: tmpProject }, run);
    expect(spy.calls[0].args).toEqual([
      "--json",
      "create",
      "--dir",
      tmpProject,
      "--list-nodes",
    ]);
  });
});

describe("coa_dry_run_create handler", () => {
  it("forces --dry-run --verbose and does not use --json (coa verbose output is text-only)", async () => {
    const { spy, run } = fakeRunCoa();
    await coaDryRunCreateHandler(
      { projectPath: tmpProject, include: "{ STG }" },
      run
    );
    const args = spy.calls[0].args;
    expect(args).toContain("--dry-run");
    expect(args).toContain("--verbose");
    expect(args).not.toContain("--json");
    expect(args).toContain("--include");
    expect(args).toContain("{ STG }");
    // parseJson should not be set — text output expected.
    expect(spy.calls[0].parseJson).toBeFalsy();
  });
});

describe("coa_dry_run_run handler", () => {
  it("targets the `run` subcommand with --dry-run", async () => {
    const { spy, run } = fakeRunCoa();
    await coaDryRunRunHandler({ projectPath: tmpProject }, run);
    expect(spy.calls[0].args).toContain("run");
    expect(spy.calls[0].args).toContain("--dry-run");
  });
});

describe("coa_list_environments handler", () => {
  it("uses --format json --skipConfirm and does not require a project path", async () => {
    const { spy, run } = fakeRunCoa({ stdout: "[]" });
    const result = await coaListEnvironmentsHandler({}, run);
    expect(spy.calls[0].args).toEqual([
      "environments",
      "list",
      "--format",
      "json",
      "--skipConfirm",
    ]);
    expect(spy.calls[0].cwd).toBeUndefined();
    expect(spy.calls[0].parseJson).toBe(true);
    expect(result.exitCode).toBe(0);
  });

  it("passes optional flags", async () => {
    const { spy, run } = fakeRunCoa();
    await coaListEnvironmentsHandler(
      {
        detail: true,
        limit: 25,
        startingFrom: "cursor-x",
        orderBy: "name",
        profile: "staging",
        token: "tok",
      },
      run
    );
    const args = spy.calls[0].args;
    expect(args).toContain("--detail");
    expect(args).toEqual(
      expect.arrayContaining([
        "--limit",
        "25",
        "--startingFrom",
        "cursor-x",
        "--orderBy",
        "name",
        "--profile",
        "staging",
        "--token",
        "tok",
      ])
    );
  });

  it("falls back to COALESCE_PROFILE when no profile is passed", async () => {
    vi.stubEnv("COALESCE_PROFILE", "MEDBASE");
    const { spy, run } = fakeRunCoa();
    await coaListEnvironmentsHandler({}, run);
    expect(spy.calls[0].args).toEqual(
      expect.arrayContaining(["--profile", "MEDBASE"])
    );
    vi.unstubAllEnvs();
  });

  it("prefers tool input profile over COALESCE_PROFILE", async () => {
    vi.stubEnv("COALESCE_PROFILE", "MEDBASE");
    const { spy, run } = fakeRunCoa();
    await coaListEnvironmentsHandler({ profile: "staging" }, run);
    const args = spy.calls[0].args;
    expect(args).toEqual(expect.arrayContaining(["--profile", "staging"]));
    expect(args).not.toContain("MEDBASE");
    vi.unstubAllEnvs();
  });
});

describe("coa_list_environment_nodes handler", () => {
  it("requires environmentID and passes it through", async () => {
    const { spy, run } = fakeRunCoa({ stdout: "[]" });
    await coaListEnvironmentNodesHandler({ environmentID: "env-42" }, run);
    expect(spy.calls[0].args).toEqual(
      expect.arrayContaining([
        "nodes",
        "list",
        "--format",
        "json",
        "--skipConfirm",
        "--environmentID",
        "env-42",
      ])
    );
  });

  it("passes --skipParsing when requested", async () => {
    const { spy, run } = fakeRunCoa();
    await coaListEnvironmentNodesHandler(
      { environmentID: "env-1", skipParsing: true },
      run
    );
    expect(spy.calls[0].args).toContain("--skipParsing");
  });
});

describe("coa_list_runs handler", () => {
  it("rejects when neither environmentID nor allEnvironments is set", async () => {
    const { spy, run } = fakeRunCoa();
    await expect(coaListRunsHandler({}, run)).rejects.toThrow(
      /environmentID or allEnvironments/
    );
    expect(spy.calls).toHaveLength(0);
  });

  it("passes allEnvironments without environmentID", async () => {
    const { spy, run } = fakeRunCoa({ stdout: "[]" });
    await coaListRunsHandler({ allEnvironments: true }, run);
    const args = spy.calls[0].args;
    expect(args).toContain("--allEnvironments");
    expect(args).not.toContain("--environmentID");
  });

  it("passes array filters as repeated flags", async () => {
    const { spy, run } = fakeRunCoa();
    await coaListRunsHandler(
      {
        environmentID: "env-1",
        projectID: ["p1", "p2"],
        runType: ["scheduled"],
        runStatus: ["failed", "running"],
      },
      run
    );
    const args = spy.calls[0].args;
    // Count occurrences of each flag.
    const countOf = (flag: string) => args.filter((a) => a === flag).length;
    expect(countOf("--projectID")).toBe(2);
    expect(countOf("--runType")).toBe(1);
    expect(countOf("--runStatus")).toBe(2);
    // And the values follow.
    expect(args).toEqual(
      expect.arrayContaining([
        "--projectID",
        "p1",
        "--projectID",
        "p2",
        "--runType",
        "scheduled",
        "--runStatus",
        "failed",
        "--runStatus",
        "running",
      ])
    );
  });
});

describe("coa_describe handler", () => {
  // Redirect the describe-service disk cache to a temp dir so our fake-COA
  // stdout never leaks into the user's real ~/.cache.
  let describeCacheDir: string;
  const originalCacheEnv =
    process.env["COALESCE_MCP_COA_DESCRIBE_CACHE_DIR"];

  beforeAll(() => {
    describeCacheDir = mkdtempSync(join(tmpdir(), "coa-describe-handler-test-"));
    vi.stubEnv("COALESCE_MCP_COA_DESCRIBE_CACHE_DIR", describeCacheDir);
  });

  afterAll(() => {
    if (originalCacheEnv === undefined) {
      vi.unstubAllEnvs();
    }
    rmSync(describeCacheDir, { recursive: true, force: true });
  });

  afterEach(() => {
    resetCoaDescribeMemoryCache();
  });

  it("passes --no-color describe <topic> through runCoa", async () => {
    const { spy, run } = fakeRunCoa({ stdout: "# selectors topic" });
    const result = await coaDescribeHandler(
      { topic: "selectors", refresh: true },
      run as any
    );
    expect(spy.calls[0].args).toEqual(["--no-color", "describe", "selectors"]);
    expect(result.topic).toBe("selectors");
    expect(result.content).toBe("# selectors topic");
    expect(result.source).toBe("coa");
  });

  it("appends subtopic for topic='command' deep dives", async () => {
    const { spy, run } = fakeRunCoa({ stdout: "create deep-dive" });
    const result = await coaDescribeHandler(
      { topic: "command", subtopic: "create", refresh: true },
      run as any
    );
    expect(spy.calls[0].args).toEqual([
      "--no-color",
      "describe",
      "command",
      "create",
    ]);
    expect(result.subtopic).toBe("create");
  });

  it("calls `coa describe` with no topic arg for the overview topic", async () => {
    const { spy, run } = fakeRunCoa({ stdout: "overview text" });
    const result = await coaDescribeHandler(
      { topic: "overview", refresh: true },
      run as any
    );
    expect(spy.calls[0].args).toEqual(["--no-color", "describe"]);
    expect(result.content).toBe("overview text");
  });
});

describe("coa_create handler (destructive)", () => {
  it("composes `create --dir --workspace --include --exclude`", async () => {
    const { spy, run } = fakeRunCoa();
    await coaCreateHandler(
      {
        projectPath: tmpProject,
        workspace: "prod",
        include: "{ STG_CUSTOMER }",
        exclude: "{ location: \"TEMP\" }",
        confirmed: true,
      },
      run
    );
    const args = spy.calls[0].args;
    expect(args).toContain("create");
    expect(args).not.toContain("--dry-run");
    expect(args).toEqual(
      expect.arrayContaining([
        "--dir",
        tmpProject,
        "--workspace",
        "prod",
        "--include",
        "{ STG_CUSTOMER }",
        "--exclude",
        '{ location: "TEMP" }',
      ])
    );
  });

  it("blocks on preflight errors (double-quoted ref) before spawning", async () => {
    const badProject = mkdtempSync(join(tmpdir(), "coa-create-bad-"));
    mkdirSync(join(badProject, "nodes"));
    writeFileSync(join(badProject, "data.yml"), "fileVersion: 3\n");
    writeFileSync(join(badProject, "workspaces.yml"), "[default]\n");
    writeFileSync(
      join(badProject, "nodes", "STG.sql"),
      `SELECT * FROM {{ ref("A","B") }}`
    );
    const { spy, run } = fakeRunCoa();
    await expect(
      coaCreateHandler(
        { projectPath: badProject, confirmed: true },
        run
      )
    ).rejects.toBeInstanceOf(CoaPreflightError);
    expect(spy.calls).toHaveLength(0);
    rmSync(badProject, { recursive: true, force: true });
  });

  it("blocks on missing workspaces.yml before spawning", async () => {
    const projectNoWs = mkdtempSync(join(tmpdir(), "coa-create-no-ws-"));
    writeFileSync(join(projectNoWs, "data.yml"), "fileVersion: 3\n");
    const { spy, run } = fakeRunCoa();
    await expect(
      coaCreateHandler({ projectPath: projectNoWs, confirmed: true }, run)
    ).rejects.toThrow(/WORKSPACES_YML_MISSING/);
    expect(spy.calls).toHaveLength(0);
    rmSync(projectNoWs, { recursive: true, force: true });
  });

  it("surfaces non-blocking preflight warnings alongside the result", async () => {
    const projectWithWarn = mkdtempSync(join(tmpdir(), "coa-create-warn-"));
    mkdirSync(join(projectWithWarn, "nodes"));
    writeFileSync(join(projectWithWarn, "data.yml"), "fileVersion: 3\n");
    writeFileSync(join(projectWithWarn, "workspaces.yml"), "[default]\n");
    writeFileSync(
      join(projectWithWarn, "nodes", "U.sql"),
      `SELECT 1\nUNION ALL\nSELECT 2`
    );
    const { run } = fakeRunCoa();
    const result = await coaCreateHandler(
      { projectPath: projectWithWarn, confirmed: true },
      run
    );
    expect(result.preflightWarnings?.map((w) => w.code)).toContain(
      "SQL_LITERAL_UNION_ALL"
    );
    rmSync(projectWithWarn, { recursive: true, force: true });
  });
});

describe("coa_run handler (destructive)", () => {
  it("composes `run --dir --include`", async () => {
    const { spy, run } = fakeRunCoa();
    await coaRunHandler(
      { projectPath: tmpProject, include: "{ STG }", confirmed: true },
      run
    );
    const args = spy.calls[0].args;
    expect(args[0]).toBe("run");
    expect(args).toContain("--include");
  });

  it("blocks on the `{ A || B }` selector footgun", async () => {
    const { spy, run } = fakeRunCoa();
    await expect(
      coaRunHandler(
        { projectPath: tmpProject, include: "{ A || B }", confirmed: true },
        run
      )
    ).rejects.toThrow(/SELECTOR_COMBINED_OR/);
    expect(spy.calls).toHaveLength(0);
  });
});

describe("coa_plan handler", () => {
  it("composes `plan --dir --environmentID` plus optional flags", async () => {
    const { spy, run } = fakeRunCoa();
    await coaPlanHandler(
      {
        projectPath: tmpProject,
        environmentID: "env-9",
        out: "/tmp/my-plan.json",
        gitsha: "abc123",
        enableCache: true,
        profile: "staging",
      },
      run
    );
    const args = spy.calls[0].args;
    expect(args[0]).toBe("plan");
    expect(args).toEqual(
      expect.arrayContaining([
        "--environmentID",
        "env-9",
        "--out",
        "/tmp/my-plan.json",
        "--gitsha",
        "abc123",
        "--enableCache",
        "--profile",
        "staging",
      ])
    );
  });

  it("does not set --enableCache when false/unset", async () => {
    const { spy, run } = fakeRunCoa();
    await coaPlanHandler(
      { projectPath: tmpProject, environmentID: "env-1" },
      run
    );
    expect(spy.calls[0].args).not.toContain("--enableCache");
  });
});

describe("coa_deploy handler (destructive)", () => {
  let planPath: string;
  beforeAll(() => {
    planPath = join(tmpProject, "coa-plan.json");
    writeFileSync(planPath, "{}");
  });

  it("passes --environmentID and --plan", async () => {
    const { spy, run } = fakeRunCoa();
    await coaDeployHandler(
      { environmentID: "env-7", plan: planPath, confirmed: true },
      run
    );
    expect(spy.calls[0].args).toEqual(
      expect.arrayContaining([
        "deploy",
        "--environmentID",
        "env-7",
        "--plan",
        planPath,
      ])
    );
  });

  it("refuses to run if the plan file does not exist", async () => {
    const { spy, run } = fakeRunCoa();
    await expect(
      coaDeployHandler(
        {
          environmentID: "env-1",
          plan: "/tmp/does-not-exist-coa-plan.json",
          confirmed: true,
        },
        run
      )
    ).rejects.toThrow(/plan file not found/);
    expect(spy.calls).toHaveLength(0);
  });
});

describe("coa_refresh handler (destructive)", () => {
  it("composes `refresh --environmentID` plus optional filters", async () => {
    const { spy, run } = fakeRunCoa();
    await coaRefreshHandler(
      {
        environmentID: "env-42",
        include: "{ DIM_CUSTOMER }",
        parallelism: 4,
        forceIgnoreEnvironmentStatus: true,
        confirmed: true,
      },
      run
    );
    const args = spy.calls[0].args;
    expect(args).toEqual(
      expect.arrayContaining([
        "refresh",
        "--environmentID",
        "env-42",
        "--include",
        "{ DIM_CUSTOMER }",
        "--parallelism",
        "4",
        "--forceIgnoreEnvironmentStatus",
      ])
    );
  });

  it("does not require a project path", async () => {
    const { run } = fakeRunCoa();
    const result = await coaRefreshHandler(
      { environmentID: "env-1", confirmed: true },
      run
    );
    expect(result.exitCode).toBe(0);
  });
});

describe("token redaction", () => {
  it("does not echo --token value back in result.command", async () => {
    const { run } = fakeRunCoa({ stdout: "[]" });
    const result = await coaListEnvironmentsHandler(
      { token: "SUPER-SECRET-TOKEN-12345" },
      run
    );
    expect(result.command).not.toContain("SUPER-SECRET-TOKEN-12345");
    expect(result.command).toContain("--token <redacted>");
  });

  it("redacts coa_doctor JSON output (cloud.checks[].detail for name='token') and clears stdout", async () => {
    const doctorJson = {
      data: {
        cloud: {
          checks: [
            { name: "domain", status: "pass", detail: "https://x.coalescesoftware.io" },
            { name: "token", status: "pass", detail: "LEAKED-PREFIX" },
          ],
        },
      },
    };
    const run = async () => ({
      exitCode: 0,
      timedOut: false,
      stdout: JSON.stringify(doctorJson),
      stderr: "",
      json: doctorJson,
    });
    const result = await coaDoctorHandler({ projectPath: tmpProject }, run as any);
    const redactedChecks = (result.json as any).data.cloud.checks;
    expect(redactedChecks[0].detail).toBe("https://x.coalescesoftware.io");
    expect(redactedChecks[1].detail).toBe("<redacted>");
    // When redaction happens AND JSON parses cleanly, stdout is cleared so the
    // un-redacted source can't reach the agent via a different path.
    expect(result.stdout).toBe("");
    expect(result.stdout).not.toContain("LEAKED-PREFIX");
  });

  it("preserves stdout when doctor output has nothing sensitive to redact", async () => {
    const clean = { data: { workspace: "dev", platform: null } };
    const run = async () => ({
      exitCode: 0,
      timedOut: false,
      stdout: JSON.stringify(clean),
      stderr: "",
      json: clean,
    });
    const result = await coaDoctorHandler({ projectPath: tmpProject }, run as any);
    expect(result.stdout).toBe(JSON.stringify(clean));
  });
});

describe("result shape", () => {
  it("surfaces stdout, stderr, exitCode, timedOut and optional json/jsonParseError", async () => {
    const { run } = fakeRunCoa({
      stdout: '{"hello":"world"}',
      stderr: "warn-x",
      exitCode: 0,
    });
    const result = await coaListEnvironmentsHandler({}, async (args, opts) => {
      const r = await run(args, opts);
      return { ...r, json: { hello: "world" } } as RunCoaResult;
    });
    expect(result).toMatchObject({
      exitCode: 0,
      stdout: '{"hello":"world"}',
      stderr: "warn-x",
      timedOut: false,
      json: { hello: "world" },
    });
    expect(result.command).toContain("coa environments list");
  });

  it("passes jsonParseError through when COA stdout is not JSON", async () => {
    const run = async () => ({
      exitCode: 0,
      timedOut: false,
      stdout: "not json",
      stderr: "",
      jsonParseError: "Unexpected token",
    }) satisfies RunCoaResult;
    const result = await coaListEnvironmentsHandler({}, run);
    expect(result.jsonParseError).toBe("Unexpected token");
  });
});

/**
 * COALESCE_PROFILE fallback is shared across every cloud-scoped COA handler
 * via `resolveProfile()`. Rather than write a near-identical test per handler,
 * parameterize over all 6 and assert the `--profile` flag shows up when the
 * env var is set and tool input omits it, and (conversely) tool input wins
 * over env when both are present.
 */
describe("COALESCE_PROFILE fallback — shared across all cloud handlers", () => {
  let planPath: string;
  beforeAll(() => {
    planPath = join(tmpProject, "coa-plan-profile-test.json");
    writeFileSync(planPath, "{}");
  });

  // Each case supplies the minimum input the handler needs to pass its
  // non-profile validation (environmentID, plan path, etc.) so the assertion
  // isolates profile behavior.
  const cases: Array<{
    name: string;
    handler: (params: any, runCoaFn: any) => Promise<unknown>;
    minimalInput: () => Record<string, unknown>;
  }> = [
    {
      name: "coa_list_environments",
      handler: coaListEnvironmentsHandler,
      minimalInput: () => ({}),
    },
    {
      name: "coa_list_environment_nodes",
      handler: coaListEnvironmentNodesHandler,
      minimalInput: () => ({ environmentID: "env-1" }),
    },
    {
      name: "coa_list_runs",
      handler: coaListRunsHandler,
      minimalInput: () => ({ environmentID: "env-1" }),
    },
    {
      name: "coa_plan",
      handler: coaPlanHandler,
      minimalInput: () => ({ projectPath: tmpProject, environmentID: "env-1" }),
    },
    {
      name: "coa_deploy",
      handler: coaDeployHandler,
      minimalInput: () => ({ environmentID: "env-1", plan: planPath, confirmed: true }),
    },
    {
      name: "coa_refresh",
      handler: coaRefreshHandler,
      minimalInput: () => ({ environmentID: "env-1", confirmed: true }),
    },
  ];

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it.each(cases)(
    "$name falls back to COALESCE_PROFILE when no profile is passed",
    async ({ handler, minimalInput }) => {
      vi.stubEnv("COALESCE_PROFILE", "MEDBASE");
      const { spy, run } = fakeRunCoa();
      await handler(minimalInput(), run);
      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0].args).toEqual(
        expect.arrayContaining(["--profile", "MEDBASE"])
      );
    }
  );

  it.each(cases)(
    "$name prefers tool input profile over COALESCE_PROFILE",
    async ({ handler, minimalInput }) => {
      vi.stubEnv("COALESCE_PROFILE", "MEDBASE");
      const { spy, run } = fakeRunCoa();
      await handler({ ...minimalInput(), profile: "staging" }, run);
      const args = spy.calls[0].args;
      expect(args).toEqual(expect.arrayContaining(["--profile", "staging"]));
      expect(args).not.toContain("MEDBASE");
    }
  );

  it.each(cases)(
    "$name omits --profile when neither input nor env sets it",
    async ({ handler, minimalInput }) => {
      vi.stubEnv("COALESCE_PROFILE", "");
      const { spy, run } = fakeRunCoa();
      await handler(minimalInput(), run);
      expect(spy.calls[0].args).not.toContain("--profile");
    }
  );
});
