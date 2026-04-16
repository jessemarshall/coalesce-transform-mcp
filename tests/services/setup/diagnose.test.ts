import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  diagnoseAccessToken,
  diagnoseCoaConfig,
  diagnoseSnowflakeCreds,
  diagnoseRepoPath,
  diagnoseSetup,
} from "../../../src/services/setup/diagnose.js";
import { CoalesceApiError } from "../../../src/client.js";
import { setupTempHome, type TempHomeHandle } from "../../helpers/coa-config-fixture.js";

// Minimal CoalesceClient shim — only listWorkspaces (which calls listProjects
// via client.get) is exercised. We mock the api module directly instead of the
// low-level http methods to keep tests focused on diagnostic logic.
vi.mock("../../../src/coalesce/api/workspaces.js", () => ({
  listWorkspaces: vi.fn(),
}));

import { listWorkspaces } from "../../../src/coalesce/api/workspaces.js";

const mockListWorkspaces = vi.mocked(listWorkspaces);

// PEM header built dynamically so the pre-commit secret scanner doesn't flag
// these test fixtures as potential private keys. See src/coalesce/run-schemas.ts
// for the same pattern applied to production code.
const PEM_BOUNDARY = "-----";
const FAKE_PEM_CONTENT =
  `${PEM_BOUNDARY}BEGIN PRIVATE KEY${PEM_BOUNDARY}\nfake\n${PEM_BOUNDARY}END PRIVATE KEY${PEM_BOUNDARY}\n`;

let tempHome: TempHomeHandle;

beforeEach(() => {
  tempHome = setupTempHome();
});

afterEach(() => {
  vi.unstubAllEnvs();
  mockListWorkspaces.mockReset();
  tempHome.cleanup();
});

describe("diagnoseAccessToken", () => {
  const fakeClient = {} as any;

  it("returns 'missing' when COALESCE_ACCESS_TOKEN is unset", async () => {
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "");
    const result = await diagnoseAccessToken(fakeClient);
    expect(result).toEqual({ status: "missing" });
  });

  it("returns 'ok' with projectCount when listWorkspaces succeeds", async () => {
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "tok");
    mockListWorkspaces.mockResolvedValueOnce({
      data: [{ id: "w1" }, { id: "w2" }],
    });
    const result = await diagnoseAccessToken(fakeClient);
    expect(result).toMatchObject({ status: "ok", projectCount: 2, source: "env" });
  });

  it("returns 'invalid' on HTTP 401", async () => {
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "tok");
    mockListWorkspaces.mockRejectedValueOnce(
      new CoalesceApiError("unauthorized", 401)
    );
    const result = await diagnoseAccessToken(fakeClient);
    expect(result).toMatchObject({ status: "invalid", httpStatus: 401 });
  });

  it("returns 'invalid' on HTTP 403", async () => {
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "tok");
    mockListWorkspaces.mockRejectedValueOnce(
      new CoalesceApiError("forbidden", 403)
    );
    const result = await diagnoseAccessToken(fakeClient);
    expect(result).toMatchObject({ status: "invalid", httpStatus: 403 });
  });

  it("returns 'error' on other API errors", async () => {
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "tok");
    mockListWorkspaces.mockRejectedValueOnce(
      new CoalesceApiError("server gone", 502)
    );
    const result = await diagnoseAccessToken(fakeClient);
    expect(result.status).toBe("error");
  });

  it("returns 'error' on network-level failures", async () => {
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "tok");
    mockListWorkspaces.mockRejectedValueOnce(new Error("ECONNREFUSED"));
    const result = await diagnoseAccessToken(fakeClient);
    expect(result).toMatchObject({ status: "error", message: expect.stringContaining("ECONN") });
  });
});

describe("diagnoseSnowflakeCreds", () => {
  beforeEach(() => {
    // Clear all SNOWFLAKE_* env vars — stubEnv with "" is treated as unset
    // by our trimming, so explicitly stubEnv each one to empty.
    vi.stubEnv("SNOWFLAKE_USERNAME", "");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", "");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_PASS", "");
    vi.stubEnv("SNOWFLAKE_PAT", "");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "");
    vi.stubEnv("SNOWFLAKE_ROLE", "");
  });

  it("reports all missing vars when nothing is set", () => {
    const result = diagnoseSnowflakeCreds();
    expect(result.status).toBe("missing");
    if (result.status === "missing") {
      expect(result.missing).toEqual(
        expect.arrayContaining([
          "SNOWFLAKE_USERNAME",
          "SNOWFLAKE_KEY_PAIR_KEY or SNOWFLAKE_PAT",
          "SNOWFLAKE_WAREHOUSE",
          "SNOWFLAKE_ROLE",
        ])
      );
    }
  });

  it("returns 'ok' with authType='PAT' when PAT + shared vars are set", () => {
    vi.stubEnv("SNOWFLAKE_USERNAME", "u");
    vi.stubEnv("SNOWFLAKE_PAT", "pat-x");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "wh");
    vi.stubEnv("SNOWFLAKE_ROLE", "r");
    const result = diagnoseSnowflakeCreds();
    expect(result).toMatchObject({
      status: "ok",
      authType: "PAT",
      username: "u",
      warehouse: "wh",
      role: "r",
    });
  });

  it("returns 'invalid' when SNOWFLAKE_KEY_PAIR_KEY points at a missing file", () => {
    vi.stubEnv("SNOWFLAKE_USERNAME", "u");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", "/tmp/does-not-exist-snowflake-key-xyz.pem");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "wh");
    vi.stubEnv("SNOWFLAKE_ROLE", "r");
    const result = diagnoseSnowflakeCreds();
    expect(result).toMatchObject({
      status: "invalid",
      variable: "SNOWFLAKE_KEY_PAIR_KEY",
    });
  });

  it("returns 'ok' with authType='KeyPair' when the PEM path exists", () => {
    const tmp = mkdtempSync(join(tmpdir(), "coa-setup-pem-"));
    const pemPath = join(tmp, "key.pem");
    writeFileSync(pemPath, FAKE_PEM_CONTENT);
    vi.stubEnv("SNOWFLAKE_USERNAME", "u");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", pemPath);
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_PASS", "secret");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "wh");
    vi.stubEnv("SNOWFLAKE_ROLE", "r");
    const result = diagnoseSnowflakeCreds();
    expect(result).toMatchObject({
      status: "ok",
      authType: "KeyPair",
      hasPassphrase: true,
    });
    rmSync(tmp, { recursive: true, force: true });
  });

  it("prefers Key Pair when both key + PAT are set", () => {
    const tmp = mkdtempSync(join(tmpdir(), "coa-setup-pem-"));
    const pemPath = join(tmp, "key.pem");
    writeFileSync(pemPath, FAKE_PEM_CONTENT);
    vi.stubEnv("SNOWFLAKE_USERNAME", "u");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", pemPath);
    vi.stubEnv("SNOWFLAKE_PAT", "pat-x");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "wh");
    vi.stubEnv("SNOWFLAKE_ROLE", "r");
    const result = diagnoseSnowflakeCreds();
    expect(result).toMatchObject({ status: "ok", authType: "KeyPair" });
    rmSync(tmp, { recursive: true, force: true });
  });
});

describe("diagnoseRepoPath", () => {
  beforeEach(() => {
    vi.stubEnv("COALESCE_REPO_PATH", "");
  });

  it("returns 'missing' when COALESCE_REPO_PATH is unset", () => {
    expect(diagnoseRepoPath()).toEqual({ status: "missing" });
  });

  it("returns 'invalid' when path does not exist", () => {
    vi.stubEnv("COALESCE_REPO_PATH", "/tmp/nonexistent-coalesce-repo-xyz");
    expect(diagnoseRepoPath()).toMatchObject({
      status: "invalid",
      reason: expect.stringContaining("does not exist"),
    });
  });

  it("returns 'invalid' when path is a file", () => {
    const tmp = mkdtempSync(join(tmpdir(), "coa-setup-repo-"));
    const filePath = join(tmp, "afile");
    writeFileSync(filePath, "x");
    vi.stubEnv("COALESCE_REPO_PATH", filePath);
    expect(diagnoseRepoPath()).toMatchObject({
      status: "invalid",
      reason: expect.stringContaining("not a directory"),
    });
    rmSync(tmp, { recursive: true, force: true });
  });

  it("returns 'ok' with isCoaProject=false for a plain repo dir", () => {
    const tmp = mkdtempSync(join(tmpdir(), "coa-setup-repo-"));
    mkdirSync(join(tmp, "nodeTypes"));
    vi.stubEnv("COALESCE_REPO_PATH", tmp);
    const result = diagnoseRepoPath();
    expect(result).toMatchObject({ status: "ok", isCoaProject: false });
    rmSync(tmp, { recursive: true, force: true });
  });

  it("returns 'ok' with isCoaProject=true when data.yml exists", () => {
    const tmp = mkdtempSync(join(tmpdir(), "coa-setup-repo-"));
    writeFileSync(join(tmp, "data.yml"), "fileVersion: 3\n");
    vi.stubEnv("COALESCE_REPO_PATH", tmp);
    const result = diagnoseRepoPath();
    expect(result).toMatchObject({ status: "ok", isCoaProject: true });
    rmSync(tmp, { recursive: true, force: true });
  });
});

describe("diagnoseSetup (aggregate)", () => {
  const fakeClient = {} as any;

  beforeEach(() => {
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "");
    vi.stubEnv("SNOWFLAKE_USERNAME", "");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", "");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_PASS", "");
    vi.stubEnv("SNOWFLAKE_PAT", "");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "");
    vi.stubEnv("SNOWFLAKE_ROLE", "");
    vi.stubEnv("COALESCE_REPO_PATH", "");
  });

  it("reports all three pieces missing on a clean env, with ready=false", async () => {
    const report = await diagnoseSetup(fakeClient);
    expect(report.ready).toBe(false);
    expect(report.accessToken.status).toBe("missing");
    expect(report.snowflakeCreds.status).toBe("missing");
    expect(report.repoPath.status).toBe("missing");
    expect(report.nextSteps.length).toBeGreaterThanOrEqual(2);
    expect(
      report.nextSteps.some((s) => s.includes("COALESCE_ACCESS_TOKEN"))
    ).toBe(true);
    expect(report.nextSteps.some((s) => s.includes("COALESCE_REPO_PATH"))).toBe(true);
  });

  it("returns ready=true when all three are ok and repo is not a COA project (coaDoctor skipped)", async () => {
    // Access token
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "tok");
    mockListWorkspaces.mockResolvedValue({ data: [{ id: "w1" }] });
    // PAT path
    vi.stubEnv("SNOWFLAKE_USERNAME", "u");
    vi.stubEnv("SNOWFLAKE_PAT", "pat-x");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "wh");
    vi.stubEnv("SNOWFLAKE_ROLE", "r");
    // Repo path — directory but NOT a COA project
    const tmp = mkdtempSync(join(tmpdir(), "coa-setup-diag-"));
    mkdirSync(join(tmp, "nodeTypes"));
    vi.stubEnv("COALESCE_REPO_PATH", tmp);

    const report = await diagnoseSetup(fakeClient);
    expect(report.accessToken.status).toBe("ok");
    expect(report.snowflakeCreds.status).toBe("ok");
    expect(report.repoPath.status).toBe("ok");
    expect(report.coaDoctor.status).toBe("skipped");
    expect(report.ready).toBe(true);
    // Note: when repo isn't a COA project there's still a next-step hint telling
    // the user coa_* tools won't work — that's an info line, not a readiness block.
    expect(report.nextSteps.some((s) => s.includes("data.yml"))).toBe(true);

    rmSync(tmp, { recursive: true, force: true });
  });

  it("surfaces a specific next step when the access token is invalid", async () => {
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "tok");
    mockListWorkspaces.mockRejectedValueOnce(
      new CoalesceApiError("unauthorized", 401)
    );
    const report = await diagnoseSetup(fakeClient);
    expect(report.ready).toBe(false);
    expect(report.nextSteps.some((s) => s.includes("rejected by the API"))).toBe(true);
  });

  it("offers both env-var and profile paths when ~/.coa/config is absent and no env creds are set", async () => {
    const report = await diagnoseSetup(fakeClient);
    expect(report.coaConfig.status).toBe("missing-file");
    const authStep = report.nextSteps.find((s) =>
      s.includes("Provide a Coalesce access token")
    );
    expect(authStep).toBeDefined();
    expect(authStep).toContain("COALESCE_ACCESS_TOKEN");
    expect(authStep).toContain("~/.coa/config");
  });

  it("warns when COALESCE_PROFILE names a non-existent profile", async () => {
    tempHome.writeConfig(`[default]\ntoken=t\ndomain=https://x.example.com\n`);
    vi.stubEnv("COALESCE_PROFILE", "DOES_NOT_EXIST");
    // Silence the module-level stderr warning from the loader
    const writeSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const report = await diagnoseSetup(fakeClient);
    expect(report.coaConfig.status).toBe("ok");
    if (report.coaConfig.status === "ok") {
      expect(report.coaConfig.profileExists).toBe(false);
      expect(report.coaConfig.availableProfiles).toEqual(["default"]);
    }
    expect(
      report.nextSteps.some((s) => s.includes('COALESCE_PROFILE="DOES_NOT_EXIST"'))
    ).toBe(true);
    writeSpy.mockRestore();
  });

  it("still warns about a missing profile even when env vars satisfy every credential", async () => {
    // Regression guard: the profile-not-found warning must be unconditional.
    // If it were gated on "missing creds" a user with stale COALESCE_PROFILE
    // plus a fully configured env block would see `ready: true` and no hint
    // that their profile name is wrong.
    tempHome.writeConfig(`[default]\ntoken=t\n`);
    vi.stubEnv("COALESCE_PROFILE", "NOPE");
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "env-tok");
    vi.stubEnv("SNOWFLAKE_USERNAME", "u");
    vi.stubEnv("SNOWFLAKE_PAT", "pat-x");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "wh");
    vi.stubEnv("SNOWFLAKE_ROLE", "r");
    mockListWorkspaces.mockResolvedValue({ data: [{ id: "w1" }] });
    const writeSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);

    const report = await diagnoseSetup(fakeClient);

    expect(report.accessToken.status).toBe("ok");
    expect(report.snowflakeCreds.status).toBe("ok");
    expect(
      report.nextSteps.some((s) => s.includes('COALESCE_PROFILE="NOPE"'))
    ).toBe(true);
    writeSpy.mockRestore();
  });
});

describe("diagnoseCoaConfig", () => {
  it("returns missing-file when ~/.coa/config is absent", () => {
    const status = diagnoseCoaConfig();
    expect(status.status).toBe("missing-file");
    expect(status.activeProfile).toBe("default");
  });

  it("lists available profiles and present keys when the file exists", () => {
    tempHome.writeConfig(
      `[default]\ntoken=t\ndomain=https://x.example.com\nsnowflakeUsername=JM\n\n[MEDBASE]\ntoken=t2\n`
    );
    const status = diagnoseCoaConfig();
    expect(status.status).toBe("ok");
    if (status.status === "ok") {
      expect(status.availableProfiles).toEqual(["default", "MEDBASE"]);
      expect(status.profileExists).toBe(true);
      expect(status.presentKeys).toEqual(
        expect.arrayContaining(["token", "domain", "snowflakeUsername"])
      );
    }
  });

  it("reports profileExists=false when COALESCE_PROFILE misses", () => {
    tempHome.writeConfig(`[default]\ntoken=t\n`);
    vi.stubEnv("COALESCE_PROFILE", "missing");
    const writeSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const status = diagnoseCoaConfig();
    expect(status.status).toBe("ok");
    if (status.status === "ok") {
      expect(status.activeProfile).toBe("missing");
      expect(status.profileExists).toBe(false);
    }
    writeSpy.mockRestore();
  });
});

describe("source reporting", () => {
  const fakeClient = {} as any;

  // Each "source" test has to neutralize ambient env from the developer's shell
  // (COALESCE_ACCESS_TOKEN, SNOWFLAKE_*) — otherwise "env" wins and the test
  // can never observe a "profile:default" source.
  beforeEach(() => {
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "");
    vi.stubEnv("SNOWFLAKE_USERNAME", "");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", "");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_PASS", "");
    vi.stubEnv("SNOWFLAKE_PAT", "");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "");
    vi.stubEnv("SNOWFLAKE_ROLE", "");
  });

  it("diagnoseAccessToken reports source=profile when only profile provides the token", async () => {
    tempHome.writeConfig(`[default]\ntoken=profile-token\n`);
    mockListWorkspaces.mockResolvedValueOnce({ data: [] });
    const result = await diagnoseAccessToken(fakeClient);
    expect(result).toMatchObject({ status: "ok", source: "profile:default" });
  });

  it("diagnoseAccessToken reports source=env when env shadows profile", async () => {
    tempHome.writeConfig(`[default]\ntoken=profile-token\n`);
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "env-token");
    mockListWorkspaces.mockResolvedValueOnce({ data: [] });
    const result = await diagnoseAccessToken(fakeClient);
    expect(result).toMatchObject({ status: "ok", source: "env" });
  });

  it("diagnoseSnowflakeCreds reports per-field sources with profile fallback", () => {
    tempHome.writeConfig(
      `[default]\nsnowflakeUsername=PROFILE_U\nsnowflakeWarehouse=PROFILE_WH\nsnowflakeRole=PROFILE_R\n`
    );
    vi.stubEnv("SNOWFLAKE_PAT", "pat-x");
    const result = diagnoseSnowflakeCreds();
    expect(result.status).toBe("ok");
    if (result.status === "ok") {
      expect(result.sources.snowflakeUsername).toBe("profile:default");
      expect(result.sources.snowflakeWarehouse).toBe("profile:default");
      expect(result.sources.snowflakeRole).toBe("profile:default");
      expect(result.sources.snowflakePat).toBe("env");
    }
  });
});
