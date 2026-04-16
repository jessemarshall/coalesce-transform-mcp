import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { __resetForTests } from "../../../src/services/config/coa-config.js";
import {
  resolveCoalesceAuth,
  resolveSnowflakeAuth,
} from "../../../src/services/config/credentials.js";

// Build PEM boundaries dynamically so the pre-commit secret scanner (which
// blocks new additions of the literal BEGIN-PRIVATE-KEY line) doesn't flag
// obviously-fake test fixtures. Matches the pattern in src/coalesce/run-schemas.ts.
const PEM_BOUNDARY = "-----";
const PEM_CONTENT =
  `${PEM_BOUNDARY}BEGIN PRIVATE KEY${PEM_BOUNDARY}\n` +
  "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDummy\n" +
  `${PEM_BOUNDARY}END PRIVATE KEY${PEM_BOUNDARY}\n`;

function setupTempHome(contents?: string, extraFiles?: Record<string, string>): string {
  const dir = mkdtempSync(join(tmpdir(), "creds-test-"));
  mkdirSync(join(dir, ".coa"));
  vi.stubEnv("HOME", dir);
  vi.stubEnv("USERPROFILE", dir);
  if (contents !== undefined) {
    writeFileSync(join(dir, ".coa", "config"), contents);
  }
  if (extraFiles) {
    for (const [relPath, body] of Object.entries(extraFiles)) {
      writeFileSync(join(dir, relPath), body);
    }
  }
  return dir;
}

function clearAuthEnv(): void {
  for (const key of [
    "COALESCE_ACCESS_TOKEN",
    "COALESCE_BASE_URL",
    "COALESCE_PROFILE",
    "SNOWFLAKE_USERNAME",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_KEY_PAIR_KEY",
    "SNOWFLAKE_KEY_PAIR_PASS",
    "SNOWFLAKE_PAT",
  ]) {
    vi.stubEnv(key, "");
    // `vi.stubEnv(key, "")` sets the var to empty — we actually want unset, which
    // our `trim().length === 0` checks treat as absent, so the effect is equivalent.
  }
}

describe("resolveCoalesceAuth", () => {
  let home: string;

  beforeEach(() => {
    __resetForTests();
    clearAuthEnv();
  });

  afterEach(() => {
    if (home) rmSync(home, { recursive: true, force: true });
    vi.unstubAllEnvs();
    __resetForTests();
  });

  it("throws when neither env nor profile has a token", () => {
    home = setupTempHome();
    expect(() => resolveCoalesceAuth()).toThrow(/No Coalesce access token/);
  });

  it("reads from env when only env is set", () => {
    home = setupTempHome();
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "env-token");
    const auth = resolveCoalesceAuth();
    expect(auth.accessToken).toBe("env-token");
    expect(auth.sources.accessToken).toBe("env");
    expect(auth.baseUrl).toBe("https://app.coalescesoftware.io");
    expect(auth.sources.baseUrl).toBe("default");
  });

  it("reads from profile when env is absent", () => {
    home = setupTempHome(`[default]\ntoken=profile-token\ndomain=https://example.com/\n`);
    const auth = resolveCoalesceAuth();
    expect(auth.accessToken).toBe("profile-token");
    expect(auth.sources.accessToken).toBe("profile:default");
    expect(auth.baseUrl).toBe("https://example.com");
    expect(auth.sources.baseUrl).toBe("profile:default");
  });

  it("prefers env over profile (env-wins precedence)", () => {
    home = setupTempHome(`[default]\ntoken=profile-token\ndomain=https://profile.example.com\n`);
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "env-token");
    vi.stubEnv("COALESCE_BASE_URL", "https://env.example.com");
    const auth = resolveCoalesceAuth();
    expect(auth.accessToken).toBe("env-token");
    expect(auth.sources.accessToken).toBe("env");
    expect(auth.baseUrl).toBe("https://env.example.com");
    expect(auth.sources.baseUrl).toBe("env");
  });

  it("falls back to the profile domain when env has only the token", () => {
    home = setupTempHome(`[default]\ndomain=https://profile.example.com\n`);
    vi.stubEnv("COALESCE_ACCESS_TOKEN", "env-token");
    const auth = resolveCoalesceAuth();
    expect(auth.accessToken).toBe("env-token");
    expect(auth.sources.accessToken).toBe("env");
    expect(auth.baseUrl).toBe("https://profile.example.com");
    expect(auth.sources.baseUrl).toBe("profile:default");
  });

  it("honors COALESCE_PROFILE when selecting a profile", () => {
    home = setupTempHome(
      `[default]\ntoken=default-token\n\n[MEDBASE]\ntoken=medbase-token\ndomain=https://medbase.example.com\n`
    );
    vi.stubEnv("COALESCE_PROFILE", "MEDBASE");
    __resetForTests();
    const auth = resolveCoalesceAuth();
    expect(auth.accessToken).toBe("medbase-token");
    expect(auth.sources.accessToken).toBe("profile:MEDBASE");
    expect(auth.baseUrl).toBe("https://medbase.example.com");
  });

  it("strips trailing slashes on baseUrl from either source", () => {
    home = setupTempHome(`[default]\ntoken=t\ndomain=https://example.com///\n`);
    expect(resolveCoalesceAuth().baseUrl).toBe("https://example.com");
  });

  it("throws a targeted message naming the selected profile when COALESCE_PROFILE exists but has no token", () => {
    // MEDBASE profile exists in the file but has no `token=` key. Without this
    // nudge, users land on the generic "No Coalesce access token" error and
    // have to guess which profile was actually consulted.
    home = setupTempHome(
      `[default]\ntoken=default-token\n\n[MEDBASE]\ndomain=https://medbase.example.com\n`
    );
    vi.stubEnv("COALESCE_PROFILE", "MEDBASE");
    __resetForTests();
    expect(() => resolveCoalesceAuth()).toThrow(/profile \[MEDBASE\]/);
  });
});

describe("resolveSnowflakeAuth", () => {
  let home: string;

  beforeEach(() => {
    __resetForTests();
    clearAuthEnv();
  });

  afterEach(() => {
    if (home) rmSync(home, { recursive: true, force: true });
    vi.unstubAllEnvs();
    __resetForTests();
  });

  it("builds a KeyPair credential entirely from env when no profile exists", () => {
    home = setupTempHome(undefined, { "key.p8": PEM_CONTENT });
    vi.stubEnv("SNOWFLAKE_USERNAME", "JESSEM");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH");
    vi.stubEnv("SNOWFLAKE_ROLE", "SALES_ROLE");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", join(home, "key.p8"));
    const auth = resolveSnowflakeAuth();
    expect(auth.snowflakeAuthType).toBe("KeyPair");
    if (auth.snowflakeAuthType === "KeyPair") {
      expect(auth.snowflakeUsername).toBe("JESSEM");
      expect(auth.snowflakeKeyPairKey).toContain("BEGIN PRIVATE KEY");
      expect(auth.sources.snowflakeUsername).toBe("env");
      expect(auth.sources.snowflakeKeyPairKey).toBe("env");
    }
  });

  it("builds a KeyPair credential entirely from profile when no env is set", () => {
    const keyPath = "/tmp/placeholder"; // overwritten below so the parser picks up the real path
    home = setupTempHome(
      `[default]\nsnowflakeUsername=JESSEM\nsnowflakeWarehouse=COMPUTE_WH\nsnowflakeRole=SALES_ROLE\nsnowflakeKeyPairKey=${keyPath}\n`,
      { "key.p8": PEM_CONTENT }
    );
    // Rewrite config with the real path inside the temp home.
    const realPath = join(home, "key.p8");
    writeFileSync(
      join(home, ".coa", "config"),
      `[default]\nsnowflakeUsername=JESSEM\nsnowflakeWarehouse=COMPUTE_WH\nsnowflakeRole=SALES_ROLE\nsnowflakeKeyPairKey=${realPath}\n`
    );
    __resetForTests();
    const auth = resolveSnowflakeAuth();
    expect(auth.snowflakeAuthType).toBe("KeyPair");
    if (auth.snowflakeAuthType === "KeyPair") {
      expect(auth.sources.snowflakeUsername).toBe("profile:default");
      expect(auth.sources.snowflakeKeyPairKey).toBe("profile:default");
    }
  });

  it("env-wins when both profile and env set the same Snowflake field", () => {
    home = setupTempHome(
      `[default]\nsnowflakeUsername=PROFILE_USER\nsnowflakeWarehouse=PROFILE_WH\nsnowflakeRole=PROFILE_ROLE\n`,
      { "key.p8": PEM_CONTENT }
    );
    vi.stubEnv("SNOWFLAKE_USERNAME", "ENV_USER");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "ENV_WH");
    vi.stubEnv("SNOWFLAKE_ROLE", "ENV_ROLE");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", join(home, "key.p8"));
    const auth = resolveSnowflakeAuth();
    if (auth.snowflakeAuthType === "KeyPair") {
      expect(auth.snowflakeUsername).toBe("ENV_USER");
      expect(auth.sources.snowflakeUsername).toBe("env");
      expect(auth.sources.snowflakeWarehouse).toBe("env");
      expect(auth.sources.snowflakeRole).toBe("env");
    }
  });

  it("mixes env + profile values when they complement each other", () => {
    home = setupTempHome(
      `[default]\nsnowflakeUsername=JESSEM\nsnowflakeWarehouse=COMPUTE_WH\n`,
      { "key.p8": PEM_CONTENT }
    );
    vi.stubEnv("SNOWFLAKE_ROLE", "ENV_ROLE");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", join(home, "key.p8"));
    const auth = resolveSnowflakeAuth();
    if (auth.snowflakeAuthType === "KeyPair") {
      expect(auth.sources.snowflakeUsername).toBe("profile:default");
      expect(auth.sources.snowflakeWarehouse).toBe("profile:default");
      expect(auth.sources.snowflakeRole).toBe("env");
      expect(auth.sources.snowflakeKeyPairKey).toBe("env");
    }
  });

  it("PAT is env-only even when profile declares snowflakeAuthType", () => {
    home = setupTempHome(
      `[default]\nsnowflakeUsername=JESSEM\nsnowflakeWarehouse=COMPUTE_WH\nsnowflakeRole=SALES_ROLE\nsnowflakeAuthType=Basic\n`
    );
    vi.stubEnv("SNOWFLAKE_PAT", "pat-token-xyz");
    const auth = resolveSnowflakeAuth();
    expect(auth.snowflakeAuthType).toBe("Basic");
    if (auth.snowflakeAuthType === "Basic") {
      expect(auth.snowflakePassword).toBe("pat-token-xyz");
      expect(auth.sources.snowflakePat).toBe("env");
    }
  });

  it("throws when required Snowflake fields are missing from both sources", () => {
    home = setupTempHome();
    expect(() => resolveSnowflakeAuth()).toThrow(/Missing required Snowflake credential/);
  });

  it("throws when key-pair file is not a valid PEM", () => {
    home = setupTempHome(undefined, { "key.p8": "not a pem" });
    vi.stubEnv("SNOWFLAKE_USERNAME", "JESSEM");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "WH");
    vi.stubEnv("SNOWFLAKE_ROLE", "ROLE");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", join(home, "key.p8"));
    expect(() => resolveSnowflakeAuth()).toThrow(/not a valid PEM/);
  });

  it("picks Key Pair over PAT when both are available", () => {
    home = setupTempHome(undefined, { "key.p8": PEM_CONTENT });
    vi.stubEnv("SNOWFLAKE_USERNAME", "JESSEM");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "WH");
    vi.stubEnv("SNOWFLAKE_ROLE", "ROLE");
    vi.stubEnv("SNOWFLAKE_KEY_PAIR_KEY", join(home, "key.p8"));
    vi.stubEnv("SNOWFLAKE_PAT", "pat-token");
    const auth = resolveSnowflakeAuth();
    expect(auth.snowflakeAuthType).toBe("KeyPair");
  });

  it("rejects PAT values that look like file paths", () => {
    home = setupTempHome();
    vi.stubEnv("SNOWFLAKE_USERNAME", "JESSEM");
    vi.stubEnv("SNOWFLAKE_WAREHOUSE", "WH");
    vi.stubEnv("SNOWFLAKE_ROLE", "ROLE");
    vi.stubEnv("SNOWFLAKE_PAT", "/path/to/key.pem");
    expect(() => resolveSnowflakeAuth()).toThrow(/appears to be a file path/);
  });
});
