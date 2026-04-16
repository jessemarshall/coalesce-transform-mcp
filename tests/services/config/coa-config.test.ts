import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  __resetForTests,
  getActiveProfileName,
  getCoaConfigStatus,
  loadCoaProfile,
} from "../../../src/services/config/coa-config.js";

const SAMPLE = `[default]
token="test-token-default"
domain=https://jesse-marshall.app.coalescesoftware.io
snowflakeKeyPairKey= /Users/jmarshall/.coa/rsa_key.p8
snowflakeAuthType='KeyPair'
snowflakeUsername=JESSEM
snowflakeRole=SALES_ROLE
snowflakeWarehouse=COMPUTE_WH
environmentID=9
#NODE_DEBUG=http

[MEDBASE]
token="test-token-medbase"
domain=https://jesse-marshall.app.coalescesoftware.io
snowflakeUsername=JESSEM
environmentID=27
`;

function withTempHome(): string {
  const dir = mkdtempSync(join(tmpdir(), "coa-config-test-"));
  mkdirSync(join(dir, ".coa"));
  vi.stubEnv("HOME", dir);
  // On some platforms Node's os.homedir() also honors USERPROFILE
  vi.stubEnv("USERPROFILE", dir);
  return dir;
}

function writeConfig(home: string, contents: string): void {
  writeFileSync(join(home, ".coa", "config"), contents);
}

describe("coa-config loader", () => {
  let home: string;

  beforeEach(() => {
    __resetForTests();
    home = withTempHome();
  });

  afterEach(() => {
    rmSync(home, { recursive: true, force: true });
    vi.unstubAllEnvs();
    __resetForTests();
  });

  it("reports missing-file when ~/.coa/config does not exist", () => {
    const status = getCoaConfigStatus();
    expect(status.kind).toBe("missing-file");
    expect(loadCoaProfile()).toBeNull();
  });

  it("parses the default profile from a real-world sample", () => {
    writeConfig(home, SAMPLE);
    const profile = loadCoaProfile();
    expect(profile).not.toBeNull();
    expect(profile?.profileName).toBe("default");
    expect(profile?.token).toBe("test-token-default");
    expect(profile?.domain).toBe("https://jesse-marshall.app.coalescesoftware.io");
    expect(profile?.snowflakeKeyPairKey).toBe("/Users/jmarshall/.coa/rsa_key.p8");
    expect(profile?.snowflakeAuthType).toBe("KeyPair");
    expect(profile?.snowflakeUsername).toBe("JESSEM");
    expect(profile?.environmentID).toBe("9");
  });

  it("strips both double and single quotes", () => {
    writeConfig(home, `[default]\ntoken="QUOTED"\nother='SINGLE'\n`);
    const profile = loadCoaProfile();
    expect(profile?.token).toBe("QUOTED");
    expect(profile?.extras.other).toBe("SINGLE");
  });

  it("trims leading whitespace after `=`", () => {
    writeConfig(home, `[default]\nsnowflakeKeyPairKey=   /path/to/key.p8   \n`);
    const profile = loadCoaProfile();
    expect(profile?.snowflakeKeyPairKey).toBe("/path/to/key.p8");
  });

  it("ignores comments and blank lines", () => {
    writeConfig(home, `# this is a comment\n\n[default]\n# inner comment\ntoken=T\n; semicolon comment\n`);
    expect(loadCoaProfile()?.token).toBe("T");
  });

  it("splits on the first `=` only so tokens with `=` survive", () => {
    writeConfig(home, `[default]\ntoken="abc=def=ghi"\n`);
    expect(loadCoaProfile()?.token).toBe("abc=def=ghi");
  });

  it("loads a named profile when COALESCE_PROFILE is set", () => {
    writeConfig(home, SAMPLE);
    vi.stubEnv("COALESCE_PROFILE", "MEDBASE");
    __resetForTests();
    const profile = loadCoaProfile();
    expect(profile?.profileName).toBe("MEDBASE");
    expect(profile?.token).toBe("test-token-medbase");
    expect(profile?.environmentID).toBe("27");
  });

  it("returns null and warns once when COALESCE_PROFILE names a missing section", () => {
    writeConfig(home, SAMPLE);
    vi.stubEnv("COALESCE_PROFILE", "NOPE");
    __resetForTests();
    const writeSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    expect(loadCoaProfile()).toBeNull();
    expect(loadCoaProfile()).toBeNull();
    const warnCalls = writeSpy.mock.calls.filter((call) =>
      String(call[0]).includes('profile "NOPE" not found')
    );
    expect(warnCalls).toHaveLength(1);
    writeSpy.mockRestore();
  });

  it("does not warn when the default profile is simply absent and COALESCE_PROFILE is unset", () => {
    writeConfig(home, `[OTHER]\ntoken=X\n`);
    const writeSpy = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    expect(loadCoaProfile()).toBeNull();
    const warnCalls = writeSpy.mock.calls.filter((call) =>
      String(call[0]).includes("falling back to env vars")
    );
    expect(warnCalls).toHaveLength(0);
    writeSpy.mockRestore();
  });

  it("captures unknown keys in extras", () => {
    writeConfig(home, `[default]\ntoken=T\ncustomField=hello\n`);
    expect(loadCoaProfile()?.extras).toEqual({ customField: "hello" });
  });

  it("lists available profiles in the status payload", () => {
    writeConfig(home, SAMPLE);
    const status = getCoaConfigStatus();
    expect(status.kind).toBe("ok");
    if (status.kind === "ok") {
      expect(status.profiles).toEqual(["default", "MEDBASE"]);
    }
  });

  it("strips a UTF-8 BOM so the first section still parses", () => {
    writeConfig(home, "\uFEFF" + SAMPLE);
    const profile = loadCoaProfile();
    expect(profile?.token).toBe("test-token-default");
  });

  it("degrades gracefully when the config path is a directory (EISDIR)", () => {
    // Overwrite the config file path with a directory to simulate `readFileSync` failing.
    const cfgPath = join(home, ".coa", "config");
    try { rmSync(cfgPath, { force: true }); } catch { /* ignore */ }
    mkdirSync(cfgPath, { recursive: true });
    __resetForTests();
    const status = getCoaConfigStatus();
    expect(status.kind).toBe("parse-error");
    expect(loadCoaProfile()).toBeNull();
  });

  it("returns null for structurally garbage input without throwing", () => {
    writeConfig(home, "\x00\x01garbage=without=section\n[unclosed");
    expect(() => loadCoaProfile()).not.toThrow();
  });

  it("getActiveProfileName defaults to 'default' when COALESCE_PROFILE is unset or blank", () => {
    expect(getActiveProfileName()).toBe("default");
    vi.stubEnv("COALESCE_PROFILE", "   ");
    expect(getActiveProfileName()).toBe("default");
    vi.stubEnv("COALESCE_PROFILE", "MEDBASE");
    expect(getActiveProfileName()).toBe("MEDBASE");
  });
});
