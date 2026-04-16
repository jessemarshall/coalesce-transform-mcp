import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { writeFileSync, unlinkSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { buildRerunBody } from "../src/coalesce/types.js";
import { setupTempHome, type TempHomeHandle } from "./helpers/coa-config-fixture.js";

describe("buildRerunBody", () => {
  const originalEnv = process.env;
  const tempDir = join(tmpdir(), "coalesce-rerun-test-" + process.pid);
  const keyFilePath = join(tempDir, "test-key.pem");
  const pemContent = "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----";
  let tempHome: TempHomeHandle;

  beforeEach(() => {
    tempHome = setupTempHome();
    mkdirSync(tempDir, { recursive: true });
    writeFileSync(keyFilePath, pemContent);
    process.env = {
      ...originalEnv,
      HOME: tempHome.home,
      USERPROFILE: tempHome.home,
      SNOWFLAKE_USERNAME: "user",
      SNOWFLAKE_KEY_PAIR_KEY: keyFilePath,
      SNOWFLAKE_WAREHOUSE: "WH",
      SNOWFLAKE_ROLE: "ROLE",
    };
  });

  afterEach(() => {
    process.env = originalEnv;
    try { unlinkSync(keyFilePath); } catch { /* ignore */ }
    vi.unstubAllEnvs();
    tempHome.cleanup();
  });

  const validParams = {
    runDetails: { runID: "401" },
  };

  it("injects snowflakeAuthType as KeyPair", () => {
    const body = buildRerunBody(validParams);
    expect(body.userCredentials.snowflakeAuthType).toBe("KeyPair");
  });

  it("preserves runDetails as-is", () => {
    const body = buildRerunBody(validParams);
    expect(body.runDetails).toEqual({ runID: "401" });
  });

  it("reads PEM key from file path", () => {
    const body = buildRerunBody(validParams);
    expect(body.userCredentials.snowflakeKeyPairKey).toBe(pemContent);
  });

  it("reads other credentials from environment variables", () => {
    const body = buildRerunBody(validParams);
    expect(body.userCredentials.snowflakeUsername).toBe("user");
    expect(body.userCredentials.snowflakeWarehouse).toBe("WH");
    expect(body.userCredentials.snowflakeRole).toBe("ROLE");
  });

  it("includes snowflakeKeyPairPass when env var is set", () => {
    process.env.SNOWFLAKE_KEY_PAIR_PASS = "secret";
    const body = buildRerunBody(validParams);
    expect(body.userCredentials.snowflakeKeyPairPass).toBe("secret");
  });

  it("omits snowflakeKeyPairPass when env var is not set", () => {
    const body = buildRerunBody(validParams);
    expect(body.userCredentials).not.toHaveProperty("snowflakeKeyPairPass");
  });

  it("throws when SNOWFLAKE_USERNAME is missing", () => {
    delete process.env.SNOWFLAKE_USERNAME;
    expect(() => buildRerunBody(validParams)).toThrow("SNOWFLAKE_USERNAME");
  });

  it("throws when neither SNOWFLAKE_KEY_PAIR_KEY nor SNOWFLAKE_PAT is set", () => {
    delete process.env.SNOWFLAKE_KEY_PAIR_KEY;
    delete process.env.SNOWFLAKE_PAT;
    expect(() => buildRerunBody(validParams)).toThrow("SNOWFLAKE_KEY_PAIR_KEY or SNOWFLAKE_PAT");
  });

  it("throws when SNOWFLAKE_WAREHOUSE is missing", () => {
    delete process.env.SNOWFLAKE_WAREHOUSE;
    expect(() => buildRerunBody(validParams)).toThrow("SNOWFLAKE_WAREHOUSE");
  });

  it("throws when SNOWFLAKE_ROLE is missing", () => {
    delete process.env.SNOWFLAKE_ROLE;
    expect(() => buildRerunBody(validParams)).toThrow("SNOWFLAKE_ROLE");
  });

  it("throws when key file does not exist", () => {
    process.env.SNOWFLAKE_KEY_PAIR_KEY = "/nonexistent/path/key.pem";
    expect(() => buildRerunBody(validParams)).toThrow("file not found");
  });

  it("throws when key file does not contain a PEM key", () => {
    writeFileSync(keyFilePath, "not a pem key");
    expect(() => buildRerunBody(validParams)).toThrow("not a valid PEM private key");
  });

  it("passes through forceIgnoreWorkspaceStatus", () => {
    const body = buildRerunBody({
      runDetails: { runID: "401", forceIgnoreWorkspaceStatus: true },
    });
    expect(body.runDetails.forceIgnoreWorkspaceStatus).toBe(true);
  });

  it("includes parameters when provided", () => {
    const body = buildRerunBody({
      ...validParams,
      parameters: { foo: "bar" },
    });
    expect(body.parameters).toEqual({ foo: "bar" });
  });

  it("omits parameters when not provided", () => {
    const body = buildRerunBody(validParams);
    expect(body).not.toHaveProperty("parameters");
  });

  // --- PAT auth ---

  it("uses Basic auth when SNOWFLAKE_PAT is set and SNOWFLAKE_KEY_PAIR_KEY is not", () => {
    delete process.env.SNOWFLAKE_KEY_PAIR_KEY;
    process.env.SNOWFLAKE_PAT = "pat-token-123";
    const body = buildRerunBody(validParams);
    expect(body.userCredentials.snowflakeAuthType).toBe("Basic");
    expect(body.userCredentials).toHaveProperty("snowflakePassword", "pat-token-123");
    expect(body.userCredentials).not.toHaveProperty("snowflakeKeyPairKey");
  });

  it("prefers Key Pair when both SNOWFLAKE_KEY_PAIR_KEY and SNOWFLAKE_PAT are set", () => {
    process.env.SNOWFLAKE_PAT = "pat-token-123";
    const body = buildRerunBody(validParams);
    expect(body.userCredentials.snowflakeAuthType).toBe("KeyPair");
    expect(body.userCredentials).toHaveProperty("snowflakeKeyPairKey");
    expect(body.userCredentials).not.toHaveProperty("snowflakePassword");
  });

  it("falls back to PAT when SNOWFLAKE_KEY_PAIR_KEY is empty string", () => {
    process.env.SNOWFLAKE_KEY_PAIR_KEY = "  ";
    process.env.SNOWFLAKE_PAT = "pat-token-123";
    const body = buildRerunBody(validParams);
    expect(body.userCredentials.snowflakeAuthType).toBe("Basic");
  });
});
