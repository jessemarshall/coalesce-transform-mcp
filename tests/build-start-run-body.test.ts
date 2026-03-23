import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { writeFileSync, unlinkSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { buildStartRunBody } from "../src/coalesce/types.js";

describe("buildStartRunBody", () => {
  const originalEnv = process.env;
  const tempDir = join(tmpdir(), "coalesce-test-" + process.pid);
  const keyFilePath = join(tempDir, "test-key.pem");
  const pemContent = "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----";

  beforeEach(() => {
    mkdirSync(tempDir, { recursive: true });
    writeFileSync(keyFilePath, pemContent);
    process.env = {
      ...originalEnv,
      SNOWFLAKE_USERNAME: "user",
      SNOWFLAKE_KEY_PAIR_KEY: keyFilePath,
      SNOWFLAKE_WAREHOUSE: "WH",
      SNOWFLAKE_ROLE: "ROLE",
    };
  });

  afterEach(() => {
    process.env = originalEnv;
    try { unlinkSync(keyFilePath); } catch { /* ignore */ }
  });

  // Params with a jobID (scoped — no confirmation needed)
  const scopedParams = {
    runDetails: { environmentID: "env-123", jobID: "job-1" },
  };

  // Params with no scope (runs all nodes — needs confirmation)
  const unscopedParams = {
    runDetails: { environmentID: "env-123" },
  };

  it("injects snowflakeAuthType as KeyPair", () => {
    const body = buildStartRunBody(scopedParams);
    expect(body.userCredentials.snowflakeAuthType).toBe("KeyPair");
  });

  it("preserves runDetails as-is", () => {
    const body = buildStartRunBody(scopedParams);
    expect(body.runDetails).toEqual({ environmentID: "env-123", jobID: "job-1" });
  });

  it("reads PEM key from file path", () => {
    const body = buildStartRunBody(scopedParams);
    expect(body.userCredentials.snowflakeKeyPairKey).toBe(pemContent);
  });

  it("reads other credentials from environment variables", () => {
    const body = buildStartRunBody(scopedParams);
    expect(body.userCredentials.snowflakeUsername).toBe("user");
    expect(body.userCredentials.snowflakeWarehouse).toBe("WH");
    expect(body.userCredentials.snowflakeRole).toBe("ROLE");
  });

  it("includes snowflakeKeyPairPass when env var is set", () => {
    process.env.SNOWFLAKE_KEY_PAIR_PASS = "secret";
    const body = buildStartRunBody(scopedParams);
    expect(body.userCredentials.snowflakeKeyPairPass).toBe("secret");
  });

  it("omits snowflakeKeyPairPass when env var is not set", () => {
    const body = buildStartRunBody(scopedParams);
    expect(body.userCredentials).not.toHaveProperty("snowflakeKeyPairPass");
  });

  it("omits parameters when not provided", () => {
    const body = buildStartRunBody(scopedParams);
    expect(body).not.toHaveProperty("parameters");
  });

  it("includes parameters when provided", () => {
    const body = buildStartRunBody({
      ...scopedParams,
      parameters: { foo: "bar" },
    });
    expect(body.parameters).toEqual({ foo: "bar" });
  });

  // --- env var validation ---

  it("throws when SNOWFLAKE_USERNAME is missing", () => {
    delete process.env.SNOWFLAKE_USERNAME;
    expect(() => buildStartRunBody(scopedParams)).toThrow("SNOWFLAKE_USERNAME");
  });

  it("throws when SNOWFLAKE_KEY_PAIR_KEY is missing", () => {
    delete process.env.SNOWFLAKE_KEY_PAIR_KEY;
    expect(() => buildStartRunBody(scopedParams)).toThrow("SNOWFLAKE_KEY_PAIR_KEY");
  });

  it("throws when SNOWFLAKE_WAREHOUSE is missing", () => {
    delete process.env.SNOWFLAKE_WAREHOUSE;
    expect(() => buildStartRunBody(scopedParams)).toThrow("SNOWFLAKE_WAREHOUSE");
  });

  it("throws when SNOWFLAKE_ROLE is missing", () => {
    delete process.env.SNOWFLAKE_ROLE;
    expect(() => buildStartRunBody(scopedParams)).toThrow("SNOWFLAKE_ROLE");
  });

  // --- key file validation ---

  it("throws when key file does not exist", () => {
    process.env.SNOWFLAKE_KEY_PAIR_KEY = "/nonexistent/path/key.pem";
    expect(() => buildStartRunBody(scopedParams)).toThrow("file not found");
  });

  it("throws when key file does not contain a PEM key", () => {
    writeFileSync(keyFilePath, "not a pem key");
    expect(() => buildStartRunBody(scopedParams)).toThrow("valid PEM key");
  });

  // --- confirmRunAllNodes safety check ---

  it("throws when no node scope and confirmRunAllNodes is not set", () => {
    expect(() => buildStartRunBody(unscopedParams)).toThrow(
      "confirmRunAllNodes"
    );
  });

  it("throws when no node scope and confirmRunAllNodes is false", () => {
    expect(() =>
      buildStartRunBody({ ...unscopedParams, confirmRunAllNodes: false })
    ).toThrow("confirmRunAllNodes");
  });

  it("allows run-all when confirmRunAllNodes is true", () => {
    const body = buildStartRunBody({
      ...unscopedParams,
      confirmRunAllNodes: true,
    });
    expect(body.runDetails).toEqual({ environmentID: "env-123" });
  });

  it("does not require confirmRunAllNodes when jobID is provided", () => {
    const body = buildStartRunBody(scopedParams);
    expect(body.runDetails.jobID).toBe("job-1");
  });

  it("does not require confirmRunAllNodes when includeNodesSelector is provided", () => {
    const body = buildStartRunBody({
      runDetails: {
        environmentID: "env-123",
        includeNodesSelector: "{ name: FOO }",
      },
    });
    expect(body.runDetails.includeNodesSelector).toBe("{ name: FOO }");
  });

  it("does not require confirmRunAllNodes when excludeNodesSelector is provided", () => {
    const body = buildStartRunBody({
      runDetails: {
        environmentID: "env-123",
        excludeNodesSelector: "{ name: BAR }",
      },
    });
    expect(body.runDetails.excludeNodesSelector).toBe("{ name: BAR }");
  });
});
