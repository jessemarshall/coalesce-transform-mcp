import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, it, expect } from "vitest";
import {
  RunDetailsSchema,
  UserCredentialsSchema,
  StartRunParams,
  buildJsonToolResponse,
  sanitizeResponse,
  validatePathSegment,
  handleToolError,
} from "../src/coalesce/types.js";
import { CoalesceApiError } from "../src/client.js";

const tempDirs: string[] = [];

afterEach(() => {
  for (const directory of tempDirs.splice(0, tempDirs.length)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("RunDetailsSchema", () => {
  it("accepts valid runDetails with only environmentID", () => {
    const result = RunDetailsSchema.safeParse({ environmentID: "env-123" });
    expect(result.success).toBe(true);
  });

  it("rejects missing environmentID", () => {
    const result = RunDetailsSchema.safeParse({});
    expect(result.success).toBe(false);
  });

  it("accepts all optional fields", () => {
    const result = RunDetailsSchema.safeParse({
      environmentID: "env-123",
      includeNodesSelector: "{ location: SAMPLE name: CUSTOMER }",
      excludeNodesSelector: "{ location: STAGING }",
      jobID: "job-456",
      parallelism: 8,
      forceIgnoreWorkspaceStatus: true,
    });
    expect(result.success).toBe(true);
  });

  it("rejects non-integer parallelism", () => {
    const result = RunDetailsSchema.safeParse({
      environmentID: "env-123",
      parallelism: 3.5,
    });
    expect(result.success).toBe(false);
  });
});

describe("UserCredentialsSchema", () => {
  it("accepts valid credentials without passphrase", () => {
    const result = UserCredentialsSchema.safeParse({
      snowflakeUsername: "user",
      snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----",
      snowflakeWarehouse: "WH",
      snowflakeRole: "ROLE",
    });
    expect(result.success).toBe(true);
  });

  it("accepts valid credentials with passphrase", () => {
    const result = UserCredentialsSchema.safeParse({
      snowflakeUsername: "user",
      snowflakeKeyPairKey: "-----BEGIN ENCRYPTED PRIVATE KEY-----\nxxx\n-----END ENCRYPTED PRIVATE KEY-----",
      snowflakeKeyPairPass: "secret",
      snowflakeWarehouse: "WH",
      snowflakeRole: "ROLE",
    });
    expect(result.success).toBe(true);
  });

  it("rejects missing snowflakeUsername", () => {
    const result = UserCredentialsSchema.safeParse({
      snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----",
      snowflakeWarehouse: "WH",
      snowflakeRole: "ROLE",
    });
    expect(result.success).toBe(false);
  });

  it("rejects missing snowflakeKeyPairKey", () => {
    const result = UserCredentialsSchema.safeParse({
      snowflakeUsername: "user",
      snowflakeWarehouse: "WH",
      snowflakeRole: "ROLE",
    });
    expect(result.success).toBe(false);
  });

  it("rejects missing snowflakeWarehouse", () => {
    const result = UserCredentialsSchema.safeParse({
      snowflakeUsername: "user",
      snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----",
      snowflakeRole: "ROLE",
    });
    expect(result.success).toBe(false);
  });

  it("rejects missing snowflakeRole", () => {
    const result = UserCredentialsSchema.safeParse({
      snowflakeUsername: "user",
      snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----",
      snowflakeWarehouse: "WH",
    });
    expect(result.success).toBe(false);
  });
});

describe("StartRunParams", () => {
  const validParams = {
    runDetails: { environmentID: "env-123" },
  };

  it("accepts valid params without parameters", () => {
    const result = StartRunParams.safeParse(validParams);
    expect(result.success).toBe(true);
  });

  it("accepts valid params with parameters", () => {
    const result = StartRunParams.safeParse({
      ...validParams,
      parameters: { foo: "bar" },
    });
    expect(result.success).toBe(true);
  });

  it("rejects missing runDetails", () => {
    const result = StartRunParams.safeParse({});
    expect(result.success).toBe(false);
  });

  it("rejects non-string parameter values", () => {
    const result = StartRunParams.safeParse({
      ...validParams,
      parameters: { foo: 123 },
    });
    expect(result.success).toBe(false);
  });
});

describe("sanitizeResponse", () => {
  it("strips top-level userCredentials", () => {
    const input = { runCounter: 1, userCredentials: { key: "secret" } };
    expect(sanitizeResponse(input)).toEqual({ runCounter: 1 });
  });

  it("strips nested userCredentials recursively", () => {
    const input = { data: { userCredentials: { key: "secret" }, id: 1 } };
    expect(sanitizeResponse(input)).toEqual({ data: { id: 1 } });
  });

  it("strips userCredentials from objects inside arrays", () => {
    const input = [{ userCredentials: { key: "secret" }, id: 1 }];
    expect(sanitizeResponse(input)).toEqual([{ id: 1 }]);
  });

  it("returns object unchanged when no userCredentials", () => {
    const input = { runCounter: 1, status: "ok" };
    expect(sanitizeResponse(input)).toEqual({ runCounter: 1, status: "ok" });
  });

  it("returns primitives as-is", () => {
    expect(sanitizeResponse(null)).toBe(null);
    expect(sanitizeResponse("hello")).toBe("hello");
    expect(sanitizeResponse(42)).toBe(42);
    expect(sanitizeResponse(undefined)).toBe(undefined);
  });
});

describe("validatePathSegment", () => {
  it("returns clean ID unchanged", () => {
    expect(validatePathSegment("env-123", "environmentID")).toBe("env-123");
  });

  it("throws for ID containing /", () => {
    expect(() => validatePathSegment("../../admin", "environmentID")).toThrow("must not contain path separators");
  });

  it("throws for ID containing ..", () => {
    expect(() => validatePathSegment("foo..bar", "environmentID")).toThrow("must not contain path separators");
  });

  it("throws for ID containing backslash", () => {
    expect(() => validatePathSegment("foo\\bar", "environmentID")).toThrow("must not contain path separators");
  });

  it("throws for empty string", () => {
    expect(() => validatePathSegment("", "environmentID")).toThrow("must not be empty");
  });
});

describe("handleToolError", () => {
  it("returns isError structure for Error instances", () => {
    const result = handleToolError(new Error("something broke"));
    expect(result).toEqual({
      isError: true,
      content: [{ type: "text", text: "something broke" }],
    });
  });

  it("returns isError structure for non-Error values", () => {
    const result = handleToolError("string error");
    expect(result).toEqual({
      isError: true,
      content: [{ type: "text", text: "string error" }],
    });
  });

  it("extracts message from CoalesceApiError", () => {
    const result = handleToolError(new CoalesceApiError("Not found", 404));
    expect(result).toEqual({
      isError: true,
      content: [{ type: "text", text: "Not found" }],
    });
  });
});

describe("buildJsonToolResponse", () => {
  it("returns small payloads inline", () => {
    const result = buildJsonToolResponse(
      "list-environments",
      { data: [{ id: "env-1" }] },
      { maxInlineBytes: 4096 }
    );

    expect(result).toEqual({
      content: [
        {
          type: "text",
          text: JSON.stringify({ data: [{ id: "env-1" }] }, null, 2),
        },
      ],
    });
  });

  it("auto-caches large payloads to disk and returns metadata", () => {
    const baseDir = mkdtempSync(join(tmpdir(), "coalesce-auto-cache-schema-"));
    tempDirs.push(baseDir);

    const payload = {
      data: [
        {
          id: "node-1",
          name: "STG_CUSTOMER",
          nodeType: "Stage",
          description: "x".repeat(1024),
        },
      ],
    };

    const result = buildJsonToolResponse("list-workspace-nodes", payload, {
      baseDir,
      maxInlineBytes: 128,
    });

    const metadata = JSON.parse(result.content[0]!.text);
    expect(metadata).toMatchObject({
      autoCached: true,
      toolName: "list-workspace-nodes",
      filePath: expect.stringContaining(join(baseDir, "data", "auto-cache")),
      maxInlineBytes: 128,
    });

    const cached = JSON.parse(readFileSync(metadata.filePath, "utf8"));
    expect(cached).toEqual(payload);
  });
});
