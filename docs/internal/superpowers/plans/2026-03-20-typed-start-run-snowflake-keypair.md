# Typed `start-run` with Snowflake Key Pair Auth — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the generic untyped body on `start-run` and `run-and-wait` with a fully typed Zod schema that enforces Snowflake key pair authentication.

**Architecture:** Add shared Zod schemas (`RunDetailsSchema`, `UserCredentialsSchema`, `StartRunParams`) to `src/types.ts`. Update `start-run` in `src/tools/runs.ts` and `run-and-wait` in `src/workflows/run-and-wait.ts` to use these schemas, constructing the API request body with auto-injected `snowflakeAuthType: "KeyPair"`.

**Tech Stack:** TypeScript, Zod, vitest, MCP SDK

---

## File Structure

| File | Action | Responsibility |
| --- | --- | --- |
| `src/types.ts` | Modify | Add `RunDetailsSchema`, `UserCredentialsSchema`, `StartRunParams` |
| `src/tools/runs.ts` | Modify | Update `startRun()` and `start-run` tool registration |
| `src/workflows/run-and-wait.ts` | Modify | Update `runAndWait()` and `run-and-wait` tool registration |
| `tests/build-start-run-body.test.ts` | Create | Unit tests for request body construction |
| `tests/schemas.test.ts` | Create | Unit tests for Zod schema validation |

---

### Task 1: Add shared Zod schemas to `src/types.ts`

**Files:**
- Modify: `src/types.ts`
- Test: `tests/schemas.test.ts`

- [ ] **Step 1: Write failing tests for the schemas**

Create `tests/schemas.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { RunDetailsSchema, UserCredentialsSchema, StartRunParams } from "../src/types.js";

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
    userCredentials: {
      snowflakeUsername: "user",
      snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----",
      snowflakeWarehouse: "WH",
      snowflakeRole: "ROLE",
    },
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
    const result = StartRunParams.safeParse({
      userCredentials: validParams.userCredentials,
    });
    expect(result.success).toBe(false);
  });

  it("rejects missing userCredentials", () => {
    const result = StartRunParams.safeParse({
      runDetails: validParams.runDetails,
    });
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/schemas.test.ts`
Expected: FAIL — `RunDetailsSchema`, `UserCredentialsSchema`, `StartRunParams` not exported from `../src/types.js`

- [ ] **Step 3: Implement the schemas in `src/types.ts`**

Add after the existing `PaginationParams` definition:

```typescript
// --- startRun / run-and-wait schemas ---

export const RunDetailsSchema = z.object({
  environmentID: z.string().describe("The environment being refreshed"),
  includeNodesSelector: z
    .string()
    .optional()
    .describe("Nodes included for an ad-hoc job"),
  excludeNodesSelector: z
    .string()
    .optional()
    .describe("Nodes excluded for an ad-hoc job"),
  jobID: z.string().optional().describe("The ID of a job being run"),
  parallelism: z
    .number()
    .int()
    .optional()
    .describe("Max parallel nodes to run (API default: 16)"),
  forceIgnoreWorkspaceStatus: z
    .boolean()
    .optional()
    .describe(
      "Allow refresh even if last deploy failed (API default: false). Use with caution."
    ),
});

export const UserCredentialsSchema = z.object({
  snowflakeUsername: z.string().describe("Snowflake account username"),
  snowflakeKeyPairKey: z
    .string()
    .describe(
      "PEM-encoded private key for Snowflake auth. Use \\n for line breaks in JSON."
    ),
  snowflakeKeyPairPass: z
    .string()
    .optional()
    .describe(
      "Password to decrypt an encrypted private key. Only required when the private key is encrypted."
    ),
  snowflakeWarehouse: z.string().describe("Snowflake compute warehouse"),
  snowflakeRole: z.string().describe("Snowflake user role"),
});

export const StartRunParams = z.object({
  runDetails: RunDetailsSchema,
  userCredentials: UserCredentialsSchema,
  parameters: z
    .record(z.string())
    .optional()
    .describe("Arbitrary key-value parameters to pass to the run"),
});
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/schemas.test.ts`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/types.ts tests/schemas.test.ts
git commit -m "feat: add RunDetailsSchema, UserCredentialsSchema, StartRunParams to types"
```

---

### Task 2: Add `buildStartRunBody` helper and test it

**Files:**
- Modify: `src/types.ts`
- Test: `tests/build-start-run-body.test.ts`

- [ ] **Step 1: Write failing tests for body construction**

Create `tests/build-start-run-body.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { buildStartRunBody } from "../src/types.js";

describe("buildStartRunBody", () => {
  const baseParams = {
    runDetails: { environmentID: "env-123" },
    userCredentials: {
      snowflakeUsername: "user",
      snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----",
      snowflakeWarehouse: "WH",
      snowflakeRole: "ROLE",
    },
  };

  it("injects snowflakeAuthType as KeyPair", () => {
    const body = buildStartRunBody(baseParams);
    expect(body.userCredentials.snowflakeAuthType).toBe("KeyPair");
  });

  it("preserves runDetails as-is", () => {
    const body = buildStartRunBody(baseParams);
    expect(body.runDetails).toEqual({ environmentID: "env-123" });
  });

  it("preserves userCredentials fields", () => {
    const body = buildStartRunBody(baseParams);
    expect(body.userCredentials.snowflakeUsername).toBe("user");
    expect(body.userCredentials.snowflakeKeyPairKey).toBe(
      "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----"
    );
    expect(body.userCredentials.snowflakeWarehouse).toBe("WH");
    expect(body.userCredentials.snowflakeRole).toBe("ROLE");
  });

  it("includes snowflakeKeyPairPass when provided", () => {
    const body = buildStartRunBody({
      ...baseParams,
      userCredentials: {
        ...baseParams.userCredentials,
        snowflakeKeyPairPass: "secret",
      },
    });
    expect(body.userCredentials.snowflakeKeyPairPass).toBe("secret");
  });

  it("omits parameters when not provided", () => {
    const body = buildStartRunBody(baseParams);
    expect(body).not.toHaveProperty("parameters");
  });

  it("includes parameters when provided", () => {
    const body = buildStartRunBody({
      ...baseParams,
      parameters: { foo: "bar" },
    });
    expect(body.parameters).toEqual({ foo: "bar" });
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/build-start-run-body.test.ts`
Expected: FAIL — `buildStartRunBody` not exported

- [ ] **Step 3: Implement `buildStartRunBody` in `src/types.ts`**

Add after `StartRunParams`:

```typescript
export type StartRunInput = z.infer<typeof StartRunParams>;

export function buildStartRunBody(params: StartRunInput) {
  return {
    runDetails: params.runDetails,
    userCredentials: {
      ...params.userCredentials,
      snowflakeAuthType: "KeyPair",
    },
    ...(params.parameters ? { parameters: params.parameters } : {}),
  };
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/build-start-run-body.test.ts`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/types.ts tests/build-start-run-body.test.ts
git commit -m "feat: add buildStartRunBody helper with KeyPair injection"
```

---

### Task 3: Update `start-run` tool in `src/tools/runs.ts`

**Files:**
- Modify: `src/tools/runs.ts:1-126`

- [ ] **Step 1: Update imports**

Replace lines 1-9 of `src/tools/runs.ts`:

```typescript
import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient, QueryParams } from "../client.js";
import {
  PaginationParams,
  StartRunParams,
  buildStartRunBody,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../types.js";
```

- [ ] **Step 2: Update `startRun` function signature (lines 34-39)**

Replace:

```typescript
export async function startRun(
  client: CoalesceClient,
  params: { body: Record<string, unknown> }
): Promise<unknown> {
  return client.post("/scheduler/startRun", params.body);
}
```

With:

```typescript
export async function startRun(
  client: CoalesceClient,
  params: z.infer<typeof StartRunParams>
): Promise<unknown> {
  const body = buildStartRunBody(params);
  return client.post("/scheduler/startRun", body);
}
```

- [ ] **Step 3: Update `start-run` tool registration (lines 109-126)**

Replace:

```typescript
  server.tool(
    "start-run",
    "Start a new Coalesce run",
    {
      body: z
        .record(z.unknown())
        .describe(
          "The startRun request body (environmentID, runDetails, etc.)"
        ),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      const result = await startRun(client, params);
      return {
        content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
      };
    }
  );
```

With:

```typescript
  server.tool(
    "start-run",
    "Start a new Coalesce run with Snowflake key pair authentication",
    StartRunParams.shape,
    WRITE_ANNOTATIONS,
    async (params) => {
      const result = await startRun(client, params);
      return {
        content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
      };
    }
  );
```

- [ ] **Step 4: Verify the project builds**

Run: `npx tsc --noEmit`
Expected: No errors

- [ ] **Step 5: Commit**

```bash
git add src/tools/runs.ts
git commit -m "feat: update start-run tool with typed Snowflake key pair schema"
```

---

### Task 4: Update `run-and-wait` workflow in `src/workflows/run-and-wait.ts`

**Files:**
- Modify: `src/workflows/run-and-wait.ts`

- [ ] **Step 1: Update imports (lines 1-4)**

Replace:

```typescript
import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { WRITE_ANNOTATIONS } from "../types.js";
```

With:

```typescript
import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  StartRunParams,
  buildStartRunBody,
  WRITE_ANNOTATIONS,
} from "../types.js";
```

- [ ] **Step 2: Update `runAndWait` function signature and body construction (lines 6-18)**

Replace:

```typescript
export async function runAndWait(
  client: CoalesceClient,
  params: {
    body: Record<string, unknown>;
    pollInterval?: number;
    timeout?: number;
  }
): Promise<unknown> {
  const pollInterval = (params.pollInterval ?? 10) * 1000;
  const timeout = (params.timeout ?? 1800) * 1000;

  // Start the run
  const startResult = (await client.post("/scheduler/startRun", params.body)) as Record<string, unknown>;
```

With:

```typescript
export async function runAndWait(
  client: CoalesceClient,
  params: z.infer<typeof StartRunParams> & {
    pollInterval?: number;
    timeout?: number;
  }
): Promise<unknown> {
  const pollInterval = (params.pollInterval ?? 10) * 1000;
  const timeout = (params.timeout ?? 1800) * 1000;

  // Start the run
  const body = buildStartRunBody(params);
  const startResult = (await client.post("/scheduler/startRun", body)) as Record<string, unknown>;
```

- [ ] **Step 3: Update tool registration schema (lines 52-67)**

Replace:

```typescript
export function registerRunAndWait(server: McpServer, client: CoalesceClient): void {
  server.tool(
    "run-and-wait",
    "Start a Coalesce run and wait for it to complete. Polls run status until finished or timeout.",
    {
      body: z.record(z.unknown()).describe("The startRun request body (environmentID, runDetails, etc.)"),
      pollInterval: z.number().optional().describe("Seconds between status checks (default: 10)"),
      timeout: z.number().optional().describe("Max seconds to wait (default: 1800 = 30 min)"),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      const result = await runAndWait(client, params);
      return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    }
  );
}
```

With:

```typescript
export function registerRunAndWait(server: McpServer, client: CoalesceClient): void {
  server.tool(
    "run-and-wait",
    "Start a Coalesce run with Snowflake key pair auth and wait for completion. Polls run status until finished or timeout.",
    StartRunParams.extend({
      pollInterval: z.number().optional().describe("Seconds between status checks (default: 10)"),
      timeout: z.number().optional().describe("Max seconds to wait (default: 1800 = 30 min)"),
    }).shape,
    WRITE_ANNOTATIONS,
    async (params) => {
      const result = await runAndWait(client, params);
      return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    }
  );
}
```

- [ ] **Step 4: Verify the project builds**

Run: `npx tsc --noEmit`
Expected: No errors

- [ ] **Step 5: Run all tests**

Run: `npx vitest run`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add src/workflows/run-and-wait.ts
git commit -m "feat: update run-and-wait with typed Snowflake key pair schema"
```

---

### Task 5: Final build and verification

**Files:** None (verification only)

- [ ] **Step 1: Full build**

Run: `npm run build`
Expected: Clean build, no errors

- [ ] **Step 2: Run full test suite**

Run: `npm test`
Expected: ALL PASS

- [ ] **Step 3: Verify no regressions in unchanged tools**

Quickly scan that `list-runs`, `get-run`, `get-run-results`, `run-status`, `retry-run`, `cancel-run` tool registrations in `src/tools/runs.ts` are untouched.

- [ ] **Step 4: Commit build output (if needed)**

If the build produced updated `dist/` files that are tracked:

```bash
git add dist/
git commit -m "chore: rebuild dist after typed start-run changes"
```
