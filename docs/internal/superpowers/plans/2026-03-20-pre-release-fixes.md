# Pre-Release Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 7 pre-release issues (1 critical, 3 important, 2 suggestions, 1 manual check) identified during code review before production release.

**Architecture:** Add three utility functions to `src/types.ts` (`sanitizeResponse`, `validatePathSegment`, `handleToolError`), extend `client.post()`/`client.delete()` signatures to accept query params, then apply these across all 37 tool handlers. Each task is independent after the foundation tasks (1-3) are complete.

**Tech Stack:** TypeScript, Zod, vitest, @modelcontextprotocol/sdk

**Spec:** `docs/superpowers/specs/2026-03-20-pre-release-fixes-design.md`

---

### Task 1: Add `sanitizeResponse`, `validatePathSegment`, `handleToolError` to `src/types.ts` with tests

**Files:**
- Modify: `src/types.ts`
- Modify: `tests/schemas.test.ts`

- [ ] **Step 1: Write failing tests for `sanitizeResponse`**

Add to `tests/schemas.test.ts`:

```typescript
import { sanitizeResponse, validatePathSegment, handleToolError } from "../src/types.js";

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
```

- [ ] **Step 2: Write failing tests for `validatePathSegment`**

Add to `tests/schemas.test.ts`:

```typescript
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

  it("returns empty string unchanged", () => {
    expect(validatePathSegment("", "environmentID")).toBe("");
  });
});
```

- [ ] **Step 3: Write failing tests for `handleToolError`**

Add to `tests/schemas.test.ts`:

```typescript
import { CoalesceApiError } from "../src/client.js";

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
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `npx vitest run tests/schemas.test.ts`
Expected: FAIL — `sanitizeResponse`, `validatePathSegment`, `handleToolError` are not exported from `types.ts`

- [ ] **Step 5: Implement the three helper functions**

Add to the end of `src/types.ts`:

```typescript
export function sanitizeResponse(data: unknown): unknown {
  if (Array.isArray(data)) {
    return data.map(sanitizeResponse);
  }
  if (data && typeof data === "object") {
    const obj = data as Record<string, unknown>;
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      if (key === "userCredentials") continue;
      result[key] = sanitizeResponse(value);
    }
    return result;
  }
  return data;
}

export function validatePathSegment(value: string, name: string): string {
  if (/[\/\\]|\.\./.test(value)) {
    throw new Error(
      `Invalid ${name}: must not contain path separators or '..'`
    );
  }
  return value;
}

export function handleToolError(
  error: unknown
): { isError: true; content: { type: "text"; text: string }[] } {
  const message = error instanceof Error ? error.message : String(error);
  return {
    isError: true,
    content: [{ type: "text" as const, text: message }],
  };
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `npx vitest run tests/schemas.test.ts`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/types.ts tests/schemas.test.ts
git commit -m "feat: add sanitizeResponse, validatePathSegment, handleToolError helpers"
```

---

### Task 2: Extend `client.post()` and `client.delete()` to accept query params

**Files:**
- Modify: `src/client.ts`
- Modify: `tests/client.test.ts`

- [ ] **Step 1: Write failing tests for post/delete query params**

Add to `tests/client.test.ts` inside the `describe("request", ...)` block:

```typescript
it("appends query params on POST", async () => {
  const mockFetch = vi.fn().mockResolvedValue({
    ok: true,
    status: 200,
    json: () => Promise.resolve({ data: {} }),
  });
  vi.stubGlobal("fetch", mockFetch);

  const client = createClient({
    accessToken: "test-token",
    baseUrl: "https://app.coalescesoftware.io",
  });
  await client.post("/api/v1/gitAccounts", { name: "test" }, { accountOwner: "user-1" });

  const calledUrl = mockFetch.mock.calls[0][0];
  expect(calledUrl).toContain("accountOwner=user-1");
});

it("appends query params on DELETE", async () => {
  const mockFetch = vi.fn().mockResolvedValue({
    ok: true,
    status: 204,
  });
  vi.stubGlobal("fetch", mockFetch);

  const client = createClient({
    accessToken: "test-token",
    baseUrl: "https://app.coalescesoftware.io",
  });
  await client.delete("/api/v1/gitAccounts/123", { accountOwner: "user-1" });

  const calledUrl = mockFetch.mock.calls[0][0];
  expect(calledUrl).toContain("accountOwner=user-1");
});

it("post passes both query params and options correctly", async () => {
  const mockFetch = vi.fn().mockResolvedValue({
    ok: true,
    status: 200,
    json: () => Promise.resolve({ runCounter: 1 }),
  });
  vi.stubGlobal("fetch", mockFetch);

  const client = createClient({
    accessToken: "test-token",
    baseUrl: "https://app.coalescesoftware.io",
  });
  await client.post("/scheduler/startRun", { env: "1" }, { key: "val" }, { timeoutMs: 5000 });

  const calledUrl = mockFetch.mock.calls[0][0];
  expect(calledUrl).toContain("key=val");
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/client.test.ts`
Expected: FAIL — `post` and `delete` don't accept query params yet

- [ ] **Step 3: Update `client.post()` and `client.delete()` signatures**

In `src/client.ts`, change the `post` and `delete` methods in `createClient`:

```typescript
async post(
  path: string,
  body?: unknown,
  params?: QueryParams,
  options?: RequestOptions
): Promise<unknown> {
  return request("POST", path, params, body, options);
},

async delete(
  path: string,
  params?: QueryParams,
  options?: RequestOptions
): Promise<unknown> {
  return request("DELETE", path, params, undefined, options);
},
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/client.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/client.ts tests/client.test.ts
git commit -m "feat: extend client.post() and client.delete() to accept query params"
```

---

### Task 3: Migrate existing `client.post()` callers to new 4-arg signature

**Files:**
- Modify: `src/workflows/run-and-wait.ts:41`
- Modify: `src/workflows/retry-and-wait.ts:37`

- [ ] **Step 1: Update `run-and-wait.ts`**

At line 41, change:
```typescript
const startResult = (await client.post("/scheduler/startRun", body, {
  timeoutMs: remainingTimeMs(startedAt, timeout),
})) as Record<string, unknown>;
```
to:
```typescript
const startResult = (await client.post("/scheduler/startRun", body, undefined, {
  timeoutMs: remainingTimeMs(startedAt, timeout),
})) as Record<string, unknown>;
```

- [ ] **Step 2: Update `retry-and-wait.ts`**

At line 37, change:
```typescript
const rerunResult = (await client.post("/scheduler/rerun", body, {
  timeoutMs: remainingTimeMs(startedAt, timeout),
})) as Record<string, unknown>;
```
to:
```typescript
const rerunResult = (await client.post("/scheduler/rerun", body, undefined, {
  timeoutMs: remainingTimeMs(startedAt, timeout),
})) as Record<string, unknown>;
```

- [ ] **Step 3: Update workflow test assertions**

In `tests/workflows/run-and-wait.test.ts` (line ~80-89), change:
```typescript
expect(client.post).toHaveBeenCalledWith("/scheduler/startRun", {
  runDetails: { environmentID: "env-1", jobID: "job-1" },
  userCredentials: {
    snowflakeUsername: "user",
    snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
    snowflakeWarehouse: "wh",
    snowflakeRole: "role",
    snowflakeAuthType: "KeyPair",
  },
}, expect.objectContaining({ timeoutMs: expect.any(Number) }));
```
to:
```typescript
expect(client.post).toHaveBeenCalledWith("/scheduler/startRun", {
  runDetails: { environmentID: "env-1", jobID: "job-1" },
  userCredentials: {
    snowflakeUsername: "user",
    snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
    snowflakeWarehouse: "wh",
    snowflakeRole: "role",
    snowflakeAuthType: "KeyPair",
  },
}, undefined, expect.objectContaining({ timeoutMs: expect.any(Number) }));
```

In `tests/workflows/retry-and-wait.test.ts` (line ~80-89), apply the same change — insert `undefined,` before the `expect.objectContaining(...)` in the `client.post` assertion:
```typescript
expect(client.post).toHaveBeenCalledWith("/scheduler/rerun", {
  runDetails: { runID: "0" },
  userCredentials: {
    snowflakeUsername: "user",
    snowflakeKeyPairKey: "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
    snowflakeWarehouse: "wh",
    snowflakeRole: "role",
    snowflakeAuthType: "KeyPair",
  },
}, undefined, expect.objectContaining({ timeoutMs: expect.any(Number) }));
```

- [ ] **Step 4: Run all tests to verify nothing is broken**

Run: `npx vitest run`
Expected: All 124+ tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/workflows/run-and-wait.ts src/workflows/retry-and-wait.ts tests/workflows/run-and-wait.test.ts tests/workflows/retry-and-wait.test.ts
git commit -m "fix: migrate workflow post() calls to 4-arg signature"
```

---

### Task 4: Update `.gitignore`

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Add entries to `.gitignore`**

Append to `.gitignore`:
```
.env
.env.*
data/
*.pem
```

- [ ] **Step 2: Commit**

```bash
git add .gitignore
git commit -m "chore: add .env, data/, and *.pem to .gitignore"
```

---

### Task 5: Apply `sanitizeResponse` to run tools and workflows

**Files:**
- Modify: `src/tools/runs.ts` — `start-run` handler (line 144) and `retry-run` handler (line 173)
- Modify: `src/workflows/run-and-wait.ts` — `registerRunAndWait` handler (line 142)
- Modify: `src/workflows/retry-and-wait.ts` — `registerRetryAndWait` handler (line 136)
- Modify: `src/workflows/get-run-details.ts` — `registerGetRunDetails` handler (line 24)

- [ ] **Step 1: Update `src/tools/runs.ts`**

Add `sanitizeResponse` to the import from `../types.js`:
```typescript
import { ..., sanitizeResponse } from "../types.js";
```

In the `start-run` handler (line ~144), change:
```typescript
const result = await startRun(client, params);
return {
  content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
};
```
to:
```typescript
const result = await startRun(client, params);
return {
  content: [{ type: "text", text: JSON.stringify(sanitizeResponse(result), null, 2) }],
};
```

Same change in the `retry-run` handler (line ~173):
```typescript
const result = await retryRun(client, params);
return {
  content: [{ type: "text", text: JSON.stringify(sanitizeResponse(result), null, 2) }],
};
```

- [ ] **Step 2: Update `src/workflows/run-and-wait.ts`**

Add `sanitizeResponse` to the import from `../types.js`:
```typescript
import { StartRunParams, buildStartRunBody, WRITE_ANNOTATIONS, sanitizeResponse } from "../types.js";
```

In the `registerRunAndWait` handler (line ~142), change:
```typescript
const result = await runAndWait(client, params);
return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
```
to:
```typescript
const result = await runAndWait(client, params);
return { content: [{ type: "text", text: JSON.stringify(sanitizeResponse(result), null, 2) }] };
```

- [ ] **Step 3: Update `src/workflows/retry-and-wait.ts`**

Add `sanitizeResponse` to the import from `../types.js`:
```typescript
import { RerunParams, buildRerunBody, WRITE_ANNOTATIONS, sanitizeResponse } from "../types.js";
```

In the `registerRetryAndWait` handler (line ~136), change:
```typescript
const result = await retryAndWait(client, params);
return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
```
to:
```typescript
const result = await retryAndWait(client, params);
return { content: [{ type: "text", text: JSON.stringify(sanitizeResponse(result), null, 2) }] };
```

- [ ] **Step 4: Update `src/workflows/get-run-details.ts`**

Add `sanitizeResponse` to the import from `../types.js`:
```typescript
import { READ_ONLY_ANNOTATIONS, sanitizeResponse } from "../types.js";
```

In the `registerGetRunDetails` handler (line ~24), change:
```typescript
const result = await getRunDetails(client, params);
return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
```
to:
```typescript
const result = await getRunDetails(client, params);
return { content: [{ type: "text", text: JSON.stringify(sanitizeResponse(result), null, 2) }] };
```

- [ ] **Step 5: Run tests**

Run: `npx vitest run`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/tools/runs.ts src/workflows/run-and-wait.ts src/workflows/retry-and-wait.ts src/workflows/get-run-details.ts
git commit -m "security: sanitize API responses to strip leaked credentials"
```

---

### Task 6: Apply `validatePathSegment` to all tool handlers

**Files:**
- Modify: `src/tools/environments.ts` — validate `environmentID`
- Modify: `src/tools/nodes.ts` — validate `environmentID`, `workspaceID`, `nodeID`
- Modify: `src/tools/runs.ts` — validate `runID`
- Modify: `src/tools/projects.ts` — validate `projectID`
- Modify: `src/tools/git-accounts.ts` — validate `gitAccountID`
- Modify: `src/tools/users.ts` — validate `userID`, `projectID`, `environmentID`
- Modify: `src/workflows/get-run-details.ts` — validate `runID`
- Modify: `src/workflows/get-environment-overview.ts` — validate `environmentID`

- [ ] **Step 1: Update `src/tools/environments.ts`**

Add `validatePathSegment` to the import from `../types.js`:
```typescript
import { PaginationParams, READ_ONLY_ANNOTATIONS, validatePathSegment } from "../types.js";
```

In `getEnvironment` function (line 17), change:
```typescript
const { environmentID } = params;
return client.get(`/api/v1/environments/${environmentID}`, {});
```
to:
```typescript
const { environmentID } = params;
return client.get(`/api/v1/environments/${validatePathSegment(environmentID, "environmentID")}`, {});
```

- [ ] **Step 2: Update `src/tools/nodes.ts`**

Add `validatePathSegment` to the import from `../types.js`:
```typescript
import { PaginationParams, READ_ONLY_ANNOTATIONS, WRITE_ANNOTATIONS, IDEMPOTENT_WRITE_ANNOTATIONS, validatePathSegment } from "../types.js";
```

Apply `validatePathSegment` in all 6 data-access functions:

`listEnvironmentNodes` (line 16-17):
```typescript
const { environmentID, ...queryParams } = params;
return client.get(`/api/v1/environments/${validatePathSegment(environmentID, "environmentID")}/nodes`, queryParams);
```

`listWorkspaceNodes` (line 26-27):
```typescript
const { workspaceID, ...queryParams } = params;
return client.get(`/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes`, queryParams);
```

`getEnvironmentNode` (line 34-36):
```typescript
const { environmentID, nodeID } = params;
return client.get(`/api/v1/environments/${validatePathSegment(environmentID, "environmentID")}/nodes/${validatePathSegment(nodeID, "nodeID")}`, {});
```

`getWorkspaceNode` (line 45-46):
```typescript
const { workspaceID, nodeID } = params;
return client.get(`/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes/${validatePathSegment(nodeID, "nodeID")}`, {});
```

`createWorkspaceNode` (line 58-66):
```typescript
const { workspaceID, nodeType, predecessorNodeIDs, body } = params;
// ... existing merge logic unchanged ...
return client.post(`/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes`, merged);
```

`setWorkspaceNode` (line 77-80):
```typescript
const { workspaceID, nodeID, body } = params;
return client.put(`/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes/${validatePathSegment(nodeID, "nodeID")}`, body);
```

- [ ] **Step 3: Update `src/tools/runs.ts`**

Add `validatePathSegment` to the import from `../types.js`:
```typescript
import { ..., validatePathSegment } from "../types.js";
```

`getRun` (line 26-27):
```typescript
const { runID } = params;
return client.get(`/api/v1/runs/${validatePathSegment(runID, "runID")}`, {});
```

`getRunResults` (line 34-35):
```typescript
const { runID } = params;
return client.get(`/api/v1/runs/${validatePathSegment(runID, "runID")}/results`, {});
```

- [ ] **Step 4: Update `src/tools/projects.ts`**

Add `validatePathSegment` to the import from `../types.js`:
```typescript
import { READ_ONLY_ANNOTATIONS, WRITE_ANNOTATIONS, IDEMPOTENT_WRITE_ANNOTATIONS, DESTRUCTIVE_ANNOTATIONS, validatePathSegment } from "../types.js";
```

`getProject` (line 25):
```typescript
return client.get(`/api/v1/projects/${validatePathSegment(params.projectID, "projectID")}`, { ... });
```

`updateProject` (line 48):
```typescript
return client.patch(`/api/v1/projects/${validatePathSegment(params.projectID, "projectID")}`, { ... }, params.body);
```

`deleteProject` (line 61):
```typescript
return client.delete(`/api/v1/projects/${validatePathSegment(params.projectID, "projectID")}`);
```

- [ ] **Step 5: Update `src/tools/git-accounts.ts`**

Add `validatePathSegment` to the import from `../types.js`:
```typescript
import { READ_ONLY_ANNOTATIONS, WRITE_ANNOTATIONS, IDEMPOTENT_WRITE_ANNOTATIONS, DESTRUCTIVE_ANNOTATIONS, validatePathSegment } from "../types.js";
```

`getGitAccount` (line 24):
```typescript
return client.get(`/api/v1/gitAccounts/${validatePathSegment(params.gitAccountID, "gitAccountID")}`, { ... });
```

`updateGitAccount` (line 44-45):
```typescript
return client.patch(`/api/v1/gitAccounts/${validatePathSegment(params.gitAccountID, "gitAccountID")}`, { ... }, params.body);
```

`deleteGitAccount` (line 55-58) — also refactor to use query params (from spec section 4):
```typescript
return client.delete(
  `/api/v1/gitAccounts/${validatePathSegment(params.gitAccountID, "gitAccountID")}`,
  params.accountOwner ? { accountOwner: params.accountOwner } : undefined
);
```

`createGitAccount` (line 33-37) — also refactor to use query params:
```typescript
const qp = params.accountOwner ? { accountOwner: params.accountOwner } : undefined;
return client.post("/api/v1/gitAccounts", params.body, qp);
```

- [ ] **Step 6: Update `src/tools/users.ts`**

Add `validatePathSegment` to the import from `../types.js`:
```typescript
import { PaginationParams, READ_ONLY_ANNOTATIONS, IDEMPOTENT_WRITE_ANNOTATIONS, DESTRUCTIVE_ANNOTATIONS, validatePathSegment } from "../types.js";
```

`getUserRoles` (line 22-23):
```typescript
const { userID, ...queryParams } = params;
return client.get(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}`, queryParams);
```

`setOrgRole` (line 37-38):
```typescript
const { userID, body } = params;
return client.put(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/organizationRole`, body);
```

`setProjectRole` (line 45-46):
```typescript
const { userID, projectID, body } = params;
return client.put(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/projects/${validatePathSegment(projectID, "projectID")}`, body);
```

`deleteProjectRole` (line 53-54):
```typescript
const { userID, projectID } = params;
return client.delete(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/projects/${validatePathSegment(projectID, "projectID")}`);
```

`setEnvRole` (line 61-62):
```typescript
const { userID, environmentID, body } = params;
return client.put(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/environments/${validatePathSegment(environmentID, "environmentID")}`, body);
```

`deleteEnvRole` (line 69-70):
```typescript
const { userID, environmentID } = params;
return client.delete(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/environments/${validatePathSegment(environmentID, "environmentID")}`);
```

- [ ] **Step 7: Update `src/workflows/get-run-details.ts`**

Add `validatePathSegment` to the import from `../types.js`:
```typescript
import { READ_ONLY_ANNOTATIONS, sanitizeResponse, validatePathSegment } from "../types.js";
```

`getRunDetails` (line 10-13):
```typescript
const validRunID = validatePathSegment(params.runID, "runID");
const [run, results] = await Promise.all([
  client.get(`/api/v1/runs/${validRunID}`),
  client.get(`/api/v1/runs/${validRunID}/results`),
]);
```

- [ ] **Step 8: Update `src/workflows/get-environment-overview.ts`**

Add `validatePathSegment` to the import from `../types.js`:
```typescript
import { READ_ONLY_ANNOTATIONS, validatePathSegment } from "../types.js";
```

`getEnvironmentOverview` (line 10):
```typescript
const basePath = `/api/v1/environments/${validatePathSegment(params.environmentID, "environmentID")}`;
```

- [ ] **Step 9: Run all tests**

Run: `npx vitest run`
Expected: All tests PASS

- [ ] **Step 10: Commit**

```bash
git add src/tools/environments.ts src/tools/nodes.ts src/tools/runs.ts src/tools/projects.ts src/tools/git-accounts.ts src/tools/users.ts src/workflows/get-run-details.ts src/workflows/get-environment-overview.ts
git commit -m "security: add path traversal validation to all tool handlers"
```

---

### Task 7: Add try/catch to all 37 tool handlers

**Files:**
- Modify: `src/tools/environments.ts` (2 handlers)
- Modify: `src/tools/nodes.ts` (6 handlers)
- Modify: `src/tools/runs.ts` (7 handlers)
- Modify: `src/tools/projects.ts` (5 handlers)
- Modify: `src/tools/git-accounts.ts` (5 handlers)
- Modify: `src/tools/users.ts` (8 handlers)
- Modify: `src/workflows/run-and-wait.ts` (1 handler)
- Modify: `src/workflows/retry-and-wait.ts` (1 handler)
- Modify: `src/workflows/get-run-details.ts` (1 handler)
- Modify: `src/workflows/get-environment-overview.ts` (1 handler)
- Modify: one test file for error-path integration test

- [ ] **Step 1: Add `handleToolError` import to all tool/workflow files**

Each file needs to import `handleToolError` from `../types.js`. Add it to the existing import statement in each file.

- [ ] **Step 2: Wrap all 37 tool handler callbacks in try/catch**

For every `server.tool(...)` callback, wrap the body in try/catch. The pattern is the same for all handlers:

```typescript
// Before:
async (params) => {
  const result = await someFunction(client, params);
  return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
}

// After:
async (params) => {
  try {
    const result = await someFunction(client, params);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  } catch (error) {
    return handleToolError(error);
  }
}
```

Apply to all 37 handlers across the 10 files listed above.

- [ ] **Step 3: Write error-path integration test**

Add to `tests/tools/environments.test.ts`:

```typescript
import { CoalesceApiError } from "../../src/client.js";

it("getEnvironment still throws CoalesceApiError from data-access layer", async () => {
  const client = createMockClient();
  client.get.mockRejectedValue(new CoalesceApiError("Not found", 404));

  const { getEnvironment } = await import("../../src/tools/environments.js");
  await expect(getEnvironment(client as any, { environmentID: "bad" })).rejects.toThrow("Not found");
});
```

Note: The try/catch is at the `server.tool()` handler level, so data-access functions still throw. The `handleToolError` unit test (Task 1) confirms the handler-level catch formats errors into `{ isError: true }` MCP responses.

- [ ] **Step 4: Run all tests**

Run: `npx vitest run`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/tools/ src/workflows/ tests/tools/environments.test.ts
git commit -m "feat: add try/catch error handling to all 37 tool handlers"
```

---

### Task 8: Update git-accounts test assertions for new signatures

**Files:**
- Modify: `tests/tools/git-accounts.test.ts`

After Task 6 refactored `createGitAccount` and `deleteGitAccount` to use query params, the test assertions need updating.

- [ ] **Step 1: Update `createGitAccount` test assertion**

In `tests/tools/git-accounts.test.ts` (line ~57), change:
```typescript
expect(client.post).toHaveBeenCalledWith("/api/v1/gitAccounts", body);
```
to:
```typescript
expect(client.post).toHaveBeenCalledWith("/api/v1/gitAccounts", body, undefined);
```

- [ ] **Step 2: Update `deleteGitAccount` test assertion**

In `tests/tools/git-accounts.test.ts` (line ~78), change:
```typescript
expect(client.delete).toHaveBeenCalledWith("/api/v1/gitAccounts/ga-1");
```
to:
```typescript
expect(client.delete).toHaveBeenCalledWith("/api/v1/gitAccounts/ga-1", undefined);
```

- [ ] **Step 3: Run full test suite**

Run: `npx vitest run`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add tests/tools/git-accounts.test.ts
git commit -m "test: update git-accounts assertions for new post/delete signatures"
```

---

### Task 9: Add `buildRerunBody` unit tests

**Files:**
- Create: `tests/build-rerun-body.test.ts`

- [ ] **Step 1: Create `tests/build-rerun-body.test.ts`**

Mirror the structure of `tests/build-start-run-body.test.ts`:

```typescript
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { writeFileSync, unlinkSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { buildRerunBody } from "../src/types.js";

describe("buildRerunBody", () => {
  const originalEnv = process.env;
  const tempDir = join(tmpdir(), "coalesce-rerun-test-" + process.pid);
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

  it("throws when SNOWFLAKE_KEY_PAIR_KEY is missing", () => {
    delete process.env.SNOWFLAKE_KEY_PAIR_KEY;
    expect(() => buildRerunBody(validParams)).toThrow("SNOWFLAKE_KEY_PAIR_KEY");
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
    expect(() => buildRerunBody(validParams)).toThrow("valid PEM key");
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
});
```

- [ ] **Step 2: Run tests**

Run: `npx vitest run tests/build-rerun-body.test.ts`
Expected: All PASS (implementation already exists, just no tests)

- [ ] **Step 3: Commit**

```bash
git add tests/build-rerun-body.test.ts
git commit -m "test: add buildRerunBody unit tests"
```

---

### Task 10: Verify npm package name and run final checks

- [ ] **Step 1: Check npm package name availability**

Run: `npm view coalesce-transform-mcp`
Expected: 404 / "Not Found" (name is available)

- [ ] **Step 2: Run full test suite**

Run: `npx vitest run`
Expected: All tests PASS

- [ ] **Step 3: Run TypeScript type check**

Run: `npx tsc --noEmit`
Expected: No errors

- [ ] **Step 4: Run build**

Run: `npm run build`
Expected: Clean build in `dist/`

- [ ] **Step 5: Run npm audit**

Run: `npm audit`
Expected: 0 vulnerabilities
