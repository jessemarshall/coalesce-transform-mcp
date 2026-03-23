# Pre-Release Fixes Design Spec

**Date:** 2026-03-20
**Status:** Draft
**Scope:** 7 fixes identified during code review, required before production release

---

## 1. Credential Sanitization (Critical)

**Problem:** `start-run`, `retry-run`, `run-and-wait`, and `retry-and-wait` return raw API responses via `JSON.stringify`. The Coalesce API may echo back `userCredentials` (including the PEM private key) in responses, which would leak into MCP tool output visible to the LLM.

**Solution:** Add a recursive `sanitizeResponse` helper in `src/types.ts` that strips `userCredentials` at any depth:

```typescript
export function sanitizeResponse(data: unknown): unknown {
  if (Array.isArray(data)) {
    return data.map(sanitizeResponse);
  }
  if (data && typeof data === 'object') {
    const obj = data as Record<string, unknown>;
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      if (key === 'userCredentials') continue;
      result[key] = sanitizeResponse(value);
    }
    return result;
  }
  return data;
}
```

Apply in:
- `src/tools/runs.ts` — `start-run` and `retry-run` handlers (wrap the API result before `JSON.stringify`)
- `src/workflows/run-and-wait.ts` — sanitize `startResult`, `status`, and `results` before returning
- `src/workflows/retry-and-wait.ts` — same as above
- `src/workflows/get-run-details.ts` — sanitize run metadata in case the GET endpoint echoes credentials

**Tests:** Add unit tests for `sanitizeResponse` covering:
- Object with top-level `userCredentials` key → stripped
- Object with nested `userCredentials` (e.g., `{ data: { userCredentials: ... } }`) → stripped recursively
- Array containing objects with `userCredentials` → each stripped
- Object without `userCredentials` → unchanged
- Non-object input (null, string, number) → returned as-is

---

## 2. `.gitignore` Updates

**Problem:** `.env` files, `data/` directory (recommended in usage guide for saved responses), and PEM key files are not in `.gitignore`, risking accidental credential commits.

**Solution:** Add to `.gitignore`:
```
.env
.env.*
data/
*.pem
```

**Tests:** None needed.

---

## 3. Path Traversal Validation

**Problem:** User-supplied IDs (environmentID, workspaceID, nodeID, etc.) are interpolated into URL paths. A malicious ID like `../../admin` could reach unintended API endpoints.

**Solution:** Add a `validatePathSegment` function in `src/types.ts`:

```typescript
export function validatePathSegment(value: string, name: string): string {
  if (/[\/\\]|\.\./.test(value)) {
    throw new Error(`Invalid ${name}: must not contain path separators or '..'`);
  }
  return value;
}
```

**Approach:** Add validation calls in each tool handler where user-supplied IDs are interpolated into paths. Export `validatePathSegment` from `types.ts` and call it inline:

```typescript
const path = `/api/v1/environments/${validatePathSegment(params.environmentID, 'environmentID')}`;
```

Note: URL-encoded traversal attacks (e.g., `%2F`, `%2e%2e`) are not a concern here because the `URL` constructor in `buildUrl` treats the path literally and will further encode percent characters.

**Parameters requiring validation by file:**

| File | Parameters to validate |
|------|----------------------|
| `src/tools/environments.ts` | `environmentID` |
| `src/tools/nodes.ts` | `environmentID`, `workspaceID`, `nodeID` |
| `src/tools/runs.ts` | `runID` |
| `src/tools/projects.ts` | `projectID` |
| `src/tools/git-accounts.ts` | `gitAccountID` |
| `src/tools/users.ts` | `userID`, `projectID`, `environmentID` |
| `src/workflows/get-run-details.ts` | `runID` |
| `src/workflows/get-environment-overview.ts` | `environmentID` |

Note: `runCounter` values derived from API responses (in `run-and-wait.ts` and `retry-and-wait.ts`) do not need validation since they are not user input.

**Tests:** Add unit tests for `validatePathSegment`:
- Clean ID string → returned unchanged
- ID with `/` → throws
- ID with `..` → throws
- ID with `\` → throws
- Empty string → returned unchanged (empty path segment is harmless and will fail at the API level)

---

## 4. Consistent Query Params in Git Accounts

**Problem:** `createGitAccount` and `deleteGitAccount` in `src/tools/git-accounts.ts` manually append `?accountOwner=...` to the URL string. Other tools use the client's query param mechanism.

**Solution:** Extend `client.post()` and `client.delete()` in `src/client.ts` to accept an optional `params` query parameter object, matching how `client.get()` and `client.patch()` already work.

Current signatures:
```typescript
post(path: string, body?: unknown, options?: RequestOptions): Promise<unknown>
delete(path: string, options?: RequestOptions): Promise<unknown>
```

New signatures:
```typescript
post(path: string, body?: unknown, params?: QueryParams, options?: RequestOptions): Promise<unknown>
delete(path: string, params?: QueryParams, options?: RequestOptions): Promise<unknown>
```

**Breaking change migration:** Two existing callers pass `RequestOptions` as the third argument to `client.post()`:
- `src/workflows/run-and-wait.ts` (line ~41): `client.post("/scheduler/startRun", body, { timeoutMs: remainingTimeMs(...) })`
- `src/workflows/retry-and-wait.ts` (line ~37): `client.post("/scheduler/rerun", body, { timeoutMs: remainingTimeMs(...) })`

These must be updated to pass `undefined` for `params` and shift `options` to the fourth argument:
```typescript
client.post("/scheduler/startRun", body, undefined, { timeoutMs: remainingTimeMs(...) })
```

Mock clients in test files (`tests/tools/*.test.ts`, `tests/workflows/*.test.ts`) will also need their `post` and `delete` mock signatures updated to match.

Then in `git-accounts.ts`:
```typescript
// Before:
const path = params.accountOwner
  ? `/api/v1/gitAccounts?accountOwner=${encodeURIComponent(params.accountOwner)}`
  : "/api/v1/gitAccounts";
return client.post(path, params.body);

// After:
const qp = params.accountOwner ? { accountOwner: params.accountOwner } : undefined;
return client.post("/api/v1/gitAccounts", params.body, qp);
```

**Tests:**
- Add client test verifying `post()` and `delete()` correctly append query params via `buildUrl`
- Add client test verifying `post()` with both `params` and `options` passes both correctly
- Update existing git account tests to verify `accountOwner` is passed as a query param

---

## 5. Try/Catch in Tool Handlers

**Problem:** Tool handler callbacks don't catch errors. If a `CoalesceApiError` is thrown, it propagates to the MCP SDK which may not format it well for the client.

**Solution:** Add a `handleToolError` helper in `src/types.ts`:

```typescript
export function handleToolError(error: unknown): { isError: true; content: { type: "text"; text: string }[] } {
  const message = error instanceof Error ? error.message : String(error);
  return {
    isError: true,
    content: [{ type: "text" as const, text: message }],
  };
}
```

Wrap each tool handler in try/catch:
```typescript
server.tool("tool-name", schema, async (params) => {
  try {
    // existing logic
  } catch (error) {
    return handleToolError(error);
  }
});
```

This applies to all 37 tool handlers across:
- `src/tools/environments.ts` (2 tools)
- `src/tools/nodes.ts` (6 tools)
- `src/tools/runs.ts` (7 tools)
- `src/tools/projects.ts` (5 tools)
- `src/tools/git-accounts.ts` (5 tools)
- `src/tools/users.ts` (8 tools)
- `src/workflows/run-and-wait.ts` (1 tool)
- `src/workflows/retry-and-wait.ts` (1 tool)
- `src/workflows/get-run-details.ts` (1 tool)
- `src/workflows/get-environment-overview.ts` (1 tool)

Note: `run-and-wait.ts` and `retry-and-wait.ts` already have internal try/catch blocks for polling errors (lines ~66-71). The new try/catch wraps the entire tool handler at the `server.tool()` callback level, which is a different scope and catches errors the internal blocks don't (e.g., errors in `buildStartRunBody` or `buildRerunBody` before polling begins).

**Tests:**
- Add a test verifying `handleToolError` returns the correct structure for `Error` inputs
- Add a test verifying `handleToolError` returns the correct structure for non-Error inputs (e.g., string)
- Add a test verifying `handleToolError` works with `CoalesceApiError` instances
- Add at least one integration test in a tool test file that mocks `client.get` to throw and verifies the handler returns `{ isError: true }` instead of throwing

---

## 6. `buildRerunBody` Tests

**Problem:** `buildRerunBody` has no direct unit tests. It's only tested indirectly through `retry-run` and `retry-and-wait` workflow tests.

**Solution:** Add a new test file `tests/build-rerun-body.test.ts` (mirroring the existing `tests/build-start-run-body.test.ts`) with tests covering:

1. Valid input produces correct request body with `snowflakeAuthType: "KeyPair"` injected
2. Missing `SNOWFLAKE_USERNAME` throws descriptive error
3. Missing `SNOWFLAKE_KEY_PAIR_KEY` throws descriptive error
4. Missing `SNOWFLAKE_WAREHOUSE` throws descriptive error
5. Missing `SNOWFLAKE_ROLE` throws descriptive error
6. Invalid PEM file (no `-----BEGIN`) throws error
7. Nonexistent key file path throws error
8. Optional `snowflakeKeyPairPass` included when env var is set
9. Optional `snowflakeKeyPairPass` omitted when env var is not set
10. `forceIgnoreWorkspaceStatus` passed through when set
11. Optional `parameters` record passed through when provided

---

## 7. Verify npm Package Name

**Action:** Manually run `npm view coalesce-transform-mcp` to confirm the name is available before publish. This is a manual check, not a code change.

---

## Files Modified

| File | Changes |
|------|---------|
| `src/types.ts` | Add `sanitizeResponse`, `validatePathSegment`, `handleToolError` |
| `src/client.ts` | Extend `post()` and `delete()` to accept `params?: QueryParams` |
| `src/tools/environments.ts` | Add try/catch + path validation |
| `src/tools/nodes.ts` | Add try/catch + path validation |
| `src/tools/runs.ts` | Add try/catch + path validation + sanitize responses |
| `src/tools/projects.ts` | Add try/catch + path validation |
| `src/tools/git-accounts.ts` | Add try/catch + path validation + use query params |
| `src/tools/users.ts` | Add try/catch + path validation |
| `src/workflows/run-and-wait.ts` | Add try/catch + sanitize responses + update `post()` call to 4-arg form |
| `src/workflows/retry-and-wait.ts` | Add try/catch + sanitize responses + update `post()` call to 4-arg form |
| `src/workflows/get-run-details.ts` | Add try/catch + sanitize response |
| `src/workflows/get-environment-overview.ts` | Add try/catch + path validation |
| `.gitignore` | Add `.env`, `.env.*`, `data/`, `*.pem` |
| `tests/build-rerun-body.test.ts` | New file — `buildRerunBody` unit tests |
| `tests/schemas.test.ts` | Add `sanitizeResponse`, `validatePathSegment`, `handleToolError` tests |
| `tests/tools/*.test.ts` | Update mock client `post`/`delete` signatures; add error-path integration test |
| `tests/workflows/*.test.ts` | Update mock client `post` signatures |

## Out of Scope

- Extract shared polling logic (refactoring, no safety impact)
- Version deduplication (cosmetic)
- `maxPages` on auto-pagination (user declined)
- Confirm `parallelism` default of 16 (documentation only)
