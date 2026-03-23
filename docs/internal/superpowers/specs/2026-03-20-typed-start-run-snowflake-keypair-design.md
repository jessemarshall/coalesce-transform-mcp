# Typed `start-run` with Snowflake Key Pair Auth

## Summary

Replace the generic `z.record(z.unknown())` body on the `start-run` tool with a fully typed Zod schema matching the Coalesce `POST /scheduler/startRun` API. Add required Snowflake key pair authentication fields under `userCredentials`.

## Motivation

The current `start-run` tool accepts an untyped body, which means:
- AI assistants must guess field names and structure
- No input validation before hitting the API
- Snowflake credentials are not enforced

A typed schema makes the tool self-documenting, validates input, and ensures Snowflake key pair auth is always provided.

## API Reference

**Endpoint:** `POST /scheduler/startRun`

**Request body structure:**
```json
{
  "runDetails": { ... },
  "userCredentials": { ... },
  "parameters": { ... }
}
```

## Schema Design

### `runDetails` (required object)

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `environmentID` | string | yes | — | The environment being refreshed |
| `includeNodesSelector` | string | no | — | Nodes included for an ad-hoc job |
| `excludeNodesSelector` | string | no | — | Nodes excluded for an ad-hoc job |
| `jobID` | string | no | — | The ID of a job being run |
| `parallelism` | integer (`z.number().int()`) | no | — (API default: 16) | Max parallel nodes to run. Use `.optional()`, not `.default()`, so the API applies its own default. |
| `forceIgnoreWorkspaceStatus` | boolean | no | — (API default: false) | Allow refresh even if last deploy failed. Use `.optional()`, not `.default()`. |

### `userCredentials` (required object)

| Field | Type | Required | Description |
|---|---|---|---|
| `snowflakeUsername` | string | yes | Snowflake account username |
| `snowflakeKeyPairKey` | string | yes | PEM-encoded private key (must start with `-----BEGIN`). Use `\n` for line breaks in JSON. Treated as a sensitive credential. |
| `snowflakeKeyPairPass` | string | no | Password to decrypt an encrypted private key. Only required when the private key is encrypted. |
| `snowflakeWarehouse` | string | yes | Snowflake compute warehouse |
| `snowflakeRole` | string | yes | Snowflake user role |

`snowflakeAuthType` is **not** exposed as a tool parameter. It is automatically injected as `"KeyPair"` when constructing the request body.

### `parameters` (optional object)

Arbitrary string key-value pairs: `z.record(z.string()).optional()`

## Request Body Construction

The tool builds the API request body as:

```typescript
{
  runDetails: params.runDetails,
  userCredentials: {
    ...params.userCredentials,
    snowflakeAuthType: "KeyPair",
  },
  ...(params.parameters ? { parameters: params.parameters } : {}),
}
```

## Files Changed

### `src/tools/runs.ts`

- Replace the `start-run` tool's generic `body: z.record(z.unknown())` with the typed schema above
- Update the tool description string to reflect the new typed parameters
- Update `startRun()` function to accept the typed tool params (runDetails, userCredentials, parameters) and construct the API request body internally (including `snowflakeAuthType` injection)
- Construct the request body with auto-injected `snowflakeAuthType: "KeyPair"`

### `src/workflows/run-and-wait.ts`

- Replace the `run-and-wait` tool's generic `body: z.record(z.unknown())` with the same typed schema (reusing shared Zod objects)
- Update `runAndWait()` function to construct the body the same way before calling the API

### Shared Schema

Define reusable Zod schemas (`RunDetailsSchema`, `UserCredentialsSchema`, `StartRunParams`) in `src/types.ts` (where shared constructs like `PaginationParams` already live) and import them in both `runs.ts` and `run-and-wait.ts`.

## Files NOT Changed

- `retry-run`, `cancel-run` — keep generic schemas for now
- `src/workflows/retry-and-wait.ts` — keep generic schema for now

**Note:** The Postman collection confirms that `/scheduler/rerun` also accepts `userCredentials` with Snowflake fields. Typing `retry-run` and `retry-and-wait` with Snowflake auth is a follow-up task.

- `src/client.ts` — HTTP client unchanged
- `src/index.ts` — no changes needed
- All other tools and workflows — unchanged

## Testing

- Verify `start-run` rejects calls missing required fields (`environmentID`, `snowflakeUsername`, etc.)
- Verify `snowflakeAuthType: "KeyPair"` is injected automatically
- Verify `snowflakeKeyPairPass` is optional (works without it for unencrypted keys)
- Verify `parameters` is optional
- Verify `parallelism` accepts integers but rejects floats (`z.number().int()`)
- Verify `run-and-wait` uses the same typed schema and constructs the body with `snowflakeAuthType` injection
