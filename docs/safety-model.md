# Safety model

Three layers prevent destructive surprises:

## 1. Tool annotations

Every tool carries MCP annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`). Clients that respect them can filter proactively. The ⚠️ marker in the [Tool reference](../README.md#tool-reference) marks `destructiveHint: true` tools.

## 2. Read-only mode

`COALESCE_MCP_READ_ONLY=true` hides all write/mutation tools at server startup. Only read, list, search, cache, analyze, review, diagnose, and plan tools are registered. Use it for:

- Audits
- Agent sandboxes
- Pairing with a prod profile (see [Multiple environments](../README.md#multiple-environments))

## 3. Explicit confirmation for destructive ops

Tools marked destructive require `confirmed: true`. When the MCP client supports elicitation, the server prompts interactively; otherwise it returns a `STOP_AND_CONFIRM` response the agent must surface before retrying with `confirmed: true`.

Applies to: `delete_*`, `propagate_column_change`, `cancel_run`, `clear_data_cache`, `coa_create`, `coa_run`, `coa_deploy`, `coa_refresh`.

## COA preflight validation

Local COA write tools run preflight validation before shelling out. Errors block execution; warnings pass through in the tool response as `preflightWarnings` so agents can surface them.

| Code | Level | What it catches |
| ---- | ----- | --------------- |
| `SQL_DOUBLE_QUOTED_REF` | error | `.sql` nodes using `ref("…")` - silently returns `UNKNOWN` columns; must be single-quoted |
| `WORKSPACES_YML_MISSING` | error | `workspaces.yml` not in project root - required for local create/run |
| `SELECTOR_COMBINED_OR` | error | `{ A \|\| B }` selector form - matches zero nodes; must be `{ A } OR { B }` |
| `SQL_LITERAL_UNION_ALL` | warning | Literal `UNION ALL` in a V2 `.sql` node - silently dropped by the V2 parser; use `insertStrategy: UNION ALL` instead |
| `DATA_YML_UNEXPECTED_FILEVERSION` | warning | `data.yml` missing or not `fileVersion: 3` |
| `DATA_YML_NO_FILEVERSION` | warning | `data.yml` has no `fileVersion` field |
