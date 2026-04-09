# SQL Platform Selection

Read this resource before writing or editing SQL in a Coalesce node.

## Goal

Determine the platform first, then read exactly one dialect resource:

- `coalesce://context/sql-snowflake`
- `coalesce://context/sql-databricks`
- `coalesce://context/sql-bigquery`

Follow exactly one dialect's rules per edit. Mixing dialect conventions in one node creates compilation errors.

## Best Signal Order

### 1. Check Project Metadata First

Use `get_project` or `list_projects` and inspect the returned project metadata for the warehouse or platform type.

This is the best first signal because it reflects the project configuration directly.

### 2. Check Existing Node SQL

If you are editing an existing node, read the current node and inspect:

- identifier casing
- quoting style
- function names
- date/time function patterns
- join and alias style

Prefer preserving the existing node and workspace conventions rather than normalizing everything.

### 3. Check Neighboring Nodes

If one node is ambiguous, inspect nearby workspace nodes in the same layer or dependency chain.

Workspace-local conventions are a better guide than generic SQL style advice.

### 4. Ask the User If Still Unclear

If project metadata and existing SQL still do not settle the dialect, ask the user.

When the dialect choice materially affects correctness (e.g., function names, quoting, type syntax), ask the user rather than guessing.

## Coalesce-Specific Rule

Inside Coalesce node SQL, prefer `{{ ref(...) }}` for node and storage references. For full reference syntax details, see `coalesce://context/storage-mappings`.

Preserve `{{ ref(...) }}` syntax for node references. Only replace with raw warehouse-qualified table names if the user explicitly wants warehouse-native SQL outside normal Coalesce patterns.

## Editing Principles

This is the canonical source for "preserve workspace conventions" when editing SQL.

When modifying existing SQL:

- preserve the current dialect
- preserve the current quoting and casing style unless it is clearly broken
- avoid broad formatting rewrites that do not change behavior
- preserve existing workspace style over personal preferences

When generating entirely new SQL and no local convention exists:

- use the selected platform resource as the default style guide

## Special Note

Run-tool authentication in this MCP server is Snowflake Key Pair-based, but that does not mean every project uses Snowflake SQL semantics. Determine the SQL platform from project metadata and node SQL, not from run-tool auth requirements.
