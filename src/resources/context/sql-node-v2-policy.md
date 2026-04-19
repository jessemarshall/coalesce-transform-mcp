# SQL Node V1 vs V2 Policy

**Scope:** this policy governs authoring on a local COA project (the files on disk) — `data.yml`, `nodeTypes/`, and the contents of `nodes/`. It does not change how the workspace API tools (`create_workspace_node_from_*`, `update_workspace_node`, etc.) behave.

> **V2 is ALPHA.** The V2 SQL node shape (`fileVersion: 2` node types + `.sql` nodes) ships in the `@next` COA channel and is not yet GA. It has known rough edges: silent column-reference false positives in `coa validate`, `UNION ALL` silently dropped from the body (use `insertStrategy` config instead), `CREATE TABLE ()` with zero columns on parse errors, and UI/CLI divergence on `isRequired` config fields. Before proceeding with V2, surface this to the user — "V2 is alpha, here's what that means for your project" — and get explicit confirmation. Never opt someone into alpha tooling silently.

## The Rule

**Default to V1. Never convert existing V1 nodes or node types to V2 unless the user explicitly asks for V2 AND has been told V2 is alpha.**

V2 is a newer project shape that uses `.sql` files in `nodes/` and requires a matching V2 node type with Jinja templates in `nodeTypes/`. It is not a drop-in upgrade — converting in either direction changes file extensions, file contents, and the node-type contract. Agents that "upgrade" silently have caused silent lineage loss and multi-hour debugging sessions in the field.

If the user says "build a basic pipeline," "add a stage," "load this CSV," or anything else that does not name V2, produce V1 output. Do not change `fileVersion` in `data.yml` or any node type `definition.yml`. Do not rewrite existing `.yml` nodes as `.sql` files.

## Detecting the Current Project Shape

Before writing or editing anything in a COA project, read:

1. **`data.yml`** — `fileVersion: 1` means a V1 project shape. `fileVersion: 3` is the current V2-capable shape (per the CLI's Getting Started guide).
2. **`nodeTypes/*/definition.yml`** — `fileVersion: 1` is a V1 node type. `fileVersion: 2` is a V2 node type (DDL/DML templates in sibling `create.sql.j2` / `run.sql.j2` files).
3. **`nodes/`** — `.yml` files are V1 nodes, `.sql` files are V2 nodes. Mixed is allowed; the node type's `fileVersion` decides which kind of `nodes/` file each one corresponds to.

Match the shape that already exists. If everything is `.yml` with V1 node types, stay V1.

## When the User Explicitly Asks For V2

Treat it as an explicit ask only when the user literally says V2, `.sql` nodes, `fileVersion: 2`, SQL transformation nodes, or similar. "Make this faster" or "use modern tooling" is not explicit.

**Before writing any file, tell the user V2 is alpha** (see the banner above) and confirm they still want to proceed. Once confirmed, walk them through the full setup below. Skipping any step produces nodes that either fail to compile, fail in the Coalesce UI, or silently drop lineage.

### Step 1 — confirm the project is V2-capable

`data.yml` must have `fileVersion: 3`. If it does not, stop and confirm with the user that they want this project migrated before touching it.

### Step 2 — create a V2 node type

V2 `.sql` nodes need a V2 node type to compile. Directory name pattern: `<DisplayName>-<UUID>/`. The UUID must match what Coalesce UI would generate (use `randomUUID()`; do not invent). Three files go inside:

**`definition.yml`** (excerpt — `fileVersion: 2` is mandatory):

```yaml
fileVersion: 2
id: <uuid>
isDisabled: false
metadata:
  defaultStorageLocation: null
  error: null
  nodeMetadataSpec: |-
    capitalized: Stage
    short: STG
    plural: Stages
    tagColor: '#2EB67D'
    config:
    - groupName: Options
      items:
      - type: materializationSelector
        default: table
        options: [table, view]
      - displayName: Insert Strategy
        attributeName: insertStrategy
        type: dropdownSelector
        default: INSERT
        options: [INSERT, MERGE]
        isRequired: false
      - displayName: Truncate Before
        attributeName: truncateBefore
        type: toggleButton
        default: true
name: <DisplayName>
type: NodeType
```

**`create.sql.j2`** — DDL template (CREATE TABLE / CREATE VIEW). See `coalesce://coa/describe/node-types` for the full template-variable reference. The TPC-H Stage example in the Getting Started guide is a reasonable starting point.

**`run.sql.j2`** — DML template (INSERT / MERGE / view refresh). Same reference applies.

Ask the user which materialization/insertStrategy defaults they want before writing the files — the templates hard-code behaviour.

### Step 3 — write the `.sql` transformation node

`.sql` files live in `nodes/` and follow the filename pattern `<LOCATION>-<NAME>.sql`. Required header annotations:

```sql
@id("<uuid>")                       -- one per node, generate with randomUUID()
@nodeType("<v2-node-type-uuid>")    -- must match a definition.yml id from nodeTypes/
```

Then the SELECT. Hard rules:

- **`ref()` uses single quotes only**: `{{ ref('SRC', 'CUSTOMER') }}`. Double-quoted `ref("SRC", "CUSTOMER")` parsed successfully in older COA but silently dropped the dependency — lineage shows a flat graph with no edges. Platform now accepts double quotes (commit 7b449dd800, CD-16981) but the convention stays single-quoted; do not switch.
- **Required config fields need annotations**: the CLI does not enforce `isRequired: true` fields from the node type's `definition.yml`, but the UI does. Add `@insertStrategy('INSERT')` (and any other required fields) to every `.sql` file. See `coalesce://coa/describe/sql-format` for the full annotation set.
- **No UNION ALL in plain SQL**: the V2 parser captures the first source block only — a second `SELECT ... UNION ALL ...` is silently dropped. Use `@insertStrategy('UNION ALL')` on the node (with the node type's `insertStrategy` options extended) instead of writing UNION ALL in the body.
- **CTAS/parse errors produce `CREATE TABLE ()` with zero columns**: if `coa create --dry-run --verbose` prints a table with no columns, the SELECT has a parse error. Dry-run does not warn. Read the generated DDL before executing.

### Step 4 — verify before executing

Always:

```bash
coa validate                                              # structural checks
coa create --include "{ <NODE_NAME> }" --dry-run --verbose
```

Check that the table name is not blank and that column types are not `UNKNOWN`. `UNKNOWN` types almost always mean a `ref()` target was misspelled or used double quotes.

**Known false positive:** `coa validate`'s Column References scanner flags aliased columns on V2 SQL nodes as "missing source columns." Non-blocking for `coa create` / `coa run`, may block `coa plan` in some environments. Tell the user when you see these rather than trying to "fix" them by rewriting SQL.

## Anti-Patterns

- Proceeding with V2 without first telling the user it's alpha.
- Changing `data.yml` `fileVersion` uninvited.
- Creating a V2 node type "to match" a project that currently has no V2 nodes.
- Rewriting a `.yml` node as `.sql` without being asked.
- Authoring a `.sql` node without first checking that a V2 node type exists for it to reference.
- Guessing UUIDs for `id` / `nodeType` annotations instead of generating them.
- "Fixing" V2 validate false positives by rewriting SELECTs.

## Authoritative References

- `coalesce://coa/describe/sql-format` — the full annotation list and examples.
- `coalesce://coa/describe/node-types` — node-type authoring, template variables, MERGE patterns.
- `coalesce://coa/describe/structure` — directory layout.
- Coalesce Getting Started with the COA CLI (internal Notion) — the canonical walkthrough this policy mirrors.
