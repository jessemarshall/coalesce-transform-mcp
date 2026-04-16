# Ecosystem Boundaries

## What This MCP Covers

The Coalesce Transform MCP manages **transform definitions and workspace configuration** within the Coalesce platform:

- Workspace nodes: create, edit, delete, configure, and inspect node definitions
- Pipeline building: plan, create, and review multi-node transform pipelines
- Runs: start, poll, diagnose, and retry deploy/refresh jobs
- Lineage: trace upstream/downstream dependencies at node and column level within a workspace
- Node types: discover, search, and inspect repo-backed and workspace node type definitions
- Configuration: intelligent auto-completion, schema resolution, storage mappings

It does **not** manage:

- Warehouse data (tables, schemas, row counts, sample data)
- Ingestion/extraction pipelines (connectors, syncs, sources)
- Data catalog or governance metadata
- Cross-platform lineage spanning ingestion -> transform -> consumption

## Adjacent Data Engineering MCPs

When a user's request falls outside this server's scope, direct them to the appropriate MCP:

### Snowflake MCP
- **Scope:** Warehouse queries, table DDL, row-level data inspection, Cortex AI features
- **When to use:** User asks about actual table data, wants to run SQL against the warehouse, needs schema introspection from the warehouse side, or wants to validate that a Coalesce-deployed node produced the expected output
- **Handoff pattern:** After deploying via `run_and_wait`, use Snowflake MCP to query the resulting table

### Fivetran MCP
- **Scope:** Ingestion pipeline management, connector configuration, sync status, destination setup
- **When to use:** User asks about source data freshness, connector health, or wants to trigger/check an ingestion sync
- **Handoff pattern:** Check Fivetran sync status before running a Coalesce refresh to ensure source data is current

### dbt MCP
- **Scope:** dbt model management, documentation generation, model lineage, SQL compilation
- **When to use:** User works in a dbt-based project alongside Coalesce, or asks about dbt models, tests, or documentation
- **Note:** Coalesce and dbt serve overlapping transform roles — users typically use one or the other, not both for the same tables

### Coalesce Catalog MCP *(planned — not yet available)*
- **Scope:** Data catalog search, governance metadata, end-to-end lineage visualization
- **When to use:** User needs lineage that spans beyond Coalesce (ingestion -> transform -> consumption), catalog search across the full data stack, or governance/compliance metadata
- **Handoff pattern:** Lineage from `get_upstream_nodes`/`get_downstream_nodes` covers Coalesce nodes only. When the Catalog MCP becomes available, combine with its results for full-stack lineage
- **Status:** This MCP is planned but not yet published. Cross-server workflow patterns referencing it describe future capabilities

## Cross-Server Workflow Patterns

### Pre-run validation
1. **Fivetran MCP** -> Check that source connectors completed their latest sync
2. **This MCP** -> `run_and_wait` to execute the Coalesce refresh job
3. **Snowflake MCP** -> Query the output tables to validate results

### Impact analysis across the stack
1. **This MCP** -> `analyze_impact` to identify downstream Coalesce nodes affected by a change
2. **Catalog MCP** *(when available)* -> Check if downstream consumers (dashboards, reports) are affected beyond Coalesce

### Debugging a data quality issue
1. **Snowflake MCP** -> Identify the problematic data in the warehouse
2. **This MCP** -> `get_upstream_nodes` and `get_workspace_node` to trace the transform logic
3. **Fivetran MCP** -> Check if the issue originated in the source data or ingestion

## COA CLI Surface (Bundled Alongside the Cloud MCP)

This MCP exposes **two** ways to reach Coalesce, and an agent should pick deliberately:

- **Cloud REST (default):** workspace/node tools (`get_workspace_node`, `set_workspace_node`, `run_and_wait`, etc.) hit the Coalesce cloud API. Use for any workspace mutation, scheduled/cloud-executed runs, or lineage on published environments.
- **COA CLI (`coa_*` tools):** shells out to the bundled `@coalescesoftware/coa` binary. See [src/services/coa/runner.ts](../../services/coa/runner.ts). **Not all `coa_*` tools are offline** — split them as follows:
  - **Offline-local** (project files + warehouse, no cloud API): `coa_doctor`, `coa_validate`, `coa_list_project_nodes`, `coa_dry_run_create`, `coa_dry_run_run`, `coa_describe_*`.
  - **Cloud-authenticated** (same `~/.coa/config` token as the REST tools, hits the scheduler API): `coa_list_environments`, `coa_list_runs`. These are functionally parallel to cloud REST tools — pick based on output shape, not auth cost.

### "Workspace" Is Overloaded

Two unrelated things share the word:

- **CLI workspace** — a key in the user's local `workspaces.yml` (e.g., `dev`, `prod`). Selects a connection profile + location mappings. Passed to `coa_*` tools as `workspace: "dev"`.
- **Cloud workspace** — a Coalesce cloud workspace resolved by `workspaceID` (a numeric/string identifier). Passed to REST tools like `get_workspace_node`, `list_workspace_nodes`.

Do not pass one where the other is expected. When a user says "workspace," disambiguate by surface before routing.

### Local Files and Cloud Workspace Do Not Sync

Editing a node via REST (`set_workspace_node`, `update_workspace_node`, etc.) changes cloud-workspace state; it does **not** touch the user's local COA project files. Conversely, editing local `.sql` / `.yml` files does not update the cloud workspace until the user explicitly syncs (commit/push or their project's equivalent). Two consequences for agents mixing surfaces on the same logical workspace:

- "What is the current state of node X?" has two answers — quote the surface you read from.
- Do not run `coa_validate` / `coa_dry_run_*` to verify a REST edit; those tools read local files, not cloud state. Use `get_workspace_node` / cloud-run tools for that.

### Shared Config File, Isolated Override Layers

Both surfaces read credentials from the **same file**: `~/.coa/config` (populated by the user by hand — run `coa describe config` for the INI schema). The cloud MCP's [coa-config.ts](../../services/config/coa-config.ts) and the COA CLI itself both resolve against that profile. What differs is the **override layer** each surface honors on top:

- **Cloud MCP** honors env overrides like `COALESCE_ACCESS_TOKEN`, `COALESCE_BASE_URL`, `SNOWFLAKE_*` (env wins over the file — see [prompts/index.ts](../../prompts/index.ts) `diagnose_setup` guidance).
- **COA CLI** honors its own override scheme (CLI flags, `COA_*`-prefixed vars); it does not check `COALESCE_*` overrides.
- **Cloud-mode COA** (K8s pods only) additionally consumes `COALESCE_COA_CLOUD_*` vars set by the scheduler. Those are never set on a developer laptop.

Our `runCoa()` runner **strips all `COALESCE_*` env vars** before spawning the COA child process — see [runner.ts](../../services/coa/runner.ts). This does **not** prevent credential sharing (both surfaces want the same `~/.coa/config` credentials). What it prevents:

- **MCP override leakage into COA.** If the user exported `COALESCE_ACCESS_TOKEN=xyz` to override their MCP token, that value must not silently bleed into COA's environment and shift COA's auth resolution. COA must resolve cleanly from `~/.coa/config`.
- **Accidental cloud-mode dispatch.** A stray `COALESCE_COA_CLOUD_JOB_ID` in the shell env would flip COA into cloud-mode (expecting Firebase Storage inputs that don't exist locally). Stripping guarantees CLI mode.

### Documentation Precedence (Skill Docs vs. `coa describe`)

Two agent-readable doc sources exist. When they intersect, use this rule:

- **`coalesce://coa/describe/*`** — authoritative for **everything inside the CLI**: selector syntax, YAML schema shapes, command flags, `~/.coa/config` file format, V2 `.sql` annotations, node-type authoring. Sourced live from the bundled COA binary, so it tracks upstream.
- **`coalesce://context/*` (these skill docs)** — authoritative for **everything outside the CLI**: cloud-REST tool usage, pipeline planning heuristics, node-type selection scoring, cross-surface decisions (this guide), and anything MCP-specific.

When a skill doc needs to reference a CLI concept, link to `coa describe <topic>` rather than copy its content. Copied content drifts; live content doesn't.

**Availability caveat:** if the bundled COA binary is missing or broken, `coalesce://coa/describe/*` returns a placeholder markdown that begins with `# COA describe: <topic> (temporarily unavailable)`. Treat that as *missing*, not authoritative — do not quote it, and surface the underlying error to the user (usually a fix via `coa_doctor` or reinstalling the MCP).

### OAuth Limitation (COA CLI Only)

Snowflake and Databricks **OAuth flows only work in cloud-mode COA** (K8s pods with secret-store access). The COA CLI path this MCP invokes runs in local/CLI mode, which supports only:

- Snowflake: `Basic` (username/password) or `KeyPair`
- Databricks: `Token`

If a user's `~/.coa/config` specifies an OAuth auth type, `coa_dry_run_create` / `coa_dry_run_run` will fail with "OAuth not supported." Direct them to switch that profile to basic/keypair/token, or run the equivalent operation through the cloud MCP tools (which go through the cloud API and do support OAuth-backed environments).

### Quick Decision Guide

- **Cloud REST tools** — edit workspace node config, start a cloud run, diagnose a failed deploy, or reach an environment where the user only has OAuth-backed warehouse credentials.
- **Offline `coa_*` tools** (`coa_doctor`, `coa_validate`, `coa_list_project_nodes`, `coa_dry_run_create`, `coa_dry_run_run`, `coa_describe_*`) — validate a local COA project directory, preview generated DDL/DML offline, read `coa describe` topics, or operate on a pure-local project with no cloud workspace.
- **Cloud-authenticated `coa_*` tools** (`coa_list_environments`, `coa_list_runs`) — same cloud token as the REST tools; pick them only when their output shape suits the question better than the REST equivalents.

## Lineage Scope

The lineage tools in this MCP (`get_upstream_nodes`, `get_downstream_nodes`, `get_column_lineage`, `analyze_impact`) operate **within a single Coalesce workspace**. They trace dependencies between Coalesce nodes only.

For lineage that crosses system boundaries (e.g., from a Fivetran source -> Coalesce Stage -> Coalesce Dimension -> a BI dashboard), a Catalog MCP would provide the broader view. When the Coalesce Catalog MCP becomes available, this MCP's lineage data can be a precise input into that broader graph, but it does not extend beyond Coalesce node boundaries on its own.
