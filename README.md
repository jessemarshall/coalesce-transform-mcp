# coalesce-transform-mcp

MCP server for [Coalesce](https://coalesce.io/). Connect AI assistants like Claude, Cursor, or Windsurf to Coalesce to manage nodes, pipelines, environments, jobs, and runs — **and**, starting with 0.5, drive the local-first `coa` CLI from the same server: validate a project, preview DDL/DML, plan a deployment, and apply it to a cloud environment. One install, two execution surfaces.

> **First time here?** Two ways to get credentials to the server: (a) if you already use the `coa` CLI locally, the server reads the same `~/.coa/config` file — one shared credential store, nothing to duplicate; (b) otherwise, set `COALESCE_ACCESS_TOKEN` in your MCP client config. Either path works. The `/coalesce-setup` prompt uses `diagnose_setup` to walk you through whatever you're missing.

## Contents

- [What you get](#what-you-get)
- [Quick Start](#quick-start)
  - [Trying a prerelease (alpha)](#trying-a-prerelease-alpha)
  - [Using the COA CLI tools](#using-the-coa-cli-tools)
- [Requirements](#requirements)
- [Environment Variables](#environment-variables)
- [Safety model](#safety-model)
- [Resources](#resources)
- [Tool Reference](#tool-reference)
  - [Cloud REST tools](#cloud-rest-tools)
  - [Intelligent tools](#intelligent-tools)
  - [COA CLI tools](#coa-cli-tools)
- [Snowflake exploration via Cortex Code](#snowflake-exploration-via-cortex-code)
- [Notes and conventions](#notes-and-conventions)
- [Links](#links)
- [License](#license)

## What you get

The server exposes two complementary ways of working with Coalesce:

**1. Cloud REST tools** (the original surface) — manage workspaces, environments, nodes, pipelines, jobs, runs, projects, git accounts, and users through the Coalesce Deploy API. All agent interaction flows through the authenticated `COALESCE_ACCESS_TOKEN`. Right for: building pipelines declaratively, editing node YAML, reviewing lineage, running deployed jobs, and auditing documentation.

**2. Local COA CLI tools** (new in 0.5) — wrap the bundled [`@coalescesoftware/coa`](https://www.npmjs.com/package/@coalescesoftware/coa) CLI to work against a local project directory and the warehouse directly. Right for: validating a project before checkin, previewing generated DDL/DML (`--dry-run`), iterating on V2 `.sql` node files, and running `plan → deploy → refresh` cycles. COA is bundled — **no separate install step**.

The two modes are orthogonal. Use both, one, or the other. Destructive tools require explicit confirmation in both modes.

## Quick Start

**1. Provide a Coalesce access token.** Pick one path:

- **Option A — reuse `~/.coa/config`** (best if you already use the `coa` CLI). Add a `token=` line under the profile you want to use. See [Reading from ~/.coa/config](#reading-from-coaconfig) for the schema the MCP consumes, and run `npx @coalescesoftware/coa describe config` for COA's own reference.
- **Option B — env var** (simplest for first-time MCP users). Generate a token from Deploy → User Settings in Coalesce, then set `COALESCE_ACCESS_TOKEN` in your MCP client config (see step 2). No file required.

When both are set, the env var wins — see [Environment Variables](#environment-variables) for precedence details.

**2. Add to your MCP client config:**

| Client | Config file |
| ------ | ----------- |
| Claude Code | `.mcp.json` in project root (or `~/.claude.json` for global) |
| Claude Desktop (macOS) | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Cursor | `.cursor/mcp.json` in project root |
| Windsurf | `~/.codeium/windsurf/mcp_config.json` |

**Claude Code** (`.mcp.json`) — with env-var auth (Option B):

```json
{
  "coalesce-transform": {
    "command": "npx",
    "args": ["coalesce-transform-mcp"],
    "env": {
      "COALESCE_ACCESS_TOKEN": "${COALESCE_ACCESS_TOKEN}"
    }
  }
}
```

With profile auth (Option A), drop the `env` block entirely — the server reads `~/.coa/config` automatically. To use a non-default profile, set `"env": { "COALESCE_PROFILE": "MEDBASE" }`.

**Claude Desktop, Cursor, Windsurf** — same thing, wrapped in `"mcpServers"`.

See [Environment Variables](#environment-variables) for every knob you might need and the env-vs-profile precedence rules.

> **Never hardcode credentials in config files tracked by git.** The `${VAR}` syntax pulls values from your shell environment.

### Trying a prerelease (alpha)

Pre-release builds are published to the `@alpha` npm dist-tag while `@latest` stays on stable. Point your MCP client at the alpha channel by appending `@alpha` to the npx arg:

```json
{
  "coalesce-transform": {
    "command": "npx",
    "args": ["coalesce-transform-mcp@alpha"]
  }
}
```

Restart your MCP client after changing the config so `npx` re-resolves.

To pin to an exact prerelease build rather than whatever `@alpha` resolves to today, replace `@alpha` with the full version, e.g. `coalesce-transform-mcp@0.5.0-alpha.2`. If `npx` is serving a stale cached copy when `@alpha` advances, force a fresh fetch with `npx -y coalesce-transform-mcp@alpha`.

To run alpha and stable side-by-side, register both with different server names (e.g. `coalesce-transform` for stable and `coalesce-transform-alpha` for the prerelease). To switch back to stable, drop the `@alpha` suffix and restart.

### Using the COA CLI tools

The COA CLI tools require no extra install — COA is bundled as a dependency. Usage notes:

- **Local commands** (`coa_doctor`, `coa_validate`, `coa_dry_run_create`, `coa_dry_run_run`, `coa_create`, `coa_run`, `coa_plan`) need a COA project directory (one that contains `data.yml`). Pass the path via the `projectPath` tool argument.
- **Cloud commands** (`coa_list_environments`, `coa_list_environment_nodes`, `coa_list_runs`, `coa_deploy`, `coa_refresh`) read credentials from `~/.coa/config` — the same file the MCP uses. Populate the file once (see [Reading from ~/.coa/config](#reading-from-coaconfig)) and both surfaces share it.
- **Profile resolution**: cloud tools accept an optional `profile` arg. When omitted, they fall back to `COALESCE_PROFILE` (env var), then to COA's own `[default]` — so you don't have to pass it on every call.
- **Warehouse-touching commands** (`coa_create`, `coa_run`) need a valid `workspaces.yml` in the project root with storage-location mappings. Preflight catches a missing file before execution.

## Requirements

- [Node.js](https://nodejs.org/) >= 22.0.0
- A [Coalesce](https://coalesce.io/) account with a workspace
- An MCP-compatible AI client
- A `COALESCE_ACCESS_TOKEN` env var OR a populated `~/.coa/config` with a `token=` field — the server reads both and prefers env when they disagree
- **For run tools (`start_run`/`run_and_wait`/`retry_*`, `coa_create`/`coa_run`) only:** Snowflake credentials (key-pair or PAT). Same source choice as above — env vars or the matching `snowflake*` keys in `~/.coa/config`

Install footprint is ~76 MB unpacked due to the bundled `@coalescesoftware/coa` CLI, which ships its own self-contained runtime. The MCP tarball itself stays under 1 MB.

## Environment Variables

The server merges values from `~/.coa/config` and the MCP env with **env-wins precedence** — a matching env var always overrides the profile value, so you can pin a single field per session without editing the config file.

<!-- ENV_METADATA_CORE_TABLE_START -->
| Variable | Description | Default |
| -------- | -------- | -------- |
| `COALESCE_ACCESS_TOKEN` | Bearer token from the Coalesce Deploy tab. Optional when `~/.coa/config` provides a `token`. | — |
| `COALESCE_PROFILE` | Selects which `~/.coa/config` profile to load. | `default` |
| `COALESCE_BASE_URL` | Region-specific base URL. | `https://app.coalescesoftware.io (US)` |
| `COALESCE_ORG_ID` | Fallback org ID for cancel-run. | — |
| `COALESCE_REPO_PATH` | Local repo root for repo-backed tools and pipeline planning. | — |
| `COALESCE_CACHE_DIR` | Base directory for the local data cache. When set, cache files are written here instead of the working directory. | — |
| `COALESCE_MCP_AUTO_CACHE_MAX_BYTES` | JSON size threshold before auto-caching to disk. | `32768` |
| `COALESCE_MCP_LINEAGE_TTL_MS` | In-memory lineage cache TTL in milliseconds. | `1800000` |
| `COALESCE_MCP_MAX_REQUEST_BODY_BYTES` | Max outbound API request body size. | `524288` |
| `COALESCE_MCP_READ_ONLY` | When `true`, hides all write/mutation tools during registration. Only read, list, search, cache, analyze, review, diagnose, and plan tools are exposed. | `false` |
| `COALESCE_MCP_SKILLS_DIR` | Directory for customizable AI skill resources. When set, reads context resources from this directory and seeds defaults on first run. Users can augment or override any skill. | — |
<!-- ENV_METADATA_CORE_TABLE_END -->

### Reading from ~/.coa/config

COA stores credentials in a standard INI file at `~/.coa/config`. You create it by hand (or let `coa` write it as you use the CLI over time). For the authoritative reference, run `npx @coalescesoftware/coa describe config`.

The MCP reads the profile selected by `COALESCE_PROFILE` (defaulting to `[default]`) and maps the keys it cares about onto the env vars above: `token` ↔ `COALESCE_ACCESS_TOKEN`, `domain` ↔ `COALESCE_BASE_URL`, and each `snowflake*` key ↔ its corresponding `SNOWFLAKE_*` env var. Env vars override individual fields.

```ini
[default]
token=<your-coalesce-refresh-token>
domain=https://your-org.app.coalescesoftware.io
snowflakeUsername=YOUR_USER
snowflakeRole=YOUR_ROLE
snowflakeWarehouse=YOUR_WAREHOUSE
snowflakeKeyPairKey=/Users/you/.coa/rsa_key.p8
snowflakeAuthType=KeyPair

[staging]
# …additional profiles; select with COALESCE_PROFILE
```

Only the fields the MCP needs are shown above — COA's config supports many more (see `coa describe config`). Unknown keys are ignored by this server.

### Snowflake overrides (for run tools only)

Run tools (`start_run`, `retry_run`, `run_and_wait`, `retry_and_wait`) and the warehouse-touching COA tools (`coa_create`, `coa_run`) need Snowflake credentials. These normally come from `~/.coa/config`. Override any field via env var if needed:

<!-- ENV_METADATA_SNOWFLAKE_TABLE_START -->
| Variable | Required | Description |
| -------- | -------- | -------- |
| `SNOWFLAKE_USERNAME` | Yes | Snowflake account username |
| `SNOWFLAKE_KEY_PAIR_KEY` | No | Path to PEM-encoded private key (required if SNOWFLAKE_PAT not set) |
| `SNOWFLAKE_PAT` | No | Snowflake Programmatic Access Token (alternative to key pair) |
| `SNOWFLAKE_KEY_PAIR_PASS` | No | Passphrase for encrypted keys |
| `SNOWFLAKE_WAREHOUSE` | Yes | Snowflake compute warehouse |
| `SNOWFLAKE_ROLE` | Yes | Snowflake user role |
<!-- ENV_METADATA_SNOWFLAKE_TABLE_END -->

"Required" means one of env OR the matching `~/.coa/config` field must supply the value. **`SNOWFLAKE_PAT` is env-only** — COA's config uses `snowflakePassword` for Basic auth (a different concept), which this server deliberately doesn't read.

### Overriding individual fields

The most common reason to set env vars is to flip a single field for a session without editing the config file:

```json
{
  "coalesce-transform": {
    "command": "npx",
    "args": ["coalesce-transform-mcp"],
    "env": {
      "COALESCE_PROFILE": "staging",
      "SNOWFLAKE_ROLE": "TRANSFORMER_ADMIN"
    }
  }
}
```

That says "use the `[staging]` profile, but override its `snowflakeRole`."

## Safety model

The server uses three layers to prevent an agent from doing something destructive by surprise:

1. **Tool annotations.** Every tool carries MCP annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`). MCP clients that respect these can filter tools proactively. The ⚠️ marker in [Tool Reference](#tool-reference) marks `destructiveHint: true` tools.
2. **`COALESCE_MCP_READ_ONLY=true`** hides all write-style tools at server startup. Only read/list/search/analyze/review/diagnose/plan tools are registered. Useful for auditing or agent sandboxes.
3. **Explicit confirmation for destructive ops.** Tools marked destructive require a `confirmed: true` argument. When the MCP client supports elicitation, the server prompts the user interactively; otherwise it returns a `STOP_AND_CONFIRM` response the agent must surface before retrying with `confirmed: true`. This applies to: `delete_*`, `propagate_column_change`, `cancel_run`, `clear_data_cache`, `coa_create`, `coa_run`, `coa_deploy`, `coa_refresh`.

Additionally, the local COA write tools run **preflight validation** before shelling out. Preflight scans the project and blocks on known footguns:

| Code | Level | What it catches |
| ---- | ----- | --------------- |
| `SQL_DOUBLE_QUOTED_REF` | error | `.sql` nodes using `ref("…")` — silently returns `UNKNOWN` columns; must be single-quoted |
| `WORKSPACES_YML_MISSING` | error | `workspaces.yml` not in project root — required for local create/run |
| `SELECTOR_COMBINED_OR` | error | `{ A \|\| B }` selector form — matches zero nodes; must be `{ A } \|\| { B }` |
| `SQL_LITERAL_UNION_ALL` | warning | Literal `UNION ALL` in a V2 `.sql` node — silently dropped by the V2 parser; use `insertStrategy: UNION ALL` instead |
| `DATA_YML_UNEXPECTED_FILEVERSION` | warning | `data.yml` missing or not `fileVersion: 3` |
| `DATA_YML_NO_FILEVERSION` | warning | `data.yml` has no `fileVersion` field |

Errors block execution; warnings pass through in the tool response as `preflightWarnings` so agents can surface them.

## Resources

Resources are read-only context documents exposed via MCP that clients can pull into their prompts on demand. There are two families.

### Coalesce context skills (customizable)

24 curated markdown resources under `coalesce://context/*` that guide how agents interact with the server — SQL conventions per warehouse, node-type selection, pipeline workflows, lineage/impact guidance, and so on. Set `COALESCE_MCP_SKILLS_DIR` to make them editable on disk:

```bash
export COALESCE_MCP_SKILLS_DIR="/path/to/my-skills"
```

On first run the server seeds the directory with 48 files:

- `coalesce_skills.<name>.md` — the default skill content (editable)
- `user_skills.<name>.md` — your customization file (starts as an inactive stub with instructions)

Each resource resolves using this priority:

1. **Override** — `user_skills.<name>.md` starts with `<!-- OVERRIDE -->` → only the user file is served
2. **Augment** — `user_skills.<name>.md` has custom content (remove the `<!-- STUB -->` line first) → default + user content are concatenated
3. **Default** — `user_skills.<name>.md` is missing, empty, or still has the seeded stub → default skill content is served
4. **Disabled** — both files deleted → empty content is served

Seeding is idempotent — it never overwrites files you've already modified.

#### Available context skills

| Skill | File | Description |
| ----- | ---- | ----------- |
| Coalesce Overview | `overview` | General Coalesce concepts, response guidelines, and operational constraints |
| SQL Platform Selection | `sql-platform-selection` | Determining the active SQL platform from project metadata |
| SQL Rules: Snowflake | `sql-snowflake` | Snowflake-specific SQL conventions for node SQL |
| SQL Rules: Databricks | `sql-databricks` | Databricks-specific SQL conventions for node SQL |
| SQL Rules: BigQuery | `sql-bigquery` | BigQuery-specific SQL conventions for node SQL |
| Data Engineering Principles | `data-engineering-principles` | Node type selection, layered architecture, methodology detection, and materialization strategies |
| Storage Locations and References | `storage-mappings` | Storage location concepts, `{{ ref() }}` syntax, and reference patterns |
| Tool Usage Patterns | `tool-usage` | Best practices for tool batching, parallelization, and SQL conversion |
| ID Discovery | `id-discovery` | Resolving project, workspace, environment, job, run, node, and org IDs |
| Node Creation Decision Tree | `node-creation-decision-tree` | Choosing between predecessor-based creation, updates, and full replacements |
| Node Payloads | `node-payloads` | Working with workspace node bodies, metadata, config, and array-replacement risks |
| Hydrated Metadata | `hydrated-metadata` | Coalesce hydrated metadata structures for advanced node payload editing |
| Run Operations | `run-operations` | Starting, retrying, polling, diagnosing, and canceling Coalesce runs |
| Node Type Corpus | `node-type-corpus` | Node type discovery, corpus search, and metadata patterns |
| Aggregation Patterns | `aggregation-patterns` | JOIN ON generation, GROUP BY detection, and join-to-aggregation conversion |
| Intelligent Node Configuration | `intelligent-node-configuration` | How intelligent config completion works, schema resolution, and automatic field detection |
| Pipeline Workflows | `pipeline-workflows` | Building pipelines end-to-end: node type selection, multi-node sequences, and execution |
| Node Operations | `node-operations` | Editing existing nodes: joins, columns, config fields, and SQL-to-graph conversion |
| Node Type Selection Guide | `node-type-selection-guide` | When to use each Coalesce node type (Stage/Work vs Dimension/Fact vs specialized) |
| Intent Pipeline Guide | `intent-pipeline-guide` | Using `build_pipeline_from_intent` to create pipelines from natural language |
| Run Diagnostics Guide | `run-diagnostics-guide` | Using `diagnose_run_failure` to analyze failed runs and determine fixes |
| Pipeline Review Guide | `pipeline-review-guide` | Using `review_pipeline` for pipeline analysis and optimization |
| Pipeline Workshop Guide | `pipeline-workshop-guide` | Using pipeline workshop tools for iterative, conversational pipeline building |
| Ecosystem Boundaries | `ecosystem-boundaries` | Scope of this MCP vs adjacent data engineering MCPs (Snowflake, Fivetran, dbt, Catalog) |

### COA describe topics (sourced from the CLI)

10 resources under `coalesce://coa/describe/*` that surface the bundled COA CLI's self-describing documentation. Content is fetched from `coa describe <topic>` on first access and cached to disk, keyed by the pinned COA version — so agents always see docs that match the CLI they're driving. Topics: `overview`, `commands`, `selectors`, `schemas`, `workflow`, `structure`, `concepts`, `sql-format`, `node-types`, `config`.

For parameterized topics (`command <name>`, `schema <type>`), use the `coa_describe` tool with a `subtopic` argument.

## Tool Reference

⚠️ = Destructive (requires `confirmed: true`). 🧰 = Runs bundled `coa` CLI.

### Cloud REST tools

Manage Coalesce platform resources through the Deploy API.

#### Environments

- `list_environments` — List all available environments
- `get_environment` — Get details of a specific environment
- `create_environment` — Create a new environment within a project
- `delete_environment` — Delete an environment ⚠️

#### Workspaces

- `list_workspaces` — List all workspaces
- `get_workspace` — Get details of a specific workspace

#### Nodes

- `list_environment_nodes` — List nodes in an environment
- `list_workspace_nodes` — List nodes in a workspace
- `get_environment_node` — Get a specific environment node
- `get_workspace_node` — Get a specific workspace node
- `set_workspace_node` — Replace a workspace node with a full body
- `update_workspace_node` — Safely update selected fields of a workspace node
- `delete_workspace_node` — Delete a node from a workspace ⚠️

#### Jobs

- `list_environment_jobs` — List all jobs for an environment
- `create_workspace_job` — Create a job in a workspace with node include/exclude selectors
- `get_environment_job` — Get details of a specific job (via environment)
- `update_workspace_job` — Update a job's name and node selectors
- `delete_workspace_job` — Delete a job ⚠️

#### Subgraphs

- `list_workspace_subgraphs` — List subgraphs in a workspace
- `get_workspace_subgraph` — Get details of a specific subgraph
- `create_workspace_subgraph` — Create a subgraph to group nodes visually
- `update_workspace_subgraph` — Update a subgraph's name and node membership
- `delete_workspace_subgraph` — Delete a subgraph (nodes are NOT deleted) ⚠️

#### Runs

- `diagnose_run_failure` — Diagnose a failed run with error classification, root-cause analysis, and actionable fix suggestions
- `list_runs` — List runs with optional filters
- `get_run` — Get details of a specific run
- `get_run_results` — Get results of a completed run
- `start_run` — Start a new run; requires Snowflake auth (Key Pair or PAT, credentials from env vars)
- `run_status` — Check status of a running job
- `retry_run` — Retry a failed run; requires Snowflake auth (Key Pair or PAT, credentials from env vars)
- `cancel_run` — Cancel a running job (requires `runID` and `environmentID`; `orgID` may come from `COALESCE_ORG_ID`) ⚠️

#### Projects

- `list_projects` — List all projects
- `get_project` — Get project details
- `create_project` — Create a new project
- `update_project` — Update a project
- `delete_project` — Delete a project ⚠️

#### Git Accounts

- `list_git_accounts` — List all git accounts
- `get_git_account` — Get git account details
- `create_git_account` — Create a new git account
- `update_git_account` — Update a git account
- `delete_git_account` — Delete a git account ⚠️

#### Users and roles

- `list_org_users` — List all organization users
- `get_user_roles` — Get roles for a specific user
- `list_user_roles` — List all user roles
- `set_org_role` — Set organization role for a user
- `set_project_role` — Set project role for a user
- `delete_project_role` — Remove project role from a user ⚠️
- `set_env_role` — Set environment role for a user
- `delete_env_role` — Remove environment role from a user ⚠️

### Intelligent tools

Custom logic on top of the Coalesce API: pipeline planning, config completion, join analysis, lineage, and more.

#### Node creation and configuration

- `create_workspace_node_from_scratch` — Create a workspace node with no predecessors, apply fields to the requested completion level, and run automatic config completion
- `create_workspace_node_from_predecessor` — Create a node from predecessor nodes, verify column coverage, suggest join columns, and run automatic config completion
- `replace_workspace_node_columns` — Replace `metadata.columns` wholesale and optionally apply additional changes for complex column rewrites
- `convert_join_to_aggregation` — Convert a join-style node into an aggregated fact-style node with generated JOIN/GROUP BY analysis
- `apply_join_condition` — Auto-generate and write a FROM/JOIN/ON clause for a multi-predecessor node
- `create_node_from_external_schema` — Create a workspace node whose columns match an existing warehouse table or external schema
- `complete_node_configuration` — Intelligently complete a node's configuration by analyzing context and applying best-practice rules
- `list_workspace_node_types` — List distinct node types observed in current workspace nodes
- `analyze_workspace_patterns` — Analyze workspace nodes to detect package adoption, pipeline layers, methodology, and generate recommendations

#### Pipeline planning and execution

- `plan_pipeline` — Plan a pipeline from SQL or a natural-language goal without mutating the workspace; ranks best-fit node types from the local repo
- `create_pipeline_from_plan` — Execute an approved pipeline plan using predecessor-based creation
- `create_pipeline_from_sql` — Plan and create a pipeline directly from SQL
- `build_pipeline_from_intent` — Build a pipeline from a natural language goal with automatic entity resolution and node type selection
- `review_pipeline` — Analyze an existing pipeline for redundant nodes, missing joins, layer violations, naming issues, and optimization opportunities
- `parse_sql_structure` — Parse a SQL statement into structural components (CTEs, source tables, projected columns) without touching the workspace
- `select_pipeline_node_type` — Rank and select the best Coalesce node type for a pipeline step using the deliberative selection loop against repo or workspace-observed types

#### Pipeline workshop

- `pipeline_workshop_open` — Open an iterative pipeline builder session with workspace context pre-loaded
- `pipeline_workshop_instruct` — Send a natural language instruction to modify the current workshop plan
- `get_pipeline_workshop_status` — Get the current state of a workshop session
- `pipeline_workshop_close` — Close a workshop session and release resources

#### Repo-backed node types and templates

- `list_repo_packages` — Inspect a committed local Coalesce repo and list package aliases plus enabled node-type coverage
- `list_repo_node_types` — List exact resolvable committed node-type identifiers from `nodeTypes/`
- `get_repo_node_type_definition` — Resolve one exact committed node type and return its outer definition plus parsed `nodeMetadataSpec`
- `generate_set_workspace_node_template` — Generate a YAML-friendly `set_workspace_node` body template from a definition object or committed repo definition
- `search_node_type_variants` — Search the committed node-type corpus by normalized family, package, primitive, or support status
- `get_node_type_variant` — Load one exact node-type corpus variant by variant key
- `generate_set_workspace_node_template_from_variant` — Generate a `set_workspace_node` body template from a committed corpus variant

#### Lineage and impact

- `get_upstream_nodes` — Walk the full upstream dependency graph for a node
- `get_downstream_nodes` — Walk the full downstream dependency graph for a node
- `get_column_lineage` — Trace a column through the pipeline upstream and downstream via column-level references
- `analyze_impact` — Analyze downstream impact of changing a node or specific column — returns impacted counts, grouped by depth, and critical path
- `propagate_column_change` — Update all downstream columns after a column rename or data type change ⚠️
- `search_workspace_content` — Search across node SQL, column names, descriptions, and config values using the lineage cache as a searchable index
- `audit_documentation_coverage` — Scan all workspace nodes and columns for missing descriptions and report coverage statistics

#### Cache and snapshots

- `cache_workspace_nodes` — Fetch every page of workspace nodes, write a full snapshot, and return cache metadata
- `cache_environment_nodes` — Fetch every page of environment nodes, write a full snapshot, and return cache metadata
- `cache_runs` — Fetch every page of run results, write a full snapshot, and return cache metadata
- `cache_org_users` — Fetch every page of organization users, write a full snapshot, and return cache metadata
- `clear_data_cache` — Delete all cached snapshots, auto-cached responses, and plan summaries ⚠️

#### Run workflows

- `run_and_wait` — Start a run and poll until completion; requires Snowflake auth (Key Pair or PAT)
- `retry_and_wait` — Retry a failed run and poll until completion; requires Snowflake auth (Key Pair or PAT)
- `get_run_details` — Get run metadata and results in one call
- `get_environment_overview` — Get environment details with full node list
- `get_environment_health` — Comprehensive health dashboard: node counts by type, run statuses, failed runs in last 24h, stale nodes, dependency health, and overall health score (walks all paginated environment runs before scoring — slower on busy environments)

#### Skills

- `personalize_skills` — Export bundled skill files to a local directory for customization; creates editable `coalesce_skills.{name}.md` and `user_skills.{name}.md` pairs (idempotent — never overwrites existing files)

#### Setup

- `diagnose_setup` — Stateless probe reporting which first-time-setup pieces are configured: access token, Snowflake credentials, `~/.coa/config` profile, local repo path, and a best-effort `coa doctor` check. Returns a structured report plus ordered `nextSteps` and per-field `source` markers (`env`, `profile:<name>`, or `missing`). Paired with the `/coalesce-setup` MCP prompt, which walks a user through any remaining gaps.

### COA CLI tools

Wrap the bundled `@coalescesoftware/coa` CLI. All local tools accept a `projectPath` argument and validate that it contains `data.yml` before shelling out. Destructive tools run preflight validation; see [Safety model](#safety-model).

#### Read-only, local

- 🧰 `coa_doctor` — Check config, credentials, and warehouse connectivity for a project. Wraps `coa doctor --json`
- 🧰 `coa_validate` — Validate YAML schemas and scan a project for configuration problems. Wraps `coa validate --json`
- 🧰 `coa_list_project_nodes` — List all nodes defined in a local project (pre-deploy). Wraps `coa create --list-nodes`
- 🧰 `coa_dry_run_create` — Preview DDL without executing against the warehouse. Forces `--dry-run --verbose`
- 🧰 `coa_dry_run_run` — Preview DML without executing against the warehouse. Forces `--dry-run --verbose`

#### Read-only, cloud (require `~/.coa/config`)

- 🧰 `coa_list_environments` — List deployment environments. Wraps `coa environments list --format json`
- 🧰 `coa_list_environment_nodes` — List deployed nodes in an environment. Wraps `coa nodes list --environmentID ...`
- 🧰 `coa_list_runs` — List pipeline runs in a cloud environment (or across all environments). Wraps `coa runs list`

#### Describe

- 🧰 `coa_describe` — Fetch a section of COA's self-describing documentation by topic + optional subtopic. Also exposed as `coalesce://coa/describe/*` [resources](#coa-describe-topics-sourced-from-the-cli)

#### Write and deploy

- 🧰 `coa_plan` — Generate a deployment plan JSON by diffing the local project against a cloud environment. Writes `coa-plan.json` (configurable via `out`). Non-destructive
- 🧰 `coa_create` — Run DDL (CREATE/REPLACE) against the warehouse for selected nodes. Preflight-gated. ⚠️
- 🧰 `coa_run` — Run DML (INSERT/MERGE) to populate selected nodes. Preflight-gated. ⚠️
- 🧰 `coa_deploy` — Apply a plan JSON to a cloud environment. Verifies the plan file exists before running. ⚠️
- 🧰 `coa_refresh` — Run DML for selected nodes in an already-deployed environment (no local project required). ⚠️

## Snowflake exploration via Cortex Code

This server manages node definitions, not live warehouse data. For Snowflake data questions (tables, schemas, row counts, sample data, permissions), add [Cortex Code](https://ai.snowflake.com) as a companion MCP server. The agent will automatically route Snowflake questions to cortex tools.

**Setup:**

1. Install Cortex Code and configure a Snowflake connection:

   ```bash
   curl -LsS https://ai.snowflake.com/static/cc-scripts/install.sh | sh
   cortex connections  # interactive connection setup
   ```

2. Add cortex as an MCP server in your `.mcp.json`:

   ```json
   {
     "cortex": {
       "command": "cortex",
       "args": ["--mcp-server"]
     }
   }
   ```

The agent will see both servers' tools and route Snowflake data questions to cortex and node/pipeline questions to Coalesce tools.

## Notes and conventions

- **Credential resolution order.** For every Coalesce/Snowflake credential field: env var wins when set, otherwise the `~/.coa/config` profile selected by `COALESCE_PROFILE` (default `[default]`) fills in. Call `diagnose_setup` to see which source supplied each value — the `source` tags in its output read `env`, `profile:default`, etc.
- **Profile file is optional.** If `~/.coa/config` doesn't exist, the server falls back to env-only mode. Startup never fails because of a missing or malformed profile file — it just logs a stderr warning.
- **SQL override is disallowed.** Nodes are built via YAML/config (columns, transforms, join conditions), not raw SQL. Template generation strips `overrideSQLToggle`, and write helpers reject `overrideSQL` fields.
- **Caching:** Large responses are auto-cached to disk. Use `cache_workspace_nodes` and similar tools when you want a reusable snapshot. Configure the threshold with `COALESCE_MCP_AUTO_CACHE_MAX_BYTES`.
- **Repo-backed tools:** Set `COALESCE_REPO_PATH` to your local Coalesce repo root (containing `nodeTypes/`, `nodes/`, `packages/`) or pass `repoPath` on individual tool calls. The server does not clone repos or install packages.
- **COA CLI versioning:** The bundled COA CLI is pinned to an exact alpha version. It is *not* a floating `@next` tag — every release of this MCP ships with a known-good COA build. Changelog scanning and bump policy live in [docs/RELEASES.md](docs/RELEASES.md).
- **COA describe cache:** COA describe output is cached under `~/.cache/coalesce-transform-mcp/coa-describe/<coa-version>/` after first access. Cache is version-keyed — upgrading the MCP automatically invalidates stale content.

## Links

- [Coalesce Docs](https://docs.coalesce.io/docs)
- [Coalesce API Docs](https://docs.coalesce.io/docs/api/authentication)
- [Coalesce CLI (`coa`)](https://docs.coalesce.io/docs/cli)
- [Coalesce Marketplace Docs](https://docs.coalesce.io/docs/marketplace)
- [Model Context Protocol](https://modelcontextprotocol.io/)

## License

MIT
