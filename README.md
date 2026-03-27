# coalesce-transform-mcp

MCP server for the [Coalesce](https://coalesce.io/) Transform API. Connect AI assistants like Claude, Cursor, or Windsurf to your Coalesce workspace to manage nodes, pipelines, environments, jobs, runs, and more.

## Quick Start

**1. Set your access token** in `~/.zshrc` or `~/.bashrc`:

```bash
export COALESCE_ACCESS_TOKEN="your-token-here"
```

Generate a token from the Deploy tab in your Coalesce workspace ([docs](https://docs.coalesce.io/docs/api/authentication)).

**2. Add to your MCP client config:**

| Client | Config file |
| ------ | ----------- |
| Claude Code | `.mcp.json` in project root (or `~/.claude.json` for global) |
| Claude Desktop (macOS) | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Cursor | `.cursor/mcp.json` in project root |
| Windsurf | `~/.codeium/windsurf/mcp_config.json` |

**Claude Code** (`.mcp.json`):

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

**Claude Desktop, Cursor, Windsurf** — same thing, wrapped in `"mcpServers"`:

```json
{
  "mcpServers": {
    "coalesce-transform": {
      "command": "npx",
      "args": ["coalesce-transform-mcp"],
      "env": {
        "COALESCE_ACCESS_TOKEN": "${COALESCE_ACCESS_TOKEN}"
      }
    }
  }
}
```

The server defaults to the US region. See [Environment Variables](#environment-variables) if you need to change the region, enable run tools, or configure repo-backed features.

> **Never hardcode credentials in config files tracked by git.** The `${VAR}` syntax pulls values from your shell environment.

## Requirements

- [Node.js](https://nodejs.org/) >= 22.0.0
- A [Coalesce](https://coalesce.io/) account with a workspace and access token
- An MCP-compatible AI client
- **For run tools only:** Snowflake key pair authentication (see below)

## Environment Variables

Only `COALESCE_ACCESS_TOKEN` is required. Everything else is optional.

<!-- ENV_METADATA_CORE_TABLE_START -->
| Variable | Description | Default |
| -------- | -------- | -------- |
| `COALESCE_ACCESS_TOKEN` | **Required.** Bearer token from the Coalesce Deploy tab. | — |
| `COALESCE_BASE_URL` | Region-specific base URL. | `https://app.coalescesoftware.io (US)` |
| `COALESCE_ORG_ID` | Fallback org ID for cancel-run. | — |
| `COALESCE_REPO_PATH` | Local repo root for repo-backed tools and pipeline planning. | — |
| `COALESCE_MCP_AUTO_CACHE_MAX_BYTES` | JSON size threshold before auto-caching to disk. | `32768` |
| `COALESCE_MCP_MAX_REQUEST_BODY_BYTES` | Max outbound API request body size. | `524288` |
<!-- ENV_METADATA_CORE_TABLE_END -->

### Snowflake (for run tools only)

Required for `coalesce_start_run`, `coalesce_retry_run`, `coalesce_run_and_wait`, and `coalesce_retry_and_wait`. The server starts without them — they're validated when you first use a run tool.

<!-- ENV_METADATA_SNOWFLAKE_TABLE_START -->
| Variable | Required | Description |
| -------- | -------- | -------- |
| `SNOWFLAKE_USERNAME` | Yes | Snowflake account username |
| `SNOWFLAKE_KEY_PAIR_KEY` | Yes | Path to PEM-encoded private key |
| `SNOWFLAKE_KEY_PAIR_PASS` | No | Passphrase for encrypted keys |
| `SNOWFLAKE_WAREHOUSE` | Yes | Snowflake compute warehouse |
| `SNOWFLAKE_ROLE` | Yes | Snowflake user role |
<!-- ENV_METADATA_SNOWFLAKE_TABLE_END -->

To use optional variables, add them to your shell profile and pass them through in your MCP config. Here's a full example with everything enabled:

**`~/.zshrc`:**

```bash
export COALESCE_ACCESS_TOKEN="your-token-here"
export COALESCE_BASE_URL="https://app.eu.coalescesoftware.io"
export COALESCE_REPO_PATH="/path/to/local/coalesce-repo"
export SNOWFLAKE_USERNAME="your-username"
export SNOWFLAKE_KEY_PAIR_KEY="/path/to/snowflake_key.pem"
export SNOWFLAKE_KEY_PAIR_PASS="your-passphrase"
export SNOWFLAKE_WAREHOUSE="your-warehouse"
export SNOWFLAKE_ROLE="your-role"
```

**`.mcp.json`:**

```json
{
  "coalesce-transform": {
    "command": "npx",
    "args": ["coalesce-transform-mcp"],
    "env": {
      "COALESCE_ACCESS_TOKEN": "${COALESCE_ACCESS_TOKEN}",
      "COALESCE_BASE_URL": "${COALESCE_BASE_URL}",
      "COALESCE_REPO_PATH": "${COALESCE_REPO_PATH}",
      "SNOWFLAKE_USERNAME": "${SNOWFLAKE_USERNAME}",
      "SNOWFLAKE_KEY_PAIR_KEY": "${SNOWFLAKE_KEY_PAIR_KEY}",
      "SNOWFLAKE_KEY_PAIR_PASS": "${SNOWFLAKE_KEY_PAIR_PASS}",
      "SNOWFLAKE_WAREHOUSE": "${SNOWFLAKE_WAREHOUSE}",
      "SNOWFLAKE_ROLE": "${SNOWFLAKE_ROLE}"
    }
  }
}
```

Only include the variables you need — the Quick Start config with just `COALESCE_ACCESS_TOKEN` is enough to get started.

## Tool Reference

⚠️ = Destructive operation

### API Tools

Coalesce Platform Tools: manage workspaces, environments, projects, runs, and other platform resources.

#### Environments

- `coalesce_list_environments` - List all available environments
- `coalesce_get_environment` - Get details of a specific environment
- `coalesce_create_environment` - Create a new environment within a project
- `coalesce_update_environment` - Update an existing environment
- `coalesce_delete_environment` - Delete an environment ⚠️

#### Workspaces

- `coalesce_list_workspaces` - List all workspaces
- `coalesce_get_workspace` - Get details of a specific workspace

#### Nodes

- `coalesce_list_environment_nodes` - List nodes in an environment
- `coalesce_list_workspace_nodes` - List nodes in a workspace
- `coalesce_get_environment_node` - Get a specific environment node
- `coalesce_get_workspace_node` - Get a specific workspace node
- `coalesce_set_workspace_node` - Replace a workspace node with a full body
- `coalesce_update_workspace_node` - Safely update selected fields of a workspace node
- `coalesce_delete_workspace_node` - Delete a node from a workspace ⚠️

#### Jobs

- `coalesce_list_environment_jobs` - List all jobs for an environment
- `coalesce_list_workspace_jobs` - List all jobs for a workspace
- `coalesce_create_workspace_job` - Create a job in a workspace with node include/exclude selectors
- `coalesce_get_environment_job` - Get details of a specific job (via environment)
- `coalesce_update_workspace_job` - Update a job's name and node selectors
- `coalesce_delete_workspace_job` - Delete a job ⚠️

#### Subgraphs

- `coalesce_list_workspace_subgraphs` - List subgraphs in a workspace
- `coalesce_get_workspace_subgraph` - Get details of a specific subgraph
- `coalesce_create_workspace_subgraph` - Create a subgraph to group nodes visually
- `coalesce_update_workspace_subgraph` - Update a subgraph's name and node membership
- `coalesce_delete_workspace_subgraph` - Delete a subgraph (nodes are NOT deleted) ⚠️

#### Runs

- `coalesce_list_runs` - List runs with optional filters
- `coalesce_get_run` - Get details of a specific run
- `coalesce_get_run_results` - Get results of a completed run
- `coalesce_start_run` - Start a new run; requires Snowflake Key Pair auth (credentials from env vars)
- `coalesce_run_status` - Check status of a running job
- `coalesce_retry_run` - Retry a failed run; requires Snowflake Key Pair auth (credentials from env vars)
- `coalesce_cancel_run` - Cancel a running job (requires `runID` and `environmentID`; `orgID` may come from `COALESCE_ORG_ID`) ⚠️

#### Projects

- `coalesce_list_projects` - List all projects
- `coalesce_get_project` - Get project details
- `coalesce_create_project` - Create a new project
- `coalesce_update_project` - Update a project
- `coalesce_delete_project` - Delete a project ⚠️

#### Git Accounts

- `coalesce_list_git_accounts` - List all git accounts
- `coalesce_get_git_account` - Get git account details
- `coalesce_create_git_account` - Create a new git account
- `coalesce_update_git_account` - Update a git account
- `coalesce_delete_git_account` - Delete a git account ⚠️

#### Users

- `coalesce_list_org_users` - List all organization users
- `coalesce_get_user_roles` - Get roles for a specific user
- `coalesce_list_user_roles` - List all user roles
- `coalesce_set_org_role` - Set organization role for a user
- `coalesce_set_project_role` - Set project role for a user
- `coalesce_delete_project_role` - Remove project role from a user ⚠️
- `coalesce_set_env_role` - Set environment role for a user
- `coalesce_delete_env_role` - Remove environment role from a user ⚠️

### Intelligent Tools

Custom logic built on top of the API: pipeline planning, config completion, join analysis, workspace analysis, and more.

#### Node Creation and Configuration

- `coalesce_create_workspace_node_from_scratch` - Create a workspace node with no predecessors, apply fields to the requested completion level, and run automatic config completion
- `coalesce_create_workspace_node_from_predecessor` - Create a node from predecessor nodes, verify column coverage, suggest join columns, and run automatic config completion
- `coalesce_replace_workspace_node_columns` - Replace `metadata.columns` wholesale and optionally apply additional changes for complex column rewrites
- `coalesce_convert_join_to_aggregation` - Convert a join-style node into an aggregated fact-style node with generated JOIN/GROUP BY analysis
- `coalesce_apply_join_condition` - Auto-generate and write a FROM/JOIN/ON clause for a multi-predecessor node
- `coalesce_complete_node_configuration` - Intelligently complete a node's configuration by analyzing context and applying best-practice rules
- `coalesce_list_workspace_node_types` - List distinct node types observed in current workspace nodes
- `coalesce_analyze_workspace_patterns` - Analyze workspace nodes to detect package adoption, pipeline layers, methodology, and generate recommendations

#### Pipeline Planning and Execution

- `coalesce_plan_pipeline` - Plan a pipeline from SQL or a natural-language goal without mutating the workspace; ranks best-fit node types from the local repo
- `coalesce_create_pipeline_from_plan` - Execute an approved pipeline plan using predecessor-based creation
- `coalesce_create_pipeline_from_sql` - Plan and create a pipeline directly from SQL

#### Repo-Backed Node Types and Templates

- `coalesce_list_repo_packages` - Inspect a committed local Coalesce repo and list package aliases plus enabled node-type coverage from `packages/*.yml`
- `coalesce_list_repo_node_types` - List exact resolvable committed node-type identifiers from `nodeTypes/`, optionally scoped to one package alias or currently in-use types
- `coalesce_get_repo_node_type_definition` - Resolve one exact committed node type from a local repo and return its outer definition plus raw and parsed `metadata.nodeMetadataSpec`
- `coalesce_generate_set_workspace_node_template` - Generate a YAML-friendly `coalesce_set_workspace_node` body template from either a raw definition object or an exact committed repo definition resolved by `repoPath` or `COALESCE_REPO_PATH`

#### Node Type Corpus

- `coalesce_search_node_type_variants` - Search the committed node-type corpus snapshot by normalized family, package, primitive, or support status
- `coalesce_get_node_type_variant` - Load one exact node-type corpus variant by variant key
- `coalesce_generate_set_workspace_node_template_from_variant` - Generate a `coalesce_set_workspace_node` body template from a committed corpus variant without needing the original external source repo at runtime; partial variants are rejected unless `allowPartial=true`

#### Cache and Snapshots

- `coalesce_cache_workspace_nodes` - Fetch every page of workspace nodes, write the full snapshot to `coalesce_transform_mcp_data_cache/nodes/`, and return only cache metadata
- `coalesce_cache_environment_nodes` - Fetch every page of environment nodes, write the full snapshot to `coalesce_transform_mcp_data_cache/nodes/`, and return only cache metadata
- `coalesce_cache_runs` - Fetch every page of run results, write the full snapshot to `coalesce_transform_mcp_data_cache/runs/`, and return only cache metadata
- `coalesce_cache_org_users` - Fetch every page of organization users, write the full snapshot to `coalesce_transform_mcp_data_cache/users/`, and return only cache metadata
- `coalesce_clear_data_cache` - Delete all cached snapshots, auto-cached responses, and plan summaries under `coalesce_transform_mcp_data_cache/` ⚠️

#### Workflows

- `coalesce_run_and_wait` - Start a run and poll until completion; requires Snowflake Key Pair auth
- `coalesce_retry_and_wait` - Retry a failed run and poll until completion; requires Snowflake Key Pair auth
- `coalesce_get_run_details` - Get run metadata and results in one call
- `coalesce_get_environment_overview` - Get environment details with full node list

## Notes

- **Caching:** Large responses are auto-cached to disk. Use `coalesce_cache_workspace_nodes` and similar tools when you want a reusable snapshot. Configure the threshold with `COALESCE_MCP_AUTO_CACHE_MAX_BYTES`.
- **Repo-backed tools:** Set `COALESCE_REPO_PATH` to your local Coalesce repo root (containing `nodeTypes/`, `nodes/`, `packages/`) or pass `repoPath` on individual tool calls. The server does not clone repos or install packages.
- **SQL override is disallowed.** Nodes are built via YAML/config (columns, transforms, join conditions), not raw SQL. Template generation strips `overrideSQLToggle`, and write helpers reject `overrideSQL` fields.

## Links

- [Coalesce Docs](https://docs.coalesce.io/docs)
- [Coalesce API Docs](https://docs.coalesce.io/docs/api/authentication)
- [Coalesce Marketplace Docs](https://docs.coalesce.io/docs/marketplace)

## License

MIT
