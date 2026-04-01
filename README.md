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
| `COALESCE_MCP_READ_ONLY` | When `true`, hides all write/mutation tools during registration. Only read, list, search, cache, analyze, review, diagnose, and plan tools are exposed. | `false` |
<!-- ENV_METADATA_CORE_TABLE_END -->

### Snowflake (for run tools only)

Required for `start_run`, `retry_run`, `run_and_wait`, and `retry_and_wait`. The server starts without them — they're validated when you first use a run tool.

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

- `list_environments` - List all available environments
- `get_environment` - Get details of a specific environment
- `create_environment` - Create a new environment within a project
- `update_environment` - Update an existing environment
- `delete_environment` - Delete an environment ⚠️

#### Workspaces

- `list_workspaces` - List all workspaces
- `get_workspace` - Get details of a specific workspace

#### Nodes

- `list_environment_nodes` - List nodes in an environment
- `list_workspace_nodes` - List nodes in a workspace
- `get_environment_node` - Get a specific environment node
- `get_workspace_node` - Get a specific workspace node
- `set_workspace_node` - Replace a workspace node with a full body
- `update_workspace_node` - Safely update selected fields of a workspace node
- `delete_workspace_node` - Delete a node from a workspace ⚠️

#### Jobs

- `list_environment_jobs` - List all jobs for an environment
- `list_workspace_jobs` - List all jobs for a workspace
- `create_workspace_job` - Create a job in a workspace with node include/exclude selectors
- `get_environment_job` - Get details of a specific job (via environment)
- `update_workspace_job` - Update a job's name and node selectors
- `delete_workspace_job` - Delete a job ⚠️

#### Subgraphs

- `list_workspace_subgraphs` - List subgraphs in a workspace
- `get_workspace_subgraph` - Get details of a specific subgraph
- `create_workspace_subgraph` - Create a subgraph to group nodes visually
- `update_workspace_subgraph` - Update a subgraph's name and node membership
- `delete_workspace_subgraph` - Delete a subgraph (nodes are NOT deleted) ⚠️

#### Runs

- `diagnose_run_failure` - Diagnose a failed run with error classification, root-cause analysis, and actionable fix suggestions
- `list_runs` - List runs with optional filters
- `get_run` - Get details of a specific run
- `get_run_results` - Get results of a completed run
- `start_run` - Start a new run; requires Snowflake Key Pair auth (credentials from env vars)
- `run_status` - Check status of a running job
- `retry_run` - Retry a failed run; requires Snowflake Key Pair auth (credentials from env vars)
- `cancel_run` - Cancel a running job (requires `runID` and `environmentID`; `orgID` may come from `COALESCE_ORG_ID`) ⚠️

#### Projects

- `list_projects` - List all projects
- `get_project` - Get project details
- `create_project` - Create a new project
- `update_project` - Update a project
- `delete_project` - Delete a project ⚠️

#### Git Accounts

- `list_git_accounts` - List all git accounts
- `get_git_account` - Get git account details
- `create_git_account` - Create a new git account
- `update_git_account` - Update a git account
- `delete_git_account` - Delete a git account ⚠️

#### Users

- `list_org_users` - List all organization users
- `get_user_roles` - Get roles for a specific user
- `list_user_roles` - List all user roles
- `set_org_role` - Set organization role for a user
- `set_project_role` - Set project role for a user
- `delete_project_role` - Remove project role from a user ⚠️
- `set_env_role` - Set environment role for a user
- `delete_env_role` - Remove environment role from a user ⚠️

### Intelligent Tools

Custom logic built on top of the API: pipeline planning, config completion, join analysis, workspace analysis, and more.

#### Node Creation and Configuration

- `create_workspace_node_from_scratch` - Create a workspace node with no predecessors, apply fields to the requested completion level, and run automatic config completion
- `create_workspace_node_from_predecessor` - Create a node from predecessor nodes, verify column coverage, suggest join columns, and run automatic config completion
- `replace_workspace_node_columns` - Replace `metadata.columns` wholesale and optionally apply additional changes for complex column rewrites
- `convert_join_to_aggregation` - Convert a join-style node into an aggregated fact-style node with generated JOIN/GROUP BY analysis
- `apply_join_condition` - Auto-generate and write a FROM/JOIN/ON clause for a multi-predecessor node
- `create_node_from_external_schema` - Create a workspace node whose columns match an existing warehouse table or external schema
- `complete_node_configuration` - Intelligently complete a node's configuration by analyzing context and applying best-practice rules
- `list_workspace_node_types` - List distinct node types observed in current workspace nodes
- `analyze_workspace_patterns` - Analyze workspace nodes to detect package adoption, pipeline layers, methodology, and generate recommendations

#### Pipeline Planning and Execution

- `plan_pipeline` - Plan a pipeline from SQL or a natural-language goal without mutating the workspace; ranks best-fit node types from the local repo
- `create_pipeline_from_plan` - Execute an approved pipeline plan using predecessor-based creation
- `create_pipeline_from_sql` - Plan and create a pipeline directly from SQL
- `build_pipeline_from_intent` - Build a pipeline from a natural language goal with automatic entity resolution and node type selection
- `review_pipeline` - Analyze an existing pipeline for redundant nodes, missing joins, layer violations, naming issues, and optimization opportunities

#### Pipeline Workshop

- `pipeline_workshop_open` - Open an iterative pipeline builder session with workspace context pre-loaded
- `pipeline_workshop_instruct` - Send a natural language instruction to modify the current workshop plan
- `pipeline_workshop_status` - Get the current state of a workshop session
- `pipeline_workshop_close` - Close a workshop session and release resources

#### Repo-Backed Node Types and Templates

- `list_repo_packages` - Inspect a committed local Coalesce repo and list package aliases plus enabled node-type coverage from `packages/*.yml`
- `list_repo_node_types` - List exact resolvable committed node-type identifiers from `nodeTypes/`, optionally scoped to one package alias or currently in-use types
- `get_repo_node_type_definition` - Resolve one exact committed node type from a local repo and return its outer definition plus raw and parsed `metadata.nodeMetadataSpec`
- `generate_set_workspace_node_template` - Generate a YAML-friendly `set_workspace_node` body template from either a raw definition object or an exact committed repo definition resolved by `repoPath` or `COALESCE_REPO_PATH`

#### Node Type Corpus

- `search_node_type_variants` - Search the committed node-type corpus snapshot by normalized family, package, primitive, or support status
- `get_node_type_variant` - Load one exact node-type corpus variant by variant key
- `generate_set_workspace_node_template_from_variant` - Generate a `set_workspace_node` body template from a committed corpus variant without needing the original external source repo at runtime; partial variants are rejected unless `allowPartial=true`

#### Cache and Snapshots

- `cache_workspace_nodes` - Fetch every page of workspace nodes, write the full snapshot to `coalesce_transform_mcp_data_cache/nodes/`, and return only cache metadata
- `cache_environment_nodes` - Fetch every page of environment nodes, write the full snapshot to `coalesce_transform_mcp_data_cache/nodes/`, and return only cache metadata
- `cache_runs` - Fetch every page of run results, write the full snapshot to `coalesce_transform_mcp_data_cache/runs/`, and return only cache metadata
- `cache_org_users` - Fetch every page of organization users, write the full snapshot to `coalesce_transform_mcp_data_cache/users/`, and return only cache metadata
- `clear_data_cache` - Delete all cached snapshots, auto-cached responses, and plan summaries under `coalesce_transform_mcp_data_cache/` ⚠️

#### Workflows

- `run_and_wait` - Start a run and poll until completion; requires Snowflake Key Pair auth
- `retry_and_wait` - Retry a failed run and poll until completion; requires Snowflake Key Pair auth
- `get_run_details` - Get run metadata and results in one call
- `get_environment_overview` - Get environment details with full node list
- `get_environment_health` - Get a comprehensive health dashboard: node counts by type, run statuses, failed runs in last 24h, stale nodes, dependency health, and overall health score

## Snowflake Exploration via Cortex Code

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

## Notes

- **Caching:** Large responses are auto-cached to disk. Use `cache_workspace_nodes` and similar tools when you want a reusable snapshot. Configure the threshold with `COALESCE_MCP_AUTO_CACHE_MAX_BYTES`.
- **Repo-backed tools:** Set `COALESCE_REPO_PATH` to your local Coalesce repo root (containing `nodeTypes/`, `nodes/`, `packages/`) or pass `repoPath` on individual tool calls. The server does not clone repos or install packages.
- **SQL override is disallowed.** Nodes are built via YAML/config (columns, transforms, join conditions), not raw SQL. Template generation strips `overrideSQLToggle`, and write helpers reject `overrideSQL` fields.

## Links

- [Coalesce Docs](https://docs.coalesce.io/docs)
- [Coalesce API Docs](https://docs.coalesce.io/docs/api/authentication)
- [Coalesce Marketplace Docs](https://docs.coalesce.io/docs/marketplace)

## License

MIT
