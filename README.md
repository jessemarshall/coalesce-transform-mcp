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

Required for `start-run`, `retry-run`, `run-and-wait`, and `retry-and-wait`. The server starts without them — they're validated when you first use a run tool.

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

- `list-environments` - List all available environments
- `get-environment` - Get details of a specific environment
- `create-environment` - Create a new environment within a project
- `delete-environment` - Delete an environment ⚠️

#### Nodes

- `list-environment-nodes` - List nodes in an environment
- `list-workspace-nodes` - List nodes in a workspace
- `get-environment-node` - Get a specific environment node
- `get-workspace-node` - Get a specific workspace node
- `set-workspace-node` - Replace a workspace node with a full body
- `update-workspace-node` - Safely update selected fields of a workspace node
- `delete-workspace-node` - Delete a node from a workspace ⚠️

#### Jobs

- `list-jobs` - List all jobs for an environment
- `create-workspace-job` - Create a job in a workspace with node include/exclude selectors
- `get-job` - Get details of a specific job (via environment)
- `update-workspace-job` - Update a job's name and node selectors
- `delete-workspace-job` - Delete a job ⚠️

#### Subgraphs

- `get-workspace-subgraph` - Get details of a specific subgraph
- `create-workspace-subgraph` - Create a subgraph to group nodes visually
- `update-workspace-subgraph` - Update a subgraph's name and node membership
- `delete-workspace-subgraph` - Delete a subgraph (nodes are NOT deleted) ⚠️

#### Runs

- `list-runs` - List runs with optional filters
- `get-run` - Get details of a specific run
- `get-run-results` - Get results of a completed run
- `start-run` - Start a new run; requires Snowflake Key Pair auth (credentials from env vars)
- `run-status` - Check status of a running job
- `retry-run` - Retry a failed run; requires Snowflake Key Pair auth (credentials from env vars)
- `cancel-run` - Cancel a running job (requires `runID` and `environmentID`; `orgID` may come from `COALESCE_ORG_ID`) ⚠️

#### Projects

- `list-projects` - List all projects
- `get-project` - Get project details
- `create-project` - Create a new project
- `update-project` - Update a project
- `delete-project` - Delete a project ⚠️

#### Git Accounts

- `list-git-accounts` - List all git accounts
- `get-git-account` - Get git account details
- `create-git-account` - Create a new git account
- `update-git-account` - Update a git account
- `delete-git-account` - Delete a git account ⚠️

#### Users

- `list-org-users` - List all organization users
- `get-user-roles` - Get roles for a specific user
- `list-user-roles` - List all user roles
- `set-org-role` - Set organization role for a user
- `set-project-role` - Set project role for a user
- `delete-project-role` - Remove project role from a user ⚠️
- `set-env-role` - Set environment role for a user
- `delete-env-role` - Remove environment role from a user ⚠️

### Intelligent Tools

Custom logic built on top of the API: pipeline planning, config completion, join analysis, workspace analysis, and more.

#### Node Creation and Configuration

- `create-workspace-node-from-scratch` - Create a workspace node with no predecessors, apply fields to the requested completion level, and run automatic config completion
- `create-workspace-node-from-predecessor` - Create a node from predecessor nodes, verify column coverage, suggest join columns, and run automatic config completion
- `replace-workspace-node-columns` - Replace `metadata.columns` wholesale and optionally apply additional changes for complex column rewrites
- `convert-join-to-aggregation` - Convert a join-style node into an aggregated fact-style node with generated JOIN/GROUP BY analysis
- `apply-join-condition` - Auto-generate and write a FROM/JOIN/ON clause for a multi-predecessor node
- `complete-node-configuration` - Intelligently complete a node's configuration by analyzing context and applying best-practice rules
- `list-workspace-node-types` - List distinct node types observed in current workspace nodes
- `analyze-workspace-patterns` - Analyze workspace nodes to detect package adoption, pipeline layers, methodology, and generate recommendations

#### Pipeline Planning and Execution

- `plan-pipeline` - Plan a pipeline from SQL or a natural-language goal without mutating the workspace; ranks best-fit node types from the local repo
- `create-pipeline-from-plan` - Execute an approved pipeline plan using predecessor-based creation
- `create-pipeline-from-sql` - Plan and create a pipeline directly from SQL

#### Repo-Backed Node Types and Templates

- `list-repo-packages` - Inspect a committed local Coalesce repo and list package aliases plus enabled node-type coverage from `packages/*.yml`
- `list-repo-node-types` - List exact resolvable committed node-type identifiers from `nodeTypes/`, optionally scoped to one package alias or currently in-use types
- `get-repo-node-type-definition` - Resolve one exact committed node type from a local repo and return its outer definition plus raw and parsed `metadata.nodeMetadataSpec`
- `generate-set-workspace-node-template` - Generate a YAML-friendly `set-workspace-node` body template from either a raw definition object or an exact committed repo definition resolved by `repoPath` or `COALESCE_REPO_PATH`

#### Node Type Corpus

- `search-node-type-variants` - Search the committed node-type corpus snapshot by normalized family, package, primitive, or support status
- `get-node-type-variant` - Load one exact node-type corpus variant by variant key
- `generate-set-workspace-node-template-from-variant` - Generate a `set-workspace-node` body template from a committed corpus variant without needing the original external source repo at runtime; partial variants are rejected unless `allowPartial=true`

#### Cache and Snapshots

- `cache-workspace-nodes` - Fetch every page of workspace nodes, write the full snapshot to `coalesce_transform_mcp_data_cache/nodes/`, and return only cache metadata
- `cache-environment-nodes` - Fetch every page of environment nodes, write the full snapshot to `coalesce_transform_mcp_data_cache/nodes/`, and return only cache metadata
- `cache-runs` - Fetch every page of run results, write the full snapshot to `coalesce_transform_mcp_data_cache/runs/`, and return only cache metadata
- `cache-org-users` - Fetch every page of organization users, write the full snapshot to `coalesce_transform_mcp_data_cache/users/`, and return only cache metadata
- `clear_coalesce_transform_mcp_data_cache` - Delete all cached snapshots, auto-cached responses, and plan summaries under `coalesce_transform_mcp_data_cache/` ⚠️

#### Workflows

- `run-and-wait` - Start a run and poll until completion; requires Snowflake Key Pair auth
- `retry-and-wait` - Retry a failed run and poll until completion; requires Snowflake Key Pair auth
- `get-run-details` - Get run metadata and results in one call
- `get-environment-overview` - Get environment details with full node list

## Notes

- **Caching:** Large responses are auto-cached to disk. Use `cache-workspace-nodes` and similar tools when you want a reusable snapshot. Configure the threshold with `COALESCE_MCP_AUTO_CACHE_MAX_BYTES`.
- **Repo-backed tools:** Set `COALESCE_REPO_PATH` to your local Coalesce repo root (containing `nodeTypes/`, `nodes/`, `packages/`) or pass `repoPath` on individual tool calls. The server does not clone repos or install packages.
- **SQL override is disallowed.** Nodes are built via YAML/config (columns, transforms, join conditions), not raw SQL. Template generation strips `overrideSQLToggle`, and write helpers reject `overrideSQL` fields.

## Links

- [Coalesce Docs](https://docs.coalesce.io/docs)
- [Coalesce API Docs](https://docs.coalesce.io/docs/api/authentication)
- [Coalesce Marketplace Docs](https://docs.coalesce.io/docs/marketplace)

## License

MIT
