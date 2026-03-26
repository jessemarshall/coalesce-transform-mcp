# coalesce-transform-mcp

**Disclaimer:** This is a sample project provided as-is for reference.

MCP server for the Coalesce Transform API. Lets MCP-compatible AI assistants handle supported Coalesce workflows such as managing environments, nodes, jobs, and subgraphs, triggering runs, and selected project, user, and git-account operations from an editor or CLI.

## Requirements

- [Node.js](https://nodejs.org/) >= 18.0.0 (includes npm)
- A [Coalesce](https://coalesce.io/) account with at least one workspace
- A Coalesce access token (generated from the Deploy tab in your workspace; see [Coalesce API docs](https://docs.coalesce.io/docs/api/authentication))
- An MCP-compatible AI client (Claude Code, Claude Desktop, Cursor, Windsurf, etc.)
- **For run tools only:** A Snowflake account with key pair authentication configured

## Installation

### 1. Clone and Build

```bash
git clone https://github.com/jessemarshall/coalesce-transform-mcp.git
cd coalesce-transform-mcp
npm install
npm run build
```

### 2. Set Environment Variables

Add your credentials to your shell profile (`~/.zshrc` or `~/.bashrc`):

```bash
# Coalesce API (required)
export COALESCE_ACCESS_TOKEN="your-token-here"
export COALESCE_BASE_URL="https://app.coalescesoftware.io"
export COALESCE_ORG_ID="your-org-id"  # Optional: used as fallback by cancel-run
export COALESCE_REPO_PATH="/path/to/local/coalesce-repo"  # Optional: fallback for repo-backed node-type tools and pipeline planning

# Snowflake Key Pair Auth (required for `start-run`, `retry-run`, `run-and-wait`, and `retry-and-wait`)
export SNOWFLAKE_USERNAME="your-snowflake-username"
export SNOWFLAKE_KEY_PAIR_KEY="/path/to/your/snowflake_key.pem"
export SNOWFLAKE_KEY_PAIR_PASS="your-key-passphrase"  # Only needed for encrypted keys
export SNOWFLAKE_WAREHOUSE="your-warehouse"
export SNOWFLAKE_ROLE="your-role"
```

Then reload your shell:

```bash
source ~/.zshrc
```

### 3. Add the MCP Server to Your Client

Add the following to your client's MCP configuration file, replacing `/absolute/path/to/coalesce-transform-mcp` with the path where you cloned the repo:

| Client | Config file location |
|--------|---------------------|
| Claude Code | `.mcp.json` in your project root (or `~/.claude.json` for global) |
| Claude Desktop (macOS) | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Cursor | `.cursor/mcp.json` in your project root |
| Windsurf | `~/.codeium/windsurf/mcp_config.json` |

```json
{
  "coalesce-transform": {
    "command": "node",
    "args": ["/absolute/path/to/coalesce-transform-mcp/dist/index.js"],
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

The `${VAR}` syntax pulls values from your shell environment, keeping credentials out of your config files. Pass through `COALESCE_ORG_ID` if you want the `cancel-run` fallback, and `COALESCE_REPO_PATH` if you want repo-backed tools and pipeline planning to default to a local repo path.

> **Never hardcode credentials in config files that are tracked by git.** Use environment variable references (`${VAR}`) instead.

## Environment Variable Reference

### Coalesce (required at startup)

| Variable | Description | Example |
|----------|-------------|---------|
| `COALESCE_ACCESS_TOKEN` | Bearer token from Coalesce Deploy tab | `eyJhbG...` |
| `COALESCE_BASE_URL` | Region-specific base URL (see [Region Base URLs](#region-base-urls)) | `https://app.coalescesoftware.io` |

### Coalesce (optional)

| Variable | Used By | Description |
|----------|---------|-------------|
| `COALESCE_ORG_ID` | `cancel-run` | Optional fallback Org ID when `orgID` is not passed to the tool |
| `COALESCE_REPO_PATH` | Repo-backed node-type tools, `plan-pipeline`, `create-pipeline-from-sql` | Optional fallback local repo path when `repoPath` is not passed explicitly. Point this at the repo root that contains `nodeTypes/`, `nodes/`, and usually `packages/`. |
| `COALESCE_MCP_AUTO_CACHE_MAX_BYTES` | All JSON-returning tools/workflows | Pretty-printed JSON size threshold in bytes before the full response is automatically written to disk and only cache metadata is returned inline. Defaults to `32768`. |
| `COALESCE_MCP_MAX_REQUEST_BODY_BYTES` | All API-calling tools | Maximum serialized JSON body size in bytes for outbound API requests. Rejects oversized payloads before they leave the process. Defaults to `524288` (512 KB). |

### Snowflake (required for run tools: `start-run`, `retry-run`, `run-and-wait`, `retry-and-wait`)

These are validated lazily: the server starts without them, but the run-triggering tools will error if they are missing.

| Variable | Required | Description |
|----------|----------|-------------|
| `SNOWFLAKE_USERNAME` | Yes | Snowflake account username |
| `SNOWFLAKE_KEY_PAIR_KEY` | Yes | File path to PEM-encoded private key for Snowflake auth |
| `SNOWFLAKE_KEY_PAIR_PASS` | No | Password to decrypt an encrypted private key |
| `SNOWFLAKE_WAREHOUSE` | Yes | Snowflake compute warehouse |
| `SNOWFLAKE_ROLE` | Yes | Snowflake user role |

## Region Base URLs

| Region | URL |
|--------|-----|
| US (default) | `https://app.coalescesoftware.io` |
| EU (west-3) | `https://app.eu.coalescesoftware.io` |
| EU (west-2) | `https://app.eu-west-2.aws.coalescesoftware.io` |
| Canada | `https://app.northamerica-northeast1.gcp.coalescesoftware.io` |
| Australia | `https://app.australia-southeast1.gcp.coalescesoftware.io` |

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

## Automatic Large-Response Caching

Large JSON tool and workflow responses are auto-cached to `coalesce_transform_mcp_data_cache/auto-cache/`.

- If the pretty-printed JSON response is at or below the inline threshold, the full payload is returned inline.
- If it exceeds the threshold, the server writes the full JSON response to `coalesce_transform_mcp_data_cache/auto-cache/` and returns compact metadata with an MCP `resourceUri` plus a `resource_link` you can read through the client.
- The default threshold is `32768` bytes and can be overridden with `COALESCE_MCP_AUTO_CACHE_MAX_BYTES`.
- Explicit cache tools such as `cache-workspace-nodes` are still the better choice when you already know you want a reusable snapshot under `coalesce_transform_mcp_data_cache/`.

## Prompt Surface

The server also exposes reusable MCP prompts for high-value workflows:

- `coalesce-start-here` - ID discovery and safe first steps before mutations
- `safe-pipeline-planning` - planner-first pipeline review and approval flow
- `run-operations-guide` - choosing the right run helper and interpreting statuses
- `large-result-handling` - working with cached responses and `coalesce://cache/...` resources

## Repo-Backed Workflow

Use repo-backed discovery, template generation, and pipeline planning when you have a local clone of your Coalesce repo. Set `COALESCE_REPO_PATH` to the repo root (the directory containing `nodeTypes/`, `nodes/`, and usually `packages/`) or pass `repoPath` explicitly on individual tool calls.

Notes:

- `repoPath` is supported on repo-backed discovery/template tools and pipeline planning.
- `COALESCE_REPO_PATH` is an optional fallback for those repo-backed tools when `repoPath` is omitted.
- The MCP does not clone repos, fetch branches, or install packages.
- If the committed repo does not contain the definition, fall back to the corpus tools.

## Large Collection Workflow

When a list response would be too large for chat context, or you want a reusable artifact on disk:

1. Use the matching cache tool instead of the inline list result:
   - `cache-workspace-nodes`
   - `cache-environment-nodes`
   - `cache-runs`
   - `cache-org-users`
2. Read the returned `fileUri` or `metaUri` resource through MCP, or follow the returned `resource_link` content blocks.
3. Use inline list tools for smaller exploratory reads and targeted follow-up calls.

## Operational Guardrails

- SQL override is intentionally disallowed in this project. Repo and corpus template generation strips `overrideSQLToggle`, and node write helpers reject `overrideSQL` and `override.*` fields.

## Helpful Coalesce Docs

- [Coalesce Docs](https://docs.coalesce.io/docs) - General product documentation, concepts, and platform guidance.
- [Coalesce API Docs](https://docs.coalesce.io/docs/api/authentication) - Authentication, endpoints, and request/response reference for the API.
- [Coalesce Marketplace Docs](https://docs.coalesce.io/docs/marketplace) - Packages, marketplace-managed node types, and template authoring context.

## License

MIT
