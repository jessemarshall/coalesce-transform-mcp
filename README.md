# coalesce-transform-mcp

[![npm version](https://img.shields.io/npm/v/coalesce-transform-mcp?color=cb3837&logo=npm)](https://www.npmjs.com/package/coalesce-transform-mcp)
[![Install in VS Code](https://img.shields.io/badge/VS_Code-Install_MCP-007ACC?style=flat&logo=visualstudiocode)](https://insiders.vscode.dev/redirect?url=vscode%3Amcp%2Finstall%3F%257B%2522name%2522%253A%2522coalesce-transform%2522%252C%2522command%2522%253A%2522npx%2522%252C%2522args%2522%253A%255B%2522coalesce-transform-mcp%2522%255D%257D)
[![Install in VS Code Insiders](https://img.shields.io/badge/VS_Code_Insiders-Install_MCP-24bfa5?style=flat&logo=visualstudiocode)](https://insiders.vscode.dev/redirect?url=vscode-insiders%3Amcp%2Finstall%3F%257B%2522name%2522%253A%2522coalesce-transform%2522%252C%2522command%2522%253A%2522npx%2522%252C%2522args%2522%253A%255B%2522coalesce-transform-mcp%2522%255D%257D)
[![Install in Cursor](https://img.shields.io/badge/Cursor-Install_MCP-000?style=flat&logo=cursor)](https://cursor.com/install-mcp?name=coalesce-transform&config=eyJjb21tYW5kIjoibnB4IiwiYXJncyI6WyJjb2FsZXNjZS10cmFuc2Zvcm0tbWNwIl19)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

MCP server for [Coalesce](https://coalesce.io/). Built for **Snowflake [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli) (CoCo)** - with first-class support for every other MCP client (Claude Code, Claude Desktop, Cursor, VS Code, Windsurf). Manage nodes, pipelines, environments, jobs, and runs, and drive the local-first [`coa`](https://www.npmjs.com/package/@coalescesoftware/coa) CLI from the same server: validate a project, preview DDL/DML, plan a deployment, and apply it to a cloud environment.

- **Cloud REST tools** - build pipelines declaratively, edit node YAML, review lineage, run deployed jobs, audit documentation.
- **Local COA CLI tools** - validate projects before check-in, preview generated DDL/DML (`--dry-run`), run `plan → deploy → refresh` cycles. COA is bundled - no separate install.

---

## I want to…

|     | Task | Jump to |
| :-: | ---- | ------- |
| 📦 | Install for my AI client | [Installation](#installation) |
| 🚀 | Get running in 2 minutes | [Quick start](#quick-start) |
| 🎛️ | Customize agent behavior | [Skills](#skills) |
| 🔍 | Find a specific tool | [Tools](#tools) |
| 🔑 | Authenticate (env var or `~/.coa/config`) | [Credentials](#credentials) |
| 🌐 | Run against multiple Coalesce environments | [Multiple environments](#multiple-environments) |
| 🔒 | Lock prod down to read-only | [Safety model](docs/safety-model.md) |
| 🧰 | Use the `coa` CLI tools | [Using the COA CLI tools](#using-the-coa-cli-tools) |
| 🧪 | Try a prerelease build | [Prerelease channel](docs/prerelease.md) |
| 🩺 | Debug "why isn't auth working?" | [Diagnosing setup](docs/diagnosing-setup.md) |

---

## Installation

Each link below opens a short install guide with a click-to-install button (where supported) and the manual config.

| Client | Install guide |
| ------ | ------------- |
| ❄️ **Snowflake Cortex Code (CoCo)** | [docs/installation-guides/cortex-code.md](docs/installation-guides/cortex-code.md) |
| Cursor | [docs/installation-guides/cursor.md](docs/installation-guides/cursor.md) |
| VS Code | [docs/installation-guides/vscode.md](docs/installation-guides/vscode.md) |
| VS Code Insiders | [docs/installation-guides/vscode-insiders.md](docs/installation-guides/vscode-insiders.md) |
| Claude Code (CLI) | [docs/installation-guides/claude-code.md](docs/installation-guides/claude-code.md) |
| Claude Desktop | [docs/installation-guides/claude-desktop.md](docs/installation-guides/claude-desktop.md) |
| Windsurf | [docs/installation-guides/windsurf.md](docs/installation-guides/windsurf.md) |

Or expand the dropdown for your client below to paste directly without leaving this page.

<details>
<summary><b>❄️ Install in Snowflake Cortex Code (CoCo)</b></summary>

**Why this pairing?** Cortex Code is Snowflake's AI coding CLI - it already authenticates to your warehouse, runs under your Snowflake role, and has native tools for querying live data. Add `coalesce-transform-mcp` and a single agent session can plan pipelines, create nodes, run DML, and verify results against real rows without leaving the terminal.

One-liner (after [installing the Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli)):

```bash
cortex mcp add coalesce-transform npx coalesce-transform-mcp
```

Or edit `~/.snowflake/cortex/mcp.json` directly:

```json
{
  "mcpServers": {
    "coalesce-transform": {
      "type": "stdio",
      "command": "npx",
      "args": ["coalesce-transform-mcp"],
      "env": {
        "COALESCE_ACCESS_TOKEN": "<YOUR_TOKEN>"
      }
    }
  }
}
```

Drop the `env` block if you're using `~/.coa/config` - Cortex Code and Coalesce can both pick the token up from the same profile. Full walkthrough: **[docs/installation-guides/cortex-code.md](docs/installation-guides/cortex-code.md)**.

</details>

<details>
<summary><b>Install in Cursor</b></summary>

Click-to-install: [![Install in Cursor](https://img.shields.io/badge/Cursor-Install_MCP-000?style=flat&logo=cursor)](https://cursor.com/install-mcp?name=coalesce-transform&config=eyJjb21tYW5kIjoibnB4IiwiYXJncyI6WyJjb2FsZXNjZS10cmFuc2Zvcm0tbWNwIl19)

Manual: paste into `.cursor/mcp.json` in your project root (or `~/.cursor/mcp.json` for global):

```json
{
  "mcpServers": {
    "coalesce-transform": {
      "command": "npx",
      "args": ["coalesce-transform-mcp"],
      "env": {
        "COALESCE_ACCESS_TOKEN": "<YOUR_TOKEN>"
      }
    }
  }
}
```

Cursor does **not** expand `${VAR}` - paste the literal token, or drop the `env` block and use `~/.coa/config` (see [Credentials](#credentials)).

</details>

<details>
<summary><b>Install in VS Code</b></summary>

Click-to-install: [![Install in VS Code](https://img.shields.io/badge/VS_Code-Install_MCP-007ACC?style=flat&logo=visualstudiocode)](https://insiders.vscode.dev/redirect?url=vscode%3Amcp%2Finstall%3F%257B%2522name%2522%253A%2522coalesce-transform%2522%252C%2522command%2522%253A%2522npx%2522%252C%2522args%2522%253A%255B%2522coalesce-transform-mcp%2522%255D%257D)

Manual: follow the [VS Code MCP install guide](https://code.visualstudio.com/docs/copilot/chat/mcp-servers#_add-an-mcp-server) and use this config:

```json
{
  "name": "coalesce-transform",
  "command": "npx",
  "args": ["coalesce-transform-mcp"]
}
```

Add the `COALESCE_ACCESS_TOKEN` via VS Code's secret input prompt, or drop the token and use `~/.coa/config`. Reload the VS Code window after install.

</details>

<details>
<summary><b>Install in VS Code Insiders</b></summary>

Click-to-install: [![Install in VS Code Insiders](https://img.shields.io/badge/VS_Code_Insiders-Install_MCP-24bfa5?style=flat&logo=visualstudiocode)](https://insiders.vscode.dev/redirect?url=vscode-insiders%3Amcp%2Finstall%3F%257B%2522name%2522%253A%2522coalesce-transform%2522%252C%2522command%2522%253A%2522npx%2522%252C%2522args%2522%253A%255B%2522coalesce-transform-mcp%2522%255D%257D)

Manual: identical to the stable [VS Code install](docs/installation-guides/vscode.md) - Insiders reads the same MCP config.

</details>

<details>
<summary><b>Install in Claude Code (CLI)</b></summary>

One-liner:

```bash
claude mcp add coalesce-transform -- npx coalesce-transform-mcp
```

Pass env vars inline if you need them:

```bash
claude mcp add coalesce-transform \
  --env COALESCE_ACCESS_TOKEN=$COALESCE_ACCESS_TOKEN \
  -- npx coalesce-transform-mcp
```

Manual: paste into `.mcp.json` in your project root (or `~/.claude.json` for global):

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

Claude Code **does** expand `${VAR}` from your shell env at load time - `.mcp.json` can stay safely committed to git with variable references. Omit the `env` block if using `~/.coa/config`.

</details>

<details>
<summary><b>Install in Claude Desktop</b></summary>

No deeplink yet - paste manually.

File: `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows).

```json
{
  "mcpServers": {
    "coalesce-transform": {
      "command": "npx",
      "args": ["coalesce-transform-mcp"],
      "env": {
        "COALESCE_ACCESS_TOKEN": "<YOUR_TOKEN>"
      }
    }
  }
}
```

Claude Desktop does **not** expand `${VAR}` - paste the literal token, or drop the `env` block and use `~/.coa/config`. Fully quit Claude Desktop (`Cmd+Q`) and relaunch after editing.

</details>

<details>
<summary><b>Install in Windsurf</b></summary>

No deeplink yet - paste manually.

File: `~/.codeium/windsurf/mcp_config.json`.

```json
{
  "mcpServers": {
    "coalesce-transform": {
      "command": "npx",
      "args": ["coalesce-transform-mcp"],
      "env": {
        "COALESCE_ACCESS_TOKEN": "<YOUR_TOKEN>"
      }
    }
  }
}
```

Windsurf does **not** expand `${VAR}` - paste the literal token, or drop the `env` block and use `~/.coa/config`. Restart Windsurf after editing.

</details>

> [!CAUTION]
> **Never hardcode credentials in git-tracked config files.** Only Claude Code's `.mcp.json` expands `${VAR}` from your shell env. For any other client, keep secrets in `~/.coa/config` or a secrets manager your client integrates with - don't commit literals into these JSON files.

> [!TIP]
> **❄️ Snowflake Cortex Code + coalesce-transform-mcp.** CoCo is Snowflake's AI coding CLI - it already knows your warehouse, role, and data. Drop this MCP in and an agent can plan pipelines, create nodes, run DML, and verify results in a single session, all under Snowflake's auth model. **[Install in Cortex Code →](docs/installation-guides/cortex-code.md)**

> [!TIP]
> The two surfaces are orthogonal. Use both, one, or neither. Every destructive tool - on either surface - requires explicit confirmation before running. New? Run the `/coalesce-setup` prompt after install - it walks you through anything missing.

---

## Quick start

**Requirements:**

- [Node.js](https://nodejs.org/) 22+
- A [Coalesce](https://coalesce.io/) account with a workspace
- An MCP-compatible AI client (see [Installation](#installation))
- Snowflake credentials - only if you plan to use run tools or `coa_create`/`coa_run` (see [Credentials](#credentials))
- Install footprint is ~76 MB unpacked (the bundled `@coalescesoftware/coa` CLI ships its own runtime; the MCP tarball itself is under 1 MB)

**1. Clone your project.** If your team already has a Coalesce project in Git, clone it locally - the bundled `coa` CLI operates on a project directory, so most local create/run tools require one on disk:

```bash
git clone <your-coalesce-project-repo-url>
cd my-project
```

Don't have a Git-linked project yet? In the Coalesce UI, open your workspace → **Settings → Git** and connect a repo (or create one via your Git provider and paste the URL). Coalesce will commit the project skeleton on first push; clone that repo locally once it's populated.

<details>
<summary>What's in a Coalesce project directory?</summary>

```text
my-project/
├── data.yml                 # Root metadata (fileVersion, platformKind)
├── locations.yml            # Storage location manifest
├── nodes/                   # Pipeline nodes (.yml for V1, .sql for V2)
├── nodeTypes/               # Node type definitions with templates
├── environments/            # Environment configs with storage mappings
├── macros/                  # Reusable SQL macros
├── jobs/                    # Job definitions
└── subgraphs/               # Subgraph definitions
```

**V1 vs V2** - the format is pinned by `fileVersion` in `data.yml`. **V1** (`fileVersion: 1` or `2`) stores each node as a single YAML file with columns, transforms, and config inline. **V2** (`fileVersion: 3`) is SQL-first: the node body lives in a `.sql` file using `@id` / `@nodeType` annotations and `{{ ref() }}` references, with YAML retained for config. New projects default to V2; existing V1 projects keep working unchanged.

</details>

Point the MCP at this directory by setting `repoPath` in `~/.coa/config` or `COALESCE_REPO_PATH` in your env block.

**2. Create `workspaces.yml`.** This file is **required** for `coa_create` / `coa_run` and their dry-run variants. It maps each storage location declared in `locations.yml` to a physical database + schema for local development. It's typically gitignored (per-developer), so cloning the project does not give it to you - you have to create it.

The `/coalesce-setup` prompt detects a missing `workspaces.yml` and walks you through it. If you'd rather do it directly, pick one of:

- **Let COA bootstrap it** (easiest): from the project root, run

  ```bash
  npx @coalescesoftware/coa doctor --fix
  ```

  Or from your MCP client, call the `coa_bootstrap_workspaces` tool (requires `confirmed: true`) which runs the same command.

  > [!WARNING]
  > **The generated file contains placeholder values.** `coa doctor --fix` seeds `database`/`schema` with defaults that won't match your real warehouse. Open the file and replace every placeholder before running `coa_create` / `coa_run` - otherwise the generated DDL/DML will target the wrong (or non-existent) database.

- **Hand-write it.** Authoritative schema (from `coa describe schema workspaces` - no top-level wrapper, no `fileVersion`):

  ```yaml
  # workspaces.yml - keys are workspace names; `dev` is the default if --workspace is omitted
  dev:
    connection: snowflake          # required - name of the connection block COA should use
    locations:                     # optional - one entry per storage location name from locations.yml
      SRC_INGEST_TASTY_BITES:
        database: JESSE_DEV        # required
        schema: INGEST_TASTY_BITES # required
      ETL_STAGE:
        database: JESSE_DEV
        schema: ETL_STAGE
      ANALYTICS:
        database: JESSE_DEV
        schema: ANALYTICS
  ```

Verify with `coa_doctor` (or `npx @coalescesoftware/coa doctor`) - it checks `data.yml`, `workspaces.yml`, credentials, and warehouse connectivity end to end.

**3. Pick an auth path:**

<table>
<tr>
<th>Option A - env var</th>
<th>Option B - reuse <code>~/.coa/config</code></th>
</tr>
<tr valign="top">
<td>

Simplest for first-time MCP users. Generate a `COALESCE_ACCESS_TOKEN` from Coalesce → Deploy → User Settings, then include it in your client config:

```json
{
  "env": {
    "COALESCE_ACCESS_TOKEN": "<YOUR_TOKEN>"
  }
}
```

</td>
<td>

Best if you already use the `coa` CLI - the server reads the same profile file, so nothing to duplicate. Drop the `env` block entirely:

```json
{
  "command": "npx",
  "args": ["coalesce-transform-mcp"]
}
```

See [Credentials](#credentials) for the profile schema.

</td>
</tr>
</table>

When both sources set a field, the env var wins.

**4. Install the server** via one of the [Installation](#installation) paths above.

**5. Restart your client,** then run the `/coalesce-setup` prompt to verify everything is wired up.

If you have more than one Coalesce environment to manage, see [Multiple environments](#multiple-environments).

---

## Configuration

### Credentials

The server reads credentials from two sources and merges them with **env-wins precedence** - a matching env var always overrides the profile value, so you can pin a single field per session without editing the config file. Call `diagnose_setup` to see which source supplied each value.

#### Source 1: `~/.coa/config` (shared with the `coa` CLI)

COA stores credentials in a standard INI file. You create it by hand, or let `coa` write it as you use the CLI. The MCP reads the profile selected by `COALESCE_PROFILE` (default `[default]`) and maps the keys below onto their matching env vars.

```ini
[default]
token=<your-coalesce-refresh-token>
domain=https://your-org.app.coalescesoftware.io
snowflakeAccount=<your-snowflake-account>   # e.g., abc12345.us-east-1 - required by coa CLI
snowflakeUsername=YOUR_USER
snowflakeRole=YOUR_ROLE
snowflakeWarehouse=YOUR_WAREHOUSE
snowflakeKeyPairKey=/Users/you/.coa/rsa_key.p8
snowflakeAuthType=KeyPair
orgID=<your-org-id>              # optional; fallback for cancel-run
repoPath=/Users/you/path/to/repo # optional; for repo-backed tools
cacheDir=/Users/you/.coa/cache   # optional; per-profile cache isolation

[staging]
# …additional profiles; select with COALESCE_PROFILE
```

**Key mapping** - each profile key maps to an env var of the same concept:

| Profile key | Env var |
| ----------- | ------- |
| `token` | `COALESCE_ACCESS_TOKEN` |
| `domain` | `COALESCE_BASE_URL` |
| `snowflake*` (all keys) | `SNOWFLAKE_*` (matching suffix) |
| `orgID` | `COALESCE_ORG_ID` |
| `repoPath` | `COALESCE_REPO_PATH` |
| `cacheDir` | `COALESCE_CACHE_DIR` |

Notes:

- `snowflakeAuthType` is read by COA itself (no env var) - include it when using key-pair auth.
- `orgID`, `repoPath`, and `cacheDir` are MCP-specific - the COA CLI ignores them.
- Only the fields the MCP needs are shown above. COA's config supports many more - run `npx @coalescesoftware/coa describe config` for the authoritative reference. Unknown keys are ignored.

If `~/.coa/config` doesn't exist the server runs env-only - startup never fails on a missing or malformed profile file; it just logs a stderr warning.

#### Source 2: env vars in your MCP config

<!-- ENV_METADATA_CORE_TABLE_START -->
| Variable | Description | Default |
| -------- | -------- | -------- |
| `COALESCE_ACCESS_TOKEN` | Bearer token from the Coalesce Deploy tab. Optional when `~/.coa/config` provides a `token`. | — |
| `COALESCE_PROFILE` | Selects which `~/.coa/config` profile to load. | `default` |
| `COALESCE_BASE_URL` | Region-specific base URL. | `https://app.coalescesoftware.io (US)` |
| `COALESCE_ORG_ID` | Fallback org ID for cancel-run. Also readable from `orgID` in the active ~/.coa/config profile. | — |
| `COALESCE_REPO_PATH` | Local repo root for repo-backed tools and pipeline planning. Also readable from `repoPath` in the active ~/.coa/config profile. | — |
| `COALESCE_CACHE_DIR` | Base directory for the local data cache. When set, cache files are written here instead of the working directory. Also readable from `cacheDir` in the active ~/.coa/config profile. | — |
| `COALESCE_MCP_AUTO_CACHE_MAX_BYTES` | JSON size threshold before auto-caching to disk. | `32768` |
| `COALESCE_MCP_LINEAGE_TTL_MS` | In-memory lineage cache TTL in milliseconds. | `1800000` |
| `COALESCE_MCP_MAX_REQUEST_BODY_BYTES` | Max outbound API request body size. | `524288` |
| `COALESCE_MCP_READ_ONLY` | When `true`, hides all write/mutation tools during registration. Only read, list, search, cache, analyze, review, diagnose, and plan tools are exposed. | `false` |
| `COALESCE_MCP_SKILLS_DIR` | Directory for customizable AI skill resources. When set, reads context resources from this directory and seeds defaults on first run. Users can augment or override any skill. | — |
<!-- ENV_METADATA_CORE_TABLE_END -->

#### Snowflake credentials (run tools only)

`start_run`, `retry_run`, `run_and_wait`, `retry_and_wait`, and the warehouse-touching COA tools (`coa_create`, `coa_run`) need Snowflake credentials. These normally come from `~/.coa/config`. Override any field via env var:

<!-- ENV_METADATA_SNOWFLAKE_TABLE_START -->
| Variable | Required | Description |
| -------- | -------- | -------- |
| `SNOWFLAKE_ACCOUNT` | Yes | Snowflake account identifier (e.g., `abc12345.us-east-1`). Required by the local `coa` CLI and `coa doctor`; not used by the MCP's REST run path. |
| `SNOWFLAKE_USERNAME` | Yes | Snowflake account username |
| `SNOWFLAKE_KEY_PAIR_KEY` | No | Path to PEM-encoded private key (required if SNOWFLAKE_PAT not set) |
| `SNOWFLAKE_PAT` | No | Snowflake Programmatic Access Token (alternative to key pair) |
| `SNOWFLAKE_KEY_PAIR_PASS` | No | Passphrase for encrypted keys |
| `SNOWFLAKE_WAREHOUSE` | Yes | Snowflake compute warehouse |
| `SNOWFLAKE_ROLE` | Yes | Snowflake user role |
<!-- ENV_METADATA_SNOWFLAKE_TABLE_END -->

"Required" means one of env OR the matching `~/.coa/config` field must supply the value. **`SNOWFLAKE_PAT` is env-only** - COA's config uses `snowflakePassword` for Basic auth (a different concept), which this server deliberately doesn't read.

#### Field-level overrides

<details>
<summary>Pin a profile but override one field without editing the config file</summary>

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

Reads: "use the `[staging]` profile, but override its `snowflakeRole`."

</details>

### Multiple environments

<details>
<summary>Register dev / staging / prod as separate namespaced servers</summary>

If you work across several Coalesce environments (dev/staging/prod, or multiple orgs), register the package once per profile under distinct server names:

```json
{
  "mcpServers": {
    "coalesce-prod": {
      "command": "npx",
      "args": ["coalesce-transform-mcp"],
      "env": {
        "COALESCE_PROFILE": "prod",
        "COALESCE_MCP_READ_ONLY": "true"
      }
    },
    "coalesce-dev": {
      "command": "npx",
      "args": ["coalesce-transform-mcp"],
      "env": { "COALESCE_PROFILE": "dev" }
    }
  }
}
```

Why this pattern:

- **Namespaced tools.** The client surfaces `coalesce-prod__*` vs `coalesce-dev__*`, so an agent can't accidentally mutate the wrong environment.
- **Per-environment safety.** Pair prod with `COALESCE_MCP_READ_ONLY=true` to hide every write tool on that server while leaving dev fully writable.
- **No per-call profile juggling.** Each server is pinned at startup.

Skip this pattern if you only use one environment - a single registration is simpler. For 2–3 environments it's worth the extra config; beyond that, each server is a separate Node process, so consider whether you actually need them all loaded at once.

</details>

### Using the COA CLI tools

COA is bundled - no extra install. Usage notes:

- **Local commands** (`coa_doctor`, `coa_validate`, `coa_dry_run_create`, `coa_dry_run_run`, `coa_create`, `coa_run`, `coa_plan`) need a COA project directory (one that contains `data.yml`). Pass the path via the `projectPath` tool argument.
- **Cloud commands** (`coa_list_environments`, `coa_list_environment_nodes`, `coa_list_runs`, `coa_deploy`, `coa_refresh`) read credentials from `~/.coa/config` - the same file the MCP uses. Populate it once and both surfaces agree.
- **Profile resolution.** Cloud tools accept an optional `profile` arg. When omitted, they fall back to `COALESCE_PROFILE`, then to COA's own `[default]` - so you don't have to pass it on every call.
- **Warehouse-touching commands** (`coa_create`, `coa_run`) need a valid `workspaces.yml` in the project root with storage-location mappings. Preflight catches a missing file before execution.

### Safety model

Three layers prevent destructive surprises. See [docs/safety-model.md](docs/safety-model.md) for the full breakdown (tool annotations, read-only mode, explicit confirmation, COA preflight validation).

- **Tool annotations** - every tool carries MCP `readOnlyHint` / `destructiveHint` / `idempotentHint`. The ⚠️ marker in [Tools](#tools) marks `destructiveHint: true` tools.
- **`COALESCE_MCP_READ_ONLY=true`** hides all write/mutation tools at server startup. Use it for audits, agent sandboxes, or pairing with a prod profile.
- **Explicit confirmation** on destructive ops - `delete_*`, `propagate_column_change`, `cancel_run`, `clear_data_cache`, `coa_create`, `coa_run`, `coa_deploy`, `coa_refresh` all require `confirmed: true`.

### More configuration

- [Prerelease channel](docs/prerelease.md) - point `npx` at `@alpha` for preview builds.
- [Diagnosing setup](docs/diagnosing-setup.md) - the `diagnose_setup` probe and `/coalesce-setup` MCP prompt.

---

## Skills

**Skills are editable markdown that shapes how the agent reasons about your Coalesce project.** Ship your team's naming conventions, grain definitions, and layering patterns as context - every agent on the server instantly picks them up. No fine-tuning, no prompt engineering, just markdown you edit and commit.

Set `COALESCE_MCP_SKILLS_DIR` to make skills editable on disk. Each skill resolves to default content, user-augmented content, or a full user override - see [docs/context-skills.md](docs/context-skills.md) for the resolution order and customization walkthrough.

**24 skills, grouped into 6 families:**

|     | Family | Skills | Covers |
| --- | ------ | :----: | ------ |
| <picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/book-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/book-light.png"><img src="docs/icons/book-light.png" width="20" height="20" alt="book"></picture> | **Foundations** | 7 | Core concepts, tool usage, ID discovery, storage mappings, ecosystem scope |
| <picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/file-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/file-light.png"><img src="docs/icons/file-light.png" width="20" height="20" alt="file"></picture> | **SQL platform rules** | 3 | Per-warehouse conventions for node SQL (Snowflake, Databricks, BigQuery) |
| <picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/git-commit-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/git-commit-light.png"><img src="docs/icons/git-commit-light.png" width="20" height="20" alt="git-commit"></picture> | **Node editing & payloads** | 6 | Decision tree, payload shape, hydrated metadata, joins, config completion |
| <picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/repo-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/repo-light.png"><img src="docs/icons/repo-light.png" width="20" height="20" alt="repo"></picture> | **Node type selection** | 2 | When to use Stage/Work vs Dimension/Fact vs specialized node types |
| <picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/workflow-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/workflow-light.png"><img src="docs/icons/workflow-light.png" width="20" height="20" alt="workflow"></picture> | **Pipeline workflows** | 4 | End-to-end pipeline building, intent, review, and workshop patterns |
| <picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/beaker-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/beaker-light.png"><img src="docs/icons/beaker-light.png" width="20" height="20" alt="beaker"></picture> | **Run operations** | 2 | Starting, retrying, polling, diagnosing, and canceling runs |

<details>

<summary>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/icons/book-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/icons/book-light.png">
    <img src="docs/icons/book-light.png" width="20" height="20" alt="book">
  </picture>
  &nbsp;<b>Foundations</b> &mdash; the shared context every agent starts with
</summary>

- **`overview`** - General Coalesce concepts, response guidelines, and operational constraints
- **`tool-usage`** - Best practices for tool batching, parallelization, and SQL conversion
- **`id-discovery`** - Resolving project, workspace, environment, job, run, node, and org IDs
- **`storage-mappings`** - Storage location concepts, `{{ ref() }}` syntax, and reference patterns
- **`ecosystem-boundaries`** - Scope of this MCP vs adjacent data-engineering MCPs (Snowflake, Fivetran, dbt, Catalog)
- **`data-engineering-principles`** - Node type selection, layered architecture, methodology detection, materialization strategies
- **`sql-platform-selection`** - Determining the active SQL platform from project metadata

</details>

<details>

<summary>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/icons/file-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/icons/file-light.png">
    <img src="docs/icons/file-light.png" width="20" height="20" alt="file">
  </picture>
  &nbsp;<b>SQL platform rules</b> &mdash; per-warehouse conventions for node SQL
</summary>

- **`sql-snowflake`** - Snowflake-specific SQL conventions for node SQL
- **`sql-databricks`** - Databricks-specific SQL conventions for node SQL
- **`sql-bigquery`** - BigQuery-specific SQL conventions for node SQL

</details>

<details>

<summary>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/icons/git-commit-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/icons/git-commit-light.png">
    <img src="docs/icons/git-commit-light.png" width="20" height="20" alt="git-commit">
  </picture>
  &nbsp;<b>Node editing &amp; payloads</b> &mdash; how the agent reasons about node bodies
</summary>

- **`node-creation-decision-tree`** - Choosing between predecessor-based creation, updates, and full replacements
- **`node-payloads`** - Working with workspace node bodies, metadata, config, and array-replacement risks
- **`hydrated-metadata`** - Coalesce hydrated metadata structures for advanced node payload editing
- **`intelligent-node-configuration`** - How intelligent config completion works, schema resolution, automatic field detection
- **`node-operations`** - Editing existing nodes: joins, columns, config fields, and SQL-to-graph conversion
- **`aggregation-patterns`** - JOIN ON generation, GROUP BY detection, and join-to-aggregation conversion

</details>

<details>

<summary>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/icons/repo-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/icons/repo-light.png">
    <img src="docs/icons/repo-light.png" width="20" height="20" alt="repo">
  </picture>
  &nbsp;<b>Node type selection</b> &mdash; picking the right node type for each step
</summary>

- **`node-type-selection-guide`** - When to use each Coalesce node type (Stage/Work vs Dimension/Fact vs specialized)
- **`node-type-corpus`** - Node type discovery, corpus search, and metadata patterns

</details>

<details>

<summary>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/icons/workflow-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/icons/workflow-light.png">
    <img src="docs/icons/workflow-light.png" width="20" height="20" alt="workflow">
  </picture>
  &nbsp;<b>Pipeline workflows</b> &mdash; end-to-end pipeline building
</summary>

- **`pipeline-workflows`** - Building pipelines end-to-end: node type selection, multi-node sequences, execution
- **`intent-pipeline-guide`** - Using `build_pipeline_from_intent` to create pipelines from natural language
- **`pipeline-review-guide`** - Using `review_pipeline` for pipeline analysis and optimization
- **`pipeline-workshop-guide`** - Using pipeline workshop tools for iterative, conversational pipeline building

</details>

<details>

<summary>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/icons/beaker-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/icons/beaker-light.png">
    <img src="docs/icons/beaker-light.png" width="20" height="20" alt="beaker">
  </picture>
  &nbsp;<b>Run operations</b> &mdash; starting, retrying, diagnosing runs
</summary>

- **`run-operations`** - Starting, retrying, polling, diagnosing, and canceling Coalesce runs
- **`run-diagnostics-guide`** - Using `diagnose_run_failure` to analyze failed runs and determine fixes

</details>

> [!TIP]
> **Companion resources:** 10 topics under `coalesce://coa/describe/*` surface the bundled COA CLI's self-describing documentation, version-pinned to the shipping CLI. Topics: `overview`, `commands`, `selectors`, `schemas`, `workflow`, `structure`, `concepts`, `sql-format`, `node-types`, `config`. Use the `coa_describe` tool for parameterized variants.

---

## Tools

⚠️ = Destructive (requires `confirmed: true`). 🧰 = Runs bundled `coa` CLI.

<!-- start of tool reference -->

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/project-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/project-light.png"><img src="docs/icons/project-light.png" width="20" height="20" alt="project"></picture> Discovery</summary>

**Environments, workspaces, projects**

- **`list_environments`** - List all available environments
- **`get_environment`** - Get details of a specific environment
- **`list_workspaces`** - List all workspaces
- **`get_workspace`** - Get details of a specific workspace
- **`list_projects`** - List all projects
- **`get_project`** - Get project details

**Nodes**

- **`list_environment_nodes`** - List nodes in an environment
- **`list_workspace_nodes`** - List nodes in a workspace
- **`get_environment_node`** - Get a specific environment node
- **`get_workspace_node`** - Get a specific workspace node
- **`analyze_workspace_patterns`** - Detect package adoption, pipeline layers, methodology, and generate recommendations
- **`list_workspace_node_types`** - List distinct node types observed in current workspace nodes

**Jobs, subgraphs, runs**

- **`list_environment_jobs`** - List all jobs for an environment
- **`get_environment_job`** - Get details of a specific job
- **`list_workspace_subgraphs`** - List subgraphs in a workspace
- **`get_workspace_subgraph`** - Get details of a specific subgraph
- **`list_runs`** - List runs with optional filters
- **`get_run`** - Get details of a specific run
- **`get_run_results`** - Get results of a completed run
- **`get_run_details`** - Run metadata plus results in one call

**Search**

- **`search_workspace_content`** - Search node SQL, column names, descriptions, and config values
- **`audit_documentation_coverage`** - Scan all workspace nodes/columns for missing descriptions

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/workflow-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/workflow-light.png"><img src="docs/icons/workflow-light.png" width="20" height="20" alt="workflow"></picture> Pipeline building</summary>

**Plan & build**

- **`plan_pipeline`** - Plan a pipeline from SQL or a natural-language goal without mutating the workspace; ranks best-fit node types from the local repo
- **`create_pipeline_from_plan`** - Execute an approved pipeline plan using predecessor-based creation
- **`create_pipeline_from_sql`** - Plan and create a pipeline directly from SQL
- **`build_pipeline_from_intent`** - Build a pipeline from a natural language goal with automatic entity resolution and node type selection
- **`review_pipeline`** - Analyze an existing pipeline for redundant nodes, missing joins, layer violations, naming issues, and optimization opportunities
- **`parse_sql_structure`** - Parse a SQL statement into structural components (CTEs, source tables, projected columns) without touching the workspace
- **`select_pipeline_node_type`** - Rank and select the best Coalesce node type for a pipeline step

**Workshop (iterative, conversational)**

- **`pipeline_workshop_open`** - Open an iterative pipeline builder session with workspace context pre-loaded
- **`pipeline_workshop_instruct`** - Send a natural language instruction to modify the current workshop plan
- **`get_pipeline_workshop_status`** - Get the current state of a workshop session
- **`pipeline_workshop_close`** - Close a workshop session and release resources

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/git-commit-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/git-commit-light.png"><img src="docs/icons/git-commit-light.png" width="20" height="20" alt="git-commit"></picture> Node editing</summary>

**Create**

- **`create_workspace_node_from_scratch`** - Create a workspace node with no predecessors
- **`create_workspace_node_from_predecessor`** - Create a node from predecessor nodes with column coverage verification
- **`create_node_from_external_schema`** - Create a workspace node whose columns match an existing warehouse table or external schema

**Update**

- **`set_workspace_node`** - Replace a workspace node with a full body
- **`update_workspace_node`** - Safely update selected fields of a workspace node
- **`replace_workspace_node_columns`** - Replace `metadata.columns` wholesale
- **`delete_workspace_node`** - Delete a node from a workspace ⚠️

**Configure**

- **`complete_node_configuration`** - Intelligently complete a node's configuration by analyzing context
- **`apply_join_condition`** - Auto-generate and write a FROM/JOIN/ON clause for a multi-predecessor node
- **`convert_join_to_aggregation`** - Convert a join-style node into an aggregated fact-style node

**Subgraphs & jobs**

- **`create_workspace_subgraph`** - Create a subgraph to group nodes visually
- **`update_workspace_subgraph`** - Update a subgraph's name and node membership
- **`delete_workspace_subgraph`** - Delete a subgraph (nodes are NOT deleted) ⚠️
- **`create_workspace_job`** - Create a job in a workspace with node include/exclude selectors
- **`update_workspace_job`** - Update a job's name and node selectors
- **`delete_workspace_job`** - Delete a job ⚠️

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/beaker-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/beaker-light.png"><img src="docs/icons/beaker-light.png" width="20" height="20" alt="beaker"></picture> Runs & execution</summary>

- **`start_run`** - Start a new run; requires Snowflake auth
- **`run_and_wait`** - Start a run and poll until completion
- **`run_status`** - Check status of a running job
- **`retry_run`** - Retry a failed run
- **`retry_and_wait`** - Retry a failed run and poll until completion
- **`cancel_run`** - Cancel a running job ⚠️
- **`diagnose_run_failure`** - Classify errors, surface root cause, suggest actionable fixes
- **`get_environment_overview`** - Environment details with full node list
- **`get_environment_health`** - Dashboard: node counts, run statuses, failed runs in last 24h, stale nodes, dependency health

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/git-branch-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/git-branch-light.png"><img src="docs/icons/git-branch-light.png" width="20" height="20" alt="git-branch"></picture> Lineage & impact</summary>

- **`get_upstream_nodes`** - Walk the full upstream dependency graph for a node
- **`get_downstream_nodes`** - Walk the full downstream dependency graph for a node
- **`get_column_lineage`** - Trace a column through the pipeline upstream and downstream
- **`analyze_impact`** - Downstream impact of changing a node or specific column - impacted counts, grouped by depth, and critical path
- **`propagate_column_change`** - Update all downstream columns after a column rename or data type change ⚠️

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/repo-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/repo-light.png"><img src="docs/icons/repo-light.png" width="20" height="20" alt="repo"></picture> Repo-backed node types</summary>

- **`list_repo_packages`** - List package aliases and enabled node-type coverage from a committed Coalesce repo
- **`list_repo_node_types`** - List exact resolvable committed node-type identifiers from `nodeTypes/`
- **`get_repo_node_type_definition`** - Resolve one node type and return its outer definition plus parsed `nodeMetadataSpec`
- **`generate_set_workspace_node_template`** - Generate a YAML-friendly `set_workspace_node` body template
- **`search_node_type_variants`** - Search the committed node-type corpus by normalized family, package, primitive, or support status
- **`get_node_type_variant`** - Load one exact node-type corpus variant by variant key
- **`generate_set_workspace_node_template_from_variant`** - Generate a template from a committed corpus variant

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/tools-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/tools-light.png"><img src="docs/icons/tools-light.png" width="20" height="20" alt="tools"></picture> COA CLI</summary>

All local tools accept a `projectPath` argument and validate that it contains `data.yml` before shelling out. Destructive tools run preflight validation; see [Safety model](docs/safety-model.md).

**Read-only, local**

- 🧰 **`coa_doctor`** - Check config, credentials, and warehouse connectivity
- 🧰 **`coa_validate`** - Validate YAML schemas and scan for configuration problems
- 🧰 **`coa_list_project_nodes`** - List all nodes defined in a local project (pre-deploy)
- 🧰 **`coa_dry_run_create`** - Preview DDL without executing (does **not** validate columns/types exist in warehouse)
- 🧰 **`coa_dry_run_run`** - Preview DML without executing (same caveat)

**Read-only, cloud**

- 🧰 **`coa_list_environments`** - List deployment environments
- 🧰 **`coa_list_environment_nodes`** - List deployed nodes in an environment
- 🧰 **`coa_list_runs`** - List pipeline runs in a cloud environment

**Describe**

- 🧰 **`coa_describe`** - Fetch a section of COA's self-describing documentation by topic + optional subtopic

**Write & deploy**

- 🧰 **`coa_plan`** - Generate a deployment plan JSON by diffing local project against a cloud environment (non-destructive)
- 🧰 **`coa_create`** - Run DDL (CREATE/REPLACE) against the warehouse for selected nodes ⚠️
- 🧰 **`coa_run`** - Run DML (INSERT/MERGE) to populate selected nodes ⚠️
- 🧰 **`coa_deploy`** - Apply a plan JSON to a cloud environment ⚠️
- 🧰 **`coa_refresh`** - Run DML for selected nodes in an already-deployed environment (no local project required) ⚠️

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/file-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/file-light.png"><img src="docs/icons/file-light.png" width="20" height="20" alt="file"></picture> Projects, environments & git accounts</summary>

- **`create_environment`** - Create a new environment within a project
- **`delete_environment`** - Delete an environment ⚠️
- **`create_project`** - Create a new project
- **`update_project`** - Update a project
- **`delete_project`** - Delete a project ⚠️
- **`list_git_accounts`** - List all git accounts
- **`get_git_account`** - Get git account details
- **`create_git_account`** - Create a new git account
- **`update_git_account`** - Update a git account
- **`delete_git_account`** - Delete a git account ⚠️

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/shield-lock-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/shield-lock-light.png"><img src="docs/icons/shield-lock-light.png" width="20" height="20" alt="shield-lock"></picture> Users & roles</summary>

- **`list_org_users`** - List all organization users
- **`get_user_roles`** - Get roles for a specific user
- **`list_user_roles`** - List all user roles
- **`set_org_role`** - Set organization role for a user
- **`set_project_role`** - Set project role for a user
- **`delete_project_role`** - Remove project role from a user ⚠️
- **`set_env_role`** - Set environment role for a user
- **`delete_env_role`** - Remove environment role from a user ⚠️

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/book-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/book-light.png"><img src="docs/icons/book-light.png" width="20" height="20" alt="book"></picture> Cache, skills & setup</summary>

**Cache snapshots**

- **`cache_workspace_nodes`** - Fetch every page of workspace nodes, write a full snapshot, and return cache metadata
- **`cache_environment_nodes`** - Fetch every page of environment nodes, write a full snapshot
- **`cache_runs`** - Fetch every page of run results, write a full snapshot
- **`cache_org_users`** - Fetch every page of organization users, write a full snapshot
- **`clear_data_cache`** - Delete all cached snapshots, auto-cached responses, and plan summaries ⚠️

**Skills & setup**

- **`personalize_skills`** - Export bundled skill files to a local directory for customization
- **`diagnose_setup`** - Stateless probe reporting configured setup pieces; pairs with the `/coalesce-setup` MCP prompt

</details>

<!-- end of tool reference -->

---

## Design notes

- **SQL override is disallowed.** Nodes are built via YAML/config (columns, transforms, join conditions), not raw SQL. Template generation strips `overrideSQLToggle`, and write helpers reject `overrideSQL` fields.
- **Caching.** Large responses are auto-cached to disk. Use `cache_workspace_nodes` and siblings when you want a reusable snapshot. Configure the threshold with `COALESCE_MCP_AUTO_CACHE_MAX_BYTES`.
- **Repo-backed tools.** Set `COALESCE_REPO_PATH` (or add `repoPath=` to your ~/.coa/config profile) to your local Coalesce repo root (containing `nodeTypes/`, `nodes/`, `packages/`), or pass `repoPath` on individual tool calls. The server does not clone repos or install packages.
- **COA CLI versioning.** The bundled COA CLI is pinned to an exact alpha version - *not* a floating `@next` tag. Every release of this MCP ships with a known-good COA build. Changelog and bump policy: [docs/RELEASES.md](docs/RELEASES.md).
- **COA describe cache.** COA describe output is cached under `~/.cache/coalesce-transform-mcp/coa-describe/<coa-version>/` after first access. Cache is version-keyed - upgrading the MCP automatically invalidates stale content.

---

## Links

| | Resource | |
| :-: | :-- | :-- |
| 📘 | [Coalesce Docs](https://docs.coalesce.io/docs) | Product documentation |
| 🔌 | [Coalesce API Docs](https://docs.coalesce.io/docs/api/authentication) | REST API reference |
| 🧰 | [Coalesce CLI (`coa`)](https://docs.coalesce.io/docs/cli) | Bundled CLI docs |
| 🛒 | [Coalesce Marketplace](https://docs.coalesce.io/docs/marketplace) | Node type packages |
| 🔗 | [Model Context Protocol](https://modelcontextprotocol.io/) | MCP spec & ecosystem |

---

## Contributing

Issues and PRs welcome. Before opening a PR, please run the preflight checks described in [docs/RELEASES.md](docs/RELEASES.md).

- 🐛 **Bug reports** - [open an issue](https://github.com/coalesceio/coalesce-transform-mcp/issues/new?labels=bug)
- 💡 **Feature requests** - [start a discussion](https://github.com/coalesceio/coalesce-transform-mcp/discussions)

## License

[MIT](LICENSE) © Coalesce - built on top of the open [Model Context Protocol](https://modelcontextprotocol.io/).
