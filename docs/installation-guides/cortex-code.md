# Install in Snowflake Cortex Code (CoCo)

[Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli) is Snowflake's AI coding CLI. It acts as an MCP client - like Cursor or Claude Code - so it can drive `coalesce-transform-mcp` directly from your terminal while sitting inside Snowflake's auth and role model.

Pairing Cortex Code with this MCP is one of the tightest Coalesce-on-Snowflake loops available: the same session can plan a pipeline, create nodes, run DML, and sanity-check the resulting data in the warehouse - no context switching.

## Prerequisites

- Snowflake user with the `SNOWFLAKE.CORTEX_USER` database role
- Network access to your Snowflake account
- bash, zsh, or fish shell (macOS / Linux / WSL) - or PowerShell on Windows

## 1. Install the Cortex Code CLI

**macOS / Linux / WSL:**

```bash
curl -LsS https://ai.snowflake.com/static/cc-scripts/install.sh | sh
```

**Windows (PowerShell):**

```powershell
irm https://ai.snowflake.com/static/cc-scripts/install.ps1 | iex
```

## 2. Configure your Snowflake connection

Run the interactive setup wizard:

```bash
cortex
```

It walks you through picking an existing connection from `~/.snowflake/connections.toml` or creating a new one with your Snowflake account details.

## 3. Register `coalesce-transform-mcp`

One-liner:

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

Drop the `env` block entirely if you're using `~/.coa/config` - Cortex Code and Coalesce will both pick the token up from there.

## 4. Start a session

```bash
cortex
```

From inside the TUI, your agent now sees the `coalesce-transform` tools alongside Cortex Code's native Snowflake tools. Example prompts that just work:

- *"Plan a pipeline that stages the raw `CUSTOMERS` table and aggregates by region."*
- *"Create the stage node, then run it and show me the row count."*
- *"What columns in the `ORDERS` stage node are missing descriptions?"*
- *"Diagnose the last failed run and propose a fix."*

Because Cortex Code already has warehouse context, it routes data questions (row counts, column samples, `CURRENT_ROLE()`) to Snowflake and node/pipeline questions to this server automatically.

## Troubleshooting

- **`cortex` not found after install** - the installer drops the binary in `~/.snowflake/bin`. Add it to your `PATH` or re-run with the absolute path.
- **MCP server not showing up** - run `cortex mcp list` to confirm registration, then restart your Cortex Code session.
- **Auth errors** - verify the Snowflake connection works outside MCP first (`cortex sql "select current_role()"`), then restart.
- **Coalesce auth** - if Coalesce tools fail, run `diagnose_setup` from inside Cortex Code to see whether the token came from env, profile, or is missing.
