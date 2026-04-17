# Install in Claude Code (CLI)

## Quick install

```bash
claude mcp add coalesce-transform -- npx coalesce-transform-mcp
```

Pass env vars inline if you need them:

```bash
claude mcp add coalesce-transform \
  --env COALESCE_ACCESS_TOKEN=$COALESCE_ACCESS_TOKEN \
  -- npx coalesce-transform-mcp
```

## Manual install

Paste into `.mcp.json` in your project root (or `~/.claude.json` for a global install):

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

## Credential notes

Claude Code **does** expand `${VAR}` from your shell env at load time - `.mcp.json` can stay safely committed to git with variable references. Omit the `env` block entirely if you're using `~/.coa/config` (see [Credentials](../../README.md#credentials)).

Restart Claude Code after editing so `npx` re-resolves.
