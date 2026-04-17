# Install in Windsurf

Windsurf has no deeplink install yet - paste the config manually.

## File location

`~/.codeium/windsurf/mcp_config.json`

## Config

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

## Credential notes

Windsurf does **not** expand `${VAR}` - paste the literal token, or drop the `env` block and use `~/.coa/config` (see [Credentials](../../README.md#credentials)).

Restart Windsurf after editing the config.
