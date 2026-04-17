# Install in Claude Desktop

Claude Desktop has no deeplink install yet — paste the config manually.

## File location

- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

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

Claude Desktop does **not** expand `${VAR}` — paste the literal token, or drop the `env` block and use `~/.coa/config` (see [Credentials](../../README.md#credentials)) so nothing sensitive lives in this file.

Fully quit Claude Desktop (`Cmd+Q` on macOS, from the tray on Windows) and relaunch it after editing the config.
