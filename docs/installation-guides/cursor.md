# Install in Cursor

## Click-to-install

[<img src="https://cursor.com/deeplink/mcp-install-dark.svg" alt="Install in Cursor">](https://cursor.com/install-mcp?name=coalesce-transform&config=eyJjb21tYW5kIjoibnB4IiwiYXJncyI6WyJjb2FsZXNjZS10cmFuc2Zvcm0tbWNwIl19)

The button opens Cursor's "Add MCP server" flow with the command and args pre-filled.

## Manual install

Paste into `.cursor/mcp.json` in your project root (or `~/.cursor/mcp.json` for a global install):

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

Cursor does **not** expand `${VAR}` - paste the literal token, or drop the `env` block and use `~/.coa/config` (see [Credentials](../../README.md#credentials)) so no secret sits in a git-tracked file.

After editing, fully quit and relaunch Cursor so the MCP server reloads.
