# Install in VS Code

## Click-to-install

[<img src="https://img.shields.io/badge/VS_Code-Install_MCP-007ACC?style=flat-square&logo=visualstudiocode" alt="Install in VS Code">](https://insiders.vscode.dev/redirect?url=vscode%3Amcp%2Finstall%3F%257B%2522name%2522%253A%2522coalesce-transform%2522%252C%2522command%2522%253A%2522npx%2522%252C%2522args%2522%253A%255B%2522coalesce-transform-mcp%2522%255D%257D)

The button opens the VS Code MCP install redirect with the command and args pre-filled.

## Manual install

Follow the [VS Code MCP install guide](https://code.visualstudio.com/docs/copilot/chat/mcp-servers#_add-an-mcp-server) and use this config:

```json
{
  "name": "coalesce-transform",
  "command": "npx",
  "args": ["coalesce-transform-mcp"]
}
```

Add a `COALESCE_ACCESS_TOKEN` via VS Code's secret input prompt, or drop the token and use `~/.coa/config` (see [Credentials](../../README.md#credentials)).

Reload the VS Code window after install so the MCP server registers.
