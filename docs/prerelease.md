# Prerelease channel

Prerelease builds publish to `@alpha` while `@latest` stays on stable. Point `npx` at the alpha channel:

```json
{
  "coalesce-transform": {
    "command": "npx",
    "args": ["coalesce-transform-mcp@alpha"]
  }
}
```

Restart your MCP client after changing the config so `npx` re-resolves.

## Pinning an exact version

To pin an exact prerelease rather than whatever `@alpha` resolves to today, replace `@alpha` with the full version, e.g. `coalesce-transform-mcp@0.5.0-alpha.2`.

If `npx` serves a stale cached copy when `@alpha` advances, force a fresh fetch with `npx -y coalesce-transform-mcp@alpha`.

## Running alpha and stable side-by-side

Register both under different server names:

```json
{
  "mcpServers": {
    "coalesce-transform": {
      "command": "npx",
      "args": ["coalesce-transform-mcp"]
    },
    "coalesce-transform-alpha": {
      "command": "npx",
      "args": ["coalesce-transform-mcp@alpha"]
    }
  }
}
```

Agents will see `coalesce-transform__*` and `coalesce-transform-alpha__*` tools as separate namespaces.
