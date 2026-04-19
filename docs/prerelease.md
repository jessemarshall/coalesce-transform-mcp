# Prerelease channel

Prereleases are cut from the `preview` branch and published to the `@preview` npm dist-tag while `@latest` stays on stable. Point `npx` at the preview channel:

```json
{
  "coalesce-transform": {
    "command": "npx",
    "args": ["coalesce-transform-mcp@preview"]
  }
}
```

Restart your MCP client after changing the config so `npx` re-resolves.

## Pinning an exact version

To pin an exact prerelease rather than whatever `@preview` resolves to today, replace `@preview` with the full version, e.g. `coalesce-transform-mcp@0.5.0-preview.2`.

If `npx` serves a stale cached copy when `@preview` advances, force a fresh fetch with `npx -y coalesce-transform-mcp@preview`.

## Running preview and stable side-by-side

Register both under different server names:

```json
{
  "mcpServers": {
    "coalesce-transform": {
      "command": "npx",
      "args": ["coalesce-transform-mcp"]
    },
    "coalesce-transform-preview": {
      "command": "npx",
      "args": ["coalesce-transform-mcp@preview"]
    }
  }
}
```

Agents will see `coalesce-transform__*` and `coalesce-transform-preview__*` tools as separate namespaces.
