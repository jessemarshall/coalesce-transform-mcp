# Diagnosing setup

`diagnose_setup` is a stateless probe that reports which first-time-setup pieces are configured:

- Access token
- Snowflake credentials
- `~/.coa/config` profile
- Local repo path
- A best-effort `coa doctor` check

## What it returns

A structured report with:

- **Per-field `source` markers** - `env`, `profile:<name>`, or `missing` - so you can tell at a glance whether a value came from an env var, the COA config, or isn't set
- **Ordered `nextSteps`** - the exact actions remaining, in priority order

## Running it

Call `diagnose_setup` any time something isn't working the way you expect. It pairs with the `/coalesce-setup` MCP prompt, which walks a user through any remaining gaps.

Typical first-run output before credentials are wired:

```json
{
  "accessToken": { "source": "missing" },
  "snowflake": { "source": "missing" },
  "coaProfile": { "source": "profile:default", "exists": true },
  "repoPath": { "source": "profile:default", "value": "/Users/you/path/to/repo" },
  "nextSteps": [
    "Set COALESCE_ACCESS_TOKEN in your MCP config or add `token=` to your ~/.coa/config profile",
    "Add snowflake* fields to ~/.coa/config or set SNOWFLAKE_* env vars",
    "Restart your MCP client to pick up changes"
  ]
}
```

## Related commands

- `coa_doctor` - full connectivity check (same output `diagnose_setup` uses internally when it can)
- `/coalesce-setup` MCP prompt - interactive walkthrough built on top of `diagnose_setup`
