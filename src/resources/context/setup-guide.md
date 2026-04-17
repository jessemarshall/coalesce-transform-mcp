# Coalesce MCP Setup Guide

Load this when the user is setting up the Coalesce MCP for the first time, says something like "help me get set up," or when any tool returns an error that points at missing credentials, missing repo path, or a misconfigured profile.

The `/coalesce-setup` slash command is a thin entrypoint to this same flow — behavior should be identical whether the user invoked the slash or asked conversationally.

## Principle: probe, don't interrogate

Always start with `diagnose_setup`. It is stateless, non-throwing, and returns a structured report. Use the report to drive the conversation — do NOT ask the user a checklist of questions before you know what's actually missing.

`diagnose_setup` returns:

```
{
  coaConfig:       { status, path?, activeProfile, ... },
  accessToken:     { status, source?, ... },
  snowflakeCreds:  { status, authType?, sources?, ... },
  repoPath:        { status, path?, isCoaProject? },
  coaDoctor:       { status, ... },
  projectWarnings: PreflightIssue[],
  nextSteps:       string[],
  ready:           boolean,
}
```

## The flow

1. **Call `diagnose_setup`.** No arguments. Report summary lines back to the user:
   - `accessToken.status` — ok / missing / invalid / error
   - `snowflakeCreds.status` — ok / missing / invalid
   - `repoPath.status` — ok / missing / invalid (note: optional)
   - `coaDoctor.status` — ok / skipped / failed / error

2. **If `ready: true`**, celebrate briefly and offer concrete next steps:
   - `list_workspaces` to pick a workspace
   - `coa_bootstrap_workspaces` if they cloned a fresh project and need `workspaces.yml`
   - `pipeline_workshop_open` to build a pipeline conversationally

3. **If `ready: false`**, walk through `nextSteps` one item at a time. For each item:
   - Translate the raw step into 1–2 plain-language sentences.
   - Present both configuration paths (env vs profile) and let the user choose.
   - Wait for them to make the change, then re-run `diagnose_setup` to confirm before moving to the next item.

## Configuration path — env vs profile

Every secret can come from either:

- **MCP client env block** — set in the MCP client's config (Claude Desktop, Cursor, etc.). Fast, isolated, wins over the profile when both are set.
- **`~/.coa/config` profile** — INI file, supports multiple named profiles via `COALESCE_PROFILE`. Shared with the `coa` CLI.

When both are set, **env wins**. Point this out when the user is debugging a "I updated the profile but it didn't pick up" problem.

Key fields per category:

| Category | Env var | Profile key |
|---|---|---|
| Access token | `COALESCE_ACCESS_TOKEN` | `token` |
| Snowflake account | `SNOWFLAKE_ACCOUNT` | `snowflakeAccount` |
| Snowflake user | `SNOWFLAKE_USERNAME` | `snowflakeUsername` |
| Snowflake auth | `SNOWFLAKE_KEY_PAIR_KEY` (path) + `SNOWFLAKE_KEY_PAIR_PASS`, or `SNOWFLAKE_PAT` | `snowflakeKeyPairKey` + `snowflakeKeyPairPass` (PAT is env-only) |
| Snowflake warehouse/role | `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE` | `snowflakeWarehouse`, `snowflakeRole` |
| Repo path (optional) | `COALESCE_REPO_PATH` | `repoPath` |

## Common scenarios

**Access token rejected (401/403).** `diagnose_setup` tells you which source provided the token — do not guess. Ask the user to regenerate the token in the Coalesce app (Deploy → User Settings) and update whichever source was reported.

**Profile misconfigured.** If `coaConfig.status === "ok"` but `profileExists: false`, the active profile name (usually from `COALESCE_PROFILE`) doesn't match any section in `~/.coa/config`. Show the `availableProfiles` list and let the user pick.

**Parse error on `~/.coa/config`.** Offer the env-var escape hatch while they fix the INI file. Do not try to parse the file yourself.

**No repo path.** This is optional — skip it if the user says they'll only be using the REST-based tools. They need it for `coa_*` local tools and `create_workspace_node_from_*` with automatic config completion.

**`workspaces.yml` missing.** `diagnose_setup` inlines a ready-to-paste template seeded from `locations.yml`. Offer to open the file for the user rather than regenerating the template from memory.

**Snowflake credentials missing.** Check whether the user is using key-pair auth or a PAT — these are mutually exclusive paths. If they don't know, key-pair is the default for most Coalesce deployments.

## What NOT to do

- Do not echo secrets back to the user. Acknowledge receipt only.
- Do not dump the raw `nextSteps` array. It's ordered and plain-language, but it's meant to inform your conversation, not replace it.
- Do not suggest "fixes" for items that are already `ok`. Only act on items where `status !== "ok"`.
- Do not skip re-running `diagnose_setup` between steps. Trust the probe, not assumptions about what the user did.
- Do not help with Coalesce app onboarding (creating a user, setting up a project in the Coalesce UI). This guide is for local MCP client setup only.
