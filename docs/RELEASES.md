# Release Process

## One-time setup for automated releases

1. Go to https://www.npmjs.com/settings/your-username/tokens
2. Create a new **Automation** token
3. Go to your GitHub repo Settings > Secrets and variables > Actions
4. Add a new repository secret: name `NPM_TOKEN`, value is the token from step 2

## Release channels

Every release lives on one of two npm dist-tags:

| Dist-tag | Version shape | Who gets it | MCP Registry | GitHub Release |
| -------- | ------------- | ----------- | ------------ | -------------- |
| `latest` | `0.5.0`, `0.5.1`, … | `npm install coalesce-transform-mcp` | Published | Full release |
| `alpha` | `0.5.0-alpha.0`, `0.5.0-alpha.1`, … | `npm install coalesce-transform-mcp@alpha` | **Skipped** | Marked **prerelease** |

The [release workflow](.github/workflows/release.yml) reads `package.json` on every tag push and routes automatically — you never pass `--tag alpha` by hand.

## Standard release flow (develop → alpha → main)

Three branches:

- **`develop`** — feature work lands here via PRs from feature branches.
- **`alpha`** — staging for alpha testing. Merged from `develop` via PR. Alpha tags (`X.Y.Z-alpha.N`) are cut here.
- **`main`** — stable. Merged from `alpha` via PR when an alpha series is ready to promote. Stable tags (`X.Y.Z`) are cut here.

Only `main` moves when stable ships; `alpha` carries every alpha in between.

### 1. Iterate on `develop` and promote to `alpha`

```bash
git checkout develop && git pull
# (merge feature branches into develop via PR as usual)
gh pr create --base alpha --head develop \
  --title "0.5 alpha: <what's new>" --body "<summary>"
gh pr merge <pr-number> --merge --delete-branch=false
```

`develop` stays open — you don't delete it when the PR merges.

### 2. First alpha of a new version

On the **alpha** branch (not main — the alpha series owns the prerelease commits):

```bash
git checkout alpha && git pull origin alpha

# Pre-flight: the preversion hook runs `npm audit --omit=dev`, which
# fails on any moderate+ vulnerability. If audit reports a new issue
# (common for transitive deps of @modelcontextprotocol/sdk), fix first
# and commit the lockfile bump BEFORE running npm version:
npm audit --omit=dev || (npm audit fix && git add package-lock.json \
  && git commit -m "chore: bump <pkg> transitive dep (<advisory>)")

# COALESCE_* env vars must be unset for the preversion run — stale
# cache/repo pointers break vitest during sync. Wrap in a subshell:
(unset COALESCE_ACCESS_TOKEN COALESCE_BASE_URL COALESCE_REPO_PATH \
        COALESCE_CACHE_DIR COALESCE_PROFILE; \
 npm version preminor --preid=alpha)
# 0.4.8 → 0.5.0-alpha.0

git push origin alpha --tags
```

(`prepatch --preid=alpha` for a patch-level series, `premajor --preid=alpha` for a major.)

The release workflow detects `-alpha.*`, publishes to the `@alpha` npm dist-tag, skips the MCP Registry, and cuts a prerelease GitHub Release. Users install with `npm install coalesce-transform-mcp@alpha`.

Watch the workflow synchronously with `gh run watch <id> --exit-status` — exits non-zero on failure so you know immediately.

### 3. Subsequent alphas in the same series

More fixes on `develop` → PR → merge to `alpha` → on `alpha`:

```bash
(unset COALESCE_ACCESS_TOKEN COALESCE_BASE_URL COALESCE_REPO_PATH \
        COALESCE_CACHE_DIR COALESCE_PROFILE; \
 npm version prerelease --preid=alpha)
git push origin alpha --tags
```

That bumps `0.5.0-alpha.0` → `0.5.0-alpha.1` → `0.5.0-alpha.2`, each publishing to `@alpha`.

**Sync `develop` after every alpha, not just after stable.** Each `npm version` creates a bump commit on `alpha`, and any pre-flight fixes (audit bumps, lockfile refreshes) also land on `alpha`. To keep `develop` from diverging:

```bash
git checkout develop && git pull
git merge alpha
git push origin develop
```

Fast-forward merge in the normal case. If you get a conflict, it usually means someone landed a fix directly on `alpha` that conflicts with in-flight work on `develop` — resolve on `develop`.

### 4. Cut the stable release

Promote `alpha` to `main`, then bump the bare version on `main`:

```bash
gh pr create --base main --head alpha \
  --title "Release 0.5.0" --body "Promote <last-alpha> to stable"
gh pr merge <pr-number> --merge --delete-branch=false

git checkout main && git pull origin main
(unset COALESCE_ACCESS_TOKEN COALESCE_BASE_URL COALESCE_REPO_PATH \
        COALESCE_CACHE_DIR COALESCE_PROFILE; \
 npm version 0.5.0)
git push origin main --tags
```

The workflow sees no `-alpha.*` suffix and routes to the stable flow: `@latest`, MCP Registry, full (non-prerelease) GitHub Release. The release content is identical to the last alpha you tested — only the version string changes.

### 5. Sync develop

```bash
git checkout develop
git merge main
git push origin develop
```

### Post-release verification

Run after any release — cheap, and catches "did it actually publish?" before you find out the hard way:

```bash
npm view coalesce-transform-mcp dist-tags
npm view coalesce-transform-mcp@<version> version
gh release view v<version> --json tagName,isPrerelease,url
gh run list --workflow=release.yml --limit 1
```

Alpha releases should show `isPrerelease: true` and **not** appear in MCP Registry searches; stable releases should show `isPrerelease: false` and land in MCP Registry within a minute of workflow completion.

## How `npm version` is wired (and why)

`package.json` splits the lifecycle across two hooks:

```json
"preversion": "npm run build && npm audit --omit=dev",
"version": "node scripts/sync-version.mjs && git add server.json README.md"
```

Per npm's documented lifecycle:

1. **`preversion`** runs *before* the version is bumped — right place for validation (build, audit). Never put `sync-version.mjs` here: at this point `package.json` still holds the OLD version, so the sync would write a stale `server.json`.
2. npm writes the NEW version into `package.json`.
3. **`version`** runs *after* the bump but *before* the commit. `sync-version.mjs` now sees the new version, rewrites `server.json` + `README.md`, and `git add` stages both. npm's subsequent commit picks them up.
4. npm commits (package.json + server.json + README together) and creates the tag.

**There is no `postversion` hook.** An earlier iteration of this repo used `postversion` to `git commit --amend` after the bump, which left the tag pointing at the *pre-amend* commit (with stale `server.json`) while the branch tip was the amended commit. Alpha releases hid the bug because they skip the MCP Registry; the first stable would have published wrong metadata. If you're tempted to add `postversion` back to "fix something" — don't. Put the behavior in `version` instead.

### 6. (Optional) Move the `@alpha` tag forward

After cutting stable, `@alpha` still points at `0.5.0-alpha.2` — older than `@latest`. Two tidy options:

```bash
# Option A: point @alpha at the stable until the next prerelease cycle starts.
npm dist-tag add coalesce-transform-mcp@0.5.0 alpha

# Option B: start the next alpha cycle immediately. @alpha re-advances ahead of @latest.
npm version preminor --preid=alpha   # 0.5.0 → 0.6.0-alpha.0
git push origin main --tags
```

Most projects pick Option B when a next version is already in flight. Otherwise Option A keeps `@alpha` from being a footgun for users who pin it.

## Testing an alpha locally

Once an alpha is published to `@alpha`, point your own MCP client at it by adding the tag to the `args` in your MCP config.

**Claude Code (`.mcp.json`):**

```json
{
  "coalesce-transform": {
    "command": "npx",
    "args": ["coalesce-transform-mcp@alpha"],
    "env": {
      "COALESCE_ACCESS_TOKEN": "${COALESCE_ACCESS_TOKEN}"
    }
  }
}
```

**Claude Desktop / Cursor / Windsurf** — same, inside `"mcpServers": { … }`.

Then restart your MCP client so `npx` re-resolves the dependency.

Notes:

- To pin to a specific alpha build rather than "whatever `@alpha` points at right now", use the full version: `"args": ["coalesce-transform-mcp@0.5.0-alpha.2"]`.
- `npx` caches downloaded packages. If the `@alpha` dist-tag advances but your client still resolves the old build, either force a fresh fetch with `npx -y coalesce-transform-mcp@alpha` or clear npx's cache (`rm -rf ~/.npm/_npx` on macOS/Linux).
- To switch back to stable: drop the `@alpha` suffix (`"args": ["coalesce-transform-mcp"]`) and restart the client.
- Running alpha and stable side-by-side: give them distinct server names in the config (e.g. `"coalesce-transform"` and `"coalesce-transform-alpha"`) so they register as separate MCP servers.

## Skipping alphas entirely

Nothing forces you through the alpha channel. A straight stable release is just:

```bash
git checkout main && git pull
npm version patch   # or minor / major
git push origin main --tags
```

## What happens to an alpha after stable ships?

It stays on npm at that exact version forever. Pinned installs (`coalesce-transform-mcp@0.5.0-alpha.2`) keep working. Only the `@alpha` dist-tag moves on (or doesn't — see step 6).

If you need to actively discourage users from a specific alpha (e.g. it had a serious bug you fixed in a later alpha):

```bash
npm deprecate coalesce-transform-mcp@0.5.0-alpha.0 "Use 0.5.0 or later — this alpha had a serious lineage-cache bug."
```

## Pre-commit hooks (husky)

The `.husky/pre-commit` hook runs automatically on every commit:

1. **Secret scanning** — blocks commits containing tokens, private keys, or API keys
2. **Type check** — `tsc --noEmit`
3. **Tests** — `npm test`

Skip with `git commit --no-verify` for false positives.

## Accounts

- **npm**: https://www.npmjs.com/package/coalesce-transform-mcp
- **MCP Registry**: https://registry.modelcontextprotocol.io (server name: `io.github.jessemarshall/coalesce-transform`)
- **GitHub Pages**: https://jessemarshall.github.io/coalesce-transform-mcp/ (Google Search Console verification)

## Re-authenticate if needed

```bash
npm login
mcp-publisher login github
```

## COA CLI dependency

The MCP bundles [`@coalescesoftware/coa`](https://www.npmjs.com/package/@coalescesoftware/coa) as a regular dependency. It is currently pinned to the **alpha track** (`@next` point-in-time version). This section captures release-time checks and the cadence for upgrading the pin.

### Pre-release checklist

Run before every release:

1. Confirm `@coalescesoftware/coa` in `package.json` is an **exact** version (not `^`, `~`, `next`, `latest`). The release workflow will fail if it is not.
2. Smoke-test the bundled CLI against a fresh install:

   ```bash
   rm -rf node_modules && npm ci
   node scripts/verify-coa-pin.mjs   # asserts pin == installed
   node scripts/smoke-coa.mjs        # resolves binary + runs `coa --version`
   ```

3. Confirm the pinned version still resolves on npm:

   ```bash
   PIN=$(node -p "require('./package.json').dependencies['@coalescesoftware/coa']")
   npm view "@coalescesoftware/coa@$PIN" version
   ```

   A missing or yanked alpha is the most common failure mode for this track.

4. (If you touched any `coa_*` tool or preflight logic) run the eval skills:

   - `eval-pipeline-e2e` — end-to-end pipeline creation
   - `eval-smoke` — live API smoke test

### Bumping the pinned COA version

The `@next` tag moves roughly weekly on the alpha track. To pull in a new build:

1. See what `@next` currently resolves to:
   ```bash
   npm view @coalescesoftware/coa@next version
   ```
2. Install and pin the exact version:
   ```bash
   npm install @coalescesoftware/coa@<exact-alpha-version>
   ```
3. Inspect the diff via `git diff package.json package-lock.json` and commit both.
4. Run the full test suite (`npx vitest run`). The `tests/services/coa/` tests and the resource-read tests will exercise the new binary.
5. Run the eval skills and note any behavior changes in the release PR description.

### Quarterly (or on-demand) review

- Read the COA changelog between the previously pinned version and the latest alpha. Look for:
  - Flag renames or removals (these silently break our arg builders)
  - New error codes / output shape changes (affect `coa doctor` and `coa validate` JSON parsing)
  - Promotion of features from alpha → stable (may let us move the pin track from `@next` to `@latest`)
- If the COA team cuts a new stable (`@latest`), consider switching the pin track. Alpha churn is the main operational cost of the current integration.

### Rollback strategy

If a COA upgrade breaks MCP users post-release:

1. **Downgrade path for users**: `npm install coalesce-transform-mcp@<prior>`.
2. **Emergency hotfix**: revert the COA version bump in `package.json` + `package-lock.json`, cut a new patch release. All `coa_*` tools degrade gracefully when COA is missing or non-functional (they return a structured error, not a crash), so we don't need to remove the dependency entirely.

The resolver (`src/services/coa/resolver.ts`) is intentionally non-fatal at MCP startup — only tool invocations surface COA failures. Cloud REST tools are unaffected by COA regressions.
