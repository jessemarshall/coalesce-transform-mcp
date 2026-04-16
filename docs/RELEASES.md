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

## Standard release flow (develop → main → alpha series → stable)

Typical cadence: iterate on `develop`, merge to `main`, publish a series of alphas from `main` for testing, then promote to stable.

### 1. Iterate

Work on `develop` as usual. When a feature or fix is ready to ship to testers:

```bash
git checkout develop && git pull
# (make changes, commit)
gh pr create --base main --title "0.5: add coalesce-setup prompt" --body "…"
```

Merge the PR on GitHub, then:

```bash
git checkout main && git pull origin main
```

### 2. First alpha of a new version

Use `preminor` + `--preid=alpha` to bump (e.g. `0.4.8` → `0.5.0-alpha.0`):

```bash
npm version preminor --preid=alpha
git push origin main --tags
```

(`prepatch --preid=alpha` for a patch-level series, `premajor --preid=alpha` for a major.)

The workflow detects `-alpha.*`, publishes to `@alpha`, skips the MCP Registry, and cuts a prerelease GitHub Release. Users test with `npm install coalesce-transform-mcp@alpha`.

### 3. Subsequent alphas in the same series

More fixes on `develop` → PR → merge to `main` → on `main`:

```bash
npm version prerelease --preid=alpha
git push origin main --tags
```

That bumps `0.5.0-alpha.0` → `0.5.0-alpha.1` → `0.5.0-alpha.2`, each publishing to `@alpha`.

### 4. Cut the stable release

Once confident the last alpha is good, bump to the bare version:

```bash
npm version 0.5.0
git push origin main --tags
```

The workflow detects no `-alpha.*` suffix and routes to the stable flow: `@latest`, MCP Registry, full GitHub Release. The release content is identical to the last alpha you tested — only the version string changes.

### 5. Sync develop

```bash
git checkout develop
git merge main
git push origin develop
```

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
