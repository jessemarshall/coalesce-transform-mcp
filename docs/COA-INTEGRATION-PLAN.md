# COA CLI Integration Build Plan

> **Status:** Phases 1–5 complete. See [RELEASES.md](./RELEASES.md#coa-cli-dependency) for the operational runbook that replaces the plan's Phase 5 checklist.


Integrating the Coalesce `coa` CLI (`@coalescesoftware/coa`) into this MCP as a bundled dependency. COA is Coalesce's local-first CLI that works against project files + warehouse directly — distinct from the existing cloud REST path this MCP already covers.

## Decisions (locked)

- **Track:** `@next` (alpha). Rationale: V2 SQL nodes, `coa describe`, and other agent-relevant features are alpha-only. MCP is testing-focused, so alpha churn is acceptable.
- **Version pin:** exact version, not a floating tag. Current target: `7.33.0-alpha.73.h7b449dd800e9` (refresh at PR time via `npm view @coalescesoftware/coa@next version`).
- **Bundle strategy:** regular `dependencies` entry. Install size (~76 MB unpacked) is acceptable.
- **Binary resolution order:** bundled (via `require.resolve`) → PATH fallback → clear error.
- **Startup behavior:** non-fatal. Missing/broken COA only errors when a `coa_*` tool is invoked.

## Open questions to resolve during Phase 1

- Does `@coalescesoftware/coa` pull in native bindings (e.g., Snowflake driver)? Check `npm ls` after install — if yes, document platform caveats in README.
- Does `coa --version` output parse cleanly? Confirm format before using it in the resolver/startup log.
- Do `coa doctor`, `coa validate`, `coa environments list` all support `--json`? The Notion doc only calls out `--json` for `validate`. Test each and fall back to stdout parsing where needed.

## Phase 1 — Dependency wiring (no tools yet)

Goal: prove the install/resolution story end-to-end before writing tool wrappers.

### Tasks

- [ ] Add `@coalescesoftware/coa` at pinned exact version to `dependencies` in [package.json](../package.json).
- [ ] `npm install` locally. Confirm `node_modules/@coalescesoftware/coa/coa.js` exists.
- [ ] Create [src/services/coa/resolver.ts](../src/services/coa/resolver.ts):
  - `resolveCoaBinary()`: returns `{ binaryPath, source: 'bundled' | 'path', version }`.
  - Try `require.resolve('@coalescesoftware/coa/coa.js')` first. Bin entry per `npm view` is `coa.js` at package root, not `bin/coa` — verify on install.
  - Fallback: look up `coa` on PATH via `which` / `where` (cross-platform).
  - Capture version via `spawnSync(binaryPath, ['--version'])`; trim stderr+stdout.
  - Cache the result for process lifetime.
- [ ] Create [src/services/coa/runner.ts](../src/services/coa/runner.ts):
  - `runCoa(args, { cwd, env, timeoutMs, parseJson })`.
  - Uses `child_process.spawn`, captures stdout/stderr, enforces timeout.
  - Returns `{ exitCode, stdout, stderr, json? }`. Never throws on non-zero exit — return the failure so tool layer can map to a useful MCP error.
  - Sanitize env: passthrough everything except `COALESCE_*` (those are for the cloud REST client, not COA).
- [ ] Unit tests for resolver + runner in [tests/coa/](../tests/coa/).
  - Mock `require.resolve` and `spawnSync` for resolver.
  - Runner tests: use a tiny shim script (`node -e "console.log(process.argv)"`) to validate arg pass-through, timeouts, exit-code capture.
- [ ] Smoke test: a dev-only script `scripts/smoke-coa.mjs` that calls `resolveCoaBinary()` and `runCoa(['--version'])` and prints the result. Not wired into MCP yet.
- [ ] Run `npm pack && ls -lh *.tgz` before/after — confirm the MCP tarball size hasn't changed (only install-time footprint should grow).
- [ ] Verify `npm install -g <packed tarball>` on a clean machine (or fresh node_modules): after global install, confirm COA resolves through `require.resolve` under the MCP's install path.

### Exit criteria for Phase 1

- `scripts/smoke-coa.mjs` prints `coa --version` output locally.
- Unit tests green.
- Global install smoke test passes.
- No new MCP tools registered yet.

## Phase 2 — Read-only tool wrappers

Goal: expose safe COA commands as MCP tools. No warehouse writes.

### Tools to add

All live in [src/mcp/coa.ts](../src/mcp/coa.ts) (single file, flat convention matching other `src/mcp/*.ts` files).

Each tool takes a required `projectPath` and validates it contains `data.yml` before shelling out (fail-fast, not opaque CLI output).

| Tool | COA command | Notes |
|------|-------------|-------|
| `coa_doctor` | `coa doctor` | Parse stdout; no `--json` confirmed yet. |
| `coa_validate` | `coa validate --json` | `--json` documented in Notion guide. |
| `coa_list_nodes` | `coa create --list-nodes` | Parse stdout. |
| `coa_environments_list` | `coa environments list` | Test for `--json` support. |
| `coa_nodes_list` | `coa nodes list --environmentID <id>` | Requires env credentials in `~/.coa/config`. |
| `coa_runs_list` | `coa runs list --environmentID <id>` | Same as above. |
| `coa_dry_run_create` | `coa create --dry-run --verbose --include '{ ... }'` | Force dry-run; surface generated DDL. |
| `coa_dry_run_run` | `coa run --dry-run --verbose --include '{ ... }'` | Force dry-run; surface generated DML. |

### Tasks

- [ ] Write `validateProjectPath(path)` helper in [src/services/coa/project.ts](../src/services/coa/project.ts) — checks path exists, is a dir, contains `data.yml`.
- [ ] Add tool definitions in [src/mcp/coa.ts](../src/mcp/coa.ts) using the same pattern as [src/mcp/workspaces.ts](../src/mcp/workspaces.ts).
- [ ] Register in [src/index.ts](../src/index.ts).
- [ ] Per-tool error mapping: COA exit code → structured MCP error (project path invalid, credentials missing, validation failed, etc.).
- [ ] Tests: mock `runCoa` to return canned stdout/exit codes; assert tool output formatting.
- [ ] Update [tests/resources.test.ts](../tests/resources.test.ts) if resource count changes (it shouldn't in Phase 2).

### Exit criteria for Phase 2

- All eight read-only tools callable via MCP inspector.
- Integration test (gated behind env var) that runs `coa_doctor` against a fixture project with real Snowflake credentials.

## Phase 3 — `coa describe` as MCP resources (highest leverage)

Goal: make COA's self-describing docs available to agents without leaving the MCP.

### Tasks

- [ ] Create [src/services/coa/describe.ts](../src/services/coa/describe.ts):
  - Topic list: `overview` (empty topic), `selectors`, `sql-format`, `node-types`, `config`, `structure`, `concepts`.
  - `fetchDescribe(topic)`: shells out, caches result to disk keyed by COA version.
  - Cache path: `~/.cache/coalesce-transform-mcp/coa-describe/<version>/<topic>.md` (fall back to `os.tmpdir()` if `~/.cache` isn't writable).
- [ ] Expose as MCP resources with URIs like `coa://describe/selectors`.
- [ ] Resources listed alongside existing entries in [src/resources/context/](../src/resources/context/) — decide whether to materialize them as static files (simpler, but drift risk) or serve dynamically from the cache.
  - **Recommendation:** dynamic serve from cache, populated on first access. Keeps resources in sync with the pinned COA version automatically.
- [ ] Update [tests/resources.test.ts](../tests/resources.test.ts) expected resource count.
- [ ] Add a `coa_describe` tool as a fallback for agents that prefer tools over resources (same underlying call).

### Exit criteria for Phase 3

- All seven describe topics resolvable via MCP resource list.
- Cache invalidates correctly on COA version bump (covered by test).

## Phase 4 — Write tool wrappers (gated)

Goal: enable actual warehouse mutations + cloud deploys via MCP, with safety rails.

### Tools to add

| Tool | COA command | Safety |
|------|-------------|--------|
| `coa_create` | `coa create --include '{ ... }'` | Requires `confirm: true`. Default is dry-run. |
| `coa_run` | `coa run --include '{ ... }'` | Requires `confirm: true`. |
| `coa_plan` | `coa plan --environmentID <id>` | Writes `coa-plan.json` — allowed without confirm (it's a plan, not an apply). |
| `coa_deploy` | `coa deploy --environmentID <id> --plan <path>` | Requires `confirm: true` + plan file must exist. |
| `coa_refresh` | `coa refresh --environmentID <id> --include '{ ... }'` | Requires `confirm: true`. |

### Pre-flight validators (block before shelling out)

Encode known gotchas from the Notion guide in [src/services/coa/preflight.ts](../src/services/coa/preflight.ts):

- Reject `.sql` files with double-quoted `ref("...")` — silently breaks lineage.
- Warn on literal `UNION ALL` in `.sql` nodes — dropped by V2 parser; direct user to `insertStrategy: UNION ALL`.
- Require `workspaces.yml` present for local commands (`create`, `run`).
- Confirm `data.yml` has `fileVersion: 3`.
- Detect empty `--include` selector patterns (`{ A || B }` instead of `{ A } || { B }`).

### Tasks

- [ ] Preflight implementation + tests.
- [ ] Tool definitions with `confirm: boolean` input gates.
- [ ] Error mapping: deploy plan failures, `environmentID` missing from `~/.coa/config`, etc.
- [ ] Integration tests against a fixture project, gated behind env var for CI safety.

### Exit criteria for Phase 4

- Write tools callable with `confirm: true` only.
- Preflight blocks all documented gotchas before warehouse calls.

## Phase 5 — Release & maintenance process

### Release checklist additions

Add to [RELEASES.md](./RELEASES.md):

- [ ] `npm view @coalescesoftware/coa@<pinned> version` — confirm pinned version still resolves.
- [ ] Run `scripts/smoke-coa.mjs` against the packed tarball.
- [ ] Eval suite: run `eval-pipeline-e2e` and `eval-smoke` skills — confirm no regressions from COA version.

### Quarterly (or on-demand) review

- [ ] Review COA changelog between pinned and latest alpha.
- [ ] Bump pin; re-run eval suite; document any behavior changes in release notes.
- [ ] If COA promotes features from alpha to stable, consider switching the track to `@latest`.

### CI

- [ ] Smoke test in CI: `resolveCoaBinary()` + `coa --version` + assert version matches `package.json` pin.

## Rollback strategy

If COA integration causes problems post-release:

1. Revert to prior MCP version via `npm install coalesce-transform-mcp@<prior>`.
2. For emergency hotfix: publish a patch version with `@coalescesoftware/coa` removed from `dependencies`. MCP degrades gracefully — `coa_*` tools return "COA not available" error; cloud tools unaffected.

Key design decision that enables this: resolver is non-fatal at startup, and all `coa_*` tools check availability at invocation time.

## File map (new files to be created)

```
src/services/coa/
  resolver.ts         # Phase 1
  runner.ts           # Phase 1
  project.ts          # Phase 2
  describe.ts         # Phase 3
  preflight.ts        # Phase 4
src/mcp/
  coa.ts              # Phase 2 (read-only tools), expanded in Phase 4
scripts/
  smoke-coa.mjs       # Phase 1
tests/coa/
  resolver.test.ts    # Phase 1
  runner.test.ts      # Phase 1
  project.test.ts     # Phase 2
  describe.test.ts    # Phase 3
  preflight.test.ts   # Phase 4
```

## Notes / risks to revisit

- **Alpha churn.** `@next` is a moving tag; we pin exact versions but need a cadence for bumps. Monthly at minimum.
- **Credential boundary.** MCP doesn't touch `~/.coa/config` today. Users manage it themselves. If we ever want to help bootstrap credentials, that's a separate scoped project.
- **Node driver bindings.** If COA's transitive deps include native modules (Snowflake driver likely does), global installs on Windows/Alpine may fail. Document in README once confirmed.
- **Tool sprawl.** After Phase 4 the MCP will have ~18 new `coa_*` tools. Consider grouping in tool descriptions so agents understand the cloud vs local boundary.
