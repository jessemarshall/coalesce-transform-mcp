#!/usr/bin/env node
/**
 * Pre-release sanity checks.
 *
 * Encodes institutional knowledge from prior release misfires:
 *   - Manual version edits instead of `npm version` broke a publish.
 *     (Not enforceable here — documented in MEMORY.md. This script guards the
 *      other known traps so `npm version` stays the happy path.)
 *   - Stale `COALESCE_CACHE_DIR` / `COALESCE_REPO_PATH` env vars broke vitest
 *     during `npm version`. We check for those up front.
 *
 * Checks run in order, short-circuiting on the first failure so slow checks
 * (vitest) don't run when a trivial one (dirty tree) already blocks release.
 *
 * Exit 0 when all checks pass. Exit 1 on any failure.
 *
 * Invoke standalone via `npm run release:doctor`. Chained into `preversion`
 * so `npm version` runs these checks automatically.
 */

import { spawnSync } from "node:child_process";

const GREEN = "\x1b[32m";
const RED = "\x1b[31m";
const YELLOW = "\x1b[33m";
const RESET = "\x1b[0m";
const BOLD = "\x1b[1m";

const SUSPECT_ENV_VARS = [
  // These leak into vitest and alter cache/filesystem behaviour. Stale values
  // from a prior dev session have broken test runs during `npm version`.
  "COALESCE_CACHE_DIR",
  "COALESCE_REPO_PATH",
  // These alter auth/profile resolution. Not fatal to tests, but a release
  // run should be a clean slate — flag them so they're deliberate.
  "COALESCE_PROFILE",
  "COALESCE_TOKEN",
  "COALESCE_MCP_SKILLS_DIR",
];

function header(text) {
  process.stdout.write(`\n${BOLD}${text}${RESET}\n`);
}

function pass(text) {
  process.stdout.write(`  ${GREEN}✓${RESET} ${text}\n`);
}

function fail(text) {
  process.stdout.write(`  ${RED}✗${RESET} ${text}\n`);
}

function warn(text) {
  process.stdout.write(`  ${YELLOW}!${RESET} ${text}\n`);
}

function note(text) {
  process.stdout.write(`    ${text}\n`);
}

function exitWithFailure(summary) {
  process.stdout.write(`\n${RED}${BOLD}release:doctor failed${RESET} — ${summary}\n`);
  process.stdout.write(`Fix the issue above and re-run \`npm run release:doctor\`.\n\n`);
  process.exit(1);
}

/** Run a command synchronously and return its full result. */
function run(command, args, { cwd, env } = {}) {
  return spawnSync(command, args, {
    cwd,
    env: env ?? process.env,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
}

// ---------- Check 1: working tree clean ----------

header("[1/3] Git working tree");
const gitStatus = run("git", ["status", "--porcelain"]);
if (gitStatus.status !== 0) {
  fail("`git status` failed");
  note(gitStatus.stderr.trim() || `exit ${gitStatus.status}`);
  exitWithFailure("git status check failed");
}
const dirty = gitStatus.stdout.trim();
if (dirty) {
  fail("working tree is dirty");
  for (const line of dirty.split("\n")) {
    note(line);
  }
  exitWithFailure(
    "commit, stash, or clean your changes before releasing — `npm version` should run on a clean tree"
  );
}
pass("working tree is clean");

// ---------- Check 2: no stale COALESCE_* env vars ----------

header("[2/3] Environment variables");
const leakedVars = SUSPECT_ENV_VARS.filter((name) => {
  const value = process.env[name];
  return value !== undefined && value !== "";
});
if (leakedVars.length > 0) {
  for (const name of leakedVars) {
    fail(`${name} is set (value hidden) — will leak into vitest`);
  }
  note("");
  note("Unset these in your current shell, then re-run:");
  for (const name of leakedVars) {
    note(`  unset ${name}`);
  }
  exitWithFailure(
    "stale COALESCE_* env vars will corrupt test runs during `npm version` — see MEMORY.md"
  );
}
pass(`no suspect env vars set (${SUSPECT_ENV_VARS.join(", ")})`);

// ---------- Check 3: tests pass ----------

header("[3/3] Test suite");
process.stdout.write("  running `npx vitest run` — this can take ~10s\n");
const testResult = spawnSync("npx", ["vitest", "run"], {
  stdio: "inherit",
  env: process.env,
});
if (testResult.status !== 0) {
  fail(`vitest exited ${testResult.status}`);
  exitWithFailure("tests failed — release blocked");
}
pass("all tests green");

// ---------- Done ----------

process.stdout.write(
  `\n${GREEN}${BOLD}release:doctor passed${RESET} — safe to run \`npm version\`.\n\n`
);
process.exit(0);
