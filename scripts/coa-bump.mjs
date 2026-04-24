#!/usr/bin/env node
// Emit a "what changed since our pin" ledger for a COA bump.
//
// Reads the current @coalescesoftware/coa pin from package.json, resolves
// current @next via `npm view`, then shells out to `gh api compare` to list
// every commit in Coalesce-Software-Inc/coalesce between the two hashes.
// Commits are bucketed with simple heuristics (material / v2-only / ops-ci-ui)
// and rendered as markdown suitable for pasting into a bump PR body.
//
// The classification is a starting point — re-check by hand before merging,
// especially anything in the "material" bucket. See
// docs/RELEASES.md → "Bumping the pinned COA version".
//
// Usage:
//   node scripts/coa-bump.mjs               # print ledger to stdout
//   node scripts/coa-bump.mjs > /tmp/l.md   # redirect for PR body

import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const COALESCE_MONOREPO = "Coalesce-Software-Inc/coalesce";
const PIN_HASH_RE = /\.h([0-9a-f]{6,})$/;
const CD_TICKET_RE = /CD-\d+/i;
const PR_NUMBER_RE = /\(#(\d+)\)\s*$/;
const V2_RE = /\bV2\b/;
const OPS_RE =
  /\b(sidebar|ux|ui[- ]fix|datadog|rum|rwx|vitest flak|ci\b|workflow|pre-?push|copilot|heartbeat|cron|scheduled? job|metrics collector|graph files|translate layer|react ?instrument|instrument ?js|browser extension|projects? list|search to projects|char limit|process\.env|curl|cascade env|bulk node edit.*metadata service|page refresh|feature flag mocking)\b/i;

function die(message) {
  console.error(`coa-bump: ${message}`);
  process.exit(1);
}

function run(cmd, args, opts = {}) {
  const result = spawnSync(cmd, args, {
    encoding: "utf8",
    ...opts,
  });
  if (result.error) die(`${cmd} failed: ${result.error.message}`);
  if (result.status !== 0) {
    die(
      `${cmd} ${args.join(" ")} exited ${result.status}: ${result.stderr.trim()}`
    );
  }
  return result.stdout.trim();
}

function extractSha(pin) {
  const match = pin.match(PIN_HASH_RE);
  if (!match) {
    die(
      `Pin "${pin}" has no .h<sha> suffix. Not a hash-suffixed alpha build — script assumes the COA @next track with embedded commit hashes.`
    );
  }
  return match[1];
}

function classify(title) {
  if (V2_RE.test(title)) return "v2";
  if (OPS_RE.test(title)) return "ops";
  return "material";
}

function parseCommit(commit) {
  const title = commit.commit.message.split("\n")[0];
  const prMatch = title.match(PR_NUMBER_RE);
  const cdMatch = title.match(CD_TICKET_RE);
  return {
    sha: commit.sha,
    shortSha: commit.sha.slice(0, 7),
    date: commit.commit.author.date.slice(0, 10),
    title,
    pr: prMatch ? prMatch[1] : null,
    ticket: cdMatch ? cdMatch[0].toUpperCase() : null,
    bucket: classify(title),
  };
}

function renderEntry(c) {
  const ticketLabel = c.ticket ?? "—";
  const titleBody = c.title.replace(PR_NUMBER_RE, "").trim();
  const links = [];
  if (c.ticket) {
    links.push(
      `[${c.ticket}](https://coalescesoftware.atlassian.net/browse/${c.ticket})`
    );
  }
  if (c.pr) {
    links.push(
      `[#${c.pr}](https://github.com/${COALESCE_MONOREPO}/pull/${c.pr})`
    );
  }
  const linkSuffix = links.length ? ` (${links.join(" · ")})` : "";
  return `- \`${c.shortSha}\` ${c.date} — ${titleBody}${linkSuffix}`;
}

function renderLedger({ curPin, newPin, curSha, newSha, commits }) {
  const sorted = commits.slice().sort((a, b) => a.date.localeCompare(b.date));
  const material = sorted.filter((c) => c.bucket === "material");
  const v2 = sorted.filter((c) => c.bucket === "v2");
  const ops = sorted.filter((c) => c.bucket === "ops");
  const dateRange =
    sorted.length > 0
      ? `${sorted[0].date} → ${sorted[sorted.length - 1].date}`
      : "(no commits)";

  const out = [];
  out.push(`# COA bump: \`${curPin}\` → \`${newPin}\``);
  out.push("");
  out.push(
    `**Range:** \`${curSha.slice(0, 8)}\` → \`${newSha.slice(0, 8)}\` (${dateRange})`
  );
  out.push(`**Commits:** ${sorted.length}`);
  out.push("");
  out.push(
    `> Classification below is heuristic — re-check each bucket before merging. See [docs/RELEASES.md](docs/RELEASES.md#bumping-the-pinned-coa-version).`
  );
  out.push("");

  out.push(`## Material to our \`coa_*\` paths (${material.length})`);
  if (material.length === 0) {
    out.push("_None flagged by heuristics. Re-scan the full list below._");
  } else {
    for (const c of material) out.push(renderEntry(c));
  }
  out.push("");

  out.push(`## V2-only — low blast radius under V1 default (${v2.length})`);
  if (v2.length === 0) out.push("_None._");
  else for (const c of v2) out.push(renderEntry(c));
  out.push("");

  out.push(`## Ops / UI / CI — out of scope for \`coa_*\` (${ops.length})`);
  if (ops.length === 0) {
    out.push("_None._");
  } else {
    out.push("<details>");
    out.push("<summary>Expand</summary>");
    out.push("");
    for (const c of ops) out.push(renderEntry(c));
    out.push("");
    out.push("</details>");
  }
  out.push("");
  return out.join("\n");
}

function main() {
  const __dirname = dirname(fileURLToPath(import.meta.url));
  const repoRoot = join(__dirname, "..");
  const pkg = JSON.parse(readFileSync(join(repoRoot, "package.json"), "utf8"));
  const curPin = pkg.dependencies?.["@coalescesoftware/coa"];
  if (!curPin) die("@coalescesoftware/coa missing from package.json");
  const curSha = extractSha(curPin);

  const newPin = run("npm", ["view", "@coalescesoftware/coa@next", "version"]);
  const newSha = extractSha(newPin);

  if (curSha === newSha) {
    console.error(`coa-bump: already at @next (${curPin}). Nothing to do.`);
    process.exit(0);
  }

  const compareJson = run("gh", [
    "api",
    `repos/${COALESCE_MONOREPO}/compare/${curSha}...${newSha}`,
  ]);
  const compare = JSON.parse(compareJson);
  const commits = (compare.commits ?? []).map(parseCommit);

  process.stdout.write(
    renderLedger({ curPin, newPin, curSha, newSha, commits })
  );
}

main();
