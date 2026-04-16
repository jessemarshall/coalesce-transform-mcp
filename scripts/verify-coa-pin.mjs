#!/usr/bin/env node
// CI gate: confirm the installed @coalescesoftware/coa matches the pinned
// version in package.json. Fails fast if either:
//   (a) the pin is a floating tag (^, ~, "next", "latest") — we require exact pins
//   (b) the installed version differs from the pin
//
// This catches two failure modes:
//   1. Someone bumps the pin without committing the lockfile.
//   2. A bundled alpha is yanked from npm; `npm ci` silently installs a nearby
//      build and diverges from what was tested.

import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..");
const require = createRequire(import.meta.url);

function fail(message) {
  console.error(`verify-coa-pin: ${message}`);
  process.exit(1);
}

// 1) Read pin from the MCP's package.json.
const pkg = JSON.parse(
  readFileSync(join(repoRoot, "package.json"), "utf8")
);
const pin = pkg.dependencies?.["@coalescesoftware/coa"];
if (!pin) {
  fail("@coalescesoftware/coa is missing from package.json dependencies.");
}

// 2) Reject floating specifiers — the COA alpha track churns under us, so
//    an exact pin is mandatory.
const floating = /^[\^~]|^(?:next|latest)$|^\s*\*\s*$/;
if (floating.test(pin)) {
  fail(
    `Pin "${pin}" is a floating specifier. Use an exact version (e.g. "7.33.0-alpha.73.h7b449dd800e9"). ` +
      "The @next tag moves; do not depend on it directly."
  );
}

// 3) Read the installed version from node_modules.
let installedPkg;
try {
  installedPkg = require("@coalescesoftware/coa/package.json");
} catch (err) {
  fail(
    `@coalescesoftware/coa is not installed. Run 'npm ci' first. (${err instanceof Error ? err.message : String(err)})`
  );
}
const installed = installedPkg.version;

if (installed !== pin) {
  fail(
    `Installed @coalescesoftware/coa@${installed} does not match package.json pin ${pin}. ` +
      "Run 'npm ci' to re-install, or update the pin + lockfile together."
  );
}

// 4) Cross-check that the bundled binary actually runs and prints a version.
const binPath = require.resolve("@coalescesoftware/coa/coa.js");
const result = spawnSync(process.execPath, [binPath, "--version"], {
  encoding: "utf8",
  timeout: 15_000,
});
if (result.status !== 0) {
  fail(
    `\`coa --version\` exited ${result.status}. stderr: ${result.stderr.trim()}`
  );
}
const reported = (result.stdout + result.stderr).trim();
if (!reported.includes(pin)) {
  fail(
    `\`coa --version\` reported "${reported}" but the pin is "${pin}". ` +
      "The installed package and binary disagree."
  );
}

console.log(
  `verify-coa-pin: OK — pinned @coalescesoftware/coa@${pin} installed and binary reports matching version.`
);
