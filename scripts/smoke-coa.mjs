#!/usr/bin/env node
// Phase 1 smoke test for the COA integration.
// Requires `npm run build` to have run (reads the compiled resolver/runner from dist/).
//
// Usage:
//   npm run build && node scripts/smoke-coa.mjs

import { resolveCoaBinary } from "../dist/services/coa/resolver.js";
import { runCoa } from "../dist/services/coa/runner.js";

function ok(label, detail) {
  console.log(`  \u2713 ${label}${detail ? ` — ${detail}` : ""}`);
}
function fail(label, detail) {
  console.error(`  \u2717 ${label}${detail ? ` — ${detail}` : ""}`);
}

async function main() {
  console.log("COA smoke test");

  let resolved;
  try {
    resolved = resolveCoaBinary();
    ok(`resolve: ${resolved.source}`, resolved.binaryPath);
    ok("version", resolved.version ?? "(probe returned null)");
  } catch (err) {
    fail("resolve", err instanceof Error ? err.message : String(err));
    process.exit(1);
  }

  const run = await runCoa(["--version"], { timeoutMs: 15_000 });
  if (run.exitCode === 0) {
    ok("runCoa --version exit 0", (run.stdout || run.stderr).trim());
  } else {
    fail(
      "runCoa --version",
      `exit=${run.exitCode} timedOut=${run.timedOut} stderr=${run.stderr.trim()}`
    );
    process.exit(1);
  }

  console.log("\nCOA smoke test passed.");
}

main().catch((err) => {
  console.error("Smoke test crashed:", err);
  process.exit(1);
});
