#!/usr/bin/env node
// Test fixture: simulates the `coa` CLI for runner.test.ts.
//
// Args it honors (only the first matching flag is acted on):
//   --echo-args-json        : print JSON.stringify(process.argv.slice(2)) to stdout
//   --echo-env-json=PREFIX  : print JSON of env vars whose keys start with PREFIX
//   --print=TEXT            : print TEXT to stdout
//   --stderr=TEXT           : print TEXT to stderr
//   --exit=N                : exit with code N after other output
//   --sleep=MS              : delay MS ms before exiting (used for timeout tests)

const args = process.argv.slice(2);

function arg(flag) {
  const hit = args.find((a) => a === flag || a.startsWith(`${flag}=`));
  if (!hit) return null;
  if (hit === flag) return "";
  return hit.slice(flag.length + 1);
}

const echoArgs = args.includes("--echo-args-json");
const envPrefix = arg("--echo-env-json");
const printText = arg("--print");
const stderrText = arg("--stderr");
const exitCode = arg("--exit");
const sleepMs = arg("--sleep");

if (echoArgs) {
  process.stdout.write(JSON.stringify(args));
}
if (envPrefix !== null) {
  const filtered = {};
  for (const [k, v] of Object.entries(process.env)) {
    if (k.startsWith(envPrefix)) filtered[k] = v;
  }
  process.stdout.write(JSON.stringify(filtered));
}
if (printText !== null) {
  process.stdout.write(printText);
}
if (stderrText !== null) {
  process.stderr.write(stderrText);
}

const exitNum = exitCode === null ? 0 : Number(exitCode);
const sleepNum = sleepMs === null ? 0 : Number(sleepMs);

if (sleepNum > 0) {
  setTimeout(() => process.exit(exitNum), sleepNum);
} else {
  process.exit(exitNum);
}
