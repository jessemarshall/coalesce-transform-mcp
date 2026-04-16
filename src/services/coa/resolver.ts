import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { delimiter, join } from "node:path";

export type CoaBinarySource = "bundled" | "path";

export type ResolvedCoaBinary = {
  /** Absolute path to the `coa.js` script (bundled) or the `coa` executable (path). */
  binaryPath: string;
  /** Where the binary was found. */
  source: CoaBinarySource;
  /** Output of `coa --version`, trimmed. Null if version probe failed. */
  version: string | null;
};

export class CoaNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CoaNotFoundError";
  }
}

let cached: ResolvedCoaBinary | null = null;
let cachedError: CoaNotFoundError | null = null;

/**
 * Locate the `coa` CLI binary. Prefers the bundled dependency; falls back to PATH.
 *
 * Result is cached for the process lifetime. Call `resetCoaBinaryCache()` in tests.
 */
export function resolveCoaBinary(): ResolvedCoaBinary {
  if (cached) return cached;
  if (cachedError) throw cachedError;

  const bundled = tryBundled();
  if (bundled) {
    cached = bundled;
    return bundled;
  }

  const fromPath = tryPath();
  if (fromPath) {
    cached = fromPath;
    return fromPath;
  }

  cachedError = new CoaNotFoundError(
    "coa CLI not found. Expected @coalescesoftware/coa to be installed as a dependency, " +
      "or `coa` to be available on PATH. Run `npm install` in the MCP package to restore the bundled CLI."
  );
  throw cachedError;
}

/** Test-only: clear the cached resolution. */
export function resetCoaBinaryCache(): void {
  cached = null;
  cachedError = null;
}

function tryBundled(): ResolvedCoaBinary | null {
  const require = createRequire(import.meta.url);
  let binaryPath: string;
  try {
    binaryPath = require.resolve("@coalescesoftware/coa/coa.js");
  } catch {
    return null;
  }
  if (!existsSync(binaryPath)) return null;

  const version = probeVersion(process.execPath, [binaryPath]);
  return { binaryPath, source: "bundled", version };
}

function tryPath(): ResolvedCoaBinary | null {
  // Bracket syntax intentional — these are OS env vars, not MCP-configured ones,
  // and the server-metadata test scans src for dot-access env references.
  const pathEnv = process.env["PATH"] ?? "";
  const pathExtEnv = process.env["PATHEXT"] ?? "";
  const extensions =
    process.platform === "win32"
      ? pathExtEnv.split(";").filter(Boolean).concat([""])
      : [""];

  for (const dir of pathEnv.split(delimiter).filter(Boolean)) {
    for (const ext of extensions) {
      const candidate = join(dir, `coa${ext}`);
      if (existsSync(candidate)) {
        const version = probeVersion(candidate, []);
        return { binaryPath: candidate, source: "path", version };
      }
    }
  }
  return null;
}

function probeVersion(command: string, prefixArgs: string[]): string | null {
  try {
    const result = spawnSync(command, [...prefixArgs, "--version"], {
      encoding: "utf8",
      timeout: 10_000,
    });
    if (result.status !== 0) return null;
    const output = `${result.stdout ?? ""}${result.stderr ?? ""}`.trim();
    return output || null;
  } catch {
    return null;
  }
}
