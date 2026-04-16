import { spawn } from "node:child_process";
import { resolveCoaBinary, type ResolvedCoaBinary } from "./resolver.js";

export type RunCoaOptions = {
  /** Working directory — typically the COA project root. */
  cwd?: string;
  /** Additional env vars merged on top of the sanitized base env. */
  env?: Record<string, string | undefined>;
  /** Hard timeout in ms. Defaults to 60_000. */
  timeoutMs?: number;
  /** If true, attempt to parse stdout as JSON and expose the result on `.json`. */
  parseJson?: boolean;
  /** Injectable resolver for tests. */
  resolve?: () => ResolvedCoaBinary;
};

export type RunCoaResult = {
  exitCode: number | null;
  /** True if the process was killed because it exceeded `timeoutMs`. */
  timedOut: boolean;
  stdout: string;
  stderr: string;
  /** Populated when `parseJson: true` and stdout was valid JSON. */
  json?: unknown;
  /** Populated when `parseJson: true` and stdout was not valid JSON. */
  jsonParseError?: string;
};

/**
 * Coalesce cloud REST env vars used elsewhere in this MCP. Stripped from the child
 * env so they cannot accidentally influence coa's behavior — coa reads its own
 * credentials from `~/.coa/config`.
 */
const STRIPPED_ENV_PREFIXES = ["COALESCE_"] as const;

/**
 * Run `coa` with the given args. Never throws on non-zero exit — returns the
 * failure so callers can map exit codes to structured MCP errors.
 */
export function runCoa(
  args: string[],
  options: RunCoaOptions = {}
): Promise<RunCoaResult> {
  const resolver = options.resolve ?? resolveCoaBinary;
  const binary = resolver();

  const { command, finalArgs } = buildSpawnArgs(binary, args);
  const env = buildChildEnv(options.env);
  const timeoutMs = options.timeoutMs ?? 60_000;

  return new Promise((resolve) => {
    const child = spawn(command, finalArgs, {
      cwd: options.cwd,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let timedOut = false;
    // When spawn fails (ENOENT, EACCES) Node fires both `error` and `close`.
    // Without a guard, the `close` handler would resolve a second time with a
    // payload that contradicts the `error` handler's. The second resolve is a
    // no-op per Promise semantics, but the race is platform-dependent — pin
    // the outcome to whichever handler wins first.
    let settled = false;
    const safeResolve = (payload: RunCoaResult) => {
      if (settled) return;
      settled = true;
      resolve(payload);
    };

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => (stdout += chunk));
    child.stderr.on("data", (chunk: string) => (stderr += chunk));

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
      // Hard kill if still alive after a grace period.
      setTimeout(() => child.kill("SIGKILL"), 2_000).unref();
    }, timeoutMs);
    timer.unref();

    child.on("error", (err) => {
      clearTimeout(timer);
      safeResolve({
        exitCode: null,
        timedOut,
        stdout,
        stderr: stderr + `\n${err.message}`,
      });
    });

    child.on("close", (code) => {
      clearTimeout(timer);
      const base: RunCoaResult = {
        exitCode: code,
        timedOut,
        stdout,
        stderr,
      };
      if (options.parseJson) {
        attachJson(base, stdout);
      }
      safeResolve(base);
    });
  });
}

function buildSpawnArgs(
  binary: ResolvedCoaBinary,
  args: string[]
): { command: string; finalArgs: string[] } {
  // Bundled `coa.js` is a plain Node script — invoke via the current Node binary
  // so we don't depend on shebang handling or the OS PATHEXT quirks on Windows.
  // PATH-resolved binaries may be shell wrappers (.cmd, .bat, shell scripts),
  // so we spawn them directly and let the OS handle invocation.
  if (binary.source === "bundled") {
    return { command: process.execPath, finalArgs: [binary.binaryPath, ...args] };
  }
  return { command: binary.binaryPath, finalArgs: args };
}

function buildChildEnv(
  overrides: Record<string, string | undefined> | undefined
): NodeJS.ProcessEnv {
  const base: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (STRIPPED_ENV_PREFIXES.some((prefix) => key.startsWith(prefix))) continue;
    base[key] = value;
  }
  if (overrides) {
    for (const [key, value] of Object.entries(overrides)) {
      if (value === undefined) {
        delete base[key];
      } else {
        base[key] = value;
      }
    }
  }
  return base;
}

function attachJson(result: RunCoaResult, stdout: string): void {
  const trimmed = stdout.trim();
  if (!trimmed) {
    result.jsonParseError = "stdout was empty";
    return;
  }
  try {
    result.json = JSON.parse(trimmed);
  } catch (err) {
    result.jsonParseError = err instanceof Error ? err.message : String(err);
  }
}
