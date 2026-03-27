import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const DEFAULT_TIMEOUT_MS = 120_000;

let cortexAvailableCache: { value: boolean; cachedAt: number } | null = null;
const FAILURE_CACHE_TTL_MS = 5 * 60 * 1000; // Re-check after 5 minutes on failure

/**
 * Strips ANSI escape codes from CLI output.
 */
function stripAnsi(text: string): string {
  // eslint-disable-next-line no-control-regex
  return text.replace(/\x1B\[[0-9;]*[A-Za-z]/g, "");
}

/**
 * Checks whether the `cortex` CLI is available in PATH.
 * Caches true permanently. Caches false with a TTL so transient failures
 * don't permanently disable Cortex tools.
 */
export async function isCortexAvailable(): Promise<boolean> {
  if (cortexAvailableCache !== null) {
    if (cortexAvailableCache.value) return true;
    // Re-check if failure cache has expired
    if (Date.now() - cortexAvailableCache.cachedAt < FAILURE_CACHE_TTL_MS) {
      return false;
    }
  }
  try {
    await execFileAsync("cortex", ["--version"], { timeout: 10_000 });
    cortexAvailableCache = { value: true, cachedAt: Date.now() };
    return true;
  } catch {
    cortexAvailableCache = { value: false, cachedAt: Date.now() };
    return false;
  }
}

/**
 * Resets the cached availability check (for testing).
 */
export function resetCortexAvailabilityCache(): void {
  cortexAvailableCache = null;
}

export interface CortexCommandResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

/**
 * Runs a cortex CLI command with the given arguments.
 * Uses execFile (no shell) to prevent injection.
 * Throws on infrastructure errors (binary not found, timeout, buffer overflow).
 * Returns a result with exitCode for normal process exits.
 */
export async function runCortexCommand(
  args: string[],
  options?: { connection?: string; timeoutMs?: number }
): Promise<CortexCommandResult> {
  const fullArgs = [...args];
  if (options?.connection) {
    fullArgs.push("--connection", options.connection);
  }
  fullArgs.push("--no-auto-update");

  try {
    const { stdout, stderr } = await execFileAsync("cortex", fullArgs, {
      timeout: options?.timeoutMs ?? DEFAULT_TIMEOUT_MS,
      maxBuffer: 10 * 1024 * 1024,
      env: { ...process.env, NO_COLOR: "1" },
    });
    return {
      stdout: stripAnsi(stdout),
      stderr: stripAnsi(stderr),
      exitCode: 0,
    };
  } catch (error: unknown) {
    const execError = error as {
      stdout?: string;
      stderr?: string;
      code?: number | string;
      killed?: boolean;
      signal?: string;
    };

    if (execError.killed || execError.signal === "SIGTERM") {
      throw new Error(
        `Cortex command timed out after ${options?.timeoutMs ?? DEFAULT_TIMEOUT_MS}ms: cortex ${fullArgs.join(" ")}`
      );
    }

    const code = execError.code;

    // Infrastructure errors — not a cortex command failure
    if (code === "ENOENT") {
      throw new Error(
        "Cortex CLI binary not found. It may have been uninstalled or PATH changed."
      );
    }
    if (code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER") {
      throw new Error(
        `Cortex command output exceeded buffer limit (10MB): cortex ${fullArgs.join(" ")}`
      );
    }
    if (typeof code === "string") {
      throw new Error(
        `Cortex command failed with system error ${code}: cortex ${fullArgs.join(" ")}`
      );
    }

    // Normal non-zero exit from the cortex process
    return {
      stdout: stripAnsi(execError.stdout ?? ""),
      stderr: stripAnsi(execError.stderr ?? ""),
      exitCode: typeof code === "number" ? code : 1,
    };
  }
}

/**
 * Asks Cortex Code a question in headless print mode.
 * Cortex handles SQL generation, Snowflake queries, Cortex AI — whatever the question needs.
 * Throws if the cortex command fails with a non-zero exit code.
 */
export async function askCortex(
  question: string,
  options?: { connection?: string; timeoutMs?: number }
): Promise<{ answer: string }> {
  const result = await runCortexCommand(
    ["-p", question, "--no-mcp"],
    { ...options, timeoutMs: options?.timeoutMs ?? DEFAULT_TIMEOUT_MS }
  );
  const answer = result.stdout.trim() || result.stderr.trim();
  if (result.exitCode !== 0) {
    throw new Error(
      `Cortex query failed (exit ${result.exitCode}): ${answer || "no output"}`
    );
  }
  return { answer };
}

export interface CortexConnection {
  account: string;
  user: string;
  role?: string;
}

export interface CortexConnections {
  activeConnection: string;
  connections: Record<string, CortexConnection>;
}

/**
 * Lists available Snowflake connections from cortex CLI.
 */
export async function listConnections(
  options?: { timeoutMs?: number }
): Promise<CortexConnections> {
  const result = await runCortexCommand(
    ["connections", "list"],
    { timeoutMs: options?.timeoutMs ?? 15_000 }
  );

  if (result.exitCode !== 0) {
    throw new Error(`Failed to list cortex connections: ${result.stderr}`);
  }

  let parsed: {
    active_connection: string;
    connections: Record<string, CortexConnection>;
  };
  try {
    parsed = JSON.parse(result.stdout);
  } catch {
    throw new Error(
      `Failed to parse cortex connections output as JSON. ` +
      `This usually means cortex requires authentication. ` +
      `Raw output: ${result.stdout.slice(0, 500)}`
    );
  }

  return {
    activeConnection: parsed.active_connection,
    connections: parsed.connections,
  };
}

/**
 * Searches Snowflake objects via cortex search.
 * Throws if the cortex command fails.
 */
export async function searchObjects(
  query: string,
  options?: { connection?: string; maxResults?: number; types?: string }
): Promise<{ results: string }> {
  const args = ["search", "object", query];
  if (options?.maxResults) {
    args.push("--max-results", String(options.maxResults));
  }
  if (options?.types) {
    args.push("--types", options.types);
  }
  const result = await runCortexCommand(args, {
    connection: options?.connection,
    timeoutMs: 30_000,
  });
  const output = result.stdout.trim() || result.stderr.trim();
  if (result.exitCode !== 0) {
    throw new Error(
      `Cortex search failed (exit ${result.exitCode}): ${output || "no output"}`
    );
  }
  return { results: output };
}

/**
 * Queries Cortex Analyst with a natural language question.
 * Throws if the cortex command fails.
 */
export async function analystQuery(
  question: string,
  options?: { connection?: string; view?: string; model?: string }
): Promise<{ answer: string }> {
  const args = ["analyst", "query", question];
  if (options?.view) {
    args.push("--view", options.view);
  }
  if (options?.model) {
    args.push("--model", options.model);
  }
  const result = await runCortexCommand(args, {
    connection: options?.connection,
    timeoutMs: 60_000,
  });
  const answer = result.stdout.trim() || result.stderr.trim();
  if (result.exitCode !== 0) {
    throw new Error(
      `Cortex analyst query failed (exit ${result.exitCode}): ${answer || "no output"}`
    );
  }
  return { answer };
}
