import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { resolveCoaBinary } from "./resolver.js";
import { runCoa, type RunCoaResult } from "./runner.js";

/**
 * Top-level `coa describe` topics we expose as MCP resources. Must match
 * COA's supported topic list — see `coa describe` help output.
 *
 * Topics with required subtopics (`command <name>`, `schema <type>`) are not
 * in this list; they are reachable through the coa_describe tool with a
 * subtopic arg.
 */
export const COA_DESCRIBE_TOPICS = [
  "overview", // the bare `coa describe` output
  "commands",
  "selectors",
  "schemas",
  "workflow",
  "structure",
  "concepts",
  "sql-format",
  "node-types",
  "config",
] as const;

export type CoaDescribeTopic = (typeof COA_DESCRIBE_TOPICS)[number];

export function isCoaDescribeTopic(value: string): value is CoaDescribeTopic {
  return (COA_DESCRIBE_TOPICS as readonly string[]).includes(value);
}

type RunCoaFn = typeof runCoa;

export type FetchDescribeOptions = {
  /** Optional subtopic (e.g., command name for `command <name>`). */
  subtopic?: string;
  /** Force refresh — bypass on-disk and in-memory caches. */
  refresh?: boolean;
  /** Test injection points. */
  runCoaFn?: RunCoaFn;
  getVersion?: () => string | null;
  cacheBaseDir?: string;
};

export type FetchDescribeResult = {
  topic: string;
  subtopic?: string;
  content: string;
  source: "memory" | "disk" | "coa";
  coaVersion: string | null;
};

// Process-lifetime in-memory cache. Keyed by `<version>::<topic>[::<subtopic>]`.
const memoryCache = new Map<string, FetchDescribeResult>();

/** Test-only. */
export function resetCoaDescribeMemoryCache(): void {
  memoryCache.clear();
}

/**
 * Correction banner prepended to `sql-format` output when the upstream CLI
 * text references dbt's `source()` macro. CD-16981 tracks the upstream fix;
 * until it ships, agents reading our `coa_describe` output get `source()`
 * inline in example SQL and cite it back to users. We prepend a banner (never
 * mutate the CLI output) so the correction is visible at the top of context.
 * Exported for tests.
 */
export const SQL_FORMAT_DBT_SOURCE_BANNER = [
  "> **Coalesce uses `ref()`, not dbt's `source()`.** The COA CLI output below",
  "> currently mentions `source(...)` in examples — that is an upstream bug",
  "> (CD-16981). In Coalesce SQL nodes, reference external tables via",
  "> `{{ ref('LOCATION', 'NAME') }}` (single-quoted). Do not author or suggest",
  "> `source()` — it is not a Coalesce concept.",
  "",
  "",
].join("\n");

/**
 * Apply Coalesce-specific corrections to raw `coa describe` output. Runs at
 * read time (not cache-write time) so updates to the correction logic take
 * effect without invalidating disk caches.
 *
 * Exported for tests.
 */
export function applyCoalesceCorrections(topic: string, content: string): string {
  // Case-insensitive `source(` match — upstream stylistic shifts (e.g.,
  // `Source(`, markdown wrappers) should not silently drop the banner. Widening
  // here reduces the chance of a silent correction failure if CD-16981 reshapes
  // the CLI text before fully removing the dbt macro references.
  if (topic === "sql-format" && /\bsource\s*\(/i.test(content)) {
    return SQL_FORMAT_DBT_SOURCE_BANNER + content;
  }
  return content;
}

function withCorrections(result: FetchDescribeResult): FetchDescribeResult {
  const corrected = applyCoalesceCorrections(result.topic, result.content);
  if (corrected === result.content) return result;
  return { ...result, content: corrected };
}

/**
 * Fetch a `coa describe <topic> [<subtopic>]` section. Caches results on disk
 * (keyed by COA version) and in memory.
 *
 * Throws if COA is unreachable and no cached copy exists. Callers that need
 * graceful degradation (resource handlers) should catch and fall back to a
 * placeholder.
 */
export async function fetchDescribeTopic(
  topic: string,
  options: FetchDescribeOptions = {}
): Promise<FetchDescribeResult> {
  const subtopic = options.subtopic?.trim() || undefined;
  const runCoaFn = options.runCoaFn ?? runCoa;
  const getVersion = options.getVersion ?? defaultGetVersion;
  const coaVersion = getVersion();
  const cacheKey = buildCacheKey(coaVersion, topic, subtopic);

  if (!options.refresh) {
    const memHit = memoryCache.get(cacheKey);
    if (memHit) return withCorrections({ ...memHit, source: "memory" });
  }

  const diskPath = coaVersion
    ? buildDiskPath(options.cacheBaseDir, coaVersion, topic, subtopic)
    : null;

  if (!options.refresh && diskPath && existsSync(diskPath)) {
    try {
      const content = readFileSync(diskPath, "utf8");
      const result: FetchDescribeResult = {
        topic,
        subtopic,
        content,
        source: "disk",
        coaVersion,
      };
      memoryCache.set(cacheKey, result);
      return withCorrections(result);
    } catch {
      // Unreadable cache entry — fall through to COA fetch.
    }
  }

  // Special case: the "overview" topic is what `coa describe` (with no topic)
  // prints. COA does not accept `describe overview` as an argument.
  const args =
    topic === "overview"
      ? ["--no-color", "describe"]
      : ["--no-color", "describe", topic];
  if (subtopic && topic !== "overview") args.push(subtopic);
  const runResult = await runCoaFn(args, { timeoutMs: 20_000 });
  assertRunSuccess(topic, subtopic, runResult);

  const content = runResult.stdout;
  const result: FetchDescribeResult = {
    topic,
    subtopic,
    content,
    source: "coa",
    coaVersion,
  };
  memoryCache.set(cacheKey, result);

  if (diskPath) {
    try {
      mkdirSync(join(diskPath, ".."), { recursive: true });
      writeFileSync(diskPath, content, "utf8");
    } catch {
      // Disk cache is best-effort — memory cache is still populated.
    }
  }

  return withCorrections(result);
}

function defaultGetVersion(): string | null {
  try {
    return resolveCoaBinary().version;
  } catch {
    return null;
  }
}

function buildCacheKey(
  version: string | null,
  topic: string,
  subtopic: string | undefined
): string {
  const v = version ?? "unknown";
  return subtopic ? `${v}::${topic}::${subtopic}` : `${v}::${topic}`;
}

function buildDiskPath(
  baseDirOverride: string | undefined,
  version: string,
  topic: string,
  subtopic: string | undefined
): string | null {
  const baseDir = baseDirOverride ?? resolveCacheBaseDir();
  if (!baseDir) return null;
  const versionDir = join(baseDir, sanitizePathSegment(version));
  const fileName = subtopic
    ? `${sanitizePathSegment(topic)}__${sanitizePathSegment(subtopic)}.md`
    : `${sanitizePathSegment(topic)}.md`;
  return join(versionDir, fileName);
}

/**
 * Pick the first writable cache root in priority order:
 *   1. $XDG_CACHE_HOME/coalesce-transform-mcp/coa-describe
 *   2. ~/.cache/coalesce-transform-mcp/coa-describe
 *   3. $TMPDIR/coalesce-transform-mcp-coa-describe
 *
 * Returns null if every candidate fails — callers degrade to memory-only.
 */
function resolveCacheBaseDir(): string | null {
  const candidates: string[] = [];
  // Bracket syntax intentional — this is a dev/test escape hatch, not a
  // first-class configured env var, so we keep it out of the server-metadata
  // manifest that's scanned for dot-access env references.
  const override = process.env["COALESCE_MCP_COA_DESCRIBE_CACHE_DIR"];
  if (override) candidates.push(override);
  const xdg = process.env["XDG_CACHE_HOME"];
  if (xdg) candidates.push(join(xdg, "coalesce-transform-mcp", "coa-describe"));
  candidates.push(join(homedir(), ".cache", "coalesce-transform-mcp", "coa-describe"));
  candidates.push(join(tmpdir(), "coalesce-transform-mcp-coa-describe"));

  for (const candidate of candidates) {
    try {
      mkdirSync(candidate, { recursive: true });
      return candidate;
    } catch {
      // Try next candidate.
    }
  }
  return null;
}

function sanitizePathSegment(segment: string): string {
  return segment.replace(/[^a-zA-Z0-9._-]/g, "_");
}

export class CoaDescribeError extends Error {
  constructor(
    public readonly topic: string,
    public readonly subtopic: string | undefined,
    public readonly runResult: RunCoaResult,
    message: string
  ) {
    super(message);
    this.name = "CoaDescribeError";
  }
}

function assertRunSuccess(
  topic: string,
  subtopic: string | undefined,
  runResult: RunCoaResult
): void {
  if (runResult.exitCode === 0 && !runResult.timedOut) return;
  const label = subtopic ? `${topic} ${subtopic}` : topic;
  const tail =
    runResult.stderr.trim() || runResult.stdout.trim() || "no output";
  const reason = runResult.timedOut
    ? "timed out"
    : `exit ${runResult.exitCode}`;
  throw new CoaDescribeError(
    topic,
    subtopic,
    runResult,
    `coa describe ${label} failed (${reason}): ${tail.slice(0, 300)}`
  );
}
