import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import YAML from "yaml";
import { SETUP_HINT } from "../setup/hint.js";

export type PreflightIssue = {
  level: "error" | "warning";
  code: string;
  message: string;
  /** Path that surfaced the issue, when known. */
  path?: string;
};

export type PreflightOptions = {
  /** If true, require workspaces.yml to exist (local execute commands). */
  requireWorkspacesYml?: boolean;
  /** If provided, selectors to validate for the common `{ A || B }` footgun. */
  selectors?: Array<string | undefined>;
};

export type PreflightReport = {
  errors: PreflightIssue[];
  warnings: PreflightIssue[];
};

const MAX_SQL_FILES_SCANNED = 500;

/**
 * Validate a COA project for known pitfalls before shelling out to a
 * destructive command. See docs/COA-INTEGRATION-PLAN.md Phase 4 for the
 * pitfall catalog.
 */
export function runPreflight(
  projectPath: string,
  options: PreflightOptions = {}
): PreflightReport {
  const errors: PreflightIssue[] = [];
  const warnings: PreflightIssue[] = [];

  checkDataYml(projectPath, errors, warnings);
  checkLocationsYml(projectPath, warnings);
  if (options.requireWorkspacesYml) {
    checkWorkspacesYml(projectPath, errors, warnings);
  } else {
    checkWorkspacesYmlShape(projectPath, warnings);
  }
  scanSqlFiles(projectPath, errors, warnings);
  for (const selector of options.selectors ?? []) {
    checkSelector(selector, errors);
  }

  return { errors, warnings };
}

/** Error codes whose fix lives in the `/coalesce-setup` flow. */
const SETUP_LINKED_CODES = new Set(["WORKSPACES_YML_MISSING"]);

export class CoaPreflightError extends Error {
  constructor(public readonly report: PreflightReport) {
    const body =
      `coa preflight failed with ${report.errors.length} error(s):\n` +
      report.errors.map((e) => `  [${e.code}] ${e.message}`).join("\n");
    const hasSetupLinked = report.errors.some((e) => SETUP_LINKED_CODES.has(e.code));
    // Paragraph break before the hint — the body is a multi-line bullet list.
    super(hasSetupLinked ? `${body}\n\n${SETUP_HINT}` : body);
    this.name = "CoaPreflightError";
  }
}

function checkDataYml(
  projectPath: string,
  errors: PreflightIssue[],
  warnings: PreflightIssue[]
): void {
  const path = join(projectPath, "data.yml");
  // validateProjectPath already confirmed this file exists. Read for content checks.
  let contents: string;
  try {
    contents = readFileSync(path, "utf8");
  } catch (err) {
    errors.push({
      level: "error",
      code: "DATA_YML_READ_FAILED",
      message: `Could not read data.yml: ${err instanceof Error ? err.message : String(err)}`,
      path,
    });
    return;
  }

  const match = contents.match(/^\s*fileVersion\s*:\s*(\d+)\s*$/m);
  if (!match) {
    warnings.push({
      level: "warning",
      code: "DATA_YML_NO_FILEVERSION",
      message: "data.yml has no fileVersion field. V2 SQL nodes require fileVersion: 3.",
      path,
    });
    return;
  }

  const version = Number.parseInt(match[1] ?? "", 10);
  if (version !== 3) {
    warnings.push({
      level: "warning",
      code: "DATA_YML_UNEXPECTED_FILEVERSION",
      message: `data.yml fileVersion is ${version}; coa's V2 SQL node workflow expects fileVersion: 3.`,
      path,
    });
  }
}

/**
 * Warn when locations.yml exists but cannot be parsed or has an invalid shape.
 * Without this, a malformed locations.yml silently disables the workspaces.yml
 * cross-reference check (WORKSPACES_YML_UNKNOWN_LOCATION) because
 * readLocationNames returns [].
 */
function checkLocationsYml(
  projectPath: string,
  warnings: PreflightIssue[]
): void {
  const path = join(projectPath, "locations.yml");
  if (!existsSync(path)) return;
  let raw: string;
  try {
    raw = readFileSync(path, "utf8");
  } catch {
    return;
  }
  let parsed: unknown;
  try {
    parsed = YAML.parse(raw);
  } catch (err) {
    warnings.push({
      level: "warning",
      code: "LOCATIONS_YML_PARSE_FAILED",
      message: `Could not parse locations.yml: ${err instanceof Error ? err.message : String(err)}`,
      path,
    });
    return;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    warnings.push({
      level: "warning",
      code: "LOCATIONS_YML_INVALID_SHAPE",
      message:
        "locations.yml should be a map of location names → config. See `coa describe schema locations`.",
      path,
    });
  }
}

function checkWorkspacesYml(
  projectPath: string,
  errors: PreflightIssue[],
  warnings: PreflightIssue[]
): void {
  const path = join(projectPath, "workspaces.yml");
  if (!existsSync(path)) {
    errors.push({
      level: "error",
      code: "WORKSPACES_YML_MISSING",
      message:
        "workspaces.yml is required for local create/run/validate. Create it in the project root with storage-location mappings. Run `coa doctor --fix` to bootstrap.",
      path,
    });
    return;
  }
  checkWorkspacesYmlShape(projectPath, warnings);
}

/**
 * Parse-check an existing `workspaces.yml` for the common typos:
 *  - top-level `workspaces:` wrapper (schema is a flat map of workspace names)
 *  - top-level `fileVersion` (schema has none)
 *  - `storageLocations` field (correct field is `locations`)
 *  - workspace block missing `connection` (required)
 *  - location keys that don't appear in locations.yml (typos / stale renames)
 *  - file not gitignored (contains per-developer database names)
 * Issues are warnings, not errors — `coa` itself will reject genuinely broken
 * files, but typos silently produce confusing "workspace not found" behaviour.
 */
function checkWorkspacesYmlShape(
  projectPath: string,
  warnings: PreflightIssue[]
): void {
  const path = join(projectPath, "workspaces.yml");
  if (!existsSync(path)) return;
  checkWorkspacesYmlGitignore(projectPath, path, warnings);
  let raw: string;
  try {
    raw = readFileSync(path, "utf8");
  } catch {
    return;
  }
  let parsed: unknown;
  try {
    parsed = YAML.parse(raw);
  } catch (err) {
    warnings.push({
      level: "warning",
      code: "WORKSPACES_YML_PARSE_FAILED",
      message: `Could not parse workspaces.yml: ${err instanceof Error ? err.message : String(err)}`,
      path,
    });
    return;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    warnings.push({
      level: "warning",
      code: "WORKSPACES_YML_INVALID_SHAPE",
      message:
        "workspaces.yml should be a map of workspace names → config. See `coa describe schema workspaces`.",
      path,
    });
    return;
  }
  const record = parsed as Record<string, unknown>;
  if ("fileVersion" in record) {
    warnings.push({
      level: "warning",
      code: "WORKSPACES_YML_UNEXPECTED_FILEVERSION",
      message:
        "workspaces.yml has a `fileVersion` field, which is not in the schema. Remove it — the file is a flat map of workspace names.",
      path,
    });
  }
  if ("workspaces" in record && record.workspaces && typeof record.workspaces === "object") {
    warnings.push({
      level: "warning",
      code: "WORKSPACES_YML_NESTED_WRAPPER",
      message:
        "workspaces.yml has a top-level `workspaces:` wrapper, but the schema expects workspace names at the top level. Remove the wrapper so your workspace keys (e.g., `dev:`) sit at the root.",
      path,
    });
  }
  // Cross-reference skips when locations.yml is missing, malformed, or empty —
  // readLocationNames returns [] for all three. Absence of declared names is
  // not evidence of a typo in workspaces.yml. Malformed locations.yml gets its
  // own LOCATIONS_YML_PARSE_FAILED warning so the user isn't left in the dark.
  const declared = readLocationNames(projectPath);
  const declaredLocations = declared.length > 0 ? new Set(declared) : null;
  for (const [name, value] of Object.entries(record)) {
    if (name === "fileVersion" || name === "workspaces") continue;
    if (!value || typeof value !== "object" || Array.isArray(value)) continue;
    const block = value as Record<string, unknown>;
    if ("storageLocations" in block) {
      warnings.push({
        level: "warning",
        code: "WORKSPACES_YML_WRONG_LOCATIONS_KEY",
        message: `Workspace \`${name}\` uses \`storageLocations\` — the schema field is \`locations\`. Rename to fix.`,
        path,
      });
    }
    if (!("connection" in block) || typeof block.connection !== "string" || !block.connection.trim()) {
      warnings.push({
        level: "warning",
        code: "WORKSPACES_YML_MISSING_CONNECTION",
        message: `Workspace \`${name}\` has no \`connection\` field. Each workspace requires a connection name (e.g., \`connection: snowflake\`).`,
        path,
      });
    }
    if (declaredLocations !== null && "locations" in block) {
      const locations = block.locations;
      if (locations && typeof locations === "object" && !Array.isArray(locations)) {
        for (const locKey of Object.keys(locations as Record<string, unknown>)) {
          if (!declaredLocations.has(locKey)) {
            warnings.push({
              level: "warning",
              code: "WORKSPACES_YML_UNKNOWN_LOCATION",
              message: `Workspace \`${name}\` references location \`${locKey}\`, which is not declared in locations.yml. Check for a typo or a renamed location.`,
              path,
            });
          }
        }
      }
    }
  }
}

/**
 * Warn when workspaces.yml exists but is not listed in .gitignore. The file
 * contains per-developer database/schema names and should not be committed.
 * A missing .gitignore is treated as "not ignored" — flag it so the user is
 * deliberate about one of: add .gitignore, add workspaces.yml to it, or (rare)
 * commit the file.
 */
function checkWorkspacesYmlGitignore(
  projectPath: string,
  workspacesPath: string,
  warnings: PreflightIssue[]
): void {
  const gitignorePath = join(projectPath, ".gitignore");
  let contents: string;
  if (!existsSync(gitignorePath)) {
    // No .gitignore at all — can't confirm the file is ignored. Silent when the
    // project is not a git repo; flag when it is, because an unignored
    // workspaces.yml in a git-tracked project will end up committed.
    if (!existsSync(join(projectPath, ".git"))) return;
    contents = "";
  } else {
    try {
      contents = readFileSync(gitignorePath, "utf8");
    } catch {
      return;
    }
  }
  if (hasWorkspacesYmlIgnoreHint(contents)) return;
  warnings.push({
    level: "warning",
    code: "WORKSPACES_YML_NOT_GITIGNORED",
    message:
      "No obvious workspaces.yml ignore rule detected in .gitignore. This file typically contains per-developer database/schema names and should not be committed. Add `workspaces.yml` to .gitignore, or commit deliberately if your team checks it in.",
    path: workspacesPath,
  });
}

/**
 * Heuristic: does .gitignore appear to cover workspaces.yml? True if any
 * non-negated line references the file by name, a broad *.yml/*.yaml glob, or
 * a literal wildcard. Does not evaluate full gitignore semantics (negations in
 * order, nested .gitignore files, `git check-ignore`) — treats negations as
 * "can't tell" and lets the warning fire. Errs on the side of false positives
 * (harmless advisory warning) rather than false negatives (silently allowing
 * a committed workspaces.yml).
 */
function hasWorkspacesYmlIgnoreHint(gitignoreContents: string): boolean {
  for (const rawLine of gitignoreContents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#") || line.startsWith("!")) continue;
    if (
      line === "*" ||
      line === "*.yml" ||
      line === "*.yaml" ||
      line === "workspaces.*" ||
      line.endsWith("workspaces.yml") ||
      line.endsWith("workspaces.yaml")
    ) {
      return true;
    }
  }
  return false;
}

/**
 * Read locations.yml and return declared location names. Returns [] when the
 * file is missing, unreadable, or malformed — callers distinguish those cases
 * via an explicit `existsSync` check when the difference matters.
 */
export function readLocationNames(projectPath: string): string[] {
  const locationsPath = join(projectPath, "locations.yml");
  if (!existsSync(locationsPath)) return [];
  let raw: string;
  try {
    raw = readFileSync(locationsPath, "utf8");
  } catch {
    return [];
  }
  let parsed: unknown;
  try {
    parsed = YAML.parse(raw);
  } catch {
    return [];
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return [];
  const record = parsed as Record<string, unknown>;
  const source =
    record.locations && typeof record.locations === "object" && !Array.isArray(record.locations)
      ? (record.locations as Record<string, unknown>)
      : record;
  return Object.keys(source).filter((key) => key !== "fileVersion");
}

function scanSqlFiles(
  projectPath: string,
  errors: PreflightIssue[],
  warnings: PreflightIssue[]
): void {
  const nodesDir = join(projectPath, "nodes");
  if (!existsSync(nodesDir)) return;

  const files: string[] = [];
  const hitCap = collectSqlFiles(nodesDir, files);

  for (const filePath of files.slice(0, MAX_SQL_FILES_SCANNED)) {
    let content: string;
    try {
      content = readFileSync(filePath, "utf8");
    } catch {
      continue;
    }
    inspectSqlFile(filePath, content, errors, warnings);
  }

  if (hitCap) {
    // File-count cap is a safety valve, not a quality signal. Tell the agent
    // the scan was partial so it can decide whether the missing files matter.
    warnings.push({
      level: "warning",
      code: "PREFLIGHT_SCAN_TRUNCATED",
      message: `More than ${MAX_SQL_FILES_SCANNED} .sql files under nodes/ — preflight scanned the first ${MAX_SQL_FILES_SCANNED} in filesystem order and skipped the rest. SQL footgun checks (double-quoted ref, literal UNION ALL) may miss issues outside the scanned set.`,
      path: nodesDir,
    });
  }
}

/**
 * Returns true when collection stopped because MAX_SQL_FILES_SCANNED was hit
 * (i.e., there are more files on disk than we scanned). False otherwise.
 */
function collectSqlFiles(directory: string, out: string[]): boolean {
  if (out.length >= MAX_SQL_FILES_SCANNED) return true;
  let names: string[];
  try {
    names = readdirSync(directory);
  } catch {
    return false;
  }
  for (const name of names) {
    if (out.length >= MAX_SQL_FILES_SCANNED) return true;
    const entryPath = join(directory, name);
    let stat: ReturnType<typeof statSync>;
    try {
      stat = statSync(entryPath);
    } catch {
      continue;
    }
    if (stat.isDirectory()) {
      if (collectSqlFiles(entryPath, out)) return true;
    } else if (stat.isFile() && name.endsWith(".sql")) {
      out.push(entryPath);
    }
  }
  return false;
}

function inspectSqlFile(
  filePath: string,
  content: string,
  errors: PreflightIssue[],
  warnings: PreflightIssue[]
): void {
  // Double-quoted ref() silently breaks lineage. Single-quoted is the only
  // supported form. Match: ref("…") or ref("…","…") or ref( "…" ...).
  if (/\bref\s*\(\s*"/.test(content)) {
    errors.push({
      level: "error",
      code: "SQL_DOUBLE_QUOTED_REF",
      message:
        `SQL node uses double-quoted ref(). coa silently treats this as an unresolved reference and columns come back as UNKNOWN. Switch to single quotes: ref('LOCATION', 'NAME').`,
      path: filePath,
    });
  }

  // Literal UNION ALL outside comments is silently dropped by the V2 parser —
  // only the first SELECT is captured. Users should configure insertStrategy
  // instead. Strip block/line comments before checking to reduce false positives.
  const stripped = content
    .replace(/--[^\n]*/g, "")
    .replace(/\/\*[\s\S]*?\*\//g, "");
  if (/\bUNION\s+ALL\b/i.test(stripped)) {
    warnings.push({
      level: "warning",
      code: "SQL_LITERAL_UNION_ALL",
      message:
        "SQL node contains literal UNION ALL. The V2 parser captures only the first SELECT; data from subsequent branches is silently dropped. Use the `insertStrategy: UNION ALL` config in the node type instead.",
      path: filePath,
    });
  }
}

/**
 * Detect the common `{ A || B }` selector footgun. Coalesce selectors require
 * separate braces around each OR operand: `{ A } || { B }`. The combined form
 * silently matches zero nodes with no error.
 */
function checkSelector(
  selector: string | undefined,
  errors: PreflightIssue[]
): void {
  if (!selector) return;
  const trimmed = selector.trim();
  if (!trimmed) return;
  // A single brace pair containing `||` is always wrong.
  const singlePairWithOr = /^{[^{}]*\|\|[^{}]*}$/;
  if (singlePairWithOr.test(trimmed)) {
    errors.push({
      level: "error",
      code: "SELECTOR_COMBINED_OR",
      message:
        `Selector "${trimmed}" uses \`{ A || B }\` form which silently matches zero nodes. Use \`{ A } || { B }\` (separate braces around each OR operand).`,
    });
  }
}

/**
 * Utility used by validateProjectPath callers that also want a full preflight.
 * Exposed for tests.
 */
export function summarizePreflight(report: PreflightReport): string {
  const lines: string[] = [];
  for (const error of report.errors) {
    lines.push(`ERROR [${error.code}]: ${error.message}${error.path ? ` (${error.path})` : ""}`);
  }
  for (const warning of report.warnings) {
    lines.push(
      `WARN  [${warning.code}]: ${warning.message}${warning.path ? ` (${warning.path})` : ""}`
    );
  }
  return lines.join("\n");
}

// Re-exported helper so handlers can do a quick existence check without
// importing from node:fs themselves.
export function pathExists(path: string): boolean {
  try {
    return existsSync(path) && statSync(path).isFile();
  } catch {
    return false;
  }
}
