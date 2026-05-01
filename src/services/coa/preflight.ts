import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import YAML from "yaml";
import { safeErrorMessage } from "../../utils.js";
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
  warnings.push(...detectV2Artifacts(projectPath));
  for (const selector of options.selectors ?? []) {
    checkSelector(selector, errors);
  }

  return { errors, warnings };
}

/**
 * Scan for V2 artifacts (fileVersion: 2 node types, .sql nodes) and return the
 * warnings to append. V2 is currently in the `@next` COA alpha channel — the
 * warning points the agent at the policy resource before it edits or executes
 * anything in a V2 project.
 *
 * Returns an empty array when the project is confirmed V1 (scan completed, no
 * V2 artifacts found). Returns a `V2_SCAN_FAILED` warning when a scan failure
 * made the counts unreliable — "couldn't read" is not the same as "nothing
 * there," and the hard guard in coa_create/coa_run depends on this distinction.
 *
 * Exported so coa_doctor can attach the same warnings without running the full
 * preflight.
 */
export function detectV2Artifacts(projectPath: string): PreflightIssue[] {
  const nodeTypesResult = countV2NodeTypes(projectPath);
  const sqlNodesResult = countSqlNodes(projectPath);

  const scanErrors = [...nodeTypesResult.errors, ...sqlNodesResult.errors];
  const totalCount = nodeTypesResult.count + sqlNodesResult.count;

  const out: PreflightIssue[] = [];

  if (totalCount > 0) {
    const parts: string[] = [];
    if (nodeTypesResult.count > 0) {
      parts.push(
        `${nodeTypesResult.count} \`fileVersion: 2\` node type${nodeTypesResult.count === 1 ? "" : "s"}`
      );
    }
    if (sqlNodesResult.count > 0) {
      parts.push(
        `${sqlNodesResult.count} \`.sql\` node${sqlNodesResult.count === 1 ? "" : "s"}`
      );
    }
    out.push({
      level: "warning",
      code: "V2_ALPHA_DETECTED",
      message:
        `V2 artifacts detected (${parts.join(", ")}). V2 SQL nodes + fileVersion: 2 node types ship in the \`@next\` COA channel and are not yet GA — known rough edges include silent validate false positives, UNION ALL dropped from the body, and UI/CLI divergence on required config fields. Surface this to the user before editing or executing anything V2-related. See \`coalesce://context/sql-node-v2-policy\`.`,
      path: projectPath,
    });
  }

  // Surface scan failures even when count > 0 — partial reads can undercount,
  // and the hard guard should not treat a partial scan that found nothing as
  // indistinguishable from a clean V1 project.
  if (scanErrors.length > 0) {
    out.push({
      level: "warning",
      code: "V2_SCAN_FAILED",
      message:
        `Could not fully scan for V2 artifacts (${scanErrors.join("; ")}). Counts may be under-reported; treat this project as potentially V2 until re-scanned. Investigate the filesystem error (permissions, broken symlink, etc.) before running coa_create / coa_run.`,
      path: projectPath,
    });
  }

  return out;
}

type CountResult = { count: number; errors: string[] };

function countV2NodeTypes(projectPath: string): CountResult {
  const nodeTypesDir = join(projectPath, "nodeTypes");
  if (!existsSync(nodeTypesDir)) return { count: 0, errors: [] };
  let entries: string[];
  try {
    entries = readdirSync(nodeTypesDir);
  } catch (err) {
    return {
      count: 0,
      errors: [
        `nodeTypes/ readdir failed: ${safeErrorMessage(err)}`,
      ],
    };
  }
  let count = 0;
  const errors: string[] = [];
  for (const entry of entries) {
    const definitionPath = join(nodeTypesDir, entry, "definition.yml");
    if (!existsSync(definitionPath)) continue;
    let raw: string;
    try {
      raw = readFileSync(definitionPath, "utf8");
    } catch (err) {
      errors.push(
        `nodeTypes/${entry}/definition.yml read failed: ${safeErrorMessage(err)}`
      );
      continue;
    }
    if (/^\s*fileVersion\s*:\s*2\s*$/m.test(raw)) count += 1;
  }
  return { count, errors };
}

function countSqlNodes(projectPath: string): CountResult {
  const nodesDir = join(projectPath, "nodes");
  if (!existsSync(nodesDir)) return { count: 0, errors: [] };
  const files: string[] = [];
  const errors: string[] = [];
  const hitCap = collectSqlFilesWithErrors(nodesDir, files, errors);
  if (hitCap) {
    // Partial scan — the V2 hard guard depends on "couldn't rule out V2" vs
    // "no V2 found" being distinguishable. A truncated scan that returns a
    // clean count must surface as a scan failure, not a V1-clean project.
    errors.push(
      `hit MAX_SQL_FILES_SCANNED cap (${MAX_SQL_FILES_SCANNED}); scan may be incomplete`
    );
  }
  return { count: files.length, errors };
}

function collectSqlFilesWithErrors(
  directory: string,
  out: string[],
  errors: string[]
): boolean {
  if (out.length >= MAX_SQL_FILES_SCANNED) return true;
  let names: string[];
  try {
    names = readdirSync(directory);
  } catch (err) {
    errors.push(
      `${directory} readdir failed: ${safeErrorMessage(err)}`
    );
    return false;
  }
  for (const name of names) {
    if (out.length >= MAX_SQL_FILES_SCANNED) return true;
    const entryPath = join(directory, name);
    let stat: ReturnType<typeof statSync>;
    try {
      stat = statSync(entryPath);
    } catch (err) {
      errors.push(
        `${entryPath} stat failed: ${safeErrorMessage(err)}`
      );
      continue;
    }
    if (stat.isDirectory()) {
      if (collectSqlFilesWithErrors(entryPath, out, errors)) return true;
    } else if (stat.isFile() && name.endsWith(".sql")) {
      out.push(entryPath);
    }
  }
  return false;
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
      message: `Could not read data.yml: ${safeErrorMessage(err)}`,
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
  } catch (err) {
    warnings.push({
      level: "warning",
      code: "LOCATIONS_YML_READ_FAILED",
      message: `locations.yml exists but could not be read: ${safeErrorMessage(err)}. Check file permissions.`,
      path,
    });
    return;
  }
  let parsed: unknown;
  try {
    parsed = YAML.parse(raw);
  } catch (err) {
    warnings.push({
      level: "warning",
      code: "LOCATIONS_YML_PARSE_FAILED",
      message: `Could not parse locations.yml: ${safeErrorMessage(err)}`,
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
  } catch (err) {
    warnings.push({
      level: "warning",
      code: "WORKSPACES_YML_READ_FAILED",
      message: `workspaces.yml exists but could not be read: ${safeErrorMessage(err)}. Check file permissions.`,
      path,
    });
    return;
  }
  let parsed: unknown;
  try {
    parsed = YAML.parse(raw);
  } catch (err) {
    warnings.push({
      level: "warning",
      code: "WORKSPACES_YML_PARSE_FAILED",
      message: `Could not parse workspaces.yml: ${safeErrorMessage(err)}`,
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
    } catch (err) {
      warnings.push({
        level: "warning",
        code: "GITIGNORE_READ_FAILED",
        message: `.gitignore exists but could not be read: ${safeErrorMessage(err)}. Cannot confirm whether workspaces.yml is ignored — check file permissions.`,
        path: gitignorePath,
      });
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
    // Intentional: callers needing the read/parse failure surfaced go through
    // checkLocationsYml, which emits LOCATIONS_YML_READ_FAILED /
    // LOCATIONS_YML_PARSE_FAILED. Cross-ref callers (checkWorkspacesYml) treat
    // "no declared names" the same as "file missing" and skip the lookup.
    return [];
  }
  let parsed: unknown;
  try {
    parsed = YAML.parse(raw);
  } catch {
    // Intentional: see comment above — parse failures are reported by
    // checkLocationsYml, not by this lookup helper.
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
      // Intentional: best-effort scan over potentially hundreds of files —
      // per-file read errors (permissions, unlinked mid-scan) shouldn't block
      // the preflight. The file is simply skipped.
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
    // Intentional: best-effort traversal. Inaccessible directories are skipped
    // rather than aborting the whole preflight.
    return false;
  }
  for (const name of names) {
    if (out.length >= MAX_SQL_FILES_SCANNED) return true;
    const entryPath = join(directory, name);
    let stat: ReturnType<typeof statSync>;
    try {
      stat = statSync(entryPath);
    } catch {
      // Intentional: a single unstattable entry (broken symlink, permissions)
      // shouldn't abort the scan. Skip it.
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
 * separate braces around each OR operand joined by the `OR` keyword:
 * `{ A } OR { B }`. The combined form silently matches zero nodes with no
 * error. (See `parseJobSelector` grammar in src/services/jobs/selector-parser.ts.)
 *
 * Catches the footgun in any brace pair, not just when it is the whole
 * selector — `{ A } OR { B || C }` and `{ X || Y } OR { location: SRC name: Z }`
 * would otherwise pass preflight while silently dropping the bad term.
 */
function checkSelector(
  selector: string | undefined,
  errors: PreflightIssue[]
): void {
  if (!selector) return;
  const trimmed = selector.trim();
  if (!trimmed) return;
  // Strip quoted strings so legitimate quoted names like
  // `{ subgraph: "A||B" }` don't false-positive.
  const withoutQuotes = trimmed.replace(/"[^"]*"|'[^']*'/g, "");
  // Any brace pair (top-level or one of several) that contains `||` is wrong.
  const bracePairWithOr = /\{[^{}]*\|\|[^{}]*\}/;
  if (bracePairWithOr.test(withoutQuotes)) {
    errors.push({
      level: "error",
      code: "SELECTOR_COMBINED_OR",
      message:
        `Selector "${trimmed}" uses \`{ A || B }\` form inside at least one brace pair, which silently matches zero nodes. Use \`{ A } OR { B }\` (separate braces around each OR operand).`,
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
