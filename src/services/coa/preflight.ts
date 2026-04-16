import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";

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
  if (options.requireWorkspacesYml) {
    checkWorkspacesYml(projectPath, errors);
  }
  scanSqlFiles(projectPath, errors, warnings);
  for (const selector of options.selectors ?? []) {
    checkSelector(selector, errors);
  }

  return { errors, warnings };
}

export class CoaPreflightError extends Error {
  constructor(public readonly report: PreflightReport) {
    super(
      `coa preflight failed with ${report.errors.length} error(s):\n` +
        report.errors.map((e) => `  [${e.code}] ${e.message}`).join("\n")
    );
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

function checkWorkspacesYml(
  projectPath: string,
  errors: PreflightIssue[]
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
  }
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
