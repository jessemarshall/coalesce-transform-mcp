import { type CoalesceClient } from "../../client.js";
import { isPlainObject, rethrowNonRecoverableApiError } from "../../utils.js";
import { validatePathSegment } from "../../coalesce/types.js";

// ── Types ────────────────────────────────────────────────────────────────────

export type FailureCategory =
  | "sql_error"
  | "reference_error"
  | "permission_error"
  | "data_type_error"
  | "timeout"
  | "missing_object"
  | "configuration_error"
  | "unknown";

export interface NodeDiagnosis {
  nodeID: string;
  nodeName: string | null;
  nodeType: string | null;
  status: string;
  category: FailureCategory;
  errorMessage: string | null;
  suggestedFixes: string[];
}

export interface RunDiagnosis {
  runID: string;
  analyzedAt: string;
  runStatus: string;
  runType: string | null;
  environmentID: string | null;
  startTime: string | null;
  endTime: string | null;
  summary: {
    totalNodes: number;
    succeeded: number;
    failed: number;
    skipped: number;
    canceled: number;
    other: number;
  };
  failures: NodeDiagnosis[];
  warnings: string[];
  recommendations: string[];
}

export interface DiagnoseRunInput {
  runID: string;
}

// ── Error classification ─────────────────────────────────────────────────────

const SQL_ERROR_PATTERNS: Array<[RegExp, string]> = [
  [/syntax error/i, "Check SQL syntax in the node's column transforms or join condition."],
  [/unexpected '([^']+)'/i, "Unexpected token in SQL — review the transform expressions."],
  [/invalid identifier/i, "A column or table name is invalid. Check for typos in column references."],
  [/ambiguous column/i, "Column name is ambiguous — qualify it with the table alias (e.g., \"TABLE\".\"COLUMN\")."],
  [/missing (comma|semicolon|parenthesis|bracket)/i, "Missing punctuation in SQL expression."],
  [/unexpected end of/i, "Incomplete SQL expression — check for unclosed parentheses or quotes."],
];

const REFERENCE_ERROR_PATTERNS: Array<[RegExp, string]> = [
  [/object '([^']+)' does not exist/i, "The referenced object doesn't exist. Verify the source node has been deployed."],
  [/table '([^']+)' does not exist/i, "Source table not found. Run a deploy to create it, or check the node name."],
  [/schema '([^']+)' does not exist/i, "Database schema not found. Check your storage location configuration."],
  [/database '([^']+)' does not exist/i, "Database not found. Verify the environment's storage mappings."],
  [/view '([^']+)' does not exist/i, "View not found. The source view may need to be deployed first."],
  [/could not resolve/i, "Unable to resolve a reference. Check node dependencies and deployment order."],
];

const PERMISSION_PATTERNS: Array<[RegExp, string]> = [
  [/insufficient privileges/i, "The Snowflake role lacks required privileges. Grant access or use a different role."],
  [/access denied/i, "Access denied. Check the Snowflake role's permissions on the target objects."],
  [/not authorized/i, "Authorization failed. Verify the credentials and role used for this environment."],
  [/warehouse '([^']+)' .*(suspended|does not exist)/i, "Snowflake warehouse is suspended or doesn't exist. Resume or update the warehouse setting."],
  [/role '([^']+)' .*(does not exist|is not authorized)/i, "Snowflake role not found or not authorized. Check the role in environment settings."],
  [/authentication/i, "Authentication failed. Check the Snowflake credentials (username, key pair, account)."],
];

const DATA_TYPE_PATTERNS: Array<[RegExp, string]> = [
  [/cannot cast/i, "Type cast failed. Check the column data types and transform expressions."],
  [/numeric value '([^']+)' is not recognized/i, "Invalid numeric value. The source data contains non-numeric values in a numeric column."],
  [/date '([^']+)' is not recognized/i, "Invalid date format. Check the date parsing in your transform expression."],
  [/invalid.*type/i, "Data type mismatch. Review the column transforms for type compatibility."],
  [/conversion/i, "Data conversion error. The source data format doesn't match the expected type."],
];

const TIMEOUT_PATTERNS: Array<[RegExp, string]> = [
  [/timeout/i, "Query timed out. Consider optimizing the SQL, adding filters, or increasing warehouse size."],
  [/exceeded.*time/i, "Execution time exceeded. The query may need optimization or a larger warehouse."],
  [/resource.*limit/i, "Resource limit reached. Try a larger warehouse or break the transformation into steps."],
];

const CONFIG_ERROR_PATTERNS: Array<[RegExp, string]> = [
  [/materialization.*type/i, "Materialization type mismatch. Check the node's materialization configuration."],
  [/config.*missing/i, "Required configuration is missing. Use complete_node_configuration to fill in defaults."],
  [/location.*not.*found/i, "Storage location not found. Verify the node's location setting matches a defined storage location."],
  [/duplicate.*column/i, "Duplicate column names detected. Remove or rename duplicate columns in the node."],
];

interface ClassificationResult {
  category: FailureCategory;
  suggestedFixes: string[];
}

function classifyError(errorMessage: string): ClassificationResult {
  const patternGroups: Array<[FailureCategory, Array<[RegExp, string]>]> = [
    ["sql_error", SQL_ERROR_PATTERNS],
    ["reference_error", REFERENCE_ERROR_PATTERNS],
    ["permission_error", PERMISSION_PATTERNS],
    ["data_type_error", DATA_TYPE_PATTERNS],
    ["timeout", TIMEOUT_PATTERNS],
    ["configuration_error", CONFIG_ERROR_PATTERNS],
  ];

  const suggestedFixes: string[] = [];
  let matchedCategory: FailureCategory | null = null;

  for (const [category, patterns] of patternGroups) {
    for (const [pattern, fix] of patterns) {
      if (pattern.test(errorMessage)) {
        if (!matchedCategory) {
          matchedCategory = category;
        }
        suggestedFixes.push(fix);
      }
    }
  }

  // Check for missing object patterns (Snowflake-style "does not exist" not caught above)
  if (!matchedCategory && /does not exist/i.test(errorMessage)) {
    return {
      category: "missing_object",
      suggestedFixes: [
        "A referenced object doesn't exist. Check that source nodes are deployed and storage locations are correct.",
      ],
    };
  }

  return {
    category: matchedCategory ?? "unknown",
    suggestedFixes:
      suggestedFixes.length > 0
        ? suggestedFixes
        : ["Review the error message and check the node's configuration, transforms, and dependencies."],
  };
}

// ── Node result extraction ───────────────────────────────────────────────────

function extractNodeStatus(nodeResult: Record<string, unknown>): string {
  if (typeof nodeResult.status === "string") return nodeResult.status;
  if (typeof nodeResult.runStatus === "string") return nodeResult.runStatus;
  if (typeof nodeResult.nodeStatus === "string") return nodeResult.nodeStatus;
  return "unknown";
}

function extractErrorMessage(nodeResult: Record<string, unknown>): string | null {
  // Try common error field names from the Coalesce API
  for (const key of ["error", "errorMessage", "message", "failureMessage", "runError", "detail"]) {
    const val = nodeResult[key];
    if (typeof val === "string" && val.length > 0) return val;
  }
  // Check nested error object
  if (isPlainObject(nodeResult.error)) {
    const nested = nodeResult.error;
    if (typeof nested.message === "string") return nested.message;
    if (typeof nested.detail === "string") return nested.detail;
  }
  return null;
}

function isFailedStatus(status: string): boolean {
  const lower = status.toLowerCase();
  return lower === "failed" || lower === "error" || lower === "failure";
}

function isSucceededStatus(status: string): boolean {
  const lower = status.toLowerCase();
  return lower === "completed" || lower === "success" || lower === "succeeded";
}

function isSkippedStatus(status: string): boolean {
  const lower = status.toLowerCase();
  return lower === "skipped" || lower === "excluded";
}

function isCanceledStatus(status: string): boolean {
  return status.toLowerCase() === "canceled";
}

// ── Main diagnosis function ──────────────────────────────────────────────────

export async function diagnoseRunFailure(
  client: CoalesceClient,
  params: DiagnoseRunInput
): Promise<RunDiagnosis> {
  const safeRunID = validatePathSegment(params.runID, "runID");

  // Fetch run metadata and results in parallel
  let run: unknown;
  let results: unknown;
  const warnings: string[] = [];

  const [runResult, resultsResult] = await Promise.allSettled([
    client.get(`/api/v1/runs/${safeRunID}`),
    client.get(`/api/v1/runs/${safeRunID}/results`),
  ]);

  // Handle run fetch result
  if (runResult.status === "rejected") {
    rethrowNonRecoverableApiError(runResult.reason);
    throw new Error(
      `Failed to fetch run ${safeRunID}: ${runResult.reason instanceof Error ? runResult.reason.message : String(runResult.reason)}`
    );
  }
  run = runResult.value;

  // Handle results fetch result
  if (resultsResult.status === "fulfilled") {
    results = resultsResult.value;
  } else {
    rethrowNonRecoverableApiError(resultsResult.reason);
    warnings.push(
      `Could not fetch run results: ${resultsResult.reason instanceof Error ? resultsResult.reason.message : String(resultsResult.reason)}. ` +
        `Diagnosis is based on run metadata only.`
    );
  }

  const runObj = isPlainObject(run) ? run : {};
  const runDetails = isPlainObject(runObj.runDetails) ? runObj.runDetails : {};

  const runStatus =
    typeof runObj.runStatus === "string" ? runObj.runStatus : "unknown";
  const runType =
    typeof runObj.runType === "string" ? runObj.runType : null;
  const environmentID =
    typeof runDetails.environmentID === "string"
      ? runDetails.environmentID
      : null;
  const startTime =
    typeof runObj.runStartTime === "string" ? runObj.runStartTime : null;
  const endTime =
    typeof runObj.runEndTime === "string" ? runObj.runEndTime : null;

  // Check for run-level error
  const runLevelError = extractErrorMessage(runObj);
  if (runLevelError) {
    warnings.push(`Run-level error: ${runLevelError}`);
  }

  // Parse node-level results
  const nodeResults = extractNodeResults(results);
  const summary = { totalNodes: 0, succeeded: 0, failed: 0, skipped: 0, canceled: 0, other: 0 };
  const failures: NodeDiagnosis[] = [];

  for (const nodeResult of nodeResults) {
    summary.totalNodes++;
    const status = extractNodeStatus(nodeResult);

    if (isSucceededStatus(status)) {
      summary.succeeded++;
    } else if (isFailedStatus(status)) {
      summary.failed++;
      const errorMessage = extractErrorMessage(nodeResult);
      const { category, suggestedFixes } = errorMessage
        ? classifyError(errorMessage)
        : { category: "unknown" as FailureCategory, suggestedFixes: ["No error message available. Check the Coalesce UI for details."] };

      failures.push({
        nodeID: typeof nodeResult.nodeID === "string" ? nodeResult.nodeID : "unknown",
        nodeName: typeof nodeResult.nodeName === "string" ? nodeResult.nodeName : null,
        nodeType: typeof nodeResult.nodeType === "string" ? nodeResult.nodeType : null,
        status,
        category,
        errorMessage,
        suggestedFixes,
      });
    } else if (isSkippedStatus(status)) {
      summary.skipped++;
    } else if (isCanceledStatus(status)) {
      summary.canceled++;
    } else {
      summary.other++;
    }
  }

  // Build recommendations
  const recommendations = buildRecommendations(runStatus, failures, summary, warnings);

  return {
    runID: safeRunID,
    analyzedAt: new Date().toISOString(),
    runStatus,
    runType,
    environmentID,
    startTime,
    endTime,
    summary,
    failures,
    warnings,
    recommendations,
  };
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function extractNodeResults(results: unknown): Array<Record<string, unknown>> {
  if (!results) return [];

  // results could be { data: [...] } or { results: [...] } or just [...]
  if (Array.isArray(results)) {
    return results.filter(isPlainObject);
  }
  if (isPlainObject(results)) {
    if (Array.isArray(results.data)) {
      return results.data.filter(isPlainObject);
    }
    if (Array.isArray(results.results)) {
      return results.results.filter(isPlainObject);
    }
  }
  return [];
}

function buildRecommendations(
  runStatus: string,
  failures: NodeDiagnosis[],
  summary: { totalNodes: number; succeeded: number; failed: number },
  warnings: string[]
): string[] {
  const recommendations: string[] = [];

  if (runStatus === "canceled") {
    recommendations.push(
      "This run was canceled. If unintentional, use retry_run to re-execute."
    );
    return recommendations;
  }

  if (summary.failed === 0 && runStatus === "completed") {
    recommendations.push("This run completed successfully with no failures.");
    return recommendations;
  }

  if (summary.failed === 0 && runStatus === "failed") {
    recommendations.push(
      "The run is marked as failed but no individual node failures were found in the results. " +
        "This may indicate a system-level failure (authentication, warehouse, or network issue). " +
        "Check the Coalesce UI for more details."
    );
    if (warnings.some((w) => w.includes("Run-level error"))) {
      recommendations.push("See the run-level error in the warnings above.");
    }
    return recommendations;
  }

  // Categorize failures
  const categoryCounts = new Map<FailureCategory, number>();
  for (const f of failures) {
    categoryCounts.set(f.category, (categoryCounts.get(f.category) ?? 0) + 1);
  }

  if (categoryCounts.has("permission_error")) {
    recommendations.push(
      `${categoryCounts.get("permission_error")} node(s) failed due to permission issues. ` +
        "Check the Snowflake role, warehouse, and account settings in the environment configuration."
    );
  }

  if (categoryCounts.has("reference_error") || categoryCounts.has("missing_object")) {
    const count =
      (categoryCounts.get("reference_error") ?? 0) +
      (categoryCounts.get("missing_object") ?? 0);
    recommendations.push(
      `${count} node(s) failed due to missing objects or broken references. ` +
        "Ensure all source nodes are deployed before running downstream nodes. " +
        "Consider running a full deploy instead of a selective refresh."
    );
  }

  if (categoryCounts.has("sql_error")) {
    recommendations.push(
      `${categoryCounts.get("sql_error")} node(s) have SQL errors. ` +
        "Use get_workspace_node to inspect the column transforms and join conditions for each failed node."
    );
  }

  if (categoryCounts.has("data_type_error")) {
    recommendations.push(
      `${categoryCounts.get("data_type_error")} node(s) have data type mismatches. ` +
        "Review the source data and column transforms. You may need CAST or TRY_CAST expressions."
    );
  }

  if (categoryCounts.has("timeout")) {
    recommendations.push(
      `${categoryCounts.get("timeout")} node(s) timed out. ` +
        "Consider increasing the warehouse size, adding WHERE filters, or breaking the transformation into smaller steps."
    );
  }

  if (categoryCounts.has("configuration_error")) {
    recommendations.push(
      `${categoryCounts.get("configuration_error")} node(s) have configuration issues. ` +
        "Use complete_node_configuration to fill in missing config, or review node settings in the Coalesce UI."
    );
  }

  if (categoryCounts.has("unknown")) {
    recommendations.push(
      `${categoryCounts.get("unknown")} node(s) failed with unclassified errors. ` +
        "Use get_run_results to see the full error details for each node."
    );
  }

  // Retry suggestion
  if (
    failures.length > 0 &&
    !categoryCounts.has("sql_error") &&
    !categoryCounts.has("configuration_error")
  ) {
    recommendations.push(
      "If the failures appear transient (permissions fixed, objects now deployed), use retry_run to re-execute only the failed nodes."
    );
  }

  // Failure ratio
  if (summary.totalNodes > 0) {
    const ratio = summary.failed / summary.totalNodes;
    if (ratio > 0.5) {
      recommendations.push(
        `Over half the nodes failed (${summary.failed}/${summary.totalNodes}). ` +
          "This may indicate a systemic issue like incorrect credentials or a missing dependency at the top of the DAG."
      );
    }
  }

  return recommendations;
}
