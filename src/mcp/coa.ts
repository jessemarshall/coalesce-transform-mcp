import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { defineLocalTool, defineDestructiveLocalTool } from "./tool-helpers.js";
import {
  READ_ONLY_LOCAL_ANNOTATIONS,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type ToolDefinition,
} from "../coalesce/types.js";
import { runCoa, type RunCoaResult } from "../services/coa/runner.js";
import { resolveCoaBinary } from "../services/coa/resolver.js";
import { validateProjectPath } from "../services/coa/project.js";
import {
  COA_DESCRIBE_TOPICS,
  fetchDescribeTopic,
  type CoaDescribeTopic,
  type FetchDescribeOptions,
  type FetchDescribeResult,
} from "../services/coa/describe.js";
import {
  runPreflight,
  CoaPreflightError,
  pathExists,
  type PreflightReport,
} from "../services/coa/preflight.js";
import { redactSensitive } from "../services/coa/redact.js";

/**
 * Injected dependency for tests — lets us exercise handlers without spawning the CLI.
 */
type RunCoaFn = typeof runCoa;

/**
 * Shape returned by every coa_* tool. Keeps the raw shell result visible so
 * agents can diagnose failures without us over-shaping the response.
 */
export type CoaToolResult = {
  command: string;
  exitCode: number | null;
  timedOut: boolean;
  stdout: string;
  stderr: string;
  json?: unknown;
  jsonParseError?: string;
  coaVersion: string | null;
  /** Preflight warnings — errors would have thrown. Only set when a preflight ran. */
  preflightWarnings?: PreflightReport["warnings"];
};

/**
 * Flags whose VALUE is a secret. We redact the value in formatCommand so it
 * never ends up in tool output visible to the model (context logs, transcripts,
 * cached responses). The flag is passed to `spawn` correctly — only the
 * display string is scrubbed.
 */
const REDACTED_FLAGS = new Set(["--token"]);
const REDACTED_PLACEHOLDER = "<redacted>";

function formatCommand(args: string[]): string {
  const parts: string[] = ["coa"];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (REDACTED_FLAGS.has(arg) && i + 1 < args.length) {
      parts.push(arg, REDACTED_PLACEHOLDER);
      i += 1;
      continue;
    }
    parts.push(arg.includes(" ") ? `'${arg}'` : arg);
  }
  return parts.join(" ");
}

function buildResult(args: string[], runResult: RunCoaResult): CoaToolResult {
  // Resolver is already cached after the first call — cheap.
  const version = (() => {
    try {
      return resolveCoaBinary().version;
    } catch {
      return null;
    }
  })();

  const out: CoaToolResult = {
    command: formatCommand(args),
    exitCode: runResult.exitCode,
    timedOut: runResult.timedOut,
    stdout: runResult.stdout,
    stderr: runResult.stderr,
    coaVersion: version,
  };
  if ("json" in runResult) out.json = runResult.json;
  if (runResult.jsonParseError) out.jsonParseError = runResult.jsonParseError;
  return out;
}

// ---------- input schemas (shared) ----------

const ProjectPathParam = z.object({
  projectPath: z
    .string()
    .describe(
      "Absolute or relative path to the COA project root (the directory containing data.yml)."
    ),
  workspace: z
    .string()
    .optional()
    .describe("COA workspace name from workspaces.yml. Defaults to 'dev'."),
});

const SelectorParams = ProjectPathParam.extend({
  include: z
    .string()
    .optional()
    .describe(
      "COA node selector, e.g., '{ STG_ORDERS }' or '{ location: \"SRC\" }'. See `coa describe selectors`."
    ),
  exclude: z.string().optional().describe("COA node selector to exclude."),
});

const CloudAuthParams = z.object({
  profile: z
    .string()
    .optional()
    .describe(
      "Profile name in ~/.coa/config. Falls back to the COALESCE_PROFILE env var, then to COA's own default (`[default]`)."
    ),
  token: z
    .string()
    .optional()
    .describe(
      "Coalesce refresh token override. Prefer ~/.coa/config over passing tokens through tool input."
    ),
});

const CloudPaginationParams = z.object({
  limit: z.number().int().positive().optional().describe("Max results (default 100)."),
  startingFrom: z.string().optional().describe("Pagination cursor from a previous call."),
  orderBy: z.string().optional().describe("Field to sort by. Defaults to 'id'."),
});

// ---------- shared arg builders ----------

function pushIf(args: string[], flag: string, value: string | undefined): void {
  if (value !== undefined && value !== "") {
    args.push(flag, value);
  }
}

/**
 * Tool input wins; otherwise fall back to COALESCE_PROFILE from the MCP env.
 * Lets agents configure the profile once in .mcp.json and skip threading it
 * through every cloud tool call.
 */
function resolveProfile(inputProfile: string | undefined): string | undefined {
  if (inputProfile && inputProfile.trim().length > 0) return inputProfile;
  const envProfile = process.env.COALESCE_PROFILE?.trim();
  return envProfile && envProfile.length > 0 ? envProfile : undefined;
}

// ---------- handlers (exported for tests) ----------

export async function coaDoctorHandler(
  params: z.infer<typeof ProjectPathParam>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const args = ["--json", "doctor", "--dir", cwd];
  pushIf(args, "--workspace", params.workspace);
  const result = await runCoaFn(args, { cwd, parseJson: true });
  return buildResult(args, redactDoctorRun(result));
}

/**
 * `coa doctor --json` echoes a truncated access token under
 * `data.cloud.checks[].detail`. Redact the parsed JSON, and when we have a
 * clean parsed JSON we also drop the raw stdout so the same values don't reach
 * the agent via an un-redacted path.
 */
function redactDoctorRun(result: RunCoaResult): RunCoaResult {
  if (!("json" in result) || result.json === undefined) return result;
  const redacted = redactSensitive(result.json);
  const next: RunCoaResult = { ...result, json: redacted.value };
  if (redacted.didRedact && !result.jsonParseError) {
    // stdout is the un-redacted source of the same fields — drop it.
    next.stdout = "";
  }
  return next;
}

const BootstrapWorkspacesParams = ProjectPathParam.extend({
  confirmed: z
    .boolean()
    .optional()
    .describe(
      "Set to true after the user explicitly confirms. Without this, the tool returns a STOP_AND_CONFIRM response instead of executing."
    ),
});

export async function coaBootstrapWorkspacesHandler(
  params: z.infer<typeof BootstrapWorkspacesParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const args = ["--json", "doctor", "--dir", cwd, "--fix"];
  pushIf(args, "--workspace", params.workspace);
  const result = await runCoaFn(args, { cwd, parseJson: true });
  return buildResult(args, redactDoctorRun(result));
}

export async function coaValidateHandler(
  params: z.infer<typeof SelectorParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const args = ["--json", "validate", "--dir", cwd];
  pushIf(args, "--workspace", params.workspace);
  pushIf(args, "--include", params.include);
  pushIf(args, "--exclude", params.exclude);
  const result = await runCoaFn(args, { cwd, parseJson: true });
  return buildResult(args, result);
}

export async function coaListProjectNodesHandler(
  params: z.infer<typeof ProjectPathParam>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const args = ["--json", "create", "--dir", cwd, "--list-nodes"];
  pushIf(args, "--workspace", params.workspace);
  const result = await runCoaFn(args, { cwd, parseJson: true });
  return buildResult(args, result);
}

const DryRunParams = SelectorParams;

export async function coaDryRunCreateHandler(
  params: z.infer<typeof DryRunParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const args = ["--verbose", "create", "--dir", cwd, "--dry-run"];
  pushIf(args, "--workspace", params.workspace);
  pushIf(args, "--include", params.include);
  pushIf(args, "--exclude", params.exclude);
  const result = await runCoaFn(args, { cwd });
  return buildResult(args, result);
}

export async function coaDryRunRunHandler(
  params: z.infer<typeof DryRunParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const args = ["--verbose", "run", "--dir", cwd, "--dry-run"];
  pushIf(args, "--workspace", params.workspace);
  pushIf(args, "--include", params.include);
  pushIf(args, "--exclude", params.exclude);
  const result = await runCoaFn(args, { cwd });
  return buildResult(args, result);
}

const ListEnvironmentsParams = CloudAuthParams.merge(CloudPaginationParams).extend({
  detail: z.boolean().optional().describe("Return full environment detail."),
});

export async function coaListEnvironmentsHandler(
  params: z.infer<typeof ListEnvironmentsParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const args = ["environments", "list", "--format", "json", "--skipConfirm"];
  if (params.detail) args.push("--detail");
  pushIf(args, "--limit", params.limit?.toString());
  pushIf(args, "--startingFrom", params.startingFrom);
  pushIf(args, "--orderBy", params.orderBy);
  pushIf(args, "--profile", resolveProfile(params.profile));
  pushIf(args, "--token", params.token);
  const result = await runCoaFn(args, { parseJson: true });
  return buildResult(args, result);
}

const ListEnvironmentNodesParams = CloudAuthParams.merge(CloudPaginationParams).extend({
  environmentID: z.string().describe("The environment ID to list nodes from."),
  detail: z.boolean().optional().describe("Return full node detail."),
  skipParsing: z
    .boolean()
    .optional()
    .describe("Skip column-reference parsing. Faster, but column sources will not be populated."),
});

export async function coaListEnvironmentNodesHandler(
  params: z.infer<typeof ListEnvironmentNodesParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const args = [
    "nodes",
    "list",
    "--format",
    "json",
    "--skipConfirm",
    "--environmentID",
    params.environmentID,
  ];
  if (params.detail) args.push("--detail");
  if (params.skipParsing) args.push("--skipParsing");
  pushIf(args, "--limit", params.limit?.toString());
  pushIf(args, "--startingFrom", params.startingFrom);
  pushIf(args, "--orderBy", params.orderBy);
  pushIf(args, "--profile", resolveProfile(params.profile));
  pushIf(args, "--token", params.token);
  const result = await runCoaFn(args, { parseJson: true });
  return buildResult(args, result);
}

const ListRunsParams = CloudAuthParams.merge(CloudPaginationParams).extend({
  environmentID: z
    .string()
    .optional()
    .describe("Environment ID. Omit with allEnvironments=true for a cross-env view."),
  allEnvironments: z.boolean().optional().describe("Include runs across all environments."),
  orderByDirection: z.enum(["asc", "desc"]).optional().describe("Sort direction (default desc)."),
  projectID: z
    .array(z.string())
    .optional()
    .describe("Filter by one or more project IDs."),
  runType: z.array(z.string()).optional().describe("Filter by one or more run types."),
  runStatus: z
    .array(z.string())
    .optional()
    .describe("Filter by one or more run status values."),
  detail: z.boolean().optional().describe("Return full run detail."),
});

export async function coaListRunsHandler(
  params: z.infer<typeof ListRunsParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  if (!params.environmentID && !params.allEnvironments) {
    throw new Error(
      "coa_list_runs requires either environmentID or allEnvironments=true"
    );
  }
  const args = ["runs", "list", "--format", "json", "--skipConfirm"];
  if (params.detail) args.push("--detail");
  if (params.allEnvironments) args.push("--allEnvironments");
  pushIf(args, "--environmentID", params.environmentID);
  pushIf(args, "--limit", params.limit?.toString());
  pushIf(args, "--startingFrom", params.startingFrom);
  pushIf(args, "--orderBy", params.orderBy);
  pushIf(args, "--orderByDirection", params.orderByDirection);
  for (const id of params.projectID ?? []) args.push("--projectID", id);
  for (const type of params.runType ?? []) args.push("--runType", type);
  for (const status of params.runStatus ?? []) args.push("--runStatus", status);
  pushIf(args, "--profile", resolveProfile(params.profile));
  pushIf(args, "--token", params.token);
  const result = await runCoaFn(args, { parseJson: true });
  return buildResult(args, result);
}

const DescribeParams = z.object({
  topic: z
    .string()
    .describe(
      `Describe topic. Well-known topics: ${COA_DESCRIBE_TOPICS.join(", ")}. You may also pass 'command' or 'schema' together with a subtopic.`
    ),
  subtopic: z
    .string()
    .optional()
    .describe(
      "Optional subtopic (command name for topic='command', schema type for topic='schema')."
    ),
  refresh: z
    .boolean()
    .optional()
    .describe(
      "Force bypass of the on-disk cache and re-run `coa describe`. Defaults to false."
    ),
});

export async function coaDescribeHandler(
  params: z.infer<typeof DescribeParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<{
  topic: string;
  subtopic?: string;
  source: FetchDescribeResult["source"];
  content: string;
  coaVersion: string | null;
}> {
  const fetchOptions: FetchDescribeOptions = {
    subtopic: params.subtopic,
    refresh: params.refresh,
    runCoaFn,
  };
  const result = await fetchDescribeTopic(params.topic, fetchOptions);
  const out: {
    topic: string;
    subtopic?: string;
    source: FetchDescribeResult["source"];
    content: string;
    coaVersion: string | null;
  } = {
    topic: result.topic,
    source: result.source,
    content: result.content,
    coaVersion: result.coaVersion,
  };
  if (result.subtopic) out.subtopic = result.subtopic;
  return out;
}

// ---------- destructive / cloud-write handlers ----------

const ExecuteSelectorParams = SelectorParams.extend({
  confirmed: z
    .boolean()
    .optional()
    .describe(
      "Set to true after the user explicitly confirms. Without this, the tool returns a STOP_AND_CONFIRM response instead of executing."
    ),
});

function attachPreflightWarnings(
  result: CoaToolResult,
  report: PreflightReport
): CoaToolResult {
  if (report.warnings.length > 0) result.preflightWarnings = report.warnings;
  return result;
}

function runOrThrow(report: PreflightReport): void {
  if (report.errors.length > 0) throw new CoaPreflightError(report);
}

export async function coaCreateHandler(
  params: z.infer<typeof ExecuteSelectorParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const report = runPreflight(cwd, {
    requireWorkspacesYml: true,
    selectors: [params.include, params.exclude],
  });
  runOrThrow(report);
  const args = ["create", "--dir", cwd];
  pushIf(args, "--workspace", params.workspace);
  pushIf(args, "--include", params.include);
  pushIf(args, "--exclude", params.exclude);
  const result = await runCoaFn(args, { cwd, timeoutMs: 10 * 60_000 });
  return attachPreflightWarnings(buildResult(args, result), report);
}

export async function coaRunHandler(
  params: z.infer<typeof ExecuteSelectorParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const report = runPreflight(cwd, {
    requireWorkspacesYml: true,
    selectors: [params.include, params.exclude],
  });
  runOrThrow(report);
  const args = ["run", "--dir", cwd];
  pushIf(args, "--workspace", params.workspace);
  pushIf(args, "--include", params.include);
  pushIf(args, "--exclude", params.exclude);
  const result = await runCoaFn(args, { cwd, timeoutMs: 30 * 60_000 });
  return attachPreflightWarnings(buildResult(args, result), report);
}

const PlanParams = ProjectPathParam.merge(CloudAuthParams).extend({
  environmentID: z
    .string()
    .describe("Target environment ID for the deployment plan."),
  out: z
    .string()
    .optional()
    .describe(
      "Output path for the plan JSON. Relative paths resolve against the project root. Defaults to coa-plan.json in the project root."
    ),
  gitsha: z.string().optional().describe("Optional git SHA to embed in the plan manifest."),
  enableCache: z
    .boolean()
    .optional()
    .describe("Enable coa's plan cache. Coalesce recommends leaving this off unless plan generation is slow."),
});

export async function coaPlanHandler(
  params: z.infer<typeof PlanParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const report = runPreflight(cwd, {});
  runOrThrow(report);
  const args = [
    "plan",
    "--dir",
    cwd,
    "--environmentID",
    params.environmentID,
  ];
  pushIf(args, "--out", params.out);
  pushIf(args, "--gitsha", params.gitsha);
  if (params.enableCache) args.push("--enableCache");
  pushIf(args, "--profile", resolveProfile(params.profile));
  pushIf(args, "--token", params.token);
  const result = await runCoaFn(args, { cwd, timeoutMs: 10 * 60_000 });
  return attachPreflightWarnings(buildResult(args, result), report);
}

const DeployParams = CloudAuthParams.extend({
  environmentID: z.string().describe("Target environment ID to deploy into."),
  plan: z
    .string()
    .describe("Path to the coa-plan.json produced by coa_plan. Must exist before calling."),
  confirmed: z
    .boolean()
    .optional()
    .describe("Set to true after explicit user confirmation."),
});

export async function coaDeployHandler(
  params: z.infer<typeof DeployParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  if (!pathExists(params.plan)) {
    throw new Error(
      `coa_deploy: plan file not found at ${params.plan}. Run coa_plan first and point 'plan' at its output.`
    );
  }
  const args = [
    "deploy",
    "--environmentID",
    params.environmentID,
    "--plan",
    params.plan,
  ];
  pushIf(args, "--profile", resolveProfile(params.profile));
  pushIf(args, "--token", params.token);
  const result = await runCoaFn(args, { timeoutMs: 30 * 60_000 });
  return buildResult(args, result);
}

const RefreshParams = CloudAuthParams.extend({
  environmentID: z.string().describe("Environment ID to refresh."),
  include: z.string().optional().describe("Node selector to scope the refresh."),
  exclude: z.string().optional().describe("Node selector to exclude."),
  jobID: z.string().optional().describe("Run a specific deployed job."),
  parallelism: z.number().int().positive().optional().describe("Parallelism level."),
  parameters: z.string().optional().describe("Runtime parameters to pass to the run."),
  forceIgnoreEnvironmentStatus: z
    .boolean()
    .optional()
    .describe(
      "Proceed even if the environment has a failed deploy. May cause refresh failures — use only when you understand the state."
    ),
  confirmed: z
    .boolean()
    .optional()
    .describe("Set to true after explicit user confirmation."),
});

export async function coaRefreshHandler(
  params: z.infer<typeof RefreshParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const args = ["refresh", "--environmentID", params.environmentID];
  pushIf(args, "--include", params.include);
  pushIf(args, "--exclude", params.exclude);
  pushIf(args, "--jobID", params.jobID);
  pushIf(args, "--parallelism", params.parallelism?.toString());
  pushIf(args, "--parameters", params.parameters);
  if (params.forceIgnoreEnvironmentStatus) args.push("--forceIgnoreEnvironmentStatus");
  pushIf(args, "--profile", resolveProfile(params.profile));
  pushIf(args, "--token", params.token);
  const result = await runCoaFn(args, { timeoutMs: 60 * 60_000 });
  return buildResult(args, result);
}

// ---------- tool registration ----------

export function defineCoaTools(server: McpServer): ToolDefinition[] {
  return [
    defineLocalTool(
      "coa_doctor",
      {
        title: "COA Doctor",
        description:
          "Run `coa doctor` against a local COA project — checks data.yml, workspaces.yml, credentials, and warehouse connectivity.\n\nArgs:\n  - projectPath (string, required): Path to the COA project root (directory with data.yml)\n  - workspace (string, optional): workspaces.yml workspace name (default: dev)\n\nReturns:\n  { command, exitCode, stdout, stderr, timedOut, json?, coaVersion }",
        inputSchema: ProjectPathParam,
        annotations: READ_ONLY_LOCAL_ANNOTATIONS,
      },
      coaDoctorHandler
    ),

    defineDestructiveLocalTool(
      server,
      "coa_bootstrap_workspaces",
      {
        title: "COA Bootstrap workspaces.yml (doctor --fix)",
        description:
          "Run `coa doctor --fix` — writes a starter `workspaces.yml` in the project root, seeded from `locations.yml`. Safe to re-run; coa will not overwrite a valid existing file.\n\nIMPORTANT: the generated file contains placeholder database/schema values. The user MUST open it and set real values before running coa_create / coa_run — otherwise warehouse operations will target non-existent databases.\n\nDESTRUCTIVE: writes a new file to the project directory. Requires confirmed=true after explicit user approval.\n\nArgs:\n  - projectPath (string, required): Path to the COA project root (directory with data.yml)\n  - workspace (string, optional): workspaces.yml workspace name (default: dev)\n  - confirmed (boolean): must be true to execute\n\nReturns:\n  { command, exitCode, stdout, stderr, json?, coaVersion }",
        inputSchema: BootstrapWorkspacesParams,
        annotations: DESTRUCTIVE_ANNOTATIONS,
        confirmMessage: (params) =>
          `coa_bootstrap_workspaces will run \`coa doctor --fix\` in ${params.projectPath}. This writes a starter workspaces.yml with placeholder database/schema values — the user must edit real values in before running warehouse operations.`,
      },
      coaBootstrapWorkspacesHandler
    ),

    defineLocalTool(
      "coa_validate",
      {
        title: "COA Validate",
        description:
          "Run `coa validate` — scans a project's YAML schemas, storage locations, column references, and types.\n\nKnown issue: V2 SQL nodes may produce false-positive column reference errors from the Column References scanner. Those don't block coa_dry_run_create / coa_dry_run_run.\n\nArgs:\n  - projectPath (string, required)\n  - workspace (string, optional)\n  - include / exclude (string, optional): Node selector\n\nReturns:\n  { command, exitCode, stdout, stderr, json?, coaVersion }",
        inputSchema: SelectorParams,
        annotations: READ_ONLY_LOCAL_ANNOTATIONS,
      },
      coaValidateHandler
    ),

    defineLocalTool(
      "coa_list_project_nodes",
      {
        title: "COA List Project Nodes",
        description:
          "List all nodes defined in a local COA project (pre-deploy). Wraps `coa create --list-nodes`.\n\nDifferent from coa_list_environment_nodes (which lists deployed nodes in a cloud environment).\n\nArgs:\n  - projectPath (string, required)\n  - workspace (string, optional)\n\nReturns:\n  { command, exitCode, stdout, json?, coaVersion }",
        inputSchema: ProjectPathParam,
        annotations: READ_ONLY_LOCAL_ANNOTATIONS,
      },
      coaListProjectNodesHandler
    ),

    defineLocalTool(
      "coa_dry_run_create",
      {
        title: "COA Dry Run Create (DDL preview)",
        description:
          "Preview the DDL that `coa create` would execute, without hitting the warehouse. Forces --dry-run --verbose.\n\nRuns entirely offline against local project files — no Coalesce cloud authentication or API calls. `coa create` (and `coa run`) are the offline local-dev commands; do not confuse with the scheduler-aware `coa deploy` / `coa plan` / `coa refresh` which target cloud environments.\n\nCheck the stdout: table names should resolve (not blank), column types should not be UNKNOWN (indicates broken ref() targets), and SQL should look correct.\n\nLIMITATION: dry-run only exercises the SQL generator. It does NOT validate that referenced columns or types exist in the actual warehouse — a dry-run can succeed with column references that will fail at run-time with 'invalid identifier'. Use cortex or another Snowflake-capable MCP to confirm the schema when that matters.\n\nArgs:\n  - projectPath (string, required)\n  - workspace (string, optional)\n  - include / exclude (string, optional): Node selector\n\nReturns:\n  { command, exitCode, stdout, stderr, coaVersion }",
        inputSchema: DryRunParams,
        annotations: READ_ONLY_LOCAL_ANNOTATIONS,
      },
      coaDryRunCreateHandler
    ),

    defineLocalTool(
      "coa_dry_run_run",
      {
        title: "COA Dry Run Run (DML preview)",
        description:
          "Preview the DML that `coa run` would execute, without hitting the warehouse. Forces --dry-run --verbose.\n\nRuns entirely offline against local project files — no Coalesce cloud authentication or API calls. `coa run` (and `coa create`) are the offline local-dev commands; do not confuse with the scheduler-aware `coa deploy` / `coa plan` / `coa refresh` which target cloud environments.\n\nLIMITATION: dry-run only exercises the SQL generator. It does NOT validate that referenced columns or types exist in the actual warehouse — a dry-run can succeed with column references that will fail at run-time with 'invalid identifier'. Use cortex or another Snowflake-capable MCP to confirm the schema when that matters.\n\nArgs:\n  - projectPath (string, required)\n  - workspace (string, optional)\n  - include / exclude (string, optional): Node selector\n\nReturns:\n  { command, exitCode, stdout, stderr, coaVersion }",
        inputSchema: DryRunParams,
        annotations: READ_ONLY_LOCAL_ANNOTATIONS,
      },
      coaDryRunRunHandler
    ),

    defineLocalTool(
      "coa_list_environments",
      {
        title: "COA List Environments (cloud)",
        description:
          "List deployment environments visible to the current COA profile. Wraps `coa environments list --format json`.\n\nRequires COA cloud credentials in ~/.coa/config (domain + token), or pass profile/token explicitly.\n\nArgs:\n  - detail, limit, startingFrom, orderBy, profile, token (all optional)\n\nReturns:\n  { command, exitCode, stdout, json?, coaVersion }",
        inputSchema: ListEnvironmentsParams,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      coaListEnvironmentsHandler
    ),

    defineLocalTool(
      "coa_list_environment_nodes",
      {
        title: "COA List Environment Nodes (cloud)",
        description:
          "List deployed nodes in a cloud environment. Wraps `coa nodes list --environmentID ...`.\n\nDifferent from coa_list_project_nodes (which lists nodes in a local COA project).\n\nArgs:\n  - environmentID (string, required)\n  - detail, skipParsing, limit, startingFrom, orderBy, profile, token (all optional)\n\nReturns:\n  { command, exitCode, stdout, json?, coaVersion }",
        inputSchema: ListEnvironmentNodesParams,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      coaListEnvironmentNodesHandler
    ),

    defineDestructiveLocalTool(
      server,
      "coa_create",
      {
        title: "COA Create (DDL)",
        description:
          "Execute `coa create` — runs DDL (CREATE/REPLACE) for the selected nodes against the configured warehouse.\n\nDESTRUCTIVE: modifies warehouse schema. Requires confirmed=true after explicit user approval.\n\nPre-flight checks run before execution (double-quoted refs, missing workspaces.yml, bad selector patterns). Errors block execution; warnings are returned alongside the result.\n\nArgs:\n  - projectPath (string, required)\n  - workspace, include, exclude (optional)\n  - confirmed (boolean): must be true to execute\n\nReturns:\n  { command, exitCode, stdout, stderr, preflightWarnings?, coaVersion }",
        inputSchema: ExecuteSelectorParams,
        annotations: DESTRUCTIVE_ANNOTATIONS,
        confirmMessage: (params) =>
          `coa_create will execute DDL against the warehouse for project ${params.projectPath}${params.include ? ` (selector: ${params.include})` : " (all nodes)"}. This modifies schema and cannot be trivially rolled back.`,
      },
      coaCreateHandler
    ),

    defineDestructiveLocalTool(
      server,
      "coa_run",
      {
        title: "COA Run (DML)",
        description:
          "Execute `coa run` — runs DML (INSERT/MERGE) to populate the selected nodes.\n\nDESTRUCTIVE: modifies warehouse data. Requires confirmed=true.\n\nSame pre-flight checks as coa_create. Uses a 30-minute timeout.\n\nArgs:\n  - projectPath (string, required)\n  - workspace, include, exclude (optional)\n  - confirmed (boolean): must be true to execute\n\nReturns:\n  { command, exitCode, stdout, stderr, preflightWarnings?, coaVersion }",
        inputSchema: ExecuteSelectorParams,
        annotations: DESTRUCTIVE_ANNOTATIONS,
        confirmMessage: (params) =>
          `coa_run will execute DML against the warehouse for project ${params.projectPath}${params.include ? ` (selector: ${params.include})` : " (all nodes)"}. Tables will be truncated/inserted/merged per node config.`,
      },
      coaRunHandler
    ),

    defineLocalTool(
      "coa_plan",
      {
        title: "COA Plan",
        description:
          "Generate a deployment plan. Reads the local project, diffs against the target environment, and writes a plan JSON (default coa-plan.json in the project root).\n\nNon-destructive: produces a plan file only. Safe to call without confirmation. The plan is then applied via coa_deploy.\n\nRequires COA cloud credentials (~/.coa/config or profile/token) and an environmentID.\n\nArgs:\n  - projectPath (string, required)\n  - environmentID (string, required)\n  - out (string, optional): plan output path\n  - gitsha, enableCache (optional)\n  - profile, token (optional)\n\nReturns:\n  { command, exitCode, stdout, stderr, preflightWarnings?, coaVersion }",
        inputSchema: PlanParams,
        annotations: WRITE_ANNOTATIONS,
      },
      coaPlanHandler
    ),

    defineDestructiveLocalTool(
      server,
      "coa_deploy",
      {
        title: "COA Deploy",
        description:
          "Apply a plan JSON to a cloud environment. Changes environment schema/state.\n\nDESTRUCTIVE: modifies the deployed environment. Requires confirmed=true.\n\nThe plan file must exist (produced by coa_plan). Does NOT re-validate project contents — validates the plan structure only.\n\nArgs:\n  - environmentID (string, required)\n  - plan (string, required): path to coa-plan.json\n  - profile, token (optional)\n  - confirmed (boolean): must be true to execute\n\nReturns:\n  { command, exitCode, stdout, stderr, coaVersion }",
        inputSchema: DeployParams,
        annotations: DESTRUCTIVE_ANNOTATIONS,
        confirmMessage: (params) =>
          `coa_deploy will apply plan ${params.plan} to environment ${params.environmentID}. This will modify the deployed environment's schema and may drop or rename objects per the plan diff.`,
      },
      coaDeployHandler
    ),

    defineDestructiveLocalTool(
      server,
      "coa_refresh",
      {
        title: "COA Refresh",
        description:
          "Run DML for selected nodes in a deployed environment. Does not require a local project — operates purely on the cloud environment.\n\nDESTRUCTIVE: modifies warehouse data in the environment. Requires confirmed=true.\n\nArgs:\n  - environmentID (string, required)\n  - include, exclude, jobID, parallelism, parameters (optional)\n  - forceIgnoreEnvironmentStatus (optional)\n  - profile, token (optional)\n  - confirmed (boolean): must be true to execute\n\nReturns:\n  { command, exitCode, stdout, stderr, coaVersion }",
        inputSchema: RefreshParams,
        annotations: DESTRUCTIVE_ANNOTATIONS,
        confirmMessage: (params) =>
          `coa_refresh will execute DML in environment ${params.environmentID}${params.include ? ` (selector: ${params.include})` : " (all nodes in the environment)"}.`,
      },
      coaRefreshHandler
    ),

    defineLocalTool(
      "coa_describe",
      {
        title: "COA Describe",
        description:
          `Fetch a section of COA's self-describing documentation. Wraps \`coa describe <topic> [<subtopic>]\`.\n\nWell-known topics (also available as coalesce://coa/describe/* resources): ${COA_DESCRIBE_TOPICS.join(", ")}.\n\nUse subtopic for the parameterized topics:\n  - topic='command', subtopic='<name>' — deep-dive on a specific command\n  - topic='schema', subtopic='<type>' — full JSON schema for a file type\n\nArgs:\n  - topic (string, required)\n  - subtopic (string, optional)\n  - refresh (boolean, optional): bypass the on-disk cache and re-run COA\n\nReturns:\n  { topic, subtopic?, source (memory|disk|coa), content, coaVersion }`,
        inputSchema: DescribeParams,
        annotations: READ_ONLY_LOCAL_ANNOTATIONS,
      },
      coaDescribeHandler
    ),

    defineLocalTool(
      "coa_list_runs",
      {
        title: "COA List Runs (cloud)",
        description:
          "List pipeline runs in a cloud environment (or across all). Wraps `coa runs list`.\n\nArgs:\n  - environmentID (string, required unless allEnvironments=true)\n  - allEnvironments (boolean, optional): Include runs across every environment\n  - detail, limit, startingFrom, orderBy, orderByDirection (asc|desc)\n  - projectID (string[], optional), runType (string[], optional), runStatus (string[], optional)\n  - profile, token (optional)\n\nReturns:\n  { command, exitCode, stdout, json?, coaVersion }",
        inputSchema: ListRunsParams,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      coaListRunsHandler
    ),
  ];
}
