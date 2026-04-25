import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { defineLocalTool, defineDestructiveLocalTool } from "./tool-helpers.js";
import {
  READ_ONLY_LOCAL_ANNOTATIONS,
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
  detectV2Artifacts,
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
    parts.push(arg.includes(" ") ? `'${arg.replace(/'/g, "'\\''")}'` : arg);
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
  const toolResult = buildResult(args, redactDoctorRun(result));
  const v2Issues = detectV2Artifacts(cwd);
  if (v2Issues.length > 0) toolResult.preflightWarnings = v2Issues;
  return toolResult;
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
  v2Acknowledged: z
    .boolean()
    .optional()
    .describe(
      "Required when the project contains V2 artifacts (fileVersion: 2 node types or .sql nodes). V2 is alpha — set to true only AFTER telling the user V2 is alpha, surfacing the known rough edges, and getting explicit confirmation. See coalesce://context/sql-node-v2-policy."
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

/**
 * Block execution when preflight detected V2 artifacts OR could not rule them
 * out, and the agent did not pass `v2Acknowledged: true`. Promotes the
 * advisory V2_ALPHA_DETECTED / V2_SCAN_FAILED warnings into a hard error
 * (V2_ALPHA_NOT_ACKNOWLEDGED) so `coa_create` / `coa_run` do not silently
 * execute against a potentially-alpha project after a warning the model may
 * have ignored.
 *
 * V2_SCAN_FAILED counts as "potentially V2" on purpose — an unreadable
 * `nodes/` subtree or `nodeTypes/` dir could hide V2 artifacts, and a
 * partial-scan pass that looks clean must not bypass the guard.
 */
function assertV2Acknowledged(
  report: PreflightReport,
  v2Acknowledged: boolean | undefined
): void {
  if (v2Acknowledged) return;
  const trigger = report.warnings.find(
    (w) => w.code === "V2_ALPHA_DETECTED" || w.code === "V2_SCAN_FAILED"
  );
  if (!trigger) return;
  const retryHint =
    "Tell the user V2 is alpha, surface the known rough edges, get explicit confirmation, then retry with v2Acknowledged: true.";
  throw new CoaPreflightError({
    errors: [
      {
        level: "error",
        code: "V2_ALPHA_NOT_ACKNOWLEDGED",
        message: `${trigger.message} ${retryHint}`,
        path: trigger.path,
      },
    ],
    warnings: report.warnings.filter((w) => w !== trigger),
  });
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
  assertV2Acknowledged(report, params.v2Acknowledged);
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
  assertV2Acknowledged(report, params.v2Acknowledged);
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
    .min(1, "environmentID must not be empty")
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
  v2Acknowledged: z
    .boolean()
    .optional()
    .describe(
      "Required when the project contains V2 artifacts. A plan built from V2 sources is alpha-contaminated by construction — the guard fires here as well as on create/run. See coalesce://context/sql-node-v2-policy."
    ),
});

export async function coaPlanHandler(
  params: z.infer<typeof PlanParams>,
  runCoaFn: RunCoaFn = runCoa
): Promise<CoaToolResult> {
  const cwd = validateProjectPath(params.projectPath);
  const report = runPreflight(cwd, {});
  runOrThrow(report);
  assertV2Acknowledged(report, params.v2Acknowledged);
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
  environmentID: z.string().min(1, "environmentID must not be empty").describe("Target environment ID to deploy into."),
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
  environmentID: z.string().min(1, "environmentID must not be empty").describe("Environment ID to refresh."),
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
          "Run `coa doctor` against a local COA project — checks data.yml, workspaces.yml, credentials, and warehouse connectivity.\n\nKNOWN ISSUE (CD-16983): doctor reports the profile selected via `workspaces.yml` / `--workspace`, but `coa_plan` / `coa_deploy` / `coa_refresh` resolve profile differently — they fall through to the `[default]` profile in `~/.coa/config` unless `--profile` or `COALESCE_PROFILE` is set. A green doctor result does NOT guarantee that plan/deploy will authenticate against the same cloud account. When a user reports plan/deploy auth mismatches, suspect profile divergence before blaming credentials. Platform fix approach still under discussion.\n\nArgs:\n  - projectPath (string, required): Path to the COA project root (directory with data.yml)\n  - workspace (string, optional): workspaces.yml workspace name (default: dev)\n\nReturns:\n  { command, exitCode, stdout, stderr, timedOut, json?, coaVersion, preflightWarnings? }",
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
          "Run `coa doctor --fix` — writes a starter `workspaces.yml` in the project root, seeded from `locations.yml`. Safe to re-run; coa will not overwrite a valid existing file.\n\nIMPORTANT: the generated file contains placeholder database/schema values. The user MUST open it and set real values before running coa_create / coa_run — otherwise warehouse operations will target non-existent databases.\n\nKNOWN ISSUE (CD-16983): the `workspaces.yml` profile controls `coa create/run`, but `coa plan/deploy/refresh` fall through to `[default]` in `~/.coa/config`. After bootstrap, check that the two profiles point at the same cloud account before relying on plan/deploy results.\n\nDESTRUCTIVE: writes a new file to the project directory. Requires confirmed=true after explicit user approval.\n\nArgs:\n  - projectPath (string, required): Path to the COA project root (directory with data.yml)\n  - workspace (string, optional): workspaces.yml workspace name (default: dev)\n  - confirmed (boolean): must be true to execute\n\nReturns:\n  { command, exitCode, stdout, stderr, json?, coaVersion }",
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
          "List all nodes defined in a local COA project (pre-deploy). Wraps `coa create --list-nodes`.\n\nDifferent from list_environment_nodes (which lists deployed nodes in a cloud environment via the REST API).\n\nArgs:\n  - projectPath (string, required)\n  - workspace (string, optional)\n\nReturns:\n  { command, exitCode, stdout, json?, coaVersion }",
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
          "Preview the DDL that `coa create` would execute, without hitting the warehouse. Forces --dry-run --verbose.\n\nRuns entirely offline against local project files — no Coalesce cloud authentication or API calls. `coa create` (and `coa run`) are the offline local-dev commands; do not confuse with the scheduler-aware `coa deploy` / `coa plan` / `coa refresh` which target cloud environments.\n\nOUTPUT SHAPE (CD-16959+): dry-run reports pass/fail for every selected node rather than stopping at the first template error. Scan the full stdout — a non-zero exit code means one or more nodes failed, but successful nodes still render their generated SQL above/below the failures. Do not assume early output implies all-clear.\n\nCheck the stdout: table names should resolve (not blank), column types should not be UNKNOWN (indicates broken ref() targets), and SQL should look correct.\n\nDOES NOT enforce the V2 acknowledgement guard that coa_create / coa_run / coa_plan apply, because dry-run runs the SQL generator offline and never touches the warehouse — there's no destructive surface to gate on. If an agent ran into the V2 hard guard on coa_create, dry-run is a useful diagnostic step but a green dry-run does NOT imply V2 alpha is safe to acknowledge for the destructive command.\n\nLIMITATION: dry-run only exercises the SQL generator. It does NOT validate that referenced columns or types exist in the actual warehouse — a dry-run can succeed with column references that will fail at run-time with 'invalid identifier'. Use cortex or another Snowflake-capable MCP to confirm the schema when that matters.\n\nArgs:\n  - projectPath (string, required)\n  - workspace (string, optional)\n  - include / exclude (string, optional): Node selector\n\nReturns:\n  { command, exitCode, stdout, stderr, coaVersion }",
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
          "Preview the DML that `coa run` would execute, without hitting the warehouse. Forces --dry-run --verbose.\n\nRuns entirely offline against local project files — no Coalesce cloud authentication or API calls. `coa run` (and `coa create`) are the offline local-dev commands; do not confuse with the scheduler-aware `coa deploy` / `coa plan` / `coa refresh` which target cloud environments.\n\nOUTPUT SHAPE (CD-16959+): dry-run reports pass/fail for every selected node rather than stopping at the first template error. Scan the full stdout — a non-zero exit code means one or more nodes failed, but successful nodes still render their generated SQL above/below the failures.\n\nDOES NOT enforce the V2 acknowledgement guard that coa_create / coa_run / coa_plan apply, because dry-run runs the SQL generator offline and never touches the warehouse — there's no destructive surface to gate on. If an agent ran into the V2 hard guard on coa_run, dry-run is a useful diagnostic step but a green dry-run does NOT imply V2 alpha is safe to acknowledge for the destructive command.\n\nLIMITATION: dry-run only exercises the SQL generator. It does NOT validate that referenced columns or types exist in the actual warehouse — a dry-run can succeed with column references that will fail at run-time with 'invalid identifier'. Use cortex or another Snowflake-capable MCP to confirm the schema when that matters.\n\nArgs:\n  - projectPath (string, required)\n  - workspace (string, optional)\n  - include / exclude (string, optional): Node selector\n\nReturns:\n  { command, exitCode, stdout, stderr, coaVersion }",
        inputSchema: DryRunParams,
        annotations: READ_ONLY_LOCAL_ANNOTATIONS,
      },
      coaDryRunRunHandler
    ),

    defineDestructiveLocalTool(
      server,
      "coa_create",
      {
        title: "COA Create (DDL)",
        description:
          "Execute `coa create` — runs DDL (CREATE/REPLACE) for the selected nodes against the configured warehouse.\n\nDESTRUCTIVE: modifies warehouse schema. Requires confirmed=true after explicit user approval.\n\nPre-flight checks run before execution (double-quoted refs, missing workspaces.yml, bad selector patterns). Errors block execution; warnings are returned alongside the result.\n\nV2 HARD GUARD: if the project contains V2 artifacts (fileVersion: 2 node types or .sql nodes), the tool refuses to execute unless `v2Acknowledged: true` is passed. V2 is alpha — set this flag ONLY after telling the user V2 is alpha, surfacing the known rough edges, and getting explicit confirmation. See coalesce://context/sql-node-v2-policy.\n\nArgs:\n  - projectPath (string, required)\n  - workspace, include, exclude (optional)\n  - confirmed (boolean): must be true to execute\n  - v2Acknowledged (boolean, optional): required when V2 artifacts are detected\n\nReturns:\n  { command, exitCode, stdout, stderr, preflightWarnings?, coaVersion }",
        inputSchema: ExecuteSelectorParams,
        annotations: DESTRUCTIVE_ANNOTATIONS,
        confirmMessage: (params) =>
          `coa_create will execute DDL against the warehouse for project ${params.projectPath}${params.include ? ` (selector: ${params.include})` : " (all nodes)"}. This modifies schema and cannot be trivially rolled back.${params.v2Acknowledged ? " V2 alpha artifacts have been acknowledged." : ""}`,
      },
      coaCreateHandler
    ),

    defineDestructiveLocalTool(
      server,
      "coa_run",
      {
        title: "COA Run (DML)",
        description:
          "Execute `coa run` — runs DML (INSERT/MERGE) to populate the selected nodes.\n\nDESTRUCTIVE: modifies warehouse data. Requires confirmed=true.\n\nSame pre-flight checks and V2 HARD GUARD as coa_create — when V2 artifacts are present, `v2Acknowledged: true` is required and should only be set after the user has been told V2 is alpha. Uses a 30-minute timeout.\n\nArgs:\n  - projectPath (string, required)\n  - workspace, include, exclude (optional)\n  - confirmed (boolean): must be true to execute\n  - v2Acknowledged (boolean, optional): required when V2 artifacts are detected\n\nReturns:\n  { command, exitCode, stdout, stderr, preflightWarnings?, coaVersion }",
        inputSchema: ExecuteSelectorParams,
        annotations: DESTRUCTIVE_ANNOTATIONS,
        confirmMessage: (params) =>
          `coa_run will execute DML against the warehouse for project ${params.projectPath}${params.include ? ` (selector: ${params.include})` : " (all nodes)"}. Tables will be truncated/inserted/merged per node config.${params.v2Acknowledged ? " V2 alpha artifacts have been acknowledged." : ""}`,
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

  ];
}
