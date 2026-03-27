import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import {
  buildCacheResourceLink,
  CACHE_DIR_NAME,
  type CacheResourceLink,
} from "../cache-dir.js";
import { z } from "zod";
import { CoalesceApiError } from "../client.js";

const SESSION_START_TIME = new Date();

// Workspace node body schema — validates known structural fields while allowing
// node-type-specific extras through. Used by set-workspace-node.
export const WorkspaceNodeBodySchema = z
  .object({
    name: z.string().optional(),
    description: z.string().optional(),
    nodeType: z.string().optional(),
    database: z.string().optional(),
    schema: z.string().optional(),
    locationName: z.string().optional(),
    storageLocations: z.array(z.unknown()).optional(),
    config: z.record(z.unknown()).optional(),
    metadata: z
      .object({
        columns: z.array(z.unknown()).optional(),
      })
      .passthrough()
      .optional(),
  })
  .passthrough();

// Pagination params — only used by endpoints that support it
export const PaginationParams = z.object({
  limit: z.number().optional().describe("Number of results to return"),
  startingFrom: z
    .string()
    .optional()
    .describe("Cursor from previous response's next field"),
  orderBy: z
    .string()
    .optional()
    .describe("Field to sort by (required with startingFrom)"),
  orderByDirection: z
    .enum(["asc", "desc"])
    .optional()
    .describe("Sort direction"),
});

// Common annotations
export const READ_ONLY_ANNOTATIONS = {
  readOnlyHint: true,
  idempotentHint: true,
  destructiveHint: false,
  openWorldHint: true,
} as const;

export const READ_ONLY_LOCAL_ANNOTATIONS = {
  readOnlyHint: true,
  idempotentHint: true,
  destructiveHint: false,
  openWorldHint: false,
} as const;

export const WRITE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: false,
  destructiveHint: false,
  openWorldHint: true,
} as const;

export const IDEMPOTENT_WRITE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: true,
  destructiveHint: false,
  openWorldHint: true,
} as const;

export const DESTRUCTIVE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: false,
  destructiveHint: true,
  openWorldHint: true,
} as const;

const DEFAULT_AUTO_CACHE_MAX_BYTES = 32 * 1024;

const JsonObjectSchema = z.object({}).passthrough();

const JsonToolErrorSchema = z.object({
  message: z.string(),
  status: z.number().optional(),
  detail: z.unknown().optional(),
}).passthrough();

export type JsonToolError = z.infer<typeof JsonToolErrorSchema>;

const ListToolOutputSchema = z.object({
  data: z.array(z.unknown()).optional(),
  next: z.string().optional(),
  total: z.number().optional(),
}).passthrough();

const EntityToolOutputSchema = z.object({
  id: z.union([z.string(), z.number()]).optional(),
  name: z.string().optional(),
  message: z.string().optional(),
}).passthrough();

const WorkspaceNodeMutationOutputSchema = z.object({
  nodeID: z.string().optional(),
  created: z.boolean().optional(),
  warning: z.string().optional(),
  validation: JsonObjectSchema.optional(),
  nextSteps: z.array(z.string()).optional(),
  joinSuggestions: z.array(z.unknown()).optional(),
  configCompletion: JsonObjectSchema.optional(),
  configCompletionSkipped: z.string().optional(),
  nodeTypeValidation: JsonObjectSchema.optional(),
}).passthrough();

const WorkspaceAnalysisOutputSchema = z.object({
  workspaceID: z.string(),
  analyzedAt: z.string(),
  nodeCount: z.number(),
  packageAdoption: JsonObjectSchema,
  layerPatterns: JsonObjectSchema,
  methodology: z.string(),
  recommendations: JsonObjectSchema,
}).passthrough();

const WorkspaceNodeTypesOutputSchema = z.object({
  workspaceID: z.string(),
  basis: z.literal("observed_nodes"),
  nodeTypes: z.array(z.string()),
  counts: z.record(z.number()),
  total: z.number(),
}).passthrough();

const RunSchedulerOutputSchema = z.object({
  runCounter: z.number().optional(),
  runStatus: z.string().optional(),
  message: z.string().optional(),
}).passthrough();

const RunDetailsOutputSchema = z.object({
  run: z.unknown(),
  results: z.unknown(),
  resultsError: JsonToolErrorSchema.optional(),
}).passthrough();

const RunWaitOutputSchema = z.object({
  status: z.unknown().optional(),
  results: z.unknown().optional(),
  resultsError: JsonToolErrorSchema.optional(),
  incomplete: z.boolean().optional(),
  timedOut: z.boolean().optional(),
}).passthrough();

const EnvironmentOverviewOutputSchema = z.object({
  environment: z.unknown(),
  nodes: z.array(z.unknown()),
}).passthrough();

const CacheArtifactOutputSchema = z.object({
  workspaceID: z.string().optional(),
  environmentID: z.string().optional(),
  runType: z.enum(["deploy", "refresh"]).optional(),
  runStatus: z
    .enum(["completed", "failed", "canceled", "running", "waitingToRun"])
    .optional(),
  detail: z.boolean().optional(),
  totalNodes: z.number().optional(),
  totalRuns: z.number().optional(),
  totalUsers: z.number().optional(),
  pageCount: z.number().optional(),
  pageSize: z.number().optional(),
  orderBy: z.string().optional(),
  orderByDirection: z.enum(["asc", "desc"]).optional(),
  fileUri: z.string().optional(),
  metaUri: z.string().optional(),
  cachedAt: z.string().optional(),
  autoCached: z.boolean().optional(),
  resourceUri: z.string().optional(),
  toolName: z.string().optional(),
  message: z.string().optional(),
  sizeBytes: z.number().optional(),
  maxInlineBytes: z.number().optional(),
}).passthrough();

const ClearCacheOutputSchema = z.object({
  deleted: z.boolean(),
  fileCount: z.number().optional(),
  totalBytes: z.number().optional(),
  sizeMB: z.string().optional(),
  message: z.string(),
}).passthrough();

const RepoPackagesOutputSchema = z.object({
  summary: JsonObjectSchema,
  packages: z.array(JsonObjectSchema),
}).passthrough();

const RepoNodeTypesOutputSchema = z.object({
  summary: JsonObjectSchema,
  nodeTypes: z.array(JsonObjectSchema),
}).passthrough();

const RepoNodeTypeDefinitionOutputSchema = z.object({
  repoPath: z.string(),
  resolvedRepoPath: z.string(),
  repoWarnings: z.array(z.string()),
  requestedNodeType: z.string(),
  resolvedNodeType: z.string(),
  resolution: JsonObjectSchema,
  outerDefinition: JsonObjectSchema,
  nodeMetadataSpecYaml: z.string().nullable().optional(),
  nodeDefinition: z.unknown(),
  parseError: z.string().nullable().optional(),
  filePaths: JsonObjectSchema,
  usageSummary: JsonObjectSchema,
  warnings: z.array(z.string()),
}).passthrough();

const WorkspaceNodeTemplateOutputSchema = z.object({
  warnings: z.array(z.string()).optional(),
  setWorkspaceNodeBodyTemplate: JsonObjectSchema.optional(),
  setWorkspaceNodeBodyTemplateYaml: z.string().optional(),
  nodeDefinition: z.unknown().optional(),
  nodeMetadataSpecYaml: z.string().nullable().optional(),
  comparison: JsonObjectSchema.optional(),
}).passthrough();

const CorpusSearchOutputSchema = z.object({
  summary: JsonObjectSchema,
  matchedCount: z.number().optional(),
  returnedCount: z.number().optional(),
  matches: z.array(JsonObjectSchema).optional(),
  totalMatches: z.number().optional(),
}).passthrough();

const CorpusVariantOutputSchema = z.object({
  variantKey: z.string().optional(),
  supportStatus: z.string().optional(),
  nodeDefinition: z.unknown().optional(),
  nodeMetadataSpec: z.string().optional(),
  warnings: z.array(z.string()).optional(),
}).passthrough();

const PipelinePlanOutputSchema = z.object({
  version: z.number().optional(),
  intent: z.string().optional(),
  status: z.string().optional(),
  workspaceID: z.string().optional(),
  platform: z.string().nullable().optional(),
  goal: z.string().nullable().optional(),
  sql: z.string().nullable().optional(),
  warning: z.string().optional(),
  warnings: z.array(z.string()).optional(),
  assumptions: z.array(z.string()).optional(),
  openQuestions: z.array(z.unknown()).optional(),
  nodes: z.array(z.unknown()).optional(),
  cteNodeSummary: z.array(z.unknown()).optional(),
  supportedNodeTypes: z.array(z.string()).optional(),
  nodeTypeSelection: JsonObjectSchema.optional(),
  STOP_AND_CONFIRM: z.string().optional(),
  USE_THIS_NODE_TYPE: z.string().optional(),
  nodeTypeDisplayName: z.string().optional(),
  nodeTypeInstruction: z.string().optional(),
  planSummaryUri: z.string().optional(),
  planCached: z.boolean().optional(),
  instruction: z.string().optional(),
}).passthrough();

const PipelineCreateOutputSchema = z.object({
  created: z.boolean().optional(),
  cancelled: z.boolean().optional(),
  dryRun: z.boolean().optional(),
  STOP_AND_CONFIRM: z.string().optional(),
  reason: z.string().optional(),
  warning: z.string().optional(),
  workspaceID: z.string().optional(),
  nodeCount: z.number().optional(),
  incomplete: z.boolean().optional(),
  failedPlanNodeID: z.string().optional(),
  plan: z.unknown().optional(),
  createdNodes: z.array(z.unknown()).optional(),
  cleanupFailedNodeIDs: z.array(z.string()).optional(),
  cleanupFailures: z.array(
    z.object({
      nodeID: z.string(),
      message: z.string(),
      status: z.number().optional(),
      detail: z.unknown().optional(),
    }).passthrough()
  ).optional(),
  error: JsonToolErrorSchema.optional(),
}).passthrough();

const LIST_TOOL_NAMES = new Set([
  "list_environments",
  "list_projects",
  "list_environment_jobs",
  "list_runs",
  "list_environment_nodes",
  "list_workspace_nodes",
  "list_org_users",
  "list_user_roles",
  "list_git_accounts",
  "list_workspaces",
  "list_workspace_subgraphs",
  "list_workspace_jobs",
]);

const ENTITY_TOOL_NAMES = new Set([
  "get_environment",
  "create_environment",
  "update_environment",
  "delete_environment",
  "get_project",
  "create_project",
  "update_project",
  "delete_project",
  "get_environment_job",
  "get_workspace",
  "create_workspace_job",
  "update_workspace_job",
  "delete_workspace_job",
  "get_run",
  "get_run_results",
  "get_environment_node",
  "get_workspace_node",
  "get_user_roles",
  "set_org_role",
  "set_project_role",
  "delete_project_role",
  "set_env_role",
  "delete_env_role",
  "get_git_account",
  "create_git_account",
  "update_git_account",
  "delete_git_account",
  "get_workspace_subgraph",
  "create_workspace_subgraph",
  "update_workspace_subgraph",
  "delete_workspace_subgraph",
]);

const WORKSPACE_NODE_MUTATION_TOOL_NAMES = new Set([
  "create_workspace_node_from_scratch",
  "set_workspace_node",
  "update_workspace_node",
  "replace_workspace_node_columns",
  "convert_join_to_aggregation",
  "apply_join_condition",
  "create_workspace_node_from_predecessor",
  "create_node_from_external_schema",
  "delete_workspace_node",
  "complete_node_configuration",
]);

const CACHE_TOOL_NAMES = new Set([
  "cache_workspace_nodes",
  "cache_environment_nodes",
  "cache_runs",
  "cache_org_users",
]);

export const JsonToolOutputSchema = JsonObjectSchema.describe(
  "Tool-specific JSON object output. Oversized responses may be replaced with cache metadata including resourceUri."
);

export function getToolOutputSchema(toolName: string) {
  if (LIST_TOOL_NAMES.has(toolName)) {
    return ListToolOutputSchema;
  }
  if (ENTITY_TOOL_NAMES.has(toolName)) {
    return EntityToolOutputSchema;
  }
  if (WORKSPACE_NODE_MUTATION_TOOL_NAMES.has(toolName)) {
    return WorkspaceNodeMutationOutputSchema;
  }
  if (CACHE_TOOL_NAMES.has(toolName)) {
    return CacheArtifactOutputSchema;
  }

  switch (toolName) {
    case "clear_data_cache":
      return ClearCacheOutputSchema;
    case "analyze_workspace_patterns":
      return WorkspaceAnalysisOutputSchema;
    case "list_workspace_node_types":
      return WorkspaceNodeTypesOutputSchema;
    case "run_status":
    case "start_run":
    case "retry_run":
    case "cancel_run":
      return RunSchedulerOutputSchema;
    case "get_run_details":
      return RunDetailsOutputSchema;
    case "run_and_wait":
    case "retry_and_wait":
      return RunWaitOutputSchema;
    case "get_environment_overview":
      return EnvironmentOverviewOutputSchema;
    case "list_repo_packages":
      return RepoPackagesOutputSchema;
    case "list_repo_node_types":
      return RepoNodeTypesOutputSchema;
    case "get_repo_node_type_definition":
      return RepoNodeTypeDefinitionOutputSchema;
    case "generate_set_workspace_node_template":
    case "generate_set_workspace_node_template_from_variant":
      return WorkspaceNodeTemplateOutputSchema;
    case "search_node_type_variants":
      return CorpusSearchOutputSchema;
    case "get_node_type_variant":
      return CorpusVariantOutputSchema;
    case "plan_pipeline":
      return PipelinePlanOutputSchema;
    case "create_pipeline_from_plan":
    case "create_pipeline_from_sql":
      return PipelineCreateOutputSchema;
    default:
      return JsonToolOutputSchema;
  }
}
type TextContent = { type: "text"; text: string };

export type JsonToolResponse = {
  content: Array<TextContent | CacheResourceLink>;
  structuredContent?: Record<string, unknown>;
};

type JsonToolErrorResponse = {
  isError: true;
  content: { type: "text"; text: string }[];
  structuredContent: {
    error: z.infer<typeof JsonToolErrorSchema>;
  };
};

type JsonToolResponseOptions = {
  baseDir?: string;
  maxInlineBytes?: number;
};

// --- startRun / run-and-wait schemas ---

export const RunDetailsSchema = z.object({
  environmentID: z.string().describe("The environment being refreshed"),
  includeNodesSelector: z
    .string()
    .optional()
    .describe("Nodes included for an ad-hoc job"),
  excludeNodesSelector: z
    .string()
    .optional()
    .describe("Nodes excluded for an ad-hoc job"),
  jobID: z.string().optional().describe("The ID of a job being run"),
  parallelism: z
    .number()
    .int()
    .optional()
    .describe("Max parallel nodes to run (API default: 16)"),
  forceIgnoreWorkspaceStatus: z
    .boolean()
    .optional()
    .describe(
      "Allow refresh even if last deploy failed (API default: false). Use with caution."
    ),
});

export const UserCredentialsSchema = z.object({
  snowflakeUsername: z.string().describe("Snowflake account username"),
  snowflakeKeyPairKey: z
    .string()
    .describe(
      "PEM-encoded private key for Snowflake auth. Use \\n for line breaks in JSON."
    ),
  snowflakeKeyPairPass: z
    .string()
    .optional()
    .describe(
      "Password to decrypt an encrypted private key. Only required when the private key is encrypted."
    ),
  snowflakeWarehouse: z.string().describe("Snowflake compute warehouse"),
  snowflakeRole: z.string().describe("Snowflake user role"),
});

export const StartRunParams = z.object({
  runDetails: RunDetailsSchema,
  parameters: z
    .record(z.string())
    .optional()
    .describe("Arbitrary key-value parameters to pass to the run"),
  confirmRunAllNodes: z
    .boolean()
    .optional()
    .describe(
      "Must be set to true when no jobID, includeNodesSelector, or excludeNodesSelector is provided. " +
      "This confirms you intend to run ALL nodes in the environment."
    ),
});

export type StartRunInput = z.infer<typeof StartRunParams>;

// --- rerun / retry-and-wait schemas ---

export const RerunDetailsSchema = z.object({
  runID: z.string().describe("The run ID to retry"),
  forceIgnoreWorkspaceStatus: z
    .boolean()
    .optional()
    .describe(
      "Allow refresh even if last deploy failed (API default: false). Use with caution."
    ),
});

export const RerunParams = z.object({
  runDetails: RerunDetailsSchema,
  parameters: z
    .record(z.string())
    .optional()
    .describe("Arbitrary key-value parameters to pass to the rerun"),
});

export type RerunInput = z.infer<typeof RerunParams>;

export function buildRerunBody(params: RerunInput) {
  const userCredentials = getSnowflakeCredentials();
  return {
    runDetails: params.runDetails,
    userCredentials,
    ...(params.parameters ? { parameters: params.parameters } : {}),
  };
}

const ALLOWED_PEM_HEADERS = [
  "-----BEGIN PRIVATE KEY-----",
  "-----BEGIN RSA PRIVATE KEY-----",
  "-----BEGIN ENCRYPTED PRIVATE KEY-----",
] as const;

function readKeyPairFile(filePath: string): string {
  if (!existsSync(filePath)) {
    throw new Error(
      "SNOWFLAKE_KEY_PAIR_KEY file not found at the configured path. " +
      "Check that the environment variable points to an existing PEM private key file."
    );
  }
  const content = readFileSync(filePath, "utf-8").trim();
  const hasValidHeader = ALLOWED_PEM_HEADERS.some((header) =>
    content.includes(header)
  );
  if (!hasValidHeader) {
    throw new Error(
      "SNOWFLAKE_KEY_PAIR_KEY file is not a valid PEM private key. " +
      "Expected a file containing one of: PRIVATE KEY, RSA PRIVATE KEY, or ENCRYPTED PRIVATE KEY."
    );
  }
  return content;
}

export function getSnowflakeCredentials() {
  const snowflakeUsername = process.env.SNOWFLAKE_USERNAME;
  const snowflakeKeyPairKeyRaw = process.env.SNOWFLAKE_KEY_PAIR_KEY;
  const snowflakeKeyPairPass = process.env.SNOWFLAKE_KEY_PAIR_PASS;
  const snowflakeWarehouse = process.env.SNOWFLAKE_WAREHOUSE;
  const snowflakeRole = process.env.SNOWFLAKE_ROLE;

  if (!snowflakeUsername) {
    throw new Error(
      "SNOWFLAKE_USERNAME environment variable is required for Snowflake Key Pair run tools."
    );
  }
  if (!snowflakeKeyPairKeyRaw) {
    throw new Error(
      "SNOWFLAKE_KEY_PAIR_KEY environment variable is required for Snowflake Key Pair run tools."
    );
  }
  const snowflakeKeyPairKey = readKeyPairFile(snowflakeKeyPairKeyRaw);
  if (!snowflakeWarehouse) {
    throw new Error(
      "SNOWFLAKE_WAREHOUSE environment variable is required for Snowflake Key Pair run tools."
    );
  }
  if (!snowflakeRole) {
    throw new Error(
      "SNOWFLAKE_ROLE environment variable is required for Snowflake Key Pair run tools."
    );
  }

  return {
    snowflakeUsername,
    snowflakeKeyPairKey,
    ...(snowflakeKeyPairPass ? { snowflakeKeyPairPass } : {}),
    snowflakeWarehouse,
    snowflakeRole,
    snowflakeAuthType: "KeyPair" as const,
  };
}

const SANITIZED_KEYS = new Set([
  "userCredentials",
  "snowflakeKeyPairKey",
  "snowflakeKeyPairPass",
]);

export function sanitizeResponse(data: unknown): unknown {
  if (Array.isArray(data)) {
    return data.map(sanitizeResponse);
  }
  if (data && typeof data === "object") {
    const obj = data as Record<string, unknown>;
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      if (SANITIZED_KEYS.has(key)) continue;
      result[key] = sanitizeResponse(value);
    }
    return result;
  }
  return data;
}

function slugifyFileComponent(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function getAutoCacheMaxBytes(): number {
  const raw = process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES;
  if (raw === undefined) {
    return DEFAULT_AUTO_CACHE_MAX_BYTES;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return DEFAULT_AUTO_CACHE_MAX_BYTES;
  }

  return parsed;
}

function cleanupStaleAutoCacheFiles(autoCacheDir: string): void {
  try {
    const sessionTimestamp = SESSION_START_TIME.toISOString().replace(/[:.]/g, "-");
    const files = readdirSync(autoCacheDir)
      .filter((f) => f.endsWith(".json"))
      .sort();

    for (const file of files) {
      // Filenames are: {ISO_timestamp}-{tool-name}-{uuid}.json
      // Compare the timestamp prefix against session start
      if (file < sessionTimestamp) {
        try {
          unlinkSync(join(autoCacheDir, file));
        } catch {
          // Best-effort — skip files that can't be deleted
        }
      }
    }
  } catch {
    // Best-effort cleanup — don't fail the write
  }
}

function buildAutoCacheFilePath(
  toolName: string,
  cachedAt: string,
  baseDir: string
): string {
  const directory = join(baseDir, CACHE_DIR_NAME, "auto-cache");
  mkdirSync(directory, { recursive: true });
  const timestamp = cachedAt.replace(/[:.]/g, "-");
  const safeToolName = slugifyFileComponent(toolName) || "tool-response";
  return join(directory, `${timestamp}-${safeToolName}-${randomUUID()}.json`);
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function humanizeFieldName(fieldName: string): string {
  const stripped = fieldName.replace(/(Path|Uri)$/, "");
  const humanized = stripped
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .trim();
  return humanized.length > 0
    ? humanized.charAt(0).toUpperCase() + humanized.slice(1)
    : "Cached artifact";
}

function buildCacheLinkForField(
  fieldName: string,
  filePath: string,
  baseDir: string
): CacheResourceLink | null {
  return buildCacheResourceLink(filePath, {
    baseDir,
    name: humanizeFieldName(fieldName),
    description: "Read this cached artifact through the MCP resource URI.",
  });
}

function externalizeCachePaths(
  value: unknown,
  baseDir: string,
  resourceLinks: Map<string, CacheResourceLink>
): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => externalizeCachePaths(item, baseDir, resourceLinks));
  }

  if (!isPlainRecord(value)) {
    return value;
  }

  const output: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value)) {
    if (typeof child === "string") {
      const link = buildCacheLinkForField(key, child, baseDir);
      if (link) {
        const renamedKey =
          key.endsWith("Path") && !Object.prototype.hasOwnProperty.call(value, `${key.slice(0, -4)}Uri`)
            ? `${key.slice(0, -4)}Uri`
            : key;
        output[renamedKey] = link.uri;
        resourceLinks.set(link.uri, link);
        continue;
      }
    }

    output[key] = externalizeCachePaths(child, baseDir, resourceLinks);
  }

  return output;
}

function buildInlineJsonResponse(
  result: unknown,
  resourceLinks: CacheResourceLink[]
): JsonToolResponse {
  const text = JSON.stringify(result, null, 2);
  return {
    content: [{ type: "text", text }, ...resourceLinks],
    structuredContent: normalizeStructuredContent(result),
  };
}

function normalizeStructuredContent(result: unknown): Record<string, unknown> {
  if (isPlainRecord(result)) {
    return result;
  }
  return { value: result ?? null };
}

export function buildJsonToolResponse(
  toolName: string,
  result: unknown,
  options: JsonToolResponseOptions = {}
): JsonToolResponse {
  const baseDir = options.baseDir ?? process.cwd();
  const resourceLinks = new Map<string, CacheResourceLink>();
  const externalizedResult = externalizeCachePaths(result, baseDir, resourceLinks);
  const text = JSON.stringify(externalizedResult, null, 2);
  const maxInlineBytes = options.maxInlineBytes ?? getAutoCacheMaxBytes();
  const sizeBytes = Buffer.byteLength(text, "utf8");

  if (sizeBytes <= maxInlineBytes) {
    return buildInlineJsonResponse(
      externalizedResult,
      [...resourceLinks.values()]
    );
  }

  const cachedAt = new Date().toISOString();
  const filePath = buildAutoCacheFilePath(toolName, cachedAt, baseDir);
  try {
    writeFileSync(filePath, `${text}\n`, "utf8");
    cleanupStaleAutoCacheFiles(dirname(filePath));
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    process.stderr.write(`[coalesce-transform-mcp] auto-cache write failed for ${toolName}: ${reason}\n`);
    return buildInlineJsonResponse(
      externalizedResult,
      [...resourceLinks.values()]
    );
  }

  const cacheLink =
    buildCacheResourceLink(filePath, {
      baseDir,
      name: `${toolName} cached response`,
      description:
        "Full tool response cached on the MCP server because it exceeded the inline response threshold.",
    }) ?? null;

  const metadata: Record<string, unknown> = {
    autoCached: true,
    toolName,
    cachedAt,
    sizeBytes,
    maxInlineBytes,
    ...(cacheLink ? { resourceUri: cacheLink.uri } : {}),
    message:
      "Full response was automatically cached to disk because it exceeded the inline response threshold.",
  };

  // Omit structuredContent for auto-cached responses: the cache metadata shape
  // does not match the tool's declared output schema, so including it would
  // violate the MCP output contract.  Clients still receive the cache metadata
  // as text content and can follow the resourceUri to fetch the full payload.
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(metadata, null, 2),
      },
      ...(cacheLink ? [cacheLink] : []),
    ],
  };
}

export function validatePathSegment(value: string, name: string): string {
  if (value.length === 0) {
    throw new Error(`Invalid ${name}: must not be empty`);
  }
  if (/[\/\\]|\.\./.test(value)) {
    throw new Error(
      `Invalid ${name}: must not contain path separators or '..'`
    );
  }
  return value;
}

export function handleToolError(
  error: unknown
): JsonToolErrorResponse {
  const normalized =
    error instanceof CoalesceApiError
      ? {
          message: error.message,
          status: error.status,
          ...(error.detail !== undefined ? { detail: error.detail } : {}),
        }
      : error instanceof Error
        ? { message: error.message }
        : { message: String(error) };

  return {
    isError: true,
    content: [{ type: "text" as const, text: normalized.message }],
    structuredContent: {
      error: normalized,
    },
  };
}


export function buildStartRunBody(params: StartRunInput) {
  const { runDetails } = params;
  const hasNodeScope =
    runDetails.jobID ||
    runDetails.includeNodesSelector ||
    runDetails.excludeNodesSelector;

  if (!hasNodeScope && !params.confirmRunAllNodes) {
    throw new Error(
      "No jobID, includeNodesSelector, or excludeNodesSelector was provided. " +
      "This will run ALL nodes in the environment. " +
      "Set confirmRunAllNodes to true to confirm this is intentional."
    );
  }

  const userCredentials = getSnowflakeCredentials();
  return {
    runDetails,
    userCredentials,
    ...(params.parameters ? { parameters: params.parameters } : {}),
  };
}
