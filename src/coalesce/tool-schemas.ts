import { z } from "zod";
import { DOCUMENTED_RUN_STATUSES } from "../constants.js";

const JsonObjectSchema = z.object({}).passthrough();

export const JsonToolErrorSchema = z.object({
  message: z.string(),
  status: z.number().optional(),
  detail: z.unknown().optional(),
}).passthrough();

export type JsonToolError = z.infer<typeof JsonToolErrorSchema>;

// coerceListPaginationFields() normalises the Coalesce API's polymorphic
// pagination before responses reach the schema — `next` (number|string|null)
// becomes string-only, and null `next`/`total` are stripped.  The schema
// therefore only declares the post-coercion shapes.
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
  workspaceID: z.string().optional(),
  analyzedAt: z.string().optional(),
  nodeCount: z.number().optional(),
  packageAdoption: JsonObjectSchema.optional(),
  layerPatterns: JsonObjectSchema.optional(),
  methodology: z.string().optional(),
  recommendations: JsonObjectSchema.optional(),
}).passthrough();

const WorkspaceNodeTypesOutputSchema = z.object({
  workspaceID: z.string().optional(),
  basis: z.literal("observed_nodes").optional(),
  nodeTypes: z.array(z.string()).optional(),
  counts: z.record(z.number()).optional(),
  total: z.number().optional(),
}).passthrough();

const RunSchedulerOutputSchema = z.object({
  runCounter: z.number().optional(),
  runStatus: z.string().optional(),
  message: z.string().optional(),
}).passthrough();

const RunDetailsOutputSchema = z.object({
  run: z.unknown().optional(),
  results: z.unknown().optional(),
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
  environment: z.unknown().optional(),
  nodes: z.array(z.unknown()).optional(),
}).passthrough();

const EnvironmentHealthOutputSchema = z.object({
  environmentID: z.string().optional(),
  assessedAt: z.string().optional(),
  totalNodes: z.number().optional(),
  nodesByType: z.record(z.number()).optional(),
  nodeRunStatus: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    lastRunStatus: z.enum(["passed", "failed", "never_run"]).optional(),
    lastRunTime: z.string().optional(),
  }).passthrough()).optional(),
  failedRunsLast24h: z.array(z.object({
    runID: z.string().optional(),
    runStatus: z.string().optional(),
    startTime: z.string().optional(),
    endTime: z.string().optional(),
  }).passthrough()).optional(),
  staleNodes: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    nodeType: z.string().optional(),
    lastRunTime: z.string().optional(),
    daysSinceLastRun: z.number().optional(),
  }).passthrough()).optional(),
  dependencyHealth: z.object({
    orphanNodes: z.array(z.object({
      nodeID: z.string().optional(),
      nodeName: z.string().optional(),
      nodeType: z.string().optional(),
    }).passthrough()).optional(),
    totalDependencyEdges: z.number().optional(),
  }).passthrough().optional(),
  healthScore: z.enum(["healthy", "warning", "critical"]).optional(),
  healthReasons: z.array(z.string()).optional(),
}).passthrough();

const CacheArtifactOutputSchema = z.object({
  workspaceID: z.string().optional(),
  environmentID: z.string().optional(),
  runType: z.enum(["deploy", "refresh"]).optional(),
  runStatus: z
    .enum(DOCUMENTED_RUN_STATUSES)
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
  deleted: z.boolean().optional(),
  fileCount: z.number().optional(),
  totalBytes: z.number().optional(),
  sizeMB: z.string().optional(),
  message: z.string().optional(),
}).passthrough();

const RepoPackagesOutputSchema = z.object({
  summary: JsonObjectSchema.optional(),
  packages: z.array(JsonObjectSchema).optional(),
}).passthrough();

const RepoNodeTypesOutputSchema = z.object({
  summary: JsonObjectSchema.optional(),
  nodeTypes: z.array(JsonObjectSchema).optional(),
}).passthrough();

const RepoNodeTypeDefinitionOutputSchema = z.object({
  repoPath: z.string().optional(),
  resolvedRepoPath: z.string().optional(),
  repoWarnings: z.array(z.string()).optional(),
  requestedNodeType: z.string().optional(),
  resolvedNodeType: z.string().optional(),
  resolution: JsonObjectSchema.optional(),
  outerDefinition: JsonObjectSchema.optional(),
  nodeMetadataSpecYaml: z.string().nullable().optional(),
  nodeDefinition: z.unknown().optional(),
  parseError: z.string().nullable().optional(),
  filePaths: JsonObjectSchema.optional(),
  usageSummary: JsonObjectSchema.optional(),
  warnings: z.array(z.string()).optional(),
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

const ParseSqlStructureOutputSchema = z.object({
  hasCtes: z.boolean().optional(),
  cteCount: z.number().optional(),
  ctes: z.array(z.object({
    name: z.string().optional(),
    sourceTable: z.string().nullable().optional(),
    hasJoin: z.boolean().optional(),
    hasGroupBy: z.boolean().optional(),
    whereClause: z.string().nullable().optional(),
    columnCount: z.number().optional(),
    columns: z.array(z.unknown()).optional(),
    transformCount: z.number().optional(),
    passthroughCount: z.number().optional(),
    dependsOnCtes: z.array(z.string()).optional(),
  }).passthrough()).optional(),
  sourceRefs: z.array(z.object({
    locationName: z.string().optional(),
    nodeName: z.string().optional(),
    alias: z.string().nullable().optional(),
    sourceStyle: z.string().optional(),
  }).passthrough()).optional(),
  sourceCount: z.number().optional(),
  hasJoin: z.boolean().optional(),
  selectItems: z.array(z.unknown()).optional(),
  selectItemCount: z.number().optional(),
  supportedSelectCount: z.number().optional(),
  unsupportedSelectCount: z.number().optional(),
  warnings: z.array(z.string()).optional(),
  guidance: z.string().optional(),
}).passthrough();

const SelectPipelineNodeTypeOutputSchema = z.object({
  selectedNodeType: z.string().nullable().optional(),
  selectedDisplayName: z.string().nullable().optional(),
  selectedFamily: z.string().nullable().optional(),
  autoExecutable: z.boolean().optional(),
  semanticSignals: z.array(z.string()).optional(),
  missingDefaultFields: z.array(z.string()).optional(),
  templateWarnings: z.array(z.string()).optional(),
  selectionDegraded: z.boolean().optional(),
  selection: JsonObjectSchema.optional(),
  warnings: z.array(z.string()).optional(),
  warning: z.string().optional(),
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

const JobNodeSummarySchema = z.object({
  id: z.string().optional(),
  name: z.string().optional(),
  location: z.string().nullable().optional(),
  nodeType: z.string().nullable().optional(),
}).passthrough();

const JobNodesOutputSchema = z.object({
  job: z.object({
    id: z.string().optional(),
    name: z.string().optional(),
    includeSelector: z.string().optional(),
    excludeSelector: z.string().optional(),
  }).passthrough().optional(),
  summary: z.object({
    totalNodes: z.number().optional(),
    subgraphCount: z.number().optional(),
    unattachedCount: z.number().optional(),
    unresolvedCount: z.number().optional(),
    warnings: z.array(z.string()).optional(),
  }).passthrough().optional(),
  nodesBySubgraph: z.array(z.object({
    subgraphID: z.string().optional(),
    subgraphName: z.string().optional(),
    nodes: z.array(JobNodeSummarySchema).optional(),
  }).passthrough()).optional(),
  unattached: z.array(JobNodeSummarySchema).optional(),
  unresolved: z.array(z.object({
    term: z.unknown().optional(),
    reason: z.string().optional(),
  }).passthrough()).optional(),
}).passthrough();

const LineageTraversalOutputSchema = z.object({
  nodeID: z.string().optional(),
  nodeName: z.string().optional(),
  nodeType: z.string().optional(),
  totalAncestors: z.number().optional(),
  totalDependents: z.number().optional(),
  ancestors: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    nodeType: z.string().optional(),
    depth: z.number().optional(),
  }).passthrough()).optional(),
  dependents: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    nodeType: z.string().optional(),
    depth: z.number().optional(),
  }).passthrough()).optional(),
}).passthrough();

const ColumnLineageOutputSchema = z.object({
  nodeID: z.string().optional(),
  nodeName: z.string().optional(),
  columnID: z.string().optional(),
  columnName: z.string().optional(),
  totalUpstream: z.number().optional(),
  totalDownstream: z.number().optional(),
  upstream: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    nodeType: z.string().optional(),
    columnID: z.string().optional(),
    columnName: z.string().optional(),
    direction: z.string().optional(),
    depth: z.number().optional(),
  }).passthrough()).optional(),
  downstream: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    nodeType: z.string().optional(),
    columnID: z.string().optional(),
    columnName: z.string().optional(),
    direction: z.string().optional(),
    depth: z.number().optional(),
  }).passthrough()).optional(),
}).passthrough();

const ImpactAnalysisOutputSchema = z.object({
  sourceNodeID: z.string().optional(),
  sourceNodeName: z.string().optional(),
  sourceNodeType: z.string().optional(),
  sourceColumnID: z.string().optional(),
  sourceColumnName: z.string().optional(),
  impactedNodes: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    nodeType: z.string().optional(),
    depth: z.number().optional(),
  }).passthrough()).optional(),
  impactedColumns: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    nodeType: z.string().optional(),
    columnID: z.string().optional(),
    columnName: z.string().optional(),
    direction: z.string().optional(),
    depth: z.number().optional(),
  }).passthrough()).optional(),
  totalImpactedNodes: z.number().optional(),
  totalImpactedColumns: z.number().optional(),
  byDepth: z.record(z.array(z.string())).optional(),
  criticalPath: z.array(z.string()).optional(),
}).passthrough();

const WorkspaceSearchOutputSchema = z.object({
  query: z.string().optional(),
  fields: z.array(z.string()).optional(),
  nodeTypeFilter: z.string().optional(),
  totalMatches: z.number().optional(),
  returnedCount: z.number().optional(),
  truncated: z.boolean().optional(),
  results: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    nodeType: z.string().optional(),
    matchedFields: z.array(z.string()).optional(),
    matches: z.array(z.object({
      field: z.string().optional(),
      columnName: z.string().optional(),
      snippet: z.string().optional(),
    }).passthrough()).optional(),
  }).passthrough()).optional(),
  cacheAge: z.string().optional(),
}).passthrough();

const DocumentationAuditOutputSchema = z.object({
  workspaceID: z.string().optional(),
  auditedAt: z.string().optional(),
  totalNodes: z.number().optional(),
  documentedNodes: z.number().optional(),
  undocumentedNodes: z.number().optional(),
  nodeDocumentationPercent: z.number().optional(),
  totalColumns: z.number().optional(),
  documentedColumns: z.number().optional(),
  undocumentedColumns: z.number().optional(),
  columnDocumentationPercent: z.number().optional(),
  undocumentedNodeList: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    nodeType: z.string().optional(),
  }).passthrough()).optional(),
  undocumentedColumnList: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    columnID: z.string().optional(),
    columnName: z.string().optional(),
  }).passthrough()).optional(),
  truncatedColumns: z.boolean().optional(),
}).passthrough();

const PropagateColumnChangeOutputSchema = z.object({
  sourceNodeID: z.string().optional(),
  sourceColumnID: z.string().optional(),
  changes: z.object({
    columnName: z.string().optional(),
    dataType: z.string().optional(),
  }).passthrough().optional(),
  preMutationSnapshot: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    columnID: z.string().optional(),
    previousColumnName: z.string().optional(),
    previousDataType: z.string().optional(),
    capturedAt: z.string().optional(),
  }).passthrough()).optional(),
  snapshotPath: z.string().optional(),
  updatedNodes: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    columnID: z.string().optional(),
    columnName: z.string().optional(),
    previousName: z.string().optional(),
    previousDataType: z.string().optional(),
  }).passthrough()).optional(),
  totalUpdated: z.number().optional(),
  errors: z.array(z.object({
    nodeID: z.string().optional(),
    columnID: z.string().optional(),
    message: z.string().optional(),
  }).passthrough()).optional(),
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

const DiagnoseRunOutputSchema = z.object({
  runID: z.string().optional(),
  analyzedAt: z.string().optional(),
  runStatus: z.string().optional(),
  runType: z.string().nullable().optional(),
  environmentID: z.string().nullable().optional(),
  startTime: z.string().nullable().optional(),
  endTime: z.string().nullable().optional(),
  summary: z.object({
    totalNodes: z.number().optional(),
    succeeded: z.number().optional(),
    failed: z.number().optional(),
    skipped: z.number().optional(),
    canceled: z.number().optional(),
    other: z.number().optional(),
  }).passthrough().optional(),
  failures: z.array(z.object({
    nodeID: z.string().optional(),
    nodeName: z.string().nullable().optional(),
    nodeType: z.string().nullable().optional(),
    status: z.string().optional(),
    category: z.string().optional(),
    errorMessage: z.string().nullable().optional(),
    suggestedFixes: z.array(z.string()).optional(),
  }).passthrough()).optional(),
  warnings: z.array(z.string()).optional(),
  recommendations: z.array(z.string()).optional(),
}).passthrough();

const ReviewPipelineOutputSchema = z.object({
  workspaceID: z.string().optional(),
  analyzedAt: z.string().optional(),
  scope: z.enum(["full", "subgraph"]).optional(),
  nodeCount: z.number().optional(),
  methodology: z.string().optional(),
  findings: z.array(z.object({
    severity: z.enum(["critical", "warning", "suggestion"]).optional(),
    category: z.string().optional(),
    nodeID: z.string().optional(),
    nodeName: z.string().optional(),
    message: z.string().optional(),
    suggestion: z.string().optional(),
  }).passthrough()).optional(),
  summary: z.object({
    critical: z.number().optional(),
    warning: z.number().optional(),
    suggestion: z.number().optional(),
  }).passthrough().optional(),
  graphStats: z.object({
    maxDepth: z.number().optional(),
    rootNodes: z.number().optional(),
    leafNodes: z.number().optional(),
    avgFanOut: z.number().optional(),
  }).passthrough().optional(),
  warnings: z.array(z.string()).optional(),
}).passthrough();

const WorkshopSessionOutputSchema = z.object({
  sessionID: z.string().optional(),
  workspaceID: z.string().optional(),
  createdAt: z.string().optional(),
  updatedAt: z.string().optional(),
  nodes: z.array(z.object({
    id: z.string().optional(),
    name: z.string().optional(),
    nodeType: z.string().nullable().optional(),
    predecessorIDs: z.array(z.string()).optional(),
    columns: z.array(z.string()).optional(),
    created: z.boolean().optional(),
    createdNodeID: z.string().nullable().optional(),
  }).passthrough()).optional(),
  history: z.array(z.object({
    instruction: z.string().optional(),
    timestamp: z.string().optional(),
    result: z.string().optional(),
  }).passthrough()).optional(),
  resolvedEntities: z.array(z.object({
    name: z.string().optional(),
    nodeID: z.string().optional(),
    locationName: z.string().nullable().optional(),
  }).passthrough()).optional(),
  openQuestions: z.array(z.string()).optional(),
  warnings: z.array(z.string()).optional(),
  error: z.string().optional(),
}).passthrough();

const WorkshopInstructOutputSchema = z.object({
  sessionID: z.string().optional(),
  action: z.string().optional(),
  changes: z.array(z.string()).optional(),
  currentPlan: z.array(z.object({}).passthrough()).optional(),
  openQuestions: z.array(z.string()).optional(),
  warnings: z.array(z.string()).optional(),
  error: z.string().optional(),
}).passthrough();

const WorkshopCloseOutputSchema = z.object({
  closed: z.boolean().optional(),
  message: z.string().optional(),
  error: z.string().optional(),
}).passthrough();

const PersonalizeSkillsOutputSchema = z.object({
  directory: z.string(),
  created: z.array(z.string()),
  alreadyExisted: z.array(z.string()),
  totalSkills: z.number(),
  configHint: z.string().nullable(),
}).passthrough();

const CoaToolOutputSchema = z.object({
  command: z.string(),
  exitCode: z.number().nullable(),
  timedOut: z.boolean(),
  stdout: z.string(),
  stderr: z.string(),
  json: z.unknown().optional(),
  jsonParseError: z.string().optional(),
  coaVersion: z.string().nullable(),
  preflightWarnings: z.array(z.object({
    level: z.enum(["error", "warning"]).optional(),
    code: z.string().optional(),
    message: z.string().optional(),
    path: z.string().optional(),
  }).passthrough()).optional(),
}).passthrough();

const CoaDescribeOutputSchema = z.object({
  topic: z.string().optional(),
  subtopic: z.string().optional(),
  source: z.string().optional(),
  content: z.string().optional(),
  coaVersion: z.string().nullable().optional(),
}).passthrough();

const DiagnoseSetupOutputSchema = z.object({
  coaConfig: z.object({
    status: z.string(),
  }).passthrough(),
  accessToken: z.object({
    status: z.string(),
  }).passthrough(),
  snowflakeCreds: z.object({
    status: z.string(),
  }).passthrough(),
  repoPath: z.object({
    status: z.string(),
  }).passthrough(),
  coaDoctor: z.object({
    status: z.string(),
  }).passthrough(),
  projectWarnings: z.array(z.object({
    level: z.enum(["error", "warning"]).optional(),
    code: z.string().optional(),
    message: z.string().optional(),
    path: z.string().optional(),
  }).passthrough()),
  nextSteps: z.array(z.string()),
  ready: z.boolean(),
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
]);

const ENTITY_TOOL_NAMES = new Set([
  "get_environment",
  "create_environment",
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

const COA_TOOL_NAMES = new Set([
  "coa_doctor",
  "coa_bootstrap_workspaces",
  "coa_validate",
  "coa_list_project_nodes",
  "coa_dry_run_create",
  "coa_dry_run_run",
  "coa_create",
  "coa_run",
  "coa_plan",
  "coa_deploy",
  "coa_refresh",
  // NOTE: coa_describe is intentionally NOT here — it has a dedicated
  // CoaDescribeOutputSchema in the switch below.
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
  if (COA_TOOL_NAMES.has(toolName)) {
    return CoaToolOutputSchema;
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
    case "get_environment_health":
      return EnvironmentHealthOutputSchema;
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
    case "parse_sql_structure":
      return ParseSqlStructureOutputSchema;
    case "select_pipeline_node_type":
      return SelectPipelineNodeTypeOutputSchema;
    case "plan_pipeline":
      return PipelinePlanOutputSchema;
    case "create_pipeline_from_plan":
    case "create_pipeline_from_sql":
    case "build_pipeline_from_intent":
      return PipelineCreateOutputSchema;
    case "diagnose_run_failure":
      return DiagnoseRunOutputSchema;
    case "review_pipeline":
      return ReviewPipelineOutputSchema;
    case "pipeline_workshop_open":
    case "get_pipeline_workshop_status":
      return WorkshopSessionOutputSchema;
    case "pipeline_workshop_instruct":
      return WorkshopInstructOutputSchema;
    case "pipeline_workshop_close":
      return WorkshopCloseOutputSchema;
    case "list_job_nodes":
      return JobNodesOutputSchema;
    case "get_upstream_nodes":
    case "get_downstream_nodes":
      return LineageTraversalOutputSchema;
    case "get_column_lineage":
      return ColumnLineageOutputSchema;
    case "analyze_impact":
      return ImpactAnalysisOutputSchema;
    case "search_workspace_content":
      return WorkspaceSearchOutputSchema;
    case "audit_documentation_coverage":
      return DocumentationAuditOutputSchema;
    case "propagate_column_change":
      return PropagateColumnChangeOutputSchema;
    case "personalize_skills":
      return PersonalizeSkillsOutputSchema;
    case "coa_describe":
      return CoaDescribeOutputSchema;
    case "diagnose_setup":
      return DiagnoseSetupOutputSchema;
    default:
      return JsonToolOutputSchema;
  }
}
