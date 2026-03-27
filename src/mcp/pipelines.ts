import { z } from "zod";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { CACHE_DIR_NAME } from "../cache-dir.js";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { buildPlanConfirmationToken, sortJsonValue } from "../services/pipelines/confirmation.js";
import {
  PipelinePlanSchema,
  planPipeline,
} from "../services/pipelines/planning.js";
import {
  createPipelineFromPlan,
} from "../services/pipelines/execution.js";
import { NodeConfigInputSchema } from "../schemas/node-payloads.js";
import {
  buildJsonToolResponse,
  getToolOutputSchema,
  handleToolError,
  type JsonToolResponse,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  validatePathSegment,
} from "../coalesce/types.js";
import { isPlainObject } from "../utils.js";

export { buildPlanConfirmationToken };

function normalizePlanFingerprintSelection(selection: unknown): Record<string, unknown> | null {
  if (!isPlainObject(selection)) {
    return null;
  }

  return {
    strategy: typeof selection.strategy === "string" ? selection.strategy : null,
    selectedNodeType:
      typeof selection.selectedNodeType === "string" ? selection.selectedNodeType : null,
    selectedDisplayName:
      typeof selection.selectedDisplayName === "string"
        ? selection.selectedDisplayName
        : null,
    selectedShortName:
      typeof selection.selectedShortName === "string" ? selection.selectedShortName : null,
    selectedFamily:
      typeof selection.selectedFamily === "string" ? selection.selectedFamily : null,
    confidence: typeof selection.confidence === "string" ? selection.confidence : null,
    autoExecutable: selection.autoExecutable === true,
    repoPath: typeof selection.repoPath === "string" ? selection.repoPath : null,
    resolvedRepoPath:
      typeof selection.resolvedRepoPath === "string" ? selection.resolvedRepoPath : null,
    supportedNodeTypes: Array.isArray(selection.supportedNodeTypes)
      ? selection.supportedNodeTypes.filter((value): value is string => typeof value === "string")
      : [],
    consideredNodeTypes: Array.isArray(selection.consideredNodeTypes)
      ? selection.consideredNodeTypes
          .filter(isPlainObject)
          .map((candidate) => ({
            nodeType: typeof candidate.nodeType === "string" ? candidate.nodeType : null,
            displayName:
              typeof candidate.displayName === "string" ? candidate.displayName : null,
            shortName:
              typeof candidate.shortName === "string" ? candidate.shortName : null,
            family: typeof candidate.family === "string" ? candidate.family : null,
            usageCount:
              typeof candidate.usageCount === "number" ? candidate.usageCount : null,
            workspaceUsageCount:
              typeof candidate.workspaceUsageCount === "number"
                ? candidate.workspaceUsageCount
                : null,
            observedInWorkspace: candidate.observedInWorkspace === true,
            autoExecutable: candidate.autoExecutable === true,
            score: typeof candidate.score === "number" ? candidate.score : null,
            reasons: Array.isArray(candidate.reasons)
              ? candidate.reasons.filter((value): value is string => typeof value === "string")
              : [],
          }))
      : [],
  };
}

function buildPlanFingerprint(
  workspaceID: string,
  selection: unknown,
  supportedNodeTypes: string[],
  requestInputs?: {
    goal?: string;
    sql?: string;
    sourceNodeIDs?: string[];
    targetNodeType?: string;
  }
): string {
  const payload = sortJsonValue({
    workspaceID,
    requestInputs: {
      goal: requestInputs?.goal ?? null,
      sql: requestInputs?.sql ?? null,
      sourceNodeIDs: requestInputs?.sourceNodeIDs
        ? [...requestInputs.sourceNodeIDs].sort()
        : [],
      targetNodeType: requestInputs?.targetNodeType ?? null,
    },
    supportedNodeTypes: [...supportedNodeTypes],
    selection: normalizePlanFingerprintSelection(selection),
  });
  return createHash("sha256").update(JSON.stringify(payload)).digest("hex").slice(0, 16);
}

function getPlanSummaryDir(): string {
  return join(process.cwd(), CACHE_DIR_NAME, "plans");
}

function findCachedPlanSummary(
  workspaceID: string,
  fingerprint: string
): { path: string; content: string } | null {
  const dir = getPlanSummaryDir();
  if (!existsSync(dir)) return null;

  const safeID = workspaceID.replace(/[^a-zA-Z0-9_\-]/g, "_");
  const prefix = `plan-${safeID}-`;
  const files = readdirSync(dir)
    .filter((f) => f.startsWith(prefix) && f.endsWith(".md"))
    .sort()
    .reverse(); // most recent first

  for (const file of files) {
    const filePath = join(dir, file);
    const content = readFileSync(filePath, "utf8");
    if (content.includes(`Fingerprint: ${fingerprint}`)) {
      return { path: filePath, content };
    }
  }
  return null;
}

function writePlanSummary(plan: unknown, fingerprint: string): string | null {
  if (!isPlainObject(plan)) return null;

  const rawWorkspaceID = typeof plan.workspaceID === "string" ? plan.workspaceID : "unknown";
  // Sanitize workspaceID for safe use in filenames
  const workspaceID = rawWorkspaceID.replace(/[^a-zA-Z0-9_\-]/g, "_");
  const selection = isPlainObject(plan.nodeTypeSelection) ? plan.nodeTypeSelection : null;
  const consideredNodeTypes = Array.isArray(selection?.consideredNodeTypes)
    ? selection.consideredNodeTypes.filter(isPlainObject)
    : [];
  const supportedNodeTypes = Array.isArray(plan.supportedNodeTypes) ? plan.supportedNodeTypes : [];

  if (consideredNodeTypes.length === 0 && supportedNodeTypes.length === 0) {
    return null;
  }

  const lines: string[] = [
    `# Pipeline Plan — Node Type Reference`,
    ``,
    `Workspace: ${workspaceID}`,
    `Strategy: ${selection?.strategy ?? "unknown"}`,
    `Selected: ${selection?.selectedNodeType ?? "none"}`,
    `Confidence: ${selection?.confidence ?? "unknown"}`,
    `Fingerprint: ${fingerprint}`,
    `Generated: ${new Date().toISOString()}`,
    ``,
    `This file is automatically invalidated when repo-backed ranking inputs or`,
    `workspace node types change enough to alter the planner's ranked guidance.`,
    `If you install new packages, commit new node type definitions, or otherwise`,
    `change ranking-relevant repo content, call plan_pipeline again to refresh.`,
    ``,
    `## Ranked Node Types`,
    ``,
    `Use these node types when calling create_workspace_node_from_predecessor.`,
    `Pick the type whose family matches your pipeline layer (stage, dimension, fact, etc.).`,
    ``,
  ];

  for (const candidate of consideredNodeTypes) {
    const nodeType = typeof candidate.nodeType === "string" ? candidate.nodeType : "?";
    const family = typeof candidate.family === "string" ? candidate.family : "unknown";
    const score = typeof candidate.score === "number" ? candidate.score : 0;
    const displayName = typeof candidate.displayName === "string" ? candidate.displayName : null;
    const reasons = Array.isArray(candidate.reasons) ? candidate.reasons.filter((r): r is string => typeof r === "string") : [];

    lines.push(`### ${nodeType}${displayName ? ` (${displayName})` : ""}`);
    lines.push(`- Family: ${family}`);
    lines.push(`- Score: ${score}`);
    if (reasons.length > 0) {
      lines.push(`- Reasons: ${reasons.join("; ")}`);
    }
    lines.push(``);
  }

  if (supportedNodeTypes.length > 0) {
    lines.push(`## Auto-Executable Types`);
    lines.push(``);
    lines.push(`These types support automatic creation via create_workspace_node_from_predecessor:`);
    lines.push(``);
    for (const nodeType of supportedNodeTypes) {
      lines.push(`- ${nodeType}`);
    }
    lines.push(``);
  }

  const dir = getPlanSummaryDir();
  mkdirSync(dir, { recursive: true });
  const fileName = `plan-${workspaceID}-${Date.now()}.md`;
  const filePath = join(dir, fileName);
  writeFileSync(filePath, lines.join("\n"), "utf8");

  // Clean up old plan files — keep only the 10 most recent per workspace
  cleanupOldPlanFiles(dir, workspaceID, 10);

  return filePath;
}

function cleanupOldPlanFiles(dir: string, workspaceID: string, maxToKeep: number): void {
  try {
    const safeID = workspaceID.replace(/[^a-zA-Z0-9_\-]/g, "_");
    const prefix = `plan-${safeID}-`;
    const files = readdirSync(dir)
      .filter((f) => f.startsWith(prefix) && f.endsWith(".md"))
      .sort()
      .reverse(); // most recent first (timestamp in filename)

    for (const file of files.slice(maxToKeep)) {
      unlinkSync(join(dir, file));
    }
  } catch (error) {
    // Best-effort cleanup — don't fail the write, but log for traceability
    const reason = error instanceof Error ? error.message : String(error);
    process.stderr.write(`[coalesce-transform-mcp] plan file cleanup failed: ${reason}\n`);
  }
}

function buildPlanSummaryForElicitation(plan: unknown): string {
  const lines: string[] = ["Pipeline plan ready. Review the nodes to be created:"];
  lines.push("");

  if (isPlainObject(plan)) {
    // cteNodeSummary is populated for SQL-sourced plans; nodes for goal-based plans
    const cteNodes = Array.isArray(plan.cteNodeSummary) ? plan.cteNodeSummary.filter(isPlainObject) : [];
    const planNodes = Array.isArray(plan.nodes) ? plan.nodes.filter(isPlainObject) : [];
    const nodesToShow = cteNodes.length > 0 ? cteNodes : planNodes;

    if (nodesToShow.length === 0) {
      lines.push("  (No node details available in plan)");
    } else {
      for (const node of nodesToShow) {
        const name = typeof node.name === "string" ? node.name : "(unnamed)";
        const nodeType = typeof node.nodeType === "string" ? node.nodeType : "(unknown type)";
        lines.push(`  • ${name}  [${nodeType}]`);
      }
    }

    const warnings = Array.isArray(plan.warnings)
      ? plan.warnings.filter((w): w is string => typeof w === "string")
      : [];
    if (warnings.length > 0) {
      lines.push("");
      lines.push("Warnings:");
      for (const w of warnings) {
        lines.push(`  ⚠  ${w}`);
      }
    }
  }

  lines.push("");
  lines.push("Confirm to proceed with node creation, or cancel to abort.");
  return lines.join("\n");
}

async function requirePipelineCreationApproval(
  server: McpServer,
  toolName: "create_pipeline_from_plan" | "create_pipeline_from_sql",
  plan: unknown,
  confirmed?: boolean,
  confirmationToken?: string,
  payload: Record<string, unknown> = {}
): Promise<JsonToolResponse | null> {
  if (confirmed === true) {
    // Verify the agent has the exact plan by comparing confirmation tokens.
    // This prevents bypass where an agent sets confirmed=true without actually
    // presenting the plan, but doesn't guarantee the agent presented it accurately.
    const expected = buildPlanConfirmationToken(plan);
    if (confirmationToken !== expected) {
      return buildJsonToolResponse(toolName, {
        created: false,
        STOP_AND_CONFIRM:
          `STOP. The confirmationToken is missing or does not match the current plan. ` +
          `Present the pipeline plan to the user in a table showing each node name and nodeType. ` +
          `Ask for explicit approval BEFORE creating any nodes. Once the user approves, call ${toolName} again with confirmed=true and the confirmationToken from this response.`,
        confirmationToken: expected,
        ...payload,
      });
    }
    return null;
  }

  const clientCapabilities = server.server.getClientCapabilities();
  if (!clientCapabilities?.elicitation?.form) {
    // Client does not support form elicitation — fall back to STOP_AND_CONFIRM convention
    const token = buildPlanConfirmationToken(plan);
    return buildJsonToolResponse(toolName, {
      created: false,
      confirmationToken: token,
      STOP_AND_CONFIRM:
        `STOP. Present the pipeline plan to the user in a table showing each node name and nodeType. ` +
        `Ask for explicit approval BEFORE creating any nodes. Once the user approves, call ${toolName} again with confirmed=true and confirmationToken="${token}".`,
      ...payload,
    });
  }

  const planSummary = buildPlanSummaryForElicitation(plan);
  const elicitation = await server.server.elicitInput({
    message: planSummary,
    requestedSchema: {
      type: "object",
      properties: {
        confirmed: {
          type: "boolean",
          title: "Create these pipeline nodes?",
          description: "Select true to proceed with node creation, false to cancel.",
        },
      },
      required: ["confirmed"],
    },
  });

  if (elicitation.action !== "accept" || elicitation.content?.confirmed !== true) {
    const ACTION_LABELS: Record<string, string> = {
      decline: "declined",
      cancel: "cancelled",
    };
    return buildJsonToolResponse(toolName, {
      created: false,
      cancelled: true,
      reason:
        elicitation.action === "accept"
          ? "User declined pipeline creation."
          : `Pipeline creation ${ACTION_LABELS[elicitation.action] ?? elicitation.action} by user.`,
      ...payload,
    });
  }

  return null;
}

export function registerPipelineTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "plan_pipeline",
    {
      title: "Plan Pipeline",
      description: "Plan a Coalesce pipeline by discovering and ranking all available node types from the repo. ALWAYS call this before creating nodes to get the correct node type.\n\nThe planner scans the repo for all committed node type definitions, scores them against your use case, and returns ranked candidates. When available, it also returns a cached `planSummaryUri` MCP resource for the ranked node type summary so you can reuse that guidance throughout the pipeline without calling the planner again.\n\nIMPORTANT — DO NOT WRITE SQL: The `sql` parameter is ONLY for converting SQL that the USER provided (pasted or typed). If you are building a pipeline yourself, provide `goal` + `sourceNodeIDs` instead.\n\nPREREQUISITE: Before calling this tool, use list_workspace_nodes to discover available source/upstream nodes and their IDs in the workspace.\n\nPreferred approach: Provide `goal` AND `sourceNodeIDs`. The planner selects the best node type and scaffolds the pipeline. Without sourceNodeIDs, the planner returns clarification questions.\n\nUser-provided SQL: When a user pastes SQL, pass it in `sql`. The planner parses refs and column projections.\n\nConsult coalesce://context/node-type-corpus for node type patterns and metadata structures.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        goal: z.string().optional().describe("Optional natural-language pipeline goal"),
        sql: z.string().optional().describe("The user's EXACT SQL, copied verbatim. It may use raw table names or existing Coalesce {{ ref() }} syntax. Do NOT rewrite between SQL styles or modify the query. If you are building a pipeline yourself, do NOT write SQL — use goal + sourceNodeIDs instead."),
        targetName: z.string().optional().describe("Optional target node name override"),
        targetNodeType: z
          .string()
          .optional()
          .describe("Optional node type override. When omitted, the planner ranks repo-backed and observed workspace node types for the use case."),
        description: z.string().optional().describe("Optional node description"),
        configOverrides: NodeConfigInputSchema
          .optional()
          .describe("Optional config overrides to merge into the planned node body."),
        locationName: z.string().optional().describe("Optional target locationName"),
        database: z.string().optional().describe("Optional target database"),
        schema: z.string().optional().describe("Optional target schema"),
        repoPath: z
          .string()
          .optional()
          .describe("Optional local committed Coalesce repo path for repo-first node-type ranking. Falls back to COALESCE_REPO_PATH when omitted."),
        sourceNodeIDs: z
          .array(z.string())
          .optional()
          .describe("Optional upstream node IDs when planning from a non-SQL goal."),
      }),
      outputSchema: getToolOutputSchema("plan_pipeline"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await planPipeline(client, params);

        // Build fingerprint from the actual ranked node-type output used in the summary.
        const selection = isPlainObject(result.nodeTypeSelection) ? result.nodeTypeSelection : null;
        const fingerprint = buildPlanFingerprint(
          params.workspaceID,
          selection,
          Array.isArray(result.supportedNodeTypes)
            ? result.supportedNodeTypes.filter((value): value is string => typeof value === "string")
            : [],
          {
            goal: params.goal,
            sql: params.sql,
            sourceNodeIDs: params.sourceNodeIDs,
            targetNodeType: params.targetNodeType,
          }
        );

        // Check for a cached plan with the same fingerprint
        const cached = findCachedPlanSummary(params.workspaceID, fingerprint);
        const summaryPath = cached?.path ?? writePlanSummary(result, fingerprint);

        // Extract the recommended nodeType and put it at the top level
        // so the agent can't miss it.
        const selectedNodeType = typeof selection?.selectedNodeType === "string"
          ? selection.selectedNodeType
          : null;
        const selectedDisplayName = typeof selection?.selectedDisplayName === "string"
          ? selection.selectedDisplayName
          : null;

        const response = summaryPath
          ? {
              // Put the recommended type FIRST so it's the most visible field
              ...(selectedNodeType ? {
                USE_THIS_NODE_TYPE: selectedNodeType,
                ...(selectedDisplayName ? { nodeTypeDisplayName: selectedDisplayName } : {}),
                nodeTypeInstruction: `Use nodeType "${selectedNodeType}" when calling create_workspace_node_from_predecessor or create_workspace_node_from_scratch. Do NOT use "Source" or any other type unless the plan explicitly recommends it.`,
              } : {}),
              ...result,
              planSummaryUri: summaryPath,
              planCached: !!cached,
              instruction: cached
                ? `Cached node type rankings found at planSummaryUri (ranking fingerprint unchanged). Reference this resource for all subsequent node creations — no need to call plan_pipeline again unless repo-backed ranking inputs or workspace node types change enough to alter the planner's ranking.`
                : `Node type rankings saved to planSummaryUri. Reference this resource for all subsequent node creations in this pipeline. The cache auto-invalidates when repo-backed ranking inputs or workspace node types change enough to alter the planner's ranking.`,
            }
          : {
              ...(selectedNodeType ? {
                USE_THIS_NODE_TYPE: selectedNodeType,
                ...(selectedDisplayName ? { nodeTypeDisplayName: selectedDisplayName } : {}),
                nodeTypeInstruction: `Use nodeType "${selectedNodeType}" when calling create_workspace_node_from_predecessor or create_workspace_node_from_scratch. Do NOT use "Source" or any other type unless the plan explicitly recommends it.`,
              } : {}),
              ...result,
            };
        return buildJsonToolResponse("plan_pipeline", response);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "create_pipeline_from_plan",
    {
      title: "Create Pipeline from Plan",
      description: "Create a Coalesce pipeline from a previously approved plan. Pass the exact plan object returned by plan_pipeline. Projection-capable node types execute by creating predecessor-based nodes first and then persisting the final full node body via set_workspace_node.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - plan (object, required): The exact plan object returned by plan_pipeline\n  - confirmed (boolean, optional): Set to true after user approves the plan. Must be paired with confirmationToken\n  - confirmationToken (string, optional): Token from prior STOP_AND_CONFIRM response. Required when confirmed=true\n  - dryRun (boolean, optional): When true, validate without creating nodes\n\nReturns:\n  { created: boolean, nodes?: CreatedNode[], warnings?: string[] }",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        plan: PipelinePlanSchema.describe("The plan object returned by plan_pipeline."),
        confirmed: z
          .boolean()
          .optional()
          .describe("Set to true only after presenting the plan to the user and receiving explicit approval. Must be paired with the confirmationToken returned by the prior STOP_AND_CONFIRM response."),
        confirmationToken: z
          .string()
          .optional()
          .describe("The token returned in the STOP_AND_CONFIRM response. Required when confirmed=true to prove the plan was presented to the user."),
        dryRun: z
          .boolean()
          .optional()
          .describe("When true, validate the plan and return it without creating any nodes."),
      }),
      outputSchema: getToolOutputSchema("create_pipeline_from_plan"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        if (!params.dryRun) {
          const approvalResponse = await requirePipelineCreationApproval(
            server,
            "create_pipeline_from_plan",
            params.plan,
            params.confirmed,
            params.confirmationToken,
            { plan: params.plan }
          );
          if (approvalResponse) {
            return approvalResponse;
          }
        }

        const result = await createPipelineFromPlan(client, params);
        const response = buildJsonToolResponse("create_pipeline_from_plan", result);
        if (isPlainObject(result) && result.isError) {
          return { ...response, isError: true };
        }
        return response;
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "create_pipeline_from_sql",
    {
      title: "Create Pipeline from SQL",
      description: "Plan and create a Coalesce pipeline from user-provided SQL. Pass the user's EXACT SQL unchanged. The SQL may use raw table names or already contain Coalesce {{ ref() }} syntax if that is what the user provided. Do NOT rewrite between styles or otherwise modify the query. The planner resolves workspace sources automatically and generates a Coalesce-compatible joinCondition for the final node.\n\nIf you are building a pipeline yourself, use declarative tools directly: create_workspace_node_from_predecessor → convert_join_to_aggregation → replace_workspace_node_columns.\n\nThis tool validates candidate node types against currently observed workspace nodes. If a selected type is not observed, the plan will include a warning asking the user to confirm installation in Coalesce.\n\nConsult coalesce://context/node-type-corpus for node type patterns and metadata structures.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        sql: z.string().describe("The user's EXACT SQL, copied verbatim. It may use raw table names or existing Coalesce {{ ref() }} syntax. Do NOT rewrite between SQL styles or modify it in any way. Pass it exactly as the user provided it."),
        goal: z.string().optional().describe("Optional business goal or context for the SQL"),
        targetName: z.string().optional().describe("Optional target node name override"),
        targetNodeType: z
          .string()
          .optional()
          .describe("Optional node type override. When omitted, the planner ranks repo-backed and observed workspace node types for the use case."),
        description: z.string().optional().describe("Optional node description"),
        configOverrides: NodeConfigInputSchema
          .optional()
          .describe("Optional config overrides to merge into the final node body."),
        locationName: z.string().optional().describe("Optional target locationName"),
        database: z.string().optional().describe("Optional target database"),
        schema: z.string().optional().describe("Optional target schema"),
        repoPath: z
          .string()
          .optional()
          .describe("Optional local committed Coalesce repo path for repo-first node-type ranking. Falls back to COALESCE_REPO_PATH when omitted."),
        confirmed: z
          .boolean()
          .optional()
          .describe("Set to true only after presenting the ready plan to the user and receiving explicit approval. Must be paired with the confirmationToken returned by the prior STOP_AND_CONFIRM response."),
        confirmationToken: z
          .string()
          .optional()
          .describe("The token returned in the STOP_AND_CONFIRM response. Required when confirmed=true to prove the plan was presented to the user."),
        dryRun: z
          .boolean()
          .optional()
          .describe("When true, return the generated plan without creating nodes."),
      }),
      outputSchema: getToolOutputSchema("create_pipeline_from_sql"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const plan = await planPipeline(client, params);

        if (params.dryRun || plan.status !== "ready") {
          return buildJsonToolResponse("create_pipeline_from_sql", {
            created: false,
            ...(params.dryRun ? { dryRun: true } : {}),
            plan,
            ...(plan.status !== "ready"
              ? {
                  warning:
                    "SQL was planned but still needs clarification before creation. Review openQuestions and warnings. Present the plan to the user and wait for approval.",
                }
              : {}),
          });
        }

        const approvalResponse = await requirePipelineCreationApproval(
          server,
          "create_pipeline_from_sql",
          plan,
          params.confirmed,
          params.confirmationToken,
          { plan }
        );
        if (approvalResponse) {
          return approvalResponse;
        }

        const execution = await createPipelineFromPlan(client, {
          workspaceID: params.workspaceID,
          plan,
        });
        const result = {
          plan,
          ...((isPlainObject(execution) ? execution : { execution }) as Record<string, unknown>),
        };
        const response = buildJsonToolResponse("create_pipeline_from_sql", result);
        if (isPlainObject(execution) && execution.isError) {
          return { ...response, isError: true };
        }
        return response;
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
