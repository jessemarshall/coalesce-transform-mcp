import { z } from "zod";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { CACHE_DIR_NAME } from "../cache-dir.js";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
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
  handleToolError,
  type JsonToolResponse,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  validatePathSegment,
} from "../coalesce/types.js";
import { isPlainObject } from "../utils.js";

/**
 * Recursively sorts JSON values to ensure deterministic serialization.
 *
 * Object keys are sorted alphabetically to guarantee that structurally
 * identical objects produce identical JSON strings when serialized.
 * This is essential for generating consistent confirmation tokens via
 * hashing, where the same plan content must always yield the same hash
 * regardless of key insertion order.
 *
 * @param value - The value to sort (arrays, objects, or primitives)
 * @returns A deep copy with all object keys sorted alphabetically
 */
function sortJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortJsonValue);
  }
  if (!isPlainObject(value)) {
    return value;
  }

  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    const nested = sortJsonValue(value[key]);
    if (nested !== undefined) {
      sorted[key] = nested;
    }
  }
  return sorted;
}

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
    `change ranking-relevant repo content, call plan-pipeline again to refresh.`,
    ``,
    `## Ranked Node Types`,
    ``,
    `Use these node types when calling create-workspace-node-from-predecessor.`,
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
    lines.push(`These types support automatic creation via create-workspace-node-from-predecessor:`);
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
  } catch {
    // Best-effort cleanup — don't fail the write
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

/**
 * Generates a confirmation token for a pipeline plan to prevent bypass of user approval.
 *
 * The token is a SHA256 hash (truncated to 16 hex chars) of the canonicalized plan JSON.
 * AI agents must provide this token when calling pipeline creation tools with `confirmed=true`,
 * proving they received and can reference the exact plan that should have been presented to the user.
 *
 * **Important limitations:**
 * - The token proves the agent received the correct plan (plan integrity)
 * - It does NOT verify the agent presented the plan accurately to the user
 * - An agent could theoretically show incomplete/misleading info but still provide the valid token
 * - This is an acceptable tradeoff: the token prevents accidental bypass and honest mistakes,
 *   while deliberate deception by a malicious agent is out of scope
 *
 * @param plan - The pipeline plan object to fingerprint
 * @returns A 16-character hex token uniquely identifying this plan's content
 */
export function buildPlanConfirmationToken(plan: unknown): string {
  return createHash("sha256")
    .update(JSON.stringify(sortJsonValue(plan)))
    .digest("hex")
    .slice(0, 16);
}

async function requirePipelineCreationApproval(
  server: McpServer,
  toolName: "create-pipeline-from-plan" | "create-pipeline-from-sql",
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
  server.tool(
    "plan-pipeline",
    "Plan a Coalesce pipeline by discovering and ranking all available node types from the repo. ALWAYS call this before creating nodes to get the correct node type.\n\nThe planner scans the repo for all committed node type definitions, scores them against your use case, and returns ranked candidates. When available, it also returns a cached `planSummaryUri` MCP resource for the ranked node type summary so you can reuse that guidance throughout the pipeline without calling the planner again.\n\nIMPORTANT — DO NOT WRITE SQL: The `sql` parameter is ONLY for converting SQL that the USER provided (pasted or typed). If you are building a pipeline yourself, provide `goal` + `sourceNodeIDs` instead.\n\nPREREQUISITE: Before calling this tool, use list-workspace-nodes to discover available source/upstream nodes and their IDs in the workspace.\n\nPreferred approach: Provide `goal` AND `sourceNodeIDs`. The planner selects the best node type and scaffolds the pipeline. Without sourceNodeIDs, the planner returns clarification questions.\n\nUser-provided SQL: When a user pastes SQL, pass it in `sql`. The planner parses refs and column projections.\n\nConsult coalesce://context/node-type-corpus for node type patterns and metadata structures.",
    {
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
    },
    READ_ONLY_ANNOTATIONS,
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
                nodeTypeInstruction: `Use nodeType "${selectedNodeType}" when calling create-workspace-node-from-predecessor or create-workspace-node-from-scratch. Do NOT use "Source" or any other type unless the plan explicitly recommends it.`,
              } : {}),
              ...result,
              planSummaryUri: summaryPath,
              planCached: !!cached,
              instruction: cached
                ? `Cached node type rankings found at planSummaryUri (ranking fingerprint unchanged). Reference this resource for all subsequent node creations — no need to call plan-pipeline again unless repo-backed ranking inputs or workspace node types change enough to alter the planner's ranking.`
                : `Node type rankings saved to planSummaryUri. Reference this resource for all subsequent node creations in this pipeline. The cache auto-invalidates when repo-backed ranking inputs or workspace node types change enough to alter the planner's ranking.`,
            }
          : {
              ...(selectedNodeType ? {
                USE_THIS_NODE_TYPE: selectedNodeType,
                ...(selectedDisplayName ? { nodeTypeDisplayName: selectedDisplayName } : {}),
                nodeTypeInstruction: `Use nodeType "${selectedNodeType}" when calling create-workspace-node-from-predecessor or create-workspace-node-from-scratch. Do NOT use "Source" or any other type unless the plan explicitly recommends it.`,
              } : {}),
              ...result,
            };
        return buildJsonToolResponse("plan-pipeline", response);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "create-pipeline-from-plan",
    "Create a Coalesce pipeline from a previously approved plan. Projection-capable node types execute by creating predecessor-based nodes first and then persisting the final full node body with set-workspace-node.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      plan: PipelinePlanSchema.describe("The plan object returned by plan-pipeline."),
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
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        if (!params.dryRun) {
          const approvalResponse = await requirePipelineCreationApproval(
            server,
            "create-pipeline-from-plan",
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
        return buildJsonToolResponse("create-pipeline-from-plan", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "create-pipeline-from-sql",
    "Plan and create a Coalesce pipeline from user-provided SQL. Pass the user's EXACT SQL unchanged. The SQL may use raw table names or already contain Coalesce {{ ref() }} syntax if that is what the user provided. Do NOT rewrite between styles or otherwise modify the query. The planner resolves workspace sources automatically and generates a Coalesce-compatible joinCondition for the final node.\n\nIf you are building a pipeline yourself, use declarative tools directly: create-workspace-node-from-predecessor → convert-join-to-aggregation → replace-workspace-node-columns.\n\nThis tool validates candidate node types against currently observed workspace nodes. If a selected type is not observed, the plan will include a warning asking the user to confirm installation in Coalesce.\n\nConsult coalesce://context/node-type-corpus for node type patterns and metadata structures.",
    {
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
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const plan = await planPipeline(client, params);

        if (params.dryRun || plan.status !== "ready") {
          return buildJsonToolResponse("create-pipeline-from-sql", {
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
          "create-pipeline-from-sql",
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
        return buildJsonToolResponse("create-pipeline-from-sql", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
