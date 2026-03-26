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
  createPipelineFromSql,
} from "../services/pipelines/execution.js";
import { NodeConfigInputSchema } from "../schemas/node-payloads.js";
import {
  buildJsonToolResponse,
  handleToolError,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  validatePathSegment,
} from "../coalesce/types.js";
import { isPlainObject } from "../utils.js";

const REWRITTEN_SQL_ERROR_MESSAGE =
  "The sql parameter contains {{ ref() }} syntax, which means you rewrote the user's SQL. " +
  "Pass the user's EXACT SQL unchanged — the planner resolves source references automatically. " +
  "Do NOT replace table names with {{ ref() }}.";

function buildPlanFingerprint(
  workspaceID: string,
  repoPath: string | null,
  workspaceNodeTypes: string[],
  requestInputs?: {
    goal?: string;
    sql?: string;
    sourceNodeIDs?: string[];
    targetNodeType?: string;
  }
): string {
  const input = [
    `workspace:${workspaceID}`,
    `repo:${repoPath ?? "none"}`,
    `types:${[...workspaceNodeTypes].sort().join(",")}`,
    `goal:${requestInputs?.goal ?? ""}`,
    `sql:${requestInputs?.sql ?? ""}`,
    `sources:${requestInputs?.sourceNodeIDs ? [...requestInputs.sourceNodeIDs].sort().join(",") : ""}`,
    `targetType:${requestInputs?.targetNodeType ?? ""}`,
  ].join("|");
  return createHash("sha256").update(input).digest("hex").slice(0, 16);
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
    `This file is automatically invalidated when the repo node types or workspace`,
    `node types change. If you install new packages or commit new node type`,
    `definitions, call plan-pipeline again to refresh.`,
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
      sql: z.string().optional().describe("The user's EXACT SQL, copied verbatim. Do NOT rewrite table names, do NOT add {{ ref() }} syntax, do NOT modify it. Pass it exactly as the user provided it. If you are building a pipeline yourself, do NOT write SQL — use goal + sourceNodeIDs instead."),
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
        // Reject SQL that the agent rewrote with {{ ref() }}
        if (params.sql && /\{\{\s*ref\s*\(/.test(params.sql)) {
          return handleToolError(new Error(REWRITTEN_SQL_ERROR_MESSAGE));
        }

        const result = await planPipeline(client, params);

        // Build fingerprint from workspace + repo + observed types
        const selection = isPlainObject(result.nodeTypeSelection) ? result.nodeTypeSelection : null;
        const workspaceNodeTypes = Array.isArray(selection?.workspaceObservedNodeTypes)
          ? (selection.workspaceObservedNodeTypes as string[])
          : [];
        const repoPath = typeof selection?.resolvedRepoPath === "string"
          ? selection.resolvedRepoPath
          : null;
        const fingerprint = buildPlanFingerprint(
          params.workspaceID,
          repoPath,
          workspaceNodeTypes,
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
                ? `Cached node type rankings found at planSummaryUri (fingerprint unchanged). Reference this resource for all subsequent node creations — no need to call plan-pipeline again unless you install new packages or commit new node type definitions.`
                : `Node type rankings saved to planSummaryUri. Reference this resource for all subsequent node creations in this pipeline. The cache auto-invalidates when repo or workspace node types change.`,
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
      dryRun: z
        .boolean()
        .optional()
        .describe("When true, validate the plan and return it without creating any nodes."),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        if (!params.dryRun) {
          const planSummary = buildPlanSummaryForElicitation(params.plan);
          try {
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
              return buildJsonToolResponse("create-pipeline-from-plan", {
                created: false,
                cancelled: true,
                reason:
                  elicitation.action === "accept"
                    ? "User declined pipeline creation."
                    : `Pipeline creation ${elicitation.action}d by user.`,
              });
            }
          } catch (elicitError) {
            // Client does not support elicitation — fall back to STOP_AND_CONFIRM convention
            if (elicitError instanceof Error && elicitError.message.includes("does not support")) {
              return buildJsonToolResponse("create-pipeline-from-plan", {
                created: false,
                STOP_AND_CONFIRM:
                  "STOP. Present the pipeline plan to the user in a table showing each node name and nodeType. Ask for explicit approval BEFORE creating any nodes. Once the user approves, call create-pipeline-from-plan again.",
                plan: params.plan,
              });
            }
            throw elicitError;
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
    "Plan and create a Coalesce pipeline from user-provided SQL. Pass the user's EXACT SQL unchanged — do NOT rewrite it, do NOT replace table references with {{ ref() }}, do NOT modify the SQL in any way. The planner handles source resolution automatically.\n\nIf you are building a pipeline yourself, use declarative tools directly: create-workspace-node-from-predecessor → convert-join-to-aggregation → replace-workspace-node-columns.\n\nThis tool validates candidate node types against currently observed workspace nodes. If a selected type is not observed, the plan will include a warning asking the user to confirm installation in Coalesce.\n\nConsult coalesce://context/node-type-corpus for node type patterns and metadata structures.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      sql: z.string().describe("The user's EXACT SQL, copied verbatim. Do NOT rewrite table names, do NOT add {{ ref() }} syntax, do NOT modify it in any way. Pass it exactly as the user provided it."),
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
      dryRun: z
        .boolean()
        .optional()
        .describe("When true, return the generated plan without creating nodes."),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        // Reject SQL that the agent rewrote with {{ ref() }} — the user's original SQL won't contain these
        if (/\{\{\s*ref\s*\(/.test(params.sql)) {
          return handleToolError(new Error(REWRITTEN_SQL_ERROR_MESSAGE));
        }
        const result = await createPipelineFromSql(client, params);
        return buildJsonToolResponse("create-pipeline-from-sql", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
