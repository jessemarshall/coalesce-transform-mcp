import { z } from "zod";
import { NodeConfigInputSchema } from "../../schemas/node-payloads.js";
import {
  PIPELINE_NODE_TYPE_FAMILIES,
  type PipelineNodeTypeFamily,
  type PipelineNodeTypeSelection,
  type PipelineTemplateDefaults,
} from "./node-type-selection.js";

export type PipelineIntent = "sql" | "goal";
export type PipelineStatus = "ready" | "needs_clarification";
export type PipelineNodeType = string;

export type PlannedSelectItemKind = "column" | "expression";

export type PlannedSelectItem = {
  expression: string;
  outputName: string | null;
  sourceNodeAlias: string | null;
  sourceNodeName: string | null;
  sourceNodeID: string | null;
  sourceColumnName: string | null;
  kind: PlannedSelectItemKind;
  supported: boolean;
  reason?: string;
};

export type PlannedSourceRef = {
  locationName: string;
  nodeName: string;
  alias: string | null;
  nodeID: string | null;
};

export type PlannedPipelineNode = {
  planNodeID: string;
  name: string;
  nodeType: PipelineNodeType;
  nodeTypeFamily?: PipelineNodeTypeFamily | null;
  predecessorNodeIDs: string[];
  predecessorPlanNodeIDs: string[];
  predecessorNodeNames: string[];
  description: string | null;
  sql: string | null;
  selectItems: PlannedSelectItem[];
  outputColumnNames: string[];
  configOverrides: Record<string, unknown>;
  sourceRefs: PlannedSourceRef[];
  joinCondition: string | null;
  location: {
    locationName?: string;
    database?: string;
    schema?: string;
  };
  requiresFullSetNode: boolean;
  templateDefaults?: PipelineTemplateDefaults;
};

export type PipelinePlan = {
  version: 1;
  intent: PipelineIntent;
  status: PipelineStatus;
  workspaceID: string;
  platform: string | null;
  goal: string | null;
  sql: string | null;
  nodes: PlannedPipelineNode[];
  assumptions: string[];
  openQuestions: string[];
  warnings: string[];
  supportedNodeTypes: PipelineNodeType[];
  nodeTypeSelection?: PipelineNodeTypeSelection;
  cteNodeSummary?: CteNodeSummary[];
  STOP_AND_CONFIRM?: string;
};

export type CteNodeSummary = {
  name: string;
  nodeType: string;
  pattern: "staging" | "multiSource" | "aggregation";
  sourceTable: string | null;
  columnCount: number;
  transforms: Array<{ column: string; expression: string }>;
  passthroughColumns: string[];
  whereFilter: string | null;
  hasGroupBy: boolean;
  hasJoin: boolean;
  dependsOn: string[];
  /** Structured columns for single-call creation (non-GROUP-BY CTEs) */
  columnsParam?: Array<{ name: string; transform?: string }>;
  /** GROUP BY column expressions for single-call aggregation */
  groupByColumnsParam?: string[];
  /** Aggregate columns for single-call aggregation */
  aggregatesParam?: Array<{ name: string; function: string; expression: string }>;
};

const PlannedSelectItemSchema = z
  .object({
    expression: z.string(),
    outputName: z.string().nullable(),
    sourceNodeAlias: z.string().nullable(),
    sourceNodeName: z.string().nullable(),
    sourceNodeID: z.string().nullable(),
    sourceColumnName: z.string().nullable(),
    kind: z.enum(["column", "expression"]),
    supported: z.boolean(),
    reason: z.string().optional(),
  })
  .strict();

const PlannedPipelineNodeSchema = z
  .object({
    planNodeID: z.string(),
    name: z.string(),
    nodeType: z.string(),
    nodeTypeFamily: z
      .enum(PIPELINE_NODE_TYPE_FAMILIES)
      .nullable()
      .optional(),
    predecessorNodeIDs: z.array(z.string()),
    predecessorPlanNodeIDs: z.array(z.string()),
    predecessorNodeNames: z.array(z.string()),
    description: z.string().nullable(),
    sql: z.string().nullable(),
    selectItems: z.array(PlannedSelectItemSchema),
    outputColumnNames: z.array(z.string()),
    configOverrides: NodeConfigInputSchema,
    sourceRefs: z.array(
      z
        .object({
          locationName: z.string(),
          nodeName: z.string(),
          alias: z.string().nullable(),
          nodeID: z.string().nullable(),
        })
        .strict()
    ),
    joinCondition: z.string().nullable(),
    location: z
      .object({
        locationName: z.string().optional(),
        database: z.string().optional(),
        schema: z.string().optional(),
      })
      .strict(),
    requiresFullSetNode: z.boolean(),
    templateDefaults: z
      .object({
        inferredTopLevelFields: z.record(z.unknown()),
        inferredConfig: NodeConfigInputSchema,
      })
      .strict()
      .optional(),
  })
  .strict();

export const PipelinePlanSchema = z
  .object({
    version: z.literal(1),
    intent: z.enum(["sql", "goal"]),
    status: z.enum(["ready", "needs_clarification"]),
    workspaceID: z.string(),
    platform: z.string().nullable(),
    goal: z.string().nullable(),
    sql: z.string().nullable(),
    nodes: z.array(PlannedPipelineNodeSchema),
    assumptions: z.array(z.string()),
    openQuestions: z.array(z.string()),
    warnings: z.array(z.string()),
    supportedNodeTypes: z.array(z.string()),
    nodeTypeSelection: z.record(z.unknown()).optional(),
    cteNodeSummary: z.array(z.record(z.unknown())).optional(),
    STOP_AND_CONFIRM: z.string().optional(),
  })
  .strict();

export type ResolvedSqlRef = {
  locationName: string;
  nodeName: string;
  alias: string | null;
  nodeID: string | null;
};

export type ParsedSqlSourceRef = ResolvedSqlRef & {
  sourceStyle: "coalesce_ref" | "table_name";
  locationCandidates: string[];
  relationStart: number;
  relationEnd: number;
};

export type SqlParseResult = {
  refs: ParsedSqlSourceRef[];
  selectItems: PlannedSelectItem[];
  warnings: string[];
};

export type WorkspaceNodeTypeInventory = {
  nodeTypes: string[];
  counts: Record<string, number>;
  total: number;
  warnings: string[];
};

export const WORKSPACE_NODE_PAGE_LIMIT = 200;
export const DEFAULT_STAGE_CONFIG: Record<string, unknown> = {
  postSQL: "",
  preSQL: "",
  testsEnabled: true,
};
