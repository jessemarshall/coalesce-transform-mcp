import { uniqueInOrder } from "../../utils.js";
import {
  type PipelineNodeTypeFamily,
  type PipelineNodeTypeSelection,
  type PipelineTemplateDefaults,
} from "./node-type-selection.js";
import {
  type PlannedSelectItem,
  type PlannedPipelineNode,
  type PipelinePlan,
  type ParsedSqlSourceRef,
  type PlannedSourceRef,
  type SqlParseResult,
  type ResolvedSqlRef,
} from "./planning-types.js";
import {
  normalizeSqlIdentifier,
  extractFromClause,
  deepClone,
} from "./sql-parsing.js";
import { getColumnNamesFromNode } from "./column-helpers.js";

export function buildSelectItemsFromSourceNode(
  sourceNodeID: string,
  sourceNodeName: string,
  node: Record<string, unknown>
): PlannedSelectItem[] {
  return getColumnNamesFromNode(node).map((columnName) => ({
    expression: `${sourceNodeName}.${columnName}`,
    outputName: columnName,
    sourceNodeAlias: sourceNodeName,
    sourceNodeName,
    sourceNodeID,
    sourceColumnName: columnName,
    kind: "column" as const,
    supported: true,
  }));
}

export function expandWildcardSelectItems(
  selectItems: PlannedSelectItem[],
  refs: ResolvedSqlRef[],
  predecessorNodes: Record<string, Record<string, unknown>>
): PlannedSelectItem[] {
  const expanded: PlannedSelectItem[] = [];

  for (const item of selectItems) {
    if (item.sourceColumnName !== "*" || !item.supported) {
      expanded.push(item);
      continue;
    }

    const ref =
      item.sourceNodeID
        ? refs.find((candidate) => candidate.nodeID === item.sourceNodeID) ?? null
        : refs.find(
            (candidate) =>
              normalizeSqlIdentifier(candidate.alias ?? candidate.nodeName) ===
              normalizeSqlIdentifier(item.sourceNodeAlias ?? "")
          ) ?? null;
    if (!ref?.nodeID) {
      expanded.push({
        ...item,
        supported: false,
        reason: "Wildcard source could not be resolved to a concrete predecessor node.",
      });
      continue;
    }

    const predecessor = predecessorNodes[ref.nodeID];
    if (!predecessor) {
      expanded.push({
        ...item,
        supported: false,
        reason: "Wildcard source predecessor body was not available for column expansion.",
      });
      continue;
    }

    const columnNames = getColumnNamesFromNode(predecessor);
    if (columnNames.length === 0) {
      expanded.push({
        ...item,
        supported: false,
        reason: "Wildcard source predecessor has no columns to expand.",
      });
      continue;
    }

    for (const columnName of columnNames) {
      expanded.push({
        expression:
          item.sourceNodeAlias && item.sourceNodeAlias.length > 0
            ? `${item.sourceNodeAlias}.${columnName}`
            : columnName,
        outputName: columnName,
        sourceNodeAlias: item.sourceNodeAlias,
        sourceNodeName: item.sourceNodeName,
        sourceNodeID: ref.nodeID,
        sourceColumnName: columnName,
        kind: "column",
        supported: true,
      });
    }
  }

  return expanded;
}

// ---------------------------------------------------------------------------
// Node naming
// ---------------------------------------------------------------------------

export function buildDefaultNodePrefix(
  nodeTypeFamily: PipelineNodeTypeFamily | null | undefined,
  shortName: string | null | undefined
): string {
  if (shortName && shortName.trim().length > 0) {
    return shortName.trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_");
  }

  switch (nodeTypeFamily) {
    case "stage":
      return "STG";
    case "persistent-stage":
      return "PSTG";
    case "view":
      return "VW";
    case "work":
      return "WRK";
    case "dimension":
      return "DIM";
    case "fact":
      return "FACT";
    case "hub":
      return "HUB";
    case "satellite":
      return "SAT";
    case "link":
      return "LNK";
    default:
      return "NODE";
  }
}

export function buildDefaultNodeName(
  targetName: string | undefined,
  refs: Array<ResolvedSqlRef | PlannedSourceRef>,
  nodeTypeFamily?: PipelineNodeTypeFamily | null,
  shortName?: string | null
): string {
  if (targetName && targetName.trim().length > 0) {
    return targetName.trim();
  }

  const prefix = buildDefaultNodePrefix(nodeTypeFamily, shortName);
  const firstRef = refs[0];
  if (!firstRef) {
    return `${prefix}_NEW_PIPELINE`;
  }

  const stripped = firstRef.nodeName.replace(
    /^(SRC[_-]?|STG[_-]?|DIM[_-]?|FACT[_-]?|FCT[_-]?|INT[_-]?|WORK[_-]?|VW[_-]?)/i,
    ""
  );
  return `${prefix}_${stripped}`.toUpperCase().replace(/__+/g, "_");
}

// ---------------------------------------------------------------------------
// Join condition from SQL
// ---------------------------------------------------------------------------

function buildJoinConditionFromSql(
  sql: string,
  refs: ParsedSqlSourceRef[]
): string | null {
  const fromClause = extractFromClause(sql);
  if (!fromClause) {
    return null;
  }

  let joinCondition = fromClause;
  for (const ref of [...refs]
    .filter((candidate) => candidate.sourceStyle === "table_name" && candidate.locationName)
    .sort((left, right) => right.relationStart - left.relationStart)) {
    const replacement = `{{ ref('${ref.locationName}', '${ref.nodeName}') }}`;
    joinCondition =
      joinCondition.slice(0, ref.relationStart) +
      replacement +
      joinCondition.slice(ref.relationEnd);
  }

  return joinCondition;
}

// ---------------------------------------------------------------------------
// Plan construction from SQL
// ---------------------------------------------------------------------------

export function buildPlanFromSql(
  params: {
    workspaceID: string;
    goal?: string;
    sql: string;
    targetName?: string;
    description?: string;
    targetNodeType?: string;
    configOverrides?: Record<string, unknown>;
    nodeTypeSelection: PipelineNodeTypeSelection;
    selectedNodeType?: {
      nodeType: string;
      displayName: string | null;
      shortName: string | null;
      family: PipelineNodeTypeFamily;
      autoExecutable: boolean;
      semanticSignals: string[];
      missingDefaultFields: string[];
      templateWarnings: string[];
      templateDefaults?: PipelineTemplateDefaults;
    } | null;
    location?: {
      locationName?: string;
      database?: string;
      schema?: string;
    };
  },
  parseResult: SqlParseResult,
  predecessorNodes: Record<string, Record<string, unknown>>,
  openQuestions: string[],
  warnings: string[]
): PipelinePlan {
  const nodeType =
    params.selectedNodeType?.nodeType ?? params.targetNodeType ?? "Stage";
  const planOpenQuestions = [...openQuestions];
  if (!params.selectedNodeType) {
    warnings.push(
      `No ranked node type candidate was available, so planning fell back to ${nodeType}.`
    );
  } else if (!params.selectedNodeType.autoExecutable) {
    warnings.push(
      `Planner selected node type ${nodeType}, but it likely needs additional semantic configuration before automatic creation.`
    );
    if (params.selectedNodeType.semanticSignals.length > 0) {
      planOpenQuestions.push(
        `Confirm the required configuration for ${nodeType}: ${params.selectedNodeType.semanticSignals.join(
          ", "
        )}.`
      );
    }
    if (params.selectedNodeType.missingDefaultFields.length > 0) {
      planOpenQuestions.push(
        `Provide values for ${nodeType} config fields without defaults: ${params.selectedNodeType.missingDefaultFields.join(
          ", "
        )}.`
      );
    }
  }

  const expandedSelectItems = expandWildcardSelectItems(
    parseResult.selectItems,
    parseResult.refs,
    predecessorNodes
  );
  const unsupportedItems = expandedSelectItems.filter((item) => !item.supported);
  if (unsupportedItems.length > 0) {
    for (const item of unsupportedItems) {
      warnings.push(
        item.reason
          ? `${item.expression}: ${item.reason}`
          : `${item.expression}: unsupported SQL projection in v1`
      );
    }
  }

  const supportedOutputColumnCount = expandedSelectItems.filter(
    (item) => item.supported && item.outputName
  ).length;
  if (
    parseResult.warnings.some((warning) =>
      warning.includes("Could not find a top-level SELECT ... FROM clause")
    )
  ) {
    planOpenQuestions.push(
      "Provide a top-level SELECT ... FROM query using direct column projections before creating this pipeline."
    );
  } else if (supportedOutputColumnCount === 0) {
    planOpenQuestions.push(
      "Specify at least one supported projected column before creating this pipeline."
    );
  }

  const predecessorNodeIDs = uniqueInOrder(parseResult.refs.flatMap((ref) =>
    ref.nodeID ? [ref.nodeID] : []
  ));
  const predecessorNodeNames = parseResult.refs.map((ref) => ref.nodeName);

  const ready =
    (params.selectedNodeType?.autoExecutable ?? true) &&
    predecessorNodeIDs.length > 0 &&
    supportedOutputColumnCount > 0 &&
    unsupportedItems.length === 0 &&
    parseResult.warnings.length === 0 &&
    planOpenQuestions.length === 0;

  const name = buildDefaultNodeName(
    params.targetName,
    parseResult.refs,
    params.selectedNodeType?.family ?? null,
    params.selectedNodeType?.shortName ?? null
  );
  const plan: PipelinePlan = {
    version: 1,
    intent: "sql",
    status: ready ? "ready" : "needs_clarification",
    workspaceID: params.workspaceID,
    platform: null,
    goal: params.goal ?? null,
    sql: params.sql,
    nodes: [
      {
        planNodeID: "node-1",
        name,
        nodeType,
        nodeTypeFamily: params.selectedNodeType?.family ?? null,
        predecessorNodeIDs,
        predecessorPlanNodeIDs: [],
        predecessorNodeNames,
        description: params.description ?? null,
        sql: params.sql,
        selectItems: expandedSelectItems,
        outputColumnNames: expandedSelectItems.flatMap((item) =>
          item.outputName ? [item.outputName] : []
        ),
        configOverrides: params.configOverrides ? deepClone(params.configOverrides) : {},
        sourceRefs: parseResult.refs.map((ref) => ({
          locationName: ref.locationName,
          nodeName: ref.nodeName,
          alias: ref.alias,
          nodeID: ref.nodeID,
        })),
        joinCondition: buildJoinConditionFromSql(params.sql, parseResult.refs),
        location: params.location ?? {},
        requiresFullSetNode: true,
        ...(params.selectedNodeType?.templateDefaults
          ? { templateDefaults: params.selectedNodeType.templateDefaults }
          : {}),
      },
    ],
    assumptions: [
      `Planner ${params.nodeTypeSelection.strategy} selected ${nodeType} from repo/workspace candidates.`,
      "The generated plan uses create_workspace_node_from_predecessor followed by set_workspace_node when the selected type is projection-capable.",
    ],
    openQuestions: planOpenQuestions,
    warnings: [...parseResult.warnings, ...warnings],
    supportedNodeTypes:
      params.nodeTypeSelection.supportedNodeTypes.length > 0
        ? params.nodeTypeSelection.supportedNodeTypes
        : [nodeType],
    nodeTypeSelection: params.nodeTypeSelection,
  };

  return plan;
}
