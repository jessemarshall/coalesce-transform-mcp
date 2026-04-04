import { CoalesceApiError, type CoalesceClient } from "../../client.js";
import {
  getWorkspaceNode,
  listWorkspaceNodes,
} from "../../coalesce/api/nodes.js";
import { listWorkspaceNodeTypes } from "../workspace/mutations.js";
import { isPlainObject, uniqueInOrder } from "../../utils.js";
import { type WorkspaceNodeIndexEntry } from "../shared/node-helpers.js";
import {
  type PipelineNodeTypeFamily,
  type PipelineNodeTypeSelection,
  type PipelineTemplateDefaults,
} from "./node-type-selection.js";
import {
  WORKSPACE_NODE_PAGE_LIMIT,
  type PlannedSelectItem,
  type PlannedPipelineNode,
  type PipelinePlan,
  type ParsedSqlSourceRef,
  type PlannedSourceRef,
  type SqlParseResult,
  type ResolvedSqlRef,
  type WorkspaceNodeTypeInventory,
} from "./planning-types.js";
import {
  normalizeSqlIdentifier,
  extractFromClause,
  deepClone,
} from "./sql-parsing.js";
import { getColumnNamesFromNode } from "./column-helpers.js";

async function listAllWorkspaceNodes(
  client: CoalesceClient,
  workspaceID: string
): Promise<WorkspaceNodeIndexEntry[]> {
  const nodes: WorkspaceNodeIndexEntry[] = [];
  const seenCursors = new Set<string>();
  let next: string | undefined;
  let isFirstPage = true;

  while (isFirstPage || next) {
    const response = await listWorkspaceNodes(client, {
      workspaceID,
      limit: WORKSPACE_NODE_PAGE_LIMIT,
      orderBy: "id",
      ...(next ? { startingFrom: next } : {}),
    });

    if (!isPlainObject(response)) {
      throw new Error("Workspace node list response was not an object");
    }

    if (Array.isArray(response.data)) {
      for (const item of response.data) {
        if (!isPlainObject(item) || typeof item.id !== "string" || typeof item.name !== "string") {
          continue;
        }
        nodes.push({
          id: item.id,
          name: item.name,
          nodeType: typeof item.nodeType === "string" ? item.nodeType : null,
          locationName:
            typeof item.locationName === "string" ? item.locationName : null,
        });
      }
    }

    const responseNext =
      typeof response.next === "string" && response.next.trim().length > 0
        ? response.next
        : typeof response.next === "number"
          ? String(response.next)
          : undefined;
    if (responseNext) {
      if (seenCursors.has(responseNext)) {
        throw new Error(`Workspace node pagination repeated cursor ${responseNext}`);
      }
      seenCursors.add(responseNext);
    }

    next = responseNext;
    isFirstPage = false;
  }

  return nodes;
}

function getNodeLocationName(node: Record<string, unknown>): string | null {
  if (typeof node.locationName === "string" && node.locationName.trim().length > 0) {
    return node.locationName;
  }
  return null;
}

export async function resolveSqlRefsToWorkspaceNodes(
  client: CoalesceClient,
  workspaceID: string,
  refs: ParsedSqlSourceRef[]
): Promise<{
  refs: ParsedSqlSourceRef[];
  openQuestions: string[];
  warnings: string[];
  predecessorNodes: Record<string, Record<string, unknown>>;
}> {
  const warnings: string[] = [];
  const openQuestions: string[] = [];
  const predecessorNodes: Record<string, Record<string, unknown>> = {};

  if (refs.length === 0) {
    openQuestions.push(
      "Which upstream Coalesce node(s) should this pipeline build from? Use a top-level FROM/JOIN that names existing workspace nodes (raw table names or {{ ref('LOCATION', 'NODE') }} syntax), or provide sourceNodeIDs."
    );
    return { refs, openQuestions, warnings, predecessorNodes };
  }

  const workspaceNodes = await listAllWorkspaceNodes(client, workspaceID);
  const nodesByNormalizedName = new Map<string, WorkspaceNodeIndexEntry[]>();
  for (const node of workspaceNodes) {
    const normalized = normalizeSqlIdentifier(node.name);
    const existing = nodesByNormalizedName.get(normalized) ?? [];
    existing.push(node);
    nodesByNormalizedName.set(normalized, existing);
  }

  for (const ref of refs) {
    const matches =
      nodesByNormalizedName.get(normalizeSqlIdentifier(ref.nodeName)) ?? [];
    if (matches.length === 0) {
      openQuestions.push(
        `Could not resolve the SQL source ${ref.nodeName} to a workspace node ID in workspace ${workspaceID}.`
      );
      continue;
    }

    const locationHints = [
      ...(ref.locationName ? [ref.locationName] : []),
      ...ref.locationCandidates,
    ].map(normalizeSqlIdentifier);
    const hintedMatches =
      locationHints.length > 0
        ? matches.filter(
            (entry) =>
              entry.locationName &&
              locationHints.includes(normalizeSqlIdentifier(entry.locationName))
          )
        : [];

    if (hintedMatches.length === 1) {
      ref.nodeID = hintedMatches[0]?.id ?? null;
      if (!ref.locationName && hintedMatches[0]?.locationName) {
        ref.locationName = hintedMatches[0].locationName;
      }
      continue;
    }
    if (hintedMatches.length > 1) {
      openQuestions.push(
        `Multiple workspace nodes matched the SQL source ${ref.nodeName}. Resolve the exact node before creation.`
      );
      continue;
    }

    if (matches.length === 1) {
      ref.nodeID = matches[0]?.id ?? null;
      if (!ref.locationName && matches[0]?.locationName) {
        ref.locationName = matches[0].locationName;
      }
      continue;
    }

    if (matches.length > 1) {
      const detailedMatches = await Promise.all(
        matches.map(async (match) => {
          const node = await getWorkspaceNode(client, {
            workspaceID,
            nodeID: match.id,
          });
          return {
            match,
            node: isPlainObject(node) ? node : null,
          };
        })
      );
      const exactLocationMatches =
        locationHints.length > 0
          ? detailedMatches.filter(
              (candidate) =>
                candidate.node &&
                getNodeLocationName(candidate.node) &&
                locationHints.includes(
                  normalizeSqlIdentifier(getNodeLocationName(candidate.node) ?? "")
                )
            )
          : [];
      if (exactLocationMatches.length === 1) {
        ref.nodeID = exactLocationMatches[0]?.match.id ?? null;
        if (!ref.locationName) {
          ref.locationName = getNodeLocationName(exactLocationMatches[0]?.node ?? {}) ?? "";
        }
        continue;
      }
      if (exactLocationMatches.length > 1) {
        openQuestions.push(
          `Multiple workspace nodes matched the SQL source ${ref.nodeName}. Resolve the exact node before creation.`
        );
        continue;
      }

      if (ref.sourceStyle === "coalesce_ref" && ref.locationName) {
        openQuestions.push(
          `Workspace nodes named ${ref.nodeName} were found, but none matched the requested location ${ref.locationName}.`
        );
        continue;
      }

      openQuestions.push(
        `Multiple workspace nodes named ${ref.nodeName} were found. Qualify the SQL source more clearly or provide sourceNodeIDs before creation.`
      );
      continue;
    }
  }

  for (const ref of refs) {
    if (!ref.nodeID) {
      continue;
    }
    const predecessor = await getWorkspaceNode(client, {
      workspaceID,
      nodeID: ref.nodeID,
    });
    if (!isPlainObject(predecessor)) {
      warnings.push(`Resolved predecessor ${ref.nodeName} did not return an object body.`);
      continue;
    }
    const predecessorLocationName = getNodeLocationName(predecessor);
    if (
      ref.sourceStyle === "coalesce_ref" &&
      predecessorLocationName &&
      normalizeSqlIdentifier(predecessorLocationName) !==
        normalizeSqlIdentifier(ref.locationName)
    ) {
      ref.nodeID = null;
      openQuestions.push(
        `Resolved node ${ref.nodeName} is in location ${predecessorLocationName}, not the requested location ${ref.locationName}.`
      );
      continue;
    }
    if (!ref.locationName && predecessorLocationName) {
      ref.locationName = predecessorLocationName;
    }
    predecessorNodes[ref.nodeID] = predecessor;
  }

  return { refs, openQuestions, warnings, predecessorNodes };
}

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

export async function getSourceNodesByID(
  client: CoalesceClient,
  workspaceID: string,
  sourceNodeIDs: string[]
): Promise<{
  sourceRefs: PlannedSourceRef[];
  predecessorNodes: Record<string, Record<string, unknown>>;
  openQuestions: string[];
  warnings: string[];
}> {
  const sourceRefs: PlannedSourceRef[] = [];
  const predecessorNodes: Record<string, Record<string, unknown>> = {};
  const openQuestions: string[] = [];
  const warnings: string[] = [];

  for (const sourceNodeID of sourceNodeIDs) {
    const node = await getWorkspaceNode(client, {
      workspaceID,
      nodeID: sourceNodeID,
    });
    if (!isPlainObject(node)) {
      openQuestions.push(
        `Could not read source node ${sourceNodeID} in workspace ${workspaceID}.`
      );
      continue;
    }
    if (typeof node.name !== "string" || node.name.trim().length === 0) {
      openQuestions.push(`Source node ${sourceNodeID} does not have a usable name.`);
      continue;
    }
    const locationName = getNodeLocationName(node);
    if (!locationName) {
      openQuestions.push(
        `Source node ${node.name} does not expose locationName. Clarify the Coalesce location before generating ref() SQL for this pipeline.`
      );
    }

    predecessorNodes[sourceNodeID] = node;
    sourceRefs.push({
      locationName: locationName ?? "UNKNOWN_LOCATION",
      nodeName: node.name,
      alias: node.name,
      nodeID: sourceNodeID,
    });
  }

  return {
    sourceRefs,
    predecessorNodes,
    openQuestions,
    warnings,
  };
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

function matchesObservedNodeType(
  requestedNodeType: string,
  observedNodeTypes: string[]
): boolean {
  const requestedID = requestedNodeType.includes(":::")
    ? requestedNodeType.split(":::")[1] ?? requestedNodeType
    : requestedNodeType;

  return observedNodeTypes.some((observed) => {
    if (observed === requestedNodeType) {
      return true;
    }
    const observedID = observed.includes(":::") ? observed.split(":::")[1] ?? observed : observed;
    return observedID === requestedID;
  });
}

export async function getWorkspaceNodeTypeInventory(
  client: CoalesceClient,
  workspaceID: string
): Promise<WorkspaceNodeTypeInventory> {
  try {
    const result = await listWorkspaceNodeTypes(client, { workspaceID });
    return {
      nodeTypes: result.nodeTypes ?? [],
      counts: result.counts ?? {},
      total: result.total ?? 0,
      warnings: [],
    };
  } catch (error) {
    // Auth and network errors indicate a broken session — let them propagate
    if (error instanceof CoalesceApiError && [401, 403, 500, 503].includes(error.status)) {
      throw error;
    }
    const reason = error instanceof Error ? error.message : String(error);
    return {
      nodeTypes: [],
      counts: {},
      total: 0,
      warnings: [
        `Observed workspace node types could not be fetched for workspace ${workspaceID} (${reason}). ` +
          `Node type selection will use defaults — use list_workspace_node_types or cache_workspace_nodes to confirm installation before execution.`,
      ],
    };
  }
}

export function applyWorkspaceNodeTypeValidation(
  plan: PipelinePlan,
  inventory: WorkspaceNodeTypeInventory,
  requestedNodeType?: string
): void {
  plan.warnings.push(...inventory.warnings);

  if (inventory.total === 0) {
    return;
  }

  const recommendedTypes: string[] = (plan.nodes ?? [])
    .map((node) => node.nodeType)
    .filter((nodeType) => typeof nodeType === "string" && nodeType.length > 0);

  if (requestedNodeType && requestedNodeType.trim().length > 0) {
    recommendedTypes.push(requestedNodeType);
  }

  const missingTypes = Array.from(new Set(recommendedTypes)).filter(
    (nodeType) => !matchesObservedNodeType(nodeType, inventory.nodeTypes)
  );

  if (missingTypes.length > 0) {
    plan.warnings.push(
      `The following node types were not observed in current workspace nodes: ${missingTypes.join(
        ", "
      )}. This observation is based on existing nodes, not a true installed-type registry. Confirm installation in Coalesce before creating nodes of these types.`
    );
    plan.status = "needs_clarification";
  }
}

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
