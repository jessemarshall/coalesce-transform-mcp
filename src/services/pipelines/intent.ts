import { type CoalesceClient } from "../../client.js";
import { getCachedOrFetchWorkspaceNodeDetail } from "../cache/workspace-node-detail-index.js";
import { isPlainObject, uniqueInOrder, rethrowNonRecoverableApiError } from "../../utils.js";
import {
  normalizeSqlIdentifier,
  getColumnNamesFromNode,
  type PlannedPipelineNode,
} from "./planning.js";
import {
  selectPipelineNodeType,
  type PipelineNodeTypeFamily,
  type PipelineNodeTypeSelection,
} from "./node-type-selection.js";
import { getWorkspaceNodeTypeInventory } from "./workspace-resolution.js";

// Re-export from extracted modules for external consumers
export { parseIntent, type ParsedIntent } from "./intent-parsing.js";
export { resolveIntentEntities, type ResolvedEntity } from "./intent-resolution.js";

import { parseIntent, type ParsedIntent } from "./intent-parsing.js";
import { resolveIntentEntities, type ResolvedEntity } from "./intent-resolution.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type IntentPipelineResult = {
  status: "ready" | "needs_clarification" | "needs_entity_resolution";
  intent: ParsedIntent;
  resolvedEntities: ResolvedEntity[];
  plan: Record<string, unknown> | null;
  openQuestions: string[];
  warnings: string[];
};

// ---------------------------------------------------------------------------
// Plan assembly helpers
// ---------------------------------------------------------------------------

function buildSelectItemsForPassthrough(
  sourceNodeID: string,
  sourceNodeName: string,
  node: Record<string, unknown>
): PlannedPipelineNode["selectItems"] {
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

function buildNodePrefix(
  family: PipelineNodeTypeFamily | null | undefined,
  shortName: string | null | undefined
): string {
  if (shortName && shortName.trim().length > 0) {
    return shortName.trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_");
  }
  switch (family) {
    case "stage": return "STG";
    case "persistent-stage": return "PSTG";
    case "view": return "VW";
    case "work": return "WRK";
    case "dimension": return "DIM";
    case "fact": return "FACT";
    case "hub": return "HUB";
    case "satellite": return "SAT";
    case "link": return "LNK";
    default: return "NODE";
  }
}

function stripNodePrefix(name: string): string {
  return name.replace(
    /^(SRC[_-]?|STG[_-]?|DIM[_-]?|FACT[_-]?|FCT[_-]?|INT[_-]?|WORK[_-]?|WRK[_-]?|VW[_-]?|RAW[_-]?)/i,
    ""
  );
}

// ---------------------------------------------------------------------------
// Plan assembly — combine parsed intent + resolved entities into PipelinePlan
// ---------------------------------------------------------------------------

export async function buildPipelinePlanFromIntent(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    intent: string;
    targetName?: string;
    targetNodeType?: string;
    repoPath?: string;
    locationName?: string;
    database?: string;
    schema?: string;
  }
): Promise<IntentPipelineResult> {
  const parsed = parseIntent(params.intent);

  // Collect all entity names across steps
  const allEntityNames = uniqueInOrder(
    parsed.steps.flatMap((step) => step.entityNames)
  );

  // Resolve entities to workspace nodes
  const resolvedEntities = await resolveIntentEntities(
    client,
    params.workspaceID,
    allEntityNames
  );

  // Check for unresolved entities
  const unresolvedEntities = resolvedEntities.filter(
    (e) => e.confidence === "unresolved"
  );

  const openQuestions = [...parsed.openQuestions];
  const warnings = [...parsed.warnings];

  for (const unresolved of unresolvedEntities) {
    if (unresolved.candidates.length > 0) {
      openQuestions.push(
        `"${unresolved.rawName}" matched multiple workspace nodes: ${unresolved.candidates
          .map((c) => `${c.name} (${c.locationName ?? "no location"})`)
          .join(", ")}. Which one should be used?`
      );
    } else {
      openQuestions.push(
        `Could not find a workspace node matching "${unresolved.rawName}". ` +
        `Use list_workspace_nodes to check available nodes.`
      );
    }
  }

  if (unresolvedEntities.length > 0) {
    return {
      status: "needs_entity_resolution",
      intent: parsed,
      resolvedEntities,
      plan: null,
      openQuestions,
      warnings,
    };
  }

  if (parsed.steps.length === 0) {
    return {
      status: "needs_clarification",
      intent: parsed,
      resolvedEntities,
      plan: null,
      openQuestions,
      warnings,
    };
  }

  // Build an entity lookup for resolved nodes
  const entityLookup = new Map<string, ResolvedEntity>();
  for (const entity of resolvedEntities) {
    entityLookup.set(entity.rawName.toUpperCase(), entity);
  }

  // Get workspace node type inventory for node type selection
  const inventory = await getWorkspaceNodeTypeInventory(client, params.workspaceID);
  warnings.push(...inventory.warnings);

  // If inventory fetch failed (empty + warnings), degrade the plan status
  const inventoryDegraded = inventory.nodeTypes.length === 0 && inventory.warnings.length > 0;

  const location = {
    ...(params.locationName ? { locationName: params.locationName } : {}),
    ...(params.database ? { database: params.database } : {}),
    ...(params.schema ? { schema: params.schema } : {}),
  };

  const planNodes: PlannedPipelineNode[] = [];
  const predecessorNodesByPlanID = new Map<string, Record<string, unknown>>();
  let nodeCounter = 0;

  for (const step of parsed.steps) {
    nodeCounter += 1;
    const planNodeID = `intent-node-${nodeCounter}`;

    // Resolve the step's entities to their workspace nodes
    const stepEntities = step.entityNames
      .map((name) => entityLookup.get(name.toUpperCase()))
      .filter((e): e is ResolvedEntity & { confidence: "exact" | "fuzzy" } => e !== undefined && e.confidence !== "unresolved");

    // If this step has no entities, it references the previous step's output
    const referencesPreviousStep = stepEntities.length === 0 && planNodes.length > 0;

    // Select node type for this step
    const selectionContext = {
      explicitNodeType: params.targetNodeType,
      goal: `${step.operation} pipeline step: ${params.intent}`,
      sourceCount: referencesPreviousStep ? 1 : stepEntities.length,
      workspaceNodeTypes: inventory.nodeTypes,
      workspaceNodeTypeCounts: inventory.counts,
      repoPath: params.repoPath,
      hasJoin: step.operation === "join",
      hasGroupBy: step.operation === "aggregate",
    };

    const selectionResult = selectPipelineNodeType(selectionContext);
    const selectedNodeType =
      selectionResult.selectedCandidate?.nodeType ??
      params.targetNodeType ??
      "Stage";
    const selectedFamily = selectionResult.selectedCandidate?.family ?? null;

    // Fetch predecessor node details for selectItems
    const predecessorNodeIDs: string[] = [];
    const predecessorNodeNames: string[] = [];
    const sourceRefs: PlannedPipelineNode["sourceRefs"] = [];
    let selectItems: PlannedPipelineNode["selectItems"] = [];

    if (!referencesPreviousStep) {
      // Wire to workspace nodes
      for (const entity of stepEntities) {
        predecessorNodeIDs.push(entity.resolvedNodeID);
        predecessorNodeNames.push(entity.resolvedNodeName);

        const locationName = entity.resolvedLocationName;
        if (!locationName) {
          warnings.push(
            `Node "${entity.resolvedNodeName}" has no location name. ` +
            `Specify a locationName parameter or ensure the source node has a location assigned.`
          );
        }

        if (!locationName) {
          openQuestions.push(
            `Node "${entity.resolvedNodeName}" has no location assigned. ` +
            `Specify a locationName or assign a storage location to the source node before building the pipeline.`
          );
        }

        sourceRefs.push({
          locationName: locationName ?? "__MISSING_LOCATION__",
          nodeName: entity.resolvedNodeName,
          alias: entity.resolvedNodeName,
          nodeID: entity.resolvedNodeID,
        });

        // Fetch the predecessor node for column passthrough
        if (!predecessorNodesByPlanID.has(entity.resolvedNodeID)) {
          try {
            const node = await getCachedOrFetchWorkspaceNodeDetail(
              client,
              params.workspaceID,
              entity.resolvedNodeID
            );
            if (isPlainObject(node)) {
              predecessorNodesByPlanID.set(entity.resolvedNodeID, node);
            }
          } catch (error) {
            rethrowNonRecoverableApiError(error);
            const reason = error instanceof Error ? error.message : String(error);
            warnings.push(
              `Could not fetch predecessor node "${entity.resolvedNodeName}" (${entity.resolvedNodeID}) — ${reason}. ` +
              `Column passthrough will not be available for this source.`
            );
          }
        }

        const predecessorNode = predecessorNodesByPlanID.get(entity.resolvedNodeID);
        if (predecessorNode) {
          const items = buildSelectItemsForPassthrough(
            entity.resolvedNodeID,
            entity.resolvedNodeName,
            predecessorNode
          );
          selectItems.push(...items);
        }
      }
    }

    // Build node name
    const prefix = buildNodePrefix(selectedFamily, selectionResult.selectedCandidate?.shortName ?? null);
    let nodeName: string;
    if (step.targetName) {
      nodeName = step.targetName;
    } else if (params.targetName && parsed.steps.length === 1) {
      nodeName = params.targetName;
    } else if (stepEntities.length === 1) {
      nodeName = `${prefix}_${stripNodePrefix(stepEntities[0]!.resolvedNodeName)}`.toUpperCase();
    } else if (stepEntities.length > 1) {
      nodeName = `${prefix}_${stepEntities.map((e) => stripNodePrefix(e.resolvedNodeName)).join("_")}`.toUpperCase();
    } else {
      nodeName = `${prefix}_${step.operation.toUpperCase()}_${nodeCounter}`;
    }
    // Clean up double underscores
    nodeName = nodeName.replace(/__+/g, "_");

    // Build join condition for join steps
    let joinCondition: string | null = null;
    if (step.operation === "join" && sourceRefs.length >= 2) {
      const fromRef = sourceRefs[0]!;
      const joinParts = [`FROM {{ ref('${fromRef.locationName}', '${fromRef.nodeName}') }} "${fromRef.alias ?? fromRef.nodeName}"`];
      for (const ref of sourceRefs.slice(1)) {
        const joinType = step.joinType ?? "INNER";
        joinParts.push(
          `${joinType} JOIN {{ ref('${ref.locationName}', '${ref.nodeName}') }} "${ref.alias ?? ref.nodeName}"`
        );
        if (step.joinKey) {
          joinParts.push(
            `  ON "${fromRef.alias ?? fromRef.nodeName}"."${step.joinKey}" = "${ref.alias ?? ref.nodeName}"."${step.joinKey}"`
          );
        }
      }
      joinCondition = joinParts.join("\n");
    } else if (sourceRefs.length === 1) {
      const ref = sourceRefs[0]!;
      joinCondition = `FROM {{ ref('${ref.locationName}', '${ref.nodeName}') }} "${ref.alias ?? ref.nodeName}"`;
    }

    const predecessorPlanNodeIDs = referencesPreviousStep
      ? [planNodes[planNodes.length - 1]!.planNodeID]
      : [];

    planNodes.push({
      planNodeID,
      name: nodeName,
      nodeType: selectedNodeType,
      nodeTypeFamily: selectedFamily,
      predecessorNodeIDs,
      predecessorPlanNodeIDs,
      predecessorNodeNames,
      description: `Auto-generated from intent: ${params.intent}`,
      sql: null,
      selectItems,
      outputColumnNames: selectItems.flatMap((item) =>
        item.outputName ? [item.outputName] : []
      ),
      configOverrides: {},
      sourceRefs,
      joinCondition,
      location,
      requiresFullSetNode: true,
      ...(selectionResult.selectedCandidate?.templateDefaults
        ? { templateDefaults: selectionResult.selectedCandidate.templateDefaults }
        : {}),
    });
  }

  // Check if the plan is ready
  const hasOpenQuestions = openQuestions.length > 0;
  const allNodesHavePredecessors = planNodes.every(
    (node) => node.predecessorNodeIDs.length > 0 || node.predecessorPlanNodeIDs.length > 0
  );

  const plan = {
    version: 1 as const,
    intent: "goal" as const,
    status: (!hasOpenQuestions && allNodesHavePredecessors && !inventoryDegraded ? "ready" : "needs_clarification") as "ready" | "needs_clarification",
    workspaceID: params.workspaceID,
    platform: null,
    goal: params.intent,
    sql: null,
    nodes: planNodes,
    assumptions: [
      "Pipeline built from natural language intent. Review node names, types, and column mappings before creating.",
      `Intent parsed ${parsed.steps.length} operation(s): ${parsed.steps.map((s) => s.operation).join(" → ")}.`,
    ],
    openQuestions,
    warnings,
    supportedNodeTypes: [planNodes[0]?.nodeType ?? "Stage"],
    nodeTypeSelection: undefined as PipelineNodeTypeSelection | undefined,
  };

  // Get the last selection result for the plan summary
  const finalSelectionContext = {
    explicitNodeType: params.targetNodeType,
    goal: params.intent,
    sourceCount: allEntityNames.length,
    workspaceNodeTypes: inventory.nodeTypes,
    workspaceNodeTypeCounts: inventory.counts,
    repoPath: params.repoPath,
  };
  const finalSelection = selectPipelineNodeType(finalSelectionContext);
  plan.supportedNodeTypes = finalSelection.selection.supportedNodeTypes.length > 0
    ? finalSelection.selection.supportedNodeTypes
    : [planNodes[0]?.nodeType ?? "Stage"];
  plan.nodeTypeSelection = finalSelection.selection;

  return {
    status: plan.status === "ready" ? "ready" : "needs_clarification",
    intent: parsed,
    resolvedEntities,
    plan,
    openQuestions,
    warnings,
  };
}
