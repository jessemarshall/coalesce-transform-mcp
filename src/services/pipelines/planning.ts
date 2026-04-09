// Re-exports for backward compatibility
export { PipelinePlanSchema, DEFAULT_STAGE_CONFIG } from "./planning-types.js";
export type { PlannedSelectItemKind, PlannedSelectItem, PlannedPipelineNode, ResolvedSqlRef, ParsedSqlSourceRef, WorkspaceNodeTypeInventory } from "./planning-types.js";

export { normalizeSqlIdentifier, deepClone, normalizeWhitespace, buildSourceDependencyKey, getUniqueSourceDependencies, parseSqlSourceRefs, parseSqlSelectItems, extractCtes, escapeRegExp } from "./sql-parsing.js";
export type { ParsedCte, CteColumn } from "./sql-parsing.js";

export { getColumnNamesFromNode, getNodeColumnArray, getColumnSourceNodeIDs, findMatchingBaseColumn, renameSourceMappingEntries, buildStageSourceMappingFromPlan } from "./column-helpers.js";

export { getWorkspaceNodeTypeInventory } from "./workspace-resolution.js";

// --- planPipeline implementation ---

import type { CoalesceClient } from "../../client.js";
import type { PipelinePlan } from "./planning-types.js";
import {
  extractCtes,
  parseSqlSourceRefs,
  parseSqlSelectItems,
  buildCtePlan,
  deepClone,
} from "./sql-parsing.js";
import {
  resolveSqlRefsToWorkspaceNodes,
  getSourceNodesByID,
  buildSelectItemsFromSourceNode,
  buildDefaultNodeName,
  buildDefaultNodePrefix,
  buildPlanFromSql,
  applyWorkspaceNodeTypeValidation,
  getWorkspaceNodeTypeInventory as getInventory,
} from "./workspace-resolution.js";
import { selectPipelineNodeType } from "./node-type-selection.js";
import { uniqueInOrder } from "../../utils.js";

export async function planPipeline(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    goal?: string;
    sql?: string;
    targetName?: string;
    targetNodeType?: string;
    description?: string;
    configOverrides?: Record<string, unknown>;
    locationName?: string;
    database?: string;
    schema?: string;
    sourceNodeIDs?: string[];
    repoPath?: string;
  }
): Promise<PipelinePlan> {
  const location = {
    ...(params.locationName ? { locationName: params.locationName } : {}),
    ...(params.database ? { database: params.database } : {}),
    ...(params.schema ? { schema: params.schema } : {}),
  };
  const workspaceNodeTypeInventory = await getInventory(
    client,
    params.workspaceID
  );

  if (params.sql && params.sql.trim().length > 0) {
    // Detect CTEs — Coalesce does not support CTEs. Each CTE should be a separate node.
    const cteResult = extractCtes(params.sql);
    const ctes = cteResult.ctes;
    if (ctes.length > 0) {
      // Evaluate each layer pattern independently.
      // Goals explicitly mention "batch ETL CTE decomposition" so that specialized
      // patterns (Dynamic Tables, Incremental, etc.) are properly excluded by the scorer.
      const sharedContext = {
        workspaceNodeTypes: workspaceNodeTypeInventory.nodeTypes,
        workspaceNodeTypeCounts: workspaceNodeTypeInventory.counts,
        repoPath: params.repoPath,
      };
      const userGoal = params.goal ? ` for ${params.goal}` : "";
      const stagingSelection = selectPipelineNodeType({
        ...sharedContext,
        explicitNodeType: params.targetNodeType,
        goal: `batch ETL CTE decomposition — staging layer${userGoal}. Use Stage or Work node type.`,
        sourceCount: 1,
        hasJoin: false,
        hasGroupBy: false,
      });
      const multiSourceSelection = selectPipelineNodeType({
        ...sharedContext,
        explicitNodeType: params.targetNodeType,
        goal: `batch ETL CTE decomposition — join transform${userGoal}. Use Stage, Work, or View node type.`,
        sourceCount: 3,
        hasJoin: true,
        hasGroupBy: false,
      });
      const aggregationSelection = selectPipelineNodeType({
        ...sharedContext,
        explicitNodeType: params.targetNodeType,
        goal: `batch ETL CTE decomposition — aggregation transform${userGoal}. Use Stage or Work node type.`,
        sourceCount: 1,
        hasJoin: false,
        hasGroupBy: true,
      });
      const ctePlan = buildCtePlan(params, ctes, {
        staging: stagingSelection.selection,
        multiSource: multiSourceSelection.selection,
        aggregation: aggregationSelection.selection,
      });
      applyWorkspaceNodeTypeValidation(
        ctePlan,
        workspaceNodeTypeInventory,
        params.targetNodeType
      );
      return ctePlan;
    }

    const sourceParseResult = parseSqlSourceRefs(params.sql);
    const parseResult = parseSqlSelectItems(params.sql, sourceParseResult.refs);
    const {
      refs,
      predecessorNodes,
      openQuestions,
      warnings,
    } = await resolveSqlRefsToWorkspaceNodes(
      client,
      params.workspaceID,
      parseResult.refs
    );
    const selectionResult = selectPipelineNodeType({
      explicitNodeType: params.targetNodeType,
      goal: params.goal,
      targetName: params.targetName,
      sql: params.sql,
      sourceCount: refs.length,
      workspaceNodeTypes: workspaceNodeTypeInventory.nodeTypes,
      workspaceNodeTypeCounts: workspaceNodeTypeInventory.counts,
      repoPath: params.repoPath,
    });
    const plan = buildPlanFromSql(
      {
        workspaceID: params.workspaceID,
        goal: params.goal,
        sql: params.sql,
        targetName: params.targetName,
        description: params.description,
        targetNodeType: params.targetNodeType,
        configOverrides: params.configOverrides,
        nodeTypeSelection: selectionResult.selection,
        selectedNodeType: selectionResult.selectedCandidate,
        location,
      },
      { ...parseResult, refs },
      predecessorNodes,
      openQuestions,
      [...warnings, ...selectionResult.warnings]
    );
    applyWorkspaceNodeTypeValidation(
      plan,
      workspaceNodeTypeInventory,
      params.targetNodeType
    );

    return plan;
  }

  if (params.sourceNodeIDs && params.sourceNodeIDs.length > 0) {
    const {
      sourceRefs,
      predecessorNodes,
      openQuestions,
      warnings,
    } = await getSourceNodesByID(client, params.workspaceID, params.sourceNodeIDs);
    const multiSource = sourceRefs.length > 1;
    const singleSource = sourceRefs.length === 1;
    const selectionResult = selectPipelineNodeType({
      explicitNodeType: params.targetNodeType,
      goal: params.goal,
      targetName: params.targetName,
      sourceCount: sourceRefs.length,
      workspaceNodeTypes: workspaceNodeTypeInventory.nodeTypes,
      workspaceNodeTypeCounts: workspaceNodeTypeInventory.counts,
      repoPath: params.repoPath,
    });
    const selectedNodeType =
      selectionResult.selectedCandidate?.nodeType ??
      params.targetNodeType ??
      "Stage";

    if (singleSource) {
      const sourceRef = sourceRefs[0]!;
      const predecessor = predecessorNodes[sourceRef.nodeID!];
      const selectItems = buildSelectItemsFromSourceNode(
        sourceRef.nodeID!,
        sourceRef.alias ?? sourceRef.nodeName,
        predecessor
      );
      const ready =
        (selectionResult.selectedCandidate?.autoExecutable ?? true) &&
        openQuestions.length === 0 &&
        selectItems.length > 0;
      const planWarnings = [...warnings, ...selectionResult.warnings];
      const planOpenQuestions = [...openQuestions];
      if (selectionResult.selectedCandidate && !selectionResult.selectedCandidate.autoExecutable) {
        planWarnings.push(
          `Planner selected node type ${selectedNodeType}, but it likely needs additional semantic configuration before automatic creation.`
        );
        if (selectionResult.selectedCandidate.semanticSignals.length > 0) {
          planOpenQuestions.push(
            `Confirm the required configuration for ${selectedNodeType}: ${selectionResult.selectedCandidate.semanticSignals.join(
              ", "
            )}.`
          );
        }
        if (selectionResult.selectedCandidate.missingDefaultFields.length > 0) {
          planOpenQuestions.push(
            `Provide values for ${selectedNodeType} config fields without defaults: ${selectionResult.selectedCandidate.missingDefaultFields.join(
              ", "
            )}.`
          );
        }
      }

      const plan: PipelinePlan = {
        version: 1,
        intent: "goal",
        status: ready ? "ready" : "needs_clarification",
        workspaceID: params.workspaceID,
        platform: null,
        goal: params.goal ?? null,
        sql: null,
        nodes: [
          {
            planNodeID: "node-1",
            name: buildDefaultNodeName(params.targetName, [
              {
                locationName: sourceRef.locationName,
                nodeName: sourceRef.nodeName,
                alias: sourceRef.alias,
                nodeID: sourceRef.nodeID,
              },
            ], selectionResult.selectedCandidate?.family ?? null, selectionResult.selectedCandidate?.shortName ?? null),
            nodeType: selectedNodeType,
            nodeTypeFamily: selectionResult.selectedCandidate?.family ?? null,
            predecessorNodeIDs: [sourceRef.nodeID!],
            predecessorPlanNodeIDs: [],
            predecessorNodeNames: [sourceRef.nodeName],
            description: params.description ?? null,
            sql: null,
            selectItems,
            outputColumnNames: selectItems.flatMap((item) =>
              item.outputName ? [item.outputName] : []
            ),
            configOverrides: params.configOverrides
              ? deepClone(params.configOverrides)
              : {},
            sourceRefs,
            joinCondition: `FROM {{ ref('${sourceRef.locationName}', '${sourceRef.nodeName}') }} "${sourceRef.alias ?? sourceRef.nodeName}"`,
            location,
            requiresFullSetNode: true,
            ...(selectionResult.selectedCandidate?.templateDefaults
              ? { templateDefaults: selectionResult.selectedCandidate.templateDefaults }
              : {}),
          },
        ],
        assumptions: [
          `Planner ${selectionResult.selection.strategy} selected ${selectedNodeType} from repo/workspace candidates.`,
          "Goal-driven planning uses a pass-through projection from the supplied source node IDs when the selected type is projection-capable.",
          "Review the generated plan before execution if the goal implies filters, joins, or computed columns.",
        ],
        openQuestions: planOpenQuestions,
        warnings: planWarnings,
        supportedNodeTypes:
          selectionResult.selection.supportedNodeTypes.length > 0
            ? selectionResult.selection.supportedNodeTypes
            : [selectedNodeType],
        nodeTypeSelection: selectionResult.selection,
      };
      applyWorkspaceNodeTypeValidation(
        plan,
        workspaceNodeTypeInventory,
        params.targetNodeType
      );

      return plan;
    }

    const multiSourceWarnings = [...warnings, ...selectionResult.warnings];
    const multiSourceOpenQuestions = [
      ...openQuestions,
      ...(multiSource
        ? [
            `How should these sources be joined or filtered: ${sourceRefs
              .map((ref) => ref.nodeName)
              .join(", ")}?`,
          ]
        : []),
    ];
    if (selectionResult.selectedCandidate && !selectionResult.selectedCandidate.autoExecutable) {
      multiSourceWarnings.push(
        `Planner selected node type ${selectedNodeType}, but it likely needs additional semantic configuration before automatic creation.`
      );
      if (selectionResult.selectedCandidate.semanticSignals.length > 0) {
        multiSourceOpenQuestions.push(
          `Confirm the required configuration for ${selectedNodeType}: ${selectionResult.selectedCandidate.semanticSignals.join(
            ", "
          )}.`
        );
      }
    }

    const plan: PipelinePlan = {
      version: 1,
      intent: "goal",
      status: "needs_clarification",
      workspaceID: params.workspaceID,
      platform: null,
      goal: params.goal ?? null,
      sql: null,
      nodes: [
        {
          planNodeID: "node-1",
          name:
            params.targetName ??
            `${buildDefaultNodePrefix(
              selectionResult.selectedCandidate?.family ?? null,
              selectionResult.selectedCandidate?.shortName ?? null
            )}_MULTI_SOURCE`,
          nodeType: selectedNodeType,
          nodeTypeFamily: selectionResult.selectedCandidate?.family ?? null,
          predecessorNodeIDs: uniqueInOrder(sourceRefs.flatMap((ref) =>
            ref.nodeID ? [ref.nodeID] : []
          )),
          predecessorPlanNodeIDs: [],
          predecessorNodeNames: sourceRefs.map((ref) => ref.nodeName),
          description: params.description ?? null,
          sql: null,
          selectItems: [],
          outputColumnNames: [],
          configOverrides: params.configOverrides
            ? deepClone(params.configOverrides)
            : {},
          sourceRefs,
          joinCondition: null,
          location,
          requiresFullSetNode: true,
          ...(selectionResult.selectedCandidate?.templateDefaults
            ? { templateDefaults: selectionResult.selectedCandidate.templateDefaults }
            : {}),
        },
      ],
      assumptions: [
        `Planner ${selectionResult.selection.strategy} selected ${selectedNodeType} from repo/workspace candidates.`,
        "Goal-based planning can scaffold a multisource request, but it does not infer joins automatically.",
      ],
      openQuestions: multiSourceOpenQuestions,
      warnings: multiSourceWarnings,
      supportedNodeTypes:
        selectionResult.selection.supportedNodeTypes.length > 0
          ? selectionResult.selection.supportedNodeTypes
          : [selectedNodeType],
      nodeTypeSelection: selectionResult.selection,
    };
    applyWorkspaceNodeTypeValidation(
      plan,
      workspaceNodeTypeInventory,
      params.targetNodeType
    );

    return plan;
  }

  const openQuestions: string[] = [];
  if (!params.goal || params.goal.trim().length === 0) {
    openQuestions.push("What pipeline should be built, and what should it produce?");
  }
  if (!params.sourceNodeIDs || params.sourceNodeIDs.length === 0) {
    openQuestions.push("Which upstream Coalesce node IDs should this pipeline build from?");
  }
  const selectionResult = selectPipelineNodeType({
    explicitNodeType: params.targetNodeType,
    goal: params.goal,
    targetName: params.targetName,
    sourceCount: 0,
    workspaceNodeTypes: workspaceNodeTypeInventory.nodeTypes,
    workspaceNodeTypeCounts: workspaceNodeTypeInventory.counts,
    repoPath: params.repoPath,
  });

  const plan: PipelinePlan = {
    version: 1,
    intent: "goal",
    status: "needs_clarification",
    workspaceID: params.workspaceID,
    platform: null,
    goal: params.goal ?? null,
    sql: null,
    nodes: [],
    assumptions: [
      selectionResult.selectedCandidate
        ? `Planner ${selectionResult.selection.strategy} would prefer ${selectionResult.selectedCandidate.nodeType} for this goal once sources are confirmed.`
        : "Planner could not rank a preferred node type because no repo-backed or observed workspace candidates were available.",
      "Goal-only planning currently returns clarification questions rather than inferred node graphs.",
    ],
    openQuestions,
    warnings: [...selectionResult.warnings],
    supportedNodeTypes:
      selectionResult.selection.supportedNodeTypes.length > 0
        ? selectionResult.selection.supportedNodeTypes
        : selectionResult.selectedCandidate
          ? [selectionResult.selectedCandidate.nodeType]
          : ["Stage"],
    nodeTypeSelection: selectionResult.selection,
  };
  applyWorkspaceNodeTypeValidation(
    plan,
    workspaceNodeTypeInventory,
    params.targetNodeType
  );

  return plan;
}
