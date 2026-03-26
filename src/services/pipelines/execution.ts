import { randomUUID } from "node:crypto";
import { CoalesceApiError, type CoalesceClient } from "../../client.js";
import { validatePathSegment } from "../../coalesce/types.js";
import { buildPlanConfirmationToken } from "./confirmation.js";
import {
  PipelinePlanSchema,
  planPipeline,
  type PlannedPipelineNode,
  DEFAULT_STAGE_CONFIG,
  buildSourceDependencyKey,
  deepClone,
  findMatchingBaseColumn,
  buildStageSourceMappingFromPlan,
  getUniqueSourceDependencies,
  renameSourceMappingEntries,
  getColumnNamesFromNode,
  normalizeSqlIdentifier,
  normalizeWhitespace,
  getNodeColumnArray,
  getColumnSourceNodeIDs,
} from "./planning.js";
import {
  getWorkspaceNode,
  setWorkspaceNode,
} from "../../coalesce/api/nodes.js";
import { createWorkspaceNodeFromPredecessor, buildUpdatedWorkspaceNodeBody } from "../workspace/mutations.js";
import { isPlainObject, uniqueInOrder } from "../../utils.js";

function isStageLikeNode(nodePlan: PlannedPipelineNode): boolean {
  return (
    nodePlan.nodeTypeFamily === "stage" ||
    nodePlan.nodeTypeFamily === "persistent-stage" ||
    /(?:^|:::)(?:persistent)?Stage$/u.test(nodePlan.nodeType)
  );
}

function buildNodeBodyFromPlan(
  currentNode: Record<string, unknown>,
  nodePlan: PlannedPipelineNode
): Record<string, unknown> {
  const updatedNode = deepClone(currentNode);
  updatedNode.name = nodePlan.name;
  if (nodePlan.description !== null) {
    updatedNode.description = nodePlan.description;
  }

  if (Object.keys(nodePlan.location).length > 0) {
    Object.assign(updatedNode, nodePlan.location);
  }

  const templateDefaults = isPlainObject(nodePlan.templateDefaults)
    ? nodePlan.templateDefaults
    : undefined;
  const inferredTopLevelFields =
    templateDefaults && isPlainObject(templateDefaults.inferredTopLevelFields)
      ? templateDefaults.inferredTopLevelFields
      : {};
  for (const [key, value] of Object.entries(inferredTopLevelFields)) {
    if (updatedNode[key] === undefined) {
      updatedNode[key] = deepClone(value);
    }
  }

  updatedNode.config = {
    ...(isStageLikeNode(nodePlan) ? DEFAULT_STAGE_CONFIG : {}),
    ...(templateDefaults && isPlainObject(templateDefaults.inferredConfig)
      ? deepClone(templateDefaults.inferredConfig)
      : {}),
    ...(isPlainObject(updatedNode.config) ? updatedNode.config : {}),
    ...nodePlan.configOverrides,
  };

  const plannedColumns: Record<string, unknown>[] = [];
  for (const selectItem of nodePlan.selectItems) {
    const baseColumn = findMatchingBaseColumn(updatedNode, selectItem);
    if (!baseColumn) {
      throw new Error(
        `Could not map planned output column ${selectItem.outputName ?? selectItem.expression} onto the created predecessor-based node body.`
      );
    }
    baseColumn.name = selectItem.outputName ?? baseColumn.name;
    if (isPlainObject(baseColumn.columnReference)) {
      baseColumn.columnReference = {
        ...baseColumn.columnReference,
        columnCounter: randomUUID(),
      };
    }
    if (typeof baseColumn.columnID === "string") {
      baseColumn.columnID = randomUUID();
    }
    plannedColumns.push(baseColumn);
  }

  const currentMetadata = isPlainObject(updatedNode.metadata)
    ? updatedNode.metadata
    : {};
  updatedNode.metadata = {
    ...currentMetadata,
    columns: plannedColumns,
    sourceMapping: buildStageSourceMappingFromPlan(updatedNode, nodePlan),
  };

  return renameSourceMappingEntries(updatedNode, nodePlan.name);
}

function getSavedNodeColumnNames(node: Record<string, unknown>): string[] {
  return getColumnNamesFromNode(node);
}

function validateSavedNode(
  node: Record<string, unknown>,
  nodePlan: PlannedPipelineNode
) {
  const savedColumnNames = getSavedNodeColumnNames(node);
  const expectedColumnNames = nodePlan.outputColumnNames;
  const normalizedSaved = savedColumnNames.map(normalizeSqlIdentifier);
  const normalizedExpected = expectedColumnNames.map(normalizeSqlIdentifier);
  const referencedPredecessorNodeIDs = new Set<string>();
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  const sourceMappingEntry =
    metadata && Array.isArray(metadata.sourceMapping)
      ? metadata.sourceMapping.find(isPlainObject)
      : undefined;
  const savedDependencies =
    isPlainObject(sourceMappingEntry) && Array.isArray(sourceMappingEntry.dependencies)
      ? sourceMappingEntry.dependencies
          .filter(isPlainObject)
          .flatMap((dependency) => {
            if (typeof dependency.nodeName !== "string") {
              return [];
            }
            return [{
              locationName:
                typeof dependency.locationName === "string" ? dependency.locationName : null,
              nodeName: dependency.nodeName,
            }];
          })
      : [];
  const expectedDependencies = getUniqueSourceDependencies(nodePlan.sourceRefs);
  const actualDependencyKeys = uniqueInOrder(
    savedDependencies.map((dependency) =>
      buildSourceDependencyKey(dependency.locationName, dependency.nodeName)
    )
  );
  const expectedDependencyKeys = expectedDependencies.map((dependency) =>
    buildSourceDependencyKey(dependency.locationName, dependency.nodeName)
  );
  const expectedPredecessorNodeIDs = uniqueInOrder(nodePlan.predecessorNodeIDs);
  const savedJoinCondition =
    isPlainObject(sourceMappingEntry) &&
    isPlainObject(sourceMappingEntry.join) &&
    typeof sourceMappingEntry.join.joinCondition === "string"
      ? normalizeWhitespace(sourceMappingEntry.join.joinCondition)
      : "";

  for (const column of getNodeColumnArray(node)) {
    for (const nodeID of getColumnSourceNodeIDs(column)) {
      referencedPredecessorNodeIDs.add(nodeID);
    }
  }

  return {
    nodeNameSatisfied: node.name === nodePlan.name,
    expectedColumnCount: expectedColumnNames.length,
    actualColumnCount: savedColumnNames.length,
    outputColumnsSatisfied:
      normalizedExpected.length === normalizedSaved.length &&
      normalizedExpected.every((name, index) => normalizedSaved[index] === name),
    expectedColumnNames,
    actualColumnNames: savedColumnNames,
    sourceMappingDependenciesSatisfied:
      actualDependencyKeys.length === expectedDependencyKeys.length &&
      expectedDependencyKeys.every((key) => actualDependencyKeys.includes(key)),
    expectedDependencyNodeNames: expectedDependencies.map((dependency) => dependency.nodeName),
    actualDependencyNodeNames: uniqueInOrder(
      savedDependencies.map((dependency) => dependency.nodeName)
    ),
    joinConditionSatisfied:
      (nodePlan.joinCondition === null && savedJoinCondition.length === 0) ||
      savedJoinCondition === normalizeWhitespace(nodePlan.joinCondition ?? ""),
    expectedJoinCondition: nodePlan.joinCondition,
    actualJoinCondition:
      savedJoinCondition.length > 0 ? savedJoinCondition : null,
    predecessorCoverageSatisfied: expectedPredecessorNodeIDs.every((nodeID) =>
      referencedPredecessorNodeIDs.has(nodeID)
    ),
    predecessorNodeIDs: expectedPredecessorNodeIDs,
    referencedPredecessorNodeIDs: Array.from(referencedPredecessorNodeIDs),
  };
}

async function deleteWorkspaceNode(
  client: CoalesceClient,
  workspaceID: string,
  nodeID: string
): Promise<void> {
  await client.delete(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes/${validatePathSegment(nodeID, "nodeID")}`
  );
}

type SerializedPipelineError = {
  message: string;
  status?: number;
  detail?: unknown;
};

type RollbackCleanupFailure = SerializedPipelineError & {
  nodeID: string;
};

function serializePipelineExecutionError(error: unknown): SerializedPipelineError {
  if (error instanceof CoalesceApiError) {
    return {
      message: error.message,
      status: error.status,
      ...(error.detail !== undefined ? { detail: error.detail } : {}),
    };
  }
  if (error instanceof Error) {
    return { message: error.message };
  }
  return { message: "Pipeline creation failed", detail: error };
}

function buildSqlPipelineConfirmationResponse(
  plan: unknown,
  reason: "missing" | "mismatch"
): Record<string, unknown> {
  const confirmationToken = buildPlanConfirmationToken(plan);

  return {
    created: false,
    confirmationToken,
    STOP_AND_CONFIRM:
      reason === "mismatch"
        ? "STOP. The confirmationToken is missing or does not match the current plan. Present the pipeline summary to the user in a table format and ask for confirmation BEFORE creating any nodes. For EACH node, display: name, the EXACT nodeType string (e.g. 'Coalesce-Base-Node-Types:::Stage'), transforms, and filters. Use the cteNodeSummary or nodes array — do NOT paraphrase or simplify the nodeType values. Do NOT proceed until the user explicitly approves. Once the user approves, call createPipelineFromSql again with confirmed=true and the confirmationToken from this response."
        : "STOP. Present the pipeline summary to the user in a table format and ask for confirmation BEFORE creating any nodes. For EACH node, display: name, the EXACT nodeType string (e.g. 'Coalesce-Base-Node-Types:::Stage'), transforms, and filters. Use the cteNodeSummary or nodes array — do NOT paraphrase or simplify the nodeType values. Do NOT proceed until the user explicitly approves. Once the user approves, call createPipelineFromSql again with confirmed=true and the confirmationToken from this response.",
    plan,
  };
}

async function rollbackCreatedPipelineNodes(
  client: CoalesceClient,
  workspaceID: string,
  nodeIDs: string[]
): Promise<RollbackCleanupFailure[]> {
  const rollbackFailures: RollbackCleanupFailure[] = [];
  const uniqueNodeIDs = Array.from(new Set(nodeIDs));

  for (const nodeID of uniqueNodeIDs.reverse()) {
    try {
      await deleteWorkspaceNode(client, workspaceID, nodeID);
    } catch (error) {
      rollbackFailures.push({
        nodeID,
        ...serializePipelineExecutionError(error),
      });
    }
  }

  return rollbackFailures;
}

export async function createPipelineFromPlan(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    plan: Record<string, unknown>;
    dryRun?: boolean;
  }
): Promise<unknown> {
  const plan = PipelinePlanSchema.parse(params.plan);
  if (plan.workspaceID !== params.workspaceID) {
    throw new Error(
      `Pipeline plan workspaceID ${plan.workspaceID} does not match requested workspaceID ${params.workspaceID}.`
    );
  }
  if (plan.status !== "ready") {
    return {
      created: false,
      warning:
        "Pipeline plan still needs clarification. Review openQuestions and warnings before creation.",
      plan,
    };
  }
  if (params.dryRun) {
    return {
      created: false,
      dryRun: true,
      plan,
    };
  }

  const createdNodes: unknown[] = [];
  const createdNodeIDsByPlanNodeID = new Map<string, string>();
  const createdNodeIDsForRollback: string[] = [];

  for (const nodePlan of plan.nodes) {
    const predecessorNodeIDs = uniqueInOrder([
      ...nodePlan.predecessorNodeIDs,
      ...nodePlan.predecessorPlanNodeIDs.flatMap((planNodeID) => {
        const createdNodeID = createdNodeIDsByPlanNodeID.get(planNodeID);
        return createdNodeID ? [createdNodeID] : [];
      }),
    ]);

    if (predecessorNodeIDs.length === 0) {
      throw new Error(
        `Pipeline node ${nodePlan.planNodeID} has no resolved predecessor node IDs.`
      );
    }

    let createdNodeID: string | null = null;
    try {
      const created = await createWorkspaceNodeFromPredecessor(client, {
        workspaceID: params.workspaceID,
        nodeType: nodePlan.nodeType,
        predecessorNodeIDs,
      });

      if (!isPlainObject(created) || !isPlainObject(created.node)) {
        throw new Error(`Pipeline node ${nodePlan.planNodeID} did not return a created node body.`);
      }
      if (typeof created.node.id === "string") {
        createdNodeID = created.node.id;
        createdNodeIDsForRollback.push(createdNodeID);
      }
      if ("warning" in created && typeof created.warning === "string") {
        throw new Error(
          `Predecessor-based creation for ${nodePlan.name} did not confirm full auto-population: ${created.warning}`
        );
      }

      if (!createdNodeID) {
        throw new Error(`Created pipeline node ${nodePlan.planNodeID} did not return a node ID.`);
      }

      const plannedBody = buildNodeBodyFromPlan(created.node, {
        ...nodePlan,
        predecessorNodeIDs,
      });

      // Route through the shared validation path to ensure all API-required
      // fields (dataType, columnID, enabledColumnTestIDs, etc.) are present.
      const finalBody = buildUpdatedWorkspaceNodeBody(created.node, plannedBody);

      await setWorkspaceNode(client, {
        workspaceID: params.workspaceID,
        nodeID: createdNodeID,
        body: finalBody,
      });

      const savedNode = await getWorkspaceNode(client, {
        workspaceID: params.workspaceID,
        nodeID: createdNodeID,
      });
      if (!isPlainObject(savedNode)) {
        throw new Error(`Saved pipeline node ${nodePlan.name} did not return an object body.`);
      }

      const validation = validateSavedNode(savedNode, {
        ...nodePlan,
        predecessorNodeIDs,
      });
      if (
        !validation.nodeNameSatisfied ||
        !validation.outputColumnsSatisfied ||
        !validation.sourceMappingDependenciesSatisfied ||
        !validation.joinConditionSatisfied ||
        !validation.predecessorCoverageSatisfied
      ) {
        throw new Error(
          `Saved pipeline node ${nodePlan.name} did not match the planned body after set-workspace-node.`
        );
      }

      createdNodeIDsByPlanNodeID.set(nodePlan.planNodeID, createdNodeID);
      createdNodes.push({
        planNodeID: nodePlan.planNodeID,
        nodeID: createdNodeID,
        name: nodePlan.name,
        nodeType: nodePlan.nodeType,
        validation,
      });
    } catch (error) {
      const rollbackFailures = await rollbackCreatedPipelineNodes(
        client,
        params.workspaceID,
        createdNodeIDsForRollback
      );
      if (rollbackFailures.length > 0) {
        return {
          created: false,
          incomplete: true,
          failedPlanNodeID: nodePlan.planNodeID,
          createdNodes,
          cleanupFailedNodeIDs: rollbackFailures.map((failure) => failure.nodeID),
          cleanupFailures: rollbackFailures,
          warning:
            "Pipeline creation failed after nodes were created, and automatic cleanup did not fully succeed. Review the workspace manually before continuing.",
          error: serializePipelineExecutionError(error),
        };
      }
      throw error;
    }
  }

  return {
    created: true,
    workspaceID: params.workspaceID,
    nodeCount: createdNodes.length,
    createdNodes,
  };
}

export async function createPipelineFromSql(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    sql: string;
    goal?: string;
    targetName?: string;
    targetNodeType?: string;
    description?: string;
    configOverrides?: Record<string, unknown>;
    locationName?: string;
    database?: string;
    schema?: string;
    repoPath?: string;
    dryRun?: boolean;
    confirmed?: boolean;
    confirmationToken?: string;
  }
): Promise<unknown> {
  const plan = await planPipeline(client, params);
  if (plan.status !== "ready" || params.dryRun) {
    return {
      created: false,
      ...(params.dryRun ? { dryRun: true } : {}),
      plan,
      ...(plan.status !== "ready"
        ? {
            warning:
              "SQL was planned but still needs clarification before creation. Review openQuestions and warnings. Present the plan to the user and wait for approval.",
          }
        : {}),
    };
  }

  if (params.confirmed !== true) {
    return buildSqlPipelineConfirmationResponse(plan, "missing");
  }

  if (params.confirmationToken !== buildPlanConfirmationToken(plan)) {
    return buildSqlPipelineConfirmationResponse(plan, "mismatch");
  }

  const execution = await createPipelineFromPlan(client, {
    workspaceID: params.workspaceID,
    plan,
    dryRun: params.dryRun,
  });

  return {
    plan,
    ...((isPlainObject(execution) ? execution : { execution }) as Record<string, unknown>),
  };
}
