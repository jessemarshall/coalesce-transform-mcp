import { type CoalesceClient } from "../../client.js";
import {
  getWorkspaceNode,
  setWorkspaceNode,
  createWorkspaceNode,
} from "../../coalesce/api/nodes.js";
import { assertNoSqlOverridePayload } from "../policies/sql-override.js";
import { isPlainObject, uniqueInOrder, rethrowNonRecoverableApiError } from "../../utils.js";
import { inferFamily } from "../pipelines/node-type-selection.js";
import {
  getNodeColumnCount,
  getNodeStorageLocationCount,
  getNodeConfigKeyCount,
  getRequestedNodeName,
  getRequestedColumnNames,
  getRequestedConfig,
  getRequestedLocationFields,
  getNodeColumnNames,
  getNodeDependencyNames,
  normalizeColumnName,
} from "./node-inspection.js";
import {
  buildPredecessorSummary,
  buildJoinSuggestions,
  type JoinSuggestion,
} from "./join-helpers.js";
import {
  tryCompleteNodeConfiguration,
  validateNodeTypeChoice,
  assertNotSourceNodeType,
  mergeWorkspaceNodeChanges,
  buildUpdatedWorkspaceNodeBody,
  type WorkspaceNodeChanges,
} from "./node-update-helpers.js";
import {
  updateWorkspaceNode,
  replaceWorkspaceNodeColumns,
} from "./mutations.js";
import { convertJoinToAggregation } from "./join-operations.js";

type ScratchNodeCompletionLevel = "created" | "named" | "configured";

function buildScratchNodeChanges(params: {
  name?: string;
  description?: string;
  storageLocations?: unknown[];
  config?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  changes?: Record<string, unknown>;
}): Record<string, unknown> {
  let merged: Record<string, unknown> = params.changes ? { ...params.changes } : {};

  if (params.name !== undefined) {
    merged = mergeWorkspaceNodeChanges(merged, { name: params.name }) as Record<string, unknown>;
  }
  if (params.description !== undefined) {
    merged = mergeWorkspaceNodeChanges(merged, {
      description: params.description,
    }) as Record<string, unknown>;
  }
  if (params.storageLocations !== undefined) {
    merged = mergeWorkspaceNodeChanges(merged, {
      storageLocations: params.storageLocations,
    }) as Record<string, unknown>;
  }
  if (params.config !== undefined) {
    merged = mergeWorkspaceNodeChanges(merged, {
      config: params.config,
    }) as Record<string, unknown>;
  }
  if (params.metadata !== undefined) {
    merged = mergeWorkspaceNodeChanges(merged, {
      metadata: params.metadata,
    }) as Record<string, unknown>;
  }

  return merged;
}

function buildScratchNodeValidation(
  node: Record<string, unknown>,
  completionLevel: ScratchNodeCompletionLevel,
  requested: {
    changes: Record<string, unknown>;
    storageLocations?: unknown[];
  }
) {
  const requestedName = getRequestedNodeName(requested.changes);
  const requestedNameSatisfied =
    requestedName !== undefined ? node.name === requestedName : true;
  const requestedColumnNames = getRequestedColumnNames(requested.changes);
  const actualColumnNameSet = new Set(
    getNodeColumnNames(node).map((name) => normalizeColumnName(name))
  );
  const requestedColumnsSatisfied =
    requestedColumnNames.length === 0
      ? getNodeColumnCount(node) > 0
      : requestedColumnNames.every((name) =>
          actualColumnNameSet.has(normalizeColumnName(name))
        );
  const requestedConfig = getRequestedConfig(requested.changes);
  const nodeConfig = isPlainObject(node.config) ? node.config : undefined;
  const requestedConfigSatisfied =
    requestedConfig === undefined
      ? isPlainObject(node.config)
      : Object.entries(requestedConfig).every(
          ([key, value]) => nodeConfig && Object.is(nodeConfig[key], value)
        );
  const requestedLocationFields = getRequestedLocationFields(requested.changes);
  const requestedLocationSatisfied = Object.entries(
    requestedLocationFields
  ).every(([key, value]) => Object.is(node[key], value));
  const nameSet = typeof node.name === "string" && node.name.trim().length > 0;
  const storageLocationsSet = getNodeStorageLocationCount(node) > 0;
  const columnCount = getNodeColumnCount(node);
  const configPresent = isPlainObject(node.config);
  const configKeyCount = getNodeConfigKeyCount(node);
  const nameRequired =
    completionLevel !== "created" || requestedName !== undefined;
  const storageLocationsRequired = requested.storageLocations !== undefined;

  let completionSatisfied = true;
  if (completionLevel === "named") {
    completionSatisfied =
      (!nameRequired || (requestedName ? requestedNameSatisfied : nameSet)) &&
      requestedLocationSatisfied &&
      (!storageLocationsRequired || storageLocationsSet);
  } else if (completionLevel === "configured") {
    completionSatisfied =
      (!nameRequired || (requestedName ? requestedNameSatisfied : nameSet)) &&
      requestedLocationSatisfied &&
      (!storageLocationsRequired || storageLocationsSet) &&
      requestedColumnsSatisfied &&
      requestedConfigSatisfied;
  }

  return {
    requestedCompletionLevel: completionLevel,
    completionSatisfied,
    nameRequired,
    nameSet,
    requestedName: requestedName ?? null,
    requestedNameSatisfied,
    requestedLocationKeys: Object.keys(requestedLocationFields),
    requestedLocationSatisfied,
    storageLocationsRequired,
    storageLocationCount: getNodeStorageLocationCount(node),
    storageLocationsSet,
    columnCount,
    configPresent,
    configKeyCount,
    requestedColumnCount: requestedColumnNames.length,
    requestedColumnNames,
    requestedColumnsSatisfied,
    requestedConfigKeys: requestedConfig ? Object.keys(requestedConfig) : [],
    requestedConfigSatisfied,
  };
}

function assertConfiguredScratchInput(
  changes: Record<string, unknown>
): void {
  const requestedName = getRequestedNodeName(changes);
  const requestedColumnNames = getRequestedColumnNames(changes);
  const missing: string[] = [];

  if (!requestedName) {
    missing.push("name");
  }
  if (requestedColumnNames.length === 0) {
    missing.push("metadata.columns");
  }

  if (missing.length > 0) {
    throw new Error(
      `Configured scratch node creation requires ${missing.join(
        " and "
      )}. Provide them explicitly or lower completionLevel to "named" or "created".`
    );
  }
}

function buildScratchNodeNextSteps(
  nodeType: string,
  node: Record<string, unknown>
): string[] {
  const steps: string[] = [];
  const family = inferFamily([nodeType]);

  // Naming convention
  const currentName = typeof node.name === "string" ? node.name : "";
  if (!currentName || currentName === nodeType || /^[A-Z]+_\d+$/.test(currentName)) {
    steps.push(`Name this node following conventions: ${suggestNamingConvention(family)}`);
  }

  // Scratch nodes have no predecessors — remind to add columns if missing
  const metadata = isPlainObject(node.metadata) ? node.metadata : {};
  const columns = Array.isArray(metadata.columns) ? metadata.columns : [];
  if (columns.length === 0) {
    steps.push(
      "This node has no columns. Add columns using replace_workspace_node_columns or update_workspace_node."
    );
  }

  // Family-specific guidance
  if (family === "fact" || family === "dimension") {
    steps.push(
      `Verify materialization: ${family === "fact" ? "Fact" : "Dimension"} nodes should typically materialize as tables, not views. ` +
      "Check that materializationType is 'table' in the config."
    );
    if (family === "dimension") {
      steps.push(
        "For dimensions: identify the business key (natural key from the source system) and mark it isBusinessKey = true. " +
        "If this is a slowly changing dimension (SCD Type 2), ensure START_DATE/END_DATE/IS_CURRENT columns exist."
      );
    }
  }

  // Verification
  steps.push(
    "Verify the node: call get_workspace_node to confirm columns and config are correct before proceeding."
  );

  return steps;
}

export async function createWorkspaceNodeFromScratch(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeType: string;
    completionLevel?: ScratchNodeCompletionLevel;
    name?: string;
    description?: string;
    storageLocations?: unknown[];
    config?: Record<string, unknown>;
    metadata?: Record<string, unknown>;
    changes?: Record<string, unknown>;
    repoPath?: string;
    goal?: string;
  }
): Promise<unknown> {
  assertNotSourceNodeType(params.nodeType);

  const completionLevel = params.completionLevel ?? "configured";
  const scratchChanges = buildScratchNodeChanges(params);
  assertNoSqlOverridePayload(
    scratchChanges,
    "create_workspace_node_from_scratch changes"
  );
  if (completionLevel === "configured") {
    assertConfiguredScratchInput(scratchChanges);
  }

  // Validate node type choice — throws if the type is excluded (e.g., inputMode: 'sql')
  // or if a specialized pattern is detected without matching context
  const nodeTypeValidation = await validateNodeTypeChoice(client, {
    workspaceID: params.workspaceID,
    nodeType: params.nodeType,
    predecessorCount: 0,
    repoPath: params.repoPath,
    goal: params.goal,
  });

  const created = await createWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeType: params.nodeType,
  });

  if (!isPlainObject(created) || typeof created.id !== "string") {
    throw new Error("Workspace node creation did not return a node ID");
  }

  const createdNode = await getWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: created.id,
  });

  if (!isPlainObject(createdNode)) {
    throw new Error("Created workspace node response was not an object");
  }

  let finalNode: unknown = createdNode;
  if (Object.keys(scratchChanges).length > 0) {
    const body = buildUpdatedWorkspaceNodeBody(createdNode, scratchChanges);

    await setWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: created.id,
      body,
    });

    finalNode = await getWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: created.id,
    });
  }

  if (!isPlainObject(finalNode)) {
    throw new Error("Final workspace node response was not an object");
  }

  const validation = buildScratchNodeValidation(finalNode, completionLevel, {
    changes: scratchChanges,
    storageLocations: params.storageLocations,
  });

  const nextSteps = buildScratchNodeNextSteps(params.nodeType, finalNode);

  if (!validation.completionSatisfied) {
    if (completionLevel === "configured") {
      throw new Error(
        `Workspace node ${created.id} was created, but configured scratch validation failed. ` +
        `Check name, metadata.columns, and config values on the saved node body. ` +
        `To clean up, use delete_workspace_node with nodeID "${created.id}".`
      );
    }
    return {
      node: finalNode,
      validation,
      nextSteps,
      warning:
        "Workspace node was created, but the requested scratch completion level was not fully satisfied. Review the node body and provide any missing name, storageLocations, metadata.columns, or config fields.",
      ...(nodeTypeValidation ? { nodeTypeValidation } : {}),
    };
  }

  // Automatically complete node configuration using intelligent rules (best-effort)
  const completion = await tryCompleteNodeConfiguration(client, {
    workspaceID: params.workspaceID,
    nodeID: created.id,
    repoPath: params.repoPath,
  });

  return {
    node: completion.configCompletion?.node ?? finalNode,
    validation,
    nextSteps,
    ...completion,
    ...(nodeTypeValidation ? { nodeTypeValidation } : {}),
  };
}


export function suggestNamingConvention(family: string): string {
  const conventions: Record<string, string> = {
    stage: "STG_<SOURCE_NAME> (e.g., STG_CUSTOMERS, STG_ORDERS)",
    dimension: "DIM_<ENTITY> (e.g., DIM_CUSTOMER, DIM_PRODUCT)",
    fact: "FACT_<BUSINESS_PROCESS> or FCT_<BUSINESS_PROCESS> (e.g., FACT_SALES, FACT_CLV)",
    view: "V_<PURPOSE> or INT_<PURPOSE> (e.g., V_CUSTOMER_ORDERS)",
    work: "INT_<PURPOSE> or WRK_<PURPOSE> (e.g., INT_ORDER_ENRICHMENT)",
    hub: "HUB_<BUSINESS_KEY> (e.g., HUB_CUSTOMER)",
    satellite: "SAT_<HUB>_<CONTEXT> (e.g., SAT_CUSTOMER_DETAILS)",
    link: "LNK_<RELATIONSHIP> (e.g., LNK_CUSTOMER_ORDER)",
  };
  return conventions[family] ?? "Use a descriptive, layer-appropriate name";
}

export function buildPostCreationNextSteps(
  predecessorCount: number,
  nodeType: string,
  joinSuggestions: JoinSuggestion[],
  node: Record<string, unknown>
): string[] {
  const steps: string[] = [];
  const family = inferFamily([nodeType]);

  // Naming convention
  const currentName = typeof node.name === "string" ? node.name : "";
  if (!currentName || currentName === nodeType || /^[A-Z]+_\d+$/.test(currentName)) {
    steps.push(`Name this node following conventions: ${suggestNamingConvention(family)}`);
  }

  // Multi-predecessor: join setup is REQUIRED
  if (predecessorCount > 1) {
    const hasCommonColumns = joinSuggestions.some((s) => s.commonColumns.length > 0);

    steps.push(
      "REQUIRED: Set up the join condition. This multi-predecessor node needs a FROM/JOIN/ON clause in the joinCondition. " +
      "Review joinSuggestions above to identify join columns, then either:" +
      "\n  - Call convert_join_to_aggregation (for GROUP BY / aggregation use cases)" +
      "\n  - Call apply_join_condition (for row-level joins — auto-generates FROM/JOIN/ON with {{ ref() }} syntax)"
    );

    if (hasCommonColumns) {
      steps.push(
        "Verify join columns: joinSuggestions shows common column names between predecessors. " +
        "Confirm these are the correct join keys (business keys, not surrogate keys). " +
        "Choose the right join type: INNER JOIN (only matching rows), LEFT JOIN (keep all rows from first table), " +
        "FULL OUTER JOIN (keep all rows from both)."
      );
    } else {
      steps.push(
        "WARNING: No common columns found between predecessors. You may need a cross join, " +
        "or the join columns have different names. Verify the correct join keys with the user."
      );
    }
  }

  // Family-specific guidance
  if (family === "fact" || family === "dimension") {
    steps.push(
      `Verify materialization: ${family === "fact" ? "Fact" : "Dimension"} nodes should typically materialize as tables, not views. ` +
      "Check that materializationType is 'table' in the config."
    );
    if (family === "fact" && predecessorCount > 1) {
      steps.push(
        "For fact tables: define the grain (the set of columns that uniquely identify each row). " +
        "These grain columns become your GROUP BY columns in convert_join_to_aggregation. " +
        "Mark them as isBusinessKey = true."
      );
    }
    if (family === "dimension") {
      steps.push(
        "For dimensions: identify the business key (natural key from the source system) and mark it isBusinessKey = true. " +
        "If this is a slowly changing dimension (SCD Type 2), ensure START_DATE/END_DATE/IS_CURRENT columns exist."
      );
    }
  }

  // Single predecessor: simpler guidance
  if (predecessorCount === 1) {
    steps.push(
      "Review auto-populated columns. Remove columns you don't need and add transforms where appropriate. " +
      "Columns without transforms are pass-throughs (inherited as-is from the predecessor)."
    );
  }

  // Verification
  steps.push(
    "Verify the node: call get_workspace_node to confirm columns, config, and join condition are correct before proceeding to downstream nodes."
  );

  return steps;
}

export async function createWorkspaceNodeFromPredecessor(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeType: string;
    predecessorNodeIDs: string[];
    changes?: Record<string, unknown>;
    repoPath?: string;
    goal?: string;
    /** Replace auto-populated columns with these specific columns+transforms in a single call. */
    columns?: Array<{ name: string; transform?: string; dataType?: string; description?: string }>;
    /** WHERE filter to append to the joinCondition. Only valid with `columns`, not with aggregation. */
    whereCondition?: string;
    /** GROUP BY columns for aggregation. Must be provided with `aggregates`. */
    groupByColumns?: string[];
    /** Aggregate columns. Must be provided with `groupByColumns`. */
    aggregates?: Array<{ name: string; function: string; expression: string; description?: string }>;
    /** JOIN type for aggregation nodes. Only used with groupByColumns/aggregates. */
    joinType?: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN";
  }
): Promise<unknown> {
  assertNotSourceNodeType(params.nodeType);
  const effectivePredecessorNodeIDs = uniqueInOrder(params.predecessorNodeIDs);

  if (params.changes) {
    assertNoSqlOverridePayload(
      params.changes,
      "create_workspace_node_from_predecessor changes"
    );
  }

  // Validate mutually exclusive params
  if (params.columns && (params.groupByColumns || params.aggregates)) {
    throw new Error(
      "Cannot provide both 'columns' and 'groupByColumns'/'aggregates'. " +
      "Use 'columns' for column replacement, or 'groupByColumns'+'aggregates' for aggregation."
    );
  }
  if (params.aggregates && !params.groupByColumns) {
    throw new Error("'aggregates' requires 'groupByColumns' to be provided.");
  }
  if (params.groupByColumns && !params.aggregates) {
    throw new Error("'groupByColumns' requires 'aggregates' to be provided.");
  }
  if (params.whereCondition && params.groupByColumns) {
    throw new Error(
      "'whereCondition' cannot be combined with 'groupByColumns'/'aggregates'. " +
      "For aggregation nodes, WHERE/HAVING filters should be applied via a separate update_workspace_node call."
    );
  }

  // Validate node type choice and fetch predecessors in parallel
  // validateNodeTypeChoice throws if the type is excluded (e.g., inputMode: 'sql')
  // or if a specialized pattern is detected without matching context
  const [nodeTypeValidation, predecessorNodes] = await Promise.all([
    validateNodeTypeChoice(client, {
      workspaceID: params.workspaceID,
      nodeType: params.nodeType,
      predecessorCount: effectivePredecessorNodeIDs.length,
      repoPath: params.repoPath,
      goal: params.goal,
    }),
    Promise.all(
      effectivePredecessorNodeIDs.map(async (nodeID) => {
        const predecessor = await getWorkspaceNode(client, {
          workspaceID: params.workspaceID,
          nodeID,
        });
        if (!isPlainObject(predecessor)) {
          throw new Error(
            `Predecessor node response was not an object for nodeID ${nodeID}`
          );
        }
        return buildPredecessorSummary(nodeID, predecessor);
      })
    ),
  ]);

  const joinSuggestions = buildJoinSuggestions(predecessorNodes);

  const created = await createWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeType: params.nodeType,
    predecessorNodeIDs: effectivePredecessorNodeIDs,
  });

  if (!isPlainObject(created) || typeof created.id !== "string") {
    throw new Error("Workspace node creation did not return a node ID");
  }

  const createdNode = await getWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: created.id,
  });

  if (!isPlainObject(createdNode)) {
    throw new Error("Created workspace node response was not an object");
  }

  const referencedPredecessorNodeIDs = getReferencedPredecessorNodeIDs(
    createdNode,
    effectivePredecessorNodeIDs
  );
  const allPredecessorsRepresented =
    referencedPredecessorNodeIDs.length === effectivePredecessorNodeIDs.length;
  const autoPopulatedColumns =
    getNodeColumnCount(createdNode) > 0 &&
    (effectivePredecessorNodeIDs.length === 1
      ? referencedPredecessorNodeIDs.length > 0
      : allPredecessorsRepresented);

  const validation = {
    autoPopulatedColumns,
    allPredecessorsRepresented,
    columnCount: getNodeColumnCount(createdNode),
    dependencyCount: getNodeDependencyNames(createdNode).length,
    dependencyNames: getNodeDependencyNames(createdNode),
    predecessorNodeIDs: effectivePredecessorNodeIDs,
    referencedPredecessorNodeIDs,
  };

  // Build context-aware next steps for multi-predecessor nodes
  const nextSteps = buildPostCreationNextSteps(
    effectivePredecessorNodeIDs.length,
    params.nodeType,
    joinSuggestions,
    createdNode
  );

  if (!validation.autoPopulatedColumns) {
    const warning =
      effectivePredecessorNodeIDs.length > 1
        ? "Workspace node was created from predecessor(s), but columns were not auto-populated from all requested predecessors. Review the suggested join columns and verify the node in Coalesce before proceeding."
        : "Workspace node was created from predecessor(s), but columns were not auto-populated. Verify the node in Coalesce before proceeding.";
    return {
      node: createdNode,
      predecessors: predecessorNodes,
      joinSuggestions,
      validation,
      warning,
      nextSteps,
      ...(nodeTypeValidation ? { nodeTypeValidation } : {}),
    };
  }

  if (params.changes && Object.keys(params.changes).length > 0) {
    const body = buildUpdatedWorkspaceNodeBody(createdNode, params.changes);

    await setWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: created.id,
      body,
    });
  }

  // Single-call aggregation path: create + replace columns + write joinCondition + config completion
  // convertJoinToAggregation handles all of this internally, including config completion.
  if (params.groupByColumns && params.aggregates) {
    const aggResult = await convertJoinToAggregation(client, {
      workspaceID: params.workspaceID,
      nodeID: created.id,
      groupByColumns: params.groupByColumns,
      aggregates: params.aggregates,
      joinType: params.joinType,
      repoPath: params.repoPath,
    });

    return {
      node: aggResult.node,
      predecessors: predecessorNodes,
      joinSuggestions,
      validation,
      joinSQL: aggResult.joinSQL,
      groupByAnalysis: aggResult.groupByAnalysis,
      aggregationValidation: aggResult.validation,
      ...(aggResult.configCompletion ? { configCompletion: aggResult.configCompletion } : {}),
      ...(aggResult.configCompletionSkipped ? { configCompletionSkipped: aggResult.configCompletionSkipped } : {}),
      nextSteps,
      ...(nodeTypeValidation ? { nodeTypeValidation } : {}),
    };
  }

  // Single-call column replacement path: create + replace columns + WHERE + config completion
  if (params.columns) {
    await replaceWorkspaceNodeColumns(client, {
      workspaceID: params.workspaceID,
      nodeID: created.id,
      columns: params.columns,
      whereCondition: params.whereCondition,
    });
  }

  // Automatically complete node configuration using intelligent rules (best-effort)
  const completion = await tryCompleteNodeConfiguration(client, {
    workspaceID: params.workspaceID,
    nodeID: created.id,
    repoPath: params.repoPath,
  });

  // Re-fetch the node if changes were applied but config completion failed
  let resultNode: unknown = completion.configCompletion?.node ?? createdNode;
  let nodeDataStale = false;
  if (completion.configCompletionSkipped) {
    const hasChanges = (params.changes && Object.keys(params.changes).length > 0) || params.columns;
    if (hasChanges) {
      try {
        resultNode = await getWorkspaceNode(client, { workspaceID: params.workspaceID, nodeID: created.id });
      } catch (error) {
        rethrowNonRecoverableApiError(error);
        const reason = error instanceof Error ? error.message : String(error);
        process.stderr.write(`[createWorkspaceNodeFromPredecessor] Re-fetch after config completion skip failed: ${reason}\n`);
        resultNode = createdNode;
        nodeDataStale = true;
      }
    }
  }

  return {
    node: resultNode,
    predecessors: predecessorNodes,
    joinSuggestions,
    validation,
    ...completion,
    nextSteps,
    ...(nodeTypeValidation ? { nodeTypeValidation } : {}),
    ...(nodeDataStale ? { warning: "Node was created successfully but re-fetch failed after config completion was skipped. The returned node data may not reflect the current server state." } : {}),
  };
}

// Re-import for use in createWorkspaceNodeFromPredecessor
import { getReferencedPredecessorNodeIDs } from "./join-helpers.js";
