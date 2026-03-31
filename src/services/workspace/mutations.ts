import { type CoalesceClient } from "../../client.js";
import {
  getWorkspaceNode,
  setWorkspaceNode,
} from "../../coalesce/api/nodes.js";
import { fetchAllWorkspaceNodes } from "../cache/snapshots.js";
import { assertNoSqlOverridePayload } from "../policies/sql-override.js";
import { isPlainObject } from "../../utils.js";
import {
  getNodeColumnCount,
  getNodeDependencyNames,
  normalizeColumnName,
} from "./node-inspection.js";
import {
  appendWhereToJoinCondition,
} from "./join-helpers.js";
import {
  tryCompleteNodeConfiguration,
  reconcileExternalSchema,
  mergeWorkspaceNodeChanges,
  buildUpdatedWorkspaceNodeBody,
  type WorkspaceNodeChanges,
} from "./node-update-helpers.js";
import type { ExternalColumnInput } from "../../schemas/node-payloads.js";
import { createWorkspaceNodeFromPredecessor } from "./node-creation.js";

// ── Barrel re-exports ──────────────────────────────────────────────────
// Preserve existing import paths: everything that used to be exported from
// this module is re-exported from the new files below.
export {
  tryCompleteNodeConfiguration,
  validateNodeTypeChoice,
  assertNotSourceNodeType,
  mergeWorkspaceNodeChanges,
  syncNodeNameIntoMetadataSourceMapping,
  buildUpdatedWorkspaceNodeBody,
  reconcileExternalSchema,
  type WorkspaceNodeChanges,
  type NodeTypeValidation,
  type SchemaReconciliationColumn,
  type SchemaReconciliation,
} from "./node-update-helpers.js";

export {
  createWorkspaceNodeFromScratch,
  createWorkspaceNodeFromPredecessor,
  suggestNamingConvention,
  buildPostCreationNextSteps,
} from "./node-creation.js";

export {
  convertJoinToAggregation,
  applyJoinCondition,
} from "./join-operations.js";

// ── Group B: Core update operations (kept in this file) ────────────────

export async function updateWorkspaceNode(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    changes: Record<string, unknown>;
  }
): Promise<unknown> {
  assertNoSqlOverridePayload(params.changes, "update_workspace_node changes");

  const current = await getWorkspaceNode(client, params);
  if (!isPlainObject(current)) {
    throw new Error("Workspace node response was not an object");
  }

  const body = buildUpdatedWorkspaceNodeBody(current, params.changes);

  return setWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
    body,
  });
}

export async function replaceWorkspaceNodeColumns(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    columns: unknown[];
    whereCondition?: string;
    additionalChanges?: Record<string, unknown>;
  }
): Promise<unknown> {
  if (params.additionalChanges) {
    assertNoSqlOverridePayload(
      params.additionalChanges,
      "replace_workspace_node_columns additionalChanges"
    );
    // Block sourceMapping in additionalChanges — use apply_join_condition or convert_join_to_aggregation instead
    const additionalMeta = isPlainObject(params.additionalChanges.metadata)
      ? params.additionalChanges.metadata
      : null;
    if (additionalMeta && ("sourceMapping" in additionalMeta || "customSQL" in additionalMeta)) {
      throw new Error(
        "replace_workspace_node_columns additionalChanges cannot set sourceMapping or customSQL. " +
        "Use the joinCondition parameter to set WHERE filters, apply_join_condition for join setup, " +
        "or convert_join_to_aggregation for GROUP BY patterns."
      );
    }
  }

  const current = await getWorkspaceNode(client, params);
  if (!isPlainObject(current)) {
    throw new Error("Workspace node response was not an object");
  }

  // Build changes: merge additionalChanges first, then overlay columns so params.columns always wins
  const columnChanges: WorkspaceNodeChanges = {
    metadata: {
      columns: params.columns,
    },
  };
  const changes = params.additionalChanges
    ? (mergeWorkspaceNodeChanges(params.additionalChanges, columnChanges) as WorkspaceNodeChanges)
    : columnChanges;

  // Use shared logic to build clean update body
  // This handles: merging, name synchronization, and metadata cleaning
  const updated = buildUpdatedWorkspaceNodeBody(current, changes);

  // Append WHERE condition to existing joinCondition if provided
  if (params.whereCondition && typeof params.whereCondition === "string") {
    appendWhereToJoinCondition(updated, params.whereCondition);
  }

  return setWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
    body: updated,
  });
}

// ── Group F: Workspace node types + external schema ────────────────────

/**
 * Returns the distinct node types observed in existing workspace nodes.
 * This is intentionally observation-based and should not be treated as a true
 * installed-type registry for the workspace.
 */
export async function listWorkspaceNodeTypes(
  client: CoalesceClient,
  params: { workspaceID: string }
): Promise<{
  workspaceID: string;
  basis: "observed_nodes";
  nodeTypes: string[];
  counts: Record<string, number>;
  total: number;
}> {
  const { workspaceID } = params;
  const nodes = await fetchAllWorkspaceNodes(client, {
    workspaceID,
    detail: false,
  });

  const data = nodes.items;
  const counts: Record<string, number> = {};
  let total = 0;

  for (const node of data) {
    if (!isPlainObject(node)) {
      continue;
    }
    const nodeType = node.nodeType;
    if (typeof nodeType === 'string' && nodeType.length > 0) {
      counts[nodeType] = (counts[nodeType] ?? 0) + 1;
      total++;
    }
  }

  const nodeTypes = Object.keys(counts).sort((a, b) => counts[b] - counts[a]);

  return {
    workspaceID,
    basis: "observed_nodes",
    nodeTypes,
    counts,
    total
  };
}

/**
 * Create a workspace node whose output columns match an external table schema
 * (e.g., from Snowflake, dbt, or any metadata source).
 *
 * Workflow:
 * 1. Create node from predecessor (auto-populates columns from source)
 * 2. Reconcile auto-populated columns against the target external schema:
 *    - Matched columns: preserve source linkage, override dataType to match external
 *    - New columns: add without source linkage, flag as needing a transform
 *    - Missing columns: drop (not in external schema)
 * 3. Replace columns on the created node with the reconciled set
 * 4. Return node + reconciliation report
 */
export async function createNodeFromExternalSchema(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeType: string;
    predecessorNodeIDs: string[];
    targetColumns: ExternalColumnInput[];
    targetName?: string;
    repoPath?: string;
    goal?: string;
    locationName?: string;
  }
): Promise<unknown> {
  if (!params.targetColumns.length) {
    throw new Error("targetColumns must contain at least one column.");
  }

  // Build changes for name and location if provided
  const changes: Record<string, unknown> = {};
  if (params.targetName) {
    changes.name = params.targetName;
  }
  if (params.locationName) {
    changes.locationName = params.locationName;
  }

  // Step 1: Create node from predecessor — this auto-populates columns
  const creationResult = await createWorkspaceNodeFromPredecessor(client, {
    workspaceID: params.workspaceID,
    nodeType: params.nodeType,
    predecessorNodeIDs: params.predecessorNodeIDs,
    repoPath: params.repoPath,
    goal: params.goal,
    ...(Object.keys(changes).length > 0 ? { changes } : {}),
  }) as Record<string, unknown>;

  // Extract the created node
  const createdNode = isPlainObject(creationResult.node) ? creationResult.node : null;
  if (!createdNode || typeof createdNode.id !== "string") {
    throw new Error("Node creation did not return a valid node.");
  }

  // Step 2: Get the auto-populated columns from the created node
  const nodeID = createdNode.id as string;
  const metadata = isPlainObject(createdNode.metadata) ? createdNode.metadata : null;
  const autoPopulatedColumns = Array.isArray(metadata?.columns)
    ? (metadata.columns as Record<string, unknown>[])
    : [];

  if (autoPopulatedColumns.length === 0) {
    throw new Error(
      `Node was created (nodeID: ${nodeID}) but has no auto-populated columns. ` +
      `Reconciliation cannot preserve source linkage. ` +
      `Verify the predecessor node(s) have columns and try again, or use create_workspace_node_from_predecessor directly.`
    );
  }

  // Step 3: Reconcile external schema against auto-populated columns
  const { columns: reconciledColumns, reconciliation } = reconcileExternalSchema(
    autoPopulatedColumns,
    params.targetColumns
  );

  // Step 4: Replace columns on the node with the reconciled set
  let updatedNode: unknown;
  try {
    updatedNode = await replaceWorkspaceNodeColumns(client, {
      workspaceID: params.workspaceID,
      nodeID,
      columns: reconciledColumns,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Column reconciliation failed after node was created (nodeID: ${nodeID}). ` +
      `The node exists in the workspace with un-reconciled columns. ` +
      `Either delete it with delete_workspace_node or retry column replacement with replace_workspace_node_columns. ` +
      `Original error: ${message}`
    );
  }

  // Step 5: Re-run config completion after column reconciliation
  // The inner createWorkspaceNodeFromPredecessor ran config completion before column
  // replacement, so column-level attributes (isBusinessKey, etc.) may be stale.
  const completion = await tryCompleteNodeConfiguration(client, {
    workspaceID: params.workspaceID,
    nodeID,
    repoPath: params.repoPath,
  });
  const { configCompletion, configCompletionSkipped } = completion;
  if (configCompletion) {
    updatedNode = configCompletion.node;
  }

  // Build next steps based on reconciliation
  const nextSteps: string[] = [];
  const unmappedNames = reconciliation.added
    .filter((a) => a.needsTransform)
    .map((a) => a.name);
  if (unmappedNames.length > 0) {
    nextSteps.push(
      `Add transforms for unmapped columns: ${unmappedNames.join(", ")}. ` +
      `These columns exist in the target schema but have no matching predecessor column.`
    );
  }
  if (reconciliation.typeChanges.length > 0) {
    nextSteps.push(
      `Review type changes: ${reconciliation.typeChanges.map((tc) => `${tc.name} (${tc.from} → ${tc.to})`).join(", ")}. ` +
      `The target schema uses different types than the predecessor — verify transforms handle the conversion.`
    );
  }
  if (reconciliation.dropped.length > 0) {
    nextSteps.push(
      `Dropped ${reconciliation.dropped.length} predecessor column(s) not in target schema: ${reconciliation.dropped.map((d) => d.name).join(", ")}.`
    );
  }
  nextSteps.push("Verify the node: call get_workspace_node to confirm columns, config, and join condition are correct.");

  return {
    node: updatedNode,
    reconciliation,
    predecessors: creationResult.predecessors,
    validation: creationResult.validation,
    ...(configCompletion ? { configCompletion } : {}),
    ...(configCompletionSkipped ? { configCompletionSkipped } : {}),
    ...(creationResult.nodeTypeValidation ? { nodeTypeValidation: creationResult.nodeTypeValidation } : {}),
    nextSteps,
  };
}
