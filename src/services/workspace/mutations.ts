import { randomUUID } from "node:crypto";
import { CoalesceApiError, type CoalesceClient } from "../../client.js";
import {
  getWorkspaceNode,
  setWorkspaceNode,
  createWorkspaceNode,
} from "../../coalesce/api/nodes.js";
import { fetchAllWorkspaceNodes } from "../cache/snapshots.js";
import { assertNoSqlOverridePayload } from "../policies/sql-override.js";
import { completeNodeConfiguration, type ConfigCompletionResult } from "../config/intelligent.js";
import { isPlainObject, uniqueInOrder } from "../../utils.js";
import { selectPipelineNodeType, inferFamily } from "../pipelines/node-type-selection.js";
import { detectSpecializedPatternPenalty } from "../pipelines/node-type-intent.js";
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
  normalizeDataType,
} from "./node-inspection.js";
import {
  buildPredecessorSummary,
  buildJoinSuggestions,
  generateJoinSQL,
  generateRefJoinSQL,
  inferDatatype,
  analyzeColumnsForGroupBy,
  extractPredecessorNodeIDs,
  extractPredecessorRefInfo,
  getReferencedPredecessorNodeIDs,
  appendWhereToJoinCondition,
  type PredecessorSummary,
  type JoinSuggestion,
  type JoinClause,
  type GroupByAnalysis,
  type ColumnTransform,
  type PredecessorRefInfo,
} from "./join-helpers.js";
import { isPassthroughTransform } from "../shared/node-helpers.js";

const CONFIG_COMPLETION_SKIP_MSG =
  "call complete_node_configuration with repoPath after creation to apply node type config and column-level attributes.";

async function tryCompleteNodeConfiguration(
  client: CoalesceClient,
  params: { workspaceID: string; nodeID: string; repoPath?: string },
): Promise<{ configCompletion?: ConfigCompletionResult; configCompletionSkipped?: string }> {
  try {
    const configCompletion = await completeNodeConfiguration(client, params);
    return { configCompletion };
  } catch (error) {
    if (error instanceof CoalesceApiError && [401, 403, 503].includes(error.status)) {
      throw error;
    }
    const reason = error instanceof Error ? error.message : String(error);
    return { configCompletionSkipped: `Config completion failed (${reason}) — ${CONFIG_COMPLETION_SKIP_MSG}` };
  }
}

type NodeTypeValidation = {
  requestedNodeType: string;
  topRankedNodeType: string | null;
  isTopRanked: boolean;
  strategy: string;
  consideredNodeTypes: Array<{ nodeType: string; displayName: string | null; score: number; reasons: string[] }>;
  warning?: string;
};

/**
 * Validates the requested nodeType against all available types from repo + workspace.
 * Returns ranking info so the agent (and user) can see all considered options.
 * Throws if the requested type is excluded (e.g., inputMode: 'sql').
 * Returns null on non-critical validation failures (creation proceeds).
 */
async function validateNodeTypeChoice(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeType: string;
    predecessorCount: number;
    repoPath?: string;
    goal?: string;
  }
): Promise<NodeTypeValidation | null> {
  try {
    const nodeTypesResult = await listWorkspaceNodeTypes(client, {
      workspaceID: params.workspaceID,
    });

    const selectionResult = selectPipelineNodeType({
      explicitNodeType: params.nodeType,
      sourceCount: params.predecessorCount,
      workspaceNodeTypes: nodeTypesResult.nodeTypes,
      workspaceNodeTypeCounts: nodeTypesResult.counts,
      repoPath: params.repoPath,
      goal: params.goal,
    });

    // Hard block: if the requested type was excluded (e.g., inputMode: 'sql'), throw
    const exclusionWarning = selectionResult.warnings.find((w) =>
      w.includes("is excluded because it relies on raw SQL override")
    );
    if (exclusionWarning) {
      throw new Error(exclusionWarning);
    }

    const topRanked = selectionResult.selection.consideredNodeTypes[0] ?? null;
    const isTopRanked = topRanked?.nodeType === params.nodeType;

    const validation: NodeTypeValidation = {
      requestedNodeType: params.nodeType,
      topRankedNodeType: topRanked?.nodeType ?? null,
      isTopRanked,
      strategy: selectionResult.selection.strategy,
      consideredNodeTypes: selectionResult.selection.consideredNodeTypes.slice(0, 5).map((c) => ({
        nodeType: c.nodeType,
        displayName: c.displayName,
        score: c.score,
        reasons: c.reasons,
      })),
    };

    if (!isTopRanked && topRanked) {
      validation.warning =
        `Requested nodeType "${params.nodeType}" is not the top-ranked type. ` +
        `Consider using "${topRanked.nodeType}" (score: ${topRanked.score}) instead. ` +
        `Call plan_pipeline first to discover and rank all available node types.`;
    }

    // Hard block specialized materialization patterns (Dynamic Tables, Incremental, etc.)
    // unless the goal explicitly requests them.
    // Build candidate signals from the matched type's display name and short name,
    // since the raw nodeType ID (e.g., "65") won't match pattern detection.
    const matchedCandidate = selectionResult.selection.consideredNodeTypes.find(
      (c) => c.nodeType === params.nodeType
    );
    const candidateSignals = [
      params.nodeType,
      matchedCandidate?.displayName ?? "",
      matchedCandidate?.shortName ?? "",
    ].join(" ");
    const contextText = params.goal ?? "";
    const specializedPenalty = detectSpecializedPatternPenalty(candidateSignals, contextText);
    if (specializedPenalty) {
      // Hard block — specialized types require explicit context
      const patternName = specializedPenalty.reason.split(" pattern")[0] ?? "Specialized";
      throw new Error(
        `Cannot create node with nodeType "${params.nodeType}" (${matchedCandidate?.displayName ?? "unknown"}): ` +
        `${specializedPenalty.reason}. ` +
        `${patternName} types require an explicit use case (e.g., "${patternName.toLowerCase()}" in the goal). ` +
        `For standard batch ETL, staging, joins, and aggregations, use a general-purpose type ` +
        `(Stage, Work, View, Dimension, Fact). ` +
        `Call plan_pipeline to discover the correct nodeType for your use case.`
      );
    }

    return validation;
  } catch (error) {
    // Auth and network errors indicate a broken session — let them propagate
    if (error instanceof CoalesceApiError && [401, 403, 503].includes(error.status)) {
      throw error;
    }
    // Re-throw hard block errors — exclusion and specialized pattern blocks
    if (error instanceof Error && (
      error.message.includes("is excluded") ||
      error.message.startsWith("Cannot create node")
    )) {
      throw error;
    }
    // Log unexpected errors so they are not completely invisible
    const reason = error instanceof Error ? error.message : String(error);
    process.stderr.write(
      `[validateNodeTypeChoice] Unexpected error for nodeType "${params.nodeType}": ${reason}. Validation skipped.\n`
    );
    return null;
  }
}

/**
 * Blocks creation of "Source" node types — Source nodes are read-only data
 * definitions created via the Coalesce UI, not downstream processing nodes.
 * Agents often confuse "Source" with "Stage".
 */
function assertNotSourceNodeType(nodeType: string): void {
  const normalized = nodeType.toLowerCase().replace(/.*:::/, "");
  if (normalized === "source") {
    throw new Error(
      `Cannot create a node with nodeType "Source". Source nodes are read-only data ` +
      `definitions — they represent external tables and are created via the Coalesce UI, ` +
      `not via the API. You probably want "Stage" for a staging/transform node. ` +
      `Call plan_pipeline to discover the correct nodeType.`
    );
  }
}

type ScratchNodeCompletionLevel = "created" | "named" | "configured";

export type WorkspaceNodeChanges = Record<string, unknown>;

function mergeWorkspaceNodeChanges(
  current: unknown,
  changes: unknown
): unknown {
  if (Array.isArray(changes)) {
    return changes;
  }

  if (isPlainObject(current) && isPlainObject(changes)) {
    const merged: Record<string, unknown> = { ...current };
    for (const [key, value] of Object.entries(changes)) {
      merged[key] = mergeWorkspaceNodeChanges(current[key], value);
    }
    return merged;
  }

  return changes;
}

function syncNodeNameIntoMetadataSourceMapping(
  current: Record<string, unknown>,
  merged: Record<string, unknown>,
  changes: WorkspaceNodeChanges
): Record<string, unknown> {
  if (typeof changes.name !== "string") {
    return merged;
  }

  const metadataChanges = isPlainObject(changes.metadata) ? changes.metadata : undefined;
  if (metadataChanges && "sourceMapping" in metadataChanges) {
    return merged;
  }

  const mergedMetadata = isPlainObject(merged.metadata) ? merged.metadata : undefined;
  if (!mergedMetadata || !Array.isArray(mergedMetadata.sourceMapping)) {
    return merged;
  }

  const previousName =
    typeof current.name === "string" && current.name.trim().length > 0
      ? current.name
      : null;
  const updateSingleUnnamedMapping =
    previousName === null && mergedMetadata.sourceMapping.length === 1;

  return {
    ...merged,
    metadata: {
      ...mergedMetadata,
      sourceMapping: mergedMetadata.sourceMapping.map((entry) => {
        if (!isPlainObject(entry)) {
          return entry;
        }
        const shouldRename =
          (previousName !== null && entry.name === previousName) ||
          updateSingleUnnamedMapping;
        if (!shouldRename) {
          return entry;
        }
        return {
          ...entry,
          name: changes.name,
        };
      }),
    },
  };
}

export function buildUpdatedWorkspaceNodeBody(
  current: unknown,
  changes: WorkspaceNodeChanges
): Record<string, unknown> {
  if (!isPlainObject(current)) {
    throw new Error("Workspace node response was not an object");
  }

  const merged = mergeWorkspaceNodeChanges(current, changes);
  if (!isPlainObject(merged)) {
    throw new Error("Merged workspace node update was not an object");
  }

  const synchronized = syncNodeNameIntoMetadataSourceMapping(current, merged, changes);
  if (!isPlainObject(synchronized)) {
    throw new Error("Synchronized workspace node update was not an object");
  }

  // Preserve the node name casing as provided by the user or existing node.
  // Snowflake treats unquoted identifiers as uppercase, but users may choose
  // lowercase names (e.g., for Databricks or personal preference) — respect that.

  // Validate nodeType and materializationType compatibility
  validateNodeTypeMaterializationCompatibility(
    synchronized.nodeType,
    synchronized.materializationType
  );

  // Strip invalid fields from metadata before sending to Coalesce
  if (isPlainObject(synchronized.metadata)) {
    synchronized.metadata = cleanMetadata(synchronized.metadata);
  }

  // Ensure API-required fields (table, overrideSQL, columnIDs) are preserved
  ensureRequiredApiFields(current, synchronized);

  return synchronized;
}

import type { ExternalColumnInput } from "../../schemas/node-payloads.js";

type SchemaReconciliationColumn = {
  name: string;
  sourceDataType: string | null;
  targetDataType: string;
  typeChanged: boolean;
};

type SchemaReconciliation = {
  matched: SchemaReconciliationColumn[];
  added: Array<{ name: string; dataType: string; needsTransform: boolean }>;
  dropped: Array<{ name: string; reason: string }>;
  typeChanges: Array<{ name: string; from: string; to: string }>;
};

/**
 * Reconcile external target columns against a node's auto-populated columns.
 *
 * For each target column:
 * - If it matches an auto-populated predecessor column by name, the source
 *   linkage is preserved but the dataType is overridden to match the external schema.
 * - If it has no match in the predecessor, it is added as a new column
 *   (flagged as needing a transform unless one was provided).
 *
 * Predecessor columns not present in the target list are dropped.
 */
function reconcileExternalSchema(
  autoPopulatedColumns: Record<string, unknown>[],
  targetColumns: ExternalColumnInput[]
): { columns: Record<string, unknown>[]; reconciliation: SchemaReconciliation } {
  // Index auto-populated columns by normalized name
  const existingByName = new Map<string, Record<string, unknown>>();
  for (const col of autoPopulatedColumns) {
    if (isPlainObject(col) && typeof col.name === "string") {
      existingByName.set(normalizeColumnName(col.name), col);
    }
  }

  const targetNameSet = new Set(
    targetColumns.map((tc) => normalizeColumnName(tc.name))
  );

  const reconciliation: SchemaReconciliation = {
    matched: [],
    added: [],
    dropped: [],
    typeChanges: [],
  };

  // Find dropped columns (in predecessor, not in target)
  for (const col of autoPopulatedColumns) {
    if (!isPlainObject(col) || typeof col.name !== "string") continue;
    if (!targetNameSet.has(normalizeColumnName(col.name))) {
      reconciliation.dropped.push({
        name: col.name,
        reason: "not in target schema",
      });
    }
  }

  // Build reconciled column list in target column order
  const reconciledColumns: Record<string, unknown>[] = [];

  for (const target of targetColumns) {
    const normalizedName = normalizeColumnName(target.name);
    const existing = existingByName.get(normalizedName);

    if (existing) {
      // Matched — preserve source linkage, override dataType
      const sourceDataType = typeof existing.dataType === "string" ? existing.dataType : null;
      const typeChanged = sourceDataType !== null && normalizeDataType(sourceDataType) !== normalizeDataType(target.dataType);

      const reconciledCol: Record<string, unknown> = {
        ...structuredClone(existing),
        name: target.name,
        dataType: target.dataType,
        nullable: target.nullable ?? true,
        description: target.description ?? (typeof existing.description === "string" ? existing.description : ""),
      };

      if (target.transform) {
        reconciledCol.transform = target.transform;
      }

      reconciledColumns.push(reconciledCol);

      reconciliation.matched.push({
        name: target.name,
        sourceDataType,
        targetDataType: target.dataType,
        typeChanged,
      });

    } else {
      // New column — no predecessor match
      const needsTransform = !target.transform;

      reconciledColumns.push({
        name: target.name,
        dataType: target.dataType,
        nullable: target.nullable ?? true,
        description: target.description ?? "",
        ...(target.transform ? { transform: target.transform } : {}),
      });

      reconciliation.added.push({
        name: target.name,
        dataType: target.dataType,
        needsTransform,
      });
    }
  }

  // Derive typeChanges from matched entries to avoid dual-update maintenance
  reconciliation.typeChanges = reconciliation.matched
    .filter((m) => m.typeChanged && m.sourceDataType !== null)
    .map((m) => ({ name: m.name, from: m.sourceDataType!, to: m.targetDataType }));

  return { columns: reconciledColumns, reconciliation };
}


/**
 * Valid metadata fields for the Coalesce PUT API.
 * The GET response may include additional read-only fields (e.g., appliedNodeTests,
 * cteString, materializationOption) that the PUT schema rejects as additional properties.
 */
const VALID_METADATA_FIELDS = new Set([
  "columns",
  "sourceMapping",
  "enabledColumnTestIDs",
]);

function cleanMetadata(metadata: unknown): Record<string, unknown> {
  if (!isPlainObject(metadata)) {
    return {};
  }

  const cleaned: Record<string, unknown> = {};
  for (const key of Object.keys(metadata)) {
    if (VALID_METADATA_FIELDS.has(key)) {
      cleaned[key] = metadata[key];
    }
  }
  return cleaned;
}

/**
 * Preserves columnID, sources, columnReference, and placement from existing
 * columns when new columns are provided by the agent.
 *
 * When agents provide columns in changes.metadata.columns, those columns
 * typically lack the source linkage fields that Coalesce auto-populates
 * from predecessors. This function restores them by matching column names.
 */
function preserveColumnLinkage(
  currentMetadata: Record<string, unknown>,
  mergedMetadata: Record<string, unknown>
): void {
  if (!Array.isArray(currentMetadata.columns) || !Array.isArray(mergedMetadata.columns)) {
    return;
  }

  const existingByName = new Map<string, Record<string, unknown>>();
  for (const col of currentMetadata.columns) {
    if (isPlainObject(col) && typeof col.name === "string") {
      existingByName.set(normalizeColumnName(col.name), col);
    }
  }

  if (existingByName.size === 0) {
    return;
  }

  for (const col of mergedMetadata.columns) {
    if (!isPlainObject(col) || typeof col.name !== "string") {
      continue;
    }
    const existing = existingByName.get(normalizeColumnName(col.name));
    if (!existing) {
      continue;
    }

    // Preserve columnID if not already set
    if ((!col.columnID || (typeof col.columnID === "string" && col.columnID.length === 0))
        && typeof existing.columnID === "string") {
      col.columnID = existing.columnID;
    }

    // Preserve source linkage fields — these connect the column back to its
    // predecessor node. Agents almost never provide these, so inherit from
    // the auto-populated column.
    if (!Array.isArray(col.sources) && Array.isArray(existing.sources)) {
      const clonedSources = structuredClone(existing.sources) as unknown[];
      col.sources = clonedSources;
      // If the agent provided a non-passthrough transform on the top-level column
      // (e.g., UPPER("TABLE"."COL")), propagate it into sources[*].transform so
      // the Coalesce UI displays it correctly.
      // The UI reads transforms from sources[0].transform, not a top-level transform field.
      if (
        typeof col.transform === "string" &&
        col.transform.trim().length > 0 &&
        typeof col.name === "string" &&
        !isPassthroughTransform(col.transform, col.name)
      ) {
        for (const source of clonedSources) {
          if (isPlainObject(source)) {
            source.transform = col.transform;
          }
        }
      }
    }
    if (!isPlainObject(col.columnReference) && isPlainObject(existing.columnReference)) {
      col.columnReference = existing.columnReference;
    }

    // Preserve placement (column ordering metadata)
    if (col.placement === undefined && existing.placement !== undefined) {
      col.placement = existing.placement;
    }
  }
}

/**
 * Ensures the Coalesce API required fields (table, overrideSQL) are present
 * in the body being sent to the PUT endpoint. These are preserved from the
 * current node — the agent is never allowed to set overrideSQL.
 */
function ensureRequiredApiFields(
  current: Record<string, unknown>,
  body: Record<string, unknown>
): void {
  // Ensure 'table' is present — required by the Coalesce PUT API.
  // Prefer current node's value, fall back to node name.
  if (!body.table || (typeof body.table === "string" && body.table.trim().length === 0)) {
    if (current.table && typeof current.table === "string" && current.table.trim().length > 0) {
      body.table = current.table;
    } else {
      const name = typeof body.name === "string" ? body.name : typeof current.name === "string" ? current.name : "";
      if (name.length > 0) {
        body.table = name;
      }
    }
  }

  // Preserve 'overrideSQL' from current node — agent must never set this
  if ("overrideSQL" in current) {
    body.overrideSQL = current.overrideSQL;
  }

  // Preserve columnIDs from current node's columns
  const currentMetadata = isPlainObject(current.metadata) ? current.metadata : undefined;
  const bodyMetadata = isPlainObject(body.metadata) ? body.metadata : undefined;
  // Strip backslash-escaped quotes from transforms BEFORE preserveColumnLinkage,
  // so cleaned transforms get propagated into sources[*].transform correctly.
  if (bodyMetadata && Array.isArray(bodyMetadata.columns)) {
    for (const col of bodyMetadata.columns) {
      if (isPlainObject(col) && typeof col.transform === "string" && col.transform.includes("\\")) {
        col.transform = col.transform.replace(/\\"/g, '"');
      }
    }
  }
  if (currentMetadata && bodyMetadata) {
    preserveColumnLinkage(currentMetadata, bodyMetadata);
  }

  // Ensure all columns have required fields and strip invalid properties
  if (bodyMetadata && Array.isArray(bodyMetadata.columns)) {
    // Build lookup of current node's columns for dataType fallback
    const currentColumns = currentMetadata && Array.isArray(currentMetadata.columns) ? currentMetadata.columns : [];
    const currentDataTypes = new Map<string, string>();
    for (const col of currentColumns) {
      if (isPlainObject(col) && typeof col.name === "string" && typeof col.dataType === "string") {
        currentDataTypes.set(col.name.toUpperCase(), col.dataType);
      }
    }

    for (const col of bodyMetadata.columns) {
      if (!isPlainObject(col)) continue;
      // Ensure 'dataType' — required by the Coalesce PUT API.
      // Prefer the current node's dataType for the same column name, otherwise default to "VARCHAR".
      if (!col.dataType || (typeof col.dataType === "string" && col.dataType.trim().length === 0)) {
        const colName = typeof col.name === "string" ? col.name.toUpperCase() : "";
        const existingType = currentDataTypes.get(colName);
        col.dataType = existingType ?? "VARCHAR";
      }
      // Ensure 'nullable' — required by the Coalesce PUT API
      if (!("nullable" in col)) {
        col.nullable = true;
      }
      // Ensure 'description' — required by the Coalesce PUT API
      if (!("description" in col)) {
        col.description = "";
      }
      // Generate columnID for columns that don't have one —
      // the PUT API requires columnID on every column
      if (!col.columnID || (typeof col.columnID === "string" && col.columnID.length === 0)) {
        col.columnID = randomUUID();
      }
      // Strip passthrough transforms — if the transform just references
      // the column's own name (e.g., "ALIAS"."COL" or {{ ref(...) }}."COL"),
      // remove it so Coalesce auto-populates the source mapping.
      if (typeof col.transform === "string" && typeof col.name === "string") {
        if (isPassthroughTransform(col.transform, col.name)) {
          delete col.transform;
        }
      }
      // Ensure computed columns (new columns with a transform but no sources) get a
      // synthetic sources entry so the Coalesce UI displays the transform correctly.
      // The UI reads transforms from sources[0].transform, not the top-level transform field.
      if (
        typeof col.transform === "string" &&
        col.transform.trim().length > 0 &&
        !Array.isArray(col.sources)
      ) {
        col.sources = [{ transform: col.transform, columnReferences: [] }];
      }
      // Strip properties that are not valid Coalesce column fields.
      // Valid: name, dataType, transform, nullable, description, columnID,
      // sources, placement, plus columnSelector attributes (isBusinessKey, etc.)
      delete col.primaryKey;
      delete col.foreignKey;
      delete col.unique;
      delete col.index;
    }
  }

  // Ensure 'enabledColumnTestIDs' is present on metadata — required by the Coalesce PUT API
  if (bodyMetadata && !Array.isArray(bodyMetadata.enabledColumnTestIDs)) {
    const currentEnabled = currentMetadata && Array.isArray(currentMetadata.enabledColumnTestIDs)
      ? currentMetadata.enabledColumnTestIDs
      : [];
    bodyMetadata.enabledColumnTestIDs = currentEnabled;
  }
}

/**
 * Validates that nodeType and materializationType are compatible
 * Throws error with actionable message if incompatible
 */
function validateNodeTypeMaterializationCompatibility(
  nodeType: unknown,
  materializationType: unknown
): void {
  if (typeof nodeType !== "string" || typeof materializationType !== "string") {
    return; // Skip validation if either field is missing or invalid type
  }

  const normalizedNodeType = nodeType.toLowerCase();
  const normalizedMaterialization = materializationType.toLowerCase();

  // View nodes can ONLY be materialized as views
  if (normalizedNodeType === "view" && normalizedMaterialization === "table") {
    throw new Error(
      `Invalid configuration: nodeType "View" cannot use materializationType "table". ` +
      `Either keep materializationType as "view" OR change nodeType to a table-capable type like "Dimension", "Fact", "Stage", or "Work".`
    );
  }

  // Note: Other node types (Dimension, Fact, Stage, Work, etc.) can be either table or view
  // So we only need to check the View + table combination
}


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


function suggestNamingConvention(family: string): string {
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

function buildPostCreationNextSteps(
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
  if (completion.configCompletionSkipped) {
    const hasChanges = (params.changes && Object.keys(params.changes).length > 0) || params.columns;
    if (hasChanges) {
      try {
        resultNode = await getWorkspaceNode(client, { workspaceID: params.workspaceID, nodeID: created.id });
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        process.stderr.write(`[createWorkspaceNodeFromPredecessor] Re-fetch after config completion skip failed: ${reason}\n`);
        resultNode = createdNode;
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
  };
}

export async function convertJoinToAggregation(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    groupByColumns: string[];
    aggregates: Array<{
      name: string;
      function: string;
      expression: string;
      description?: string;
    }>;
    joinType?: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN";
    maintainJoins?: boolean;
    repoPath?: string;
  }
): Promise<{
  node: unknown;
  joinSQL: {
    fromClause: string;
    joinClauses: string[];
    fullSQL: string;
    warnings: string[];
  };
  groupByAnalysis: GroupByAnalysis;
  validation: {
    valid: boolean;
    warnings: string[];
  };
  configCompletion?: ConfigCompletionResult;
  configCompletionSkipped?: string;
}> {
  // Get the current node to analyze predecessors
  const current = await getWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
  });

  if (!isPlainObject(current)) {
    throw new Error("Node response was not an object");
  }

  const metadata = isPlainObject(current.metadata) ? current.metadata : {};
  const predecessorNodeIDs = extractPredecessorNodeIDs(metadata);

  // Fetch predecessor nodes to build join suggestions and ref info
  const predecessorNodes: PredecessorSummary[] = [];
  const predecessorRefInfos: PredecessorRefInfo[] = [];
  const shouldMaintainJoins = params.maintainJoins !== false; // default true
  if (shouldMaintainJoins && predecessorNodeIDs.length > 0) {
    const fetched = await Promise.all(
      predecessorNodeIDs.map(async (nodeID) => ({
        nodeID,
        node: await getWorkspaceNode(client, { workspaceID: params.workspaceID, nodeID }),
      }))
    );
    for (const { nodeID, node: predecessor } of fetched) {
      if (isPlainObject(predecessor)) {
        predecessorNodes.push(buildPredecessorSummary(nodeID, predecessor));
        const refInfo = extractPredecessorRefInfo(nodeID, predecessor);
        if (refInfo) {
          predecessorRefInfos.push(refInfo);
        }
      }
    }
  }

  // Generate JOIN SQL with {{ ref() }} syntax if maintaining joins
  const joinSuggestions = buildJoinSuggestions(predecessorNodes);
  let joinSQL: { fromClause: string; joinClauses: string[]; fullSQL: string; warnings: string[] };
  if (predecessorRefInfos.length >= 2) {
    joinSQL = generateRefJoinSQL(
      predecessorRefInfos,
      joinSuggestions,
      params.joinType || "INNER JOIN"
    );
  } else {
    // Fallback: predecessors missing locationName — use bare-name join SQL
    const bareJoin = generateJoinSQL(joinSuggestions, params.joinType || "INNER JOIN");
    joinSQL = {
      fromClause: bareJoin.fromClause,
      joinClauses: bareJoin.joinClauses.map((jc) =>
        `${jc.type} ${jc.rightTableAlias}\n  ON ${jc.onConditions.join("\n  AND ")}`
      ),
      fullSQL: bareJoin.fullSQL,
      warnings: predecessorNodeIDs.length >= 2
        ? ["Predecessors are missing locationName — generated join uses bare table names instead of {{ ref() }} syntax. Set locationName on predecessor nodes for proper Coalesce references."]
        : [],
    };
  }

  // Build a lookup map of existing column datatypes from the current node
  // so GROUP BY pass-through columns can inherit their predecessor's dataType
  const existingColumns = Array.isArray(metadata.columns) ? metadata.columns : [];
  const existingDataTypeByName = new Map<string, string>();
  for (const col of existingColumns) {
    if (isPlainObject(col) && typeof col.name === "string" && typeof col.dataType === "string") {
      existingDataTypeByName.set(normalizeColumnName(col.name), col.dataType);
    }
  }

  // Build columns: group by columns + aggregates
  const columns: ColumnTransform[] = [];

  // Add GROUP BY columns
  for (const groupByCol of params.groupByColumns) {
    const colName = groupByCol.split(".").pop()?.replace(/"/g, "") || groupByCol;
    const inferredDatatype = inferDatatype(groupByCol)
      ?? existingDataTypeByName.get(normalizeColumnName(colName))
      ?? "VARCHAR";
    columns.push({
      name: colName,
      transform: groupByCol,
      dataType: inferredDatatype,
    });
  }

  // Add aggregate columns
  for (const agg of params.aggregates) {
    const transform = `${agg.function}(${agg.expression})`;
    const inferredDatatype = inferDatatype(transform) ?? "VARCHAR";
    columns.push({
      name: agg.name,
      transform,
      dataType: inferredDatatype,
      description: agg.description,
    });
  }

  // Analyze GROUP BY requirements
  const groupByAnalysis = analyzeColumnsForGroupBy(columns);

  const warnings: string[] = [];
  if (!groupByAnalysis.validation.valid) {
    warnings.push(...groupByAnalysis.validation.errors);
  }

  // Derive business key and change tracking column names
  // Business key = GROUP BY columns (dimensions)
  // Change tracking = aggregate columns (measures that change over time)
  const businessKeyColumnNames = new Set(
    params.groupByColumns.map(
      (col) => col.split(".").pop()?.replace(/"/g, "") || col
    )
  );
  const changeTrackingColumnNames = new Set(
    params.aggregates.map((agg) => agg.name)
  );

  // Convert columns to metadata format with column-level attributes
  // columnSelector attributes (isBusinessKey, isChangeTracking) are set directly
  // on each column object — this is how Coalesce node type definitions work
  const metadataColumns = columns.map((col) => {
    const metadataCol: Record<string, unknown> = {
      name: col.name,
      dataType: col.dataType ?? "VARCHAR",
      transform: col.transform,
      nullable: true,
    };
    if (col.description) {
      metadataCol.description = col.description;
    }
    if (businessKeyColumnNames.has(col.name)) {
      metadataCol.isBusinessKey = true;
    }
    if (changeTrackingColumnNames.has(col.name)) {
      metadataCol.isChangeTracking = true;
    }
    return metadataCol;
  });

  // Replace columns with aggregation columns
  const updated = await replaceWorkspaceNodeColumns(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
    columns: metadataColumns,
  });

  // Write the generated JOIN SQL and/or GROUP BY directly to the node's sourceMapping.
  // Re-fetch the node to get fresh sourceMapping after column replacement.
  const hasJoinSQL = joinSQL.fullSQL.length > 0;
  const hasGroupBy = groupByAnalysis.groupByColumns.length > 0;

  if (hasJoinSQL || hasGroupBy) {
    const freshNode = await getWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: params.nodeID,
    });
    const freshMetadata = isPlainObject(freshNode) && isPlainObject(freshNode.metadata)
      ? freshNode.metadata
      : {};
    const freshSourceMapping = Array.isArray(freshMetadata.sourceMapping)
      ? freshMetadata.sourceMapping
      : [];
    const firstEntry = freshSourceMapping.find(isPlainObject);
    if (!firstEntry) {
      joinSQL.warnings.push(
        "Could not write joinCondition — node has no sourceMapping entries. " +
        "The generated SQL is returned but was not persisted to the node."
      );
    } else {
      const existingJoin = isPlainObject(firstEntry.join) ? firstEntry.join : {};
      const existingJoinCondition = typeof existingJoin.joinCondition === "string"
        ? existingJoin.joinCondition.trim()
        : "";

      let fullJoinCondition: string;
      if (hasJoinSQL) {
        // Multi-predecessor: use generated FROM/JOIN + GROUP BY
        const groupByClause = hasGroupBy
          ? `\nGROUP BY ${groupByAnalysis.groupByColumns.join(", ")}`
          : "";
        fullJoinCondition = joinSQL.fullSQL + groupByClause;
      } else {
        // Single-predecessor aggregation: append GROUP BY to existing joinCondition
        const groupByClause = `\nGROUP BY ${groupByAnalysis.groupByColumns.join(", ")}`;
        fullJoinCondition = existingJoinCondition.length > 0
          ? existingJoinCondition + groupByClause
          : groupByClause.trim();
      }

      const updatedSourceMapping = freshSourceMapping.map((entry) =>
        entry === firstEntry
          ? { ...firstEntry, join: { ...existingJoin, joinCondition: fullJoinCondition } }
          : entry
      );
      await updateWorkspaceNode(client, {
        workspaceID: params.workspaceID,
        nodeID: params.nodeID,
        changes: {
          metadata: {
            sourceMapping: updatedSourceMapping,
          },
        },
      });
    }
  }

  // Complete configuration with intelligent rules (best-effort)
  const completion = await tryCompleteNodeConfiguration(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
    repoPath: params.repoPath,
  });

  return {
    node: completion.configCompletion?.node ?? updated,
    joinSQL,
    groupByAnalysis,
    validation: {
      valid: groupByAnalysis.validation.valid && warnings.length === 0,
      warnings,
    },
    ...completion,
  };
}

export async function applyJoinCondition(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    joinType?: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN";
    whereClause?: string;
    qualifyClause?: string;
    joinColumnOverrides?: Array<{
      leftPredecessor: string;
      rightPredecessor: string;
      leftColumn: string;
      rightColumn: string;
    }>;
  }
): Promise<{
  joinCondition: string;
  joinSuggestions: JoinSuggestion[];
  predecessors: Array<{ nodeID: string; nodeName: string; locationName: string; columnCount: number }>;
  warnings: string[];
}> {
  const joinType = params.joinType ?? "INNER JOIN";

  // Fetch the node to extract predecessors
  const current = await getWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
  });
  if (!isPlainObject(current)) {
    throw new Error("Node response was not an object");
  }

  const metadata = isPlainObject(current.metadata) ? current.metadata : {};
  const sourceMapping = Array.isArray(metadata.sourceMapping) ? metadata.sourceMapping : [];
  const predecessorNodeIDs = extractPredecessorNodeIDs(metadata);

  if (predecessorNodeIDs.length < 2) {
    throw new Error(
      "apply_join_condition requires a node with 2+ predecessors. " +
      "This node has " + predecessorNodeIDs.length + " predecessor(s). " +
      "For single-predecessor nodes, set the joinCondition directly via update_workspace_node."
    );
  }

  // Fetch all predecessors in parallel to get their names, locationNames, and columns
  const predecessorRefInfos: PredecessorRefInfo[] = [];
  const predecessorSummaries: PredecessorSummary[] = [];
  const warnings: string[] = [];

  const fetchedPredecessors = await Promise.all(
    predecessorNodeIDs.map(async (nodeID) => ({
      nodeID,
      node: await getWorkspaceNode(client, { workspaceID: params.workspaceID, nodeID }),
    }))
  );

  for (const { nodeID, node: predecessor } of fetchedPredecessors) {
    if (!isPlainObject(predecessor)) {
      warnings.push(`Could not fetch predecessor ${nodeID}`);
      continue;
    }
    const refInfo = extractPredecessorRefInfo(nodeID, predecessor);
    if (!refInfo) {
      const name = typeof predecessor.name === "string" ? predecessor.name : nodeID;
      warnings.push(
        `Predecessor "${name}" is missing locationName. ` +
        `Set it in the Coalesce UI or via update_workspace_node before applying joins.`
      );
      continue;
    }
    predecessorRefInfos.push(refInfo);
    predecessorSummaries.push(buildPredecessorSummary(nodeID, predecessor));
  }

  if (predecessorRefInfos.length < 2) {
    throw new Error(
      "Could not resolve 2+ predecessors with valid name and locationName. " +
      "Ensure all predecessor nodes have a locationName set."
    );
  }

  // Build join suggestions from common columns
  const joinSuggestions = buildJoinSuggestions(predecessorSummaries);

  // Generate FROM/JOIN/ON with {{ ref() }} syntax
  const joinResult = generateRefJoinSQL(
    predecessorRefInfos,
    joinSuggestions,
    joinType,
    params.joinColumnOverrides
  );
  warnings.push(...joinResult.warnings);

  // Build full joinCondition: FROM/JOIN + WHERE + QUALIFY
  const parts = [joinResult.fullSQL];
  if (params.whereClause) {
    const trimmedWhere = params.whereClause.trim();
    const whereStr = /^where\b/i.test(trimmedWhere)
      ? trimmedWhere
      : `WHERE ${trimmedWhere}`;
    parts.push(whereStr);
  }
  if (params.qualifyClause) {
    const trimmedQualify = params.qualifyClause.trim();
    const qualifyStr = /^qualify\b/i.test(trimmedQualify)
      ? trimmedQualify
      : `QUALIFY ${trimmedQualify}`;
    parts.push(qualifyStr);
  }

  const fullJoinCondition = parts.join("\n");

  // Write to node's sourceMapping
  const firstEntry = sourceMapping.find(isPlainObject);
  if (!firstEntry) {
    warnings.push(
      "Could not write joinCondition — node has no sourceMapping entries. " +
      "The generated SQL is returned but was not persisted to the node."
    );
  } else {
    const updatedSourceMapping = sourceMapping.map((entry) =>
      entry === firstEntry
        ? {
            ...firstEntry,
            join: {
              ...(isPlainObject(firstEntry.join) ? firstEntry.join : {}),
              joinCondition: fullJoinCondition,
            },
          }
        : entry
    );
    await updateWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: params.nodeID,
      changes: {
        metadata: {
          sourceMapping: updatedSourceMapping,
        },
      },
    });
  }

  return {
    joinCondition: fullJoinCondition,
    joinSuggestions,
    predecessors: predecessorRefInfos.map((p) => ({
      nodeID: p.nodeID,
      nodeName: p.nodeName,
      locationName: p.locationName,
      columnCount: p.columnNames.length,
    })),
    warnings,
  };
}

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
