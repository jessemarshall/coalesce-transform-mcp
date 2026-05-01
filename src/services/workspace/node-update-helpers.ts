import { randomUUID } from "node:crypto";
import { type CoalesceClient } from "../../client.js";
import { completeNodeConfiguration, type ConfigCompletionResult } from "../config/intelligent.js";
import { isPlainObject, rethrowNonRecoverableApiError, safeErrorMessage } from "../../utils.js";
import { selectPipelineNodeType } from "../pipelines/node-type-selection.js";
import { detectSpecializedPatternPenalty } from "../pipelines/node-type-intent.js";
import {
  normalizeColumnName,
  normalizeDataType,
} from "./node-inspection.js";
import { isPassthroughTransform } from "../shared/node-helpers.js";

const CONFIG_COMPLETION_SKIP_MSG =
  "call complete_node_configuration with repoPath after creation to apply node type config and column-level attributes.";

export async function tryCompleteNodeConfiguration(
  client: CoalesceClient,
  params: { workspaceID: string; nodeID: string; repoPath?: string },
): Promise<{ configCompletion?: ConfigCompletionResult; configCompletionSkipped?: string }> {
  try {
    const configCompletion = await completeNodeConfiguration(client, params);
    return { configCompletion };
  } catch (error) {
    rethrowNonRecoverableApiError(error);
    const reason = safeErrorMessage(error);
    return { configCompletionSkipped: `Config completion failed (${reason}) — ${CONFIG_COMPLETION_SKIP_MSG}` };
  }
}

export type NodeTypeValidation = {
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
 *
 * NOTE: This function imports listWorkspaceNodeTypes lazily to avoid circular
 * dependencies (mutations.ts -> node-update-helpers.ts -> mutations.ts).
 */
export async function validateNodeTypeChoice(
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
    // Lazy import to break the cycle: mutations re-exports from this file,
    // and this function needs listWorkspaceNodeTypes which lives in mutations.
    const { listWorkspaceNodeTypes } = await import("./mutations.js");
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
    rethrowNonRecoverableApiError(error);
    // Re-throw hard block errors — exclusion and specialized pattern blocks
    if (error instanceof Error && (
      error.message.includes("is excluded") ||
      error.message.startsWith("Cannot create node")
    )) {
      throw error;
    }
    // Log unexpected errors so they are not completely invisible
    const reason = safeErrorMessage(error);
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
export function assertNotSourceNodeType(nodeType: string): void {
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

export type WorkspaceNodeChanges = Record<string, unknown>;

export function mergeWorkspaceNodeChanges(
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

export function syncNodeNameIntoMetadataSourceMapping(
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

export type SchemaReconciliationColumn = {
  name: string;
  sourceDataType: string | null;
  targetDataType: string;
  typeChanged: boolean;
};

export type SchemaReconciliation = {
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
export function reconcileExternalSchema(
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

  const normalizedNodeType = nodeType.trim().toLowerCase().replace(/.*:::/, "");
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
