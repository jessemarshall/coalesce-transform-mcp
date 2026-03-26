import { randomUUID } from "node:crypto";
import type { CoalesceClient } from "../../client.js";
import {
  getWorkspaceNode,
  setWorkspaceNode,
  createWorkspaceNode,
} from "../../coalesce/api/nodes.js";
import { fetchAllWorkspaceNodes } from "../cache/snapshots.js";
import { assertNoSqlOverridePayload } from "../policies/sql-override.js";
import { completeNodeConfiguration, type ConfigCompletionResult } from "../config/intelligent.js";
import { isPlainObject, uniqueInOrder } from "../../utils.js";
import { selectPipelineNodeType } from "../pipelines/node-type-selection.js";
import { detectSpecializedPatternPenalty } from "../pipelines/node-type-intent.js";

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
        `Call plan-pipeline first to discover and rank all available node types.`;
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
        `Call plan-pipeline to discover the correct nodeType for your use case.`
      );
    }

    return validation;
  } catch (error) {
    // Re-throw hard block errors — exclusion and specialized pattern blocks
    if (error instanceof Error && (
      error.message.includes("is excluded") ||
      error.message.startsWith("Cannot create node")
    )) {
      throw error;
    }
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
      `Call plan-pipeline to discover the correct nodeType.`
    );
  }
}

type PredecessorSummary = {
  nodeID: string;
  nodeName: string | null;
  columnCount: number;
  columnNames: string[];
};

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

function getNodeColumnCount(node: Record<string, unknown>): number {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  return Array.isArray(metadata?.columns) ? metadata.columns.length : 0;
}

function getNodeStorageLocationCount(node: Record<string, unknown>): number {
  return Array.isArray(node.storageLocations) ? node.storageLocations.length : 0;
}

function getNodeConfigKeyCount(node: Record<string, unknown>): number {
  return isPlainObject(node.config) ? Object.keys(node.config).length : 0;
}

function getRequestedNodeName(changes: Record<string, unknown>): string | undefined {
  return typeof changes.name === "string" && changes.name.trim().length > 0
    ? changes.name
    : undefined;
}

function getRequestedColumnNames(changes: Record<string, unknown>): string[] {
  const metadata = isPlainObject(changes.metadata) ? changes.metadata : undefined;
  if (!metadata || !Array.isArray(metadata.columns)) {
    return [];
  }

  const names: string[] = [];
  for (const column of metadata.columns) {
    if (isPlainObject(column) && typeof column.name === "string" && column.name.trim().length > 0) {
      names.push(column.name);
    }
  }
  return names;
}

function getRequestedConfig(changes: Record<string, unknown>): Record<string, unknown> | undefined {
  return isPlainObject(changes.config) ? changes.config : undefined;
}

function getRequestedLocationFields(
  changes: Record<string, unknown>
): Record<string, unknown> {
  const requested: Record<string, unknown> = {};
  for (const key of ["database", "schema", "locationName"]) {
    if (Object.prototype.hasOwnProperty.call(changes, key)) {
      requested[key] = changes[key];
    }
  }
  return requested;
}

function getNodeColumnNames(node: Record<string, unknown>): string[] {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!Array.isArray(metadata?.columns)) {
    return [];
  }

  return metadata.columns.flatMap((column) => {
    if (!isPlainObject(column) || typeof column.name !== "string") {
      return [];
    }
    return [column.name];
  });
}

function getNodeDependencyNames(node: Record<string, unknown>): string[] {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!Array.isArray(metadata?.sourceMapping)) {
    return [];
  }

  return metadata.sourceMapping.flatMap((mapping) => {
    if (!isPlainObject(mapping) || !Array.isArray(mapping.dependencies)) {
      return [];
    }

    return mapping.dependencies.flatMap((dependency) => {
      if (!isPlainObject(dependency) || typeof dependency.nodeName !== "string") {
        return [];
      }
      return [dependency.nodeName];
    });
  });
}

function getReferencedPredecessorNodeIDs(
  node: Record<string, unknown>,
  predecessorNodeIDs: string[]
): string[] {
  const uniquePredecessorNodeIDs = uniqueInOrder(predecessorNodeIDs);
  const predecessorSet = new Set(uniquePredecessorNodeIDs);
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!Array.isArray(metadata?.columns)) {
    return [];
  }

  const referenced = new Set<string>();
  for (const column of metadata.columns) {
    if (!isPlainObject(column) || !Array.isArray(column.sources)) {
      continue;
    }
    for (const source of column.sources) {
      if (!isPlainObject(source) || !Array.isArray(source.columnReferences)) {
        continue;
      }
      for (const ref of source.columnReferences) {
        if (isPlainObject(ref) && typeof ref.nodeID === "string" && predecessorSet.has(ref.nodeID)) {
          referenced.add(ref.nodeID);
        }
      }
    }
  }

  return uniquePredecessorNodeIDs.filter((nodeID) => referenced.has(nodeID));
}

function normalizeColumnName(name: string): string {
  return name.trim().toUpperCase();
}

function buildPredecessorSummary(
  requestedNodeID: string,
  node: Record<string, unknown>
): PredecessorSummary {
  return {
    nodeID: requestedNodeID,
    nodeName: typeof node.name === "string" ? node.name : null,
    columnCount: getNodeColumnCount(node),
    columnNames: getNodeColumnNames(node),
  };
}

type JoinColumnSuggestion = {
  normalizedName: string;
  leftColumnName: string;
  rightColumnName: string;
};

type JoinSuggestion = {
  leftPredecessorNodeID: string;
  leftPredecessorName: string | null;
  rightPredecessorNodeID: string;
  rightPredecessorName: string | null;
  commonColumns: JoinColumnSuggestion[];
};

type JoinClause = {
  type: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN";
  rightTable: string;
  rightTableAlias: string;
  onConditions: string[];
};

type GroupByAnalysis = {
  groupByColumns: string[];
  aggregateColumns: { name: string; transform: string }[];
  hasAggregates: boolean;
  groupByClause: string;
  validation: {
    valid: boolean;
    errors: string[];
  };
};

type ColumnTransform = {
  name: string;
  transform: string;
  dataType?: string;
  description?: string;
};

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
 * Detects whether a column transform is just a passthrough — i.e., it only
 * references the column's own name without any actual transformation.
 *
 * Passthrough patterns:
 *   "ALIAS"."COLUMN_NAME"
 *   {{ ref('NODE', 'SOURCE') }}."COLUMN_NAME"
 *   COLUMN_NAME (bare name)
 */
function isPassthroughTransform(transform: string, columnName: string): boolean {
  const trimmed = transform.trim();
  if (trimmed.length === 0) return true;

  const upperName = columnName.trim().toUpperCase();
  const upperTransform = trimmed.toUpperCase();

  // Bare column name: COLUMN_NAME
  if (upperTransform === upperName) return true;

  // Quoted bare name: "COLUMN_NAME"
  if (upperTransform === `"${upperName}"`) return true;

  // "ALIAS"."COLUMN_NAME" — any single-segment alias
  const aliasColPattern = /^"[^"]+"\s*\.\s*"([^"]+)"$/i;
  const aliasMatch = trimmed.match(aliasColPattern);
  if (aliasMatch && aliasMatch[1].toUpperCase() === upperName) return true;

  // {{ ref(...) }}."COLUMN_NAME"
  const refPattern = /^\{\{\s*ref\s*\([^)]*\)\s*\}\}\s*\.\s*"([^"]+)"$/i;
  const refMatch = trimmed.match(refPattern);
  if (refMatch && refMatch[1].toUpperCase() === upperName) return true;

  return false;
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

function buildJoinSuggestions(
  predecessors: PredecessorSummary[]
): JoinSuggestion[] {
  const suggestions: JoinSuggestion[] = [];

  for (let leftIndex = 0; leftIndex < predecessors.length; leftIndex += 1) {
    for (
      let rightIndex = leftIndex + 1;
      rightIndex < predecessors.length;
      rightIndex += 1
    ) {
      const left = predecessors[leftIndex];
      const right = predecessors[rightIndex];

      const leftColumns = new Map<string, string>();
      for (const columnName of left.columnNames) {
        const normalized = normalizeColumnName(columnName);
        if (!leftColumns.has(normalized)) {
          leftColumns.set(normalized, columnName);
        }
      }

      const rightColumns = new Map<string, string>();
      for (const columnName of right.columnNames) {
        const normalized = normalizeColumnName(columnName);
        if (!rightColumns.has(normalized)) {
          rightColumns.set(normalized, columnName);
        }
      }

      const commonColumns: JoinColumnSuggestion[] = [];
      for (const [normalizedName, leftColumnName] of leftColumns.entries()) {
        const rightColumnName = rightColumns.get(normalizedName);
        if (rightColumnName) {
          commonColumns.push({
            normalizedName,
            leftColumnName,
            rightColumnName,
          });
        }
      }

      commonColumns.sort((a, b) =>
        a.normalizedName.localeCompare(b.normalizedName)
      );

      suggestions.push({
        leftPredecessorNodeID: left.nodeID,
        leftPredecessorName: left.nodeName,
        rightPredecessorNodeID: right.nodeID,
        rightPredecessorName: right.nodeName,
        commonColumns,
      });
    }
  }

  return suggestions;
}

function generateJoinSQL(
  joinSuggestions: JoinSuggestion[],
  joinType: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN" = "INNER JOIN"
): {
  fromClause: string;
  joinClauses: JoinClause[];
  fullSQL: string;
} {
  if (joinSuggestions.length === 0) {
    return {
      fromClause: "",
      joinClauses: [],
      fullSQL: "",
    };
  }

  const firstSuggestion = joinSuggestions[0];
  const leftTableName = firstSuggestion.leftPredecessorName || "LEFT_TABLE";
  const leftAlias = `"${leftTableName}"`;

  const fromClause = `FROM ${leftAlias}`;
  const joinClauses: JoinClause[] = [];
  const sqlParts: string[] = [fromClause];

  for (const suggestion of joinSuggestions) {
    const rightTableName = suggestion.rightPredecessorName || "RIGHT_TABLE";
    const rightAlias = `"${rightTableName}"`;

    const onConditions = suggestion.commonColumns.map(
      (col) =>
        `${leftAlias}."${col.leftColumnName}" = ${rightAlias}."${col.rightColumnName}"`
    );

    const joinClause: JoinClause = {
      type: joinType,
      rightTable: rightTableName,
      rightTableAlias: rightAlias,
      onConditions,
    };

    joinClauses.push(joinClause);

    const joinSQL = [
      `${joinType} ${rightAlias}`,
      `  ON ${onConditions.join("\n  AND ")}`,
    ].join("\n");

    sqlParts.push(joinSQL);
  }

  return {
    fromClause,
    joinClauses,
    fullSQL: sqlParts.join("\n"),
  };
}

function inferDatatype(transform: string): string | undefined {
  const upperTransform = transform.toUpperCase();

  // Date/Time functions - check these FIRST before MIN/MAX
  if (upperTransform.includes("DATEDIFF(")) return "NUMBER";
  if (upperTransform.includes("DATEADD(")) return "DATE";
  if (upperTransform.includes("CURRENT_DATE")) return "DATE";
  if (upperTransform.includes("CURRENT_TIMESTAMP")) return "TIMESTAMP_NTZ(9)";

  // Aggregate functions
  if (upperTransform.includes("COUNT(DISTINCT")) return "NUMBER";
  if (upperTransform.includes("COUNT(")) return "NUMBER";
  if (upperTransform.includes("SUM(")) return "NUMBER(38,4)";
  if (upperTransform.includes("AVG(")) return "NUMBER(38,4)";
  if (upperTransform.includes("STDDEV(")) return "NUMBER(38,4)";
  if (upperTransform.includes("VARIANCE(")) return "NUMBER(38,4)";

  // MIN/MAX with timestamp/date context
  if (upperTransform.includes("MIN(") && upperTransform.includes("_TS"))
    return "TIMESTAMP_NTZ(9)";
  if (upperTransform.includes("MAX(") && upperTransform.includes("_TS"))
    return "TIMESTAMP_NTZ(9)";
  if (upperTransform.includes("MIN(") && upperTransform.includes("_DATE"))
    return "DATE";
  if (upperTransform.includes("MAX(") && upperTransform.includes("_DATE"))
    return "DATE";

  // String functions
  if (upperTransform.includes("CONCAT(")) return "VARCHAR";
  if (upperTransform.includes("UPPER(")) return "VARCHAR";
  if (upperTransform.includes("LOWER(")) return "VARCHAR";
  if (upperTransform.includes("TRIM(")) return "VARCHAR";
  if (upperTransform.includes("SUBSTR(")) return "VARCHAR";
  if (upperTransform.includes("LEFT(")) return "VARCHAR";
  if (upperTransform.includes("RIGHT(")) return "VARCHAR";

  // Boolean
  if (upperTransform.includes("CASE")) return "VARCHAR";

  // Window functions
  if (upperTransform.includes("ROW_NUMBER()")) return "NUMBER";
  if (upperTransform.includes("RANK()")) return "NUMBER";
  if (upperTransform.includes("DENSE_RANK()")) return "NUMBER";

  return undefined;
}

function analyzeColumnsForGroupBy(
  columns: ColumnTransform[]
): GroupByAnalysis {
  const aggregateFunctions = [
    "COUNT(",
    "SUM(",
    "AVG(",
    "MIN(",
    "MAX(",
    "STDDEV(",
    "VARIANCE(",
    "LISTAGG(",
    "ARRAY_AGG(",
  ];

  const windowFunctions = [
    "ROW_NUMBER()",
    "RANK()",
    "DENSE_RANK()",
    "LEAD(",
    "LAG(",
    "FIRST_VALUE(",
    "LAST_VALUE(",
  ];

  const groupByColumns: string[] = [];
  const aggregateColumns: { name: string; transform: string }[] = [];
  const errors: string[] = [];

  for (const col of columns) {
    const upperTransform = col.transform.toUpperCase();

    const isAggregate = aggregateFunctions.some((fn) =>
      upperTransform.includes(fn)
    );
    const isWindow = windowFunctions.some((fn) => upperTransform.includes(fn));

    if (isAggregate || isWindow) {
      aggregateColumns.push({ name: col.name, transform: col.transform });
    } else {
      // This is a non-aggregate column, needs to be in GROUP BY
      groupByColumns.push(col.transform);
    }
  }

  const hasAggregates = aggregateColumns.length > 0;

  // Validation: if we have aggregates, we need GROUP BY for non-aggregate columns
  let valid = true;
  if (hasAggregates && groupByColumns.length === 0 && columns.length > 1) {
    errors.push(
      "Query has aggregate functions but no GROUP BY columns. All non-aggregate columns must be in GROUP BY."
    );
    valid = false;
  }

  const groupByClause =
    hasAggregates && groupByColumns.length > 0
      ? `GROUP BY ${groupByColumns.join(", ")}`
      : "";

  return {
    groupByColumns,
    aggregateColumns,
    hasAggregates,
    groupByClause,
    validation: {
      valid,
      errors,
    },
  };
}

export async function updateWorkspaceNode(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    changes: Record<string, unknown>;
  }
): Promise<unknown> {
  assertNoSqlOverridePayload(params.changes, "update-workspace-node changes");

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
      "replace-workspace-node-columns additionalChanges"
    );
    // Block sourceMapping in additionalChanges — use apply-join-condition or convert-join-to-aggregation instead
    const additionalMeta = isPlainObject(params.additionalChanges.metadata)
      ? params.additionalChanges.metadata
      : null;
    if (additionalMeta && ("sourceMapping" in additionalMeta || "customSQL" in additionalMeta)) {
      throw new Error(
        "replace-workspace-node-columns additionalChanges cannot set sourceMapping or customSQL. " +
        "Use the joinCondition parameter to set WHERE filters, apply-join-condition for join setup, " +
        "or convert-join-to-aggregation for GROUP BY patterns."
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

/**
 * Append a WHERE condition to the existing joinCondition in the first sourceMapping entry.
 * The FROM/JOIN clause from node creation is preserved — only the WHERE is added.
 * If no existing joinCondition exists, creates one with just the WHERE clause.
 */
function appendWhereToJoinCondition(
  body: Record<string, unknown>,
  whereCondition: string
): void {
  const metadata = isPlainObject(body.metadata) ? body.metadata : null;
  if (!metadata) return;

  const sourceMapping = Array.isArray(metadata.sourceMapping) ? metadata.sourceMapping : [];
  if (sourceMapping.length === 0) return;

  const first = sourceMapping[0];
  if (!isPlainObject(first)) return;

  const join = isPlainObject(first.join) ? { ...first.join } : {};
  const existing = typeof join.joinCondition === "string" ? join.joinCondition.trim() : "";

  // Strip backslash-escaped quotes (agents sometimes over-escape: \" → ")
  const unescaped = whereCondition.replace(/\\"/g, '"');
  // Normalize: strip leading "WHERE" if the user included it
  const cleanWhere = unescaped.replace(/^\s*WHERE\s+/i, "").trim();
  if (!cleanWhere) return;

  if (existing) {
    // Append WHERE to existing FROM/JOIN clause
    // Check if existing already has a WHERE — if so, add with AND
    if (/\bWHERE\b/i.test(existing)) {
      join.joinCondition = `${existing}\n  AND ${cleanWhere}`;
    } else {
      join.joinCondition = `${existing}\nWHERE ${cleanWhere}`;
    }
  } else {
    join.joinCondition = `WHERE ${cleanWhere}`;
  }
  first.join = join;
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
  const family = inferNodeTypeFamily(nodeType);

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
      "This node has no columns. Add columns using replace-workspace-node-columns or update-workspace-node."
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
    "Verify the node: call get-workspace-node to confirm columns and config are correct before proceeding."
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
    "create-workspace-node-from-scratch changes"
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
        `To clean up, use delete-workspace-node with nodeID "${created.id}".`
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
  try {
    const configCompletion = await completeNodeConfiguration(client, {
      workspaceID: params.workspaceID,
      nodeID: created.id,
      repoPath: params.repoPath,
    });

    return {
      node: configCompletion.node,
      validation,
      nextSteps,
      configCompletion,
      ...(nodeTypeValidation ? { nodeTypeValidation } : {}),
    };
  } catch {
    return {
      node: finalNode,
      validation,
      nextSteps,
      configCompletionSkipped: "Config completion failed — call complete-node-configuration with repoPath after creation to apply node type config and column-level attributes.",
      ...(nodeTypeValidation ? { nodeTypeValidation } : {}),
    };
  }
}

function inferNodeTypeFamily(nodeType: string): string {
  const normalized = nodeType.toLowerCase().replace(/.*:::/, "");
  if (/dimension|(?:^|[-_])dim(?:$|[-_])/.test(normalized)) return "dimension";
  if (/fact|(?:^|[-_])fct(?:$|[-_])/.test(normalized)) return "fact";
  if (/(?:^|[-_])hub(?:$|[-_])/.test(normalized)) return "hub";
  if (/satellite|(?:^|[-_])sat(?:$|[-_])/.test(normalized)) return "satellite";
  if (/(?:^|[-_])link(?:$|[-_])/.test(normalized)) return "link";
  if (/(?:^|[-_])view(?:$|[-_])/.test(normalized)) return "view";
  if (/(?:^|[-_])work(?:$|[-_])/.test(normalized)) return "work";
  if (/stage|(?:^|[-_])stg(?:$|[-_])|persistent/.test(normalized)) return "stage";
  return "stage";
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
  const family = inferNodeTypeFamily(nodeType);

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
      "\n  - Call convert-join-to-aggregation (for GROUP BY / aggregation use cases)" +
      "\n  - Call apply-join-condition (for row-level joins — auto-generates FROM/JOIN/ON with {{ ref() }} syntax)"
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
        "These grain columns become your GROUP BY columns in convert-join-to-aggregation. " +
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
    "Verify the node: call get-workspace-node to confirm columns, config, and join condition are correct before proceeding to downstream nodes."
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
      "create-workspace-node-from-predecessor changes"
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
      "For aggregation nodes, WHERE/HAVING filters should be applied via a separate update-workspace-node call."
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
  try {
    const configCompletion = await completeNodeConfiguration(client, {
      workspaceID: params.workspaceID,
      nodeID: created.id,
      repoPath: params.repoPath,
    });

    return {
      node: configCompletion.node,
      predecessors: predecessorNodes,
      joinSuggestions,
      validation,
      configCompletion,
      nextSteps,
      ...(nodeTypeValidation ? { nodeTypeValidation } : {}),
    };
  } catch {
    // Re-fetch the node after any changes were applied
    const hasChanges = (params.changes && Object.keys(params.changes).length > 0) || params.columns;
    const latestNode = hasChanges
      ? await getWorkspaceNode(client, { workspaceID: params.workspaceID, nodeID: created.id })
      : createdNode;

    return {
      node: latestNode,
      predecessors: predecessorNodes,
      joinSuggestions,
      validation,
      configCompletionSkipped: "Config completion failed — call complete-node-configuration with repoPath after creation to apply node type config and column-level attributes.",
      nextSteps,
      ...(nodeTypeValidation ? { nodeTypeValidation } : {}),
    };
  }
}

/**
 * Extract predecessor node IDs from a node's sourceMapping aliases
 * and column-level source references (fallback).
 *
 * In Coalesce, sourceMapping.dependencies[] has nodeName/locationName but NOT nodeID.
 * The nodeID is available in sourceMapping.aliases (name→nodeID map) and in
 * column sources[].columnReferences[].nodeID.
 */
function extractPredecessorNodeIDs(metadata: Record<string, unknown>): string[] {
  const sourceMapping = Array.isArray(metadata.sourceMapping)
    ? metadata.sourceMapping
    : [];

  const ids = new Set<string>();

  // First: extract from aliases (alias → nodeID map)
  for (const mapping of sourceMapping) {
    if (isPlainObject(mapping) && isPlainObject(mapping.aliases)) {
      for (const nodeID of Object.values(mapping.aliases)) {
        if (typeof nodeID === "string" && nodeID.length > 0) {
          ids.add(nodeID);
        }
      }
    }
  }

  // Second: extract from column-level source references as fallback
  if (ids.size === 0 && Array.isArray(metadata.columns)) {
    for (const column of metadata.columns) {
      if (!isPlainObject(column) || !Array.isArray(column.sources)) continue;
      for (const source of column.sources) {
        if (!isPlainObject(source) || !Array.isArray(source.columnReferences)) continue;
        for (const ref of source.columnReferences) {
          if (isPlainObject(ref) && typeof ref.nodeID === "string") {
            ids.add(ref.nodeID);
          }
        }
      }
    }
  }

  return Array.from(ids);
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
  try {
    const configCompletion = await completeNodeConfiguration(client, {
      workspaceID: params.workspaceID,
      nodeID: params.nodeID,
      repoPath: params.repoPath,
    });

    return {
      node: configCompletion.node,
      joinSQL,
      groupByAnalysis,
      validation: {
        valid: groupByAnalysis.validation.valid && warnings.length === 0,
        warnings,
      },
      configCompletion,
    };
  } catch {
    return {
      node: updated,
      joinSQL,
      groupByAnalysis,
      validation: {
        valid: groupByAnalysis.validation.valid && warnings.length === 0,
        warnings,
      },
      configCompletionSkipped: "Config completion failed — call complete-node-configuration with repoPath after creation to apply node type config and column-level attributes.",
    };
  }
}

type PredecessorRefInfo = {
  nodeID: string;
  nodeName: string;
  locationName: string;
  columnNames: string[];
};

function extractPredecessorRefInfo(
  nodeID: string,
  node: Record<string, unknown>
): PredecessorRefInfo | null {
  const nodeName = typeof node.name === "string" ? node.name : null;
  const locationName = typeof node.locationName === "string" ? node.locationName : null;
  if (!nodeName || !locationName) return null;
  return {
    nodeID,
    nodeName,
    locationName,
    columnNames: getNodeColumnNames(node),
  };
}

function generateRefJoinSQL(
  predecessors: PredecessorRefInfo[],
  joinSuggestions: JoinSuggestion[],
  joinType: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN",
  joinColumnOverrides?: Array<{
    leftPredecessor: string;
    rightPredecessor: string;
    leftColumn: string;
    rightColumn: string;
  }>
): {
  fromClause: string;
  joinClauses: string[];
  fullSQL: string;
  warnings: string[];
} {
  if (predecessors.length === 0) {
    return { fromClause: "", joinClauses: [], fullSQL: "", warnings: [] };
  }

  const warnings: string[] = [];
  const primary = predecessors[0];
  const fromClause = `FROM {{ ref('${primary.locationName}', '${primary.nodeName}') }} "${primary.nodeName}"`;
  const joinClauses: string[] = [];

  // Build a lookup from nodeID → PredecessorRefInfo
  const predByID = new Map(predecessors.map((p) => [p.nodeID, p]));
  const predByName = new Map(predecessors.map((p) => [p.nodeName.toUpperCase(), p]));

  // Track which predecessors got joined
  const joinedPredecessors = new Set<string>([primary.nodeID]);

  for (const suggestion of joinSuggestions) {
    const right = predByID.get(suggestion.rightPredecessorNodeID)
      ?? predByName.get((suggestion.rightPredecessorName ?? "").toUpperCase());

    if (!right) continue;
    if (joinedPredecessors.has(right.nodeID)) continue; // Already joined — skip duplicate pair
    joinedPredecessors.add(right.nodeID);

    // Check for explicit overrides for this pair
    const overridesForPair = joinColumnOverrides?.filter(
      (o) =>
        (o.leftPredecessor === suggestion.leftPredecessorName ||
          o.leftPredecessor === suggestion.leftPredecessorNodeID) &&
        (o.rightPredecessor === suggestion.rightPredecessorName ||
          o.rightPredecessor === suggestion.rightPredecessorNodeID)
    );

    let onConditions: string[];
    if (overridesForPair && overridesForPair.length > 0) {
      onConditions = overridesForPair.map(
        (o) => `"${suggestion.leftPredecessorName}"."${o.leftColumn}" = "${right.nodeName}"."${o.rightColumn}"`
      );
    } else if (suggestion.commonColumns.length > 0) {
      onConditions = suggestion.commonColumns.map(
        (col) =>
          `"${suggestion.leftPredecessorName}"."${col.leftColumnName}" = "${right.nodeName}"."${col.rightColumnName}"`
      );
    } else {
      warnings.push(
        `No common columns between "${suggestion.leftPredecessorName}" and "${right.nodeName}". ` +
        `Provide joinColumnOverrides to specify the join keys explicitly.`
      );
      continue;
    }

    const clause = `${joinType} {{ ref('${right.locationName}', '${right.nodeName}') }} "${right.nodeName}"\n  ON ${onConditions.join("\n  AND ")}`;
    joinClauses.push(clause);
  }

  // Warn about predecessors that weren't joined
  for (const pred of predecessors) {
    if (!joinedPredecessors.has(pred.nodeID)) {
      warnings.push(
        `Predecessor "${pred.nodeName}" was not included in any join. ` +
        `It has no common columns with other predecessors. Provide joinColumnOverrides to specify the join keys.`
      );
    }
  }

  const fullSQL = [fromClause, ...joinClauses].join("\n");
  return { fromClause, joinClauses, fullSQL, warnings };
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
      "apply-join-condition requires a node with 2+ predecessors. " +
      "This node has " + predecessorNodeIDs.length + " predecessor(s). " +
      "For single-predecessor nodes, set the joinCondition directly via update-workspace-node."
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
        `Set it in the Coalesce UI or via update-workspace-node before applying joins.`
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
