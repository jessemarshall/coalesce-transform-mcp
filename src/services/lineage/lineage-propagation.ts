import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { CoalesceClient } from "../../client.js";
import { CACHE_DIR_NAME, getCacheBaseDir } from "../../cache-dir.js";
import type { LineageCacheEntry } from "./lineage-cache.js";
import { invalidateLineageCache } from "./lineage-cache.js";
import { walkColumnLineage } from "./lineage-traversal.js";
import { setWorkspaceNode } from "../../coalesce/api/nodes.js";
import { buildUpdatedWorkspaceNodeBody } from "../workspace/node-update-helpers.js";
import { validatePathSegment } from "../../coalesce/types.js";
import { isPlainObject } from "../../utils.js";
import type { WorkflowProgressReporter } from "../../workflows/progress.js";

export type PropagationChange = {
  columnName?: string;
  dataType?: string;
};

type ColumnUpdateInfo = {
  nodeID: string;
  nodeName: string;
  columnID: string;
  columnName: string;
  previousName?: string;
  previousDataType?: string;
};

export type PreMutationSnapshotEntry = {
  nodeID: string;
  nodeName: string;
  columnID: string;
  previousColumnName: string;
  previousDataType: string;
  capturedAt: string;
  nodeBody: Record<string, unknown>;
};

/** Inline summary returned in the tool response (no nodeBody — LLM-friendly). */
export type PreMutationSnapshotSummary = Omit<PreMutationSnapshotEntry, "nodeBody">;

export type PropagationResult = {
  sourceNodeID: string;
  sourceColumnID: string;
  changes: PropagationChange;
  /** Inline summary without nodeBody — safe for LLM context windows. */
  preMutationSnapshot: PreMutationSnapshotSummary[];
  /** Path to the disk snapshot file containing full nodeBody for reversal. */
  snapshotPath?: string;
  updatedNodes: ColumnUpdateInfo[];
  totalUpdated: number;
  errors: Array<{ nodeID: string; columnID: string; message: string }>;
  partialFailure?: boolean;
  skippedNodes?: ColumnUpdateInfo[];
  rolledBack?: boolean;
};

type PreparedNodeUpdate = {
  nodeID: string;
  nodeName: string;
  body: Record<string, unknown>;
  originalBody: Record<string, unknown>;
  affectedColumns: ColumnUpdateInfo[];
};

type PropagationError = PropagationResult["errors"][number];

function expandNodeError(
  update: PreparedNodeUpdate,
  message: string
): PropagationError[] {
  return update.affectedColumns.map((column) => ({
    nodeID: column.nodeID,
    columnID: column.columnID,
    message,
  }));
}

async function rollbackPreparedNodeUpdates(
  client: CoalesceClient,
  workspaceID: string,
  appliedUpdates: PreparedNodeUpdate[],
): Promise<PropagationError[]> {
  const rollbackErrors: PropagationError[] = [];

  for (const update of [...appliedUpdates].reverse()) {
    try {
      await setWorkspaceNode(client, {
        workspaceID,
        nodeID: validatePathSegment(update.nodeID, "nodeID"),
        body: update.originalBody,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      rollbackErrors.push(...expandNodeError(
        update,
        `Rollback failed after propagation error: ${message}`
      ));
    }
  }

  return rollbackErrors;
}

export async function propagateColumnChange(
  client: CoalesceClient,
  cache: LineageCacheEntry,
  workspaceID: string,
  nodeID: string,
  columnID: string,
  changes: PropagationChange,
  reportProgress?: WorkflowProgressReporter,
  baseDir?: string,
): Promise<PropagationResult> {
  if (!changes.columnName && !changes.dataType) {
    throw new Error("At least one change (columnName or dataType) must be specified");
  }

  const node = cache.nodes.get(nodeID);
  if (!node) {
    throw new Error(`Node ${nodeID} not found in lineage cache`);
  }

  const sourceCol = node.columns.find((c) => c.id === columnID);
  if (!sourceCol) {
    throw new Error(`Column ${columnID} not found on node ${nodeID} (${node.name})`);
  }

  // Find all downstream columns that reference this column
  const downstreamEntries = walkColumnLineage(cache, nodeID, columnID).filter(
    (e) => e.direction === "downstream"
  );

  const safeWorkspaceID = validatePathSegment(workspaceID, "workspaceID");

  async function emitProgress(label: string, itemName: string, index: number, total: number): Promise<void> {
    if (reportProgress) {
      await reportProgress(`${label} column ${itemName} (${index + 1}/${total})`, total);
    }
  }

  // --- Phase 1: Prepare all changes (read-only, no writes) ---
  const prepared: PreparedNodeUpdate[] = [];
  const prepareErrors: PropagationResult["errors"] = [];
  const snapshotTimestamp = new Date().toISOString();
  const fullSnapshot: PreMutationSnapshotEntry[] = [];
  const downstreamByNodeID = new Map<string, typeof downstreamEntries>();

  for (const entry of downstreamEntries) {
    const existing = downstreamByNodeID.get(entry.nodeID) ?? [];
    existing.push(entry);
    downstreamByNodeID.set(entry.nodeID, existing);
  }

  const downstreamNodeGroups = Array.from(downstreamByNodeID.values());
  for (let i = 0; i < downstreamNodeGroups.length; i++) {
    const nodeEntries = downstreamNodeGroups[i] ?? [];
    const firstEntry = nodeEntries[0];
    if (!firstEntry) continue;
    await emitProgress("Preparing", firstEntry.nodeName, i, downstreamNodeGroups.length);

    try {
      const currentNode = await client.get(
        `/api/v1/workspaces/${safeWorkspaceID}/nodes/${validatePathSegment(firstEntry.nodeID, "nodeID")}`,
        {}
      );

      if (!isPlainObject(currentNode) || !isPlainObject(currentNode.metadata)) {
        for (const entry of nodeEntries) {
          prepareErrors.push({
            nodeID: entry.nodeID,
            columnID: entry.columnID,
            message: "Could not read node metadata",
          });
        }
        continue;
      }

      const metadata = currentNode.metadata as Record<string, unknown>;
      const columns = Array.isArray(metadata.columns) ? [...metadata.columns] : [];
      const originalBody = structuredClone(currentNode) as Record<string, unknown>;
      const affectedColumns: ColumnUpdateInfo[] = [];

      for (const entry of nodeEntries) {
        const colIndex = columns.findIndex(
          (c) =>
            isPlainObject(c) &&
            (c.id === entry.columnID || c.columnID === entry.columnID)
        );

        if (colIndex === -1) {
          prepareErrors.push({
            nodeID: entry.nodeID,
            columnID: entry.columnID,
            message: "Column not found in current node state",
          });
          continue;
        }

        const col = columns[colIndex] as Record<string, unknown>;
        const previousName = typeof col.name === "string" ? col.name : undefined;
        const previousDataType = typeof col.dataType === "string" ? col.dataType : undefined;

        fullSnapshot.push({
          nodeID: entry.nodeID,
          nodeName: entry.nodeName,
          columnID: entry.columnID,
          previousColumnName: previousName ?? entry.columnName,
          previousDataType: previousDataType ?? "unknown",
          capturedAt: snapshotTimestamp,
          nodeBody: structuredClone(originalBody),
        });

        const updatedCol = { ...col };
        if (changes.columnName) updatedCol.name = changes.columnName;
        if (changes.dataType) updatedCol.dataType = changes.dataType;
        columns[colIndex] = updatedCol;

        affectedColumns.push({
          nodeID: entry.nodeID,
          nodeName: entry.nodeName,
          columnID: entry.columnID,
          columnName: changes.columnName ?? entry.columnName,
          previousName,
          previousDataType,
        });
      }

      if (affectedColumns.length === 0) {
        continue;
      }

      const body = buildUpdatedWorkspaceNodeBody(currentNode, { metadata: { columns } });

      prepared.push({
        nodeID: firstEntry.nodeID,
        nodeName: firstEntry.nodeName,
        body,
        originalBody,
        affectedColumns,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      for (const entry of nodeEntries) {
        prepareErrors.push({ nodeID: entry.nodeID, columnID: entry.columnID, message });
      }
    }
  }

  /** Strip nodeBody from full snapshot entries for inline (LLM-friendly) response. */
  function toSummary(entries: PreMutationSnapshotEntry[]): PreMutationSnapshotSummary[] {
    return entries.map(({ nodeBody: _, ...rest }) => rest);
  }

  // If nothing could be prepared, return early -- no writes attempted
  if (prepared.length === 0) {
    return {
      sourceNodeID: nodeID,
      sourceColumnID: columnID,
      changes,
      preMutationSnapshot: toSummary(fullSnapshot),
      updatedNodes: [],
      totalUpdated: 0,
      errors: prepareErrors,
    };
  }

  if (prepareErrors.length > 0) {
    return {
      sourceNodeID: nodeID,
      sourceColumnID: columnID,
      changes,
      preMutationSnapshot: toSummary(fullSnapshot),
      updatedNodes: [],
      totalUpdated: 0,
      errors: prepareErrors,
      skippedNodes: prepared.flatMap((update) => update.affectedColumns),
    };
  }

  // --- Persist pre-mutation snapshot to disk ---
  let snapshotPath: string | undefined;
  try {
    const resolvedBase = getCacheBaseDir(baseDir);
    const snapshotDir = join(resolvedBase, CACHE_DIR_NAME, "propagation-snapshots");
    mkdirSync(snapshotDir, { recursive: true });
    const fileName = `propagation-${safeWorkspaceID}-${snapshotTimestamp.replace(/[:.]/g, "-")}.json`;
    snapshotPath = join(snapshotDir, fileName);
    const diskSnapshot = {
      workspaceID,
      sourceNodeID: nodeID,
      sourceColumnID: columnID,
      changes,
      capturedAt: snapshotTimestamp,
      entries: fullSnapshot,
    };
    writeFileSync(snapshotPath, JSON.stringify(diskSnapshot, null, 2));
  } catch (err) {
    // Snapshot write failure should not block propagation
    const reason = err instanceof Error ? err.message : String(err);
    process.stderr.write(`Warning: failed to write pre-mutation snapshot to disk: ${reason}\n`);
    snapshotPath = undefined;
  }

  // --- Phase 2: Apply all prepared changes ---
  const appliedUpdates: PreparedNodeUpdate[] = [];
  const writeErrors: PropagationResult["errors"] = [];

  for (let i = 0; i < prepared.length; i++) {
    const update = prepared[i];
    await emitProgress("Applying", update.nodeName, i, prepared.length);

    try {
      await setWorkspaceNode(client, {
        workspaceID: safeWorkspaceID,
        nodeID: validatePathSegment(update.nodeID, "nodeID"),
        body: update.body,
      });

      appliedUpdates.push(update);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      writeErrors.push(...expandNodeError(update, message));
      const rollbackErrors = await rollbackPreparedNodeUpdates(client, safeWorkspaceID, appliedUpdates);
      const rollbackFailedNodeIDs = new Set(rollbackErrors.map((entry) => entry.nodeID));
      const updatedNodes = appliedUpdates
        .filter((applied) => rollbackFailedNodeIDs.has(applied.nodeID))
        .flatMap((applied) => applied.affectedColumns);
      const skippedNodes = rollbackErrors.length > 0
        ? prepared
            .flatMap((preparedNode) => preparedNode.affectedColumns)
            .filter((column) => !updatedNodes.some(
              (updated) => updated.nodeID === column.nodeID && updated.columnID === column.columnID
            ))
        : prepared.flatMap((preparedNode) => preparedNode.affectedColumns);

      if (appliedUpdates.length > 0 || rollbackErrors.length > 0) {
        invalidateLineageCache(workspaceID);
      }

      return {
        sourceNodeID: nodeID,
        sourceColumnID: columnID,
        changes,
        preMutationSnapshot: toSummary(fullSnapshot),
        ...(snapshotPath ? { snapshotPath } : {}),
        updatedNodes,
        totalUpdated: updatedNodes.length,
        errors: [...writeErrors, ...rollbackErrors],
        partialFailure: rollbackErrors.length > 0 ? true : undefined,
        skippedNodes,
        rolledBack: rollbackErrors.length === 0 ? true : undefined,
      };
    }
  }

  const updatedNodes = appliedUpdates.flatMap((update) => update.affectedColumns);

  // Only invalidate cache if writes were actually made
  if (updatedNodes.length > 0) {
    invalidateLineageCache(workspaceID);
  }

  const result: PropagationResult = {
    sourceNodeID: nodeID,
    sourceColumnID: columnID,
    changes,
    preMutationSnapshot: toSummary(fullSnapshot),
    ...(snapshotPath ? { snapshotPath } : {}),
    updatedNodes,
    totalUpdated: updatedNodes.length,
    errors: writeErrors,
  };

  return result;
}
