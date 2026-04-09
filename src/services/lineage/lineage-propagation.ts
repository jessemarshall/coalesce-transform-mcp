import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { CoalesceClient } from "../../client.js";
import { CACHE_DIR_NAME } from "../../cache-dir.js";
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
};

type PreparedColumnUpdate = ColumnUpdateInfo & {
  body: Record<string, unknown>;
};

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
  const prepared: PreparedColumnUpdate[] = [];
  const prepareErrors: PropagationResult["errors"] = [];
  const snapshotTimestamp = new Date().toISOString();
  const fullSnapshot: PreMutationSnapshotEntry[] = [];

  for (let i = 0; i < downstreamEntries.length; i++) {
    const entry = downstreamEntries[i];
    await emitProgress("Preparing", `${entry.columnName} on ${entry.nodeName}`, i, downstreamEntries.length);

    try {
      const currentNode = await client.get(
        `/api/v1/workspaces/${safeWorkspaceID}/nodes/${validatePathSegment(entry.nodeID, "nodeID")}`,
        {}
      );

      if (!isPlainObject(currentNode) || !isPlainObject(currentNode.metadata)) {
        prepareErrors.push({ nodeID: entry.nodeID, columnID: entry.columnID, message: "Could not read node metadata" });
        continue;
      }

      const metadata = currentNode.metadata as Record<string, unknown>;
      const columns = Array.isArray(metadata.columns) ? [...metadata.columns] : [];
      const colIndex = columns.findIndex(
        (c) =>
          isPlainObject(c) &&
          (c.id === entry.columnID || c.columnID === entry.columnID)
      );

      if (colIndex === -1) {
        prepareErrors.push({ nodeID: entry.nodeID, columnID: entry.columnID, message: "Column not found in current node state" });
        continue;
      }

      const col = columns[colIndex] as Record<string, unknown>;
      const previousName = typeof col.name === "string" ? col.name : undefined;
      const previousDataType = typeof col.dataType === "string" ? col.dataType : undefined;

      // Capture pre-mutation snapshot (full nodeBody for disk persistence)
      fullSnapshot.push({
        nodeID: entry.nodeID,
        nodeName: entry.nodeName,
        columnID: entry.columnID,
        previousColumnName: previousName ?? entry.columnName,
        previousDataType: previousDataType ?? "unknown",
        capturedAt: snapshotTimestamp,
        nodeBody: structuredClone(currentNode) as Record<string, unknown>,
      });

      const updatedCol = { ...col };
      if (changes.columnName) updatedCol.name = changes.columnName;
      if (changes.dataType) updatedCol.dataType = changes.dataType;
      columns[colIndex] = updatedCol;

      const body = buildUpdatedWorkspaceNodeBody(currentNode, { metadata: { columns } });

      prepared.push({
        nodeID: entry.nodeID,
        nodeName: entry.nodeName,
        columnID: entry.columnID,
        columnName: changes.columnName ?? entry.columnName,
        previousName,
        previousDataType,
        body,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      prepareErrors.push({ nodeID: entry.nodeID, columnID: entry.columnID, message });
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

  // --- Persist pre-mutation snapshot to disk ---
  let snapshotPath: string | undefined;
  try {
    const resolvedBase = baseDir ?? process.cwd();
    const snapshotDir = join(resolvedBase, CACHE_DIR_NAME, "propagation-snapshots");
    mkdirSync(snapshotDir, { recursive: true });
    const fileName = `propagation-${workspaceID}-${snapshotTimestamp.replace(/[:.]/g, "-")}.json`;
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
  const updatedNodes: ColumnUpdateInfo[] = [];
  const writeErrors: PropagationResult["errors"] = [];

  for (let i = 0; i < prepared.length; i++) {
    const update = prepared[i];
    await emitProgress("Applying", `${update.columnName} on ${update.nodeName}`, i, prepared.length);

    try {
      await setWorkspaceNode(client, {
        workspaceID: safeWorkspaceID,
        nodeID: validatePathSegment(update.nodeID, "nodeID"),
        body: update.body,
      });

      const { body: _, ...info } = update;
      updatedNodes.push(info);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      writeErrors.push({ nodeID: update.nodeID, columnID: update.columnID, message });
      // Stop on first write failure to minimize workspace inconsistency
      break;
    }
  }

  const allErrors = [...prepareErrors, ...writeErrors];
  const partialFailure = updatedNodes.length > 0 && writeErrors.length > 0;

  // Skipped = prepared entries after the last written + 1 failed entry.
  // The write loop processes prepared[] in order and breaks on first error,
  // so everything beyond (updatedNodes.length + writeErrors.length) was skipped.
  const firstSkippedIndex = updatedNodes.length + writeErrors.length;
  const skippedNodes = prepared.slice(firstSkippedIndex).map(({ body: _, ...info }) => info);

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
    errors: allErrors,
  };
  if (partialFailure) result.partialFailure = true;
  if (skippedNodes.length > 0) result.skippedNodes = skippedNodes;

  return result;
}
