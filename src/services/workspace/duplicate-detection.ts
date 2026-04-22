import { type CoalesceClient } from "../../client.js";
import { CoalesceApiError } from "../../client.js";
import { getWorkspaceNode } from "../../coalesce/api/nodes.js";
import { getWorkspaceNodeIndex } from "../cache/workspace-node-index.js";
import { rethrowNonRecoverableApiError } from "../../utils.js";
import { type WorkspaceNodeIndexEntry } from "../shared/node-helpers.js";

const UNIQUE_NAME_PATTERN = /unique names|same Storage Location/i;

export function isUniqueStorageLocationNameError(error: unknown): boolean {
  if (!(error instanceof CoalesceApiError)) return false;
  if (error.status !== 400) return false;
  if (UNIQUE_NAME_PATTERN.test(error.message ?? "")) return true;
  try {
    return UNIQUE_NAME_PATTERN.test(JSON.stringify(error.detail ?? ""));
  } catch {
    return false;
  }
}

function normalize(value: string): string {
  return value.trim().toUpperCase();
}

function nodeTypesMatch(a: string | null, b: string | null): boolean {
  if (!a || !b) return false;
  if (a === b) return true;
  // Accept bare-vs-prefixed matches ("Stage" <-> "base-nodes:::Stage")
  const aID = a.includes(":::") ? (a.split(":::")[1] ?? a) : a;
  const bID = b.includes(":::") ? (b.split(":::")[1] ?? b) : b;
  return aID === bID;
}

export type FindCriteria = {
  name: string;
  locationName?: string | null;
  nodeType?: string | null;
};

/**
 * Look up a workspace node by name in the cached index, optionally narrowed by
 * locationName and/or nodeType. Returns the lightweight index entry — callers
 * should fetch the full body via `getWorkspaceNode` when needed.
 */
export async function findWorkspaceNodeIndexEntry(
  client: CoalesceClient,
  workspaceID: string,
  criteria: FindCriteria
): Promise<WorkspaceNodeIndexEntry | null> {
  const nodes = await getWorkspaceNodeIndex(client, workspaceID);
  const targetName = normalize(criteria.name);
  const targetLocation = criteria.locationName ? normalize(criteria.locationName) : null;

  const byName = nodes.filter((node) => normalize(node.name) === targetName);
  if (byName.length === 0) return null;

  const byLocation = targetLocation
    ? byName.filter(
        (node) => node.locationName && normalize(node.locationName) === targetLocation
      )
    : byName;

  const pool = byLocation.length > 0 ? byLocation : byName;

  if (criteria.nodeType) {
    const typed = pool.filter((node) => nodeTypesMatch(node.nodeType, criteria.nodeType!));
    if (typed.length > 0) return typed[0]!;
    // A name/location match exists but no type match: return null so the
    // caller does not silently hand back a wrong-type node as "preExisting".
    // The caller is free to create a distinct node (uniqueness is enforced
    // at the server, so a same-name different-type collision would surface
    // there as a 400 we can still recover from).
    return null;
  }

  return pool[0] ?? null;
}

export type DuplicateNodeResult = {
  node: Record<string, unknown>;
  preExisting: true;
  warning: string;
  nextSteps: string[];
};

function buildWarning(params: {
  name: string;
  locationName?: string | null;
  requestedNodeType?: string | null;
  existing: WorkspaceNodeIndexEntry;
  timedOutRecovery: boolean;
}): string {
  const locationHint = params.locationName
    ? ` in storage location "${params.locationName}"`
    : "";
  const typeMismatch =
    params.requestedNodeType &&
    params.existing.nodeType &&
    !nodeTypesMatch(params.existing.nodeType, params.requestedNodeType)
      ? ` Note: the existing node is of type "${params.existing.nodeType}", ` +
        `which differs from the requested "${params.requestedNodeType}". ` +
        `Verify this is the node you intended before using it.`
      : "";
  const recoveryHint = params.timedOutRecovery
    ? "This often happens when a previous creation call timed out at the client " +
      "but succeeded on the server. "
    : "";
  return (
    `A node named "${params.name}"${locationHint} already exists. ` +
    `Returning the existing node instead of creating a duplicate. ` +
    recoveryHint +
    `If this is not the node you intended, choose a different name or delete ` +
    `the existing node before retrying.${typeMismatch}`
  );
}

async function locateExistingNode(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    name: string;
    locationName?: string | null;
    nodeType?: string | null;
    timedOutRecovery: boolean;
  }
): Promise<DuplicateNodeResult | null> {
  let entry: WorkspaceNodeIndexEntry | null;
  try {
    entry = await findWorkspaceNodeIndexEntry(client, params.workspaceID, params);
  } catch (error) {
    rethrowNonRecoverableApiError(error);
    return null;
  }
  if (!entry) return null;

  let fullNode: Record<string, unknown>;
  try {
    const fetched = await getWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: entry.id,
    });
    if (fetched && typeof fetched === "object" && !Array.isArray(fetched)) {
      fullNode = fetched as Record<string, unknown>;
    } else {
      fullNode = { id: entry.id, name: entry.name };
    }
  } catch (error) {
    rethrowNonRecoverableApiError(error);
    fullNode = { id: entry.id, name: entry.name };
  }

  return {
    node: fullNode,
    preExisting: true,
    warning: buildWarning({
      name: params.name,
      locationName: params.locationName ?? null,
      requestedNodeType: params.nodeType ?? null,
      existing: entry,
      timedOutRecovery: params.timedOutRecovery,
    }),
    nextSteps: [
      "Verify the existing node matches your intent (columns, transforms, config).",
      "If this is the node you wanted, proceed with downstream work — no further creation is needed.",
      "If you intended a different node, choose a new name or delete the existing node and retry.",
    ],
  };
}

/**
 * Pre-flight duplicate detection: before creating, check whether a node with
 * the requested name already exists in the target location. Short-circuits
 * creation and avoids leaving an orphan placeholder when the PUT to rename a
 * just-created node would otherwise fail with the "unique names" 400.
 *
 * Non-recoverable errors (401/403/503) are rethrown so auth failures do not
 * get silently interpreted as "no duplicate".
 */
export async function findExistingNodeForCreation(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    name: string;
    locationName?: string | null;
    nodeType?: string | null;
  }
): Promise<DuplicateNodeResult | null> {
  return locateExistingNode(client, { ...params, timedOutRecovery: false });
}

/**
 * Recovery path used after a creation call has already failed with a unique-
 * name 400 (same semantics as `findExistingNodeForCreation` with a warning
 * worded for the retry-after-timeout case).
 */
export async function recoverFromUniqueNameError(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    name: string;
    locationName?: string | null;
    nodeType?: string | null;
  }
): Promise<DuplicateNodeResult | null> {
  return locateExistingNode(client, { ...params, timedOutRecovery: true });
}
