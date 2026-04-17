import type { CoalesceClient } from "../../client.js";
import { isPlainObject } from "../../utils.js";
import {
  createWorkspaceSubgraph as apiCreate,
  updateWorkspaceSubgraph as apiUpdate,
  deleteWorkspaceSubgraph as apiDelete,
} from "../../coalesce/api/subgraphs.js";
import {
  removeSubgraphFromCache,
  saveSubgraphToCache,
} from "./cache.js";
import { resolveSubgraphByName, type ResolvedSubgraph } from "./resolve.js";

export type SubgraphTarget = {
  workspaceID: string;
  subgraphID?: string;
  subgraphName?: string;
  repoPath?: string;
};

async function resolveTarget(
  client: CoalesceClient,
  target: SubgraphTarget
): Promise<{ id: string; resolved: ResolvedSubgraph | null }> {
  if (target.subgraphID) {
    return { id: target.subgraphID, resolved: null };
  }
  if (!target.subgraphName) {
    throw new Error(
      "Either subgraphID or subgraphName is required. Pass subgraphID if you know it, or subgraphName (with optional repoPath) to resolve by name."
    );
  }
  const resolved = await resolveSubgraphByName(client, {
    workspaceID: target.workspaceID,
    name: target.subgraphName,
    repoPath: target.repoPath,
  });
  return { id: resolved.id, resolved };
}

export async function createSubgraphWithCache(
  client: CoalesceClient,
  params: { workspaceID: string; name: string; steps: string[] }
): Promise<Record<string, unknown>> {
  const created = await apiCreate(client, params);
  const id = isPlainObject(created) && typeof created.id === "string" && created.id.length > 0
    ? created.id
    : null;
  if (id) {
    saveSubgraphToCache({
      workspaceID: params.workspaceID,
      id,
      name: params.name,
      steps: params.steps,
    });
  }
  return {
    subgraphID: id,
    subgraph: created,
    cached: id !== null,
    message: id
      ? `Subgraph "${params.name}" created with ID ${id}. ID cached for future edits by name.`
      : `Subgraph "${params.name}" created, but no ID was returned by the API — edits by name will not find it.`,
  };
}

export async function updateSubgraphResolved(
  client: CoalesceClient,
  params: SubgraphTarget & { name: string; steps: string[] }
): Promise<Record<string, unknown>> {
  const { id, resolved } = await resolveTarget(client, params);
  const updated = await apiUpdate(client, {
    workspaceID: params.workspaceID,
    subgraphID: id,
    name: params.name,
    steps: params.steps,
  });
  saveSubgraphToCache({
    workspaceID: params.workspaceID,
    id,
    name: params.name,
    steps: params.steps,
  });
  return {
    subgraphID: id,
    subgraph: updated,
    resolvedFrom: resolved?.source ?? "input",
  };
}

export async function deleteSubgraphByID(
  client: CoalesceClient,
  params: { workspaceID: string; subgraphID: string }
): Promise<Record<string, unknown>> {
  const result = await apiDelete(client, params);
  removeSubgraphFromCache({ workspaceID: params.workspaceID, id: params.subgraphID });
  return {
    subgraphID: params.subgraphID,
    result,
  };
}
