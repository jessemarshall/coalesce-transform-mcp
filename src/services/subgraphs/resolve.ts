import type { CoalesceClient } from "../../client.js";
import { isPlainObject } from "../../utils.js";
import { listWorkspaceSubgraphs } from "../../coalesce/api/subgraphs.js";
import { resolveOptionalRepoPathInput } from "../repo/path.js";
import { findSubgraphInCache, saveSubgraphToCache } from "./cache.js";
import { scanRepoSubgraphs } from "./repo-scan.js";

export type ResolvedSubgraph = {
  id: string;
  name: string;
  source: "cache" | "repo" | "workspace";
};

/**
 * Resolve a subgraph ID given a name. Order:
 *   1. Local cache (fastest, written on create)
 *   2. Repo {repoPath}/subgraphs/ YAML files
 *   3. Workspace API list_workspace_subgraphs
 *
 * Throws a clear error with candidate names if nothing matches.
 */
export async function resolveSubgraphByName(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    name: string;
    repoPath?: string;
  }
): Promise<ResolvedSubgraph> {
  const cached = findSubgraphInCache({
    workspaceID: params.workspaceID,
    name: params.name,
  });
  if (cached) {
    return { id: cached.id, name: cached.name, source: "cache" };
  }

  const repoPath = resolveOptionalRepoPathInput(params.repoPath);
  // Scan the repo once and reuse both for the fast-path match and the
  // not-found error listing — avoids a second scan + duplicate stderr on
  // corrupt/unparseable YAML files in large repos.
  const repoMatches = repoPath ? scanRepoSubgraphs(repoPath) : [];
  const repoMatch = repoMatches.find((s) => s.name === params.name);
  if (repoMatch) {
    saveSubgraphToCache({
      workspaceID: params.workspaceID,
      id: repoMatch.id,
      name: repoMatch.name,
      steps: repoMatch.steps,
    });
    return { id: repoMatch.id, name: repoMatch.name, source: "repo" };
  }

  const listResponse = await listWorkspaceSubgraphs(client, {
    workspaceID: params.workspaceID,
  });
  const data = isPlainObject(listResponse) && Array.isArray(listResponse.data)
    ? listResponse.data
    : [];
  const matches = data
    .filter(isPlainObject)
    .map((entry) => ({
      id: typeof entry.id === "string" ? entry.id : "",
      name: typeof entry.name === "string" ? entry.name : "",
      steps: Array.isArray(entry.steps)
        ? entry.steps.filter((s: unknown): s is string => typeof s === "string")
        : [],
    }))
    .filter((entry) => entry.id.length > 0 && entry.name.length > 0);

  const workspaceMatch = matches.find((m) => m.name === params.name);
  if (workspaceMatch) {
    saveSubgraphToCache({
      workspaceID: params.workspaceID,
      id: workspaceMatch.id,
      name: workspaceMatch.name,
      steps: workspaceMatch.steps,
    });
    return { id: workspaceMatch.id, name: workspaceMatch.name, source: "workspace" };
  }

  const availableNames = matches.map((m) => m.name);
  const repoNames = repoMatches.map((s) => s.name);
  const combined = Array.from(new Set([...availableNames, ...repoNames]));
  const listing = combined.length > 0
    ? `Available: ${combined.slice(0, 20).join(", ")}${combined.length > 20 ? ", ..." : ""}`
    : "No subgraphs found in workspace or repo.";
  const searched = repoPath
    ? "Searched local cache, repo subgraphs/ folder, and workspace."
    : "Searched local cache and workspace. Pass repoPath to also check the repo subgraphs/ folder.";
  throw new Error(
    `Could not find a subgraph named "${params.name}" in workspace "${params.workspaceID}". ` +
      `${searched} Pass subgraphID directly if you already know it. ${listing}`
  );
}
