import { resolveOptionalRepoPathInput } from "../repo/path.js";
import { findSubgraphInCache, saveSubgraphToCache } from "./cache.js";
import { scanRepoSubgraphs } from "./repo-scan.js";

export type ResolvedSubgraph = {
  id: string;
  name: string;
  source: "cache" | "repo";
};

/**
 * Resolve a subgraph ID given a name. Order:
 *   1. Local cache (fastest, written on create)
 *   2. Repo {repoPath}/subgraphs/ YAML files
 *
 * The public scheduler API has no list endpoint for subgraphs, so this
 * function performs no HTTP I/O — callers that don't have a cache hit must
 * provide a repoPath (or pass subgraphID directly).
 */
export function resolveSubgraphByName(
  params: {
    workspaceID: string;
    name: string;
    repoPath?: string;
  }
): ResolvedSubgraph {
  const cached = findSubgraphInCache({
    workspaceID: params.workspaceID,
    name: params.name,
  });
  if (cached) {
    return { id: cached.id, name: cached.name, source: "cache" };
  }

  const repoPath = resolveOptionalRepoPathInput(params.repoPath);
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

  const repoNames = repoMatches.map((s) => s.name);
  const listing = repoNames.length > 0
    ? `Available in repo: ${repoNames.slice(0, 20).join(", ")}${repoNames.length > 20 ? ", ..." : ""}`
    : "No subgraphs found in cache or repo.";
  const searched = repoPath
    ? "Searched local cache and repo subgraphs/ folder."
    : "Searched local cache. Pass repoPath (or set COALESCE_REPO_PATH) to also check the repo subgraphs/ folder.";
  throw new Error(
    `Could not find a subgraph named "${params.name}" in workspace "${params.workspaceID}". ` +
      `${searched} The public Coalesce API has no subgraph list endpoint, so subgraphs created ` +
      `outside this MCP session cannot be resolved by name without a repo checkout. ` +
      `Pass subgraphID directly if you already know it. ${listing}`
  );
}
