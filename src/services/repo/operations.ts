import {
  parseRepo,
  resolveRepoNodeType,
  type RepoNodeTypeRecord,
} from "./parser.js";

export interface RepoNodeTypeDefinition {
  nodeDefinition: Record<string, unknown> | null;
  resolvedNodeType: string;
  warnings: string[];
}

export async function getRepoNodeTypeDefinition(
  repoPath: string,
  nodeType: string
): Promise<RepoNodeTypeDefinition> {
  const parsedRepo = parseRepo(repoPath);
  const resolution = resolveRepoNodeType(parsedRepo, nodeType);

  return {
    nodeDefinition: resolution.nodeTypeRecord.nodeDefinition,
    resolvedNodeType: resolution.resolvedNodeType,
    warnings: resolution.nodeTypeRecord.warnings,
  };
}
