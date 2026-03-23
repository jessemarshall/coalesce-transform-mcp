export function resolveOptionalRepoPathInput(repoPath?: string): string | undefined {
  const explicitRepoPath =
    typeof repoPath === "string" && repoPath.trim().length > 0
      ? repoPath
      : undefined;
  if (explicitRepoPath) {
    return explicitRepoPath;
  }

  const envRepoPath = process.env.COALESCE_REPO_PATH;
  if (typeof envRepoPath === "string" && envRepoPath.trim().length > 0) {
    return envRepoPath;
  }

  return undefined;
}

export function resolveRepoPathInput(repoPath?: string): string {
  const resolved = resolveOptionalRepoPathInput(repoPath);
  if (resolved) {
    return resolved;
  }

  throw new Error(
    "repoPath is required for repo-backed tools. Provide repoPath explicitly or set COALESCE_REPO_PATH."
  );
}
