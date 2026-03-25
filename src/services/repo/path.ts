import { existsSync, realpathSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

/**
 * Validates that a path is a legitimate Coalesce repo directory.
 *
 * Security boundary: repoPath originates from tool input (LLM-controlled) or
 * the COALESCE_REPO_PATH env var. We resolve symlinks, verify it is a real
 * directory, and require the Coalesce-specific nodeTypes/ subdirectory before
 * allowing any filesystem reads downstream.
 *
 * Error messages intentionally omit the resolved path to avoid leaking
 * filesystem layout through MCP tool responses.
 */
function validateRepoPath(rawPath: string): string {
  const absolutePath = resolve(rawPath);

  if (!existsSync(absolutePath)) {
    throw new Error(
      "repoPath does not exist. Check the provided path or COALESCE_REPO_PATH environment variable."
    );
  }

  const stats = statSync(absolutePath);
  if (!stats.isDirectory()) {
    throw new Error(
      "repoPath is not a directory. Expected a Coalesce repo directory containing a nodeTypes/ subdirectory."
    );
  }

  // Resolve symlinks to get the canonical path
  const resolvedPath = realpathSync(absolutePath);

  // Structural validation: a Coalesce repo must have a nodeTypes/ directory
  const nodeTypesDir = join(resolvedPath, "nodeTypes");
  if (!existsSync(nodeTypesDir) || !statSync(nodeTypesDir).isDirectory()) {
    throw new Error(
      "repoPath is not a valid Coalesce repo: missing nodeTypes/ subdirectory. " +
      "Expected a directory containing nodeTypes/, typically a cloned Coalesce project repo."
    );
  }

  return resolvedPath;
}

export function resolveOptionalRepoPathInput(repoPath?: string): string | undefined {
  const explicitRepoPath =
    typeof repoPath === "string" && repoPath.trim().length > 0
      ? repoPath
      : undefined;
  if (explicitRepoPath) {
    return validateRepoPath(explicitRepoPath);
  }

  const envRepoPath = process.env.COALESCE_REPO_PATH;
  if (typeof envRepoPath === "string" && envRepoPath.trim().length > 0) {
    return validateRepoPath(envRepoPath);
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
