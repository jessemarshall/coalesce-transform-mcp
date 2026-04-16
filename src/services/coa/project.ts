import { existsSync, statSync } from "node:fs";
import { isAbsolute, join, resolve } from "node:path";

export class InvalidCoaProjectPathError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidCoaProjectPathError";
  }
}

/**
 * Verify a path is a COA project root (directory containing `data.yml`).
 * Returns the resolved absolute path. Throws InvalidCoaProjectPathError
 * with a specific message for each failure mode so MCP error output can
 * distinguish "path doesn't exist" from "path is not a COA project."
 */
export function validateProjectPath(projectPath: string): string {
  if (!projectPath || typeof projectPath !== "string") {
    throw new InvalidCoaProjectPathError("projectPath is required");
  }

  const absolute = isAbsolute(projectPath)
    ? projectPath
    : resolve(process.cwd(), projectPath);

  if (!existsSync(absolute)) {
    throw new InvalidCoaProjectPathError(
      `projectPath does not exist: ${absolute}`
    );
  }

  const stat = statSync(absolute);
  if (!stat.isDirectory()) {
    throw new InvalidCoaProjectPathError(
      `projectPath is not a directory: ${absolute}`
    );
  }

  const dataYml = join(absolute, "data.yml");
  if (!existsSync(dataYml)) {
    throw new InvalidCoaProjectPathError(
      `projectPath is not a COA project (missing data.yml): ${absolute}`
    );
  }

  return absolute;
}
