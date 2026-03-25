import { join } from "node:path";

/**
 * Root directory name for all MCP cache data.
 * Deliberately distinctive to avoid collisions when the server runs from a project root.
 */
export const CACHE_DIR_NAME = "coalesce_transform_mcp_data_cache";

export function getCacheDir(baseDir?: string): string {
  return join(baseDir ?? process.cwd(), CACHE_DIR_NAME);
}
