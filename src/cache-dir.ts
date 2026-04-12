import { basename, extname, isAbsolute, join, relative, resolve } from "node:path";

/**
 * Root directory name for all MCP cache data.
 * Deliberately distinctive to avoid collisions when the server runs from a project root.
 */
export const CACHE_DIR_NAME = "coalesce_transform_mcp_data_cache";
export const CACHE_RESOURCE_URI_PREFIX = "coalesce://cache/";

export function getCacheDir(baseDir?: string): string {
  return join(baseDir ?? process.env.COALESCE_CACHE_DIR ?? process.cwd(), CACHE_DIR_NAME);
}

export type CacheResourceLink = {
  type: "resource_link";
  uri: string;
  name: string;
  mimeType?: string;
  description?: string;
};

function normalizeRelativeCachePath(relativePath: string): string | null {
  const normalized = relativePath.replace(/\\/g, "/");
  if (!normalized || normalized.startsWith("/") || normalized.includes("\0")) {
    return null;
  }

  const segments = normalized.split("/");
  if (segments.some((segment) => segment.length === 0 || segment === "." || segment === "..")) {
    return null;
  }

  return normalized;
}

export function getCacheRelativePath(filePath: string, baseDir?: string): string | null {
  const cacheDir = resolve(getCacheDir(baseDir));
  const resolvedFilePath = resolve(filePath);
  const relativePath = relative(cacheDir, resolvedFilePath);

  if (
    !relativePath ||
    relativePath.startsWith("..") ||
    isAbsolute(relativePath)
  ) {
    return null;
  }

  return normalizeRelativeCachePath(relativePath);
}

export function buildCacheResourceUri(filePath: string, baseDir?: string): string | null {
  const relativePath = getCacheRelativePath(filePath, baseDir);
  if (!relativePath) {
    return null;
  }

  const cacheKey = Buffer.from(relativePath, "utf8").toString("base64url");
  return `${CACHE_RESOURCE_URI_PREFIX}${cacheKey}`;
}

export function resolveCacheResourceUri(
  uri: string,
  baseDir?: string
): { filePath: string; relativePath: string } | null {
  let parsedUri: URL;
  try {
    parsedUri = new URL(uri);
  } catch {
    return null;
  }

  if (parsedUri.protocol !== "coalesce:" || parsedUri.host !== "cache") {
    return null;
  }

  const cacheKey = parsedUri.pathname.replace(/^\/+/, "");
  if (!cacheKey) {
    return null;
  }

  let relativePath: string;
  try {
    relativePath = Buffer.from(cacheKey, "base64url").toString("utf8");
  } catch {
    return null;
  }

  const normalizedRelativePath = normalizeRelativeCachePath(relativePath);
  if (!normalizedRelativePath) {
    return null;
  }

  const filePath = join(
    getCacheDir(baseDir),
    ...normalizedRelativePath.split("/").filter(Boolean)
  );

  const resolvedFilePath = resolve(filePath);
  const resolvedCacheDir = resolve(getCacheDir(baseDir));
  const relativeToCache = relative(resolvedCacheDir, resolvedFilePath);
  if (
    relativeToCache.startsWith("..") ||
    isAbsolute(relativeToCache)
  ) {
    return null;
  }

  return {
    filePath: resolvedFilePath,
    relativePath: normalizedRelativePath,
  };
}

export function getCacheResourceMimeType(filePath: string): string {
  switch (extname(filePath).toLowerCase()) {
    case ".json":
      return "application/json";
    case ".ndjson":
      return "application/x-ndjson";
    case ".md":
      return "text/markdown";
    case ".txt":
      return "text/plain";
    default:
      return "text/plain";
  }
}

export function buildCacheResourceLink(
  filePath: string,
  options: {
    baseDir?: string;
    name?: string;
    description?: string;
  } = {}
): CacheResourceLink | null {
  const uri = buildCacheResourceUri(filePath, options.baseDir);
  if (!uri) {
    return null;
  }

  return {
    type: "resource_link",
    uri,
    name: options.name ?? basename(filePath),
    mimeType: getCacheResourceMimeType(filePath),
    ...(options.description ? { description: options.description } : {}),
  };
}
