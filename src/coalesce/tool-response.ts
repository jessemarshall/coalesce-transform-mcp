import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import {
  buildCacheResourceLink,
  CACHE_DIR_NAME,
  getCacheBaseDir,
  type CacheResourceLink,
} from "../cache-dir.js";
import { z } from "zod";
import { isPlainObject, safeErrorMessage } from "../utils.js";
import { CoalesceApiError } from "../client.js";
import { JsonToolErrorSchema } from "./tool-schemas.js";

const SESSION_START_TIME = new Date();
const DEFAULT_AUTO_CACHE_MAX_BYTES = 32 * 1024;

type TextContent = { type: "text"; text: string };

export type JsonToolResponse = {
  content: Array<TextContent | CacheResourceLink>;
  structuredContent?: Record<string, unknown>;
};

type JsonToolErrorResponse = {
  isError: true;
  content: { type: "text"; text: string }[];
  structuredContent: {
    error: z.infer<typeof JsonToolErrorSchema>;
  };
};

type JsonToolResponseOptions = {
  baseDir?: string;
  maxInlineBytes?: number;
  /**
   * Workspace identifier used to partition the on-disk auto-cache.
   * When present, responses are written under `<cache>/workspace-<id>/auto-cache/`
   * to match the snapshot cache convention in `services/cache/snapshots.ts` and
   * `services/lineage/lineage-cache.ts`, so every file belonging to one workspace
   * lives under a single `workspace-<id>/` directory.
   */
  workspaceID?: string;
  /**
   * Environment identifier, used when a tool operates on a deployed environment
   * rather than a workspace (the run-task family). Written under
   * `<cache>/environment-<id>/auto-cache/`. Ignored when `workspaceID` is set —
   * workspace scope takes precedence because dev runs carry both IDs but belong
   * to the workspace's cache namespace.
   * Falls through to `_global` when both are absent.
   */
  environmentID?: string;
};

const GLOBAL_AUTO_CACHE_BUCKET = "_global";

function sanitizeIdSegment(id: string): string {
  // Strip anything that isn't a safe filesystem segment; collapse `.`/`..`
  // style inputs to empty so the caller can fall back to `_global`.
  const cleaned = id.replace(/[^a-zA-Z0-9._-]/g, "-").replace(/^[-.]+|[-.]+$/g, "");
  if (!cleaned || cleaned === "." || cleaned === "..") return "";
  return cleaned;
}

function buildAutoCacheBucket(opts: {
  workspaceID?: string;
  environmentID?: string;
}): string {
  if (opts.workspaceID) {
    const cleaned = sanitizeIdSegment(opts.workspaceID);
    if (cleaned) return `workspace-${cleaned}`;
  }
  if (opts.environmentID) {
    const cleaned = sanitizeIdSegment(opts.environmentID);
    if (cleaned) return `environment-${cleaned}`;
  }
  return GLOBAL_AUTO_CACHE_BUCKET;
}

function slugifyFileComponent(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function getAutoCacheMaxBytes(): number {
  const raw = process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES;
  if (raw === undefined) {
    return DEFAULT_AUTO_CACHE_MAX_BYTES;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return DEFAULT_AUTO_CACHE_MAX_BYTES;
  }

  return parsed;
}

/** Maximum auto-cache files to keep regardless of timestamp — prevents unbounded growth if
 *  timestamp-based cleanup keeps failing (e.g. persistent permission errors). */
const AUTO_CACHE_MAX_FILES = 200;

function cleanupStaleAutoCacheFilesInBucket(autoCacheDir: string): void {
  try {
    const sessionTimestamp = SESSION_START_TIME.toISOString().replace(/[:.]/g, "-");
    const files = readdirSync(autoCacheDir)
      .filter((f) => f.endsWith(".json"))
      .sort();

    // Phase 1: delete files from previous sessions
    for (const file of files) {
      // Filenames are: {ISO_timestamp}-{tool-name}-{uuid}.json
      // Compare the timestamp prefix against session start
      if (file < sessionTimestamp) {
        try {
          unlinkSync(join(autoCacheDir, file));
        } catch (error) {
          const reason = safeErrorMessage(error);
          process.stderr.write(`[auto-cache] Failed to delete stale file ${file}: ${reason}\n`);
        }
      }
    }

    // Phase 2: hard cap — if the directory STILL exceeds the limit after
    // phase 1 (e.g. a current-session flood), re-read and evict oldest beyond
    // the cap. Gating on `currentFiles.length` (not `files.length`) is load
    // bearing: after phase 1 drops the count below the cap,
    // `currentFiles.length - MAX` is negative and `slice(0, -N)` would evict
    // current-session files we just wrote.
    if (files.length > AUTO_CACHE_MAX_FILES) {
      const currentFiles = readdirSync(autoCacheDir)
        .filter((f) => f.endsWith(".json"))
        .sort();
      if (currentFiles.length > AUTO_CACHE_MAX_FILES) {
        for (const file of currentFiles.slice(0, currentFiles.length - AUTO_CACHE_MAX_FILES)) {
          try {
            unlinkSync(join(autoCacheDir, file));
          } catch (err) {
            const reason = safeErrorMessage(err);
            process.stderr.write(`[auto-cache] Phase 2 eviction failed for ${file}: ${reason}\n`);
          }
        }
      }
    }
  } catch (error) {
    const reason = safeErrorMessage(error);
    process.stderr.write(`[auto-cache] Stale file cleanup failed for ${autoCacheDir}: ${reason}\n`);
  }
}

/**
 * Clean up stale auto-cache files across every workspace bucket (and _global)
 * beneath the cache root. Each bucket's per-session + max-file-count limits
 * are enforced independently so one busy workspace cannot starve another.
 */
function cleanupStaleAutoCacheFiles(writtenBucketDir: string, baseDir: string): void {
  const cacheRoot = join(baseDir, CACHE_DIR_NAME);
  let entries: string[];
  try {
    entries = readdirSync(cacheRoot);
  } catch (error) {
    // Cache root may not exist yet — clean the bucket we just wrote to and stop.
    cleanupStaleAutoCacheFilesInBucket(writtenBucketDir);
    const reason = safeErrorMessage(error);
    process.stderr.write(`[auto-cache] Unable to enumerate cache root ${cacheRoot}: ${reason}\n`);
    return;
  }

  const visited = new Set<string>();
  for (const entry of entries) {
    const bucketAutoCache = join(cacheRoot, entry, "auto-cache");
    try {
      if (!existsSync(bucketAutoCache)) continue;
      if (!statSync(bucketAutoCache).isDirectory()) continue;
    } catch {
      continue;
    }
    visited.add(bucketAutoCache);
    cleanupStaleAutoCacheFilesInBucket(bucketAutoCache);
  }

  // Ensure the bucket we just wrote to is cleaned even if the readdir missed it
  // (e.g. race condition with concurrent writes).
  if (!visited.has(writtenBucketDir)) {
    cleanupStaleAutoCacheFilesInBucket(writtenBucketDir);
  }
}

function buildAutoCacheFilePath(
  toolName: string,
  cachedAt: string,
  baseDir: string,
  scope: { workspaceID?: string; environmentID?: string }
): string {
  const bucket = buildAutoCacheBucket(scope);
  const directory = join(baseDir, CACHE_DIR_NAME, bucket, "auto-cache");
  mkdirSync(directory, { recursive: true });
  const timestamp = cachedAt.replace(/[:.]/g, "-");
  const safeToolName = slugifyFileComponent(toolName) || "tool-response";
  return join(directory, `${timestamp}-${safeToolName}-${randomUUID()}.json`);
}

function humanizeFieldName(fieldName: string): string {
  const stripped = fieldName.replace(/(Path|Uri)$/, "");
  const humanized = stripped
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .trim();
  return humanized.length > 0
    ? humanized.charAt(0).toUpperCase() + humanized.slice(1)
    : "Cached artifact";
}

function buildCacheLinkForField(
  fieldName: string,
  filePath: string,
  baseDir: string
): CacheResourceLink | null {
  return buildCacheResourceLink(filePath, {
    baseDir,
    name: humanizeFieldName(fieldName),
    description: "Read this cached artifact through the MCP resource URI.",
  });
}

function externalizeCachePaths(
  value: unknown,
  baseDir: string,
  resourceLinks: Map<string, CacheResourceLink>
): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => externalizeCachePaths(item, baseDir, resourceLinks));
  }

  if (!isPlainObject(value)) {
    return value;
  }

  const output: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value)) {
    if (typeof child === "string") {
      const link = buildCacheLinkForField(key, child, baseDir);
      if (link) {
        const renamedKey =
          key.endsWith("Path") && !Object.prototype.hasOwnProperty.call(value, `${key.slice(0, -4)}Uri`)
            ? `${key.slice(0, -4)}Uri`
            : key;
        output[renamedKey] = link.uri;
        resourceLinks.set(link.uri, link);
        continue;
      }
    }

    output[key] = externalizeCachePaths(child, baseDir, resourceLinks);
  }

  return output;
}

/**
 * Coerce pagination fields so text content and structuredContent stay
 * consistent and match the MCP output schema.  The Coalesce API may return
 * `next` as a number (page index) and `next`/`total` as null — normalise
 * both before any serialisation path consumes the result.
 */
function coerceListPaginationFields(result: unknown): unknown {
  if (!isPlainObject(result)) return result;
  const patched: Record<string, unknown> = { ...result };
  if ("next" in result) {
    if (typeof result.next === "number") {
      patched.next = String(result.next);
    } else if (result.next === null || result.next === undefined) {
      delete patched.next;
    }
  }
  if ("total" in result && (result.total === null || result.total === undefined)) {
    delete patched.total;
  }
  return patched;
}

function buildInlineJsonResponse(
  result: unknown,
  resourceLinks: CacheResourceLink[]
): JsonToolResponse {
  const text = JSON.stringify(result, null, 2);
  return {
    content: [{ type: "text", text }, ...resourceLinks],
    structuredContent: normalizeStructuredContent(result),
  };
}

function normalizeStructuredContent(result: unknown): Record<string, unknown> {
  if (isPlainObject(result)) {
    return result;
  }
  return { value: result ?? null };
}

export function buildJsonToolResponse(
  toolName: string,
  result: unknown,
  options: JsonToolResponseOptions = {}
): JsonToolResponse {
  const baseDir = getCacheBaseDir(options.baseDir);
  const coerced = coerceListPaginationFields(result);
  const resourceLinks = new Map<string, CacheResourceLink>();
  const externalizedResult = externalizeCachePaths(coerced, baseDir, resourceLinks);
  const text = JSON.stringify(externalizedResult, null, 2);
  const maxInlineBytes = options.maxInlineBytes ?? getAutoCacheMaxBytes();
  const sizeBytes = Buffer.byteLength(text, "utf8");

  if (sizeBytes <= maxInlineBytes) {
    return buildInlineJsonResponse(
      externalizedResult,
      [...resourceLinks.values()]
    );
  }

  const cachedAt = new Date().toISOString();
  let filePath: string;
  try {
    filePath = buildAutoCacheFilePath(toolName, cachedAt, baseDir, {
      workspaceID: options.workspaceID,
      environmentID: options.environmentID,
    });
    writeFileSync(filePath, `${text}\n`, "utf8");
    cleanupStaleAutoCacheFiles(dirname(filePath), baseDir);
  } catch (error) {
    const reason = safeErrorMessage(error);
    process.stderr.write(`[coalesce-transform-mcp] auto-cache write failed for ${toolName}: ${reason}\n`);
    return buildInlineJsonResponse(
      externalizedResult,
      [...resourceLinks.values()]
    );
  }

  const cacheLink =
    buildCacheResourceLink(filePath, {
      baseDir,
      name: `${toolName} cached response`,
      description:
        "Full tool response cached on the MCP server because it exceeded the inline response threshold.",
    }) ?? null;

  const metadata: Record<string, unknown> = {
    autoCached: true,
    toolName,
    cachedAt,
    sizeBytes,
    maxInlineBytes,
    ...(cacheLink ? { resourceUri: cacheLink.uri } : {}),
    message:
      "Full response was automatically cached to disk because it exceeded the inline response threshold.",
  };

  // Include structuredContent from the metadata so tools with an outputSchema
  // pass the MCP SDK's server-side validation (which requires structuredContent
  // when outputSchema is defined).  All output schemas MUST use .passthrough()
  // and all fields MUST be .optional() so cache metadata passes validation.
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(metadata, null, 2),
      },
      ...(cacheLink ? [cacheLink] : []),
    ],
    structuredContent: metadata,
  };
}

export function handleToolError(
  error: unknown
): JsonToolErrorResponse {
  const normalized =
    error instanceof CoalesceApiError
      ? {
          message: error.message,
          status: error.status,
          ...(error.detail !== undefined ? { detail: error.detail } : {}),
        }
      : error instanceof Error
        ? { message: error.message }
        : { message: String(error) };

  return {
    isError: true,
    content: [{ type: "text" as const, text: normalized.message }],
    structuredContent: {
      error: normalized,
    },
  };
}
