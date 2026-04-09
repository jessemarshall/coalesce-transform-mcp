import { randomUUID } from "node:crypto";
import { mkdirSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import {
  buildCacheResourceLink,
  CACHE_DIR_NAME,
  type CacheResourceLink,
} from "../cache-dir.js";
import { z } from "zod";
import { isPlainObject } from "../utils.js";
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
};

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

function cleanupStaleAutoCacheFiles(autoCacheDir: string): void {
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
          const reason = error instanceof Error ? error.message : String(error);
          process.stderr.write(`[auto-cache] Failed to delete stale file ${file}: ${reason}\n`);
        }
      }
    }

    // Phase 2: hard cap — if the directory could still exceed the limit
    // (original count was high enough), re-read and evict oldest beyond the cap.
    if (files.length > AUTO_CACHE_MAX_FILES) {
      const currentFiles = readdirSync(autoCacheDir)
        .filter((f) => f.endsWith(".json"))
        .sort();
      for (const file of currentFiles.slice(0, currentFiles.length - AUTO_CACHE_MAX_FILES)) {
        try {
          unlinkSync(join(autoCacheDir, file));
        } catch (err) {
          const reason = err instanceof Error ? err.message : String(err);
          process.stderr.write(`[auto-cache] Phase 2 eviction failed for ${file}: ${reason}\n`);
        }
      }
    }
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    process.stderr.write(`[auto-cache] Stale file cleanup failed for ${autoCacheDir}: ${reason}\n`);
  }
}

function buildAutoCacheFilePath(
  toolName: string,
  cachedAt: string,
  baseDir: string
): string {
  const directory = join(baseDir, CACHE_DIR_NAME, "auto-cache");
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
  const baseDir = options.baseDir ?? process.cwd();
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
    filePath = buildAutoCacheFilePath(toolName, cachedAt, baseDir);
    writeFileSync(filePath, `${text}\n`, "utf8");
    cleanupStaleAutoCacheFiles(dirname(filePath));
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
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
