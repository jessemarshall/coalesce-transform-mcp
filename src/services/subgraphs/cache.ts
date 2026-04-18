import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

import { getCacheDir } from "../../cache-dir.js";

const SUBGRAPH_CACHE_FILE = "subgraphs.json";

export type CachedSubgraph = {
  workspaceID: string;
  id: string;
  name: string;
  steps: string[];
  updatedAt: string;
};

type CacheFile = {
  version: 1;
  entries: CachedSubgraph[];
};

function getCachePath(baseDir?: string): string {
  return join(getCacheDir(baseDir), SUBGRAPH_CACHE_FILE);
}

function readCacheFile(baseDir?: string): CacheFile {
  const path = getCachePath(baseDir);
  if (!existsSync(path)) {
    return { version: 1, entries: [] };
  }
  try {
    const raw = readFileSync(path, "utf8");
    const parsed = JSON.parse(raw);
    if (
      parsed &&
      typeof parsed === "object" &&
      parsed.version === 1 &&
      Array.isArray(parsed.entries)
    ) {
      return parsed as CacheFile;
    }
    process.stderr.write(
      `[subgraph-cache] Ignoring cache at ${path}: unexpected shape — treating as empty\n`
    );
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    process.stderr.write(
      `[subgraph-cache] Corrupt cache at ${path}: ${reason} — treating as empty\n`
    );
  }
  return { version: 1, entries: [] };
}

function writeCacheFile(file: CacheFile, baseDir?: string): void {
  const path = getCachePath(baseDir);
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(file, null, 2), "utf8");
}

/**
 * Best-effort cache write. A failure (permissions, disk full, concurrent writer)
 * must never abort a successful API mutation — the subgraph already exists on
 * the server, the cache is just a convenience lookup that will be refilled by
 * the next resolve call.
 */
export function saveSubgraphToCache(
  entry: Omit<CachedSubgraph, "updatedAt">,
  baseDir?: string
): void {
  try {
    const file = readCacheFile(baseDir);
    const filtered = file.entries.filter(
      (e) =>
        !(e.workspaceID === entry.workspaceID && (e.id === entry.id || e.name === entry.name))
    );
    filtered.push({ ...entry, updatedAt: new Date().toISOString() });
    writeCacheFile({ version: 1, entries: filtered }, baseDir);
  } catch (err) {
    process.stderr.write(
      `[subgraph-cache] Failed to save ${entry.workspaceID}:${entry.name} — ${err instanceof Error ? err.message : String(err)}\n`
    );
  }
}

export function findSubgraphInCache(
  params: { workspaceID: string; name: string },
  baseDir?: string
): CachedSubgraph | null {
  const file = readCacheFile(baseDir);
  const match = file.entries.find(
    (e) => e.workspaceID === params.workspaceID && e.name === params.name
  );
  return match ?? null;
}

export function removeSubgraphFromCache(
  params: { workspaceID: string; id: string },
  baseDir?: string
): void {
  try {
    const file = readCacheFile(baseDir);
    const filtered = file.entries.filter(
      (e) => !(e.workspaceID === params.workspaceID && e.id === params.id)
    );
    if (filtered.length !== file.entries.length) {
      writeCacheFile({ version: 1, entries: filtered }, baseDir);
    }
  } catch (err) {
    process.stderr.write(
      `[subgraph-cache] Failed to remove ${params.workspaceID}:${params.id} — ${err instanceof Error ? err.message : String(err)}\n`
    );
  }
}
