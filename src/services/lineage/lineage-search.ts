import type { LineageCacheEntry } from "./lineage-cache.js";
import { isPlainObject } from "../../utils.js";

// --- Workspace content search ---

export type SearchField = "name" | "nodeType" | "sql" | "columnName" | "columnDataType" | "description" | "config";

const ALL_SEARCH_FIELDS: SearchField[] = ["name", "nodeType", "sql", "columnName", "columnDataType", "description", "config"];

export type WorkspaceSearchParams = {
  query: string;
  fields?: SearchField[];
  nodeType?: string;
  limit?: number;
};

export type SearchMatch = {
  nodeID: string;
  nodeName: string;
  nodeType: string;
  matchedFields: string[];
  matches: Array<{
    field: string;
    /** For column matches, the column name */
    columnName?: string;
    /** Snippet of the matched content (truncated for large values) */
    snippet: string;
  }>;
};

export type WorkspaceSearchResult = {
  query: string;
  fields: SearchField[];
  nodeTypeFilter?: string;
  totalMatches: number;
  returnedCount: number;
  truncated: boolean;
  results: SearchMatch[];
  cacheAge: string;
};

function extractSql(raw: Record<string, unknown>): string | undefined {
  const metadata = isPlainObject(raw.metadata) ? raw.metadata : {};
  if (typeof metadata.query === "string" && metadata.query.length > 0) return metadata.query;
  if (typeof metadata.sqlQuery === "string" && metadata.sqlQuery.length > 0) return metadata.sqlQuery;
  const sourceMapping = Array.isArray(metadata.sourceMapping) ? metadata.sourceMapping : [];
  for (const mapping of sourceMapping) {
    if (isPlainObject(mapping) && typeof mapping.query === "string" && mapping.query.length > 0) {
      return mapping.query;
    }
  }
  return undefined;
}

function extractDescription(raw: Record<string, unknown>): string | undefined {
  if (typeof raw.description === "string" && raw.description.length > 0) return raw.description;
  const metadata = isPlainObject(raw.metadata) ? raw.metadata : {};
  if (typeof metadata.description === "string" && metadata.description.length > 0) return metadata.description;
  return undefined;
}

function extractConfigString(raw: Record<string, unknown>): string | undefined {
  const config = isPlainObject(raw.config) ? raw.config : {};
  const keys = Object.keys(config);
  if (keys.length === 0) return undefined;
  return JSON.stringify(config);
}

function snippet(text: string, query: string, maxLen: number = 200): string {
  const lowerText = text.toLowerCase();
  const lowerQuery = query.toLowerCase();
  const idx = lowerText.indexOf(lowerQuery);
  if (idx === -1) {
    return text.length > maxLen ? `${text.slice(0, maxLen)}…` : text;
  }
  const start = Math.max(0, idx - 60);
  const end = Math.min(text.length, idx + query.length + 60);
  let s = text.slice(start, end);
  if (start > 0) s = `…${s}`;
  if (end < text.length) s = `${s}…`;
  return s;
}

export function searchWorkspaceContent(
  cache: LineageCacheEntry,
  params: WorkspaceSearchParams
): WorkspaceSearchResult {
  const { query, limit = 50 } = params;
  const fields = params.fields && params.fields.length > 0 ? params.fields : ALL_SEARCH_FIELDS;

  if (query.length === 0) {
    throw new Error("Search query must not be empty");
  }

  const lowerQuery = query.toLowerCase();
  const allMatches: SearchMatch[] = [];

  for (const node of cache.nodes.values()) {
    if (params.nodeType && node.nodeType.toLowerCase() !== params.nodeType.toLowerCase()) {
      continue;
    }

    const matchedFields: string[] = [];
    const matches: SearchMatch["matches"] = [];

    if (fields.includes("name") && node.name.toLowerCase().includes(lowerQuery)) {
      matchedFields.push("name");
      matches.push({ field: "name", snippet: node.name });
    }

    if (fields.includes("nodeType") && node.nodeType.toLowerCase().includes(lowerQuery)) {
      matchedFields.push("nodeType");
      matches.push({ field: "nodeType", snippet: node.nodeType });
    }

    if (fields.includes("sql")) {
      const sql = extractSql(node.raw);
      if (sql && sql.toLowerCase().includes(lowerQuery)) {
        matchedFields.push("sql");
        matches.push({ field: "sql", snippet: snippet(sql, query) });
      }
    }

    if (fields.includes("description")) {
      const desc = extractDescription(node.raw);
      if (desc && desc.toLowerCase().includes(lowerQuery)) {
        matchedFields.push("description");
        matches.push({ field: "description", snippet: snippet(desc, query) });
      }
    }

    if (fields.includes("config")) {
      const configStr = extractConfigString(node.raw);
      if (configStr && configStr.toLowerCase().includes(lowerQuery)) {
        matchedFields.push("config");
        matches.push({ field: "config", snippet: snippet(configStr, query) });
      }
    }

    if (fields.includes("columnName")) {
      for (const col of node.columns) {
        if (col.name.toLowerCase().includes(lowerQuery)) {
          if (!matchedFields.includes("columnName")) matchedFields.push("columnName");
          matches.push({ field: "columnName", columnName: col.name, snippet: col.name });
        }
      }
    }

    if (fields.includes("columnDataType")) {
      for (const col of node.columns) {
        if (col.dataType && col.dataType.toLowerCase().includes(lowerQuery)) {
          if (!matchedFields.includes("columnDataType")) matchedFields.push("columnDataType");
          matches.push({ field: "columnDataType", columnName: col.name, snippet: col.dataType });
        }
      }
    }

    if (matches.length > 0) {
      allMatches.push({
        nodeID: node.id,
        nodeName: node.name,
        nodeType: node.nodeType,
        matchedFields,
        matches,
      });
    }
  }

  const results = allMatches.slice(0, limit);

  const ageMs = Date.now() - cache.cachedAt;
  const ageMin = Math.round(ageMs / 60_000);

  return {
    query,
    fields,
    ...(params.nodeType ? { nodeTypeFilter: params.nodeType } : {}),
    totalMatches: allMatches.length,
    returnedCount: results.length,
    truncated: allMatches.length > limit,
    results,
    cacheAge: ageMin < 1 ? "< 1 minute" : `${ageMin} minute${ageMin === 1 ? "" : "s"}`,
  };
}
