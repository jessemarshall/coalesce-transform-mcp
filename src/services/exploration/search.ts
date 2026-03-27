import type { CoalesceClient } from "../../client.js";
import { listWorkspaces } from "../../coalesce/api/workspaces.js";
import { listWorkspaceNodes } from "../../coalesce/api/nodes.js";

export interface TableReference {
  database?: string;
  schema?: string;
  table: string;
}

export interface CoalesceNodeMatch {
  nodeID: string;
  name: string;
  database?: string;
  schema?: string;
  workspaceID: string;
  nodeType?: string;
}

export interface SkippedWorkspace {
  id: string;
  reason: string;
}

export interface CoalesceSearchResult {
  found: boolean;
  matches: CoalesceNodeMatch[];
  searchedWorkspaces: string[];
  skippedWorkspaces: SkippedWorkspace[];
}

/**
 * Parses a dotted table reference like "RAW.PUBLIC.CUSTOMERS" into components.
 * For 2-part refs (ambiguous in Snowflake), sets both database and schema
 * so matching works regardless of which field the node uses.
 * Supports: TABLE, DB_OR_SCHEMA.TABLE, DATABASE.SCHEMA.TABLE
 * Returns null if the input doesn't look like a table reference.
 */
export function parseTableReference(input: string): TableReference | null {
  const cleaned = input.trim().replace(/["`]/g, "");
  if (!cleaned || /\s/.test(cleaned)) return null;

  const parts = cleaned.split(".");
  if (parts.length === 1) {
    return { table: parts[0].toUpperCase() };
  }
  if (parts.length === 2) {
    // 2-part is ambiguous: could be DATABASE.TABLE or SCHEMA.TABLE
    // Set both so matchesTableRef can check against either field
    const qualifier = parts[0].toUpperCase();
    return {
      database: qualifier,
      schema: qualifier,
      table: parts[1].toUpperCase(),
    };
  }
  if (parts.length === 3) {
    return {
      database: parts[0].toUpperCase(),
      schema: parts[1].toUpperCase(),
      table: parts[2].toUpperCase(),
    };
  }
  return null;
}

/**
 * Extracts a table reference from a natural language question.
 * Looks for patterns like DATABASE.SCHEMA.TABLE or DATABASE.TABLE.
 */
export function extractTableReference(question: string): string | null {
  // Match dotted identifiers (2-3 parts) — DATABASE.SCHEMA.TABLE or SCHEMA.TABLE
  const dottedMatch = question.match(
    /(?:^|\s|['"`(])([A-Za-z_]\w*\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)(?:[\s,;?!'"`)]|$)/
  );
  if (dottedMatch) return dottedMatch[1];

  // Match single identifiers that look like table names (ALL_CAPS with underscores)
  const upperMatch = question.match(
    /(?:^|\s)([A-Z][A-Z0-9_]{2,})(?:[\s,;?!'"`)]|$)/
  );
  if (upperMatch) return upperMatch[1];

  return null;
}

/**
 * Searches Coalesce workspaces for nodes matching a table reference.
 * Throws if workspace listing fails entirely.
 * Skips individual workspaces on access-denied (403/404) but tracks them.
 * Throws if ALL workspaces fail with non-access errors.
 */
export async function searchCoalesceForTable(
  client: CoalesceClient,
  tableRef: TableReference,
  workspaceID?: string
): Promise<CoalesceSearchResult> {
  const searchedWorkspaces: string[] = [];
  const skippedWorkspaces: SkippedWorkspace[] = [];
  const matches: CoalesceNodeMatch[] = [];

  let workspaceIDs: string[];

  if (workspaceID) {
    workspaceIDs = [workspaceID];
  } else {
    try {
      const workspacesResponse = (await listWorkspaces(client)) as {
        data?: Array<{ id: string }>;
      };
      workspaceIDs = (workspacesResponse.data ?? []).map((w) => w.id);
    } catch (error) {
      throw new Error(
        `Failed to list Coalesce workspaces: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  let successCount = 0;

  for (const wsID of workspaceIDs) {
    searchedWorkspaces.push(wsID);
    try {
      const nodesResponse = (await listWorkspaceNodes(client, {
        workspaceID: wsID,
      })) as { data?: Array<Record<string, unknown>> };

      successCount++;
      const nodes = nodesResponse.data ?? [];
      for (const node of nodes) {
        if (matchesTableRef(node, tableRef)) {
          matches.push({
            nodeID: String(node.id ?? ""),
            name: String(node.name ?? ""),
            database: node.database as string | undefined,
            schema: node.schema as string | undefined,
            workspaceID: wsID,
            nodeType: node.nodeType as string | undefined,
          });
        }
      }
    } catch (error: unknown) {
      const status = (error as { status?: number }).status;
      if (status === 403 || status === 404) {
        skippedWorkspaces.push({ id: wsID, reason: "access denied" });
      } else {
        skippedWorkspaces.push({
          id: wsID,
          reason: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  // If ALL workspaces failed (and we had workspaces to search), throw
  if (successCount === 0 && workspaceIDs.length > 0 && skippedWorkspaces.length === workspaceIDs.length) {
    throw new Error(
      `Failed to search any Coalesce workspace. Errors: ${JSON.stringify(skippedWorkspaces)}`
    );
  }

  return { found: matches.length > 0, matches, searchedWorkspaces, skippedWorkspaces };
}

/**
 * Matches a node against a table reference.
 * For 2-part refs where database and schema are the same value,
 * matches if the qualifier matches EITHER the node's database OR schema.
 */
function matchesTableRef(
  node: Record<string, unknown>,
  ref: TableReference
): boolean {
  const nodeName = String(node.name ?? "").toUpperCase();
  const nodeDb = String(node.database ?? "").toUpperCase();
  const nodeSchema = String(node.schema ?? "").toUpperCase();

  if (nodeName !== ref.table) return false;

  // For 2-part refs: database and schema are the same value (the qualifier).
  // Match if qualifier matches EITHER the node's database OR schema.
  if (ref.database && ref.schema && ref.database === ref.schema) {
    const qualifier = ref.database;
    if (nodeDb && nodeSchema) {
      // Node has both fields — qualifier must match at least one
      return nodeDb === qualifier || nodeSchema === qualifier;
    }
    if (nodeDb) return nodeDb === qualifier;
    if (nodeSchema) return nodeSchema === qualifier;
    // Node has neither — match on name alone
    return true;
  }

  // For 3-part refs or explicit database/schema
  if (ref.database && nodeDb && nodeDb !== ref.database) return false;
  if (ref.schema && nodeSchema && nodeSchema !== ref.schema) return false;

  return true;
}
