import { type CoalesceClient } from "../../client.js";
import { listWorkspaceNodes } from "../../coalesce/api/nodes.js";
import { isPlainObject } from "../../utils.js";
import { normalizeSqlIdentifier } from "./sql-parsing.js";
import { type WorkspaceNodeIndexEntry } from "../shared/node-helpers.js";
import { WORKSPACE_NODE_PAGE_LIMIT } from "./planning-types.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type ResolvedEntityCandidate = { id: string; name: string; locationName: string | null };

export type ResolvedEntity =
  | {
      rawName: string;
      confidence: "exact" | "fuzzy";
      resolvedNodeID: string;
      resolvedNodeName: string;
      resolvedLocationName: string | null;
      candidates: ResolvedEntityCandidate[];
    }
  | {
      rawName: string;
      confidence: "unresolved";
      resolvedNodeID: null;
      resolvedNodeName: null;
      resolvedLocationName: null;
      candidates: ResolvedEntityCandidate[];
    };

// ---------------------------------------------------------------------------
// Workspace node listing
// ---------------------------------------------------------------------------

const MAX_PAGES = 500;

async function listAllWorkspaceNodes(
  client: CoalesceClient,
  workspaceID: string
): Promise<WorkspaceNodeIndexEntry[]> {
  const nodes: WorkspaceNodeIndexEntry[] = [];
  const seenCursors = new Set<string>();
  let next: string | undefined;
  let isFirstPage = true;
  let pageCount = 0;

  while (isFirstPage || next) {
    if (++pageCount > MAX_PAGES) {
      throw new Error(
        `Workspace node pagination exceeded ${MAX_PAGES} pages (${nodes.length} nodes fetched). ` +
        `This likely indicates an API bug. The nodes fetched so far are not returned.`
      );
    }
    const response = await listWorkspaceNodes(client, {
      workspaceID,
      limit: WORKSPACE_NODE_PAGE_LIMIT,
      orderBy: "id",
      ...(next ? { startingFrom: next } : {}),
    });

    if (!isPlainObject(response)) {
      throw new Error("Workspace node list response was not an object");
    }

    if (Array.isArray(response.data)) {
      for (const item of response.data) {
        if (!isPlainObject(item) || typeof item.id !== "string" || typeof item.name !== "string") {
          continue;
        }
        nodes.push({
          id: item.id,
          name: item.name,
          nodeType: typeof item.nodeType === "string" ? item.nodeType : null,
          locationName: typeof item.locationName === "string" ? item.locationName : null,
        });
      }
    }

    const responseNext =
      typeof response.next === "string" && response.next.trim().length > 0
        ? response.next
        : typeof response.next === "number"
          ? String(response.next)
          : undefined;
    if (responseNext) {
      if (seenCursors.has(responseNext)) {
        throw new Error(`Workspace node pagination repeated cursor ${responseNext}`);
      }
      seenCursors.add(responseNext);
    }

    next = responseNext;
    isFirstPage = false;
  }

  return nodes;
}

// ---------------------------------------------------------------------------
// Fuzzy matching
// ---------------------------------------------------------------------------

function fuzzyMatchScore(
  queryNormalized: string,
  candidateNormalized: string
): number {
  // Exact match
  if (queryNormalized === candidateNormalized) return 100;

  // Candidate ends with the query (e.g. "CUSTOMERS" matches "STG_CUSTOMERS")
  if (candidateNormalized.endsWith(`_${queryNormalized}`)) return 90;

  // Candidate starts with the query
  if (candidateNormalized.startsWith(`${queryNormalized}_`)) return 80;

  // Query is contained in candidate
  if (candidateNormalized.includes(queryNormalized)) return 70;

  // Strip common prefixes and compare
  const stripped = candidateNormalized.replace(
    /^(SRC[_-]?|STG[_-]?|DIM[_-]?|FACT[_-]?|FCT[_-]?|INT[_-]?|WORK[_-]?|WRK[_-]?|VW[_-]?|RAW[_-]?)/,
    ""
  );
  if (stripped === queryNormalized) return 85;

  // Pluralization — try adding/removing trailing S
  const queryPlural = queryNormalized.endsWith("S")
    ? queryNormalized.slice(0, -1)
    : `${queryNormalized}S`;
  if (candidateNormalized === queryPlural || stripped === queryPlural) return 82;
  if (candidateNormalized.endsWith(`_${queryPlural}`)) return 78;

  return 0;
}

// ---------------------------------------------------------------------------
// Entity resolution
// ---------------------------------------------------------------------------

export async function resolveIntentEntities(
  client: CoalesceClient,
  workspaceID: string,
  entityNames: string[]
): Promise<ResolvedEntity[]> {
  const workspaceNodes = await listAllWorkspaceNodes(client, workspaceID);
  const resolved: ResolvedEntity[] = [];

  for (const rawName of entityNames) {
    const queryNormalized = normalizeSqlIdentifier(rawName);

    const scored = workspaceNodes
      .map((node) => ({
        node,
        score: fuzzyMatchScore(queryNormalized, normalizeSqlIdentifier(node.name)),
      }))
      .filter(({ score }) => score > 0)
      .sort((a, b) => b.score - a.score);

    if (scored.length === 0) {
      resolved.push({
        rawName,
        resolvedNodeID: null,
        resolvedNodeName: null,
        resolvedLocationName: null,
        confidence: "unresolved",
        candidates: [],
      });
      continue;
    }

    const best = scored[0]!;
    const topTier = scored.filter(({ score }) => score === best.score);

    if (topTier.length === 1) {
      resolved.push({
        rawName,
        resolvedNodeID: best.node.id,
        resolvedNodeName: best.node.name,
        resolvedLocationName: best.node.locationName,
        confidence: best.score >= 85 ? "exact" : "fuzzy",
        candidates: scored.slice(0, 5).map(({ node }) => ({
          id: node.id,
          name: node.name,
          locationName: node.locationName,
        })),
      });
    } else {
      resolved.push({
        rawName,
        resolvedNodeID: null,
        resolvedNodeName: null,
        resolvedLocationName: null,
        confidence: "unresolved",
        candidates: topTier.map(({ node }) => ({
          id: node.id,
          name: node.name,
          locationName: node.locationName,
        })),
      });
    }
  }

  return resolved;
}
