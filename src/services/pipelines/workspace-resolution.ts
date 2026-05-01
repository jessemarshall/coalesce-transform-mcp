import { type CoalesceClient } from "../../client.js";
import { listWorkspaceNodeTypes } from "../workspace/mutations.js";
import { isPlainObject, rethrowNonRecoverableOrServerError, safeErrorMessage } from "../../utils.js";
import { type WorkspaceNodeIndexEntry } from "../shared/node-helpers.js";
import { getWorkspaceNodeIndex } from "../cache/workspace-node-index.js";
import { getCachedOrFetchWorkspaceNodeDetail } from "../cache/workspace-node-detail-index.js";
import {
  type PipelinePlan,
  type ParsedSqlSourceRef,
  type PlannedSourceRef,
  type WorkspaceNodeTypeInventory,
} from "./planning-types.js";
import {
  normalizeSqlIdentifier,
} from "./sql-parsing.js";

// Re-export from plan-builder for external consumers
export {
  buildSelectItemsFromSourceNode,
  expandWildcardSelectItems,
  buildDefaultNodePrefix,
  buildDefaultNodeName,
  buildPlanFromSql,
} from "./plan-builder.js";

function getNodeLocationName(node: Record<string, unknown>): string | null {
  if (typeof node.locationName === "string" && node.locationName.trim().length > 0) {
    return node.locationName;
  }
  return null;
}

export async function resolveSqlRefsToWorkspaceNodes(
  client: CoalesceClient,
  workspaceID: string,
  refs: ParsedSqlSourceRef[]
): Promise<{
  refs: ParsedSqlSourceRef[];
  openQuestions: string[];
  warnings: string[];
  predecessorNodes: Record<string, Record<string, unknown>>;
}> {
  const warnings: string[] = [];
  const openQuestions: string[] = [];
  const predecessorNodes: Record<string, Record<string, unknown>> = {};

  if (refs.length === 0) {
    openQuestions.push(
      "Which upstream Coalesce node(s) should this pipeline build from? Use a top-level FROM/JOIN that names existing workspace nodes (raw table names or {{ ref('LOCATION', 'NODE') }} syntax), or provide sourceNodeIDs."
    );
    return { refs, openQuestions, warnings, predecessorNodes };
  }

  const workspaceNodes = await getWorkspaceNodeIndex(client, workspaceID);
  const nodesByNormalizedName = new Map<string, WorkspaceNodeIndexEntry[]>();
  for (const node of workspaceNodes) {
    const normalized = normalizeSqlIdentifier(node.name);
    const existing = nodesByNormalizedName.get(normalized) ?? [];
    existing.push(node);
    nodesByNormalizedName.set(normalized, existing);
  }

  for (const ref of refs) {
    const matches =
      nodesByNormalizedName.get(normalizeSqlIdentifier(ref.nodeName)) ?? [];
    if (matches.length === 0) {
      openQuestions.push(
        `Could not resolve the SQL source ${ref.nodeName} to a workspace node ID in workspace ${workspaceID}.`
      );
      continue;
    }

    const locationHints = [
      ...(ref.locationName ? [ref.locationName] : []),
      ...ref.locationCandidates,
    ].map(normalizeSqlIdentifier);
    const hintedMatches =
      locationHints.length > 0
        ? matches.filter(
            (entry) =>
              entry.locationName &&
              locationHints.includes(normalizeSqlIdentifier(entry.locationName))
          )
        : [];

    if (hintedMatches.length === 1) {
      ref.nodeID = hintedMatches[0]?.id ?? null;
      if (!ref.locationName && hintedMatches[0]?.locationName) {
        ref.locationName = hintedMatches[0].locationName;
      }
      continue;
    }
    if (hintedMatches.length > 1) {
      openQuestions.push(
        `Multiple workspace nodes matched the SQL source ${ref.nodeName}. Resolve the exact node before creation.`
      );
      continue;
    }

    if (matches.length === 1) {
      ref.nodeID = matches[0]?.id ?? null;
      if (!ref.locationName && matches[0]?.locationName) {
        ref.locationName = matches[0].locationName;
      }
      continue;
    }

    if (matches.length > 1) {
      let detailedMatches: Array<{ match: (typeof matches)[0]; node: Record<string, unknown> | null }>;
      try {
        detailedMatches = await Promise.all(
          matches.map(async (match) => {
            const node = await getCachedOrFetchWorkspaceNodeDetail(
              client,
              workspaceID,
              match.id
            );
            return {
              match,
              node: isPlainObject(node) ? node : null,
            };
          })
        );
      } catch (error) {
        rethrowNonRecoverableOrServerError(error);
        const reason = safeErrorMessage(error);
        warnings.push(`Could not fetch details for candidates of "${ref.nodeName}" (${reason}).`);
        openQuestions.push(
          `Multiple workspace nodes named "${ref.nodeName}" were found but could not be fully inspected. Provide sourceNodeIDs before creation.`
        );
        continue;
      }
      const exactLocationMatches =
        locationHints.length > 0
          ? detailedMatches.filter(
              (candidate) =>
                candidate.node &&
                getNodeLocationName(candidate.node) &&
                locationHints.includes(
                  normalizeSqlIdentifier(getNodeLocationName(candidate.node) ?? "")
                )
            )
          : [];
      if (exactLocationMatches.length === 1) {
        ref.nodeID = exactLocationMatches[0]?.match.id ?? null;
        if (!ref.locationName) {
          ref.locationName = getNodeLocationName(exactLocationMatches[0]?.node ?? {}) ?? "";
        }
        continue;
      }
      if (exactLocationMatches.length > 1) {
        openQuestions.push(
          `Multiple workspace nodes matched the SQL source ${ref.nodeName}. Resolve the exact node before creation.`
        );
        continue;
      }

      if (ref.sourceStyle === "coalesce_ref" && ref.locationName) {
        openQuestions.push(
          `Workspace nodes named ${ref.nodeName} were found, but none matched the requested location ${ref.locationName}.`
        );
        continue;
      }

      openQuestions.push(
        `Multiple workspace nodes named ${ref.nodeName} were found. Qualify the SQL source more clearly or provide sourceNodeIDs before creation.`
      );
      continue;
    }
  }

  for (const ref of refs) {
    if (!ref.nodeID) {
      continue;
    }
    let predecessor: unknown;
    try {
      predecessor = await getCachedOrFetchWorkspaceNodeDetail(
        client,
        workspaceID,
        ref.nodeID
      );
    } catch (error) {
      rethrowNonRecoverableOrServerError(error);
      const reason = safeErrorMessage(error);
      warnings.push(`Could not fetch predecessor node "${ref.nodeName}" (${ref.nodeID}): ${reason}. Column passthrough will not be available.`);
      continue;
    }
    if (!isPlainObject(predecessor)) {
      warnings.push(`Resolved predecessor ${ref.nodeName} did not return an object body.`);
      continue;
    }
    const predecessorLocationName = getNodeLocationName(predecessor);
    if (
      ref.sourceStyle === "coalesce_ref" &&
      predecessorLocationName &&
      normalizeSqlIdentifier(predecessorLocationName) !==
        normalizeSqlIdentifier(ref.locationName)
    ) {
      ref.nodeID = null;
      openQuestions.push(
        `Resolved node ${ref.nodeName} is in location ${predecessorLocationName}, not the requested location ${ref.locationName}.`
      );
      continue;
    }
    if (!ref.locationName && predecessorLocationName) {
      ref.locationName = predecessorLocationName;
    }
    predecessorNodes[ref.nodeID] = predecessor;
  }

  return { refs, openQuestions, warnings, predecessorNodes };
}

export async function getSourceNodesByID(
  client: CoalesceClient,
  workspaceID: string,
  sourceNodeIDs: string[]
): Promise<{
  sourceRefs: PlannedSourceRef[];
  predecessorNodes: Record<string, Record<string, unknown>>;
  openQuestions: string[];
  warnings: string[];
}> {
  const sourceRefs: PlannedSourceRef[] = [];
  const predecessorNodes: Record<string, Record<string, unknown>> = {};
  const openQuestions: string[] = [];
  const warnings: string[] = [];

  for (const sourceNodeID of sourceNodeIDs) {
    const node = await getCachedOrFetchWorkspaceNodeDetail(
      client,
      workspaceID,
      sourceNodeID
    );
    if (!isPlainObject(node)) {
      openQuestions.push(
        `Could not read source node ${sourceNodeID} in workspace ${workspaceID}.`
      );
      continue;
    }
    if (typeof node.name !== "string" || node.name.trim().length === 0) {
      openQuestions.push(`Source node ${sourceNodeID} does not have a usable name.`);
      continue;
    }
    const locationName = getNodeLocationName(node);
    if (!locationName) {
      openQuestions.push(
        `Source node ${node.name} does not expose locationName. Clarify the Coalesce location before generating ref() SQL for this pipeline.`
      );
    }

    predecessorNodes[sourceNodeID] = node;
    sourceRefs.push({
      locationName: locationName ?? "UNKNOWN_LOCATION",
      nodeName: node.name,
      alias: node.name,
      nodeID: sourceNodeID,
    });
  }

  return {
    sourceRefs,
    predecessorNodes,
    openQuestions,
    warnings,
  };
}

function matchesObservedNodeType(
  requestedNodeType: string,
  observedNodeTypes: string[]
): boolean {
  const requestedID = requestedNodeType.includes(":::")
    ? requestedNodeType.split(":::")[1] ?? requestedNodeType
    : requestedNodeType;

  return observedNodeTypes.some((observed) => {
    if (observed === requestedNodeType) {
      return true;
    }
    const observedID = observed.includes(":::") ? observed.split(":::")[1] ?? observed : observed;
    return observedID === requestedID;
  });
}

export async function getWorkspaceNodeTypeInventory(
  client: CoalesceClient,
  workspaceID: string
): Promise<WorkspaceNodeTypeInventory> {
  try {
    const result = await listWorkspaceNodeTypes(client, { workspaceID });
    return {
      nodeTypes: result.nodeTypes ?? [],
      counts: result.counts ?? {},
      total: result.total ?? 0,
      warnings: [],
    };
  } catch (error) {
    // Auth and network errors indicate a broken session — let them propagate
    rethrowNonRecoverableOrServerError(error);
    const reason = safeErrorMessage(error);
    return {
      nodeTypes: [],
      counts: {},
      total: 0,
      warnings: [
        `Observed workspace node types could not be fetched for workspace ${workspaceID} (${reason}). ` +
          `Node type selection will use defaults — use list_workspace_node_types or cache_workspace_nodes to confirm installation before execution.`,
      ],
    };
  }
}

export function applyWorkspaceNodeTypeValidation(
  plan: PipelinePlan,
  inventory: WorkspaceNodeTypeInventory,
  requestedNodeType?: string
): void {
  plan.warnings.push(...inventory.warnings);

  if (inventory.total === 0) {
    return;
  }

  const recommendedTypes: string[] = (plan.nodes ?? [])
    .map((node) => node.nodeType)
    .filter((nodeType) => typeof nodeType === "string" && nodeType.length > 0);

  if (requestedNodeType && requestedNodeType.trim().length > 0) {
    recommendedTypes.push(requestedNodeType);
  }

  const missingTypes = Array.from(new Set(recommendedTypes)).filter(
    (nodeType) => !matchesObservedNodeType(nodeType, inventory.nodeTypes)
  );

  if (missingTypes.length > 0) {
    plan.warnings.push(
      `The following node types were not observed in current workspace nodes: ${missingTypes.join(
        ", "
      )}. This observation is based on existing nodes, not a true installed-type registry. Confirm installation in Coalesce before creating nodes of these types.`
    );
    plan.status = "needs_clarification";
  }
}
