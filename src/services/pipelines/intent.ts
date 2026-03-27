import { CoalesceApiError, type CoalesceClient } from "../../client.js";
import { getWorkspaceNode, listWorkspaceNodes } from "../../coalesce/api/nodes.js";
import { isPlainObject, uniqueInOrder } from "../../utils.js";
import {
  normalizeSqlIdentifier,
  getColumnNamesFromNode,
  type PlannedPipelineNode,
} from "./planning.js";
import {
  selectPipelineNodeType,
  type PipelineNodeTypeFamily,
  type PipelineNodeTypeSelection,
} from "./node-type-selection.js";
import { listWorkspaceNodeTypes } from "../workspace/mutations.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type IntentOperation = "stage" | "join" | "aggregate" | "union";

type IntentColumn = {
  name: string;
  aggregateFunction: string | null;
  expression: string | null;
};

type IntentStep = {
  operation: IntentOperation;
  entityNames: string[];
  targetName: string | null;
  columns: IntentColumn[];
  groupByColumns: string[];
  filters: string[];
  joinKey: string | null;
  joinType: "INNER" | "LEFT" | "FULL OUTER" | null;
};

export type ParsedIntent = {
  steps: IntentStep[];
  rawIntent: string;
  warnings: string[];
  openQuestions: string[];
};

type ResolvedEntityCandidate = { id: string; name: string; locationName: string | null };

type ResolvedEntity =
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

type WorkspaceNodeEntry = {
  id: string;
  name: string;
  nodeType: string | null;
  locationName: string | null;
};

export type IntentPipelineResult = {
  status: "ready" | "needs_clarification" | "needs_entity_resolution";
  intent: ParsedIntent;
  resolvedEntities: ResolvedEntity[];
  plan: Record<string, unknown> | null;
  openQuestions: string[];
  warnings: string[];
};

// ---------------------------------------------------------------------------
// Intent parsing — pure text analysis, no API calls
// ---------------------------------------------------------------------------

const AGGREGATE_PATTERNS: Array<{
  pattern: RegExp;
  fn: string;
}> = [
  { pattern: /\b(?:total|sum(?:\s+of)?)\s+(\w+)/gi, fn: "SUM" },
  { pattern: /\b(?:count(?:\s+of)?)\s+(\w+)/gi, fn: "COUNT" },
  { pattern: /\b(?:average|avg(?:\s+of)?)\s+(\w+)/gi, fn: "AVG" },
  { pattern: /\b(?:max(?:imum)?(?:\s+of)?)\s+(\w+)/gi, fn: "MAX" },
  { pattern: /\b(?:min(?:imum)?(?:\s+of)?)\s+(\w+)/gi, fn: "MIN" },
];

const JOIN_KEYWORDS = /\b(?:combine|join|merge|link|connect|match)\b/i;
const AGGREGATE_KEYWORDS = /\b(?:aggregate|group|sum|total|count|average|avg|rollup|summarize|summarise)\b/i;
const FILTER_KEYWORDS = /\b(?:filter|where|only|exclude|remove|active|inactive)\b/i;
const UNION_KEYWORDS = /\b(?:union|stack|append|combine\s+all)\b/i;
const STAGE_KEYWORDS = /\b(?:stage|load|ingest|source|land|raw)\b/i;

const GROUP_BY_PATTERN = /\b(?:(?:group|aggregate|summarize|summarise|rollup)\s+by|per|by)\s+([\w\s,]+?)(?:\s+(?:and|then|with|from|where|filter)|$)/gi;

const FILTER_PATTERN = /\b(?:filter(?:ed)?(?:\s+(?:to|for|by|on))?|where|only(?:\s+(?:include|keep|show))?)\s+([\w\s=<>!']+?)(?:\s+(?:and\s+(?:group|aggregate|join)|then|from)|$)/gi;

// Matches "on COLUMN_NAME" or "using COLUMN_NAME" near join context
const JOIN_ON_PATTERN = /\b(?:(?:left|right|inner|full|outer|cross)\s+)?(?:join|combine|merge|link|connect|match)\s+[\w_]+\s+(?:and|with|to)\s+[\w_]+\s+(?:on|using)\s+([\w_]+)/gi;

const ENTITY_PATTERNS = [
  // "join X and Y", "combine X with Y", "left join X and Y"
  /\b(?:(?:left|right|inner|full|outer|cross)\s+)?(?:join|combine|merge|link|connect|match)\s+([\w_]+)\s+(?:and|with|to)\s+([\w_]+)/gi,
  // "from X and Y"
  /\bfrom\s+([\w_]+)\s+(?:and|,)\s+([\w_]+)/gi,
  // "stage/load/ingest X" — single entity after staging verb
  /\b(?:stage|load|ingest)\s+(?:the\s+)?(?:raw\s+)?([\w_]+)/gi,
  // standalone table-like names (2+ uppercase chars with underscores)
  /\b([A-Z][A-Z0-9_]{2,})\b/g,
];

function extractEntityNames(intent: string): string[] {
  const entities = new Set<string>();

  // Try structured patterns (join/combine, from, stage/load)
  for (const pattern of ENTITY_PATTERNS.slice(0, 3)) {
    let match: RegExpExecArray | null;
    const re = new RegExp(pattern.source, pattern.flags);
    while ((match = re.exec(intent)) !== null) {
      if (match[1]) entities.add(match[1].toUpperCase());
      if (match[2]) entities.add(match[2].toUpperCase());
    }
  }

  // If we found structured entities, return them
  if (entities.size > 0) {
    return Array.from(entities);
  }

  // Fall back to standalone uppercase identifiers (index 3)
  const uppercasePattern = ENTITY_PATTERNS[3]!;
  let match: RegExpExecArray | null;
  const re = new RegExp(uppercasePattern.source, uppercasePattern.flags);
  // Common SQL/English words to exclude
  const STOP_WORDS = new Set([
    "SUM", "COUNT", "AVG", "MAX", "MIN", "GROUP", "TOTAL", "FILTER",
    "WHERE", "JOIN", "AND", "FROM", "INTO", "INNER", "LEFT", "FULL",
    "OUTER", "UNION", "ALL", "SELECT", "WITH", "THE", "FOR",
  ]);
  while ((match = re.exec(intent)) !== null) {
    if (match[1] && !STOP_WORDS.has(match[1])) {
      entities.add(match[1]);
    }
  }

  return Array.from(entities);
}

function extractAggregateColumns(intent: string): IntentColumn[] {
  const columns: IntentColumn[] = [];
  const seen = new Set<string>();

  for (const { pattern, fn } of AGGREGATE_PATTERNS) {
    let match: RegExpExecArray | null;
    const re = new RegExp(pattern.source, pattern.flags);
    while ((match = re.exec(intent)) !== null) {
      const colName = match[1]?.toUpperCase();
      if (colName && !seen.has(`${fn}:${colName}`)) {
        seen.add(`${fn}:${colName}`);
        columns.push({
          name: `${fn}_${colName}`,
          aggregateFunction: fn,
          expression: `${fn}(${colName})`,
        });
      }
    }
  }

  return columns;
}

function extractGroupByColumns(intent: string): string[] {
  const columns: string[] = [];
  const seen = new Set<string>();

  let match: RegExpExecArray | null;
  const re = new RegExp(GROUP_BY_PATTERN.source, GROUP_BY_PATTERN.flags);
  while ((match = re.exec(intent)) !== null) {
    if (match[1]) {
      const parts = match[1].split(/[,\s]+/).filter((p) => p.length > 0);
      for (const part of parts) {
        const col = part.toUpperCase().replace(/[^A-Z0-9_]/g, "");
        if (col.length > 0 && !seen.has(col)) {
          seen.add(col);
          columns.push(col);
        }
      }
    }
  }

  return columns;
}

function extractFilters(intent: string): string[] {
  const filters: string[] = [];

  let match: RegExpExecArray | null;
  const re = new RegExp(FILTER_PATTERN.source, FILTER_PATTERN.flags);
  while ((match = re.exec(intent)) !== null) {
    if (match[1]) {
      const filter = match[1].trim();
      if (filter.length > 2) {
        filters.push(filter);
      }
    }
  }

  return filters;
}

function extractJoinKey(intent: string): string | null {
  let match: RegExpExecArray | null;
  const re = new RegExp(JOIN_ON_PATTERN.source, JOIN_ON_PATTERN.flags);
  while ((match = re.exec(intent)) !== null) {
    if (match[1]) {
      return match[1].toUpperCase();
    }
  }
  return null;
}

function detectJoinType(intent: string): "INNER" | "LEFT" | "FULL OUTER" | null {
  if (/\bleft\s+(?:outer\s+)?join\b/i.test(intent)) return "LEFT";
  if (/\bfull\s+(?:outer\s+)?join\b/i.test(intent)) return "FULL OUTER";
  if (/\b(?:inner\s+)?join\b/i.test(intent)) return "INNER";
  if (JOIN_KEYWORDS.test(intent)) return "INNER";
  return null;
}

export function parseIntent(intentText: string): ParsedIntent {
  const warnings: string[] = [];
  const openQuestions: string[] = [];
  const steps: IntentStep[] = [];

  const entityNames = extractEntityNames(intentText);
  const hasJoin = JOIN_KEYWORDS.test(intentText);
  const hasAggregate = AGGREGATE_KEYWORDS.test(intentText);
  const hasFilter = FILTER_KEYWORDS.test(intentText);
  const hasUnion = UNION_KEYWORDS.test(intentText);
  const hasStage = STAGE_KEYWORDS.test(intentText) && !hasJoin && !hasAggregate;

  if (entityNames.length === 0) {
    openQuestions.push(
      "Could not identify source tables or nodes from the description. " +
      "Please mention the table/node names explicitly (e.g., 'combine CUSTOMERS and ORDERS')."
    );
  }

  // Build steps based on detected operations
  if (hasUnion) {
    steps.push({
      operation: "union",
      entityNames,
      targetName: null,
      columns: [],
      groupByColumns: [],
      filters: [],
      joinKey: null,
      joinType: null,
    });
  } else if (hasJoin && entityNames.length < 2) {
    openQuestions.push(
      `A join operation requires at least two source tables, but only ${entityNames.length === 0 ? "none were" : `"${entityNames[0]}" was`} found. ` +
      `Please mention both tables (e.g., 'join CUSTOMERS and ORDERS on CUSTOMER_ID').`
    );
  } else if (hasJoin && entityNames.length >= 2) {
    const joinKey = extractJoinKey(intentText);
    const joinType = detectJoinType(intentText);

    steps.push({
      operation: "join",
      entityNames,
      targetName: null,
      columns: [],
      groupByColumns: [],
      filters: [],
      joinKey,
      joinType,
    });

    if (!joinKey) {
      openQuestions.push(
        `What column should be used to join ${entityNames.join(" and ")}? ` +
        `(e.g., 'join on CUSTOMER_ID')`
      );
    }
  } else if (hasStage || (!hasJoin && !hasAggregate && entityNames.length > 0)) {
    for (const entityName of entityNames) {
      steps.push({
        operation: "stage",
        entityNames: [entityName],
        targetName: null,
        columns: [],
        groupByColumns: [],
        filters: [],
        joinKey: null,
        joinType: null,
      });
    }
  }

  // Add aggregate step if detected (may follow a join or stand alone)
  if (hasAggregate) {
    const aggregateColumns = extractAggregateColumns(intentText);
    const groupByColumns = extractGroupByColumns(intentText);

    if (groupByColumns.length === 0 && aggregateColumns.length > 0) {
      openQuestions.push(
        "Aggregation detected but no GROUP BY columns found. " +
        "Which columns should be used for grouping? (e.g., 'group by REGION, CATEGORY')"
      );
    }

    // If there's already a join step, aggregation follows it
    // If there's no join, aggregate from the entities directly
    const aggEntities = steps.length > 0
      ? [] // will reference previous step's output
      : entityNames;

    steps.push({
      operation: "aggregate",
      entityNames: aggEntities,
      targetName: null,
      columns: aggregateColumns,
      groupByColumns,
      filters: [],
      joinKey: null,
      joinType: null,
    });
  }

  // Add filter to the last step if detected
  if (hasFilter && steps.length > 0) {
    const filters = extractFilters(intentText);
    const lastStep = steps[steps.length - 1]!;
    lastStep.filters.push(...filters);
  }

  if (steps.length === 0) {
    openQuestions.push(
      "Could not determine what pipeline operations to perform. " +
      "Please describe the transformation (e.g., 'join CUSTOMERS and ORDERS on CUSTOMER_ID, then aggregate total REVENUE by REGION')."
    );
  }

  return {
    steps,
    rawIntent: intentText,
    warnings,
    openQuestions,
  };
}

// ---------------------------------------------------------------------------
// Entity resolution — fuzzy match intent names to workspace nodes
// ---------------------------------------------------------------------------

const WORKSPACE_NODE_PAGE_LIMIT = 200;

async function listAllWorkspaceNodes(
  client: CoalesceClient,
  workspaceID: string
): Promise<WorkspaceNodeEntry[]> {
  const nodes: WorkspaceNodeEntry[] = [];
  const seenCursors = new Set<string>();
  let next: string | undefined;
  let isFirstPage = true;

  while (isFirstPage || next) {
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

export async function resolveIntentEntities(
  client: CoalesceClient,
  workspaceID: string,
  entityNames: string[]
): Promise<ResolvedEntity[]> {
  const workspaceNodes = await listAllWorkspaceNodes(client, workspaceID);
  const resolved: ResolvedEntity[] = [];

  for (const rawName of entityNames) {
    const queryNormalized = normalizeSqlIdentifier(rawName);

    // Score all workspace nodes
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
      // Ambiguous — multiple nodes at the same score
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

// ---------------------------------------------------------------------------
// Plan assembly — combine parsed intent + resolved entities into PipelinePlan
// ---------------------------------------------------------------------------

async function getWorkspaceNodeTypeInventory(
  client: CoalesceClient,
  workspaceID: string
): Promise<{ nodeTypes: string[]; counts: Record<string, number>; total: number; warnings: string[] }> {
  try {
    const result = await listWorkspaceNodeTypes(client, { workspaceID });
    return {
      nodeTypes: result.nodeTypes ?? [],
      counts: result.counts ?? {},
      total: result.total ?? 0,
      warnings: [],
    };
  } catch (error) {
    if (error instanceof CoalesceApiError && [401, 403, 503].includes(error.status)) {
      throw error;
    }
    const reason = error instanceof Error ? error.message : String(error);
    return {
      nodeTypes: [],
      counts: {},
      total: 0,
      warnings: [
        `Workspace node types could not be fetched (${reason}). Confirm installation before creating nodes.`,
      ],
    };
  }
}

function buildSelectItemsForPassthrough(
  sourceNodeID: string,
  sourceNodeName: string,
  node: Record<string, unknown>
): PlannedPipelineNode["selectItems"] {
  return getColumnNamesFromNode(node).map((columnName) => ({
    expression: `${sourceNodeName}.${columnName}`,
    outputName: columnName,
    sourceNodeAlias: sourceNodeName,
    sourceNodeName,
    sourceNodeID,
    sourceColumnName: columnName,
    kind: "column" as const,
    supported: true,
  }));
}

function buildNodePrefix(
  family: PipelineNodeTypeFamily | null | undefined,
  shortName: string | null | undefined
): string {
  if (shortName && shortName.trim().length > 0) {
    return shortName.trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_");
  }
  switch (family) {
    case "stage": return "STG";
    case "persistent-stage": return "PSTG";
    case "view": return "VW";
    case "work": return "WRK";
    case "dimension": return "DIM";
    case "fact": return "FACT";
    case "hub": return "HUB";
    case "satellite": return "SAT";
    case "link": return "LNK";
    default: return "NODE";
  }
}

function stripNodePrefix(name: string): string {
  return name.replace(
    /^(SRC[_-]?|STG[_-]?|DIM[_-]?|FACT[_-]?|FCT[_-]?|INT[_-]?|WORK[_-]?|WRK[_-]?|VW[_-]?|RAW[_-]?)/i,
    ""
  );
}

export async function buildPipelinePlanFromIntent(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    intent: string;
    targetName?: string;
    targetNodeType?: string;
    repoPath?: string;
    locationName?: string;
    database?: string;
    schema?: string;
  }
): Promise<IntentPipelineResult> {
  const parsed = parseIntent(params.intent);

  // Collect all entity names across steps
  const allEntityNames = uniqueInOrder(
    parsed.steps.flatMap((step) => step.entityNames)
  );

  // Resolve entities to workspace nodes
  const resolvedEntities = await resolveIntentEntities(
    client,
    params.workspaceID,
    allEntityNames
  );

  // Check for unresolved entities
  const unresolvedEntities = resolvedEntities.filter(
    (e) => e.confidence === "unresolved"
  );

  const openQuestions = [...parsed.openQuestions];
  const warnings = [...parsed.warnings];

  for (const unresolved of unresolvedEntities) {
    if (unresolved.candidates.length > 0) {
      openQuestions.push(
        `"${unresolved.rawName}" matched multiple workspace nodes: ${unresolved.candidates
          .map((c) => `${c.name} (${c.locationName ?? "no location"})`)
          .join(", ")}. Which one should be used?`
      );
    } else {
      openQuestions.push(
        `Could not find a workspace node matching "${unresolved.rawName}". ` +
        `Use list-workspace-nodes to check available nodes.`
      );
    }
  }

  if (unresolvedEntities.length > 0) {
    return {
      status: "needs_entity_resolution",
      intent: parsed,
      resolvedEntities,
      plan: null,
      openQuestions,
      warnings,
    };
  }

  if (parsed.steps.length === 0) {
    return {
      status: "needs_clarification",
      intent: parsed,
      resolvedEntities,
      plan: null,
      openQuestions,
      warnings,
    };
  }

  // Build an entity lookup for resolved nodes
  const entityLookup = new Map<string, ResolvedEntity>();
  for (const entity of resolvedEntities) {
    entityLookup.set(entity.rawName.toUpperCase(), entity);
  }

  // Get workspace node type inventory for node type selection
  const inventory = await getWorkspaceNodeTypeInventory(client, params.workspaceID);
  warnings.push(...inventory.warnings);

  const location = {
    ...(params.locationName ? { locationName: params.locationName } : {}),
    ...(params.database ? { database: params.database } : {}),
    ...(params.schema ? { schema: params.schema } : {}),
  };

  const planNodes: PlannedPipelineNode[] = [];
  const predecessorNodesByPlanID = new Map<string, Record<string, unknown>>();
  let nodeCounter = 0;

  for (const step of parsed.steps) {
    nodeCounter += 1;
    const planNodeID = `intent-node-${nodeCounter}`;

    // Resolve the step's entities to their workspace nodes
    const stepEntities = step.entityNames
      .map((name) => entityLookup.get(name.toUpperCase()))
      .filter((e): e is ResolvedEntity & { confidence: "exact" | "fuzzy" } => e !== undefined && e.confidence !== "unresolved");

    // If this step has no entities, it references the previous step's output
    const referencesPreviousStep = stepEntities.length === 0 && planNodes.length > 0;

    // Select node type for this step
    const selectionContext = {
      explicitNodeType: params.targetNodeType,
      goal: `${step.operation} pipeline step: ${params.intent}`,
      sourceCount: referencesPreviousStep ? 1 : stepEntities.length,
      workspaceNodeTypes: inventory.nodeTypes,
      workspaceNodeTypeCounts: inventory.counts,
      repoPath: params.repoPath,
      hasJoin: step.operation === "join",
      hasGroupBy: step.operation === "aggregate",
    };

    const selectionResult = selectPipelineNodeType(selectionContext);
    const selectedNodeType =
      selectionResult.selectedCandidate?.nodeType ??
      params.targetNodeType ??
      "Stage";
    const selectedFamily = selectionResult.selectedCandidate?.family ?? null;

    // Fetch predecessor node details for selectItems
    const predecessorNodeIDs: string[] = [];
    const predecessorNodeNames: string[] = [];
    const sourceRefs: PlannedPipelineNode["sourceRefs"] = [];
    let selectItems: PlannedPipelineNode["selectItems"] = [];

    if (!referencesPreviousStep) {
      // Wire to workspace nodes
      for (const entity of stepEntities) {
        predecessorNodeIDs.push(entity.resolvedNodeID);
        predecessorNodeNames.push(entity.resolvedNodeName);

        const locationName = entity.resolvedLocationName;
        if (!locationName) {
          warnings.push(
            `Node "${entity.resolvedNodeName}" has no location name. ` +
            `Specify a locationName parameter or ensure the source node has a location assigned.`
          );
        }

        sourceRefs.push({
          locationName: locationName ?? "UNKNOWN_LOCATION",
          nodeName: entity.resolvedNodeName,
          alias: entity.resolvedNodeName,
          nodeID: entity.resolvedNodeID,
        });

        // Fetch the predecessor node for column passthrough
        if (!predecessorNodesByPlanID.has(entity.resolvedNodeID)) {
          try {
            const node = await getWorkspaceNode(client, {
              workspaceID: params.workspaceID,
              nodeID: entity.resolvedNodeID,
            });
            if (isPlainObject(node)) {
              predecessorNodesByPlanID.set(entity.resolvedNodeID, node);
            }
          } catch (error) {
            if (error instanceof CoalesceApiError && [401, 403, 503].includes(error.status)) {
              throw error;
            }
            const reason = error instanceof Error ? error.message : String(error);
            warnings.push(
              `Could not fetch predecessor node "${entity.resolvedNodeName}" (${entity.resolvedNodeID}) — ${reason}. ` +
              `Column passthrough will not be available for this source.`
            );
          }
        }

        const predecessorNode = predecessorNodesByPlanID.get(entity.resolvedNodeID);
        if (predecessorNode) {
          const items = buildSelectItemsForPassthrough(
            entity.resolvedNodeID,
            entity.resolvedNodeName,
            predecessorNode
          );
          selectItems.push(...items);
        }
      }
    }

    // Build node name
    const prefix = buildNodePrefix(selectedFamily, selectionResult.selectedCandidate?.shortName ?? null);
    let nodeName: string;
    if (step.targetName) {
      nodeName = step.targetName;
    } else if (params.targetName && parsed.steps.length === 1) {
      nodeName = params.targetName;
    } else if (stepEntities.length === 1) {
      nodeName = `${prefix}_${stripNodePrefix(stepEntities[0]!.resolvedNodeName)}`.toUpperCase();
    } else if (stepEntities.length > 1) {
      nodeName = `${prefix}_${stepEntities.map((e) => stripNodePrefix(e.resolvedNodeName)).join("_")}`.toUpperCase();
    } else {
      nodeName = `${prefix}_${step.operation.toUpperCase()}_${nodeCounter}`;
    }
    // Clean up double underscores
    nodeName = nodeName.replace(/__+/g, "_");

    // Build join condition for join steps
    let joinCondition: string | null = null;
    if (step.operation === "join" && sourceRefs.length >= 2) {
      const fromRef = sourceRefs[0]!;
      const joinParts = [`FROM {{ ref('${fromRef.locationName}', '${fromRef.nodeName}') }} "${fromRef.alias ?? fromRef.nodeName}"`];
      for (const ref of sourceRefs.slice(1)) {
        const joinType = step.joinType ?? "INNER";
        joinParts.push(
          `${joinType} JOIN {{ ref('${ref.locationName}', '${ref.nodeName}') }} "${ref.alias ?? ref.nodeName}"`
        );
        if (step.joinKey) {
          joinParts.push(
            `  ON "${fromRef.alias ?? fromRef.nodeName}"."${step.joinKey}" = "${ref.alias ?? ref.nodeName}"."${step.joinKey}"`
          );
        }
      }
      joinCondition = joinParts.join("\n");
    } else if (sourceRefs.length === 1) {
      const ref = sourceRefs[0]!;
      joinCondition = `FROM {{ ref('${ref.locationName}', '${ref.nodeName}') }} "${ref.alias ?? ref.nodeName}"`;
    }

    const predecessorPlanNodeIDs = referencesPreviousStep
      ? [planNodes[planNodes.length - 1]!.planNodeID]
      : [];

    planNodes.push({
      planNodeID,
      name: nodeName,
      nodeType: selectedNodeType,
      nodeTypeFamily: selectedFamily,
      predecessorNodeIDs,
      predecessorPlanNodeIDs,
      predecessorNodeNames,
      description: `Auto-generated from intent: ${params.intent}`,
      sql: null,
      selectItems,
      outputColumnNames: selectItems.flatMap((item) =>
        item.outputName ? [item.outputName] : []
      ),
      configOverrides: {},
      sourceRefs,
      joinCondition,
      location,
      requiresFullSetNode: true,
      ...(selectionResult.selectedCandidate?.templateDefaults
        ? { templateDefaults: selectionResult.selectedCandidate.templateDefaults }
        : {}),
    });
  }

  // Check if the plan is ready
  const hasOpenQuestions = openQuestions.length > 0;
  const allNodesHavePredecessors = planNodes.every(
    (node) => node.predecessorNodeIDs.length > 0 || node.predecessorPlanNodeIDs.length > 0
  );

  const plan = {
    version: 1 as const,
    intent: "goal" as const,
    status: (!hasOpenQuestions && allNodesHavePredecessors ? "ready" : "needs_clarification") as "ready" | "needs_clarification",
    workspaceID: params.workspaceID,
    platform: null,
    goal: params.intent,
    sql: null,
    nodes: planNodes,
    assumptions: [
      "Pipeline built from natural language intent. Review node names, types, and column mappings before creating.",
      `Intent parsed ${parsed.steps.length} operation(s): ${parsed.steps.map((s) => s.operation).join(" → ")}.`,
    ],
    openQuestions,
    warnings,
    supportedNodeTypes: [planNodes[0]?.nodeType ?? "Stage"],
    nodeTypeSelection: undefined as Record<string, unknown> | undefined,
  };

  // Get the last selection result for the plan summary
  const finalSelectionContext = {
    explicitNodeType: params.targetNodeType,
    goal: params.intent,
    sourceCount: allEntityNames.length,
    workspaceNodeTypes: inventory.nodeTypes,
    workspaceNodeTypeCounts: inventory.counts,
    repoPath: params.repoPath,
  };
  const finalSelection = selectPipelineNodeType(finalSelectionContext);
  plan.supportedNodeTypes = finalSelection.selection.supportedNodeTypes.length > 0
    ? finalSelection.selection.supportedNodeTypes
    : [planNodes[0]?.nodeType ?? "Stage"];
  plan.nodeTypeSelection = finalSelection.selection as unknown as Record<string, unknown>;

  return {
    status: plan.status === "ready" ? "ready" : "needs_clarification",
    intent: parsed,
    resolvedEntities,
    plan,
    openQuestions,
    warnings,
  };
}
