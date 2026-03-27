import type { CoalesceClient } from "../../client.js";
import { isPlainObject } from "../../utils.js";
import { listWorkspaceNodes, getWorkspaceNode } from "../../coalesce/api/nodes.js";
import { CoalesceApiError } from "../../client.js";
import { extractNodeArray, isPassthroughTransform } from "../shared/node-helpers.js";
import {
  getNodeColumnArray,
  getColumnNamesFromNode,
  normalizeSqlIdentifier,
} from "./planning.js";
import {
  inferNodeLayer,
  detectMethodology,
  type NodeSummary,
  type NodeLayer,
  type Methodology,
} from "../workspace/analysis.js";

// ── Types ────────────────────────────────────────────────────────────────────

export type FindingSeverity = "critical" | "warning" | "suggestion";

export type FindingCategory =
  | "redundant_passthrough"
  | "missing_transform"
  | "layer_violation"
  | "naming_inconsistency"
  | "fan_out_risk"
  | "orphan_node"
  | "missing_join_condition"
  | "type_mismatch"
  | "unused_columns"
  | "deep_chain"
  | "missing_config";

export interface ReviewFinding {
  severity: FindingSeverity;
  category: FindingCategory;
  nodeID: string;
  nodeName: string;
  message: string;
  suggestion: string;
}

export interface PipelineReview {
  workspaceID: string;
  analyzedAt: string;
  scope: "full" | "subgraph";
  nodeCount: number;
  methodology: Methodology;
  findings: ReviewFinding[];
  summary: {
    critical: number;
    warning: number;
    suggestion: number;
  };
  graphStats: {
    maxDepth: number;
    rootNodes: number;
    leafNodes: number;
    avgFanOut: number;
  };
  warnings: string[];
}

export interface ReviewPipelineInput {
  workspaceID: string;
  /** Optional: only review nodes in this list (e.g., from a subgraph) */
  nodeIDs?: string[];
}

// ── Node detail with graph context ───────────────────────────────────────────

interface NodeDetail {
  id: string;
  name: string;
  nodeType: string;
  locationName: string | null;
  layer: NodeLayer;
  predecessorIDs: string[];
  successorIDs: string[];
  columns: Array<Record<string, unknown>>;
  columnCount: number;
  passthroughCount: number;
  transformCount: number;
  hasJoinCondition: boolean;
  joinCondition: string | null;
  hasConfig: boolean;
  sourceMapping: Record<string, unknown> | null;
}

// ── Main review function ─────────────────────────────────────────────────────

export async function reviewPipeline(
  client: CoalesceClient,
  params: ReviewPipelineInput
): Promise<PipelineReview> {
  const { workspaceID } = params;
  const warnings: string[] = [];

  // Fetch all workspace nodes (summary)
  const allNodesRaw = await listWorkspaceNodes(client, { workspaceID });
  const allNodesList = extractNodeArray(allNodesRaw);

  // Determine scope
  const scopeNodeIDs = params.nodeIDs
    ? new Set(params.nodeIDs)
    : null;

  // Build graph index from summary data
  const nodeIndex = new Map<string, { id: string; name: string; nodeType: string; locationName: string | null; predecessorIDs: string[] }>();
  for (const raw of allNodesList) {
    const id = typeof raw.id === "string" ? raw.id : null;
    const name = typeof raw.name === "string" ? raw.name : "UNKNOWN";
    const nodeType = typeof raw.nodeType === "string" ? raw.nodeType : "Unknown";
    const locationName = typeof raw.locationName === "string" ? raw.locationName : null;
    if (!id) continue;
    if (scopeNodeIDs && !scopeNodeIDs.has(id)) continue;

    // Extract predecessors from columns' source references or from node data
    const predecessorIDs = extractPredecessorIDs(raw);
    nodeIndex.set(id, { id, name, nodeType, locationName, predecessorIDs });
  }

  // Build successor map
  const successorMap = new Map<string, string[]>();
  for (const [id, node] of nodeIndex) {
    for (const predID of node.predecessorIDs) {
      const existing = successorMap.get(predID) ?? [];
      existing.push(id);
      successorMap.set(predID, existing);
    }
  }

  // Fetch full detail for scoped nodes (limit to 50 to avoid API overload)
  const nodeIDs = Array.from(nodeIndex.keys());
  const fetchIDs = nodeIDs.slice(0, 50);
  if (nodeIDs.length > 50) {
    warnings.push(
      `Workspace has ${nodeIDs.length} nodes in scope — only the first 50 are analyzed in detail. ` +
        `Use nodeIDs to scope the review to a specific pipeline section.`
    );
  }

  const detailMap = new Map<string, NodeDetail>();
  let fetchFailureCount = 0;

  // Fetch in batches of 10 to avoid overwhelming the API with concurrent requests
  const BATCH_SIZE = 10;
  for (let i = 0; i < fetchIDs.length; i += BATCH_SIZE) {
    const batch = fetchIDs.slice(i, i + BATCH_SIZE);
    const batchPromises = batch.map(async (nodeID) => {
      try {
        const fullNode = await getWorkspaceNode(client, { workspaceID, nodeID });
        if (!isPlainObject(fullNode)) return;

        const summary = nodeIndex.get(nodeID)!;
        const columns = getNodeColumnArray(fullNode);
        const { passthroughCount, transformCount } = analyzeColumnTransforms(columns);

        const sm = isPlainObject(fullNode.metadata) && isPlainObject((fullNode.metadata as Record<string, unknown>).sourceMapping)
          ? (fullNode.metadata as Record<string, unknown>).sourceMapping as Record<string, unknown>
          : null;

        const joinObj = sm && isPlainObject(sm.join) ? sm.join : null;
        const joinCondition = joinObj && typeof (joinObj as Record<string, unknown>).joinCondition === "string"
          ? (joinObj as Record<string, unknown>).joinCondition as string
          : null;

        detailMap.set(nodeID, {
          id: nodeID,
          name: summary.name,
          nodeType: summary.nodeType,
          locationName: summary.locationName,
          layer: inferNodeLayer({ nodeType: summary.nodeType, name: summary.name }),
          predecessorIDs: summary.predecessorIDs,
          successorIDs: successorMap.get(nodeID) ?? [],
          columns,
          columnCount: columns.length,
          passthroughCount,
          transformCount,
          hasJoinCondition: joinCondition !== null && joinCondition.trim().length > 0,
          joinCondition,
          hasConfig: isPlainObject(fullNode.config) && Object.keys(fullNode.config as Record<string, unknown>).length > 0,
          sourceMapping: sm,
        });
      } catch (error) {
        if (error instanceof CoalesceApiError && [401, 403, 503].includes(error.status)) {
          throw error;
        }
        fetchFailureCount += 1;
        warnings.push(
          `Could not fetch node ${nodeID}: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    });

    const batchResults = await Promise.allSettled(batchPromises);
    for (const result of batchResults) {
      if (result.status === "rejected") {
        if (result.reason instanceof CoalesceApiError && [401, 403, 503].includes(result.reason.status)) {
          throw result.reason;
        }
      }
    }
  }

  // Flag degraded review if too many nodes could not be fetched
  const reviewDegraded = fetchIDs.length > 0 && fetchFailureCount / fetchIDs.length > 0.2;
  if (reviewDegraded) {
    warnings.unshift(
      `Review is degraded: ${fetchFailureCount} of ${fetchIDs.length} nodes could not be fetched. ` +
        `Findings may be incomplete. Check API connectivity and retry.`
    );
  }

  // Detect methodology for context
  const nodeSummaries: NodeSummary[] = Array.from(nodeIndex.values()).map((n) => ({
    nodeType: n.nodeType,
    name: n.name,
  }));
  const methodology = detectMethodology(nodeSummaries);

  // Run analysis checks
  const findings: ReviewFinding[] = [];

  for (const [, detail] of detailMap) {
    checkRedundantPassthrough(detail, detailMap, findings);
    checkMissingJoinCondition(detail, findings);
    checkLayerViolation(detail, detailMap, findings);
    checkFanOutRisk(detail, findings);
    checkOrphanNode(detail, nodeIndex, successorMap, findings);
    checkDeepChain(detail, detailMap, findings);
    checkNamingConsistency(detail, methodology, findings);
    checkNodeTypeMismatch(detail, findings);
    checkUnusedColumns(detail, detailMap, findings);
  }

  // Compute graph stats
  const graphStats = computeGraphStats(detailMap, nodeIndex, successorMap);

  // Summarize
  const summary = { critical: 0, warning: 0, suggestion: 0 };
  for (const f of findings) {
    summary[f.severity]++;
  }

  // Sort: critical first, then warning, then suggestion
  const severityOrder: Record<FindingSeverity, number> = { critical: 0, warning: 1, suggestion: 2 };
  findings.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);

  return {
    workspaceID,
    analyzedAt: new Date().toISOString(),
    scope: scopeNodeIDs ? "subgraph" : "full",
    nodeCount: nodeIndex.size,
    methodology,
    findings,
    summary,
    graphStats,
    warnings,
  };
}

// ── Analysis checks ──────────────────────────────────────────────────────────

function checkRedundantPassthrough(
  node: NodeDetail,
  detailMap: Map<string, NodeDetail>,
  findings: ReviewFinding[]
): void {
  // A node where ALL columns are passthrough and it has exactly 1 predecessor
  // is potentially redundant — it just passes data through without transformation
  if (
    node.predecessorIDs.length === 1 &&
    node.columnCount > 0 &&
    node.passthroughCount === node.columnCount &&
    !node.hasJoinCondition
  ) {
    // Check that the successor doesn't already reference the predecessor directly
    findings.push({
      severity: "warning",
      category: "redundant_passthrough",
      nodeID: node.id,
      nodeName: node.name,
      message: `All ${node.columnCount} columns are passthrough with a single predecessor. This node adds no transformations.`,
      suggestion:
        "Consider removing this node and connecting its successor directly to its predecessor, " +
        "or add column transforms, filters, or business logic to justify the layer.",
    });
  }
}

function checkMissingJoinCondition(
  node: NodeDetail,
  findings: ReviewFinding[]
): void {
  // Multiple predecessors but no join condition = likely misconfigured
  if (node.predecessorIDs.length >= 2 && !node.hasJoinCondition) {
    findings.push({
      severity: "critical",
      category: "missing_join_condition",
      nodeID: node.id,
      nodeName: node.name,
      message: `Node has ${node.predecessorIDs.length} predecessors but no join condition defined.`,
      suggestion:
        "Use apply_join_condition to set the FROM/JOIN/ON clause, or use convert_join_to_aggregation if this should be an aggregation node.",
    });
  }
}

function checkLayerViolation(
  node: NodeDetail,
  detailMap: Map<string, NodeDetail>,
  findings: ReviewFinding[]
): void {
  const layerOrder: Record<NodeLayer, number> = {
    bronze: 0,
    staging: 1,
    intermediate: 2,
    mart: 3,
    unknown: -1,
  };

  const nodeOrder = layerOrder[node.layer];
  if (nodeOrder <= 0) return; // Skip bronze and unknown

  for (const predID of node.predecessorIDs) {
    const pred = detailMap.get(predID);
    if (!pred) continue;

    const predOrder = layerOrder[pred.layer];
    if (predOrder < 0) continue; // Skip unknown predecessors

    // Skip from bronze directly to mart (skipping staging/intermediate)
    if (nodeOrder - predOrder > 1) {
      findings.push({
        severity: "suggestion",
        category: "layer_violation",
        nodeID: node.id,
        nodeName: node.name,
        message: `Node in "${node.layer}" layer depends directly on "${pred.name}" in "${pred.layer}" layer, skipping intermediate layers.`,
        suggestion:
          "Consider adding a staging or intermediate node between these layers for better data lineage and reusability.",
      });
    }
  }
}

function checkFanOutRisk(
  node: NodeDetail,
  findings: ReviewFinding[]
): void {
  // High fan-out: a node feeds many downstream nodes
  if (node.successorIDs.length > 10) {
    findings.push({
      severity: "suggestion",
      category: "fan_out_risk",
      nodeID: node.id,
      nodeName: node.name,
      message: `Node has ${node.successorIDs.length} downstream dependents. Changes to this node will cascade widely.`,
      suggestion:
        "Consider if this node's contract is stable. If it changes frequently, add an intermediate abstraction layer " +
        "to buffer downstream consumers from upstream changes.",
    });
  }
}

function checkOrphanNode(
  node: NodeDetail,
  nodeIndex: Map<string, { id: string; name: string; nodeType: string; locationName: string | null; predecessorIDs: string[] }>,
  successorMap: Map<string, string[]>,
  findings: ReviewFinding[]
): void {
  // Leaf node with no successors and no predecessors = orphan
  const hasSuccessors = (successorMap.get(node.id) ?? []).length > 0;
  if (node.predecessorIDs.length === 0 && !hasSuccessors) {
    findings.push({
      severity: "warning",
      category: "orphan_node",
      nodeID: node.id,
      nodeName: node.name,
      message: "Node has no predecessors and no downstream dependents — it is disconnected from the pipeline.",
      suggestion:
        "If this node is no longer needed, consider deleting it. If it's a source node, connect it as a predecessor to downstream nodes.",
    });
  }
}

function checkDeepChain(
  node: NodeDetail,
  detailMap: Map<string, NodeDetail>,
  findings: ReviewFinding[]
): void {
  // Walk predecessors to measure chain depth (max 20 to avoid cycles)
  let depth = 0;
  let current: NodeDetail | undefined = node;
  const visited = new Set<string>();

  while (current && depth < 20) {
    if (visited.has(current.id)) break;
    visited.add(current.id);

    if (current.predecessorIDs.length === 0) break;
    // Follow first predecessor for depth measurement
    current = detailMap.get(current.predecessorIDs[0]!);
    depth++;
  }

  if (depth >= 8) {
    findings.push({
      severity: "suggestion",
      category: "deep_chain",
      nodeID: node.id,
      nodeName: node.name,
      message: `Node is at the end of a chain ${depth} nodes deep. Deep chains increase complexity and deployment time.`,
      suggestion:
        "Review if intermediate nodes are all necessary. Consider collapsing passthrough nodes " +
        "or materializing key intermediate results for faster iteration.",
    });
  }
}

function checkNamingConsistency(
  node: NodeDetail,
  methodology: Methodology,
  findings: ReviewFinding[]
): void {
  const name = node.name;
  const layer = node.layer;

  // Check for mixed case (some uppercase, some lowercase segments)
  const hasLower = /[a-z]/.test(name);
  const hasUpper = /[A-Z]/.test(name);
  if (hasLower && hasUpper && !name.includes("_")) {
    findings.push({
      severity: "suggestion",
      category: "naming_inconsistency",
      nodeID: node.id,
      nodeName: node.name,
      message: "Node name uses mixed case without underscores — inconsistent with typical Coalesce conventions.",
      suggestion: "Consider using UPPER_SNAKE_CASE (e.g., STG_CUSTOMERS) for consistency with other nodes.",
    });
  }

  // Check mart-layer nodes without proper prefix
  if (methodology === "kimball" && layer === "mart") {
    if (!/^(DIM_|DIMENSION_|FACT_|FCT_)/i.test(name)) {
      findings.push({
        severity: "suggestion",
        category: "naming_inconsistency",
        nodeID: node.id,
        nodeName: node.name,
        message: `Node is in the mart layer with Kimball methodology but doesn't follow DIM_/FACT_ naming convention.`,
        suggestion: "Consider prefixing with DIM_ for dimension tables or FACT_/FCT_ for fact tables.",
      });
    }
  }
}

function checkNodeTypeMismatch(
  node: NodeDetail,
  findings: ReviewFinding[]
): void {
  const baseType = node.nodeType.includes(":::")
    ? node.nodeType.split(":::")[1]!
    : node.nodeType;

  // View type with multiple predecessors = likely should be Stage/Work for join
  if (
    baseType.toLowerCase() === "view" &&
    node.predecessorIDs.length >= 2
  ) {
    findings.push({
      severity: "warning",
      category: "type_mismatch",
      nodeID: node.id,
      nodeName: node.name,
      message: "View node type with multiple predecessors — views joining multiple sources may have performance issues.",
      suggestion:
        "Consider using a Stage or Work node type for joins. Views are best for lightweight projections on a single source.",
    });
  }

  // Dimension/Fact type at staging layer
  if (
    (baseType === "Dimension" || baseType === "Fact") &&
    (node.layer === "bronze" || node.layer === "staging")
  ) {
    findings.push({
      severity: "warning",
      category: "type_mismatch",
      nodeID: node.id,
      nodeName: node.name,
      message: `${baseType} node type used in the ${node.layer} layer — dimensional types are designed for mart-layer business entities.`,
      suggestion:
        "Use Stage or Work type for staging/bronze layers. Reserve Dimension/Fact for the mart layer.",
    });
  }
}

function checkUnusedColumns(
  node: NodeDetail,
  detailMap: Map<string, NodeDetail>,
  findings: ReviewFinding[]
): void {
  // Only check nodes that have successors
  if (node.successorIDs.length === 0) return;
  if (node.columnCount === 0) return;

  // Get all columns referenced by successors
  const referencedColumns = new Set<string>();
  for (const succID of node.successorIDs) {
    const succ = detailMap.get(succID);
    if (!succ) continue;

    for (const col of succ.columns) {
      if (!Array.isArray(col.sources)) continue;
      for (const source of col.sources) {
        if (!isPlainObject(source) || !Array.isArray(source.columnReferences)) continue;
        for (const ref of source.columnReferences) {
          if (isPlainObject(ref) && typeof ref.columnName === "string") {
            referencedColumns.add(normalizeSqlIdentifier(ref.columnName));
          }
        }
      }
    }
  }

  // If we couldn't determine references (successors not in detail map), skip
  if (referencedColumns.size === 0) return;

  const nodeColumnNames = getColumnNamesFromNode({ metadata: { columns: node.columns } });
  const unreferenced = nodeColumnNames.filter(
    (name) => !referencedColumns.has(normalizeSqlIdentifier(name))
  );

  const ratio = unreferenced.length / nodeColumnNames.length;
  if (ratio > 0.5 && unreferenced.length > 5) {
    findings.push({
      severity: "suggestion",
      category: "unused_columns",
      nodeID: node.id,
      nodeName: node.name,
      message: `${unreferenced.length} of ${nodeColumnNames.length} columns are not referenced by any downstream node.`,
      suggestion:
        "Consider removing unused columns to reduce storage and improve query performance. " +
        "Unused columns: " + unreferenced.slice(0, 10).join(", ") +
        (unreferenced.length > 10 ? ` (and ${unreferenced.length - 10} more)` : ""),
    });
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────


function extractPredecessorIDs(node: Record<string, unknown>): string[] {
  // Try direct predecessorNodeIDs field
  if (Array.isArray(node.predecessorNodeIDs)) {
    return node.predecessorNodeIDs.filter((id): id is string => typeof id === "string");
  }
  // Try sources array pattern
  if (isPlainObject(node.metadata) && isPlainObject((node.metadata as Record<string, unknown>).sourceMapping)) {
    const sm = (node.metadata as Record<string, unknown>).sourceMapping as Record<string, unknown>;
    if (Array.isArray(sm.sources)) {
      return sm.sources
        .filter(isPlainObject)
        .map((s) => (s as Record<string, unknown>).nodeID)
        .filter((id): id is string => typeof id === "string");
    }
  }
  return [];
}

function analyzeColumnTransforms(columns: Array<Record<string, unknown>>): {
  passthroughCount: number;
  transformCount: number;
} {
  let passthroughCount = 0;
  let transformCount = 0;

  for (const col of columns) {
    const name = typeof col.name === "string" ? col.name : "";
    const transform = typeof col.transform === "string" ? col.transform : "";

    if (isPassthroughTransform(transform, name)) {
      passthroughCount++;
    } else {
      transformCount++;
    }
  }

  return { passthroughCount, transformCount };
}


function computeGraphStats(
  detailMap: Map<string, NodeDetail>,
  nodeIndex: Map<string, { id: string; predecessorIDs: string[] }>,
  successorMap: Map<string, string[]>
): { maxDepth: number; rootNodes: number; leafNodes: number; avgFanOut: number } {
  let rootNodes = 0;
  let leafNodes = 0;
  let totalSuccessors = 0;
  let maxDepth = 0;

  for (const [id, node] of nodeIndex) {
    const predCount = node.predecessorIDs.length;
    const succCount = (successorMap.get(id) ?? []).length;

    if (predCount === 0) rootNodes++;
    if (succCount === 0) leafNodes++;
    totalSuccessors += succCount;
  }

  // Compute max depth via BFS from roots
  const roots = Array.from(nodeIndex.entries())
    .filter(([, n]) => n.predecessorIDs.length === 0)
    .map(([id]) => id);

  const depths = new Map<string, number>();
  const queue = roots.map((id) => ({ id, depth: 0 }));
  while (queue.length > 0) {
    const { id, depth } = queue.shift()!;
    if (depths.has(id) && depths.get(id)! >= depth) continue;
    depths.set(id, depth);
    if (depth > maxDepth) maxDepth = depth;

    for (const succID of successorMap.get(id) ?? []) {
      if (!depths.has(succID) || depths.get(succID)! < depth + 1) {
        queue.push({ id: succID, depth: depth + 1 });
      }
    }
  }

  const nodeCount = nodeIndex.size;
  return {
    maxDepth,
    rootNodes,
    leafNodes,
    avgFanOut: nodeCount > 0 ? Math.round((totalSuccessors / nodeCount) * 10) / 10 : 0,
  };
}
