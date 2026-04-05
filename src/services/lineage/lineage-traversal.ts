import type { LineageCacheEntry } from "./lineage-cache.js";
import { parseColumnKey, columnKey } from "./lineage-cache.js";

// --- Graph traversal helpers ---

export type AncestorNode = {
  nodeID: string;
  nodeName: string;
  nodeType: string;
  depth: number;
};

export function walkUpstream(cache: LineageCacheEntry, startNodeID: string): AncestorNode[] {
  const result: AncestorNode[] = [];
  const visited = new Set<string>();
  const queue: Array<{ nodeID: string; depth: number }> = [{ nodeID: startNodeID, depth: 0 }];

  while (queue.length > 0) {
    const { nodeID, depth } = queue.shift()!;
    if (visited.has(nodeID)) continue;
    visited.add(nodeID);

    if (nodeID !== startNodeID) {
      const node = cache.nodes.get(nodeID);
      if (node) {
        result.push({ nodeID: node.id, nodeName: node.name, nodeType: node.nodeType, depth });
      }
    }

    const upstream = cache.upstreamNodes.get(nodeID);
    if (upstream) {
      for (const upID of upstream) {
        if (!visited.has(upID)) {
          queue.push({ nodeID: upID, depth: depth + 1 });
        }
      }
    }
  }

  return result;
}

export function walkDownstream(cache: LineageCacheEntry, startNodeID: string): AncestorNode[] {
  const result: AncestorNode[] = [];
  const visited = new Set<string>();
  const queue: Array<{ nodeID: string; depth: number }> = [{ nodeID: startNodeID, depth: 0 }];

  while (queue.length > 0) {
    const { nodeID, depth } = queue.shift()!;
    if (visited.has(nodeID)) continue;
    visited.add(nodeID);

    if (nodeID !== startNodeID) {
      const node = cache.nodes.get(nodeID);
      if (node) {
        result.push({ nodeID: node.id, nodeName: node.name, nodeType: node.nodeType, depth });
      }
    }

    const downstream = cache.downstreamNodes.get(nodeID);
    if (downstream) {
      for (const downID of downstream) {
        if (!visited.has(downID)) {
          queue.push({ nodeID: downID, depth: depth + 1 });
        }
      }
    }
  }

  return result;
}

export type ColumnLineageEntry = {
  nodeID: string;
  nodeName: string;
  nodeType: string;
  columnID: string;
  columnName: string;
  direction: "upstream" | "downstream";
  depth: number;
};

export function walkColumnLineage(
  cache: LineageCacheEntry,
  nodeID: string,
  columnID: string
): ColumnLineageEntry[] {
  const result: ColumnLineageEntry[] = [];
  const startKey = columnKey(nodeID, columnID);

  // Walk upstream
  const visitedUp = new Set<string>();
  const queueUp: Array<{ key: string; depth: number }> = [{ key: startKey, depth: 0 }];
  while (queueUp.length > 0) {
    const { key, depth } = queueUp.shift()!;
    if (visitedUp.has(key)) continue;
    visitedUp.add(key);

    if (key !== startKey) {
      const parsed = parseColumnKey(key);
      const node = cache.nodes.get(parsed.nodeID);
      if (node) {
        const col = node.columns.find((c) => c.id === parsed.columnID);
        if (col) {
          result.push({
            nodeID: parsed.nodeID,
            nodeName: node.name,
            nodeType: node.nodeType,
            columnID: parsed.columnID,
            columnName: col.name,
            direction: "upstream",
            depth,
          });
        }
      }
    }

    const upstream = cache.columnUpstream.get(key);
    if (upstream) {
      for (const srcKey of upstream) {
        if (!visitedUp.has(srcKey)) {
          queueUp.push({ key: srcKey, depth: depth + 1 });
        }
      }
    }
  }

  // Walk downstream
  const visitedDown = new Set<string>();
  const queueDown: Array<{ key: string; depth: number }> = [{ key: startKey, depth: 0 }];
  while (queueDown.length > 0) {
    const { key, depth } = queueDown.shift()!;
    if (visitedDown.has(key)) continue;
    visitedDown.add(key);

    if (key !== startKey) {
      const parsed = parseColumnKey(key);
      const node = cache.nodes.get(parsed.nodeID);
      if (node) {
        const col = node.columns.find((c) => c.id === parsed.columnID);
        if (col) {
          result.push({
            nodeID: parsed.nodeID,
            nodeName: node.name,
            nodeType: node.nodeType,
            columnID: parsed.columnID,
            columnName: col.name,
            direction: "downstream",
            depth,
          });
        }
      }
    }

    const downstream = cache.columnDownstream.get(key);
    if (downstream) {
      for (const dstKey of downstream) {
        if (!visitedDown.has(dstKey)) {
          queueDown.push({ key: dstKey, depth: depth + 1 });
        }
      }
    }
  }

  return result;
}

export type ImpactResult = {
  sourceNodeID: string;
  sourceNodeName: string;
  sourceNodeType: string;
  sourceColumnID?: string;
  sourceColumnName?: string;
  impactedNodes: AncestorNode[];
  impactedColumns: ColumnLineageEntry[];
  totalImpactedNodes: number;
  totalImpactedColumns: number;
  byDepth: Record<number, string[]>;
  criticalPath: string[];
};

export function analyzeNodeImpact(
  cache: LineageCacheEntry,
  nodeID: string,
  columnID?: string
): ImpactResult {
  const node = cache.nodes.get(nodeID);
  if (!node) {
    throw new Error(`Node ${nodeID} not found in lineage cache. Ensure the workspace has been cached with detail=true.`);
  }

  let sourceColumnName: string | undefined;
  if (columnID) {
    const col = node.columns.find((c) => c.id === columnID);
    if (!col) {
      const available = node.columns.map((c) => `${c.id} (${c.name})`).join(", ");
      throw new Error(
        `Column ${columnID} not found on node ${nodeID} (${node.name}). Available columns: ${available || "none"}`
      );
    }
    sourceColumnName = col.name;
  }

  // Node-level impact: all downstream nodes
  const impactedNodes = walkDownstream(cache, nodeID);

  // Column-level impact
  let impactedColumns: ColumnLineageEntry[] = [];
  if (columnID) {
    impactedColumns = walkColumnLineage(cache, nodeID, columnID).filter(
      (e) => e.direction === "downstream"
    );
  } else {
    // All columns on this node -> trace each downstream
    for (const col of node.columns) {
      const downstream = walkColumnLineage(cache, nodeID, col.id).filter(
        (e) => e.direction === "downstream"
      );
      impactedColumns.push(...downstream);
    }
    // Deduplicate
    const seen = new Set<string>();
    impactedColumns = impactedColumns.filter((e) => {
      const key = columnKey(e.nodeID, e.columnID);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  // Group by depth
  const byDepth: Record<number, string[]> = {};
  for (const n of impactedNodes) {
    if (!byDepth[n.depth]) byDepth[n.depth] = [];
    byDepth[n.depth].push(n.nodeName);
  }

  // Critical path: longest path from source to leaf
  const criticalPath = findCriticalPath(cache, nodeID);

  return {
    sourceNodeID: nodeID,
    sourceNodeName: node.name,
    sourceNodeType: node.nodeType,
    ...(columnID ? { sourceColumnID: columnID } : {}),
    ...(sourceColumnName ? { sourceColumnName } : {}),
    impactedNodes,
    impactedColumns,
    totalImpactedNodes: impactedNodes.length,
    totalImpactedColumns: impactedColumns.length,
    byDepth,
    criticalPath,
  };
}

function findCriticalPath(cache: LineageCacheEntry, startNodeID: string): string[] {
  const node = cache.nodes.get(startNodeID);
  if (!node) return [];

  // Collect reachable downstream subgraph via BFS
  const reachable = new Set<string>();
  const bfsQueue = [startNodeID];
  while (bfsQueue.length > 0) {
    const id = bfsQueue.shift()!;
    if (reachable.has(id)) continue;
    reachable.add(id);
    const downstream = cache.downstreamNodes.get(id);
    if (downstream) {
      for (const nextID of downstream) {
        if (!reachable.has(nextID)) bfsQueue.push(nextID);
      }
    }
  }

  // Topological sort (Kahn's algorithm) over the reachable subgraph
  const inDegree = new Map<string, number>();
  for (const id of reachable) inDegree.set(id, 0);
  for (const id of reachable) {
    const downstream = cache.downstreamNodes.get(id);
    if (downstream) {
      for (const nextID of downstream) {
        if (reachable.has(nextID)) {
          inDegree.set(nextID, (inDegree.get(nextID) ?? 0) + 1);
        }
      }
    }
  }
  const topoQueue: string[] = [];
  for (const [id, deg] of inDegree) {
    if (deg === 0) topoQueue.push(id);
  }
  const topoOrder: string[] = [];
  while (topoQueue.length > 0) {
    const id = topoQueue.shift()!;
    topoOrder.push(id);
    const downstream = cache.downstreamNodes.get(id);
    if (downstream) {
      for (const nextID of downstream) {
        if (!reachable.has(nextID)) continue;
        const newDeg = (inDegree.get(nextID) ?? 1) - 1;
        inDegree.set(nextID, newDeg);
        if (newDeg === 0) topoQueue.push(nextID);
      }
    }
  }

  // DP longest path from startNodeID — O(V+E)
  const dist = new Map<string, number>();
  const predecessor = new Map<string, string>();
  dist.set(startNodeID, 0);

  for (const id of topoOrder) {
    const d = dist.get(id);
    if (d === undefined) continue; // not reachable from start
    const downstream = cache.downstreamNodes.get(id);
    if (downstream) {
      for (const nextID of downstream) {
        if (!reachable.has(nextID)) continue;
        if (d + 1 > (dist.get(nextID) ?? -1)) {
          dist.set(nextID, d + 1);
          predecessor.set(nextID, id);
        }
      }
    }
  }

  // Find the farthest node
  let farthestID = startNodeID;
  let maxDist = 0;
  for (const [id, d] of dist) {
    if (d > maxDist) {
      maxDist = d;
      farthestID = id;
    }
  }

  // Reconstruct path
  const pathIDs: string[] = [];
  let cur: string | undefined = farthestID;
  while (cur !== undefined) {
    pathIDs.push(cur);
    cur = predecessor.get(cur);
  }
  pathIDs.reverse();

  return pathIDs.map((id) => cache.nodes.get(id)?.name ?? id);
}
