import type { CoalesceClient } from "../../client.js";
import { listWorkspaceNodes, listEnvironmentNodes } from "../../coalesce/api/nodes.js";
import { isPlainObject } from "../../utils.js";

const PAGE_SIZE = 500;

export type DeploymentNodeSummary = {
  nodeID: string;
  name: string;
  nodeType: string;
};

export type ModifiedNodeSummary = {
  nodeID: string;
  workspaceName: string;
  environmentName: string;
  workspaceNodeType: string;
  environmentNodeType: string;
};

export type DeploymentDiff = {
  workspaceID: string;
  environmentID: string;
  diffedAt: string;
  summary: {
    total: number;
    new: number;
    removed: number;
    modified: number;
    unchanged: number;
  };
  new: DeploymentNodeSummary[];
  removed: DeploymentNodeSummary[];
  modified: ModifiedNodeSummary[];
};

// The Coalesce API may return `next` as a number (page index) — normalise to
// string so the cursor loop handles both formats. Mirrors coerceListPaginationFields
// in tool-response.ts.
function normalizeCursor(next: unknown): string | undefined {
  if (typeof next === "string" && next.length > 0) return next;
  if (typeof next === "number") return String(next);
  return undefined;
}

async function fetchAllWorkspaceNodes(
  client: CoalesceClient,
  workspaceID: string
): Promise<DeploymentNodeSummary[]> {
  const nodes: DeploymentNodeSummary[] = [];
  const seenCursors = new Set<string>();
  let cursor: string | undefined;

  do {
    const page = (await listWorkspaceNodes(client, {
      workspaceID,
      limit: PAGE_SIZE,
      orderBy: "id",
      ...(cursor ? { startingFrom: cursor } : {}),
    })) as { data?: unknown[]; next?: unknown };

    for (const item of page.data ?? []) {
      if (isPlainObject(item)) {
        const id = typeof item.id === "string" ? item.id : undefined;
        if (id) {
          nodes.push({
            nodeID: id,
            name: typeof item.name === "string" ? item.name : "",
            nodeType: typeof item.nodeType === "string" ? item.nodeType : "",
          });
        }
      }
    }

    cursor = normalizeCursor(page.next);
    if (cursor) {
      if (seenCursors.has(cursor)) {
        throw new Error(`Pagination repeated cursor ${cursor}`);
      }
      seenCursors.add(cursor);
    }
  } while (cursor);

  return nodes;
}

async function fetchAllEnvironmentNodes(
  client: CoalesceClient,
  environmentID: string
): Promise<DeploymentNodeSummary[]> {
  const nodes: DeploymentNodeSummary[] = [];
  const seenCursors = new Set<string>();
  let cursor: string | undefined;

  do {
    const page = (await listEnvironmentNodes(client, {
      environmentID,
      limit: PAGE_SIZE,
      orderBy: "id",
      ...(cursor ? { startingFrom: cursor } : {}),
    })) as { data?: unknown[]; next?: unknown };

    for (const item of page.data ?? []) {
      if (isPlainObject(item)) {
        const id = typeof item.id === "string" ? item.id : undefined;
        if (id) {
          nodes.push({
            nodeID: id,
            name: typeof item.name === "string" ? item.name : "",
            nodeType: typeof item.nodeType === "string" ? item.nodeType : "",
          });
        }
      }
    }

    cursor = normalizeCursor(page.next);
    if (cursor) {
      if (seenCursors.has(cursor)) {
        throw new Error(`Pagination repeated cursor ${cursor}`);
      }
      seenCursors.add(cursor);
    }
  } while (cursor);

  return nodes;
}

export async function previewDeployment(
  client: CoalesceClient,
  workspaceID: string,
  environmentID: string
): Promise<DeploymentDiff> {
  const [workspaceNodes, environmentNodes] = await Promise.all([
    fetchAllWorkspaceNodes(client, workspaceID),
    fetchAllEnvironmentNodes(client, environmentID),
  ]);

  const envMap = new Map(environmentNodes.map((n) => [n.nodeID, n]));
  const wsMap = new Map(workspaceNodes.map((n) => [n.nodeID, n]));

  const newNodes: DeploymentNodeSummary[] = [];
  const modifiedNodes: ModifiedNodeSummary[] = [];
  let unchangedCount = 0;

  for (const wsNode of workspaceNodes) {
    const envNode = envMap.get(wsNode.nodeID);
    if (!envNode) {
      newNodes.push(wsNode);
    } else if (wsNode.name !== envNode.name || wsNode.nodeType !== envNode.nodeType) {
      modifiedNodes.push({
        nodeID: wsNode.nodeID,
        workspaceName: wsNode.name,
        environmentName: envNode.name,
        workspaceNodeType: wsNode.nodeType,
        environmentNodeType: envNode.nodeType,
      });
    } else {
      unchangedCount++;
    }
  }

  const removedNodes: DeploymentNodeSummary[] = [];
  for (const envNode of environmentNodes) {
    if (!wsMap.has(envNode.nodeID)) {
      removedNodes.push(envNode);
    }
  }

  return {
    workspaceID,
    environmentID,
    diffedAt: new Date().toISOString(),
    summary: {
      total: workspaceNodes.length + removedNodes.length,
      new: newNodes.length,
      removed: removedNodes.length,
      modified: modifiedNodes.length,
      unchanged: unchangedCount,
    },
    new: newNodes,
    removed: removedNodes,
    modified: modifiedNodes,
  };
}
