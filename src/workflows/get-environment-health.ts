import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  fetchAllEnvironmentNodes,
  fetchAllRuns,
} from "../services/cache/snapshots.js";
import {
  READ_ONLY_ANNOTATIONS,
  validatePathSegment,
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool } from "../mcp/tool-helpers.js";
import { isPlainObject } from "../utils.js";
import { MS_PER_DAY } from "../constants.js";
import { isTerminalRunStatus } from "./run-status.js";

type NodeRecord = Record<string, unknown>;

type HealthStatus = "healthy" | "warning" | "critical";

type RunRecord = {
  id: string;
  runStatus: string;
  runStartTime?: string;
  runEndTime?: string;
  runDetails?: Record<string, unknown>;
  [key: string]: unknown;
};

type NodeRunStatus = {
  nodeID: string;
  nodeName: string;
  lastRunStatus: "passed" | "failed" | "never_run";
  lastRunTime?: string;
};

type FailedRunSummary = {
  runID: string;
  runStatus: string;
  startTime: string;
  endTime?: string;
};

type StaleNode = {
  nodeID: string;
  nodeName: string;
  nodeType: string;
  lastRunTime?: string;
  daysSinceLastRun?: number;
};

type DependencyHealth = {
  orphanNodes: Array<{ nodeID: string; nodeName: string; nodeType: string }>;
  totalDependencyEdges: number;
};

export type EnvironmentHealthResult = {
  environmentID: string;
  assessedAt: string;
  totalNodes: number;
  nodesByType: Record<string, number>;
  nodeRunStatus: NodeRunStatus[];
  failedRunsLast24h: FailedRunSummary[];
  staleNodes: StaleNode[];
  dependencyHealth: DependencyHealth;
  healthScore: HealthStatus;
  healthReasons: string[];
};

function extractNodes(items: unknown[]): NodeRecord[] {
  return items.filter((item): item is NodeRecord => isPlainObject(item));
}

function extractRuns(items: unknown[]): RunRecord[] {
  return items.filter((item): item is RunRecord =>
    isPlainObject(item) && typeof (item as Record<string, unknown>).id === "string"
  );
}

function countNodesByType(nodes: NodeRecord[]): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const node of nodes) {
    const nodeType = typeof node.nodeType === "string" ? node.nodeType : "unknown";
    counts[nodeType] = (counts[nodeType] ?? 0) + 1;
  }
  return counts;
}

function getNodeID(node: NodeRecord): string {
  return typeof node.id === "string" ? node.id : String(node.id ?? "");
}

function getNodeName(node: NodeRecord): string {
  return typeof node.name === "string" ? node.name : "unnamed";
}

function getNodeType(node: NodeRecord): string {
  return typeof node.nodeType === "string" ? node.nodeType : "unknown";
}


function isRunScoped(details: Record<string, unknown>): boolean {
  return (
    typeof details.jobID === "string" ||
    typeof details.includeNodesSelector === "string" ||
    typeof details.excludeNodesSelector === "string"
  );
}

function getAttributedRunNodeIDs(
  run: RunRecord,
  allNodeIDs: string[]
): string[] {
  if (!isTerminalRunStatus(run.runStatus)) {
    return [];
  }

  const runTime = run.runEndTime ?? run.runStartTime;
  if (typeof runTime !== "string") {
    return [];
  }

  const details = isPlainObject(run.runDetails) ? run.runDetails : undefined;
  if (!details) {
    return [];
  }

  const nodes = Array.isArray(details.nodes) ? details.nodes : [];
  const explicitNodeIDs = nodes.flatMap((node) => {
    if (!isPlainObject(node) || typeof node.nodeID !== "string") {
      return [];
    }
    return [node.nodeID];
  });
  if (explicitNodeIDs.length > 0) {
    return explicitNodeIDs;
  }

  if (isRunScoped(details)) {
    return [];
  }

  const nodesInRun =
    typeof details.nodesInRun === "number"
      ? details.nodesInRun
      : undefined;
  if (nodesInRun === allNodeIDs.length) {
    return allNodeIDs;
  }

  return [];
}

function buildNodeRunStatuses(
  nodes: NodeRecord[],
  runs: RunRecord[]
): NodeRunStatus[] {
  const nodeLastRun = new Map<string, { status: string; time: string }>();
  const allNodeIDs = nodes.map(getNodeID);
  const knownNodeIDs = new Set(allNodeIDs);

  for (const run of runs) {
    if (!isTerminalRunStatus(run.runStatus)) {
      continue;
    }

    // Skip canceled runs — a cancellation is a deliberate user action, not a
    // pass or failure.  Skipping means the node's last-run status reflects the
    // most recent completed-or-failed outcome, which is what the health score
    // should be based on.
    if (run.runStatus === "canceled") {
      continue;
    }

    const runTime = run.runEndTime ?? run.runStartTime;
    if (!runTime || typeof runTime !== "string") continue;

    for (const nodeID of getAttributedRunNodeIDs(run, allNodeIDs)) {
      if (!knownNodeIDs.has(nodeID)) continue;
      const existing = nodeLastRun.get(nodeID);
      if (!existing || runTime > existing.time) {
        nodeLastRun.set(nodeID, { status: run.runStatus, time: runTime });
      }
    }
  }

  return nodes.map((node) => {
    const nodeID = getNodeID(node);
    const lastRun = nodeLastRun.get(nodeID);
    if (!lastRun) {
      return {
        nodeID,
        nodeName: getNodeName(node),
        lastRunStatus: "never_run" as const,
      };
    }
    return {
      nodeID,
      nodeName: getNodeName(node),
      lastRunStatus: lastRun.status === "completed" ? "passed" as const : "failed" as const,
      lastRunTime: lastRun.time,
    };
  });
}

function getFailedRunsLast24h(runs: RunRecord[]): FailedRunSummary[] {
  const cutoff = new Date(Date.now() - MS_PER_DAY).toISOString();
  return runs
    .filter((run) => {
      if (run.runStatus !== "failed") return false;
      const startTime = run.runStartTime;
      return typeof startTime === "string" && startTime >= cutoff;
    })
    .map((run) => ({
      runID: run.id,
      runStatus: run.runStatus,
      startTime: run.runStartTime as string,
      endTime: typeof run.runEndTime === "string" ? run.runEndTime : undefined,
    }));
}

function getStaleNodes(
  nodes: NodeRecord[],
  nodeRunStatuses: NodeRunStatus[],
  staleDays: number = 7
): StaleNode[] {
  const cutoff = new Date(Date.now() - staleDays * MS_PER_DAY);
  const stale: StaleNode[] = [];

  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    const status = nodeRunStatuses[i];
    if (status.lastRunStatus === "never_run") {
      stale.push({
        nodeID: getNodeID(node),
        nodeName: getNodeName(node),
        nodeType: getNodeType(node),
      });
      continue;
    }
    if (status.lastRunTime) {
      const lastRunDate = new Date(status.lastRunTime);
      if (lastRunDate < cutoff) {
        const daysSince = Math.floor(
          (Date.now() - lastRunDate.getTime()) / MS_PER_DAY
        );
        stale.push({
          nodeID: getNodeID(node),
          nodeName: getNodeName(node),
          nodeType: getNodeType(node),
          lastRunTime: status.lastRunTime,
          daysSinceLastRun: daysSince,
        });
      }
    }
  }
  return stale;
}

function analyzeDependencyHealth(nodes: NodeRecord[]): DependencyHealth {
  const nodeIDs = new Set(nodes.map(getNodeID));
  const referencedNodes = new Set<string>();
  const edgeKeys = new Set<string>();

  const nodesWithUpstream = new Set<string>();
  for (const node of nodes) {
    const nodeID = getNodeID(node);
    const predecessors = node.predecessorNodeIDs;
    if (Array.isArray(predecessors) && predecessors.length > 0) {
      nodesWithUpstream.add(nodeID);
    }

    const metadata = node.metadata as Record<string, unknown> | undefined;
    if (isPlainObject(metadata)) {
      const sourceMapping = metadata.sourceMapping;
      if (Array.isArray(sourceMapping) && sourceMapping.length > 0) {
        nodesWithUpstream.add(nodeID);
      }
      if (Array.isArray(sourceMapping)) {
        for (const mapping of sourceMapping) {
          if (!isPlainObject(mapping)) continue;

          const aliases = isPlainObject(mapping.aliases) ? mapping.aliases : {};
          for (const value of Object.values(aliases)) {
            if (typeof value === "string" && nodeIDs.has(value)) {
              referencedNodes.add(value);
              edgeKeys.add(`${value}->${nodeID}`);
            }
          }

          const deps = Array.isArray(mapping.dependencies)
            ? mapping.dependencies
            : [];
          for (const dep of deps) {
            if (typeof dep === "string" && nodeIDs.has(dep)) {
              referencedNodes.add(dep);
              edgeKeys.add(`${dep}->${nodeID}`);
            } else if (
              isPlainObject(dep) &&
              typeof dep.nodeID === "string" &&
              nodeIDs.has(dep.nodeID)
            ) {
              referencedNodes.add(dep.nodeID);
              edgeKeys.add(`${dep.nodeID}->${nodeID}`);
            }
          }
        }
      }
    }

    if (Array.isArray(predecessors)) {
      for (const pred of predecessors) {
        if (typeof pred === "string" && nodeIDs.has(pred)) {
          referencedNodes.add(pred);
          edgeKeys.add(`${pred}->${nodeID}`);
        }
      }
    }
  }

  const orphanNodes = nodes
    .filter((node) => {
      const id = getNodeID(node);
      return !referencedNodes.has(id) && !nodesWithUpstream.has(id);
    })
    .map((node) => ({
      nodeID: getNodeID(node),
      nodeName: getNodeName(node),
      nodeType: getNodeType(node),
    }));

  return { orphanNodes, totalDependencyEdges: edgeKeys.size };
}

function computeHealthScore(
  totalNodes: number,
  failedRunsLast24h: FailedRunSummary[],
  staleNodes: StaleNode[],
  dependencyHealth: DependencyHealth,
  nodeRunStatuses: NodeRunStatus[]
): { score: HealthStatus; reasons: string[] } {
  const reasons: string[] = [];

  if (totalNodes === 0) {
    return { score: "warning", reasons: ["Environment has no deployed nodes"] };
  }

  const failedCount = failedRunsLast24h.length;
  const staleCount = staleNodes.length;
  const orphanCount = dependencyHealth.orphanNodes.length;
  const neverRunCount = nodeRunStatuses.filter(
    (s) => s.lastRunStatus === "never_run"
  ).length;
  const failedNodeCount = nodeRunStatuses.filter(
    (s) => s.lastRunStatus === "failed"
  ).length;

  if (failedCount > 0) {
    reasons.push(`${failedCount} failed ${failedCount === 1 ? "run" : "runs"} in the last 24 hours`);
  }
  if (failedNodeCount > 0) {
    reasons.push(
      `${failedNodeCount}/${totalNodes} nodes have a failed last run`
    );
  }
  if (staleCount > 0) {
    reasons.push(`${staleCount} stale node(s) not run in 7+ days`);
  }
  if (orphanCount > 0) {
    reasons.push(
      `${orphanCount} orphan node(s) with no upstream or downstream connections`
    );
  }
  if (neverRunCount > 0) {
    reasons.push(`${neverRunCount} node(s) have never been run`);
  }

  if (reasons.length === 0) {
    reasons.push("All nodes healthy, no recent failures");
  }

  const hasCritical =
    failedCount >= 3 || failedNodeCount > totalNodes * 0.5;
  const hasWarning =
    failedCount > 0 ||
    staleCount > 0 ||
    orphanCount > 0 ||
    neverRunCount > 0;

  const score: HealthStatus = hasCritical
    ? "critical"
    : hasWarning
      ? "warning"
      : "healthy";

  return { score, reasons };
}

export async function getEnvironmentHealth(
  client: CoalesceClient,
  params: { environmentID: string }
): Promise<EnvironmentHealthResult> {
  const environmentID = validatePathSegment(
    params.environmentID,
    "environmentID"
  );

  const [nodesResult, runsResult] = await Promise.all([
    fetchAllEnvironmentNodes(client, { environmentID, detail: true }),
    fetchAllRuns(client, {
      environmentID,
      detail: true,
      orderBy: "id",
      orderByDirection: "desc",
    }),
  ]);

  const nodes = extractNodes(nodesResult.items);
  const runs = extractRuns(runsResult.items);
  const assessedAt = new Date().toISOString();

  const nodesByType = countNodesByType(nodes);
  const nodeRunStatuses = buildNodeRunStatuses(nodes, runs);
  const failedRunsLast24h = getFailedRunsLast24h(runs);
  const staleNodes = getStaleNodes(nodes, nodeRunStatuses);
  const dependencyHealth = analyzeDependencyHealth(nodes);

  const { score, reasons } = computeHealthScore(
    nodes.length,
    failedRunsLast24h,
    staleNodes,
    dependencyHealth,
    nodeRunStatuses
  );

  return {
    environmentID,
    assessedAt,
    totalNodes: nodes.length,
    nodesByType,
    nodeRunStatus: nodeRunStatuses,
    failedRunsLast24h,
    staleNodes,
    dependencyHealth,
    healthScore: score,
    healthReasons: reasons,
  };
}

export function defineGetEnvironmentHealth(
  _server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
    defineSimpleTool(client, "get_environment_health", {
      title: "Get Environment Health",
      description:
        "Get a comprehensive health dashboard for a deployed environment. Composes multiple API calls into a single health summary.\n\nThis tool paginates through all environment nodes and all environment runs before scoring health, so it may take longer on large or busy environments.\n\nArgs:\n  - environmentID (string, required): The environment ID\n\nReturns:\n  {\n    environmentID, assessedAt, totalNodes,\n    nodesByType: { Stage: 5, Dimension: 3, ... },\n    nodeRunStatus: [{ nodeID, nodeName, lastRunStatus, lastRunTime }],\n    failedRunsLast24h: [{ runID, runStatus, startTime, endTime }],\n    staleNodes: [{ nodeID, nodeName, nodeType, lastRunTime, daysSinceLastRun }],\n    dependencyHealth: { orphanNodes: [...], totalDependencyEdges },\n    healthScore: \"healthy\" | \"warning\" | \"critical\",\n    healthReasons: [\"...\"]\n  }",
      inputSchema: z.object({
        environmentID: z
          .string()
          .describe("The environment ID to assess health for"),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
      sanitize: true,
    }, getEnvironmentHealth),
  ];
}
