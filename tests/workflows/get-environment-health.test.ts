import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  getEnvironmentHealth,
  defineGetEnvironmentHealth,
} from "../../src/workflows/get-environment-health.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ ok: true }),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

const NOW = Date.now();
const HOURS_AGO = (h: number) => new Date(NOW - h * 60 * 60 * 1000).toISOString();
const DAYS_AGO = (d: number) => new Date(NOW - d * 24 * 60 * 60 * 1000).toISOString();

function makeNode(id: string, name: string, nodeType: string, extra: Record<string, unknown> = {}) {
  return { id, name, nodeType, ...extra };
}

function makeRun(id: string, status: string, startTime: string, endTime?: string) {
  return {
    id,
    runStatus: status,
    runStartTime: startTime,
    ...(endTime ? { runEndTime: endTime } : {}),
  };
}

describe("get-environment-health workflow", () => {
  it("registers without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    defineGetEnvironmentHealth(server, client as any).forEach(t => server.registerTool(...t));
    expect(true).toBe(true);
  });

  it("returns healthy score for environment with no failures", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
      makeNode("n2", "DIM_CUSTOMER", "Dimension", { predecessorNodeIDs: ["n1"] }),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r1", "completed", HOURS_AGO(2), HOURS_AGO(1)),
              runDetails: {
                nodes: [{ nodeID: "n1" }, { nodeID: "n2" }],
                nodesInRun: 2,
              },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.environmentID).toBe("env-1");
    expect(result.totalNodes).toBe(2);
    expect(result.nodesByType).toEqual({ Stage: 1, Dimension: 1 });
    expect(result.healthScore).toBe("healthy");
    expect(result.failedRunsLast24h).toHaveLength(0);
    expect(result.staleNodes).toHaveLength(0);
  });

  it("returns warning score when there are stale nodes", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
      makeNode("n2", "OLD_TABLE", "Stage"),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r1", "completed", DAYS_AGO(10), DAYS_AGO(10)),
              runDetails: {
                nodes: [{ nodeID: "n1" }, { nodeID: "n2" }],
                nodesInRun: 2,
              },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.healthScore).toBe("warning");
    expect(result.staleNodes.length).toBeGreaterThan(0);
    expect(result.healthReasons.some((r: string) => r.includes("stale"))).toBe(true);
  });

  it("returns critical score when many runs failed recently", async () => {
    const client = createMockClient();
    const nodes = [makeNode("n1", "STG_ORDERS", "Stage")];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r1", "failed", HOURS_AGO(3), HOURS_AGO(2)),
              runDetails: {
                nodes: [{ nodeID: "n1" }],
                nodesInRun: 1,
              },
            },
            {
              ...makeRun("r2", "failed", HOURS_AGO(5), HOURS_AGO(4)),
              runDetails: {
                nodes: [{ nodeID: "n1" }],
                nodesInRun: 1,
              },
            },
            {
              ...makeRun("r3", "failed", HOURS_AGO(7), HOURS_AGO(6)),
              runDetails: {
                nodes: [{ nodeID: "n1" }],
                nodesInRun: 1,
              },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.healthScore).toBe("critical");
    expect(result.failedRunsLast24h).toHaveLength(3);
    expect(result.healthReasons.some((r: string) => r.includes("failed runs"))).toBe(true);
  });

  it("does not attribute scoped summary runs to unrelated nodes", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
      makeNode("n2", "DIM_CUSTOMER", "Dimension"),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r1", "failed", HOURS_AGO(1), HOURS_AGO(0)),
              runDetails: {
                environmentID: "env-1",
                jobID: "job-1",
                nodesInRun: 1,
              },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.nodeRunStatus.every((status) => status.lastRunStatus === "never_run")).toBe(true);
    expect(
      result.healthReasons.some((reason: string) => reason.includes("failed last run"))
    ).toBe(false);
  });

  it("handles empty environment", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: [] });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.totalNodes).toBe(0);
    expect(result.healthScore).toBe("warning");
    expect(result.healthReasons).toContain("Environment has no deployed nodes");
  });

  it("identifies orphan nodes with no connections", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
      makeNode("n2", "DIM_CUSTOMER", "Dimension", {
        predecessorNodeIDs: ["n1"],
      }),
      makeNode("n3", "ORPHAN_TABLE", "Stage"),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [makeRun("r1", "completed", HOURS_AGO(1), HOURS_AGO(0))],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.dependencyHealth.orphanNodes).toEqual([
      { nodeID: "n3", nodeName: "ORPHAN_TABLE", nodeType: "Stage" },
    ]);
    expect(result.dependencyHealth.totalDependencyEdges).toBe(1);
  });

  it("resolves alias-based dependencies when detecting orphan nodes", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "SRC_ORDERS", "Source"),
      makeNode("n2", "STG_ORDERS", "Stage", {
        metadata: {
          sourceMapping: [
            {
              aliases: { SRC_ORDERS: "n1" },
              dependencies: [{ locationName: "SRC", nodeName: "SRC_ORDERS" }],
            },
          ],
        },
      }),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.dependencyHealth.totalDependencyEdges).toBe(1);
    expect(result.dependencyHealth.orphanNodes).toHaveLength(0);
  });

  it("identifies never-run nodes", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
      makeNode("n2", "NEW_NODE", "Dimension"),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.nodeRunStatus.every((s) => s.lastRunStatus === "never_run")).toBe(true);
    expect(result.healthReasons.some((r: string) => r.includes("never been run"))).toBe(true);
  });

  it("counts nodes by type correctly", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
      makeNode("n2", "STG_CUSTOMER", "Stage"),
      makeNode("n3", "DIM_CUSTOMER", "Dimension"),
      makeNode("n4", "FCT_SALES", "Fact"),
      makeNode("n5", "FCT_REVENUE", "Fact"),
      makeNode("n6", "FCT_ORDERS", "Fact"),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [makeRun("r1", "completed", HOURS_AGO(1))],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.totalNodes).toBe(6);
    expect(result.nodesByType).toEqual({
      Stage: 2,
      Dimension: 1,
      Fact: 3,
    });
  });

  it("only includes failed runs from the last 24 hours", async () => {
    const client = createMockClient();
    const nodes = [makeNode("n1", "STG_ORDERS", "Stage")];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [
            makeRun("r1", "failed", HOURS_AGO(12), HOURS_AGO(11)),
            makeRun("r2", "failed", DAYS_AGO(3), DAYS_AGO(3)),
            makeRun("r3", "completed", HOURS_AGO(1)),
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.failedRunsLast24h).toHaveLength(1);
    expect(result.failedRunsLast24h[0].runID).toBe("r1");
  });

  it("auto-paginates nodes across multiple pages", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        if (!params || !params.startingFrom) {
          return Promise.resolve({
            data: [makeNode("n1", "NODE_1", "Stage")],
            next: "cursor-2",
          });
        }
        if (params.startingFrom === "cursor-2") {
          return Promise.resolve({
            data: [makeNode("n2", "NODE_2", "Dimension")],
          });
        }
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [makeRun("r1", "completed", HOURS_AGO(1))],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.totalNodes).toBe(2);
    expect(result.nodesByType).toEqual({ Stage: 1, Dimension: 1 });
  });

  it("auto-paginates runs across multiple pages before deriving node status", async () => {
    const client = createMockClient();
    const nodes = [makeNode("n1", "NODE_1", "Stage")];

    client.get.mockImplementation((path: string, params?: Record<string, unknown>) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        if (!params || !params.startingFrom) {
          return Promise.resolve({
            data: [],
            next: "cursor-2",
          });
        }
        if (params.startingFrom === "cursor-2") {
          return Promise.resolve({
            data: [
              {
                ...makeRun("r1", "completed", HOURS_AGO(3), HOURS_AGO(2)),
                runDetails: {
                  nodes: [{ nodeID: "n1" }],
                  nodesInRun: 1,
                },
              },
            ],
          });
        }
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.nodeRunStatus).toHaveLength(1);
    expect(result.nodeRunStatus[0].lastRunStatus).toBe("passed");
    expect(result.nodeRunStatus[0].lastRunTime).toBe(HOURS_AGO(2));
  });

  it("returns assessedAt timestamp", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: [] });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({});
    });

    const before = new Date().toISOString();
    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });
    const after = new Date().toISOString();

    expect(result.assessedAt >= before).toBe(true);
    expect(result.assessedAt <= after).toBe(true);
  });

  it("detects dependency edges from predecessorNodeIDs", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "SOURCE", "Stage"),
      makeNode("n2", "TRANSFORM", "Stage", { predecessorNodeIDs: ["n1"] }),
      makeNode("n3", "OUTPUT", "Dimension", { predecessorNodeIDs: ["n1", "n2"] }),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [makeRun("r1", "completed", HOURS_AGO(1))],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.dependencyHealth.totalDependencyEdges).toBe(3);
    expect(result.dependencyHealth.orphanNodes).toHaveLength(0);
  });

  it("ignores non-terminal runs when deriving node last-run status", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
      makeNode("n2", "DIM_ORDERS", "Dimension", { predecessorNodeIDs: ["n1"] }),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r-running", "running", HOURS_AGO(1)),
              runDetails: {
                nodes: [{ nodeID: "n1" }, { nodeID: "n2" }],
                nodesInRun: 2,
              },
            },
            {
              ...makeRun("r-completed", "completed", HOURS_AGO(3), HOURS_AGO(2)),
              runDetails: {
                nodes: [{ nodeID: "n1" }, { nodeID: "n2" }],
                nodesInRun: 2,
              },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.nodeRunStatus).toHaveLength(2);
    expect(result.nodeRunStatus.every((status) => status.lastRunStatus === "passed")).toBe(true);
    expect(result.healthScore).toBe("healthy");
  });

  it("does not classify canceled runs as failures in per-node status", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
      makeNode("n2", "DIM_ORDERS", "Dimension", { predecessorNodeIDs: ["n1"] }),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [
            {
              // Most recent run was canceled — should be skipped
              ...makeRun("r2", "canceled", HOURS_AGO(1), HOURS_AGO(0)),
              runDetails: {
                nodes: [{ nodeID: "n1" }, { nodeID: "n2" }],
                nodesInRun: 2,
              },
            },
            {
              // Previous run completed successfully — should be the node's status
              ...makeRun("r1", "completed", HOURS_AGO(3), HOURS_AGO(2)),
              runDetails: {
                nodes: [{ nodeID: "n1" }, { nodeID: "n2" }],
                nodesInRun: 2,
              },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    // Nodes should reflect the completed run, not the canceled one
    expect(result.nodeRunStatus).toHaveLength(2);
    expect(result.nodeRunStatus.every((s) => s.lastRunStatus === "passed")).toBe(true);
    // Canceled run should not appear in failedRunsLast24h
    expect(result.failedRunsLast24h).toHaveLength(0);
    // Health score should be healthy (no failures, no orphans, all recently run)
    expect(result.healthScore).toBe("healthy");
    // Verify no failure-related health reasons
    expect(result.healthReasons.some((r: string) => r.includes("failed"))).toBe(false);
  });

  it("shows never_run when only run was canceled (no prior completions)", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r1", "canceled", HOURS_AGO(1), HOURS_AGO(0)),
              runDetails: {
                nodes: [{ nodeID: "n1" }],
                nodesInRun: 1,
              },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    // With only a canceled run, node should show as never_run
    expect(result.nodeRunStatus).toHaveLength(1);
    expect(result.nodeRunStatus[0].lastRunStatus).toBe("never_run");
  });

  it("marks node with failed last run correctly", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "STG_ORDERS", "Stage"),
    ];

    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/environments/env-1/nodes") {
        return Promise.resolve({ data: nodes });
      }
      if (path === "/api/v1/runs") {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r1", "failed", HOURS_AGO(2), HOURS_AGO(1)),
              runDetails: {
                nodes: [{ nodeID: "n1" }],
                nodesInRun: 1,
              },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });

    expect(result.nodeRunStatus).toHaveLength(1);
    expect(result.nodeRunStatus[0].lastRunStatus).toBe("failed");
    expect(result.nodeRunStatus[0].nodeName).toBe("STG_ORDERS");
  });
});
