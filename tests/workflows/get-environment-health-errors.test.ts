import { describe, it, expect, vi } from "vitest";
import {
  getEnvironmentHealth,
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

describe("get-environment-health error paths", () => {
  it("throws on empty environmentID", async () => {
    const client = createMockClient();
    await expect(
      getEnvironmentHealth(client as any, { environmentID: "" })
    ).rejects.toThrow("Invalid environmentID: must not be empty");
  });

  it("throws on environmentID with path separators", async () => {
    const client = createMockClient();
    await expect(
      getEnvironmentHealth(client as any, { environmentID: "../escape" })
    ).rejects.toThrow("Invalid environmentID");
  });

  it("throws on environmentID with control characters", async () => {
    const client = createMockClient();
    await expect(
      getEnvironmentHealth(client as any, { environmentID: "env\x00id" })
    ).rejects.toThrow("Invalid environmentID");
  });

  it("propagates API error when nodes fetch fails", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new Error("Network timeout"));

    await expect(
      getEnvironmentHealth(client as any, { environmentID: "env-1" })
    ).rejects.toThrow("Network timeout");
  });

  it("propagates API error when runs fetch fails but nodes succeeds", async () => {
    const client = createMockClient();
    let callCount = 0;
    client.get.mockImplementation(() => {
      callCount++;
      // fetchAllEnvironmentNodes and fetchAllRuns run in parallel via Promise.all,
      // so we distinguish by call order: first call is nodes, second is runs
      if (callCount === 1) {
        return Promise.resolve({ data: [makeNode("n1", "NODE", "Stage")] });
      }
      return Promise.reject(new Error("Runs API down"));
    });

    await expect(
      getEnvironmentHealth(client as any, { environmentID: "env-1" })
    ).rejects.toThrow("Runs API down");
  });

  it("handles nodes response with non-object items gracefully", async () => {
    const client = createMockClient();
    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes")) {
        return Promise.resolve({ data: ["string-item", 42, null, makeNode("n1", "VALID", "Stage")] });
      }
      if (path.includes("/runs")) {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });
    // Only the valid node object should be counted
    expect(result.totalNodes).toBe(1);
    expect(result.nodesByType).toEqual({ Stage: 1 });
  });

  it("handles runs response with non-object items gracefully", async () => {
    const client = createMockClient();
    const nodes = [makeNode("n1", "NODE", "Stage")];

    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes")) {
        return Promise.resolve({ data: nodes });
      }
      if (path.includes("/runs")) {
        return Promise.resolve({ data: ["not-a-run", null, 123] });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });
    // Invalid runs should be filtered out
    expect(result.failedRunsLast24h).toHaveLength(0);
    expect(result.nodeRunStatus[0].lastRunStatus).toBe("never_run");
  });

  it("returns critical when >50% of nodes have failed last run", async () => {
    const client = createMockClient();
    const nodes = [
      makeNode("n1", "A", "Stage"),
      makeNode("n2", "B", "Stage"),
      makeNode("n3", "C", "Stage"),
    ];

    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes")) {
        return Promise.resolve({ data: nodes });
      }
      if (path.includes("/runs")) {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r1", "failed", HOURS_AGO(2), HOURS_AGO(1)),
              runDetails: { nodes: [{ nodeID: "n1" }, { nodeID: "n2" }], nodesInRun: 2 },
            },
            {
              ...makeRun("r2", "completed", HOURS_AGO(3), HOURS_AGO(2)),
              runDetails: { nodes: [{ nodeID: "n3" }], nodesInRun: 1 },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });
    expect(result.healthScore).toBe("critical");
    expect(result.healthReasons.some((r: string) => r.includes("failed last run"))).toBe(true);
  });

  it("handles run with missing runDetails gracefully", async () => {
    const client = createMockClient();
    const nodes = [makeNode("n1", "NODE", "Stage")];

    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes")) {
        return Promise.resolve({ data: nodes });
      }
      if (path.includes("/runs")) {
        return Promise.resolve({
          data: [
            { ...makeRun("r1", "completed", HOURS_AGO(1), HOURS_AGO(0)) },
            // No runDetails at all
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });
    // Without runDetails, the run can't be attributed to any node
    expect(result.nodeRunStatus[0].lastRunStatus).toBe("never_run");
  });

  it("handles run with empty nodes array in runDetails", async () => {
    const client = createMockClient();
    const nodes = [makeNode("n1", "NODE", "Stage")];

    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes")) {
        return Promise.resolve({ data: nodes });
      }
      if (path.includes("/runs")) {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r1", "completed", HOURS_AGO(1), HOURS_AGO(0)),
              runDetails: { nodes: [], nodesInRun: 0 },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });
    expect(result.nodeRunStatus[0].lastRunStatus).toBe("never_run");
  });

  it("picks the most recent terminal run for each node", async () => {
    const client = createMockClient();
    const nodes = [makeNode("n1", "NODE", "Stage")];

    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes")) {
        return Promise.resolve({ data: nodes });
      }
      if (path.includes("/runs")) {
        return Promise.resolve({
          data: [
            {
              ...makeRun("r-old", "failed", HOURS_AGO(5), HOURS_AGO(4)),
              runDetails: { nodes: [{ nodeID: "n1" }], nodesInRun: 1 },
            },
            {
              ...makeRun("r-new", "completed", HOURS_AGO(2), HOURS_AGO(1)),
              runDetails: { nodes: [{ nodeID: "n1" }], nodesInRun: 1 },
            },
          ],
        });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });
    expect(result.nodeRunStatus[0].lastRunStatus).toBe("passed");
    expect(result.nodeRunStatus[0].lastRunTime).toBe(HOURS_AGO(1));
  });

  it("handles nodes with missing id or name fields", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes")) {
        return Promise.resolve({
          data: [
            { nodeType: "Stage" }, // missing id and name
            { id: "n2", nodeType: "Dimension" }, // missing name
          ],
        });
      }
      if (path.includes("/runs")) {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });
    expect(result.totalNodes).toBe(2);
    expect(result.nodeRunStatus[0].nodeName).toBe("unnamed");
    expect(result.nodeRunStatus[1].nodeName).toBe("unnamed");
  });

  it("handles nodes with missing nodeType", async () => {
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes")) {
        return Promise.resolve({
          data: [{ id: "n1", name: "MYSTERY" }],
        });
      }
      if (path.includes("/runs")) {
        return Promise.resolve({ data: [] });
      }
      return Promise.resolve({});
    });

    const result = await getEnvironmentHealth(client as any, { environmentID: "env-1" });
    expect(result.nodesByType).toEqual({ unknown: 1 });
  });
});
