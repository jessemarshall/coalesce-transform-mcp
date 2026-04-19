import { describe, it, expect, vi } from "vitest";
import { listJobNodes } from "../../src/services/jobs/resolve.js";

type NodeRecord = {
  id: string;
  name: string;
  locationName?: string;
  nodeType?: string;
};

type SubgraphRecord = { id: string; name: string; steps: string[] };

type JobRecord = {
  id: string;
  name: string;
  includeSelector: string;
  excludeSelector: string;
};

type Fixture = {
  workspaceID?: string;
  job: JobRecord;
  jobsIndex?: JobRecord[]; // for listWorkspaceJobs scan (name→id lookup)
  nodes: NodeRecord[];
  subgraphs: SubgraphRecord[];
};

/**
 * Builds a minimal client that serves getWorkspaceJob, listWorkspaceNodes,
 * listWorkspaceSubgraphs, and the sequential scan that listWorkspaceJobs
 * performs (GET /workspaces/{wid}/jobs/{numericID}).
 */
function makeClient(f: Fixture) {
  const get = vi.fn(async (path: string) => {
    // getWorkspaceJob: /api/v1/workspaces/<wid>/jobs/<jid>
    const jobMatch = path.match(/\/api\/v1\/workspaces\/[^/]+\/jobs\/([^/?]+)$/);
    if (jobMatch) {
      const jid = jobMatch[1];
      if (jid === f.job.id) return f.job;
      const hit = (f.jobsIndex ?? []).find((j) => j.id === jid);
      if (hit) return hit;
      // simulate 404 via the CoalesceApiError shape so scanResourcesByID skips it
      const { CoalesceApiError } = await import("../../src/client.js");
      throw new CoalesceApiError("Not found", 404);
    }
    // listWorkspaceNodes
    if (/\/api\/v1\/workspaces\/[^/]+\/nodes$/.test(path)) {
      return {
        data: f.nodes.map((n) => ({
          id: n.id,
          name: n.name,
          locationName: n.locationName ?? null,
          nodeType: n.nodeType ?? null,
        })),
      };
    }
    // listWorkspaceSubgraphs uses scanResourcesByID /api/v1/workspaces/<wid>/subgraphs/<id>
    const sgMatch = path.match(/\/api\/v1\/workspaces\/[^/]+\/subgraphs\/([^/?]+)$/);
    if (sgMatch) {
      const id = sgMatch[1];
      const hit = f.subgraphs.find((s) => s.id === id);
      if (hit) return hit;
      const { CoalesceApiError } = await import("../../src/client.js");
      throw new CoalesceApiError("Not found", 404);
    }
    throw new Error(`Unexpected GET ${path}`);
  });
  return { get, post: vi.fn(), put: vi.fn(), delete: vi.fn() };
}

describe("listJobNodes", () => {
  it("groups nodes by subgraph when a subgraph selector matches", async () => {
    const job: JobRecord = {
      id: "10",
      name: "JOB_DIM_DATE",
      includeSelector: `{ subgraph: "DIM_DATE" }`,
      excludeSelector: "",
    };
    const client = makeClient({
      job,
      nodes: [
        { id: "n1", name: "STG_DATE", locationName: "SILVER_STG", nodeType: "Work" },
        { id: "n2", name: "DIM_DATE", locationName: "SILVER_EDM", nodeType: "Dimension" },
        { id: "n3", name: "OTHER", locationName: "SILVER_STG", nodeType: "Work" },
      ],
      subgraphs: [{ id: "1", name: "DIM_DATE", steps: ["n1", "n2"] }],
    });

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "10",
    });

    expect(result.job).toEqual({
      id: "10",
      name: "JOB_DIM_DATE",
      includeSelector: `{ subgraph: "DIM_DATE" }`,
      excludeSelector: "",
    });
    expect(result.summary.totalNodes).toBe(2);
    expect(result.summary.subgraphCount).toBe(1);
    expect(result.summary.unattachedCount).toBe(0);
    expect(result.nodesBySubgraph).toHaveLength(1);
    expect(result.nodesBySubgraph[0].subgraphName).toBe("DIM_DATE");
    expect(result.nodesBySubgraph[0].nodes.map((n) => n.id).sort()).toEqual(["n1", "n2"]);
    expect(result.unattached).toEqual([]);
  });

  it("puts location+name matches that aren't in any subgraph into unattached", async () => {
    const job: JobRecord = {
      id: "11",
      name: "JOB_MIXED",
      includeSelector: `{ subgraph: "A" } OR { location: SILVER_STG name: STG_LOOSE }`,
      excludeSelector: "",
    };
    const client = makeClient({
      job,
      nodes: [
        { id: "n1", name: "STG_X", locationName: "SILVER_STG", nodeType: "Work" },
        { id: "n2", name: "STG_LOOSE", locationName: "SILVER_STG", nodeType: "Work" },
      ],
      subgraphs: [{ id: "1", name: "A", steps: ["n1"] }],
    });

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "11",
    });

    expect(result.summary.totalNodes).toBe(2);
    expect(result.nodesBySubgraph).toHaveLength(1);
    expect(result.nodesBySubgraph[0].nodes).toEqual([
      { id: "n1", name: "STG_X", location: "SILVER_STG", nodeType: "Work" },
    ]);
    expect(result.unattached).toEqual([
      { id: "n2", name: "STG_LOOSE", location: "SILVER_STG", nodeType: "Work" },
    ]);
  });

  it("groups a location+name match into its subgraph when the node is a step", async () => {
    const job: JobRecord = {
      id: "12",
      name: "JOB_DIRECT",
      includeSelector: `{ location: SILVER_EDM name: DIM_DATE }`,
      excludeSelector: "",
    };
    const client = makeClient({
      job,
      nodes: [
        { id: "n1", name: "STG_DATE", locationName: "SILVER_STG", nodeType: "Work" },
        { id: "n2", name: "DIM_DATE", locationName: "SILVER_EDM", nodeType: "Dimension" },
      ],
      subgraphs: [{ id: "1", name: "DIM_DATE", steps: ["n1", "n2"] }],
    });

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "12",
    });

    expect(result.summary.totalNodes).toBe(1);
    expect(result.nodesBySubgraph).toHaveLength(1);
    expect(result.nodesBySubgraph[0].nodes).toEqual([
      { id: "n2", name: "DIM_DATE", location: "SILVER_EDM", nodeType: "Dimension" },
    ]);
    expect(result.unattached).toEqual([]);
  });

  it("applies excludeSelector after include resolution", async () => {
    const job: JobRecord = {
      id: "13",
      name: "JOB_EXCLUDED",
      includeSelector: `{ subgraph: "ALL" }`,
      excludeSelector: `{ location: SILVER_STG name: STG_LOOSE }`,
    };
    const client = makeClient({
      job,
      nodes: [
        { id: "n1", name: "STG_X", locationName: "SILVER_STG", nodeType: "Work" },
        { id: "n2", name: "STG_LOOSE", locationName: "SILVER_STG", nodeType: "Work" },
      ],
      subgraphs: [{ id: "1", name: "ALL", steps: ["n1", "n2"] }],
    });

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "13",
    });

    expect(result.summary.totalNodes).toBe(1);
    expect(result.nodesBySubgraph[0].nodes.map((n) => n.id)).toEqual(["n1"]);
  });

  it("reports unresolved terms when a selector matches nothing", async () => {
    const job: JobRecord = {
      id: "14",
      name: "JOB_STALE",
      includeSelector: `{ subgraph: "DOES_NOT_EXIST" } OR { location: LOC name: GONE }`,
      excludeSelector: "",
    };
    const client = makeClient({
      job,
      nodes: [{ id: "n1", name: "KEEP", locationName: "LOC", nodeType: "Work" }],
      subgraphs: [],
    });

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "14",
    });

    expect(result.summary.totalNodes).toBe(0);
    expect(result.summary.unresolvedCount).toBe(2);
    expect(result.unresolved).toHaveLength(2);
    expect(result.unresolved[0].reason).toContain("no subgraph");
    expect(result.unresolved[1].reason).toContain("no node");
  });

  it("surfaces parser warnings in summary.warnings", async () => {
    const job: JobRecord = {
      id: "15",
      name: "JOB_BADSEL",
      includeSelector: `{ subgraph: "A" || subgraph: "B" }`,
      excludeSelector: "",
    };
    const client = makeClient({
      job,
      nodes: [],
      subgraphs: [],
    });

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "15",
    });

    expect(result.summary.warnings).toHaveLength(1);
    expect(result.summary.warnings[0]).toContain("||");
  });

  it("resolves jobID from jobName when ID is not provided", async () => {
    const job: JobRecord = {
      id: "42",
      name: "JOB_BY_NAME",
      includeSelector: `{ subgraph: "X" }`,
      excludeSelector: "",
    };
    const client = makeClient({
      job,
      jobsIndex: [job],
      nodes: [{ id: "n1", name: "A", locationName: "L", nodeType: "Work" }],
      subgraphs: [{ id: "1", name: "X", steps: ["n1"] }],
    });

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobName: "JOB_BY_NAME",
    });

    expect(result.job.id).toBe("42");
    expect(result.summary.totalNodes).toBe(1);
  });

  it("throws when neither jobID nor jobName is provided", async () => {
    const client = makeClient({
      job: {
        id: "1",
        name: "",
        includeSelector: "",
        excludeSelector: "",
      },
      nodes: [],
      subgraphs: [],
    });
    await expect(
      listJobNodes(client as any, { workspaceID: "ws1" } as any)
    ).rejects.toThrow(/jobID or jobName/);
  });
});
