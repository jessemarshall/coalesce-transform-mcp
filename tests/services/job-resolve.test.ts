import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import YAML from "yaml";
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
};

/**
 * Builds a minimal client that serves getWorkspaceJob, listWorkspaceNodes,
 * and the sequential scan that listWorkspaceJobs performs
 * (GET /workspaces/{wid}/jobs/{numericID}).
 *
 * Subgraphs are NOT served by the API — listJobNodes loads them from the repo
 * path, so tests seed a tmp `subgraphs/` folder and pass it as `repoPath`.
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
    throw new Error(`Unexpected GET ${path}`);
  });
  return { get, post: vi.fn(), put: vi.fn(), delete: vi.fn() };
}

let repoDir: string;

function seedSubgraphs(subgraphs: SubgraphRecord[]): string {
  repoDir = mkdtempSync(join(tmpdir(), "coalesce-job-resolve-test-"));
  const sgDir = join(repoDir, "subgraphs");
  mkdirSync(sgDir, { recursive: true });
  for (const sg of subgraphs) {
    writeFileSync(join(sgDir, `${sg.name}.yml`), YAML.stringify(sg), "utf8");
  }
  return repoDir;
}

describe("listJobNodes", () => {
  beforeEach(() => {
    // Isolate from ambient COALESCE_REPO_PATH so tests control repo resolution.
    delete process.env.COALESCE_REPO_PATH;
  });

  afterEach(() => {
    if (repoDir) {
      rmSync(repoDir, { recursive: true, force: true });
      repoDir = "";
    }
  });

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
    });
    const repoPath = seedSubgraphs([{ id: "1", name: "DIM_DATE", steps: ["n1", "n2"] }]);

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "10",
      repoPath,
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
    });
    const repoPath = seedSubgraphs([{ id: "1", name: "A", steps: ["n1"] }]);

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "11",
      repoPath,
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
    });
    const repoPath = seedSubgraphs([{ id: "1", name: "DIM_DATE", steps: ["n1", "n2"] }]);

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "12",
      repoPath,
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
    });
    const repoPath = seedSubgraphs([{ id: "1", name: "ALL", steps: ["n1", "n2"] }]);

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "13",
      repoPath,
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
    });
    const repoPath = seedSubgraphs([]);

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "14",
      repoPath,
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
    });
    const repoPath = seedSubgraphs([]);

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "15",
      repoPath,
    });

    expect(result.summary.warnings.some((w) => w.includes("||"))).toBe(true);
  });

  it("warns when subgraph selectors are used without a repoPath", async () => {
    const job: JobRecord = {
      id: "16",
      name: "JOB_NO_REPO",
      includeSelector: `{ subgraph: "DIM_DATE" }`,
      excludeSelector: "",
    };
    const client = makeClient({
      job,
      nodes: [{ id: "n1", name: "STG_DATE", locationName: "SILVER_STG", nodeType: "Work" }],
    });

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobID: "16",
    });

    expect(result.summary.totalNodes).toBe(0);
    expect(result.summary.unresolvedCount).toBe(1);
    expect(result.summary.warnings.some((w) => w.includes("repoPath"))).toBe(true);
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
    });
    const repoPath = seedSubgraphs([{ id: "1", name: "X", steps: ["n1"] }]);

    const result = await listJobNodes(client as any, {
      workspaceID: "ws1",
      jobName: "JOB_BY_NAME",
      repoPath,
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
    });
    await expect(
      listJobNodes(client as any, { workspaceID: "ws1" } as any)
    ).rejects.toThrow(/jobID or jobName/);
  });
});
