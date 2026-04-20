import type { CoalesceClient } from "../../client.js";
import { isPlainObject } from "../../utils.js";
import {
  getWorkspaceJob,
  listWorkspaceJobs,
} from "../../coalesce/api/jobs.js";
import {
  listWorkspaceNodes,
} from "../../coalesce/api/nodes.js";
import { resolveOptionalRepoPathInput } from "../repo/path.js";
import { scanRepoSubgraphs } from "../subgraphs/repo-scan.js";
import { WORKSPACE_NODE_PAGE_LIMIT } from "../pipelines/planning-types.js";
import { parseJobSelector, type SelectorTerm } from "./selector-parser.js";

const MAX_NODE_PAGES = 500;

type NodeIndexEntry = {
  id: string;
  name: string;
  locationName: string | null;
  nodeType: string | null;
};

type SubgraphEntry = {
  id: string;
  name: string;
  steps: string[];
};

export type JobRef = {
  id: string;
  name: string;
  includeSelector: string;
  excludeSelector: string;
};

export type NodeSummary = {
  id: string;
  name: string;
  location: string | null;
  nodeType: string | null;
};

export type SubgraphGroup = {
  subgraphID: string;
  subgraphName: string;
  nodes: NodeSummary[];
};

export type JobNodesResult = {
  job: JobRef;
  summary: {
    totalNodes: number;
    subgraphCount: number;
    unattachedCount: number;
    unresolvedCount: number;
    warnings: string[];
  };
  nodesBySubgraph: SubgraphGroup[];
  unattached: NodeSummary[];
  /** Selector terms that did not match any node — useful for debugging stale selectors. */
  unresolved: Array<{ term: SelectorTerm; reason: string }>;
};

export async function listJobNodes(
  client: CoalesceClient,
  params: { workspaceID: string; jobID?: string; jobName?: string; repoPath?: string }
): Promise<JobNodesResult> {
  if (!params.jobID && !params.jobName) {
    throw new Error("list_job_nodes requires either jobID or jobName.");
  }

  const jobID = params.jobID
    ? params.jobID
    : await resolveJobIdByName(client, params.workspaceID, params.jobName!);

  const jobRaw = await getWorkspaceJob(client, {
    workspaceID: params.workspaceID,
    jobID,
  });
  const job = coerceJob(jobRaw, jobID);

  const include = parseJobSelector(job.includeSelector);
  const exclude = parseJobSelector(job.excludeSelector);
  const warnings = [...include.warnings, ...exclude.warnings];

  const resolvedRepoPath = resolveOptionalRepoPathInput(params.repoPath);
  const subgraphs = resolvedRepoPath ? loadSubgraphsFromRepo(resolvedRepoPath) : [];
  if (!resolvedRepoPath && hasSubgraphSelector([...include.terms, ...exclude.terms])) {
    warnings.push(
      "Job selectors reference `{ subgraph: NAME }` but no repoPath was provided and the Coalesce API has no subgraph list endpoint — subgraph terms will appear in `unresolved`. Pass repoPath (or set COALESCE_REPO_PATH) to resolve them from the repo's subgraphs/ folder."
    );
  }
  const nodes = await listAllWorkspaceNodes(client, params.workspaceID);

  const nodesByID = new Map(nodes.map((n) => [n.id, n]));
  const subgraphByName = new Map<string, SubgraphEntry>();
  for (const sg of subgraphs) {
    subgraphByName.set(sg.name, sg);
  }
  const nodeIDToSubgraph = new Map<string, SubgraphEntry>();
  for (const sg of subgraphs) {
    for (const step of sg.steps) {
      if (!nodeIDToSubgraph.has(step)) nodeIDToSubgraph.set(step, sg);
    }
  }
  const nodesByLocationName = indexByLocationName(nodes);

  const resolved = resolveTerms(include.terms, {
    nodesByID,
    subgraphByName,
    nodesByLocationName,
  });
  const excluded = resolveTerms(exclude.terms, {
    nodesByID,
    subgraphByName,
    nodesByLocationName,
  });
  for (const id of excluded.ids) {
    resolved.ids.delete(id);
  }

  const groupedByID = new Map<string, SubgraphGroup>();
  const unattached: NodeSummary[] = [];
  for (const id of resolved.ids) {
    const node = nodesByID.get(id);
    if (!node) continue;
    const summary: NodeSummary = {
      id: node.id,
      name: node.name,
      location: node.locationName,
      nodeType: node.nodeType,
    };
    const sg = nodeIDToSubgraph.get(id);
    if (sg) {
      let group = groupedByID.get(sg.id);
      if (!group) {
        group = { subgraphID: sg.id, subgraphName: sg.name, nodes: [] };
        groupedByID.set(sg.id, group);
      }
      group.nodes.push(summary);
    } else {
      unattached.push(summary);
    }
  }

  const nodesBySubgraph = Array.from(groupedByID.values()).sort((a, b) =>
    a.subgraphName.localeCompare(b.subgraphName)
  );
  for (const g of nodesBySubgraph) {
    g.nodes.sort((a, b) => a.name.localeCompare(b.name));
  }
  unattached.sort((a, b) => a.name.localeCompare(b.name));

  return {
    job,
    summary: {
      totalNodes: resolved.ids.size,
      subgraphCount: nodesBySubgraph.length,
      unattachedCount: unattached.length,
      unresolvedCount: resolved.unresolved.length,
      warnings,
    },
    nodesBySubgraph,
    unattached,
    unresolved: resolved.unresolved,
  };
}

async function resolveJobIdByName(
  client: CoalesceClient,
  workspaceID: string,
  name: string
): Promise<string> {
  const response = await listWorkspaceJobs(client, { workspaceID });
  const data =
    isPlainObject(response) && Array.isArray(response.data) ? response.data : [];
  const matches: Array<{ id: string; name: string }> = [];
  for (const entry of data) {
    if (!isPlainObject(entry)) continue;
    const id = typeof entry.id === "string" ? entry.id : null;
    const n = typeof entry.name === "string" ? entry.name : null;
    if (id && n) matches.push({ id, name: n });
  }
  const exact = matches.find((m) => m.name === name);
  if (exact) return exact.id;
  const available = matches
    .slice(0, 20)
    .map((m) => m.name)
    .join(", ");
  throw new Error(
    `No workspace job named "${name}" in workspace "${workspaceID}". ` +
      (available
        ? `Available jobs: ${available}${matches.length > 20 ? ", ..." : ""}.`
        : "No jobs found.")
  );
}

function coerceJob(raw: unknown, fallbackID: string): JobRef {
  const obj = isPlainObject(raw) ? raw : {};
  return {
    id: typeof obj.id === "string" ? obj.id : fallbackID,
    name: typeof obj.name === "string" ? obj.name : "",
    includeSelector:
      typeof obj.includeSelector === "string" ? obj.includeSelector : "",
    excludeSelector:
      typeof obj.excludeSelector === "string" ? obj.excludeSelector : "",
  };
}

async function listAllWorkspaceNodes(
  client: CoalesceClient,
  workspaceID: string
): Promise<NodeIndexEntry[]> {
  const out: NodeIndexEntry[] = [];
  const seenCursors = new Set<string>();
  let next: string | undefined;
  let isFirstPage = true;
  let pageCount = 0;

  while (isFirstPage || next) {
    if (++pageCount > MAX_NODE_PAGES) {
      throw new Error(
        `Workspace node pagination exceeded ${MAX_NODE_PAGES} pages while resolving a job. Aborting.`
      );
    }
    const response = await listWorkspaceNodes(client, {
      workspaceID,
      limit: WORKSPACE_NODE_PAGE_LIMIT,
      orderBy: "id",
      ...(next ? { startingFrom: next } : {}),
    });
    if (!isPlainObject(response)) break;

    if (Array.isArray(response.data)) {
      for (const item of response.data) {
        if (
          !isPlainObject(item) ||
          typeof item.id !== "string" ||
          typeof item.name !== "string"
        ) {
          continue;
        }
        out.push({
          id: item.id,
          name: item.name,
          locationName:
            typeof item.locationName === "string" ? item.locationName : null,
          nodeType: typeof item.nodeType === "string" ? item.nodeType : null,
        });
      }
    }

    const responseNext =
      typeof response.next === "string" && response.next.trim().length > 0
        ? response.next
        : typeof response.next === "number"
          ? String(response.next)
          : undefined;
    if (responseNext) {
      if (seenCursors.has(responseNext)) {
        throw new Error(
          `Workspace node pagination repeated cursor ${responseNext} while resolving a job.`
        );
      }
      seenCursors.add(responseNext);
    }
    next = responseNext;
    isFirstPage = false;
  }

  return out;
}

function loadSubgraphsFromRepo(repoPath: string): SubgraphEntry[] {
  return scanRepoSubgraphs(repoPath).map((s) => ({
    id: s.id,
    name: s.name,
    steps: s.steps,
  }));
}

function hasSubgraphSelector(terms: SelectorTerm[]): boolean {
  return terms.some((t) => t.kind === "subgraph");
}

function indexByLocationName(
  nodes: NodeIndexEntry[]
): Map<string, Map<string, NodeIndexEntry>> {
  const byLoc = new Map<string, Map<string, NodeIndexEntry>>();
  for (const n of nodes) {
    if (!n.locationName) continue;
    let inner = byLoc.get(n.locationName);
    if (!inner) {
      inner = new Map();
      byLoc.set(n.locationName, inner);
    }
    // First-seen wins for ambiguous duplicates (should not occur in well-formed
    // workspaces — location+name is unique in Coalesce).
    if (!inner.has(n.name)) inner.set(n.name, n);
  }
  return byLoc;
}

function resolveTerms(
  terms: SelectorTerm[],
  ctx: {
    nodesByID: Map<string, NodeIndexEntry>;
    subgraphByName: Map<string, SubgraphEntry>;
    nodesByLocationName: Map<string, Map<string, NodeIndexEntry>>;
  }
): { ids: Set<string>; unresolved: Array<{ term: SelectorTerm; reason: string }> } {
  const ids = new Set<string>();
  const unresolved: Array<{ term: SelectorTerm; reason: string }> = [];
  for (const term of terms) {
    if (term.kind === "subgraph") {
      const sg = ctx.subgraphByName.get(term.name);
      if (!sg) {
        unresolved.push({ term, reason: `no subgraph named "${term.name}"` });
        continue;
      }
      let matched = 0;
      for (const step of sg.steps) {
        if (ctx.nodesByID.has(step)) {
          ids.add(step);
          matched++;
        }
      }
      if (matched === 0) {
        unresolved.push({
          term,
          reason: `subgraph "${term.name}" has no steps that match workspace nodes`,
        });
      }
    } else {
      const inner = ctx.nodesByLocationName.get(term.location);
      const node = inner?.get(term.name);
      if (!node) {
        unresolved.push({
          term,
          reason: `no node with location="${term.location}" name="${term.name}"`,
        });
        continue;
      }
      ids.add(node.id);
    }
  }
  return { ids, unresolved };
}
