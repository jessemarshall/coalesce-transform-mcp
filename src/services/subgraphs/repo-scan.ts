import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";

import YAML from "yaml";
import { isPlainObject } from "../../utils.js";

export type RepoSubgraph = {
  id: string;
  name: string;
  steps: string[];
  filePath: string;
};

function listYamlFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...listYamlFiles(full));
    } else if (entry.isFile() && /\.ya?ml$/i.test(entry.name)) {
      out.push(full);
    }
  }
  return out;
}

function parseSubgraphFile(filePath: string): RepoSubgraph | null {
  let parsed: unknown;
  try {
    parsed = YAML.parse(readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
  if (!isPlainObject(parsed)) return null;
  const id = parsed.id;
  const name = parsed.name;
  if (typeof id !== "string" || id.length === 0) return null;
  if (typeof name !== "string") return null;
  const rawSteps = Array.isArray(parsed.steps) ? parsed.steps : [];
  const steps = rawSteps.filter((s): s is string => typeof s === "string");
  return { id, name, steps, filePath };
}

/**
 * Scan {repoPath}/subgraphs/ for YAML subgraph definitions.
 * Returns an empty array if the directory does not exist.
 */
export function scanRepoSubgraphs(repoPath: string): RepoSubgraph[] {
  const dir = join(repoPath, "subgraphs");
  if (!existsSync(dir) || !statSync(dir).isDirectory()) {
    return [];
  }
  const out: RepoSubgraph[] = [];
  for (const filePath of listYamlFiles(dir)) {
    const parsed = parseSubgraphFile(filePath);
    if (parsed) out.push(parsed);
  }
  return out;
}

export function findRepoSubgraphByName(
  repoPath: string,
  name: string
): RepoSubgraph | null {
  return scanRepoSubgraphs(repoPath).find((s) => s.name === name) ?? null;
}
