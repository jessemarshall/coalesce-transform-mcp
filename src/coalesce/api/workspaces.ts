import type { CoalesceClient } from "../../client.js";
import { isPlainObject } from "../../utils.js";

export async function listWorkspaces(
  client: CoalesceClient,
  params?: { projectID?: string }
): Promise<unknown> {
  if (params?.projectID) {
    // Single project — fetch with workspaces included
    const project = await client.get(`/api/v1/projects/${params.projectID}`, {
      includeWorkspaces: true,
    });
    return extractWorkspacesFromProjects(
      Array.isArray(project) ? project : [project]
    );
  }

  // All projects — fetch with workspaces included
  const projects = await client.get("/api/v1/projects", {
    includeWorkspaces: true,
  });
  return extractWorkspacesFromProjects(
    Array.isArray(projects) ? projects : []
  );
}

function extractWorkspacesFromProjects(
  projects: unknown[]
): { workspaces: WorkspaceSummary[] } {
  const workspaces: WorkspaceSummary[] = [];

  for (const project of projects) {
    if (!isPlainObject(project)) continue;
    const projectID = typeof project.id === "string" ? project.id : undefined;
    const projectName =
      typeof project.name === "string" ? project.name : undefined;

    const nested = Array.isArray(project.workspaces)
      ? project.workspaces
      : [];
    for (const ws of nested) {
      if (!isPlainObject(ws)) continue;
      workspaces.push({
        id: typeof ws.id === "string" ? ws.id : String(ws.id ?? ""),
        name: typeof ws.name === "string" ? ws.name : undefined,
        projectID,
        projectName,
      });
    }
  }

  return { workspaces };
}

type WorkspaceSummary = {
  id: string;
  name?: string;
  projectID?: string;
  projectName?: string;
};
