import type { CoalesceClient } from "../../client.js";
import { validatePathSegment } from "../types.js";

export async function listProjects(
  client: CoalesceClient,
  params?: { includeWorkspaces?: boolean; includeJobs?: boolean }
): Promise<unknown> {
  return client.get("/api/v1/projects", {
    ...(params?.includeWorkspaces !== undefined ? { includeWorkspaces: params.includeWorkspaces } : {}),
    ...(params?.includeJobs !== undefined ? { includeJobs: params.includeJobs } : {}),
  });
}

export async function getProject(
  client: CoalesceClient,
  params: { projectID: string; includeWorkspaces?: boolean; includeJobs?: boolean }
): Promise<unknown> {
  return client.get(`/api/v1/projects/${validatePathSegment(params.projectID, "projectID")}`, {
    ...(params.includeWorkspaces !== undefined ? { includeWorkspaces: params.includeWorkspaces } : {}),
    ...(params.includeJobs !== undefined ? { includeJobs: params.includeJobs } : {}),
  });
}

export async function createProject(
  client: CoalesceClient,
  params: { body: Record<string, unknown> }
): Promise<unknown> {
  return client.post("/api/v1/projects", params.body);
}

export async function updateProject(
  client: CoalesceClient,
  params: {
    projectID: string;
    body: Record<string, unknown>;
    includeWorkspaces?: boolean;
    includeJobs?: boolean;
  }
): Promise<unknown> {
  return client.patch(
    `/api/v1/projects/${validatePathSegment(params.projectID, "projectID")}`,
    params.body,
    {
      ...(params.includeWorkspaces !== undefined ? { includeWorkspaces: params.includeWorkspaces } : {}),
      ...(params.includeJobs !== undefined ? { includeJobs: params.includeJobs } : {}),
    }
  );
}

export async function deleteProject(
  client: CoalesceClient,
  params: { projectID: string }
): Promise<unknown> {
  return client.delete(`/api/v1/projects/${validatePathSegment(params.projectID, "projectID")}`);
}
