import type { CoalesceClient, QueryParams } from "../../client.js";
import { validatePathSegment } from "../types.js";

export async function listWorkspaceJobs(
  client: CoalesceClient,
  params: { workspaceID: string } & QueryParams
): Promise<unknown> {
  const { workspaceID, ...query } = params;
  return client.get(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/jobs`,
    query
  );
}

export async function listEnvironmentJobs(
  client: CoalesceClient,
  params: { environmentID: string } & QueryParams
): Promise<unknown> {
  const { environmentID, ...query } = params;
  return client.get(
    `/api/v1/environments/${validatePathSegment(environmentID, "environmentID")}/jobs`,
    query
  );
}

export async function createWorkspaceJob(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    name: string;
    includeSelector: string;
    excludeSelector: string;
  }
): Promise<unknown> {
  const { workspaceID, ...body } = params;
  return client.post(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/jobs`,
    body
  );
}

export async function getEnvironmentJob(
  client: CoalesceClient,
  params: { environmentID: string; jobID: string }
): Promise<unknown> {
  const { environmentID, jobID } = params;
  return client.get(
    `/api/v1/environments/${validatePathSegment(environmentID, "environmentID")}/jobs/${validatePathSegment(jobID, "jobID")}`,
    {}
  );
}

export async function updateWorkspaceJob(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    jobID: string;
    name: string;
    includeSelector: string;
    excludeSelector: string;
  }
): Promise<unknown> {
  const { workspaceID, jobID, ...body } = params;
  return client.put(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/jobs/${validatePathSegment(jobID, "jobID")}`,
    body
  );
}

export async function deleteWorkspaceJob(
  client: CoalesceClient,
  params: { workspaceID: string; jobID: string }
): Promise<unknown> {
  const { workspaceID, jobID } = params;
  return client.delete(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/jobs/${validatePathSegment(jobID, "jobID")}`
  );
}
