import type { CoalesceClient } from "../../client.js";
import { validatePathSegment } from "../types.js";
import { scanResourcesByID } from "./scan.js";

export async function listEnvironmentJobs(
  client: CoalesceClient,
  params: { environmentID: string; limit?: number | string }
): Promise<unknown> {
  const eid = validatePathSegment(params.environmentID, "environmentID");
  return scanResourcesByID(
    client,
    `/api/v1/environments/${eid}/jobs`,
    params.limit ? Number(params.limit) : undefined
  );
}

export async function listWorkspaceJobs(
  client: CoalesceClient,
  params: { workspaceID: string; limit?: number | string }
): Promise<unknown> {
  const wid = validatePathSegment(params.workspaceID, "workspaceID");
  return scanResourcesByID(
    client,
    `/api/v1/workspaces/${wid}/jobs`,
    params.limit ? Number(params.limit) : undefined
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

export async function getWorkspaceJob(
  client: CoalesceClient,
  params: { workspaceID: string; jobID: string }
): Promise<unknown> {
  const { workspaceID, jobID } = params;
  return client.get(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/jobs/${validatePathSegment(jobID, "jobID")}`,
    {}
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
