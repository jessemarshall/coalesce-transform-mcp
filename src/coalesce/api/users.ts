import type { CoalesceClient, QueryParams } from "../../client.js";
import { validatePathSegment } from "../types.js";

export async function listOrgUsers(
  client: CoalesceClient,
  params: QueryParams
): Promise<unknown> {
  return client.get("/api/v1/users", params);
}

export async function getUserRoles(
  client: CoalesceClient,
  params: { userID: string; projectID?: string; environmentID?: string }
): Promise<unknown> {
  const { userID, ...queryParams } = params;
  return client.get(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}`, queryParams);
}

export async function listUserRoles(
  client: CoalesceClient,
  params: QueryParams
): Promise<unknown> {
  return client.get("/api/v2/userRoles", params);
}

export async function setOrgRole(
  client: CoalesceClient,
  params: { userID: string; body: Record<string, unknown> }
): Promise<unknown> {
  const { userID, body } = params;
  return client.put(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/organizationRole`, body);
}

export async function setProjectRole(
  client: CoalesceClient,
  params: { userID: string; projectID: string; body: Record<string, unknown> }
): Promise<unknown> {
  const { userID, projectID, body } = params;
  return client.put(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/projects/${validatePathSegment(projectID, "projectID")}`, body);
}

export async function deleteProjectRole(
  client: CoalesceClient,
  params: { userID: string; projectID: string }
): Promise<unknown> {
  const { userID, projectID } = params;
  return client.delete(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/projects/${validatePathSegment(projectID, "projectID")}`);
}

export async function setEnvRole(
  client: CoalesceClient,
  params: { userID: string; environmentID: string; body: Record<string, unknown> }
): Promise<unknown> {
  const { userID, environmentID, body } = params;
  return client.put(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/environments/${validatePathSegment(environmentID, "environmentID")}`, body);
}

export async function deleteEnvRole(
  client: CoalesceClient,
  params: { userID: string; environmentID: string }
): Promise<unknown> {
  const { userID, environmentID } = params;
  return client.delete(`/api/v2/userRoles/${validatePathSegment(userID, "userID")}/environments/${validatePathSegment(environmentID, "environmentID")}`);
}

