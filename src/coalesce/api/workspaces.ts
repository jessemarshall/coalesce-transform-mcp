import type { CoalesceClient, QueryParams } from "../../client.js";
import { validatePathSegment } from "../types.js";

export async function listWorkspaces(
  client: CoalesceClient,
  params: QueryParams = {}
): Promise<unknown> {
  return client.get("/api/v1/workspaces", params);
}

export async function getWorkspace(
  client: CoalesceClient,
  params: { workspaceID: string }
): Promise<unknown> {
  return client.get(
    `/api/v1/workspaces/${validatePathSegment(params.workspaceID, "workspaceID")}`,
    {}
  );
}
