import type { CoalesceClient, QueryParams, RequestOptions } from "../../client.js";
import { validatePathSegment } from "../types.js";

export async function listEnvironmentNodes(
  client: CoalesceClient,
  params: QueryParams & { environmentID: string },
  options?: RequestOptions
): Promise<unknown> {
  const { environmentID, ...queryParams } = params;
  const url = `/api/v1/environments/${validatePathSegment(environmentID, "environmentID")}/nodes`;
  return options ? client.get(url, queryParams, options) : client.get(url, queryParams);
}

export async function listWorkspaceNodes(
  client: CoalesceClient,
  params: QueryParams & { workspaceID: string },
  options?: RequestOptions
): Promise<unknown> {
  const { workspaceID, ...queryParams } = params;
  const url = `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes`;
  return options ? client.get(url, queryParams, options) : client.get(url, queryParams);
}

export async function getEnvironmentNode(
  client: CoalesceClient,
  params: { environmentID: string; nodeID: string }
): Promise<unknown> {
  const { environmentID, nodeID } = params;
  return client.get(
    `/api/v1/environments/${validatePathSegment(environmentID, "environmentID")}/nodes/${validatePathSegment(nodeID, "nodeID")}`,
    {}
  );
}

export async function getWorkspaceNode(
  client: CoalesceClient,
  params: { workspaceID: string; nodeID: string }
): Promise<unknown> {
  const { workspaceID, nodeID } = params;
  return client.get(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes/${validatePathSegment(nodeID, "nodeID")}`,
    {}
  );
}

export async function createWorkspaceNode(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeType: string;
    predecessorNodeIDs?: string[];
  }
): Promise<unknown> {
  const { workspaceID, nodeType, predecessorNodeIDs } = params;
  const payload: Record<string, unknown> = { nodeType };
  if (predecessorNodeIDs !== undefined) {
    payload.predecessorNodeIDs = predecessorNodeIDs;
  }
  return client.post(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes`,
    payload
  );
}

export async function setWorkspaceNode(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    body: Record<string, unknown>;
  }
): Promise<unknown> {
  const { workspaceID, nodeID, body } = params;
  return client.put(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes/${validatePathSegment(nodeID, "nodeID")}`,
    body
  );
}

export async function deleteWorkspaceNode(
  client: CoalesceClient,
  params: { workspaceID: string; nodeID: string }
): Promise<unknown> {
  const { workspaceID, nodeID } = params;
  return client.delete(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes/${validatePathSegment(nodeID, "nodeID")}`
  );
}
