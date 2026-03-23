import type { CoalesceClient } from "../../client.js";
import { validatePathSegment } from "../types.js";

export async function getWorkspaceSubgraph(
  client: CoalesceClient,
  params: { workspaceID: string; subgraphID: string }
): Promise<unknown> {
  const { workspaceID, subgraphID } = params;
  return client.get(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/subgraphs/${validatePathSegment(subgraphID, "subgraphID")}`,
    {}
  );
}

export async function createWorkspaceSubgraph(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    name: string;
    steps: string[];
  }
): Promise<unknown> {
  const { workspaceID, ...body } = params;
  return client.post(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/subgraphs`,
    body
  );
}

export async function updateWorkspaceSubgraph(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    subgraphID: string;
    name: string;
    steps: string[];
  }
): Promise<unknown> {
  const { workspaceID, subgraphID, ...body } = params;
  return client.put(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/subgraphs/${validatePathSegment(subgraphID, "subgraphID")}`,
    body
  );
}

export async function deleteWorkspaceSubgraph(
  client: CoalesceClient,
  params: { workspaceID: string; subgraphID: string }
): Promise<unknown> {
  const { workspaceID, subgraphID } = params;
  return client.delete(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/subgraphs/${validatePathSegment(subgraphID, "subgraphID")}`
  );
}
