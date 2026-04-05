import type { CoalesceClient, QueryParams } from "../../client.js";
import { validatePathSegment } from "../types.js";

export async function listEnvironments(
  client: CoalesceClient,
  params: QueryParams
): Promise<unknown> {
  return client.get("/api/v1/environments", params);
}

export async function getEnvironment(
  client: CoalesceClient,
  params: { environmentID: string }
): Promise<unknown> {
  const { environmentID } = params;
  return client.get(`/api/v1/environments/${validatePathSegment(environmentID, "environmentID")}`, {});
}

export async function createEnvironment(
  client: CoalesceClient,
  params: {
    project: string;
    name: string;
    oauthEnabled?: boolean;
    devEnv?: boolean;
    connectionAccount?: string;
    runTimeParameters?: Record<string, unknown>;
    tagColors?: { backgroundColor?: string; textColor?: string };
  }
): Promise<unknown> {
  return client.post("/api/v1/environments", params);
}

export async function deleteEnvironment(
  client: CoalesceClient,
  params: { environmentID: string }
): Promise<unknown> {
  const { environmentID } = params;
  return client.delete(
    `/api/v1/environments/${validatePathSegment(environmentID, "environmentID")}`
  );
}
