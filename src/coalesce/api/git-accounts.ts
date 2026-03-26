import type { CoalesceClient } from "../../client.js";
import { validatePathSegment } from "../types.js";

export async function listGitAccounts(
  client: CoalesceClient,
  params?: { accountOwner?: string }
): Promise<unknown> {
  return client.get("/api/v1/gitAccounts", {
    ...(params?.accountOwner !== undefined ? { accountOwner: params.accountOwner } : {}),
  });
}

export async function getGitAccount(
  client: CoalesceClient,
  params: { gitAccountID: string; accountOwner?: string }
): Promise<unknown> {
  return client.get(`/api/v1/gitAccounts/${validatePathSegment(params.gitAccountID, "gitAccountID")}`, {
    ...(params.accountOwner !== undefined ? { accountOwner: params.accountOwner } : {}),
  });
}

export async function createGitAccount(
  client: CoalesceClient,
  params: { body: Record<string, unknown>; accountOwner?: string }
): Promise<unknown> {
  const qp = params.accountOwner ? { accountOwner: params.accountOwner } : undefined;
  return client.post("/api/v1/gitAccounts", params.body, qp);
}

export async function updateGitAccount(
  client: CoalesceClient,
  params: { gitAccountID: string; body: Record<string, unknown>; accountOwner?: string }
): Promise<unknown> {
  return client.patch(
    `/api/v1/gitAccounts/${validatePathSegment(params.gitAccountID, "gitAccountID")}`,
    params.body,
    { ...(params.accountOwner !== undefined ? { accountOwner: params.accountOwner } : {}) }
  );
}

export async function deleteGitAccount(
  client: CoalesceClient,
  params: { gitAccountID: string; accountOwner?: string }
): Promise<unknown> {
  return client.delete(
    `/api/v1/gitAccounts/${validatePathSegment(params.gitAccountID, "gitAccountID")}`,
    params.accountOwner ? { accountOwner: params.accountOwner } : undefined
  );
}
