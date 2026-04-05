import type { CoalesceClient, QueryParams } from "../../client.js";
import type { StartRunInput, RerunInput } from "../types.js";
import { validatePathSegment, buildStartRunBody, buildRerunBody } from "../types.js";

// The Coalesce API uses two distinct run identifiers — this is intentional, not inconsistent naming:
//   runID    (string)  — numeric ID passed as a path segment to REST endpoints (/api/v1/runs/{runID})
//   runCounter (number) — numeric counter used as a query param by scheduler endpoints (/scheduler/runStatus)
// Both refer to the same run but are consumed by different parts of the Coalesce API.

export async function listRuns(
  client: CoalesceClient,
  params: QueryParams
): Promise<unknown> {
  return client.get("/api/v1/runs", params);
}

export async function getRun(
  client: CoalesceClient,
  params: { runID: string }
): Promise<unknown> {
  const { runID } = params;
  return client.get(`/api/v1/runs/${validatePathSegment(runID, "runID")}`, {});
}

export async function getRunResults(
  client: CoalesceClient,
  params: { runID: string }
): Promise<unknown> {
  const { runID } = params;
  return client.get(`/api/v1/runs/${validatePathSegment(runID, "runID")}/results`, {});
}

export async function startRun(
  client: CoalesceClient,
  params: StartRunInput
): Promise<unknown> {
  const body = buildStartRunBody(params);
  return client.post("/scheduler/startRun", body);
}

export async function runStatus(
  client: CoalesceClient,
  params: { runCounter: number }
): Promise<unknown> {
  return client.get("/scheduler/runStatus", { runCounter: params.runCounter });
}

export async function retryRun(
  client: CoalesceClient,
  params: RerunInput
): Promise<unknown> {
  const body = buildRerunBody(params);
  return client.post("/scheduler/rerun", body);
}

export async function cancelRun(
  client: CoalesceClient,
  params: { runID: string; orgID?: string; environmentID: string }
): Promise<unknown> {
  validatePathSegment(params.runID, "runID");
  validatePathSegment(params.environmentID, "environmentID");
  const orgID = params.orgID?.trim() || process.env.COALESCE_ORG_ID?.trim();
  if (!orgID) {
    throw new Error(
      "orgID is required for cancel-run. Provide it explicitly or set COALESCE_ORG_ID."
    );
  }
  return client.post("/scheduler/cancelRun", {
    runID: params.runID,
    orgID,
    environmentID: params.environmentID,
  });
}
