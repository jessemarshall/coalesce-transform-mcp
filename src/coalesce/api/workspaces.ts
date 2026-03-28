import type { CoalesceClient } from "../../client.js";
import { CoalesceApiError } from "../../client.js";
import { listProjects } from "./projects.js";

/**
 * The Coalesce API does not expose a /api/v1/workspaces endpoint.
 * Workspaces are only accessible nested under projects via includeWorkspaces=true.
 * These functions wrap the projects endpoint to provide workspace-level access.
 * Note: getWorkspace fetches all workspaces and filters client-side,
 * since there is no direct workspace lookup endpoint.
 */

export async function listWorkspaces(
  client: CoalesceClient
): Promise<unknown> {
  const result = await listProjects(client, { includeWorkspaces: true });
  const envelope = result as { data?: unknown };
  if (!Array.isArray(envelope?.data)) {
    throw new CoalesceApiError(
      "Unexpected response from projects endpoint: missing or non-array 'data' field",
      502
    );
  }
  const projects: unknown[] = envelope.data;
  const workspaces = projects.flatMap((project: unknown) => {
    const p = project as { workspaces?: unknown[] };
    const wsList = Array.isArray(p.workspaces) ? p.workspaces : [];
    return wsList.map((ws: unknown) => ({
      ...(ws as Record<string, unknown>),
      projectID: (project as { id?: string }).id,
    }));
  });
  return { data: workspaces };
}

export async function getWorkspace(
  client: CoalesceClient,
  params: { workspaceID: string }
): Promise<unknown> {
  const { data } = (await listWorkspaces(client)) as { data: Array<Record<string, unknown>> };
  const workspace = data.find((ws) => ws.id === params.workspaceID);
  if (!workspace) {
    throw new CoalesceApiError(
      `Workspace not found: ${params.workspaceID}`,
      404
    );
  }
  return workspace;
}
