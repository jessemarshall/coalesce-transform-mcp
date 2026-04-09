import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { fetchAllEnvironmentNodes } from "../services/cache/snapshots.js";
import {
  READ_ONLY_ANNOTATIONS,
  validatePathSegment,
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool } from "../mcp/tool-helpers.js";

export async function getEnvironmentOverview(
  client: CoalesceClient,
  params: { environmentID: string }
): Promise<{ environment: unknown; nodes: unknown[] }> {
  const environmentID = validatePathSegment(params.environmentID, "environmentID");
  const basePath = `/api/v1/environments/${environmentID}`;

  const [environment, nodes] = await Promise.all([
    client.get(basePath),
    fetchAllEnvironmentNodes(client, { environmentID }),
  ]);

  return { environment, nodes: nodes.items };
}

export function defineGetEnvironmentOverview(_server: McpServer, client: CoalesceClient): ToolDefinition[] {
  return [
    defineSimpleTool(client, "get_environment_overview", {
      title: "Get Environment Overview",
      description:
        "Get environment details and all its deployed nodes in a single call.\n\nArgs:\n  - environmentID (string, required): The environment ID\n\nReturns:\n  { environment: EnvironmentObject, nodes: NodeObject[] }",
      inputSchema: z.object({
        environmentID: z.string().describe("The environment ID"),
      }),
      annotations: READ_ONLY_ANNOTATIONS,
    }, getEnvironmentOverview),
  ];
}
