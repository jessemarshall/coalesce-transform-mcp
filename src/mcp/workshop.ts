import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  validatePathSegment,
} from "../coalesce/types.js";
import {
  openWorkshop,
  workshopInstruct,
  getWorkshopStatus,
  workshopClose,
} from "../services/pipelines/workshop.js";

export function registerWorkshopTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "pipeline_workshop_open",
    {
      title: "Pipeline Workshop Open",
      description:
        "Open a new pipeline workshop session for iterative, conversational pipeline building. " +
        "The workshop maintains state between calls so you can incrementally add nodes, change join keys, " +
        "add filters, rename nodes, and refine the plan before creating anything.\n\n" +
        "Optionally provide an initial intent (e.g., 'join customers and orders on customer_id') " +
        "to bootstrap the session with initial nodes.\n\n" +
        "Returns a sessionID that must be passed to subsequent workshop calls.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID to build the pipeline in"),
        intent: z
          .string()
          .optional()
          .describe(
            "Optional initial intent to bootstrap the session (e.g., 'join CUSTOMERS and ORDERS on CUSTOMER_ID')"
          ),
      }),
      outputSchema: getToolOutputSchema("pipeline_workshop_open"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await openWorkshop(client, {
          workspaceID: validatePathSegment(params.workspaceID, "workspaceID"),
          intent: params.intent,
        });
        return buildJsonToolResponse("pipeline_workshop_open", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "pipeline_workshop_instruct",
    {
      title: "Pipeline Workshop Instruct",
      description:
        "Send a natural language instruction to an open pipeline workshop session. " +
        "The instruction modifies the current plan — you can:\n\n" +
        "- Add nodes: 'add a staging node for PAYMENTS'\n" +
        "- Join sources: 'join CUSTOMERS and ORDERS on CUSTOMER_ID'\n" +
        "- Add aggregation: 'aggregate total REVENUE by REGION'\n" +
        "- Change join key: 'change the join key to ORDER_ID'\n" +
        "- Add filters: 'add filter for STATUS = active'\n" +
        "- Add/remove columns: 'add column FULL_NAME' or 'remove column MIDDLE_NAME'\n" +
        "- Rename nodes: 'rename STG_ORDERS to STG_SALES'\n" +
        "- Remove nodes: 'remove the ORPHAN node'\n\n" +
        "Each instruction is processed against the current session state, " +
        "and the updated plan is returned.",
      inputSchema: z.object({
        sessionID: z.string().describe("The workshop session ID from pipeline_workshop_open"),
        instruction: z.string().describe("Natural language instruction to modify the plan"),
      }),
      outputSchema: getToolOutputSchema("pipeline_workshop_instruct"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await workshopInstruct(client, {
          sessionID: params.sessionID,
          instruction: params.instruction,
        });
        return buildJsonToolResponse("pipeline_workshop_instruct", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "pipeline_workshop_status",
    {
      title: "Pipeline Workshop Status",
      description:
        "Get the current state of a pipeline workshop session, including all planned nodes, " +
        "their configuration, and the instruction history.",
      inputSchema: z.object({
        sessionID: z.string().describe("The workshop session ID"),
      }),
      outputSchema: getToolOutputSchema("pipeline_workshop_status"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const session = getWorkshopStatus(params.sessionID);
        if (!session) {
          throw new Error(
            `Session "${params.sessionID}" not found. Use pipeline_workshop_open to start a new session.`
          );
        }
        return buildJsonToolResponse("pipeline_workshop_status", session);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "pipeline_workshop_close",
    {
      title: "Pipeline Workshop Close",
      description:
        "Close a pipeline workshop session and clean up the session state. " +
        "If there are uncreated nodes in the plan, use build_pipeline_from_intent or plan_pipeline " +
        "to create them before closing.",
      inputSchema: z.object({
        sessionID: z.string().describe("The workshop session ID to close"),
      }),
      outputSchema: getToolOutputSchema("pipeline_workshop_close"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = workshopClose(params.sessionID);
        return buildJsonToolResponse("pipeline_workshop_close", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
