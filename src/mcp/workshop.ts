import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  buildJsonToolResponse,
  handleToolError,
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
  server.tool(
    "pipeline-workshop-open",
    "Open a new pipeline workshop session for iterative, conversational pipeline building. " +
      "The workshop maintains state between calls so you can incrementally add nodes, change join keys, " +
      "add filters, rename nodes, and refine the plan before creating anything.\n\n" +
      "Optionally provide an initial intent (e.g., 'join customers and orders on customer_id') " +
      "to bootstrap the session with initial nodes.\n\n" +
      "Returns a sessionID that must be passed to subsequent workshop calls.",
    {
      workspaceID: z.string().describe("The workspace ID to build the pipeline in"),
      intent: z
        .string()
        .optional()
        .describe(
          "Optional initial intent to bootstrap the session (e.g., 'join CUSTOMERS and ORDERS on CUSTOMER_ID')"
        ),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await openWorkshop(client, {
          workspaceID: validatePathSegment(params.workspaceID, "workspaceID"),
          intent: params.intent,
        });
        return buildJsonToolResponse("pipeline-workshop-open", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "pipeline-workshop-instruct",
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
    {
      sessionID: z.string().describe("The workshop session ID from pipeline-workshop-open"),
      instruction: z.string().describe("Natural language instruction to modify the plan"),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await workshopInstruct(client, {
          sessionID: params.sessionID,
          instruction: params.instruction,
        });
        return buildJsonToolResponse("pipeline-workshop-instruct", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "pipeline-workshop-status",
    "Get the current state of a pipeline workshop session, including all planned nodes, " +
      "their configuration, and the instruction history.",
    {
      sessionID: z.string().describe("The workshop session ID"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const session = getWorkshopStatus(params.sessionID);
        if (!session) {
          return buildJsonToolResponse("pipeline-workshop-status", {
            error: `Session "${params.sessionID}" not found. Use pipeline-workshop-open to start a new session.`,
          });
        }
        return buildJsonToolResponse("pipeline-workshop-status", session);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "pipeline-workshop-close",
    "Close a pipeline workshop session and clean up the session state. " +
      "If there are uncreated nodes in the plan, use build-pipeline-from-intent or plan-pipeline " +
      "to create them before closing.",
    {
      sessionID: z.string().describe("The workshop session ID to close"),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = workshopClose(params.sessionID);
        return buildJsonToolResponse("pipeline-workshop-close", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
