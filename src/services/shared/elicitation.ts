import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  buildJsonToolResponse,
  type JsonToolResponse,
} from "../../coalesce/types.js";

const ACTION_LABELS: Record<string, string> = {
  decline: "declined",
  cancel: "cancelled",
};

/**
 * Require explicit user confirmation before executing a destructive operation.
 *
 * Uses MCP elicitation when the client supports it; falls back to the
 * STOP_AND_CONFIRM convention for non-elicitation-capable clients.
 *
 * Returns `null` when the operation is approved (confirmed=true or user
 * accepted via elicitation). Returns a `JsonToolResponse` when the operation
 * should be blocked — the caller should return this response directly.
 */
export async function requireDestructiveConfirmation(
  server: McpServer,
  toolName: string,
  message: string,
  confirmed?: boolean,
  extra?: Record<string, unknown>,
): Promise<JsonToolResponse | null> {
  if (confirmed === true) return null;

  const clientCapabilities = server.server.getClientCapabilities();
  if (!clientCapabilities?.elicitation?.form) {
    return buildJsonToolResponse(toolName, {
      executed: false,
      STOP_AND_CONFIRM:
        `STOP. ${message} ` +
        `Ask the user for explicit confirmation before proceeding. ` +
        `Once confirmed, call ${toolName} again with confirmed=true.`,
      ...extra,
    });
  }

  const elicitation = await server.server.elicitInput({
    message,
    requestedSchema: {
      type: "object",
      properties: {
        confirmed: {
          type: "boolean",
          title: "Proceed?",
          description: "Select true to proceed, false to cancel.",
        },
      },
      required: ["confirmed"],
    },
  });

  if (
    elicitation.action !== "accept" ||
    elicitation.content?.confirmed !== true
  ) {
    return buildJsonToolResponse(toolName, {
      executed: false,
      cancelled: true,
      reason:
        elicitation.action === "accept"
          ? "User declined the operation."
          : `Operation ${ACTION_LABELS[elicitation.action] ?? elicitation.action} by user.`,
      ...extra,
    });
  }

  return null;
}
