import type { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  sanitizeResponse,
  type ToolDefinition,
} from "../coalesce/types.js";
import { requireDestructiveConfirmation } from "../services/shared/elicitation.js";

type ToolAnnotations = {
  readOnlyHint?: boolean;
  idempotentHint?: boolean;
  destructiveHint?: boolean;
  openWorldHint?: boolean;
};

type ToolDef<S extends z.ZodType> = {
  title: string;
  description: string;
  inputSchema: S;
  annotations: ToolAnnotations;
  sanitize?: boolean;
};

// The MCP SDK's ToolCallback type triggers deep TypeScript recursion when combined
// with Zod generics.  This single cast helper isolates the type unsafety so the
// rest of the file stays fully typed.  Track: https://github.com/modelcontextprotocol/typescript-sdk/issues — remove when SDK exports a generic-friendly callback type.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ToolCallback = any;

/**
 * Extract a workspaceID from an arbitrary tool input object so the auto-cache
 * writer can partition cached responses by workspace. Returns undefined for
 * tools whose inputs are not workspace-scoped (e.g. list_workspaces).
 */
function extractWorkspaceID(params: unknown): string | undefined {
  if (params && typeof params === "object" && !Array.isArray(params)) {
    const value = (params as Record<string, unknown>).workspaceID;
    if (typeof value === "string" && value.length > 0) return value;
  }
  return undefined;
}

/**
 * Define a simple passthrough tool: calls an API function with (client, params)
 * and wraps the result in buildJsonToolResponse / handleToolError.
 */
export function defineSimpleTool<S extends z.ZodType>(
  client: CoalesceClient,
  name: string,
  def: ToolDef<S>,
  apiFunc: (client: CoalesceClient, params: z.infer<S>) => Promise<unknown>
): ToolDefinition {
  return [
    name,
    {
      title: def.title,
      description: def.description,
      inputSchema: def.inputSchema,
      outputSchema: getToolOutputSchema(name),
      annotations: def.annotations,
    },
    (async (params: z.infer<S>) => {
      try {
        const result = await apiFunc(client, params);
        return buildJsonToolResponse(name, def.sanitize ? sanitizeResponse(result) : result, {
          workspaceID: extractWorkspaceID(params),
        });
      } catch (error) {
        return handleToolError(error);
      }
    }) as ToolCallback,
  ];
}

/**
 * Define a local-only tool that does not call the Coalesce API.
 * Provides the same error handling and response formatting as defineSimpleTool
 * without requiring a CoalesceClient dependency.
 */
export function defineLocalTool<S extends z.ZodType>(
  name: string,
  def: ToolDef<S>,
  handler: (params: z.infer<S>) => unknown | Promise<unknown>
): ToolDefinition {
  return [
    name,
    {
      title: def.title,
      description: def.description,
      inputSchema: def.inputSchema,
      outputSchema: getToolOutputSchema(name),
      annotations: def.annotations,
    },
    (async (params: z.infer<S>) => {
      try {
        const result = await handler(params);
        return buildJsonToolResponse(name, def.sanitize ? sanitizeResponse(result) : result, {
          workspaceID: extractWorkspaceID(params),
        });
      } catch (error) {
        return handleToolError(error);
      }
    }) as ToolCallback,
  ];
}

/**
 * Define a destructive tool that does not talk to the Coalesce REST API
 * (e.g., shells out to the coa CLI). Same confirmation gate as
 * defineDestructiveTool, but no CoalesceClient dependency.
 */
export function defineDestructiveLocalTool<S extends z.ZodType>(
  server: McpServer,
  name: string,
  def: ToolDef<S> & { confirmMessage: (params: z.infer<S>) => string },
  handler: (params: z.infer<S>) => unknown | Promise<unknown>
): ToolDefinition {
  return [
    name,
    {
      title: def.title,
      description: def.description,
      inputSchema: def.inputSchema,
      outputSchema: getToolOutputSchema(name),
      annotations: def.annotations,
    },
    (async (params: z.infer<S>) => {
      try {
        const approvalResponse = await requireDestructiveConfirmation(
          server,
          name,
          def.confirmMessage(params),
          params.confirmed,
        );
        if (approvalResponse) return approvalResponse;

        const result = await handler(params);
        return buildJsonToolResponse(name, def.sanitize ? sanitizeResponse(result) : result, {
          workspaceID: extractWorkspaceID(params),
        });
      } catch (error) {
        return handleToolError(error);
      }
    }) as ToolCallback,
  ];
}

/**
 * Define a destructive tool that requires user confirmation before executing.
 * The `server` parameter is still needed for elicitation.
 */
export function defineDestructiveTool<S extends z.ZodType>(
  server: McpServer,
  client: CoalesceClient,
  name: string,
  def: ToolDef<S> & { confirmMessage: (params: z.infer<S>) => string },
  apiFunc: (client: CoalesceClient, params: z.infer<S>) => Promise<unknown>
): ToolDefinition {
  return [
    name,
    {
      title: def.title,
      description: def.description,
      inputSchema: def.inputSchema,
      outputSchema: getToolOutputSchema(name),
      annotations: def.annotations,
    },
    (async (params: z.infer<S>) => {
      try {
        const approvalResponse = await requireDestructiveConfirmation(
          server,
          name,
          def.confirmMessage(params),
          params.confirmed,
        );
        if (approvalResponse) return approvalResponse;

        const result = await apiFunc(client, params);
        return buildJsonToolResponse(name, def.sanitize ? sanitizeResponse(result) : result, {
          workspaceID: extractWorkspaceID(params),
        });
      } catch (error) {
        return handleToolError(error);
      }
    }) as ToolCallback,
  ];
}
