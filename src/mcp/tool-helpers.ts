import type { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  buildJsonToolResponse,
  handleToolError,
  getToolOutputSchema,
  sanitizeResponse,
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
 * Register a simple passthrough tool: calls an API function with (client, params)
 * and wraps the result in buildJsonToolResponse / handleToolError.
 */
export function registerSimpleTool<S extends z.ZodType>(
  server: McpServer,
  client: CoalesceClient,
  name: string,
  def: ToolDef<S>,
  apiFunc: (client: CoalesceClient, params: z.infer<S>) => Promise<unknown>
): void {
  server.registerTool(
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
        return buildJsonToolResponse(name, def.sanitize ? sanitizeResponse(result) : result);
      } catch (error) {
        return handleToolError(error);
      }
    }) as ToolCallback
  );
}

/**
 * Register a local-only tool that does not call the Coalesce API.
 * Provides the same error handling and response formatting as registerSimpleTool
 * without requiring a CoalesceClient dependency.
 */
export function registerLocalTool<S extends z.ZodType>(
  server: McpServer,
  name: string,
  def: ToolDef<S>,
  handler: (params: z.infer<S>) => unknown | Promise<unknown>
): void {
  server.registerTool(
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
        return buildJsonToolResponse(name, def.sanitize ? sanitizeResponse(result) : result);
      } catch (error) {
        return handleToolError(error);
      }
    }) as ToolCallback
  );
}

/**
 * Register a destructive tool that requires user confirmation before executing.
 */
export function registerDestructiveTool<S extends z.ZodType>(
  server: McpServer,
  client: CoalesceClient,
  name: string,
  def: ToolDef<S> & { confirmMessage: (params: z.infer<S>) => string },
  apiFunc: (client: CoalesceClient, params: z.infer<S>) => Promise<unknown>
): void {
  server.registerTool(
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
        return buildJsonToolResponse(name, def.sanitize ? sanitizeResponse(result) : result);
      } catch (error) {
        return handleToolError(error);
      }
    }) as ToolCallback
  );
}
