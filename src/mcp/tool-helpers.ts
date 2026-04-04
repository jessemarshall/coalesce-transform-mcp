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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- return shape matches at runtime
    (async (params: any) => {
      try {
        const result = await apiFunc(client, params);
        return buildJsonToolResponse(name, def.sanitize ? sanitizeResponse(result) : result);
      } catch (error) {
        return handleToolError(error);
      }
    }) as any
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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- return shape matches at runtime
    (async (params: any) => {
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
    }) as any
  );
}
