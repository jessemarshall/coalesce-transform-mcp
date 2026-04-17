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
    const record = params as Record<string, unknown>;
    const top = record.workspaceID;
    if (typeof top === "string" && top.length > 0) return top;
    const runDetails = record.runDetails;
    if (runDetails && typeof runDetails === "object" && !Array.isArray(runDetails)) {
      const rd = runDetails as Record<string, unknown>;
      const ws = rd.workspaceID;
      if (typeof ws === "string" && ws.length > 0) return ws;
      const env = rd.environmentID;
      if (typeof env === "string" && env.length > 0) return env;
    }
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
 * Preview of what a destructive tool is about to mutate. Returned from a tool's
 * `resolve` hook and surfaced in both the confirmation message and the final
 * response so the caller can verify targets before and after the operation.
 */
export interface DestructivePreview {
  /** The primary target of the mutation (what the user named). */
  primary: { type: string; id: string; name?: string };
  /** Additional entities that will be affected (cascaded deletes, dependents). */
  affected?: Array<{ type: string; id: string; name?: string; note?: string }>;
  /** Arbitrary extra context to include in the confirmation and response. */
  context?: Record<string, unknown>;
}

/**
 * Best-effort extraction of a human-readable name from a Coalesce API response.
 * Returns undefined when the shape is unexpected — callers should fall back to
 * the ID for confirmation text.
 */
export function extractEntityName(value: unknown): string | undefined {
  if (!value || typeof value !== "object") return undefined;
  const candidate = (value as Record<string, unknown>).name
    ?? (value as Record<string, unknown>).label
    ?? (value as Record<string, unknown>).displayName;
  return typeof candidate === "string" && candidate.length > 0 ? candidate : undefined;
}

function formatPreviewForConfirmation(preview: DestructivePreview): string {
  const name = preview.primary.name ? `"${preview.primary.name}"` : `(unnamed)`;
  const header = `Target: ${preview.primary.type} ${name} [${preview.primary.id}]`;
  if (!preview.affected || preview.affected.length === 0) return header;
  const lines = preview.affected.slice(0, 10).map((item) => {
    const n = item.name ? `"${item.name}"` : "(unnamed)";
    const note = item.note ? ` — ${item.note}` : "";
    return `  • ${item.type} ${n} [${item.id}]${note}`;
  });
  const suffix = preview.affected.length > 10
    ? `\n  … and ${preview.affected.length - 10} more`
    : "";
  return `${header}\nAlso affected (${preview.affected.length}):\n${lines.join("\n")}${suffix}`;
}

/**
 * Define a destructive tool that requires user confirmation before executing.
 * The `server` parameter is still needed for elicitation.
 *
 * Safety guardrail: when `resolve` is provided, it runs BEFORE confirmation and
 * BEFORE mutation — even when `confirmed: true` is already set. If resolve throws
 * (typical case: the primary target returns 404), the mutation is blocked. This
 * prevents destructive calls from succeeding against phantom/unresolvable IDs.
 */
export function defineDestructiveTool<S extends z.ZodType>(
  server: McpServer,
  client: CoalesceClient,
  name: string,
  def: ToolDef<S> & {
    confirmMessage: (params: z.infer<S>, preview?: DestructivePreview) => string;
    resolve?: (client: CoalesceClient, params: z.infer<S>) => Promise<DestructivePreview>;
  },
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
        let preview: DestructivePreview | undefined;
        if (def.resolve) {
          try {
            preview = await def.resolve(client, params);
          } catch (err) {
            const reason = err instanceof Error ? err.message : String(err);
            throw new Error(
              `Refusing to run ${name}: could not resolve target before mutation — ${reason}`
            );
          }
        }

        const messageBody = def.confirmMessage(params, preview);
        const previewBlock = preview
          ? `\n\n${formatPreviewForConfirmation(preview)}`
          : "";
        const approvalResponse = await requireDestructiveConfirmation(
          server,
          name,
          `${messageBody}${previewBlock}`,
          params.confirmed,
          preview ? { preview } : undefined,
        );
        if (approvalResponse) return approvalResponse;

        const result = await apiFunc(client, params);
        const body = def.sanitize ? sanitizeResponse(result) : result;
        const enriched = preview && body && typeof body === "object" && !Array.isArray(body)
          ? { ...(body as Record<string, unknown>), resolvedTargets: preview }
          : body;
        return buildJsonToolResponse(name, enriched, {
          workspaceID: extractWorkspaceID(params),
        });
      } catch (error) {
        return handleToolError(error);
      }
    }) as ToolCallback,
  ];
}
