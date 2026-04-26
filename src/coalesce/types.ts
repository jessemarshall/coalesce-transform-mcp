import { z } from "zod";
import { CoalesceApiError } from "../client.js";

// Re-export from extracted modules so existing imports continue to work
export {
  type JsonToolError,
  JsonToolOutputSchema,
  getToolOutputSchema,
} from "./tool-schemas.js";

export {
  type JsonToolResponse,
  buildJsonToolResponse,
  handleToolError,
} from "./tool-response.js";

export {
  RunDetailsSchema,
  UserCredentialsSchema,
  StartRunParams,
  type StartRunInput,
  RerunDetailsSchema,
  RerunParams,
  type RerunInput,
  buildRerunBody,
  getSnowflakeCredentials,
  buildStartRunBody,
} from "./run-schemas.js";

// Workspace node body schema — validates known structural fields while allowing
// node-type-specific extras through. Used by set-workspace-node.
export const WorkspaceNodeBodySchema = z
  .object({
    name: z.string().optional(),
    description: z.string().optional(),
    nodeType: z.string().optional(),
    database: z.string().optional(),
    schema: z.string().optional(),
    locationName: z.string().optional(),
    storageLocations: z.array(z.unknown()).optional(),
    config: z.record(z.unknown()).optional(),
    metadata: z
      .object({
        columns: z.array(z.unknown()).optional(),
      })
      .passthrough()
      .optional(),
  })
  .passthrough();

// Pagination params — only used by endpoints that support it
export const PaginationParams = z.object({
  limit: z.number().int().positive().max(500).optional().describe("Number of results to return (max 500)"),
  startingFrom: z
    .string()
    .optional()
    .describe("Cursor from previous response's next field"),
  orderBy: z
    .string()
    .optional()
    .describe("Field to sort by (required with startingFrom)"),
  orderByDirection: z
    .enum(["asc", "desc"])
    .optional()
    .describe("Sort direction"),
});

// Common annotations
export const READ_ONLY_ANNOTATIONS = {
  readOnlyHint: true,
  idempotentHint: true,
  destructiveHint: false,
  openWorldHint: true,
} as const;

export const READ_ONLY_LOCAL_ANNOTATIONS = {
  readOnlyHint: true,
  idempotentHint: true,
  destructiveHint: false,
  openWorldHint: false,
} as const;

export const WRITE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: false,
  destructiveHint: false,
  openWorldHint: true,
} as const;

export const IDEMPOTENT_WRITE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: true,
  destructiveHint: false,
  openWorldHint: true,
} as const;

export const DESTRUCTIVE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: false,
  destructiveHint: true,
  openWorldHint: true,
} as const;

const SANITIZED_KEYS = new Set([
  "userCredentials",
  "snowflakeKeyPairKey",
  "snowflakeKeyPairPass",
  "snowflakePassword",
  "gitToken",
  "accessToken",
]);

export function sanitizeResponse(data: unknown): unknown {
  if (Array.isArray(data)) {
    return data.map(sanitizeResponse);
  }
  if (data && typeof data === "object") {
    const obj = data as Record<string, unknown>;
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      if (SANITIZED_KEYS.has(key)) continue;
      result[key] = sanitizeResponse(value);
    }
    return result;
  }
  return data;
}

/**
 * A declarative tool definition tuple matching the `server.registerTool(name, config, handler)` signature.
 * Modules export arrays of these; `server.ts` registers them in a central loop.
 *
 * The handler uses explicit `any` params so that callback parameters are contextually
 * typed (avoiding TS7006 implicit-any errors that would occur with a bare `any` handler slot).
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ToolDefinition = [name: string, config: any, handler: (...args: any[]) => any];

export function validatePathSegment(value: string, name: string): string {
  if (value.length === 0) {
    throw new Error(`Invalid ${name}: must not be empty`);
  }
  if (/[\u0000-\u001F\u007F]/.test(value)) {
    throw new Error(
      `Invalid ${name}: must not contain control characters. Pass the raw ID returned by a list_/get_ tool, e.g. "abc123" or "env-456".`
    );
  }
  if (/[\/\\?#%]|\.\./.test(value)) {
    throw new Error(
      `Invalid ${name}: must not contain path separators, '..', or URI delimiters like '?', '#', or '%'. Pass the raw ID returned by a list_/get_ tool (e.g. "env-1", "ws-abc"), not a URL or path.`
    );
  }
  return value;
}
