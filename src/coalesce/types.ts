import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { z } from "zod";

// Pagination params — only used by endpoints that support it
export const PaginationParams = z.object({
  limit: z.number().optional().describe("Number of results to return"),
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
} as const;

export const WRITE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: false,
  destructiveHint: false,
} as const;

export const IDEMPOTENT_WRITE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: true,
  destructiveHint: false,
} as const;

export const DESTRUCTIVE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: false,
  destructiveHint: true,
} as const;

const DEFAULT_AUTO_CACHE_MAX_BYTES = 32 * 1024;

export type JsonToolResponse = {
  content: { type: "text"; text: string }[];
};

type JsonToolResponseOptions = {
  baseDir?: string;
  maxInlineBytes?: number;
};

// --- startRun / run-and-wait schemas ---

export const RunDetailsSchema = z.object({
  environmentID: z.string().describe("The environment being refreshed"),
  includeNodesSelector: z
    .string()
    .optional()
    .describe("Nodes included for an ad-hoc job"),
  excludeNodesSelector: z
    .string()
    .optional()
    .describe("Nodes excluded for an ad-hoc job"),
  jobID: z.string().optional().describe("The ID of a job being run"),
  parallelism: z
    .number()
    .int()
    .optional()
    .describe("Max parallel nodes to run (API default: 16)"),
  forceIgnoreWorkspaceStatus: z
    .boolean()
    .optional()
    .describe(
      "Allow refresh even if last deploy failed (API default: false). Use with caution."
    ),
});

export const UserCredentialsSchema = z.object({
  snowflakeUsername: z.string().describe("Snowflake account username"),
  snowflakeKeyPairKey: z
    .string()
    .describe(
      "PEM-encoded private key for Snowflake auth. Use \\n for line breaks in JSON."
    ),
  snowflakeKeyPairPass: z
    .string()
    .optional()
    .describe(
      "Password to decrypt an encrypted private key. Only required when the private key is encrypted."
    ),
  snowflakeWarehouse: z.string().describe("Snowflake compute warehouse"),
  snowflakeRole: z.string().describe("Snowflake user role"),
});

export const StartRunParams = z.object({
  runDetails: RunDetailsSchema,
  parameters: z
    .record(z.string())
    .optional()
    .describe("Arbitrary key-value parameters to pass to the run"),
  confirmRunAllNodes: z
    .boolean()
    .optional()
    .describe(
      "Must be set to true when no jobID, includeNodesSelector, or excludeNodesSelector is provided. " +
      "This confirms you intend to run ALL nodes in the environment."
    ),
});

export type StartRunInput = z.infer<typeof StartRunParams>;

// --- rerun / retry-and-wait schemas ---

export const RerunDetailsSchema = z.object({
  runID: z.string().describe("The run ID to retry"),
  forceIgnoreWorkspaceStatus: z
    .boolean()
    .optional()
    .describe(
      "Allow refresh even if last deploy failed (API default: false). Use with caution."
    ),
});

export const RerunParams = z.object({
  runDetails: RerunDetailsSchema,
  parameters: z
    .record(z.string())
    .optional()
    .describe("Arbitrary key-value parameters to pass to the rerun"),
});

export type RerunInput = z.infer<typeof RerunParams>;

export function buildRerunBody(params: RerunInput) {
  const userCredentials = getSnowflakeCredentials();
  return {
    runDetails: params.runDetails,
    userCredentials,
    ...(params.parameters ? { parameters: params.parameters } : {}),
  };
}

function readKeyPairFile(filePath: string): string {
  if (!existsSync(filePath)) {
    throw new Error(
      `SNOWFLAKE_KEY_PAIR_KEY file not found: ${filePath}`
    );
  }
  const content = readFileSync(filePath, "utf-8").trim();
  if (!content.includes("-----BEGIN")) {
    throw new Error(
      `SNOWFLAKE_KEY_PAIR_KEY file does not contain a valid PEM key: ${filePath}`
    );
  }
  return content;
}

export function getSnowflakeCredentials() {
  const snowflakeUsername = process.env.SNOWFLAKE_USERNAME;
  const snowflakeKeyPairKeyRaw = process.env.SNOWFLAKE_KEY_PAIR_KEY;
  const snowflakeKeyPairPass = process.env.SNOWFLAKE_KEY_PAIR_PASS;
  const snowflakeWarehouse = process.env.SNOWFLAKE_WAREHOUSE;
  const snowflakeRole = process.env.SNOWFLAKE_ROLE;

  if (!snowflakeUsername) {
    throw new Error(
      "SNOWFLAKE_USERNAME environment variable is required for Snowflake Key Pair run tools."
    );
  }
  if (!snowflakeKeyPairKeyRaw) {
    throw new Error(
      "SNOWFLAKE_KEY_PAIR_KEY environment variable is required for Snowflake Key Pair run tools."
    );
  }
  const snowflakeKeyPairKey = readKeyPairFile(snowflakeKeyPairKeyRaw);
  if (!snowflakeWarehouse) {
    throw new Error(
      "SNOWFLAKE_WAREHOUSE environment variable is required for Snowflake Key Pair run tools."
    );
  }
  if (!snowflakeRole) {
    throw new Error(
      "SNOWFLAKE_ROLE environment variable is required for Snowflake Key Pair run tools."
    );
  }

  return {
    snowflakeUsername,
    snowflakeKeyPairKey,
    ...(snowflakeKeyPairPass ? { snowflakeKeyPairPass } : {}),
    snowflakeWarehouse,
    snowflakeRole,
    snowflakeAuthType: "KeyPair" as const,
  };
}

const SANITIZED_KEYS = new Set([
  "userCredentials",
  "snowflakeKeyPairKey",
  "snowflakeKeyPairPass",
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

function slugifyFileComponent(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function getAutoCacheMaxBytes(): number {
  const raw = process.env.COALESCE_MCP_AUTO_CACHE_MAX_BYTES;
  if (raw === undefined) {
    return DEFAULT_AUTO_CACHE_MAX_BYTES;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return DEFAULT_AUTO_CACHE_MAX_BYTES;
  }

  return parsed;
}

function buildAutoCacheFilePath(
  toolName: string,
  cachedAt: string,
  baseDir: string
): string {
  const directory = join(baseDir, "data", "auto-cache");
  mkdirSync(directory, { recursive: true });
  const timestamp = cachedAt.replace(/[:.]/g, "-");
  const safeToolName = slugifyFileComponent(toolName) || "tool-response";
  return join(directory, `${timestamp}-${safeToolName}-${randomUUID()}.json`);
}

export function buildJsonToolResponse(
  toolName: string,
  result: unknown,
  options: JsonToolResponseOptions = {}
): JsonToolResponse {
  const text = JSON.stringify(result, null, 2);
  const maxInlineBytes = options.maxInlineBytes ?? getAutoCacheMaxBytes();
  const sizeBytes = Buffer.byteLength(text, "utf8");

  if (sizeBytes <= maxInlineBytes) {
    return {
      content: [{ type: "text", text }],
    };
  }

  const cachedAt = new Date().toISOString();
  const baseDir = options.baseDir ?? process.cwd();
  const filePath = buildAutoCacheFilePath(toolName, cachedAt, baseDir);
  try {
    writeFileSync(filePath, `${text}\n`, "utf8");
  } catch {
    return {
      content: [{ type: "text", text }],
    };
  }

  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(
          {
            autoCached: true,
            toolName,
            cachedAt,
            filePath,
            sizeBytes,
            maxInlineBytes,
            message:
              "Full response was automatically cached to disk because it exceeded the inline response threshold.",
          },
          null,
          2
        ),
      },
    ],
  };
}

export function validatePathSegment(value: string, name: string): string {
  if (value.length === 0) {
    throw new Error(`Invalid ${name}: must not be empty`);
  }
  if (/[\/\\]|\.\./.test(value)) {
    throw new Error(
      `Invalid ${name}: must not contain path separators or '..'`
    );
  }
  return value;
}

export function handleToolError(
  error: unknown
): { isError: true; content: { type: "text"; text: string }[] } {
  const message = error instanceof Error ? error.message : String(error);
  return {
    isError: true,
    content: [{ type: "text" as const, text: message }],
  };
}

export function buildStartRunBody(params: StartRunInput) {
  const { runDetails } = params;
  const hasNodeScope =
    runDetails.jobID ||
    runDetails.includeNodesSelector ||
    runDetails.excludeNodesSelector;

  if (!hasNodeScope && !params.confirmRunAllNodes) {
    throw new Error(
      "No jobID, includeNodesSelector, or excludeNodesSelector was provided. " +
      "This will run ALL nodes in the environment. " +
      "Set confirmRunAllNodes to true to confirm this is intentional."
    );
  }

  const userCredentials = getSnowflakeCredentials();
  return {
    runDetails,
    userCredentials,
    ...(params.parameters ? { parameters: params.parameters } : {}),
  };
}
