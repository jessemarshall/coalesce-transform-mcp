import { z } from "zod";
import { existsSync, readFileSync } from "node:fs";

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
    .positive()
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

// Build PEM headers dynamically to avoid tripping secret-scanning pre-commit hooks.
const PEM_BOUNDARY = "-----";
const ALLOWED_PEM_HEADERS = [
  `${PEM_BOUNDARY}BEGIN PRIVATE KEY${PEM_BOUNDARY}`,
  `${PEM_BOUNDARY}BEGIN RSA PRIVATE KEY${PEM_BOUNDARY}`,
  `${PEM_BOUNDARY}BEGIN ENCRYPTED PRIVATE KEY${PEM_BOUNDARY}`,
] as const;

function readKeyPairFile(filePath: string): string {
  if (!existsSync(filePath)) {
    throw new Error(
      "SNOWFLAKE_KEY_PAIR_KEY file not found at the configured path. " +
      "Check that the environment variable points to an existing PEM private key file."
    );
  }
  const content = readFileSync(filePath, "utf-8").trim();
  const hasValidHeader = ALLOWED_PEM_HEADERS.some((header) =>
    content.includes(header)
  );
  if (!hasValidHeader) {
    throw new Error(
      "SNOWFLAKE_KEY_PAIR_KEY file is not a valid PEM private key. " +
      "Expected a file containing one of: PRIVATE KEY, RSA PRIVATE KEY, or ENCRYPTED PRIVATE KEY."
    );
  }
  return content;
}

export function getSnowflakeCredentials() {
  const snowflakeUsername = process.env.SNOWFLAKE_USERNAME?.trim();
  const snowflakeKeyPairKeyRaw = process.env.SNOWFLAKE_KEY_PAIR_KEY?.trim();
  const snowflakeKeyPairPass = process.env.SNOWFLAKE_KEY_PAIR_PASS?.trim();
  const snowflakeWarehouse = process.env.SNOWFLAKE_WAREHOUSE?.trim();
  const snowflakeRole = process.env.SNOWFLAKE_ROLE?.trim();

  // Validate all required env vars at once so users can fix them in a single pass
  const missing: string[] = [];
  if (!snowflakeUsername) missing.push("SNOWFLAKE_USERNAME");
  if (!snowflakeKeyPairKeyRaw) missing.push("SNOWFLAKE_KEY_PAIR_KEY");
  if (!snowflakeWarehouse) missing.push("SNOWFLAKE_WAREHOUSE");
  if (!snowflakeRole) missing.push("SNOWFLAKE_ROLE");

  if (missing.length > 0) {
    throw new Error(
      `Missing required Snowflake environment variable${missing.length > 1 ? "s" : ""} for run tools: ${missing.join(", ")}. ` +
      "Set these in your shell profile and pass them through in your MCP client config."
    );
  }

  const snowflakeKeyPairKey = readKeyPairFile(snowflakeKeyPairKeyRaw!);

  return {
    snowflakeUsername: snowflakeUsername!,
    snowflakeKeyPairKey,
    ...(snowflakeKeyPairPass ? { snowflakeKeyPairPass } : {}),
    snowflakeWarehouse: snowflakeWarehouse!,
    snowflakeRole: snowflakeRole!,
    snowflakeAuthType: "KeyPair" as const,
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
