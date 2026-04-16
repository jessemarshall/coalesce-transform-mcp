import { z } from "zod";

import { resolveSnowflakeAuth } from "../services/config/credentials.js";

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

export const UserCredentialsSchema = z.discriminatedUnion("snowflakeAuthType", [
  z.object({
    snowflakeAuthType: z.literal("KeyPair"),
    snowflakeUsername: z.string().describe("Snowflake account username"),
    snowflakeKeyPairKey: z
      .string()
      .describe(
        "File path to a PEM-encoded private key for Snowflake key-pair auth."
      ),
    snowflakeKeyPairPass: z
      .string()
      .optional()
      .describe(
        "Password to decrypt an encrypted private key. Only required when the private key is encrypted."
      ),
    snowflakeWarehouse: z.string().describe("Snowflake compute warehouse"),
    snowflakeRole: z.string().describe("Snowflake user role"),
  }),
  z.object({
    snowflakeAuthType: z.literal("Basic"),
    snowflakeUsername: z.string().describe("Snowflake account username"),
    snowflakePassword: z
      .string()
      .describe("Snowflake Programmatic Access Token (PAT)"),
    snowflakeWarehouse: z.string().describe("Snowflake compute warehouse"),
    snowflakeRole: z.string().describe("Snowflake user role"),
  }),
]);

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

export function getSnowflakeCredentials() {
  const auth = resolveSnowflakeAuth();
  // Strip the `sources` metadata — the API body only wants the credential fields.
  const { sources: _sources, ...body } = auth;
  return body;
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
