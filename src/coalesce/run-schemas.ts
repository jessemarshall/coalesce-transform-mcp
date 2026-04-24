import { z } from "zod";

import { resolveSnowflakeAuth } from "../services/config/credentials.js";

// Shared validator: runID must be a numeric run ID string, not a UUID.
// The REST API rejects non-numeric IDs with an opaque HTTP error; this surfaces
// the constraint at the MCP validation boundary with a helpful message.
export const RunIDSchema = z
  .string()
  .min(1)
  .regex(
    /^\d+$/,
    "runID must be a numeric run ID (integer), e.g. '401' — not the UUID from a run URL."
  );

// --- startRun / run-and-wait schemas ---

export const RunDetailsSchema = z
  .object({
    environmentID: z
      .string()
      .optional()
      .describe(
        "Numeric ID of the deployed environment to run against. Provide either environmentID or workspaceID, not both."
      ),
    workspaceID: z
      .string()
      .optional()
      .describe(
        "Numeric ID of the workspace to run against (development run). Provide either environmentID or workspaceID, not both."
      ),
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
  })
  .refine(
    (d) => Boolean(d.environmentID) !== Boolean(d.workspaceID),
    {
      message:
        "Provide exactly one of runDetails.environmentID or runDetails.workspaceID.",
    }
  );

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
      "This confirms you intend to run ALL nodes in the target."
    ),
});

export type StartRunInput = z.infer<typeof StartRunParams>;

// --- rerun / retry-and-wait schemas ---

export const RerunDetailsSchema = z.object({
  runID: RunIDSchema.describe("The numeric run ID (integer, e.g. '401') to retry — not the UUID from a run URL."),
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
      "This will run ALL nodes in the target. " +
      "Set confirmRunAllNodes to true to confirm this is intentional."
    );
  }

  // The Coalesce scheduler only reads `runDetails.environmentID` for routing — the
  // field is polymorphic and accepts a workspace numeric ID there too.
  const { workspaceID, environmentID, ...rest } = runDetails;
  const target = environmentID ?? workspaceID;
  if (!target || (environmentID && workspaceID)) {
    throw new Error(
      "Provide exactly one of runDetails.environmentID or runDetails.workspaceID."
    );
  }
  const apiRunDetails = { ...rest, environmentID: target };

  const userCredentials = getSnowflakeCredentials();
  return {
    runDetails: apiRunDetails,
    userCredentials,
    ...(params.parameters ? { parameters: params.parameters } : {}),
  };
}
