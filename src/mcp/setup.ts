import { z } from "zod";
import type { CoalesceClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type ToolDefinition,
} from "../coalesce/types.js";
import { defineSimpleTool } from "./tool-helpers.js";
import { diagnoseSetup } from "../services/setup/diagnose.js";

export function defineSetupTools(client: CoalesceClient): ToolDefinition[] {
  return [
    defineSimpleTool(
      client,
      "diagnose_setup",
      {
        title: "Diagnose Setup",
        description:
          "Stateless probe that reports which first-time-setup pieces are configured: Coalesce access token, Snowflake credentials (for run tools + coa_create/coa_run), local repo path + whether it's a COA project, and a best-effort `coa doctor` check when applicable.\n\nReturns a structured report plus ordered nextSteps. Use this during or after the /coalesce-setup prompt flow to confirm progress between phases.\n\nNever throws. Safe to call repeatedly.\n\nReturns:\n  {\n    accessToken: { status, ... },\n    snowflakeCreds: { status, ... },\n    repoPath: { status, ... },\n    coaDoctor: { status, ... },\n    nextSteps: string[],\n    ready: boolean\n  }",
        inputSchema: z.object({}),
        annotations: READ_ONLY_ANNOTATIONS,
      },
      async (client) => diagnoseSetup(client)
    ),
  ];
}
