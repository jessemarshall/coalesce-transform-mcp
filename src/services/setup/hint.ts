/**
 * Short, stable suffix appended to error messages whose root cause is almost
 * always a setup gap (missing/invalid access token, missing ~/.coa/config
 * profile, missing workspaces.yml). Points the user at the guided prompt so
 * they don't have to debug it manually.
 *
 * Keep terse — it is concatenated onto existing error strings.
 */
export const SETUP_HINT = "Run `/coalesce-setup` or call `diagnose_setup` to diagnose and fix.";

/**
 * Append SETUP_HINT to a message unless it's already there (idempotent — some
 * call sites stack wrappers around the same error).
 */
export function withSetupHint(message: string): string {
  if (message.includes("/coalesce-setup")) return message;
  const trimmed = message.trimEnd();
  const separator = trimmed.endsWith(".") ? " " : ". ";
  return `${trimmed}${separator}${SETUP_HINT}`;
}
