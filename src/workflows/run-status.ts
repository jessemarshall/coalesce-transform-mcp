const TERMINAL_RUN_STATUS_VALUES = ["completed", "failed", "canceled"] as const;
const NON_TERMINAL_RUN_STATUS_VALUES = ["waitingToRun", "running"] as const;

export type TerminalRunStatus = (typeof TERMINAL_RUN_STATUS_VALUES)[number];
export type NonTerminalRunStatus = (typeof NON_TERMINAL_RUN_STATUS_VALUES)[number];
export type KnownRunStatus = TerminalRunStatus | NonTerminalRunStatus;

const TERMINAL_RUN_STATUSES = new Set<string>(TERMINAL_RUN_STATUS_VALUES);
const KNOWN_RUN_STATUSES = new Set<string>([
  ...TERMINAL_RUN_STATUS_VALUES,
  ...NON_TERMINAL_RUN_STATUS_VALUES,
]);

export const DOCUMENTED_RUN_STATUSES = [
  ...NON_TERMINAL_RUN_STATUS_VALUES,
  ...TERMINAL_RUN_STATUS_VALUES,
] as const;

export function formatRunStatusForMessage(runStatus: unknown): string {
  return typeof runStatus === "string" ? runStatus : "unknown";
}

export function validateRunStatus(runCounter: number, runStatus: unknown): KnownRunStatus {
  const expected = DOCUMENTED_RUN_STATUSES.join(", ");

  if (typeof runStatus !== "string") {
    throw new Error(
      `Run ${runCounter} returned a non-string runStatus (${typeof runStatus}). Expected one of: ${expected}.`
    );
  }

  if (!KNOWN_RUN_STATUSES.has(runStatus)) {
    throw new Error(
      `Run ${runCounter} returned unexpected runStatus '${runStatus}'. Expected one of: ${expected}.`
    );
  }

  return runStatus as KnownRunStatus;
}

export function isTerminalRunStatus(runStatus: string): runStatus is TerminalRunStatus {
  return TERMINAL_RUN_STATUSES.has(runStatus);
}
