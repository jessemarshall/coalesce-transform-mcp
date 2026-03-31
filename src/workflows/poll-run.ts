import { CoalesceApiError, type CoalesceClient } from "../client.js";
import {
  remainingTimeMs,
  serializeResultsError,
  sleepWithAbort,
  type WorkflowProgressReporter,
} from "./progress.js";
import {
  formatRunStatusForMessage,
  isTerminalRunStatus,
  validateRunStatus,
} from "./run-status.js";

export interface PollRunOptions {
  client: CoalesceClient;
  runCounter: number;
  label: string;
  pollIntervalMs: number;
  timeoutMs: number;
  startedAt: number;
  signal?: AbortSignal;
  reportProgress?: WorkflowProgressReporter;
}

export async function pollRunToCompletion(opts: PollRunOptions): Promise<unknown> {
  const { client, runCounter, label, pollIntervalMs, timeoutMs, startedAt, signal, reportProgress } = opts;

  let lastStatus: unknown = null;
  let pollCount = 0;

  while (remainingTimeMs(startedAt, timeoutMs) > 0) {
    const nextPollDelay = Math.min(pollIntervalMs, remainingTimeMs(startedAt, timeoutMs));
    await sleepWithAbort(nextPollDelay, signal);

    const statusTimeoutMs = remainingTimeMs(startedAt, timeoutMs);
    if (statusTimeoutMs <= 0) {
      break;
    }

    let status: Record<string, unknown>;
    try {
      status = (await client.get(
        "/scheduler/runStatus",
        { runCounter },
        { timeoutMs: statusTimeoutMs }
      )) as Record<string, unknown>;
    } catch (error) {
      if (error instanceof CoalesceApiError && error.status === 408) {
        pollCount += 1;
        await reportProgress?.(
          `Status check ${pollCount} for ${label} ${runCounter} timed out. Retrying while time remains.`
        );
        continue;
      }
      throw error;
    }
    lastStatus = status;
    pollCount += 1;

    const runStatus = status.runStatus;
    await reportProgress?.(
      `Status check ${pollCount} for ${label} ${runCounter}: ${formatRunStatusForMessage(runStatus)}.`
    );
    const validatedRunStatus = validateRunStatus(runCounter, runStatus);
    if (isTerminalRunStatus(validatedRunStatus)) {
      await reportProgress?.(
        `${capitalize(label)} ${runCounter} reached terminal status ${validatedRunStatus}. Fetching results.`
      );
      const resultsTimeoutMs = remainingTimeMs(startedAt, timeoutMs);
      if (resultsTimeoutMs <= 0) {
        await reportProgress?.(
          `Workflow deadline reached before results could be fetched for ${label} ${runCounter}.`
        );
        return {
          status,
          results: null,
          resultsError: {
            message: "Workflow timeout reached before run results could be fetched",
            status: 408,
          },
          incomplete: true,
        };
      }

      try {
        const results = await client.get(
          `/api/v1/runs/${runCounter}/results`,
          undefined,
          { timeoutMs: resultsTimeoutMs }
        );
        await reportProgress?.(`Fetched results for ${label} ${runCounter}.`);
        return { status, results };
      } catch (error) {
        const serializedError = serializeResultsError(error);
        await reportProgress?.(
          `${capitalize(label)} ${runCounter} finished, but fetching results failed: ${serializedError.message}.`
        );
        return {
          status,
          results: null,
          resultsError: serializedError,
          incomplete: true,
        };
      }
    }
  }

  // Timeout — return last known status
  let finalStatus = lastStatus;
  const finalStatusTimeoutMs = remainingTimeMs(startedAt, timeoutMs);
  if (finalStatusTimeoutMs > 0) {
    try {
      finalStatus = await client.get(
        "/scheduler/runStatus",
        { runCounter },
        { timeoutMs: finalStatusTimeoutMs }
      );
    } catch (error) {
      if (!(error instanceof CoalesceApiError && error.status === 408)) {
        throw error;
      }
    }
  }
  if (finalStatus && typeof finalStatus === "object") {
    validateRunStatus(runCounter, (finalStatus as Record<string, unknown>).runStatus);
  }
  await reportProgress?.(
    `Timed out waiting for ${label} ${runCounter}. Returning the last known status.`
  );
  return { status: finalStatus, results: null, timedOut: true };
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}
