import { CoalesceApiError } from "../client.js";
import { safeErrorMessage } from "../utils.js";

export function remainingTimeMs(startedAt: number, totalTimeoutMs: number): number {
  return Math.max(0, totalTimeoutMs - (Date.now() - startedAt));
}

export function serializeResultsError(error: unknown): { message: string; status?: number; detail?: unknown } {
  if (error instanceof CoalesceApiError) {
    return {
      message: error.message,
      status: error.status,
      ...(error.detail !== undefined ? { detail: error.detail } : {}),
    };
  }
  if (error instanceof Error) {
    return { message: error.message };
  }
  return { message: "Unable to fetch run results", detail: error };
}

export type WorkflowProgressNotification = {
  method: "notifications/progress";
  params: {
    progressToken: string | number;
    progress: number;
    total?: number;
    message?: string;
  };
};

export type WorkflowProgressExtra = {
  signal?: AbortSignal;
  _meta?: {
    progressToken?: string | number;
  };
  sendNotification?: (
    notification: WorkflowProgressNotification
  ) => Promise<void>;
};

export type WorkflowProgressReporter = (
  message: string,
  total?: number
) => Promise<void>;

function createAbortError(): Error {
  const error = new Error("Request was cancelled");
  error.name = "AbortError";
  return error;
}

export function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw createAbortError();
  }
}

export async function sleepWithAbort(
  ms: number,
  signal?: AbortSignal
): Promise<void> {
  throwIfAborted(signal);

  if (!signal) {
    await new Promise((resolve) => setTimeout(resolve, ms));
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const timeoutHandle = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timeoutHandle);
      signal.removeEventListener("abort", onAbort);
      reject(createAbortError());
    };

    signal.addEventListener("abort", onAbort, { once: true });
  });
}

export function createWorkflowProgressReporter(
  extra?: WorkflowProgressExtra
): WorkflowProgressReporter | undefined {
  const progressToken = extra?._meta?.progressToken;
  const sendNotification = extra?.sendNotification;

  if (progressToken === undefined || !sendNotification) {
    return undefined;
  }

  let progress = 0;

  return async (message: string, total?: number) => {
    progress += 1;

    try {
      await sendNotification({
        method: "notifications/progress",
        params: {
          progressToken,
          progress,
          ...(total !== undefined ? { total } : {}),
          ...(message ? { message } : {}),
        },
      });
    } catch (error) {
      // Progress is best-effort and should not fail the workflow.
      const reason = safeErrorMessage(error);
      process.stderr.write(`[progress] Notification failed (token=${progressToken}): ${reason}\n`);
    }
  };
}
