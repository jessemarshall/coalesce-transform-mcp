export interface ClientConfig {
  accessToken: string;
  baseUrl: string;
  requestTimeoutMs?: number;
}

export interface RequestOptions {
  timeoutMs?: number;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_REQUEST_BODY_BYTES = 512 * 1024; // 512 KB

function getMaxRequestBodyBytes(): number {
  const raw = process.env.COALESCE_MCP_MAX_REQUEST_BODY_BYTES;
  if (!raw) return DEFAULT_MAX_REQUEST_BODY_BYTES;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 1) return DEFAULT_MAX_REQUEST_BODY_BYTES;
  return parsed;
}

const MAX_RETRY_ATTEMPTS = 5;
const RETRY_BASE_DELAY_MS = 1_000;
const RETRY_MAX_DELAY_MS = 30_000;

export function validateConfig(): ClientConfig {
  const accessToken = process.env.COALESCE_ACCESS_TOKEN;
  const baseUrl = process.env.COALESCE_BASE_URL;

  if (!accessToken) {
    throw new Error(
      "COALESCE_ACCESS_TOKEN environment variable is required. " +
        "Generate a token from the Deploy tab in Coalesce."
    );
  }
  if (!baseUrl) {
    throw new Error(
      "COALESCE_BASE_URL environment variable is required. " +
        "Example: https://app.coalescesoftware.io"
    );
  }

  return {
    accessToken,
    baseUrl: baseUrl.replace(/\/+$/, ""),
  };
}

export class CoalesceApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public detail?: unknown
  ) {
    super(message);
    this.name = "CoalesceApiError";
  }
}

function parseRetryAfterMs(header: string | null): number | undefined {
  if (!header) return undefined;
  const seconds = Number(header);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.ceil(seconds * 1000);
  }
  // Retry-After can also be an HTTP-date; fall back to undefined
  return undefined;
}

function retryDelayMs(attempt: number, retryAfterMs?: number): number {
  if (retryAfterMs !== undefined && retryAfterMs > 0) {
    return Math.min(retryAfterMs, RETRY_MAX_DELAY_MS);
  }
  // Exponential backoff: 1s, 2s, 4s, 8s, ...
  return Math.min(RETRY_BASE_DELAY_MS * Math.pow(2, attempt), RETRY_MAX_DELAY_MS);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function handleResponse(response: Response): Promise<unknown> {
  if (response.status === 204) {
    return { message: "Operation completed successfully" };
  }

  if (!response.ok) {
    let detail: unknown;
    try {
      detail = await response.json();
    } catch {
      detail = undefined;
    }

    switch (response.status) {
      case 400:
        // Include full validation body in error message for debugging
        const message =
          detail && typeof detail === "object" && "message" in detail
            ? String((detail as Record<string, unknown>).message)
            : "Bad request";
        const fullMessage =
          detail && typeof detail === "object"
            ? `${message}\n\nValidation details: ${JSON.stringify(detail, null, 2)}`
            : message;
        throw new CoalesceApiError(fullMessage, 400, detail);
      case 401:
        throw new CoalesceApiError(
          "Invalid or expired access token",
          401,
          detail
        );
      case 403:
        throw new CoalesceApiError(
          "Insufficient permissions for this operation",
          403,
          detail
        );
      case 404:
        throw new CoalesceApiError("Resource not found", 404, detail);
      case 429: {
        const retryAfterHeader = response.headers.get("Retry-After");
        const retryAfterMs = parseRetryAfterMs(retryAfterHeader);
        throw new CoalesceApiError(
          "Coalesce API rate limit exceeded",
          429,
          { ...(retryAfterMs !== undefined ? { retryAfterMs } : {}), ...( detail && typeof detail === "object" ? detail : {}) }
        );
      }
      default:
        throw new CoalesceApiError(
          `Coalesce API unavailable (HTTP ${response.status})`,
          response.status,
          detail
        );
    }
  }

  return response.json();
}

export interface QueryParams {
  [key: string]: string | number | boolean | undefined;
}

export function createClient(config: ClientConfig) {
  const defaultRequestTimeoutMs = Math.max(
    1,
    Math.floor(config.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS)
  );

  function buildUrl(path: string, params?: QueryParams): string {
    const url = new URL(path, config.baseUrl);
    if (params) {
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined) {
          url.searchParams.set(key, String(value));
        }
      }
    }
    return url.toString();
  }

  function headers(method: string): Record<string, string> {
    const h: Record<string, string> = {
      Authorization: `Bearer ${config.accessToken}`,
      Accept: "application/json",
    };
    if (method === "POST" || method === "PUT" || method === "PATCH") {
      h["Content-Type"] = "application/json";
    }
    return h;
  }

  function effectiveTimeoutMs(options?: RequestOptions): number {
    if (options?.timeoutMs === undefined) {
      return defaultRequestTimeoutMs;
    }
    return Math.max(
      1,
      Math.min(defaultRequestTimeoutMs, Math.floor(options.timeoutMs))
    );
  }

  async function requestOnce(
    method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE",
    path: string,
    params?: QueryParams,
    body?: unknown,
    options?: RequestOptions
  ): Promise<unknown> {
    const timeoutMs = effectiveTimeoutMs(options);
    const serializedBody = body !== undefined ? JSON.stringify(body) : undefined;

    if (serializedBody !== undefined) {
      const maxBytes = getMaxRequestBodyBytes();
      const bodyBytes = Buffer.byteLength(serializedBody, "utf8");
      if (bodyBytes > maxBytes) {
        const sizeMB = (bodyBytes / (1024 * 1024)).toFixed(2);
        const limitKB = Math.round(maxBytes / 1024);
        throw new CoalesceApiError(
          `Request body exceeds ${limitKB} KB limit (got ${sizeMB} MB). ` +
          `This usually means a large cached response was accidentally passed as tool input. ` +
          `Override with COALESCE_MCP_MAX_REQUEST_BODY_BYTES if this payload is intentional.`,
          413,
          { bodyBytes, maxBytes, method, path }
        );
      }
    }

    const controller = new AbortController();
    const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs);
    timeoutHandle.unref?.();

    try {
      const response = await fetch(buildUrl(path, params), {
        method,
        headers: headers(method),
        body: serializedBody,
        signal: controller.signal,
      });
      return await handleResponse(response);
    } catch (error) {
      if (error instanceof CoalesceApiError) {
        throw error;
      }
      if (error instanceof Error && error.name === "AbortError") {
        throw new CoalesceApiError(
          `Coalesce API request timed out after ${timeoutMs}ms`,
          408,
          { method, path }
        );
      }
      throw new CoalesceApiError(
        "Unable to reach Coalesce API",
        503,
        error instanceof Error ? { message: error.message } : error
      );
    } finally {
      clearTimeout(timeoutHandle);
    }
  }

  async function request(
    method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE",
    path: string,
    params?: QueryParams,
    body?: unknown,
    options?: RequestOptions
  ): Promise<unknown> {
    for (let attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
      try {
        return await requestOnce(method, path, params, body, options);
      } catch (error) {
        if (
          error instanceof CoalesceApiError &&
          error.status === 429 &&
          attempt < MAX_RETRY_ATTEMPTS - 1
        ) {
          const detail = error.detail as Record<string, unknown> | undefined;
          const retryAfterMs = typeof detail?.retryAfterMs === "number"
            ? detail.retryAfterMs
            : undefined;
          await sleep(retryDelayMs(attempt, retryAfterMs));
          continue;
        }
        throw error;
      }
    }
    // Unreachable, but satisfies TypeScript
    throw new CoalesceApiError("Unexpected retry loop exit", 500);
  }

  return {
    async get(
      path: string,
      params?: QueryParams,
      options?: RequestOptions
    ): Promise<unknown> {
      return request("GET", path, params, undefined, options);
    },

    async post(
      path: string,
      body?: unknown,
      params?: QueryParams,
      options?: RequestOptions
    ): Promise<unknown> {
      return request("POST", path, params, body, options);
    },

    async put(
      path: string,
      body?: unknown,
      options?: RequestOptions
    ): Promise<unknown> {
      return request("PUT", path, undefined, body, options);
    },

    async patch(
      path: string,
      params?: QueryParams,
      body?: unknown,
      options?: RequestOptions
    ): Promise<unknown> {
      return request("PATCH", path, params, body, options);
    },

    async delete(
      path: string,
      params?: QueryParams,
      options?: RequestOptions
    ): Promise<unknown> {
      return request("DELETE", path, params, undefined, options);
    },
  };
}

export type CoalesceClient = ReturnType<typeof createClient>;
