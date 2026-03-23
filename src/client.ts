export interface ClientConfig {
  accessToken: string;
  baseUrl: string;
  requestTimeoutMs?: number;
}

export interface RequestOptions {
  timeoutMs?: number;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

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

  async function request(
    method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE",
    path: string,
    params?: QueryParams,
    body?: unknown,
    options?: RequestOptions
  ): Promise<unknown> {
    const timeoutMs = effectiveTimeoutMs(options);
    const controller = new AbortController();
    const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs);
    timeoutHandle.unref?.();

    try {
      const response = await fetch(buildUrl(path, params), {
        method,
        headers: headers(method),
        body: body !== undefined ? JSON.stringify(body) : undefined,
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
