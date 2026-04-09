import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { validateConfig, createClient } from "../src/client.js";
import { validatePathSegment } from "../src/coalesce/types.js";

describe("CoalesceClient", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
    process.env.COALESCE_ACCESS_TOKEN = "test-token";
    process.env.COALESCE_BASE_URL = "https://app.coalescesoftware.io";
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("validateConfig", () => {
    it("throws if COALESCE_ACCESS_TOKEN is missing", () => {
      delete process.env.COALESCE_ACCESS_TOKEN;
      expect(() => validateConfig()).toThrow("COALESCE_ACCESS_TOKEN");
    });

    it("defaults COALESCE_BASE_URL to US region when missing", () => {
      delete process.env.COALESCE_BASE_URL;
      const config = validateConfig();
      expect(config.baseUrl).toBe("https://app.coalescesoftware.io");
    });

    it("returns config when both vars are set", () => {
      const config = validateConfig();
      expect(config.accessToken).toBe("test-token");
      expect(config.baseUrl).toBe("https://app.coalescesoftware.io");
    });

    it("strips trailing slash from base URL", () => {
      process.env.COALESCE_BASE_URL = "https://app.coalescesoftware.io/";
      const config = validateConfig();
      expect(config.baseUrl).toBe("https://app.coalescesoftware.io");
    });
  });

  describe("validatePathSegment", () => {
    it("rejects query and fragment delimiters", () => {
      expect(() => validatePathSegment("abc?x=1", "runID")).toThrow("URI delimiters");
      expect(() => validatePathSegment("abc#frag", "runID")).toThrow("URI delimiters");
    });

    it("rejects percent signs used for encoded path tricks", () => {
      expect(() => validatePathSegment("abc%2Fdef", "runID")).toThrow("URI delimiters");
    });
  });

  describe("request", () => {
    it("includes auth and accept headers on GET", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ data: [] }),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      await client.get("/api/v1/environments");

      expect(mockFetch).toHaveBeenCalledWith(
        "https://app.coalescesoftware.io/api/v1/environments",
        expect.objectContaining({
          method: "GET",
          headers: expect.objectContaining({
            Authorization: "Bearer test-token",
            Accept: "application/json",
          }),
        })
      );
    });

    it("includes Content-Type on POST", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ data: {} }),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      await client.post("/scheduler/startRun", { environmentID: "123" });

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            "Content-Type": "application/json",
          }),
        })
      );
    });

    it("maps 401 to auth error message", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 401,
        json: () => Promise.resolve({}),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "bad-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      await expect(client.get("/api/v1/environments")).rejects.toThrow(
        "Invalid or expired access token"
      );
    });

    it("maps 403 to permissions error", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 403,
        json: () => Promise.resolve({}),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      await expect(client.get("/api/v1/projects")).rejects.toThrow(
        "Insufficient permissions"
      );
    });

    it("maps 404 to not found error", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 404,
        json: () => Promise.resolve({}),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      await expect(client.get("/api/v1/runs/bad-id")).rejects.toThrow(
        "Resource not found"
      );
    });

    it("maps 400 with message to API error detail", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ message: "Invalid environmentID format" }),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      await expect(client.post("/scheduler/startRun", {})).rejects.toThrow(
        "Invalid environmentID format"
      );
    });

    it("maps 400 without message to generic bad request", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 400,
        json: () => Promise.resolve({ error: "something" }),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      await expect(client.post("/scheduler/startRun", {})).rejects.toThrow(
        "Bad request"
      );
    });

    it("maps 5xx to unavailable error", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 502,
        json: () => Promise.resolve({}),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      await expect(client.get("/api/v1/runs")).rejects.toThrow(
        "Coalesce API unavailable (HTTP 502)"
      );
    });

    it("rejects request body exceeding size limit", async () => {
      const mockFetch = vi.fn();
      vi.stubGlobal("fetch", mockFetch);

      // Set a very low limit for testing
      process.env.COALESCE_MCP_MAX_REQUEST_BODY_BYTES = "100";

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      const largeBody = { data: "x".repeat(200) };
      await expect(client.post("/scheduler/startRun", largeBody)).rejects.toMatchObject({
        message: expect.stringContaining("exceeds"),
        status: 413,
      });

      // Should never have called fetch
      expect(mockFetch).not.toHaveBeenCalled();
    });

    it("allows request body within size limit", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ ok: true }),
      });
      vi.stubGlobal("fetch", mockFetch);

      process.env.COALESCE_MCP_MAX_REQUEST_BODY_BYTES = "10000";

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      const smallBody = { data: "hello" };
      const result = await client.post("/scheduler/startRun", smallBody);
      expect(result).toEqual({ ok: true });
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    it("uses default 512KB limit when env var is not set", async () => {
      const mockFetch = vi.fn();
      vi.stubGlobal("fetch", mockFetch);

      delete process.env.COALESCE_MCP_MAX_REQUEST_BODY_BYTES;

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      // 100KB body should be fine under 512KB default
      const body = { data: "x".repeat(100_000) };
      const mockResponse = {
        ok: true,
        status: 200,
        json: () => Promise.resolve({ ok: true }),
      };
      mockFetch.mockResolvedValue(mockResponse);

      const result = await client.post("/api/v1/test", body);
      expect(result).toEqual({ ok: true });
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    it("includes override hint in body size error message", async () => {
      vi.stubGlobal("fetch", vi.fn());
      process.env.COALESCE_MCP_MAX_REQUEST_BODY_BYTES = "10";

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      await expect(client.post("/test", { big: "data" })).rejects.toThrow(
        "COALESCE_MCP_MAX_REQUEST_BODY_BYTES"
      );
    });

    it("retries on 429 and succeeds on subsequent attempt", async () => {
      vi.useFakeTimers();
      const mockFetch = vi.fn()
        .mockResolvedValueOnce({
          ok: false,
          status: 429,
          headers: new Headers({ "Retry-After": "1" }),
          json: () => Promise.resolve({}),
        })
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ data: "ok" }),
        });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      const promise = client.get("/api/v1/environments");

      // Advance past the Retry-After delay
      await vi.advanceTimersByTimeAsync(1_000);
      const result = await promise;

      expect(result).toEqual({ data: "ok" });
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    it("throws after exhausting all 5 retry attempts on 429", async () => {
      vi.useFakeTimers();
      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 429,
        headers: new Headers(),
        json: () => Promise.resolve({}),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      const promise = expect(client.get("/api/v1/environments")).rejects.toMatchObject({
        message: "Coalesce API rate limit exceeded",
        status: 429,
      });

      // Advance through all 4 retry delays: 1s + 2s + 4s + 8s = 15s
      await vi.advanceTimersByTimeAsync(15_000);
      await promise;

      expect(mockFetch).toHaveBeenCalledTimes(5);
    });

    it("uses exponential backoff when no Retry-After header", async () => {
      vi.useFakeTimers();
      const mockFetch = vi.fn()
        .mockResolvedValueOnce({
          ok: false,
          status: 429,
          headers: new Headers(),
          json: () => Promise.resolve({}),
        })
        .mockResolvedValueOnce({
          ok: false,
          status: 429,
          headers: new Headers(),
          json: () => Promise.resolve({}),
        })
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ data: "ok" }),
        });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      const promise = client.get("/api/v1/environments");

      // First retry: 1s backoff (attempt 0)
      await vi.advanceTimersByTimeAsync(1_000);
      expect(mockFetch).toHaveBeenCalledTimes(2);

      // Second retry: 2s backoff (attempt 1)
      await vi.advanceTimersByTimeAsync(2_000);
      expect(mockFetch).toHaveBeenCalledTimes(3);

      const result = await promise;
      expect(result).toEqual({ data: "ok" });
    });

    it("respects Retry-After header over exponential backoff", async () => {
      vi.useFakeTimers();
      const mockFetch = vi.fn()
        .mockResolvedValueOnce({
          ok: false,
          status: 429,
          headers: new Headers({ "Retry-After": "5" }),
          json: () => Promise.resolve({}),
        })
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ data: "ok" }),
        });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      const promise = client.get("/api/v1/environments");

      // Should not have retried yet at 1s (exponential would be 1s, but Retry-After says 5)
      await vi.advanceTimersByTimeAsync(1_000);
      expect(mockFetch).toHaveBeenCalledTimes(1);

      // Should retry after 5s
      await vi.advanceTimersByTimeAsync(4_000);
      const result = await promise;

      expect(result).toEqual({ data: "ok" });
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    it("does not retry non-429 errors", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        headers: new Headers(),
        json: () => Promise.resolve({}),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      await expect(client.get("/api/v1/environments")).rejects.toMatchObject({
        status: 500,
      });
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    it("does not retry write requests on 429", async () => {
      const verbs = [
        {
          label: "POST",
          invoke: (client: ReturnType<typeof createClient>) => client.post("/api/v1/projects", { name: "Test" }),
        },
        {
          label: "PUT",
          invoke: (client: ReturnType<typeof createClient>) => client.put("/api/v1/projects/1", { name: "Test" }),
        },
        {
          label: "PATCH",
          invoke: (client: ReturnType<typeof createClient>) => client.patch("/api/v1/projects/1", { name: "Test" }),
        },
        {
          label: "DELETE",
          invoke: (client: ReturnType<typeof createClient>) => client.delete("/api/v1/projects/1"),
        },
      ] as const;

      for (const { label, invoke } of verbs) {
        const mockFetch = vi.fn()
          .mockResolvedValueOnce({
            ok: false,
            status: 429,
            headers: new Headers({ "Retry-After": "1" }),
            json: () => Promise.resolve({}),
          })
          .mockResolvedValueOnce({
            ok: true,
            status: 200,
            json: () => Promise.resolve({ success: true }),
          });
        vi.stubGlobal("fetch", mockFetch);

        const client = createClient({
          accessToken: "test-token",
          baseUrl: "https://app.coalescesoftware.io",
        });

        await expect(invoke(client)).rejects.toMatchObject({
          message: "Coalesce API rate limit exceeded",
          status: 429,
        });
        expect(mockFetch, `${label} should not be retried on 429`).toHaveBeenCalledTimes(1);
      }
    });


    it("handles 204 no-content response", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 204,
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      const result = await client.post("/scheduler/cancelRun", {
        runID: "123",
      });

      expect(result).toEqual({ message: "Operation completed successfully" });
    });

    it("appends pagination query params on GET", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ data: [], next: null }),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      await client.get("/api/v1/environments", {
        limit: 10,
        orderBy: "name",
        orderByDirection: "asc",
      });

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain("limit=10");
      expect(calledUrl).toContain("orderBy=name");
      expect(calledUrl).toContain("orderByDirection=asc");
    });

    it("appends query params on POST", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ data: {} }),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      await client.post("/api/v1/gitAccounts", { name: "test" }, { accountOwner: "user-1" });

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain("accountOwner=user-1");
    });

    it("appends query params on DELETE", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 204,
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      await client.delete("/api/v1/gitAccounts/123", { accountOwner: "user-1" });

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain("accountOwner=user-1");
    });

    it("post passes both query params and options correctly", async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ runCounter: 1 }),
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });
      await client.post("/scheduler/startRun", { env: "1" }, { key: "val" }, { timeoutMs: 5000 });

      const calledUrl = mockFetch.mock.calls[0][0];
      expect(calledUrl).toContain("key=val");
    });

    it("aborts hung requests after the configured timeout", async () => {
      vi.useFakeTimers();
      const mockFetch = vi.fn().mockImplementation((_url: string, init?: RequestInit) => {
        return new Promise((_, reject) => {
          init?.signal?.addEventListener("abort", () => {
            const error = new Error("aborted");
            error.name = "AbortError";
            reject(error);
          });
        });
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
        requestTimeoutMs: 50,
      });

      const request = expect(client.get("/api/v1/environments")).rejects.toMatchObject({
        message: "Coalesce API request timed out after 50ms",
        status: 408,
      });
      await vi.advanceTimersByTimeAsync(50);

      await request;
    });

    it("allows per-request timeouts to exceed the client default", async () => {
      vi.useFakeTimers();
      let abortCount = 0;
      const mockFetch = vi.fn().mockImplementation((_url: string, init?: RequestInit) => {
        return new Promise((_, reject) => {
          init?.signal?.addEventListener("abort", () => {
            abortCount += 1;
            const error = new Error("aborted");
            error.name = "AbortError";
            reject(error);
          });
        });
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
        requestTimeoutMs: 50,
      });

      const request = expect(
        client.get("/api/v1/environments", undefined, { timeoutMs: 200 })
      ).rejects.toMatchObject({
        message: "Coalesce API request timed out after 200ms",
        status: 408,
      });

      await vi.advanceTimersByTimeAsync(50);
      expect(abortCount).toBe(0);

      await vi.advanceTimersByTimeAsync(150);
      expect(abortCount).toBe(1);

      await request;
    });

    it("propagates cancellation when the external signal aborts", async () => {
      vi.useFakeTimers();
      const controller = new AbortController();
      const mockFetch = vi.fn().mockImplementation((_url: string, init?: RequestInit) => {
        return new Promise((_, reject) => {
          init?.signal?.addEventListener("abort", () => {
            const error = new Error("aborted");
            error.name = "AbortError";
            reject(error);
          });
        });
      });
      vi.stubGlobal("fetch", mockFetch);

      const client = createClient({
        accessToken: "test-token",
        baseUrl: "https://app.coalescesoftware.io",
      });

      const request = expect(
        client.get("/api/v1/environments", undefined, { signal: controller.signal })
      ).rejects.toMatchObject({
        message: "Request was cancelled",
        name: "AbortError",
      });

      controller.abort();
      await vi.runAllTimersAsync();
      await request;
    });
  });
});
