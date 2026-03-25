import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { validateConfig, createClient } from "../src/client.js";

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

    it("throws if COALESCE_BASE_URL is missing", () => {
      delete process.env.COALESCE_BASE_URL;
      expect(() => validateConfig()).toThrow("COALESCE_BASE_URL");
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
  });
});
