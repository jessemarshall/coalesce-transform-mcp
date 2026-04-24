import { describe, it, expect, beforeEach, vi } from "vitest";
import {
  createTtlCache,
  parseTtlMs,
} from "../../src/services/cache/ttl-cache.js";

describe("createTtlCache", () => {
  let cache: ReturnType<typeof createTtlCache<string, { v: number }>>;
  let ttl = 60_000;

  beforeEach(() => {
    ttl = 60_000;
    cache = createTtlCache(() => ttl);
  });

  it("stores and returns values within TTL", () => {
    cache.set("a", { v: 1 });
    expect(cache.get("a")).toEqual({ v: 1 });
  });

  it("returns undefined on cache miss", () => {
    expect(cache.get("missing")).toBeUndefined();
  });

  it("expires entries past the TTL", () => {
    vi.useFakeTimers();
    try {
      cache.set("a", { v: 1 });
      vi.advanceTimersByTime(ttl + 1);
      expect(cache.get("a")).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not cache writes when resolved TTL is 0", () => {
    ttl = 0;
    cache.set("a", { v: 1 });
    expect(cache.get("a")).toBeUndefined();
  });

  it("loadWithCache loads on miss and caches the result", async () => {
    const loader = vi.fn(async () => ({ v: 42 }));
    const first = await cache.loadWithCache("k", loader);
    const second = await cache.loadWithCache("k", loader);

    expect(first).toEqual({ v: 42 });
    expect(second).toEqual({ v: 42 });
    expect(loader).toHaveBeenCalledTimes(1);
  });

  it("coalesces concurrent loaders into a single call", async () => {
    let release!: (value: { v: number }) => void;
    const pending = new Promise<{ v: number }>((resolve) => {
      release = resolve;
    });
    const loader = vi.fn(() => pending);

    const waitA = cache.loadWithCache("k", loader);
    const waitB = cache.loadWithCache("k", loader);
    const waitC = cache.loadWithCache("k", loader);

    release({ v: 1 });
    const results = await Promise.all([waitA, waitB, waitC]);
    expect(results).toEqual([{ v: 1 }, { v: 1 }, { v: 1 }]);
    expect(loader).toHaveBeenCalledTimes(1);
  });

  it("invalidate() forces the next load to re-fetch", async () => {
    const loader = vi
      .fn()
      .mockResolvedValueOnce({ v: 1 })
      .mockResolvedValueOnce({ v: 2 });

    expect(await cache.loadWithCache("k", loader)).toEqual({ v: 1 });
    cache.invalidate("k");
    expect(await cache.loadWithCache("k", loader)).toEqual({ v: 2 });
    expect(loader).toHaveBeenCalledTimes(2);
  });

  it("does not cache a stale fetch when invalidated mid-flight", async () => {
    let release!: (value: { v: number }) => void;
    const loader = vi.fn(
      () =>
        new Promise<{ v: number }>((resolve) => {
          release = resolve;
        })
    );

    const pending = cache.loadWithCache("k", loader);
    cache.invalidate("k"); // invalidate while loader is still pending
    release({ v: 99 });
    await pending;

    expect(cache.get("k")).toBeUndefined();
  });

  it("clear() drops everything", async () => {
    cache.set("a", { v: 1 });
    cache.set("b", { v: 2 });
    cache.clear();
    expect(cache.get("a")).toBeUndefined();
    expect(cache.get("b")).toBeUndefined();
  });

  it("keys are independent", async () => {
    const loader = vi
      .fn()
      .mockResolvedValueOnce({ v: 1 })
      .mockResolvedValueOnce({ v: 2 });

    const a = await cache.loadWithCache("a", loader);
    const b = await cache.loadWithCache("b", loader);
    expect(a).toEqual({ v: 1 });
    expect(b).toEqual({ v: 2 });
    expect(loader).toHaveBeenCalledTimes(2);
  });

  it("propagates loader errors and does not cache the failure", async () => {
    const loader = vi
      .fn()
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValueOnce({ v: 5 });

    await expect(cache.loadWithCache("k", loader)).rejects.toThrow("boom");
    expect(await cache.loadWithCache("k", loader)).toEqual({ v: 5 });
    expect(loader).toHaveBeenCalledTimes(2);
  });
});

describe("parseTtlMs", () => {
  it("returns the default when the value is undefined", () => {
    expect(parseTtlMs(undefined, 1234)).toBe(1234);
  });

  it("returns the default when the value is empty", () => {
    expect(parseTtlMs("", 1234)).toBe(1234);
  });

  it("returns the default when the value is non-numeric", () => {
    expect(parseTtlMs("abc", 1234)).toBe(1234);
  });

  it("returns the default when the value is negative", () => {
    expect(parseTtlMs("-1", 1234)).toBe(1234);
  });

  it("parses a valid non-negative integer", () => {
    expect(parseTtlMs("500", 1234)).toBe(500);
  });

  it("treats 0 as a literal 0 (disables caching)", () => {
    expect(parseTtlMs("0", 1234)).toBe(0);
  });
});
