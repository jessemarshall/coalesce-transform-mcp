/**
 * Per-process TTL cache with in-flight request coalescing and generation-guarded
 * invalidation. Shared by workspace-inventory and workspace-node-index.
 *
 * - `loadWithCache` coalesces concurrent callers into a single loader.
 * - A per-key generation counter ensures an `invalidate()` during an in-flight
 *   fetch is not silently overwritten when the stale fetch resolves.
 * - A `ttl` of 0 disables writes (useful for tests).
 */

export type TtlCache<K, V> = {
  /** Read a still-valid entry without loading. Returns undefined on miss or expiry. */
  get(key: K): V | undefined;
  /** Explicitly cache a value (skipped when resolved TTL is 0). */
  set(key: K, value: V): void;
  /** Drop any cached value, cancel in-flight bookkeeping, bump the generation. */
  invalidate(key: K): void;
  /** Drop all state. */
  clear(): void;
  /**
   * Return a cached value if present; otherwise call `loader`, cache its
   * result (unless invalidated mid-flight), and return it.
   */
  loadWithCache(key: K, loader: () => Promise<V>): Promise<V>;
};

type CacheRecord<V> = { value: V; expiresAt: number };

export function createTtlCache<K, V>(resolveTtlMs: () => number): TtlCache<K, V> {
  const cache = new Map<K, CacheRecord<V>>();
  const inflight = new Map<K, Promise<V>>();
  const generations = new Map<K, number>();

  const currentGeneration = (key: K): number => generations.get(key) ?? 0;

  const get = (key: K): V | undefined => {
    const hit = cache.get(key);
    if (!hit) return undefined;
    if (hit.expiresAt <= Date.now()) {
      cache.delete(key);
      return undefined;
    }
    return hit.value;
  };

  const set = (key: K, value: V): void => {
    const ttl = resolveTtlMs();
    if (ttl <= 0) return;
    cache.set(key, { value, expiresAt: Date.now() + ttl });
  };

  const invalidate = (key: K): void => {
    cache.delete(key);
    inflight.delete(key);
    generations.set(key, currentGeneration(key) + 1);
  };

  const clear = (): void => {
    cache.clear();
    inflight.clear();
    generations.clear();
  };

  const loadWithCache = async (key: K, loader: () => Promise<V>): Promise<V> => {
    const cached = get(key);
    if (cached !== undefined) return cached;

    const existing = inflight.get(key);
    if (existing) return existing;

    const startGeneration = currentGeneration(key);
    let promise!: Promise<V>;
    promise = (async () => {
      try {
        const value = await loader();
        // Skip the write if the key was invalidated mid-flight — the fetched
        // value may no longer reflect current state.
        if (currentGeneration(key) === startGeneration) set(key, value);
        return value;
      } finally {
        // Only clear the inflight entry if it's still ours — an invalidate()
        // plus a new call could have replaced it while we awaited.
        if (inflight.get(key) === promise) inflight.delete(key);
      }
    })();

    inflight.set(key, promise);
    return promise;
  };

  return { get, set, invalidate, clear, loadWithCache };
}

/**
 * Parse a raw env-var value as non-negative milliseconds. Falls back to
 * `defaultMs` when the value is unset, empty, non-numeric, or negative.
 *
 * Callers pass the raw env-var value (not its name) so the shared
 * env-metadata scanner can statically find every variable we read.
 */
export function parseTtlMs(raw: string | undefined, defaultMs: number): number {
  if (!raw) return defaultMs;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return defaultMs;
  return parsed;
}
