/**
 * Shared constants used across multiple modules.
 * Centralised here to avoid duplication and drift.
 */

// ── Pagination ────────────────────────────────────────────────────────────────

export const DEFAULT_PAGE_SIZE = 250;

// ── Timeouts ──────────────────────────────────────────────────────────────────

const DEFAULT_DETAIL_FETCH_TIMEOUT_MS = 180_000;

/** Per-page timeout (ms) for detail=true API fetches. Evaluated per call so
 *  tests (and at-runtime env changes) can override. Override via
 *  COALESCE_MCP_DETAIL_FETCH_TIMEOUT_MS. */
export function getDetailFetchTimeoutMs(): number {
  const raw = process.env.COALESCE_MCP_DETAIL_FETCH_TIMEOUT_MS;
  if (!raw) return DEFAULT_DETAIL_FETCH_TIMEOUT_MS;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed < 1) return DEFAULT_DETAIL_FETCH_TIMEOUT_MS;
  return parsed;
}

/** One day in milliseconds (24 hours). */
export const MS_PER_DAY = 24 * 60 * 60 * 1000;

/** Duration after which a workshop session is considered stale (24 hours). */
export const STALE_SESSION_MS = MS_PER_DAY;

// ── Run status values ────────────────────────────────────────────────────────

export { DOCUMENTED_RUN_STATUSES } from "./workflows/run-status.js";
export type { KnownRunStatus as RunStatus } from "./workflows/run-status.js";

// ── Workflow poll / timeout boundaries ────────────────────────────────────────

export const POLL_INTERVAL_MIN_S = 5;
export const POLL_INTERVAL_DEFAULT_S = 10;
export const POLL_INTERVAL_MAX_S = 300;

export const WORKFLOW_TIMEOUT_MIN_S = 30;
export const WORKFLOW_TIMEOUT_DEFAULT_S = 1800;
export const WORKFLOW_TIMEOUT_MAX_S = 3600;

/**
 * Clamp a numeric value to [min, max] and return both the clamped value
 * and an optional warning string when the value was adjusted.
 */
export function clampWithWarning(
  value: number,
  min: number,
  max: number,
  label: string
): { value: number; warning?: string } {
  if (value < min) {
    return {
      value: min,
      warning: `${label} ${value} is below the minimum (${min}); using ${min} instead.`,
    };
  }
  if (value > max) {
    return {
      value: max,
      warning: `${label} ${value} exceeds the maximum (${max}); using ${max} instead.`,
    };
  }
  return { value };
}
