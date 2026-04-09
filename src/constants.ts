/**
 * Shared constants used across multiple modules.
 * Centralised here to avoid duplication and drift.
 */

// ── Pagination ────────────────────────────────────────────────────────────────

export const DEFAULT_PAGE_SIZE = 250;

// ── Timeouts ──────────────────────────────────────────────────────────────────

/** Per-page timeout for detail=true API fetches (2 minutes). */
export const DETAIL_FETCH_TIMEOUT_MS = 120_000;

/** One day in milliseconds (24 hours). */
export const MS_PER_DAY = 24 * 60 * 60 * 1000;

/** Duration after which a workshop session is considered stale (24 hours). */
export const STALE_SESSION_MS = MS_PER_DAY;

// ── Run status values ────────────────────────────────────────────────────────

/**
 * Re-exported from run-status.ts — single source of truth for all run status values.
 * DOCUMENTED_RUN_STATUSES is the canonical list; RUN_STATUS_VALUES is the alias
 * used by Zod schemas and type definitions outside of the workflow module.
 */
export { DOCUMENTED_RUN_STATUSES as RUN_STATUS_VALUES } from "./workflows/run-status.js";
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
