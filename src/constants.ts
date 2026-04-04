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

// ── Workflow poll / timeout boundaries ────────────────────────────────────────

export const POLL_INTERVAL_MIN_S = 5;
export const POLL_INTERVAL_DEFAULT_S = 10;
export const POLL_INTERVAL_MAX_S = 300;

export const WORKFLOW_TIMEOUT_MIN_S = 30;
export const WORKFLOW_TIMEOUT_DEFAULT_S = 1800;
export const WORKFLOW_TIMEOUT_MAX_S = 3600;
