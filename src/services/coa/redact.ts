/**
 * Redact token-like fields from `coa` command output before we surface it to
 * an MCP agent. `coa doctor --json` echoes a truncated access token under
 * `data.cloud.checks[].detail` (where `name === "token"`); even a prefix is
 * sensitive enough to keep out of model context, transcripts, and cache files.
 *
 * We walk parsed JSON and redact:
 *   1. Any object's `detail` field when a sibling `name` is token-ish.
 *   2. Any value whose key name is token-ish (defense in depth against schema
 *      drift — if a future coa version nests a real token elsewhere, the same
 *      redaction rules still apply).
 *
 * Returns the redacted clone plus a flag indicating whether any redaction
 * actually happened. Input is not mutated.
 */

/**
 * Normalize a key name for comparison: lowercase + strip `_` / `-`. Collapses
 * `api_key`, `api-key`, `apiKey`, `APIKey` all to `apikey`.
 */
function normalizeKey(key: string): string {
  return key.toLowerCase().replace(/[_-]/g, "");
}

/** Full key matches — exact tokens agents should never see. */
const SENSITIVE_KEY_EXACT = new Set([
  "token",
  "accesstoken",
  "refreshtoken",
  "password",
  "passphrase",
  "secret",
  "apikey",
  "pat",
]);

/**
 * Sibling `name` values that mark this object's `detail` as sensitive.
 * Matches the shape of `coa doctor --json` cloud checks.
 */
const SENSITIVE_SIBLING_NAMES = new Set([
  "token",
  "accesstoken",
  "refreshtoken",
  "password",
  "passphrase",
  "secret",
  "apikey",
  "pat",
]);

export const REDACTED_PLACEHOLDER = "<redacted>";

export type RedactResult<T> = { value: T; didRedact: boolean };

export function redactSensitive<T>(input: T): RedactResult<T> {
  let didRedact = false;

  const walk = (value: unknown): unknown => {
    if (Array.isArray(value)) {
      return value.map((item) => walk(item));
    }
    if (value && typeof value === "object") {
      const obj = value as Record<string, unknown>;
      const siblingName =
        typeof obj.name === "string" ? normalizeKey(obj.name) : null;
      const hasSensitiveSibling =
        siblingName !== null && SENSITIVE_SIBLING_NAMES.has(siblingName);
      const out: Record<string, unknown> = {};
      for (const [key, v] of Object.entries(obj)) {
        if (
          SENSITIVE_KEY_EXACT.has(normalizeKey(key)) &&
          typeof v === "string" &&
          v.length > 0
        ) {
          out[key] = REDACTED_PLACEHOLDER;
          didRedact = true;
          continue;
        }
        if (
          hasSensitiveSibling &&
          key === "detail" &&
          typeof v === "string" &&
          v.length > 0
        ) {
          out[key] = REDACTED_PLACEHOLDER;
          didRedact = true;
          continue;
        }
        out[key] = walk(v);
      }
      return out;
    }
    return value;
  };

  return { value: walk(input) as T, didRedact };
}
