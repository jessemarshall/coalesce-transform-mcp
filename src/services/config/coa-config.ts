import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface CoaProfile {
  profileName: string;
  token?: string;
  domain?: string;
  snowflakeUsername?: string;
  snowflakeRole?: string;
  snowflakeWarehouse?: string;
  snowflakeKeyPairKey?: string;
  snowflakeKeyPairPass?: string;
  snowflakeAuthType?: string;
  environmentID?: string;
  extras: Record<string, string>;
}

export type CoaConfigStatus =
  | { kind: "ok"; path: string; profiles: string[] }
  | { kind: "missing-file"; path: string }
  | { kind: "parse-error"; path: string; message: string };

interface ParsedConfig {
  status: CoaConfigStatus;
  sections: Map<string, Map<string, string>>;
}

const DEFAULT_PROFILE_NAME = "default";

let cached: ParsedConfig | null = null;
const warnedMissingProfiles = new Set<string>();

function coaConfigPath(): string {
  return join(homedir(), ".coa", "config");
}

function unquote(raw: string): string {
  const trimmed = raw.trim();
  if (trimmed.length >= 2) {
    const first = trimmed[0];
    const last = trimmed[trimmed.length - 1];
    if ((first === '"' && last === '"') || (first === "'" && last === "'")) {
      return trimmed.slice(1, -1);
    }
  }
  return trimmed;
}

function parseIni(contents: string): Map<string, Map<string, string>> {
  // Strip UTF-8 BOM: editors occasionally write it and the first [section] header
  // would otherwise fail to match, silently dropping every key in that section.
  if (contents.charCodeAt(0) === 0xfeff) contents = contents.slice(1);

  const sections = new Map<string, Map<string, string>>();
  let current: Map<string, string> | null = null;

  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#") || line.startsWith(";")) continue;

    if (line.startsWith("[") && line.endsWith("]")) {
      const name = line.slice(1, -1).trim();
      current = new Map();
      sections.set(name, current);
      continue;
    }

    const eq = line.indexOf("=");
    if (eq === -1 || !current) continue;

    const key = line.slice(0, eq).trim();
    const value = unquote(line.slice(eq + 1));
    if (key) current.set(key, value);
  }

  return sections;
}

function loadParsed(): ParsedConfig {
  if (cached) return cached;
  const path = coaConfigPath();
  if (!existsSync(path)) {
    cached = { status: { kind: "missing-file", path }, sections: new Map() };
    return cached;
  }
  try {
    const contents = readFileSync(path, "utf8");
    const sections = parseIni(contents);
    cached = {
      status: { kind: "ok", path, profiles: Array.from(sections.keys()) },
      sections,
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    cached = {
      status: { kind: "parse-error", path, message },
      sections: new Map(),
    };
  }
  return cached;
}

function toProfile(profileName: string, section: Map<string, string>): CoaProfile {
  const take = (key: string): string | undefined => {
    const v = section.get(key);
    return v && v.length > 0 ? v : undefined;
  };
  const consumed = new Set([
    "token",
    "domain",
    "snowflakeUsername",
    "snowflakeRole",
    "snowflakeWarehouse",
    "snowflakeKeyPairKey",
    "snowflakeKeyPairPass",
    "snowflakeAuthType",
    "environmentID",
  ]);
  const extras: Record<string, string> = {};
  for (const [key, value] of section) {
    if (!consumed.has(key)) extras[key] = value;
  }
  return {
    profileName,
    token: take("token"),
    domain: take("domain"),
    snowflakeUsername: take("snowflakeUsername"),
    snowflakeRole: take("snowflakeRole"),
    snowflakeWarehouse: take("snowflakeWarehouse"),
    snowflakeKeyPairKey: take("snowflakeKeyPairKey"),
    snowflakeKeyPairPass: take("snowflakeKeyPairPass"),
    snowflakeAuthType: take("snowflakeAuthType"),
    environmentID: take("environmentID"),
    extras,
  };
}

export function getActiveProfileName(): string {
  const raw = process.env.COALESCE_PROFILE?.trim();
  return raw && raw.length > 0 ? raw : DEFAULT_PROFILE_NAME;
}

export function getCoaConfigStatus(): CoaConfigStatus {
  return loadParsed().status;
}

export function loadCoaProfile(profileName?: string): CoaProfile | null {
  const name = profileName ?? getActiveProfileName();
  const parsed = loadParsed();
  const section = parsed.sections.get(name);
  if (!section) {
    if (
      parsed.status.kind === "ok" &&
      process.env.COALESCE_PROFILE &&
      !warnedMissingProfiles.has(name)
    ) {
      warnedMissingProfiles.add(name);
      const available = parsed.status.profiles.join(", ") || "(none)";
      process.stderr.write(
        `[coalesce-mcp] profile "${name}" not found in ~/.coa/config (available: ${available}) — falling back to env vars\n`
      );
    }
    return null;
  }
  return toProfile(name, section);
}

export function __resetForTests(): void {
  cached = null;
  warnedMissingProfiles.clear();
}

/**
 * Source tag for a resolved credential field. Used by the resolver (to enforce
 * env-wins precedence) and by diagnose_setup (to tell the user where each
 * resolved value came from).
 */
export type FieldSource = "env" | `profile:${string}` | "default";

export interface PickedField {
  value: string | undefined;
  source: FieldSource | undefined;
}

/**
 * Env wins. Profile fills in when env is empty/unset. Returns undefined value +
 * undefined source when neither is present.
 *
 * Whitespace-only values are treated as unset — `COALESCE_ACCESS_TOKEN="   "` is
 * equivalent to not setting it at all. The returned `value` is always trimmed.
 */
export function pickField(
  envValue: string | undefined,
  profileValue: string | undefined,
  profileName: string
): PickedField {
  const env = envValue?.trim();
  if (env && env.length > 0) return { value: env, source: "env" };
  const prof = profileValue?.trim();
  if (prof && prof.length > 0) return { value: prof, source: `profile:${profileName}` };
  return { value: undefined, source: undefined };
}
