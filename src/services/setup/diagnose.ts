import { existsSync, statSync } from "node:fs";
import { isAbsolute, join, resolve } from "node:path";
import type { CoalesceClient } from "../../client.js";
import { CoalesceApiError } from "../../client.js";
import { listWorkspaces } from "../../coalesce/api/workspaces.js";
import { runCoa } from "../coa/runner.js";
import { redactSensitive } from "../coa/redact.js";
import {
  getActiveProfileName,
  getCoaConfigStatus,
  loadCoaProfile,
  pickField,
  type CoaProfile,
  type FieldSource,
} from "../config/coa-config.js";

export type { FieldSource };

// ---------- coa config ----------

export type CoaConfigStatus =
  | {
      status: "ok";
      path: string;
      activeProfile: string;
      profileExists: boolean;
      availableProfiles: string[];
      presentKeys: string[];
    }
  | { status: "missing-file"; path: string; activeProfile: string }
  | { status: "parse-error"; path: string; message: string; activeProfile: string };

export function diagnoseCoaConfig(): CoaConfigStatus {
  const activeProfile = getActiveProfileName();
  const fileStatus = getCoaConfigStatus();
  if (fileStatus.kind === "missing-file") {
    return { status: "missing-file", path: fileStatus.path, activeProfile };
  }
  if (fileStatus.kind === "parse-error") {
    return {
      status: "parse-error",
      path: fileStatus.path,
      message: fileStatus.message,
      activeProfile,
    };
  }
  const profile = loadCoaProfile(activeProfile);
  const profileExists = profile !== null;
  const presentKeys = profile ? collectPresentKeys(profile) : [];
  return {
    status: "ok",
    path: fileStatus.path,
    activeProfile,
    profileExists,
    availableProfiles: fileStatus.profiles,
    presentKeys,
  };
}

function collectPresentKeys(profile: CoaProfile): string[] {
  const keys: string[] = [];
  if (profile.token) keys.push("token");
  if (profile.domain) keys.push("domain");
  if (profile.snowflakeUsername) keys.push("snowflakeUsername");
  if (profile.snowflakeRole) keys.push("snowflakeRole");
  if (profile.snowflakeWarehouse) keys.push("snowflakeWarehouse");
  if (profile.snowflakeKeyPairKey) keys.push("snowflakeKeyPairKey");
  if (profile.snowflakeKeyPairPass) keys.push("snowflakeKeyPairPass");
  if (profile.snowflakeAuthType) keys.push("snowflakeAuthType");
  if (profile.environmentID) keys.push("environmentID");
  if (profile.orgID) keys.push("orgID");
  if (profile.repoPath) keys.push("repoPath");
  if (profile.cacheDir) keys.push("cacheDir");
  return keys;
}

// ---------- access token ----------

export type AccessTokenStatus =
  | { status: "ok"; projectCount: number; source: FieldSource }
  | { status: "missing" }
  | { status: "invalid"; httpStatus: number; message: string; source: FieldSource }
  | { status: "error"; message: string; source?: FieldSource };

/**
 * Probe the resolved Coalesce access token by listing workspaces. Non-throwing.
 * Reports which source (env or profile) provided the token so the setup prompt
 * can point the user at the right fix.
 */
export async function diagnoseAccessToken(
  client: CoalesceClient
): Promise<AccessTokenStatus> {
  const profile = loadCoaProfile();
  const profileName = profile?.profileName ?? getActiveProfileName();
  const token = pickField(process.env.COALESCE_ACCESS_TOKEN, profile?.token, profileName);
  if (!token.source) return { status: "missing" };
  const source = token.source;
  try {
    const result = (await listWorkspaces(client)) as { data?: unknown[] };
    const count = Array.isArray(result.data) ? result.data.length : 0;
    return { status: "ok", projectCount: count, source };
  } catch (err) {
    if (err instanceof CoalesceApiError) {
      if (err.status === 401 || err.status === 403) {
        return {
          status: "invalid",
          httpStatus: err.status,
          message: err.message,
          source,
        };
      }
      return {
        status: "error",
        message: `Coalesce API returned ${err.status}: ${err.message}`,
        source,
      };
    }
    return {
      status: "error",
      message: err instanceof Error ? err.message : String(err),
      source,
    };
  }
}

// ---------- Snowflake credentials ----------

export interface SnowflakeSourceMap {
  snowflakeUsername: FieldSource;
  snowflakeWarehouse: FieldSource;
  snowflakeRole: FieldSource;
  snowflakeKeyPairKey?: FieldSource;
  snowflakeKeyPairPass?: FieldSource;
  snowflakePat?: FieldSource;
}

export type SnowflakeCredsStatus =
  | {
      status: "ok";
      authType: "KeyPair" | "PAT";
      username: string;
      warehouse: string;
      role: string;
      hasPassphrase?: boolean;
      sources: SnowflakeSourceMap;
    }
  | { status: "missing"; missing: string[] }
  | { status: "invalid"; reason: string; variable?: string };

/**
 * Check the resolved Snowflake credentials without round-tripping Snowflake or
 * reading the PEM file content. Reports sources for each field so the setup
 * prompt can pinpoint where values come from.
 */
export function diagnoseSnowflakeCreds(): SnowflakeCredsStatus {
  const profile = loadCoaProfile();
  const profileName = profile?.profileName ?? getActiveProfileName();

  const username = pickField(process.env.SNOWFLAKE_USERNAME, profile?.snowflakeUsername, profileName);
  const keyPairPath = pickField(process.env.SNOWFLAKE_KEY_PAIR_KEY, profile?.snowflakeKeyPairKey, profileName);
  const keyPairPass = pickField(process.env.SNOWFLAKE_KEY_PAIR_PASS, profile?.snowflakeKeyPairPass, profileName);
  const pat = pickField(process.env.SNOWFLAKE_PAT, undefined, profileName); // PAT is env-only
  const warehouse = pickField(process.env.SNOWFLAKE_WAREHOUSE, profile?.snowflakeWarehouse, profileName);
  const role = pickField(process.env.SNOWFLAKE_ROLE, profile?.snowflakeRole, profileName);

  const missing: string[] = [];
  if (!username.value) missing.push("SNOWFLAKE_USERNAME");
  if (!keyPairPath.value && !pat.value) missing.push("SNOWFLAKE_KEY_PAIR_KEY or SNOWFLAKE_PAT");
  if (!warehouse.value) missing.push("SNOWFLAKE_WAREHOUSE");
  if (!role.value) missing.push("SNOWFLAKE_ROLE");
  if (missing.length > 0) return { status: "missing", missing };

  const sources: SnowflakeSourceMap = {
    snowflakeUsername: username.source!,
    snowflakeWarehouse: warehouse.source!,
    snowflakeRole: role.source!,
  };
  if (keyPairPath.value) {
    sources.snowflakeKeyPairKey = keyPairPath.source!;
    if (keyPairPass.value) sources.snowflakeKeyPairPass = keyPairPass.source!;
  } else if (pat.value) {
    sources.snowflakePat = pat.source!;
  }

  if (keyPairPath.value) {
    if (!existsSync(keyPairPath.value)) {
      return {
        status: "invalid",
        reason: `Snowflake key-pair path does not exist: ${keyPairPath.value}`,
        variable: "SNOWFLAKE_KEY_PAIR_KEY",
      };
    }
    const stat = statSync(keyPairPath.value);
    if (!stat.isFile()) {
      return {
        status: "invalid",
        reason: `Snowflake key-pair path is not a file: ${keyPairPath.value}`,
        variable: "SNOWFLAKE_KEY_PAIR_KEY",
      };
    }
    return {
      status: "ok",
      authType: "KeyPair",
      username: username.value!,
      warehouse: warehouse.value!,
      role: role.value!,
      hasPassphrase: !!keyPairPass.value,
      sources,
    };
  }

  return {
    status: "ok",
    authType: "PAT",
    username: username.value!,
    warehouse: warehouse.value!,
    role: role.value!,
    sources,
  };
}

// ---------- repo path ----------

export type RepoPathStatus =
  | { status: "ok"; path: string; isCoaProject: boolean }
  | { status: "missing" }
  | { status: "invalid"; reason: string; path: string };

/**
 * Verify the configured repo path points somewhere usable. Reads COALESCE_REPO_PATH
 * first, then falls back to `repoPath` from the active ~/.coa/config profile.
 * "isCoaProject" means the path has a data.yml — which makes it a valid COA project
 * root. That's not required for the existing repo-backed tools (which want
 * nodeTypes/, etc.), but it tells the setup prompt whether coa_* tools will work
 * against it.
 */
export function diagnoseRepoPath(): RepoPathStatus {
  const envRaw = process.env.COALESCE_REPO_PATH?.trim();
  const raw = envRaw || loadCoaProfile()?.repoPath?.trim();
  const sourceLabel = envRaw ? "COALESCE_REPO_PATH" : "repoPath in ~/.coa/config";
  if (!raw) return { status: "missing" };
  const path = isAbsolute(raw) ? raw : resolve(process.cwd(), raw);
  if (!existsSync(path)) {
    return {
      status: "invalid",
      reason: `${sourceLabel} does not exist on disk.`,
      path,
    };
  }
  const stat = statSync(path);
  if (!stat.isDirectory()) {
    return {
      status: "invalid",
      reason: `${sourceLabel} is not a directory.`,
      path,
    };
  }
  const isCoaProject = existsSync(join(path, "data.yml"));
  return { status: "ok", path, isCoaProject };
}

// ---------- coa doctor ----------

export type CoaDoctorStatus =
  | { status: "skipped"; reason: string }
  | { status: "ok"; json?: unknown }
  | { status: "failed"; exitCode: number | null; stderr: string; json?: unknown }
  | { status: "error"; message: string };

/**
 * Best-effort `coa doctor --json` against the repo path. Skips cleanly when
 * the repo path is missing or not a COA project.
 */
export async function diagnoseCoaDoctor(
  repoPath: RepoPathStatus
): Promise<CoaDoctorStatus> {
  if (repoPath.status !== "ok") {
    return { status: "skipped", reason: "COALESCE_REPO_PATH is not set or not usable." };
  }
  if (!repoPath.isCoaProject) {
    return {
      status: "skipped",
      reason: "Repo has no data.yml — not a COA project. Skipping coa doctor.",
    };
  }
  try {
    const result = await runCoa(["--json", "doctor", "--dir", repoPath.path], {
      cwd: repoPath.path,
      parseJson: true,
      timeoutMs: 30_000,
    });
    // coa doctor --json echoes a truncated access token under data.cloud.checks;
    // redact before surfacing to the agent.
    const redactedJson =
      result.json !== undefined
        ? redactSensitive(result.json).value
        : undefined;
    if (result.exitCode === 0) {
      return { status: "ok", json: redactedJson };
    }
    return {
      status: "failed",
      exitCode: result.exitCode,
      stderr: result.stderr,
      json: redactedJson,
    };
  } catch (err) {
    return {
      status: "error",
      message: err instanceof Error ? err.message : String(err),
    };
  }
}

// ---------- aggregate ----------

export type DiagnoseSetupResult = {
  coaConfig: CoaConfigStatus;
  accessToken: AccessTokenStatus;
  snowflakeCreds: SnowflakeCredsStatus;
  repoPath: RepoPathStatus;
  coaDoctor: CoaDoctorStatus;
  /** Human-ordered next steps; empty when fully configured. */
  nextSteps: string[];
  /** Convenience: true only when every probe is in a happy state. */
  ready: boolean;
};

export async function diagnoseSetup(
  client: CoalesceClient
): Promise<DiagnoseSetupResult> {
  const coaConfig = diagnoseCoaConfig();
  const [accessToken, snowflakeCreds, repoPath] = [
    await diagnoseAccessToken(client),
    diagnoseSnowflakeCreds(),
    diagnoseRepoPath(),
  ];
  const coaDoctor = await diagnoseCoaDoctor(repoPath);
  const nextSteps = buildNextSteps({
    coaConfig,
    accessToken,
    snowflakeCreds,
    repoPath,
    coaDoctor,
  });
  const ready =
    accessToken.status === "ok" &&
    snowflakeCreds.status === "ok" &&
    repoPath.status === "ok" &&
    (coaDoctor.status === "ok" || coaDoctor.status === "skipped");
  return { coaConfig, accessToken, snowflakeCreds, repoPath, coaDoctor, nextSteps, ready };
}

function buildNextSteps(parts: {
  coaConfig: CoaConfigStatus;
  accessToken: AccessTokenStatus;
  snowflakeCreds: SnowflakeCredsStatus;
  repoPath: RepoPathStatus;
  coaDoctor: CoaDoctorStatus;
}): string[] {
  const steps: string[] = [];

  // Warn on profile misconfiguration even when env vars happen to satisfy the
  // token/Snowflake requirements — otherwise a stale COALESCE_PROFILE silently
  // points at a nonexistent profile while the setup appears green.
  if (parts.coaConfig.status === "parse-error") {
    steps.push(
      `~/.coa/config exists but could not be parsed: ${parts.coaConfig.message}. Fix the file or delete it to fall back to env-only mode.`
    );
  } else if (
    parts.coaConfig.status === "ok" &&
    !parts.coaConfig.profileExists
  ) {
    const available =
      parts.coaConfig.availableProfiles.length > 0
        ? parts.coaConfig.availableProfiles.join(", ")
        : "(none)";
    steps.push(
      `COALESCE_PROFILE="${parts.coaConfig.activeProfile}" but that profile is not in ~/.coa/config. Available profiles: ${available}. Set COALESCE_PROFILE to one of those, or add a [${parts.coaConfig.activeProfile}] section.`
    );
  }

  if (parts.accessToken.status === "missing") {
    if (parts.coaConfig.status === "missing-file") {
      steps.push(
        "Provide a Coalesce access token. Two options: (a) set COALESCE_ACCESS_TOKEN in your MCP client env block — generate the token from Deploy → User Settings in Coalesce; or (b) create ~/.coa/config with a `token=` line (run `coa describe config` for the INI schema). Env wins when both are set."
      );
    } else if (parts.coaConfig.status === "ok" && parts.coaConfig.profileExists) {
      steps.push(
        `No access token found. Profile [${parts.coaConfig.activeProfile}] is loaded but has no \`token\` field. Add one, or set COALESCE_ACCESS_TOKEN in your MCP client env.`
      );
    } else {
      // parse-error or (ok && !profileExists) — the standalone config warning
      // above explains the file issue; here we surface the env-var escape hatch
      // so a broken/misnamed profile isn't a hard block.
      steps.push(
        "Provide a Coalesce access token via COALESCE_ACCESS_TOKEN in your MCP client env block while the profile issue is sorted out."
      );
    }
  } else if (parts.accessToken.status === "invalid") {
    steps.push(
      `Access token is rejected by the API (${parts.accessToken.httpStatus}). ` +
        `Source: ${formatSource(parts.accessToken.source)}. ` +
        `Generate a new token from Deploy → User Settings, update whichever source the token came from, and confirm the domain matches your region.`
    );
  } else if (parts.accessToken.status === "error") {
    steps.push(
      `Could not reach the Coalesce API: ${parts.accessToken.message}. Check the profile/env domain and your network.`
    );
  }

  if (parts.snowflakeCreds.status === "missing") {
    steps.push(
      `Missing Snowflake credentials (needed for run tools and coa_create/coa_run): ${parts.snowflakeCreds.missing.join(", ")}. Set the env vars in your MCP client config, or add the matching fields to ~/.coa/config.`
    );
  } else if (parts.snowflakeCreds.status === "invalid") {
    steps.push(`Fix Snowflake credentials: ${parts.snowflakeCreds.reason}`);
  }

  if (parts.repoPath.status === "missing") {
    steps.push(
      "Optional: clone your Coalesce project locally and either set COALESCE_REPO_PATH, or add `repoPath=` to your profile in ~/.coa/config. Enables repo-backed node-type lookup and all coa_* local tools."
    );
  } else if (parts.repoPath.status === "invalid") {
    steps.push(
      `Configured repo path is unusable: ${parts.repoPath.reason} (path: ${parts.repoPath.path})`
    );
  } else if (parts.repoPath.status === "ok" && !parts.repoPath.isCoaProject) {
    steps.push(
      `Configured repo path (${parts.repoPath.path}) has no data.yml, so coa_* local tools will not work against it. If you intend to use COA, point COALESCE_REPO_PATH or your profile's repoPath at a directory containing data.yml.`
    );
  }

  if (parts.coaDoctor.status === "failed") {
    steps.push(
      `coa doctor reports issues in the project. Re-run coa_doctor for details. stderr: ${parts.coaDoctor.stderr.trim().slice(0, 200)}`
    );
  } else if (parts.coaDoctor.status === "error") {
    steps.push(`coa doctor could not run: ${parts.coaDoctor.message}`);
  }

  return steps;
}

function formatSource(source: FieldSource | undefined): string {
  if (!source) return "unknown";
  return source;
}
