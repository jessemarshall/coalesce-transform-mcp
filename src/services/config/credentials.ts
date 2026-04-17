import { existsSync, readFileSync, statSync } from "node:fs";

import {
  getActiveProfileName,
  loadCoaProfile,
  pickField,
  type CoaProfile,
  type FieldSource,
} from "./coa-config.js";
import { withSetupHint } from "../setup/hint.js";

export type { FieldSource };

const DEFAULT_BASE_URL = "https://app.coalescesoftware.io";

const PEM_BOUNDARY = "-----";
const ALLOWED_PEM_HEADERS = [
  `${PEM_BOUNDARY}BEGIN PRIVATE KEY${PEM_BOUNDARY}`,
  `${PEM_BOUNDARY}BEGIN RSA PRIVATE KEY${PEM_BOUNDARY}`,
  `${PEM_BOUNDARY}BEGIN ENCRYPTED PRIVATE KEY${PEM_BOUNDARY}`,
] as const;
const MAX_KEY_FILE_BYTES = 64 * 1024;

export interface CoalesceAuth {
  accessToken: string;
  baseUrl: string;
  sources: {
    accessToken: FieldSource;
    baseUrl: FieldSource;
  };
}

export type SnowflakeAuth =
  | {
      snowflakeAuthType: "KeyPair";
      snowflakeUsername: string;
      snowflakeKeyPairKey: string;
      snowflakeKeyPairPass?: string;
      snowflakeWarehouse: string;
      snowflakeRole: string;
      sources: SnowflakeFieldSources;
    }
  | {
      snowflakeAuthType: "Basic";
      snowflakeUsername: string;
      snowflakePassword: string;
      snowflakeWarehouse: string;
      snowflakeRole: string;
      sources: SnowflakeFieldSources;
    };

export interface SnowflakeFieldSources {
  snowflakeUsername: FieldSource;
  snowflakeWarehouse: FieldSource;
  snowflakeRole: FieldSource;
  snowflakeKeyPairKey?: FieldSource;
  snowflakeKeyPairPass?: FieldSource;
  snowflakePat?: FieldSource;
}

function stripTrailingSlashes(url: string): string {
  return url.replace(/\/+$/, "");
}

export function resolveCoalesceAuth(): CoalesceAuth {
  const profile = loadCoaProfile();
  const profileName = profile?.profileName ?? getActiveProfileName();

  const token = pickField(process.env.COALESCE_ACCESS_TOKEN, profile?.token, profileName);
  const base = pickField(process.env.COALESCE_BASE_URL, profile?.domain, profileName);

  if (!token.value || !token.source) {
    throw new Error(
      withSetupHint(
        `No Coalesce access token found. Set COALESCE_ACCESS_TOKEN in your MCP client env, ` +
          `or add a \`token=\` line to profile [${profileName}] in ~/.coa/config ` +
          `(run \`coa describe config\` for the INI schema)`
      )
    );
  }

  const baseUrl = base.value
    ? stripTrailingSlashes(base.value)
    : DEFAULT_BASE_URL;

  return {
    accessToken: token.value,
    baseUrl,
    sources: {
      accessToken: token.source,
      baseUrl: base.source ?? "default",
    },
  };
}

function readKeyPairFile(filePath: string): string {
  if (!existsSync(filePath)) {
    throw new Error(
      "Snowflake key-pair file not found at the configured path. " +
        "Check that SNOWFLAKE_KEY_PAIR_KEY or `snowflakeKeyPairKey` in ~/.coa/config points to an existing PEM private key file."
    );
  }
  const fileSize = statSync(filePath).size;
  if (fileSize > MAX_KEY_FILE_BYTES) {
    const sizeKB = Math.round(fileSize / 1024);
    throw new Error(
      `Snowflake key-pair file is ${sizeKB} KB, which exceeds the ${MAX_KEY_FILE_BYTES / 1024} KB limit for PEM key files. ` +
        "Check that the path points to a private key file, not a different file."
    );
  }
  const content = readFileSync(filePath, "utf-8").trim();
  const hasValidHeader = ALLOWED_PEM_HEADERS.some((header) => content.includes(header));
  if (!hasValidHeader) {
    throw new Error(
      "Snowflake key-pair file is not a valid PEM private key. " +
        "Expected a file containing one of: PRIVATE KEY, RSA PRIVATE KEY, or ENCRYPTED PRIVATE KEY."
    );
  }
  return content;
}

export function resolveSnowflakeAuth(): SnowflakeAuth {
  const profile: CoaProfile | null = loadCoaProfile();
  const profileName = profile?.profileName ?? getActiveProfileName();

  const username = pickField(process.env.SNOWFLAKE_USERNAME, profile?.snowflakeUsername, profileName);
  const warehouse = pickField(process.env.SNOWFLAKE_WAREHOUSE, profile?.snowflakeWarehouse, profileName);
  const role = pickField(process.env.SNOWFLAKE_ROLE, profile?.snowflakeRole, profileName);
  const keyPath = pickField(process.env.SNOWFLAKE_KEY_PAIR_KEY, profile?.snowflakeKeyPairKey, profileName);
  const keyPass = pickField(process.env.SNOWFLAKE_KEY_PAIR_PASS, profile?.snowflakeKeyPairPass, profileName);
  // PAT is env-only. COA's `snowflakePassword` profile field is documented as a
  // Basic-auth password for COA's own Snowflake connection — a different concept
  // from the Coalesce Deploy API's PAT, which this MCP uses for run tools. We
  // deliberately don't read it from the profile to avoid conflating the two.
  const pat = pickField(process.env.SNOWFLAKE_PAT, undefined, profileName);

  const useKeyPair = Boolean(keyPath.value);
  const usePat = !useKeyPair && Boolean(pat.value);

  const missing: string[] = [];
  if (!username.value) missing.push(`SNOWFLAKE_USERNAME (or \`snowflakeUsername\` in profile [${profileName}])`);
  if (!useKeyPair && !usePat) {
    missing.push(
      `SNOWFLAKE_KEY_PAIR_KEY or SNOWFLAKE_PAT (or \`snowflakeKeyPairKey\` in profile [${profileName}])`
    );
  }
  if (!warehouse.value) missing.push(`SNOWFLAKE_WAREHOUSE (or \`snowflakeWarehouse\` in profile [${profileName}])`);
  if (!role.value) missing.push(`SNOWFLAKE_ROLE (or \`snowflakeRole\` in profile [${profileName}])`);

  if (missing.length > 0) {
    throw new Error(
      `Missing required Snowflake credential${missing.length > 1 ? "s" : ""} for run tools: ${missing.join(", ")}. ` +
        "Set these in your MCP client env, or add the matching fields to ~/.coa/config " +
        "(run `coa describe config` for the schema)."
    );
  }

  if (usePat) {
    const patValue = pat.value!;
    if (patValue.startsWith("/") || patValue.startsWith("~") || patValue.endsWith(".pem")) {
      throw new Error(
        "SNOWFLAKE_PAT appears to be a file path, not a token. " +
          "SNOWFLAKE_PAT should contain the token string itself. " +
          "If you meant to use Key Pair auth, set SNOWFLAKE_KEY_PAIR_KEY instead."
      );
    }
    return {
      snowflakeAuthType: "Basic",
      snowflakeUsername: username.value!,
      snowflakePassword: patValue,
      snowflakeWarehouse: warehouse.value!,
      snowflakeRole: role.value!,
      sources: {
        snowflakeUsername: username.source!,
        snowflakeWarehouse: warehouse.source!,
        snowflakeRole: role.source!,
        snowflakePat: pat.source!,
      },
    };
  }

  const keyContent = readKeyPairFile(keyPath.value!);
  return {
    snowflakeAuthType: "KeyPair",
    snowflakeUsername: username.value!,
    snowflakeKeyPairKey: keyContent,
    ...(keyPass.value ? { snowflakeKeyPairPass: keyPass.value } : {}),
    snowflakeWarehouse: warehouse.value!,
    snowflakeRole: role.value!,
    sources: {
      snowflakeUsername: username.source!,
      snowflakeWarehouse: warehouse.source!,
      snowflakeRole: role.source!,
      snowflakeKeyPairKey: keyPath.source!,
      ...(keyPass.source ? { snowflakeKeyPairPass: keyPass.source } : {}),
    },
  };
}
