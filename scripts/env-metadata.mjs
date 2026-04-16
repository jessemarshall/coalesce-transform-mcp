export const ENV_METADATA = [
  {
    name: "COALESCE_ACCESS_TOKEN",
    group: "core",
    description: "Bearer token from the Coalesce Deploy tab. Optional when `~/.coa/config` provides a `token`.",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: true,
  },
  {
    name: "COALESCE_PROFILE",
    group: "core",
    description: "Selects which `~/.coa/config` profile to load.",
    defaultValue: "default",
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "COALESCE_BASE_URL",
    group: "core",
    description: "Region-specific base URL.",
    defaultValue: "https://app.coalescesoftware.io (US)",
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "COALESCE_ORG_ID",
    group: "core",
    description: "Fallback org ID for cancel-run. Also readable from `orgID` in the active ~/.coa/config profile.",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "COALESCE_REPO_PATH",
    group: "core",
    description: "Local repo root for repo-backed tools and pipeline planning. Also readable from `repoPath` in the active ~/.coa/config profile.",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "COALESCE_CACHE_DIR",
    group: "core",
    description: "Base directory for the local data cache. When set, cache files are written here instead of the working directory. Also readable from `cacheDir` in the active ~/.coa/config profile.",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "COALESCE_MCP_AUTO_CACHE_MAX_BYTES",
    group: "core",
    description: "JSON size threshold before auto-caching to disk.",
    defaultValue: "32768",
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "COALESCE_MCP_LINEAGE_TTL_MS",
    group: "core",
    description: "In-memory lineage cache TTL in milliseconds.",
    defaultValue: "1800000",
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "COALESCE_MCP_MAX_REQUEST_BODY_BYTES",
    group: "core",
    description: "Max outbound API request body size.",
    defaultValue: "524288",
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "COALESCE_MCP_READ_ONLY",
    group: "core",
    description: "When `true`, hides all write/mutation tools during registration. Only read, list, search, cache, analyze, review, diagnose, and plan tools are exposed.",
    defaultValue: "false",
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "COALESCE_MCP_SKILLS_DIR",
    group: "core",
    description: "Directory for customizable AI skill resources. When set, reads context resources from this directory and seeds defaults on first run. Users can augment or override any skill.",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "SNOWFLAKE_USERNAME",
    group: "snowflake",
    description: "Snowflake account username",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: true,
    isSecret: false,
  },
  {
    name: "SNOWFLAKE_KEY_PAIR_KEY",
    group: "snowflake",
    description: "Path to PEM-encoded private key (required if SNOWFLAKE_PAT not set)",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: false,
  },
  {
    name: "SNOWFLAKE_PAT",
    group: "snowflake",
    description: "Snowflake Programmatic Access Token (alternative to key pair)",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: true,
  },
  {
    name: "SNOWFLAKE_KEY_PAIR_PASS",
    group: "snowflake",
    description: "Passphrase for encrypted keys",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: false,
    isSecret: true,
  },
  {
    name: "SNOWFLAKE_WAREHOUSE",
    group: "snowflake",
    description: "Snowflake compute warehouse",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: true,
    isSecret: false,
  },
  {
    name: "SNOWFLAKE_ROLE",
    group: "snowflake",
    description: "Snowflake user role",
    defaultValue: null,
    requiredForServer: false,
    requiredForRunTools: true,
    isSecret: false,
  },
];

const README_TABLE_MARKERS = {
  coreStart: "<!-- ENV_METADATA_CORE_TABLE_START -->",
  coreEnd: "<!-- ENV_METADATA_CORE_TABLE_END -->",
  snowflakeStart: "<!-- ENV_METADATA_SNOWFLAKE_TABLE_START -->",
  snowflakeEnd: "<!-- ENV_METADATA_SNOWFLAKE_TABLE_END -->",
};

function quoteMarkdown(value) {
  return `\`${value}\``;
}

function renderMarkdownTable(headers, rows) {
  const separator = headers.map(() => "--------");
  return [
    `| ${headers.join(" | ")} |`,
    `| ${separator.join(" | ")} |`,
    ...rows.map((row) => `| ${row.join(" | ")} |`),
  ].join("\n");
}

export function getRuntimeEnvironmentVariableNames() {
  return ENV_METADATA.map((entry) => entry.name);
}

export function buildServerEnvironmentVariables() {
  return ENV_METADATA.map((entry) => ({
    description:
      entry.group === "snowflake" && entry.requiredForRunTools
        ? `${entry.description} (required for run tools)`
        : entry.group === "snowflake"
          ? entry.description
          : entry.defaultValue
            ? `${entry.description} Defaults to ${entry.defaultValue}.`
            : entry.description,
    isRequired: entry.requiredForServer,
    format: "string",
    isSecret: entry.isSecret,
    name: entry.name,
  }));
}

export function renderReadmeCoreEnvironmentTable() {
  const rows = ENV_METADATA.filter((entry) => entry.group === "core").map((entry) => [
    quoteMarkdown(entry.name),
    entry.requiredForServer ? `**Required.** ${entry.description}` : entry.description,
    entry.defaultValue ? quoteMarkdown(entry.defaultValue) : "—",
  ]);
  return renderMarkdownTable(["Variable", "Description", "Default"], rows);
}

export function renderReadmeSnowflakeEnvironmentTable() {
  const rows = ENV_METADATA.filter((entry) => entry.group === "snowflake").map((entry) => [
    quoteMarkdown(entry.name),
    entry.requiredForRunTools ? "Yes" : "No",
    entry.description,
  ]);
  return renderMarkdownTable(["Variable", "Required", "Description"], rows);
}

export function replaceReadmeEnvironmentTables(readme) {
  const coreSection = [
    README_TABLE_MARKERS.coreStart,
    renderReadmeCoreEnvironmentTable(),
    README_TABLE_MARKERS.coreEnd,
  ].join("\n");
  const snowflakeSection = [
    README_TABLE_MARKERS.snowflakeStart,
    renderReadmeSnowflakeEnvironmentTable(),
    README_TABLE_MARKERS.snowflakeEnd,
  ].join("\n");

  return readme
    .replace(
      new RegExp(
        `${README_TABLE_MARKERS.coreStart}[\\s\\S]*?${README_TABLE_MARKERS.coreEnd}`
      ),
      coreSection
    )
    .replace(
      new RegExp(
        `${README_TABLE_MARKERS.snowflakeStart}[\\s\\S]*?${README_TABLE_MARKERS.snowflakeEnd}`
      ),
      snowflakeSection
    );
}

export { README_TABLE_MARKERS };
