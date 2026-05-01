import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { Dirent, existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "fs";
import { basename, join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildCacheResourceUri,
  getCacheDir,
  getCacheResourceMimeType,
  resolveCacheResourceUri,
} from "../cache-dir.js";
import { registerCoaDescribeResources } from "./coa-describe.js";
import { safeErrorMessage } from "../utils.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Single source of truth for the bundled Coalesce context resources.
 *
 * Adding a resource: append a row here and ship the .md file at `path`. The
 * registered URI, file path, display name, description, and test fixture all
 * derive from this array — no separate maps to keep in sync.
 *
 * All entries currently serve markdown; `mimeType` defaults to `text/markdown`
 * unless an entry overrides it.
 */
type ContextResource = {
  uri: string;
  path: string;
  name: string;
  description: string;
  mimeType?: string;
};

const CONTEXT_RESOURCES: readonly ContextResource[] = [
  {
    uri: "coalesce://context/overview",
    path: "context/overview.md",
    name: "Coalesce Overview",
    description:
      "General Coalesce concepts, response guidelines, and operational constraints for AI assistants",
  },
  {
    uri: "coalesce://context/sql-platform-selection",
    path: "context/sql-platform-selection.md",
    name: "SQL Platform Selection",
    description:
      "How to determine the active SQL platform from project metadata and existing node SQL before choosing a dialect-specific resource",
  },
  {
    uri: "coalesce://context/sql-snowflake",
    path: "context/sql-snowflake.md",
    name: "SQL Rules: Snowflake",
    description: "Snowflake-specific SQL conventions for Coalesce node SQL",
  },
  {
    uri: "coalesce://context/sql-databricks",
    path: "context/sql-databricks.md",
    name: "SQL Rules: Databricks",
    description: "Databricks-specific SQL conventions for Coalesce node SQL",
  },
  {
    uri: "coalesce://context/sql-bigquery",
    path: "context/sql-bigquery.md",
    name: "SQL Rules: BigQuery",
    description: "BigQuery-specific SQL conventions for Coalesce node SQL",
  },
  {
    uri: "coalesce://context/data-engineering-principles",
    path: "context/data-engineering-principles.md",
    name: "Data Engineering Principles",
    description:
      "Data engineering best practices for node type selection, layered architecture, methodology detection, materialization strategies, and dependency management",
  },
  {
    uri: "coalesce://context/storage-mappings",
    path: "context/storage-mappings.md",
    name: "Storage Locations and References",
    description:
      "Storage location concepts, {{ ref() }} syntax, and reference patterns in Coalesce SQL",
  },
  {
    uri: "coalesce://context/tool-usage",
    path: "context/tool-usage.md",
    name: "Tool Usage Patterns",
    description:
      "Best practices for tool batching, parallelization, SQL conversion, and node operations",
  },
  {
    uri: "coalesce://context/id-discovery",
    path: "context/id-discovery.md",
    name: "ID Discovery",
    description:
      "How to resolve project, workspace, environment, job, run, node, and org IDs before calling Coalesce tools",
  },
  {
    uri: "coalesce://context/node-creation-decision-tree",
    path: "context/node-creation-decision-tree.md",
    name: "Node Creation Decision Tree",
    description:
      "How to choose between predecessor-based creation, updates, and full replacements for workspace nodes",
  },
  {
    uri: "coalesce://context/node-payloads",
    path: "context/node-payloads.md",
    name: "Node Payloads",
    description:
      "Practical guidance for working with workspace node bodies, including top-level fields, metadata, config, and array-replacement risks",
  },
  {
    uri: "coalesce://context/hydrated-metadata",
    path: "context/hydrated-metadata.md",
    name: "Hydrated Metadata",
    description:
      "Practical summary of Coalesce hydrated metadata structures for advanced node payload editing",
  },
  {
    uri: "coalesce://context/run-operations",
    path: "context/run-operations.md",
    name: "Run Operations",
    description:
      "Guidance for starting, retrying, polling, diagnosing, and canceling Coalesce runs",
  },
  {
    uri: "coalesce://context/node-type-corpus",
    path: "context/node-type-corpus.md",
    name: "Node Type Corpus",
    description:
      "Node type discovery, corpus search, metadata patterns (consult BEFORE creating or editing nodes)",
  },
  {
    uri: "coalesce://context/aggregation-patterns",
    path: "context/aggregation-patterns.md",
    name: "Aggregation Patterns",
    description:
      "Automatic JOIN ON generation, GROUP BY detection, datatype inference, and patterns for converting joins to aggregations",
  },
  {
    uri: "coalesce://context/intelligent-node-configuration",
    path: "context/intelligent-node-configuration.md",
    name: "Intelligent Node Configuration",
    description:
      "How intelligent config completion works for workspace nodes, including schema resolution, intelligence rules, and automatic field detection",
  },
  {
    uri: "coalesce://context/pipeline-workflows",
    path: "context/pipeline-workflows.md",
    name: "Pipeline Workflows",
    description:
      "Building pipelines end-to-end: node type selection, multi-node sequences, incremental setup, and pipeline execution",
  },
  {
    uri: "coalesce://context/node-operations",
    path: "context/node-operations.md",
    name: "Node Operations",
    description:
      "Editing existing nodes: join conditions, column operations, config fields, rename safety, SQL-to-graph conversion, and debugging",
  },
  {
    uri: "coalesce://context/node-type-selection-guide",
    path: "context/node-type-selection-guide.md",
    name: "Node Type Selection Guide",
    description:
      "When to use each Coalesce node type: Stage/Work for general transforms, Dimension/Fact only for dimensional modeling, and when to avoid Dynamic Tables, Incremental Loads, and other specialized patterns",
  },
  {
    uri: "coalesce://context/intent-pipeline-guide",
    path: "context/intent-pipeline-guide.md",
    name: "Intent Pipeline Guide",
    description:
      "How to use build_pipeline_from_intent to create pipelines from natural language descriptions, including entity resolution, operation detection, and the clarification flow",
  },
  {
    uri: "coalesce://context/run-diagnostics-guide",
    path: "context/run-diagnostics-guide.md",
    name: "Run Diagnostics Guide",
    description:
      "How to use diagnose_run_failure to analyze failed runs, classify node-level errors, and determine actionable fixes",
  },
  {
    uri: "coalesce://context/pipeline-review-guide",
    path: "context/pipeline-review-guide.md",
    name: "Pipeline Review Guide",
    description:
      "How to use review_pipeline to analyze existing pipelines for redundant nodes, missing joins, layer violations, naming issues, and optimization opportunities",
  },
  {
    uri: "coalesce://context/pipeline-workshop-guide",
    path: "context/pipeline-workshop-guide.md",
    name: "Pipeline Workshop Guide",
    description:
      "How to use the pipeline workshop tools for iterative, conversational pipeline building with session state",
  },
  {
    uri: "coalesce://context/ecosystem-boundaries",
    path: "context/ecosystem-boundaries.md",
    name: "Ecosystem Boundaries",
    description:
      "Scope of this MCP vs adjacent data engineering MCPs (Snowflake, Fivetran, dbt, Catalog) with cross-server workflow patterns",
  },
  {
    uri: "coalesce://context/setup-guide",
    path: "context/setup-guide.md",
    name: "Setup Guide",
    description:
      "How to walk a user through first-time Coalesce MCP setup conversationally — driven by diagnose_setup output. Load when the user is getting configured for the first time or a tool error points at missing credentials/profile/repo path.",
  },
  {
    uri: "coalesce://context/sql-node-v2-policy",
    path: "context/sql-node-v2-policy.md",
    name: "SQL Node V1 vs V2 Policy",
    description:
      "Default-V1 policy for local COA project authoring. Covers how to detect project shape, when V2 is permitted (explicit user ask + alpha warning), and the full V2 node-type + .sql file setup. Load before editing anything in a COA project on disk.",
  },
];

const DEFAULT_MIME_TYPE = "text/markdown";

// Derived lookup: URI → relative path. Exported for skills-dir seeding.
export const RESOURCE_FILES: Record<string, string> = Object.fromEntries(
  CONTEXT_RESOURCES.map((r) => [r.uri, r.path])
);

// Derived lookup: URI → display metadata.
const RESOURCE_METADATA: Record<
  string,
  { name: string; description: string; mimeType: string }
> = Object.fromEntries(
  CONTEXT_RESOURCES.map((r) => [
    r.uri,
    {
      name: r.name,
      description: r.description,
      mimeType: r.mimeType ?? DEFAULT_MIME_TYPE,
    },
  ])
);

export const OVERRIDE_MARKER = "<!-- OVERRIDE -->";
const STUB_MARKER = "<!-- STUB -->";

let skillsInitialized = false;

/** Reset skills initialization state (for testing only). */
export function resetSkillsState(): void {
  skillsInitialized = false;
}

/**
 * Extract the resource name from a relative path like "context/overview.md" → "overview"
 */
export function extractResourceName(relativePath: string): string {
  return basename(relativePath, ".md");
}

/**
 * Read the bundled (fallback) content for a resource from dist/resources/context/
 */
function readBundledContent(relativePath: string): string {
  return readFileSync(join(__dirname, relativePath), "utf-8");
}

/**
 * Seed the skills directory with default coalesce_skills.* and user_skills.* files.
 * Idempotent — never overwrites existing files.
 */
export function initializeSkillsDir(skillsDir: string): void {
  try {
    mkdirSync(skillsDir, { recursive: true });

    for (const relativePath of Object.values(RESOURCE_FILES)) {
      const resourceName = extractResourceName(relativePath);
      const skillsFile = join(skillsDir, `coalesce_skills.${resourceName}.md`);
      const userFile = join(skillsDir, `user_skills.${resourceName}.md`);

      const bundledContent = readBundledContent(relativePath);

      if (!existsSync(skillsFile)) {
        writeFileSync(skillsFile, bundledContent, "utf-8");
      }

      if (!existsSync(userFile)) {
        const stub = [
          STUB_MARKER,
          "<!-- User customization file for this skill.",
          `    To OVERRIDE the default, add "${OVERRIDE_MARKER}" as the very first line`,
          "    followed by your replacement content.",
          `    To AUGMENT the default, just add your content below (remove the "${STUB_MARKER}" line first).`,
          "    To DISABLE, delete both this file and the coalesce_skills file. -->",
        ].join("\n");
        writeFileSync(userFile, stub, "utf-8");
      }
    }
  } catch (error) {
    process.stderr.write(
      `Warning: Failed to initialize skills directory at ${skillsDir}: ${safeErrorMessage(error)}\n`
    );
  }
}

/**
 * Read a resource content file, resolving from the skills directory when
 * COALESCE_MCP_SKILLS_DIR is set, otherwise falling back to bundled files.
 *
 * Resolution order:
 * 1. user_skills file starts with <!-- OVERRIDE --> → use only user file
 * 2. user_skills file has content → concatenate coalesce_skills + user_skills
 * 3. user_skills empty/missing → use coalesce_skills file
 * 4. Both missing → return empty string
 */
function readResourceContent(relativePath: string): string {
  const skillsDir = process.env.COALESCE_MCP_SKILLS_DIR;

  if (!skillsDir) {
    return readBundledContent(relativePath);
  }

  if (!skillsInitialized) {
    initializeSkillsDir(skillsDir);
    skillsInitialized = true;
  }

  const resourceName = extractResourceName(relativePath);
  const skillsFile = join(skillsDir, `coalesce_skills.${resourceName}.md`);
  const userFile = join(skillsDir, `user_skills.${resourceName}.md`);

  // Read user file content
  let userContent: string | null = null;
  if (existsSync(userFile)) {
    // Strip UTF-8 BOM if present (common with Windows editors)
    const raw = readFileSync(userFile, "utf-8").replace(/^\uFEFF/, "");
    if (raw.startsWith(STUB_MARKER)) {
      // Seeded stub — treat as empty (user hasn't customized yet)
      userContent = null;
    } else if (raw.trim().length > 0) {
      userContent = raw;
    }
  }

  // Case 1: user file has override marker → use only user content
  if (userContent !== null && userContent.startsWith(OVERRIDE_MARKER)) {
    return userContent;
  }

  // Read default skills file
  const skillsExists = existsSync(skillsFile);
  const skillsContent = skillsExists ? readFileSync(skillsFile, "utf-8") : null;

  // Case 2: user file has content (no override) → concatenate
  if (userContent !== null && skillsContent !== null) {
    return skillsContent + "\n\n" + userContent;
  }
  if (userContent !== null) {
    return userContent;
  }

  // Case 3: no user content → use default skills file
  if (skillsContent !== null) {
    return skillsContent;
  }

  // Case 4: both missing → empty
  return "";
}

function listCacheFilePaths(directory: string): string[] {
  if (!existsSync(directory)) {
    return [];
  }

  const entries: Dirent[] = readdirSync(directory, { withFileTypes: true });
  const filePaths: string[] = [];
  for (const entry of entries) {
    const entryPath = join(directory, entry.name);
    if (entry.isDirectory()) {
      filePaths.push(...listCacheFilePaths(entryPath));
      continue;
    }
    if (entry.isFile()) {
      filePaths.push(entryPath);
    }
  }
  return filePaths.sort();
}

function isCompleteSnapshotArtifact(filePath: string): boolean {
  if (filePath.includes(".tmp-") || filePath.includes(".bak-")) {
    return false;
  }

  if (filePath.endsWith(".ndjson")) {
    return existsSync(filePath.replace(/\.ndjson$/, ".meta.json"));
  }

  if (filePath.endsWith(".meta.json")) {
    return existsSync(filePath.replace(/\.meta\.json$/, ".ndjson"));
  }

  return true;
}

function listCacheResources(baseDir?: string): {
  uri: string;
  name: string;
  description: string;
  mimeType: string;
}[] {
  const cacheDir = getCacheDir(baseDir);
  return listCacheFilePaths(cacheDir)
    .filter(isCompleteSnapshotArtifact)
    .flatMap((filePath) => {
    const uri = buildCacheResourceUri(filePath, baseDir);
    if (!uri) {
      return [];
    }

    return [
      {
        uri,
        name: basename(filePath),
        description: "Cached Coalesce MCP artifact stored on disk and exposed through MCP resources.",
        mimeType: getCacheResourceMimeType(filePath),
      },
    ];
    });
}

/**
 * Register all Coalesce context resources with the MCP server
 */
export function registerResources(server: McpServer): void {
  for (const [uri, metadata] of Object.entries(RESOURCE_METADATA)) {
    server.resource(
      metadata.name,
      uri,
      {
        description: metadata.description,
        mimeType: metadata.mimeType,
      },
      async (resourceUri) => {
        const filePath = RESOURCE_FILES[uri];
        if (!filePath) {
          throw new Error(`Unknown resource: ${uri}`);
        }

        try {
          const content = readResourceContent(filePath);
          return {
            contents: [
              {
                uri: resourceUri.toString(),
                mimeType: metadata.mimeType,
                text: content,
              },
            ],
          };
        } catch (error) {
          throw new Error(
            `Failed to read resource ${uri}: ${safeErrorMessage(error)}`
          );
        }
      }
    );
  }

  registerCoaDescribeResources(server);

  server.resource(
    "Coalesce Cache Artifact",
    new ResourceTemplate("coalesce://cache/{cacheKey}", {
      list: async () => ({
        resources: listCacheResources(),
      }),
      complete: {
        cacheKey: async (value) =>
          listCacheResources()
            .map((resource) => resource.uri.split("/").pop() ?? "")
            .filter((cacheKey) => cacheKey.startsWith(value))
            .slice(0, 50),
      },
    }),
    {
      description:
        "Dynamic resources for cached tool responses, cache snapshots, and pipeline summaries.",
    },
    async (resourceUri) => {
      const resolved = resolveCacheResourceUri(resourceUri.toString());
      if (!resolved || !isCompleteSnapshotArtifact(resolved.filePath)) {
        throw new Error(`Unknown cache resource: ${resourceUri.toString()}`);
      }

      try {
        return {
          contents: [
            {
              uri: resourceUri.toString(),
              mimeType: getCacheResourceMimeType(resolved.filePath),
              text: readFileSync(resolved.filePath, "utf8"),
            },
          ],
        };
      } catch (error) {
        throw new Error(
          `Failed to read cache resource ${resourceUri.toString()}: ${safeErrorMessage(error)}`
        );
      }
    }
  );
}
