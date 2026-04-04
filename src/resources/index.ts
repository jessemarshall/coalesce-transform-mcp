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

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Resource URIs
const RESOURCES = {
  OVERVIEW: "coalesce://context/overview",
  SQL_PLATFORM_SELECTION: "coalesce://context/sql-platform-selection",
  SQL_SNOWFLAKE: "coalesce://context/sql-snowflake",
  SQL_DATABRICKS: "coalesce://context/sql-databricks",
  SQL_BIGQUERY: "coalesce://context/sql-bigquery",
  DATA_ENGINEERING_PRINCIPLES: "coalesce://context/data-engineering-principles",
  STORAGE_MAPPINGS: "coalesce://context/storage-mappings",
  TOOL_USAGE: "coalesce://context/tool-usage",
  ID_DISCOVERY: "coalesce://context/id-discovery",
  NODE_CREATION_DECISION_TREE:
    "coalesce://context/node-creation-decision-tree",
  NODE_PAYLOADS: "coalesce://context/node-payloads",
  HYDRATED_METADATA: "coalesce://context/hydrated-metadata",
  RUN_OPERATIONS: "coalesce://context/run-operations",
  NODE_TYPE_CORPUS: "coalesce://context/node-type-corpus",
  AGGREGATION_PATTERNS: "coalesce://context/aggregation-patterns",
  INTELLIGENT_NODE_CONFIGURATION:
    "coalesce://context/intelligent-node-configuration",
  PIPELINE_WORKFLOWS: "coalesce://context/pipeline-workflows",
  NODE_OPERATIONS: "coalesce://context/node-operations",
  NODE_TYPE_SELECTION_GUIDE:
    "coalesce://context/node-type-selection-guide",
  INTENT_PIPELINE_GUIDE:
    "coalesce://context/intent-pipeline-guide",
  RUN_DIAGNOSTICS_GUIDE:
    "coalesce://context/run-diagnostics-guide",
  PIPELINE_REVIEW_GUIDE:
    "coalesce://context/pipeline-review-guide",
  PIPELINE_WORKSHOP_GUIDE:
    "coalesce://context/pipeline-workshop-guide",
} as const;

// Map URIs to file paths
export const RESOURCE_FILES: Record<string, string> = {
  [RESOURCES.OVERVIEW]: "context/overview.md",
  [RESOURCES.SQL_PLATFORM_SELECTION]: "context/sql-platform-selection.md",
  [RESOURCES.SQL_SNOWFLAKE]: "context/sql-snowflake.md",
  [RESOURCES.SQL_DATABRICKS]: "context/sql-databricks.md",
  [RESOURCES.SQL_BIGQUERY]: "context/sql-bigquery.md",
  [RESOURCES.DATA_ENGINEERING_PRINCIPLES]: "context/data-engineering-principles.md",
  [RESOURCES.STORAGE_MAPPINGS]: "context/storage-mappings.md",
  [RESOURCES.TOOL_USAGE]: "context/tool-usage.md",
  [RESOURCES.ID_DISCOVERY]: "context/id-discovery.md",
  [RESOURCES.NODE_CREATION_DECISION_TREE]:
    "context/node-creation-decision-tree.md",
  [RESOURCES.NODE_PAYLOADS]: "context/node-payloads.md",
  [RESOURCES.HYDRATED_METADATA]: "context/hydrated-metadata.md",
  [RESOURCES.RUN_OPERATIONS]: "context/run-operations.md",
  [RESOURCES.NODE_TYPE_CORPUS]: "context/node-type-corpus.md",
  [RESOURCES.AGGREGATION_PATTERNS]: "context/aggregation-patterns.md",
  [RESOURCES.INTELLIGENT_NODE_CONFIGURATION]:
    "context/intelligent-node-configuration.md",
  [RESOURCES.PIPELINE_WORKFLOWS]: "context/pipeline-workflows.md",
  [RESOURCES.NODE_OPERATIONS]: "context/node-operations.md",
  [RESOURCES.NODE_TYPE_SELECTION_GUIDE]:
    "context/node-type-selection-guide.md",
  [RESOURCES.INTENT_PIPELINE_GUIDE]:
    "context/intent-pipeline-guide.md",
  [RESOURCES.RUN_DIAGNOSTICS_GUIDE]:
    "context/run-diagnostics-guide.md",
  [RESOURCES.PIPELINE_REVIEW_GUIDE]:
    "context/pipeline-review-guide.md",
  [RESOURCES.PIPELINE_WORKSHOP_GUIDE]:
    "context/pipeline-workshop-guide.md",
};

// Resource metadata
const RESOURCE_METADATA: Record<
  string,
  { name: string; description: string; mimeType: string }
> = {
  [RESOURCES.OVERVIEW]: {
    name: "Coalesce Overview",
    description:
      "General Coalesce concepts, response guidelines, and operational constraints for AI assistants",
    mimeType: "text/markdown",
  },
  [RESOURCES.SQL_PLATFORM_SELECTION]: {
    name: "SQL Platform Selection",
    description:
      "How to determine the active SQL platform from project metadata and existing node SQL before choosing a dialect-specific resource",
    mimeType: "text/markdown",
  },
  [RESOURCES.SQL_SNOWFLAKE]: {
    name: "SQL Rules: Snowflake",
    description:
      "Snowflake-specific SQL conventions for Coalesce node SQL",
    mimeType: "text/markdown",
  },
  [RESOURCES.SQL_DATABRICKS]: {
    name: "SQL Rules: Databricks",
    description:
      "Databricks-specific SQL conventions for Coalesce node SQL",
    mimeType: "text/markdown",
  },
  [RESOURCES.SQL_BIGQUERY]: {
    name: "SQL Rules: BigQuery",
    description:
      "BigQuery-specific SQL conventions for Coalesce node SQL",
    mimeType: "text/markdown",
  },
  [RESOURCES.DATA_ENGINEERING_PRINCIPLES]: {
    name: "Data Engineering Principles",
    description:
      "Data engineering best practices for node type selection, layered architecture, methodology detection, materialization strategies, and dependency management",
    mimeType: "text/markdown",
  },
  [RESOURCES.STORAGE_MAPPINGS]: {
    name: "Storage Locations and References",
    description:
      "Storage location concepts, {{ ref() }} syntax, and reference patterns in Coalesce SQL",
    mimeType: "text/markdown",
  },
  [RESOURCES.TOOL_USAGE]: {
    name: "Tool Usage Patterns",
    description:
      "Best practices for tool batching, parallelization, SQL conversion, and node operations",
    mimeType: "text/markdown",
  },
  [RESOURCES.ID_DISCOVERY]: {
    name: "ID Discovery",
    description:
      "How to resolve project, workspace, environment, job, run, node, and org IDs before calling Coalesce tools",
    mimeType: "text/markdown",
  },
  [RESOURCES.NODE_CREATION_DECISION_TREE]: {
    name: "Node Creation Decision Tree",
    description:
      "How to choose between predecessor-based creation, updates, and full replacements for workspace nodes",
    mimeType: "text/markdown",
  },
  [RESOURCES.NODE_PAYLOADS]: {
    name: "Node Payloads",
    description:
      "Practical guidance for working with workspace node bodies, including top-level fields, metadata, config, and array-replacement risks",
    mimeType: "text/markdown",
  },
  [RESOURCES.HYDRATED_METADATA]: {
    name: "Hydrated Metadata",
    description:
      "Practical summary of Coalesce hydrated metadata structures for advanced node payload editing",
    mimeType: "text/markdown",
  },
  [RESOURCES.RUN_OPERATIONS]: {
    name: "Run Operations",
    description:
      "Guidance for starting, retrying, polling, diagnosing, and canceling Coalesce runs",
    mimeType: "text/markdown",
  },
  [RESOURCES.NODE_TYPE_CORPUS]: {
    name: "Node Type Corpus",
    description:
      "Node type discovery, corpus search, metadata patterns (consult BEFORE creating or editing nodes)",
    mimeType: "text/markdown",
  },
  [RESOURCES.AGGREGATION_PATTERNS]: {
    name: "Aggregation Patterns",
    description:
      "Automatic JOIN ON generation, GROUP BY detection, datatype inference, and patterns for converting joins to aggregations",
    mimeType: "text/markdown",
  },
  [RESOURCES.INTELLIGENT_NODE_CONFIGURATION]: {
    name: "Intelligent Node Configuration",
    description:
      "How intelligent config completion works for workspace nodes, including schema resolution, intelligence rules, and automatic field detection",
    mimeType: "text/markdown",
  },
  [RESOURCES.PIPELINE_WORKFLOWS]: {
    name: "Pipeline Workflows",
    description:
      "Building pipelines end-to-end: node type selection, multi-node sequences, incremental setup, and pipeline execution",
    mimeType: "text/markdown",
  },
  [RESOURCES.NODE_OPERATIONS]: {
    name: "Node Operations",
    description:
      "Editing existing nodes: join conditions, column operations, config fields, rename safety, SQL-to-graph conversion, and debugging",
    mimeType: "text/markdown",
  },
  [RESOURCES.NODE_TYPE_SELECTION_GUIDE]: {
    name: "Node Type Selection Guide",
    description:
      "When to use each Coalesce node type: Stage/Work for general transforms, Dimension/Fact only for dimensional modeling, and when to avoid Dynamic Tables, Incremental Loads, and other specialized patterns",
    mimeType: "text/markdown",
  },
  [RESOURCES.INTENT_PIPELINE_GUIDE]: {
    name: "Intent Pipeline Guide",
    description:
      "How to use build_pipeline_from_intent to create pipelines from natural language descriptions, including entity resolution, operation detection, and the clarification flow",
    mimeType: "text/markdown",
  },
  [RESOURCES.RUN_DIAGNOSTICS_GUIDE]: {
    name: "Run Diagnostics Guide",
    description:
      "How to use diagnose_run_failure to analyze failed runs, classify node-level errors, and determine actionable fixes",
    mimeType: "text/markdown",
  },
  [RESOURCES.PIPELINE_REVIEW_GUIDE]: {
    name: "Pipeline Review Guide",
    description:
      "How to use review_pipeline to analyze existing pipelines for redundant nodes, missing joins, layer violations, naming issues, and optimization opportunities",
    mimeType: "text/markdown",
  },
  [RESOURCES.PIPELINE_WORKSHOP_GUIDE]: {
    name: "Pipeline Workshop Guide",
    description:
      "How to use the pipeline workshop tools for iterative, conversational pipeline building with session state",
    mimeType: "text/markdown",
  },
};

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
    console.error(
      `Warning: Failed to initialize skills directory at ${skillsDir}: ${error instanceof Error ? error.message : String(error)}`
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
            `Failed to read resource ${uri}: ${error instanceof Error ? error.message : String(error)}`
          );
        }
      }
    );
  }

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
          `Failed to read cache resource ${resourceUri.toString()}: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    }
  );
}
