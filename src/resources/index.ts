import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { Dirent, existsSync, readFileSync, readdirSync } from "fs";
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
} as const;

// Map URIs to file paths
const RESOURCE_FILES: Record<string, string> = {
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
};

/**
 * Read a resource content file
 */
function readResourceContent(relativePath: string): string {
  const fullPath = join(__dirname, relativePath);
  return readFileSync(fullPath, "utf-8");
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

function listCacheResources(baseDir?: string): {
  uri: string;
  name: string;
  description: string;
  mimeType: string;
}[] {
  const cacheDir = getCacheDir(baseDir);
  return listCacheFilePaths(cacheDir).flatMap((filePath) => {
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
      if (!resolved) {
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
