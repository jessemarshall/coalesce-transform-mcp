import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  COA_DESCRIBE_TOPICS,
  fetchDescribeTopic,
  type CoaDescribeTopic,
} from "../services/coa/describe.js";
import { safeErrorMessage } from "../utils.js";

type TopicMetadata = {
  name: string;
  description: string;
};

const TOPIC_METADATA: Record<CoaDescribeTopic, TopicMetadata> = {
  overview: {
    name: "COA Describe: Overview",
    description:
      "Top-level COA CLI overview — core workflow, commands, and pointers into the rest of the describe topics.",
  },
  commands: {
    name: "COA Describe: Commands",
    description:
      "Full CLI command reference — flags, arguments, and examples for every coa subcommand.",
  },
  selectors: {
    name: "COA Describe: Selectors",
    description:
      "Node selector syntax for --include and --exclude (e.g., `{ STG_ORDERS }`, `{ location: \"SRC\" }`).",
  },
  schemas: {
    name: "COA Describe: Schemas",
    description:
      "List of YAML schema types supported by COA (data.yml, locations.yml, workspaces.yml, etc.).",
  },
  workflow: {
    name: "COA Describe: Workflow",
    description:
      "Iterative local development workflow: define → dry-run → create → run → verify → iterate.",
  },
  structure: {
    name: "COA Describe: Structure",
    description:
      "COA workspace directory layout — where nodes, nodeTypes, environments, and macros belong.",
  },
  concepts: {
    name: "COA Describe: Concepts",
    description:
      "Coalesce object and workflow model — node types, pipeline architecture, data engineering guidance.",
  },
  "sql-format": {
    name: "COA Describe: SQL Format",
    description:
      "V2 .sql file format: annotations (@id, @nodeType, config annotations), ref() syntax, and gotchas.",
  },
  "node-types": {
    name: "COA Describe: Node Types",
    description:
      "Node type authoring — fileVersion, nodeMetadataSpec, create.sql.j2 and run.sql.j2 template patterns.",
  },
  config: {
    name: "COA Describe: Config",
    description:
      "~/.coa/config file shape, multi-profile setups, and platform credential formats.",
  },
};

export const COA_DESCRIBE_RESOURCE_URIS: Record<CoaDescribeTopic, string> =
  Object.fromEntries(
    COA_DESCRIBE_TOPICS.map((topic) => [
      topic,
      `coalesce://coa/describe/${topic}`,
    ])
  ) as Record<CoaDescribeTopic, string>;

const PRECEDENCE_NOTE =
  "Authoritative for CLI concepts (selectors, YAML schemas, command flags, config file, SQL format). For cloud-REST tools and cross-surface decisions, see `coalesce://context/*`.";

/**
 * Register the `coalesce://coa/describe/<topic>` resources. Each resource is
 * backed by a cached `coa describe <topic>` fetch — see services/coa/describe.ts.
 *
 * Read callbacks never throw: if COA is unreachable or errors, the callback
 * returns a placeholder markdown so the resource list stays usable.
 */
export function registerCoaDescribeResources(server: McpServer): void {
  for (const topic of COA_DESCRIBE_TOPICS) {
    const uri = COA_DESCRIBE_RESOURCE_URIS[topic];
    const metadata = TOPIC_METADATA[topic];
    server.resource(
      metadata.name,
      uri,
      {
        description: `${metadata.description} ${PRECEDENCE_NOTE}`,
        mimeType: "text/markdown",
      },
      async (resourceUri) => {
        const text = await readCoaDescribeContent(topic);
        return {
          contents: [
            {
              uri: resourceUri.toString(),
              mimeType: "text/markdown",
              text,
            },
          ],
        };
      }
    );
  }
}

async function readCoaDescribeContent(topic: CoaDescribeTopic): Promise<string> {
  try {
    const result = await fetchDescribeTopic(topic);
    return result.content;
  } catch (err) {
    const message = safeErrorMessage(err);
    return buildUnavailablePlaceholder(topic, message);
  }
}

function buildUnavailablePlaceholder(
  topic: CoaDescribeTopic,
  message: string
): string {
  return [
    `# COA describe: ${topic} (temporarily unavailable)`,
    "",
    "Could not fetch this topic from the bundled COA CLI. The MCP-side issue is:",
    "",
    "```",
    message,
    "```",
    "",
    "To diagnose, try running the command directly:",
    "",
    "```",
    `coa --no-color describe ${topic}`,
    "```",
    "",
    "If the CLI is missing, re-run `npm install` in the MCP package to restore the bundled `@coalescesoftware/coa` dependency.",
  ].join("\n");
}
