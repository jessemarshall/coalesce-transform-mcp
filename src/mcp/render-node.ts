import { z } from "zod";
import YAML from "yaml";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  buildJsonToolResponse,
  handleToolError,
  type ToolDefinition,
} from "../coalesce/types.js";
import { getWorkspaceNode } from "../coalesce/api/nodes.js";
import { setWorkspaceNodeAndInvalidate } from "../services/workspace/mutations.js";
import {
  cloudNodeToDisk,
  diskNodeToCloud,
} from "../services/templates/node-shape-bridge.js";
import {
  applyColumnDiff,
  diffColumns,
  parseCreateTableColumns,
} from "../services/templates/sql-column-diff.js";
import { isPlainObject } from "../utils.js";

// Mark unused to satisfy `noUnusedParameters`; the SDK helper signatures take a
// server instance so other tools can register callbacks. These two converters
// are pure enough that we don't need it, but we keep the parameter for
// signature symmetry with `defineCoaTools(server)`.
function _unused(server: McpServer): void {
  void server;
}

/**
 * Tools that bridge the two Coalesce workspace-node serializations:
 *
 *   - **Cloud shape**: returned by `get_workspace_node`, accepted by `set_workspace_node`.
 *     Flat top-level fields, `metadata.columns[].sources[].columnReferences[].nodeID/columnID`.
 *   - **Disk YAML shape**: read from `nodes/*.yml` by coa. Wraps under `operation:`,
 *     uses `columnReference.{columnCounter,stepCounter}` and `sourceColumnReferences[]`.
 *
 * These two tools expose the bidirectional converter (see
 * services/templates/node-shape-bridge.ts) so callers can:
 *   - Pull a cloud node and write it into a local coa project for dry-run rendering
 *     ({@link defineSerializeWorkspaceNodeToDiskYaml}).
 *   - Read a node from disk and push it back to the cloud workspace
 *     ({@link defineParseDiskNodeToWorkspaceBody}).
 */
export function defineRenderNodeTools(
  server: McpServer,
  client: CoalesceClient,
): ToolDefinition[] {
  _unused(server);
  return [
    defineSerializeWorkspaceNodeToDiskYaml(client),
    defineParseDiskNodeToWorkspaceBody(client),
    defineApplySqlToWorkspaceNode(client),
  ];
}

// ── serialize_workspace_node_to_disk_yaml ─────────────────────────────────────

const SerializeInputSchema = z.object({
  workspaceID: z.string().describe("The workspace ID containing the node."),
  nodeID: z.string().describe("The node ID to serialize."),
});

function defineSerializeWorkspaceNodeToDiskYaml(client: CoalesceClient): ToolDefinition {
  const name = "serialize_workspace_node_to_disk_yaml";
  return [
    name,
    {
      title: "Serialize Workspace Node to Disk YAML",
      description:
        "Fetch a workspace node from the cloud and convert it to the on-disk coa YAML " +
        "shape (the `nodes/*.yml` format). Returns both the parsed disk-shape object " +
        "and a YAML string ready to write to disk.\n\n" +
        "Use this when an editor or local-dev workflow needs the disk representation " +
        "of a cloud node — for example, to drop the YAML into a coa project and run " +
        "`coa create --dry-run` to preview the generated SQL without an extra round-trip.\n\n" +
        "The conversion handles the schema diff (top-level → operation: wrapper, " +
        "nodeID/columnID → stepCounter/columnCounter, sources[] → sourceColumnReferences[], " +
        "synthesized columnReference per column, fileVersion: 1 header).\n\n" +
        "UUIDs round-trip; a node serialized this way and pushed back via " +
        "parse_disk_node_to_workspace_body produces the same cloud body. The converter " +
        "is lossy on cloud-only metadata (createdAt, updatedAt, etc.) — those are dropped.",
      inputSchema: SerializeInputSchema,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    (async (params: z.infer<typeof SerializeInputSchema>) => {
      try {
        const cloudNode = await getWorkspaceNode(client, params);
        if (!cloudNode || typeof cloudNode !== "object") {
          throw new Error(`get_workspace_node returned ${typeof cloudNode}, expected object`);
        }
        const diskNode = cloudNodeToDisk(cloudNode as Record<string, unknown>);
        const yamlString = YAML.stringify(diskNode, { lineWidth: 0 });
        return buildJsonToolResponse(name, {
          diskNode,
          yaml: yamlString,
          nodeName: typeof diskNode.name === "string" ? diskNode.name : undefined,
          locationName:
            typeof (diskNode.operation as Record<string, unknown> | undefined)?.locationName ===
              "string"
              ? ((diskNode.operation as Record<string, unknown>).locationName as string)
              : undefined,
          suggestedFilename: buildSuggestedFilename(diskNode),
        });
      } catch (err) {
        return handleToolError(err);
      }
    }),
  ];
}

// ── parse_disk_node_to_workspace_body ─────────────────────────────────────────

const ParseInputSchema = z
  .object({
    yaml: z.string().optional().describe("Raw disk-shape YAML string from a `nodes/*.yml` file."),
    diskNode: z
      .record(z.unknown())
      .optional()
      .describe("Parsed disk-shape node object. Provide instead of `yaml` if already parsed."),
  })
  .refine((data) => data.yaml !== undefined || data.diskNode !== undefined, {
    message: "Provide either `yaml` or `diskNode`.",
  });

function defineParseDiskNodeToWorkspaceBody(client: CoalesceClient): ToolDefinition {
  // The client is unused (this tool is a pure converter), but keeping the
  // signature uniform with the rest of the file makes the registration
  // boilerplate in defineRenderNodeTools symmetric.
  void client;
  const name = "parse_disk_node_to_workspace_body";
  return [
    name,
    {
      title: "Parse Disk Node YAML to Workspace Body",
      description:
        "Convert an on-disk coa node YAML (the `nodes/*.yml` format) into a cloud " +
        "workspace-node body suitable for `set_workspace_node` or `update_workspace_node`. " +
        "Returns the cloud-shape object.\n\n" +
        "Use this when local-dev edits to a YAML node need to be pushed back to a cloud " +
        "workspace without manually translating the schema (operation: → flat top-level, " +
        "stepCounter/columnCounter → nodeID/columnID, sourceColumnReferences[] → sources[]).\n\n" +
        "For nodes that originated in the cloud, UUIDs round-trip and the cloud will " +
        "treat the result as an idempotent update. For nodes authored from scratch on disk, " +
        "the UUIDs are local-only — use `create_workspace_node_from_predecessor` or " +
        "`create_workspace_node_from_scratch` instead of pushing this body via set/update.\n\n" +
        "Provide one of:\n" +
        "  - `yaml`: raw YAML string (will be parsed)\n" +
        "  - `diskNode`: pre-parsed disk-shape object",
      inputSchema: ParseInputSchema,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    (async (params: z.infer<typeof ParseInputSchema>) => {
      try {
        let diskNode: Record<string, unknown>;
        if (params.diskNode) {
          diskNode = params.diskNode;
        } else if (params.yaml) {
          const parsed = YAML.parse(params.yaml);
          if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
            throw new Error("YAML did not parse to an object — does the input start with a node document?");
          }
          diskNode = parsed as Record<string, unknown>;
        } else {
          throw new Error("Provide either `yaml` or `diskNode`.");
        }
        const cloudBody = diskNodeToCloud(diskNode);
        return buildJsonToolResponse(name, {
          cloudBody,
          nodeID: typeof cloudBody.id === "string" ? cloudBody.id : undefined,
          name: typeof cloudBody.name === "string" ? cloudBody.name : undefined,
        });
      } catch (err) {
        return handleToolError(err);
      }
    }),
  ];
}

// ── apply_sql_to_workspace_node ───────────────────────────────────────────────

const ApplySqlInputSchema = z.object({
  workspaceID: z.string().describe("The workspace ID containing the node."),
  nodeID: z.string().describe("The node ID to update."),
  sql: z
    .string()
    .min(1)
    .describe(
      "The edited SQL to apply. For v1 this should be a CREATE [OR REPLACE] TABLE statement — "
        + "the same shape produced by `coa create --dry-run`. The tool extracts the column list "
        + "and diffs it against the node's current metadata.columns[].",
    ),
  dryRun: z
    .boolean()
    .optional()
    .default(false)
    .describe(
      "When true, returns the diff and updated metadata.columns[] without writing to the workspace. "
        + "Useful for previewing what would change before pushing.",
    ),
});

function defineApplySqlToWorkspaceNode(client: CoalesceClient): ToolDefinition {
  const name = "apply_sql_to_workspace_node";
  return [
    name,
    {
      title: "Apply SQL Edits to Workspace Node",
      description:
        "Round-trip a user's edits on a rendered DDL/DML SQL document back to the cloud "
        + "workspace node it came from. Parses the SQL's column list, diffs it against the "
        + "node's current metadata.columns[], and updates the node via set_workspace_node "
        + "(unless dryRun: true).\n\n"
        + "**v1 scope is column-level only**:\n"
        + "  - Removed columns (in YAML, missing in SQL) → dropped\n"
        + "  - Type changes (matching name, different dataType) → updated in place; existing\n"
        + "    sources / columnReferences / lineage metadata is preserved\n"
        + "  - New columns in the SQL → REJECTED (a new column needs a source mapping that\n"
        + "    the SQL alone doesn't carry — edit the YAML or use the cloud UI to add columns)\n\n"
        + "Structural changes (joins, CTEs, WHERE/GROUP BY edits, custom expressions) are not "
        + "reverse-engineered out of edited SQL. Those parts of the node are owned by the YAML "
        + "and not safely inferrable from the rendered output.\n\n"
        + "Args:\n"
        + "  - workspaceID (string, required)\n"
        + "  - nodeID (string, required)\n"
        + "  - sql (string, required): A CREATE [OR REPLACE] TABLE statement. v1 doesn't yet\n"
        + "    handle SELECT/INSERT shapes — pass the DDL output, not DML.\n"
        + "  - dryRun (boolean, optional): If true, return the diff without writing.\n\n"
        + "Returns: { applied, dryRun, diff: { unchanged, typeChanged, removed, added }, body? }",
      inputSchema: ApplySqlInputSchema,
      annotations: WRITE_ANNOTATIONS,
    },
    (async (params: z.infer<typeof ApplySqlInputSchema>) => {
      try {
        const parsed = parseCreateTableColumns(params.sql);
        if (!parsed) {
          throw new Error(
            "Could not parse a CREATE TABLE column list out of the SQL. v1 only handles "
              + "DDL-shaped input (the output of coa create --dry-run). For SELECT/INSERT shapes "
              + "or structural edits, edit the node YAML directly.",
          );
        }

        const current = await getWorkspaceNode(client, {
          workspaceID: params.workspaceID,
          nodeID: params.nodeID,
        });
        if (!isPlainObject(current)) {
          throw new Error(
            `get_workspace_node returned ${typeof current}, expected an object`,
          );
        }
        const metadata = isPlainObject(current.metadata) ? current.metadata : {};
        const existingColumns = Array.isArray(metadata.columns) ? metadata.columns : [];

        const diff = diffColumns(parsed, existingColumns);

        if (diff.added.length > 0) {
          // Surface the failure as structured data instead of throwing — the
          // caller may want to render the diff to the user before deciding
          // what to do (e.g., prompt them to edit YAML for the adds).
          return buildJsonToolResponse(name, {
            applied: false,
            dryRun: params.dryRun ?? false,
            diff,
            error:
              `Cannot apply: ${diff.added.length} new column(s) (${diff.added.map((a) => a.name).join(", ")}) `
              + "appear in the SQL but not on the node. Adding columns from edited SQL is not supported "
              + "(no source mapping). Edit the YAML or add the columns via the cloud UI first.",
          });
        }

        if (diff.typeChanged.length === 0 && diff.removed.length === 0) {
          return buildJsonToolResponse(name, {
            applied: false,
            dryRun: params.dryRun ?? false,
            diff,
            message: "No changes to apply — the SQL's columns match the node's current metadata.",
          });
        }

        const updatedColumns = applyColumnDiff(parsed, existingColumns, diff);
        const updatedMetadata = { ...metadata, columns: updatedColumns };
        const body = { ...current, metadata: updatedMetadata };

        if (params.dryRun) {
          return buildJsonToolResponse(name, {
            applied: false,
            dryRun: true,
            diff,
            body,
          });
        }

        await setWorkspaceNodeAndInvalidate(client, {
          workspaceID: params.workspaceID,
          nodeID: params.nodeID,
          body,
        });

        return buildJsonToolResponse(name, {
          applied: true,
          dryRun: false,
          diff,
        });
      } catch (err) {
        return handleToolError(err);
      }
    }),
  ];
}

// ── helpers ────────────────────────────────────────────────────────────────────

function buildSuggestedFilename(diskNode: Record<string, unknown>): string | undefined {
  const operation = diskNode.operation as Record<string, unknown> | undefined;
  const location = typeof operation?.locationName === "string" ? operation.locationName : undefined;
  const name = typeof diskNode.name === "string" ? diskNode.name : undefined;
  if (!name) { return undefined; }
  // coa convention: nodes/<LocationName>-<NodeName>.yml
  return location ? `${location}-${name}.yml` : `${name}.yml`;
}
