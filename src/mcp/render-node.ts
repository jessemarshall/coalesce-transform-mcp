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
import { getCachedOrFetchWorkspaceNodeDetail } from "../services/cache/workspace-node-detail-index.js";
import { setWorkspaceNodeAndInvalidate } from "../services/workspace/mutations.js";
import {
  cloudNodeToDisk,
  diskNodeToCloud,
} from "../services/templates/node-shape-bridge.js";
import {
  applyColumnDiff,
  diffColumns,
  isMergeShape,
  normalizeColumnKey,
  parseCreateTableColumns,
  parseSelectColumnsForApply,
  type ParsedSqlColumn,
} from "../services/templates/sql-column-diff.js";
import {
  inferColumnFromAddedItem,
  type InferredColumn,
  type ResolvedColumnSource,
} from "../services/templates/infer-source-mapping.js";
import {
  diffFromBlock,
  type FromBlockDiff,
} from "../services/templates/from-block-diff.js";
import {
  diffTailClauses,
  type TailClausesDiff,
} from "../services/templates/tail-clauses-diff.js";
import {
  appendLimitToJoinCondition,
  diffLimit,
  stripTrailingLimit,
  type LimitDiff,
} from "../services/templates/limit-diff.js";
import {
  diffInsertHeader,
  type InsertHeaderDiff,
} from "../services/templates/insert-header-diff.js";
import { isPlainObject } from "../utils.js";

/**
 * Tools that bridge the two Coalesce workspace-node serializations:
 *
 *   - **Cloud shape**: returned by `get_workspace_node`, accepted by `set_workspace_node`.
 *     Flat top-level fields, `metadata.columns[].sources[].columnReferences[].nodeID/columnID`.
 *   - **Disk YAML shape**: read from `nodes/*.yml` by coa. Wraps under `operation:`,
 *     uses `columnReference.{columnCounter,stepCounter}` and `sourceColumnReferences[]`.
 *
 * These tools are pure converters / writers and don't need `server` for
 * elicitation, but the registration call site passes it for signature
 * symmetry with the rest of the `define*Tools` family.
 */
export function defineRenderNodeTools(
  _server: McpServer,
  client: CoalesceClient,
): ToolDefinition[] {
  return [
    defineSerializeWorkspaceNodeToDiskYaml(client),
    defineParseDiskNodeToWorkspaceBody(client),
    defineApplySqlToWorkspaceNode(client),
  ];
}

// ── serialize_workspace_node_to_disk_yaml ─────────────────────────────────────

const SerializeInputSchema = z.object({
  workspaceID: z
    .string()
    .min(1, "workspaceID must not be empty")
    .describe("The workspace ID containing the node."),
  nodeID: z
    .string()
    .min(1, "nodeID must not be empty")
    .describe("The node ID to serialize."),
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
    yaml: z
      .string()
      .min(1, "yaml must not be empty when provided")
      .optional()
      .describe("Raw disk-shape YAML string from a `nodes/*.yml` file."),
    diskNode: z
      .record(z.unknown())
      .refine((d) => Object.keys(d).length > 0, {
        message: "diskNode must not be an empty object when provided",
      })
      .optional()
      .describe("Parsed disk-shape node object. Provide instead of `yaml` if already parsed."),
  })
  .refine((data) => data.yaml !== undefined || data.diskNode !== undefined, {
    message: "Provide either `yaml` or `diskNode`.",
  });

function defineParseDiskNodeToWorkspaceBody(_client: CoalesceClient): ToolDefinition {
  // The client is unused (this tool is a pure converter); keeping the
  // signature uniform with the rest of the file makes the registration
  // boilerplate in defineRenderNodeTools symmetric.
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
  workspaceID: z
    .string()
    .min(1, "workspaceID must not be empty")
    .describe("The workspace ID containing the node."),
  nodeID: z
    .string()
    .min(1, "nodeID must not be empty")
    .describe("The node ID to update."),
  sql: z
    .string()
    .min(1)
    .describe(
      "The edited SQL to apply. Accepts either: (a) a CREATE [OR REPLACE] TABLE statement "
        + "(`coa_dry_run_create` output) — name + dataType per column, no transforms; or "
        + "(b) a SELECT/INSERT/MERGE statement (`coa_dry_run_run` output) — name + expression "
        + "per column, dataType inferred from the expression. For MERGE shapes (Coalesce "
        + "dim / Type-2 SCD nodes) only the inner SELECT inside USING (...) is editable; the "
        + "MERGE envelope (target, ON, WHEN MATCHED / WHEN NOT MATCHED) is Coalesce-managed and "
        + "ignored by the apply path. The tool extracts the column list and diffs it against "
        + "the node's current metadata.columns[].",
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
        + "(unless dryRun: true). Both DDL and DML inputs feed the same node metadata — a "
        + "DDL edit and a DML edit on the same node update the same `metadata.columns[]`.\n\n"
        + "**Column-level diff** (structural changes — joins, CTEs, WHERE/GROUP BY, custom "
        + "expressions on the join itself — are NOT reverse-engineered; those live in the "
        + "node YAML):\n"
        + "  - Removed columns (in YAML, missing in SQL) → dropped\n"
        + "  - Type changes (matching name, different dataType) → updated in place; existing\n"
        + "    sources / columnReferences / lineage metadata is preserved\n"
        + "  - Renamed columns (DML only — same transform, new AS alias) → renamed in\n"
        + "    place, lineage preserved (column id stays stable so downstream refs keep\n"
        + "    working). Detection requires unambiguous transform pairing; ambiguous\n"
        + "    cases fall back to drop+add.\n"
        + "  - Expression-changed columns (DML only — same name, edited transform) →\n"
        + "    `sources[].transform` is updated and source-mapping inference is re-run\n"
        + "    to refresh `columnReferences` if the new transform references different\n"
        + "    upstream columns.\n"
        + "  - New columns → for DML inputs, source-mapping inference resolves the\n"
        + "    column's `transform` against upstream predecessors (alias-prefix match,\n"
        + "    all-pred name fallback, multi-source COALESCE/GREATEST). For DDL inputs,\n"
        + "    new columns are added with empty `columnReferences[]` (DDL doesn't carry\n"
        + "    transforms); fill them in via the cloud UI.\n\n"
        + "**Block-level diff** (DML only — single-sourceMapping nodes):\n"
        + "  - WHERE / ON-condition edits → written to `sourceMapping[0].join.joinCondition`\n"
        + "    with table refs rewritten to `{{ ref('LOC', 'NAME') }}` form.\n"
        + "  - Adding or removing a predecessor source via SQL is REJECTED — add/remove\n"
        + "    the predecessor in the cloud UI first, then re-render.\n\n"
        + "**Tail-clause diff** (DML only):\n"
        + "  - GROUP BY presence toggle → flips `config.groupByAll`. Coalesce derives\n"
        + "    the actual GROUP BY column list from non-aggregate columns at render time.\n"
        + "  - ORDER BY adds / changes / removes → `config.orderby` flag and\n"
        + "    `config.orderbycolumn.items[]` array of `{sortColName, sortOrder}`.\n"
        + "  - LIMIT adds / changes / removes → appended to / stripped from the\n"
        + "    trailing position of `sourceMapping[0].join.joinCondition`. Coalesce\n"
        + "    has no native limit config field for typical SQL nodes; the apply\n"
        + "    path warns when groupByAll/orderby is set (the auto-rendered tail\n"
        + "    clauses would land AFTER LIMIT, producing invalid SQL).\n\n"
        + "**INSERT header diff** (DML only — INSERT-shaped inputs):\n"
        + "  - Target identifier change (`INSERT INTO \"DB\".\"LOC\".\"NAME\"`) → updates\n"
        + "    the cloud node's top-level `database` / `locationName` / `name` fields.\n"
        + "    Renames/relocates the node; downstream references are unaffected (they\n"
        + "    use the node id, not the name).\n"
        + "  - INSERT column list out-of-sync with the SELECT projection → REJECTED.\n"
        + "    Coalesce auto-generates the INSERT list from `metadata.columns[]`; edit\n"
        + "    the SELECT (using AS aliases) to add/rename/reorder columns.\n\n"
        + "Args:\n"
        + "  - workspaceID (string, required)\n"
        + "  - nodeID (string, required)\n"
        + "  - sql (string, required): Either a CREATE [OR REPLACE] TABLE statement (DDL,\n"
        + "    from coa_dry_run_create) OR a SELECT/INSERT statement (DML, from\n"
        + "    coa_dry_run_run). The shape is auto-detected.\n"
        + "  - dryRun (boolean, optional): If true, return the diff without writing.\n\n"
        + "Returns: { applied, dryRun, inputKind, "
        + "diff: { unchanged, typeChanged, renamed, expressionChanged, removed, added }, "
        + "joinDiff: { kind: 'identical' | 'whereOrJoinEdit' | 'newSource' | 'removedSource' | 'unsupported', ... }, "
        + "tailDiff: { groupBy: { kind: 'identical' | 'added' | 'removed', ... }, orderBy: { kind: 'identical' | 'added' | 'changed' | 'removed', ... } }, "
        + "limitDiff: { kind: 'identical' | 'added' | 'changed' | 'removed' | 'unsupported', ... }, "
        + "insertHeaderDiff: { kind: 'identical' | 'targetChanged' | 'columnListMismatch' | 'notApplicable', ... }, "
        + "body?, warnings?, error? }",
      inputSchema: ApplySqlInputSchema,
      annotations: WRITE_ANNOTATIONS,
    },
    (async (params: z.infer<typeof ApplySqlInputSchema>) => {
      try {
        // Auto-detect input shape. CREATE TABLE → name+dataType (no
        // transforms; can't synthesize lineage for adds). SELECT/INSERT → name
        // + expression (drives inference for adds, dataType inferred via
        // join-helpers.inferDatatype). Either flows back into the same
        // metadata.columns[] — a DDL edit and a DML edit on the same node
        // produce the same write.
        //
        // Lock the kind by inspecting the leading verb FIRST. Without this,
        // a malformed CREATE TABLE that returns undefined from
        // parseCreateTableColumns would silently fall through to the DML
        // parser (which can opportunistically extract a column list out of
        // a `CREATE TABLE ... AS SELECT ...` clause), and we'd run
        // source-mapping inference against an upstream graph the user
        // never intended.
        const sqlPreamble = params.sql.replace(/^(?:\s|--[^\n]*\n|\/\*[\s\S]*?\*\/)*/, "");
        const isDdl = /^create\s+/i.test(sqlPreamble);
        const isDml = /^(?:insert|merge|select|with|\()\s*/i.test(sqlPreamble);
        // MERGE shapes (Coalesce dim / Type-2 SCD) flow through the same
        // column-level diff path as INSERT/SELECT — but the structural
        // diffs (joinDiff, tailDiff, limitDiff, insertHeaderDiff) don't
        // apply: the MERGE envelope is Coalesce-managed, not user-editable.
        // Track the shape so we can skip those branches below.
        const isMerge = isDml && isMergeShape(params.sql);
        let parsed: ParsedSqlColumn[] | undefined;
        let inputKind: "ddl" | "dml";
        if (isDdl) {
          parsed = parseCreateTableColumns(params.sql);
          inputKind = "ddl";
          if (!parsed) {
            throw new Error(
              "SQL starts with CREATE but no column list could be extracted. Pass the full "
                + "CREATE [OR REPLACE] TABLE statement (the output of coa_dry_run_create).",
            );
          }
        } else if (isDml) {
          parsed = parseSelectColumnsForApply(params.sql);
          inputKind = "dml";
          if (!parsed) {
            throw new Error(
              isMerge
                ? "SQL is a MERGE but no SELECT-list could be extracted from the USING (…) "
                  + "clause. Pass the full MERGE statement (the output of coa_dry_run_run on a "
                  + "dim / Type-2 SCD node)."
                : "SQL appears to be DML but no SELECT-list could be extracted. Pass the full "
                  + "INSERT/SELECT statement (the output of coa_dry_run_run).",
            );
          }
        } else {
          throw new Error(
            "Could not classify the SQL. Expected either a CREATE TABLE statement "
              + "(coa_dry_run_create output) or a SELECT/INSERT/MERGE statement "
              + "(coa_dry_run_run output).",
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

        // Source-mapping inference is needed for two diff buckets:
        //   - `added` columns (build sources[] from scratch)
        //   - `expressionChanged` columns (re-derive sources[] from the
        //     new transform — mirrors how migration-agents column_builder
        //     calls `resolve_column_sources` with `auto_sources: existing.sources`
        //     after a transform update).
        // Both need the same `aliases` + `predColLookup` setup, so build
        // it once and share. Only fetched for DML inputs — DDL has no
        // transforms to feed the inference.
        const addedColumnsByName = new Map<string, InferredColumn["column"]>();
        const updatedSourcesByName = new Map<string, ResolvedColumnSource[]>();
        const inferenceWarnings: string[] = [];
        const needsInference =
          inputKind === "dml"
          && (diff.added.length > 0 || diff.expressionChanged.length > 0);

        let aliasToUpstreamNodeID: Record<string, string> = {};
        let predColLookup: Record<string, Record<string, string>> = {};
        let defaultPredID: string | null = null;
        if (needsInference) {
          aliasToUpstreamNodeID = extractSourceMappingAliases(metadata);
          const upstreamNodeIDs = new Set(Object.values(aliasToUpstreamNodeID));
          if (upstreamNodeIDs.size > 0) {
            // Fetch each predecessor once to build (nodeID → name → columnID).
            // Tolerate per-pred fetch failures: a single inaccessible upstream
            // shouldn't sink the whole apply — we just lose lineage for refs
            // that point at it (they fall through to bare-column adds).
            //
            // Use allSettled + iteration-order push so the resulting warnings
            // array is deterministic regardless of network completion order.
            // A flaky network shouldn't shuffle response payloads (downstream
            // CI snapshot tests / payload diffing rely on stable ordering).
            const predIDs = Array.from(upstreamNodeIDs);
            const settled = await Promise.allSettled(
              predIDs.map((predID) =>
                getCachedOrFetchWorkspaceNodeDetail(
                  client,
                  params.workspaceID,
                  predID,
                ),
              ),
            );
            for (let i = 0; i < predIDs.length; i++) {
              const result = settled[i];
              const predID = predIDs[i];
              if (result.status === "fulfilled") {
                predColLookup[predID] = indexUpstreamColumns(result.value);
              } else {
                const reason = result.reason;
                inferenceWarnings.push(
                  `Could not fetch predecessor node ${predID}: `
                  + `${reason instanceof Error ? reason.message : String(reason)}`,
                );
              }
            }
          }
          // `defaultPredID` follows the migration-agents convention: the
          // first alias in `metadata.sourceMapping[0].aliases` wins. This
          // is what `extractSourceMappingAliases` produces (insertion-order
          // iteration over each entry's aliases dict, in array order). It
          // matters when an expression has no alias prefix and resolves
          // via the all-pred name fallback (`resolve_column_sources`
          // strategy 3) — reordering sourceMapping entries can change which
          // upstream wins. Document explicitly so a future cleanup of the
          // helper doesn't accidentally change this contract.
          defaultPredID = Object.values(aliasToUpstreamNodeID)[0] ?? null;
        }

        // Adds: build full cloud-shape entries (name + dataType + sources).
        // For DDL inputs (no expression), `inferColumnFromAddedItem` falls
        // back to a bare column with empty sources.
        for (const added of diff.added) {
          const transform = added.expression ?? "";
          const inferred = inferColumnFromAddedItem(
            { name: added.name, dataType: added.dataType, transform },
            { smAliases: aliasToUpstreamNodeID, predColLookup, defaultPredID },
          );
          if (!inferred.resolved && transform) {
            inferenceWarnings.push(
              `Could not infer lineage for new column ${added.name}; writing it with empty `
              + `columnReferences (you can fill them in via the cloud UI).`,
            );
          }
          addedColumnsByName.set(normalizeColumnKey(added.name), inferred.column);
        }

        // Expression changes: re-derive sources[] for each. We pass the
        // NEW transform through the same inference path so any change in
        // referenced upstream columns gets new `columnReferences`. If
        // inference fails, applyColumnDiff falls back to patching just
        // the transform on the existing sources entry (lineage refs may
        // be stale; surfaced via inferenceWarnings).
        for (const change of diff.expressionChanged) {
          const inferred = inferColumnFromAddedItem(
            // dataType doesn't matter here — we only consume `sources`.
            { name: change.name, dataType: "", transform: change.to },
            { smAliases: aliasToUpstreamNodeID, predColLookup, defaultPredID },
          );
          if (!inferred.resolved) {
            inferenceWarnings.push(
              `Edited expression on ${change.name} did not resolve to any upstream column; `
              + `the transform was updated but columnReferences may be stale.`,
            );
          }
          updatedSourcesByName.set(normalizeColumnKey(change.name), inferred.column.sources);
        }

        // Iteration 2 + 3: detect and apply edits to the FROM/JOIN/WHERE
        // block AND the GROUP BY / ORDER BY tail. Both run for DML
        // inputs; DDL has neither so they stay identical.
        //
        // Tail-clauses are computed up front so they're available to all
        // downstream branches (including the joinDiff rejection path —
        // the user sees what they tried to change, even when the apply
        // can't proceed).
        let joinDiff: FromBlockDiff = { kind: "identical" };
        let tailDiff: TailClausesDiff = {
          groupBy: { kind: "identical" },
          orderBy: { kind: "identical" },
          unsupportedOrderByExpressions: [],
        };
        let limitDiff: LimitDiff = { kind: "identical" };
        let insertHeaderDiff: InsertHeaderDiff = { kind: "notApplicable" };
        let updatedSourceMapping: unknown[] | undefined;
        let updatedConfig: Record<string, unknown> | undefined;
        let updatedTopLevel: Record<string, unknown> | undefined;
        if (inputKind === "dml" && !isMerge) {
          // MERGE shapes (Coalesce dim / Type-2 SCD) skip structural diffs:
          // the MERGE envelope (USING, ON, WHEN MATCHED/NOT MATCHED) is
          // Coalesce-managed and not user-editable. Only column-level edits
          // inside the inner SELECT round-trip via the column diff above.
          //
          // Compute the tail diff first so the joinDiff rejection branch
          // (a few lines below) can include it in the response.
          const existingConfig = isPlainObject(current.config)
            ? current.config as Record<string, unknown>
            : {};
          tailDiff = diffTailClauses(params.sql, {
            groupByAll: existingConfig.groupByAll === true,
            orderby: existingConfig.orderby === true,
            orderbycolumn: isPlainObject(existingConfig.orderbycolumn)
              ? { items: Array.isArray(existingConfig.orderbycolumn.items)
                  ? existingConfig.orderbycolumn.items as Array<Record<string, unknown>>
                  : undefined }
              : undefined,
          });
          // Surface ORDER BY expression warnings — the user wrote
          // `ORDER BY COALESCE(a, 0) DESC` or similar; Coalesce's
          // orderbycolumn schema doesn't model expressions so we drop
          // those items but tell the user.
          for (const expr of tailDiff.unsupportedOrderByExpressions) {
            inferenceWarnings.push(
              `ORDER BY item "${expr}" is an expression; Coalesce's orderbycolumn `
              + `only models bare column names. The item was dropped — express the `
              + `ordering via a derived column on the node instead.`,
            );
          }
          if (tailDiff.groupBy.kind !== "identical" || tailDiff.orderBy.kind !== "identical") {
            updatedConfig = { ...existingConfig };

            // Exhaustive switch matches the iteration-2 pattern for
            // joinDiff: adding a new GroupByDiff or OrderByDiff variant
            // produces a compile error here via the `_exhaustive: never`
            // assertion, instead of silently no-op'ing the new branch.
            switch (tailDiff.groupBy.kind) {
              case "identical":
                break;
              case "added":
              case "removed":
                updatedConfig.groupByAll = tailDiff.groupBy.groupByAll;
                break;
              default: {
                const _exhaustive: never = tailDiff.groupBy;
                void _exhaustive;
              }
            }

            // Spread existing orderbycolumn so unrelated keys (any
            // future Coalesce additions like a per-orderbycolumn id)
            // survive — only `items` is touched.
            const existingOrderbycolumn = isPlainObject(existingConfig.orderbycolumn)
              ? existingConfig.orderbycolumn as Record<string, unknown>
              : {};
            switch (tailDiff.orderBy.kind) {
              case "identical":
                break;
              case "added":
              case "changed":
                updatedConfig.orderby = true;
                updatedConfig.orderbycolumn = {
                  ...existingOrderbycolumn,
                  items: tailDiff.orderBy.items,
                };
                break;
              case "removed":
                updatedConfig.orderby = false;
                // Reset items to Coalesce's default-empty placeholder so
                // the cloud doesn't see stale columns under a disabled
                // ORDER BY toggle.
                updatedConfig.orderbycolumn = {
                  ...existingOrderbycolumn,
                  items: [{}],
                };
                break;
              default: {
                const _exhaustive: never = tailDiff.orderBy;
                void _exhaustive;
              }
            }
          }
        }
        if (inputKind === "dml" && !isMerge) {
          const sourceMapping = Array.isArray(metadata.sourceMapping) ? metadata.sourceMapping : [];
          const firstSm = sourceMapping[0];
          const firstJoin = isPlainObject(firstSm) && isPlainObject(firstSm.join) ? firstSm.join : undefined;
          const existingJoinCondition = firstJoin && typeof firstJoin.joinCondition === "string"
            ? firstJoin.joinCondition
            : "";
          if (sourceMapping.length > 1) {
            // Without this warning, the user would see `applied: true`
            // and silently lose any FROM/JOIN/WHERE edits they made on a
            // multi-source node (e.g. UNION-shaped). Surface so they
            // know to re-apply via the cloud UI.
            inferenceWarnings.push(
              `Node has ${sourceMapping.length} sourceMapping entries; FROM/JOIN/WHERE edits `
              + `are not yet supported on multi-source nodes — only column-level changes were applied.`,
            );
          } else if (sourceMapping.length === 1) {
            // Run the diff even when the existing joinCondition is empty
            // — otherwise a node that was created without one (or one
            // that was hand-cleared) would silently swallow the user's
            // FROM/WHERE edits with `applied: true`. Empty existing JC
            // means everything in the user's FROM block is "new
            // content"; the diff path either applies it or rejects on
            // newSource, both of which give the user feedback.
            joinDiff = diffFromBlock(params.sql, existingJoinCondition);
            // Exhaustive switch on the discriminated union — adding a new
            // FromBlockDiff variant (e.g. iteration-4's LIMIT detection)
            // produces a compile error here via the `_exhaustive: never`
            // assertion, instead of silently falling through to a
            // no-op. The "newSource" / "removedSource" branches return
            // early so the surrounding fallthrough flow only sees the
            // remaining kinds.
            switch (joinDiff.kind) {
              case "identical":
                break;
              case "unsupported":
                // Surface the reason as a warning; treat as no-change
                // for the apply path so column edits can still proceed.
                inferenceWarnings.push(joinDiff.reason);
                break;
              case "newSource":
              case "removedSource": {
                // Reject the WHOLE apply (including column edits) when
                // the user's SQL implies a dependency change. Applying
                // column edits in isolation when the user clearly
                // intended broader changes would be confusing — surface
                // the rejection so they can resolve it (add/remove the
                // predecessor manually).
                const verb = joinDiff.kind === "newSource" ? "added" : "removed";
                const list = (joinDiff.kind === "newSource" ? joinDiff.added : joinDiff.removed)
                  .map((s) => `${s.locationName}.${s.nodeName}`).join(", ");
                return buildJsonToolResponse(name, {
                  applied: false,
                  dryRun: params.dryRun ?? false,
                  inputKind,
                  diff,
                  joinDiff,
                  tailDiff,
                  limitDiff,
                  insertHeaderDiff,
                  warnings: inferenceWarnings,
                  error:
                    `Cannot apply: the FROM block ${verb} predecessor source(s) (${list}). `
                    + `Adding or removing predecessors via SQL edit isn't supported yet — `
                    + `${verb === "added" ? "add" : "remove"} the predecessor in the cloud UI first, `
                    + `then re-render and try again.`,
                });
              }
              case "whereOrJoinEdit":
                // Build the updated sourceMapping array — preserves all
                // other entries / fields (aliases, dependencies,
                // customSQL, etc.) and only swaps `join.joinCondition`.
                updatedSourceMapping = [
                  {
                    ...firstSm,
                    join: { ...firstJoin, joinCondition: joinDiff.newJoinCondition },
                  },
                  ...sourceMapping.slice(1),
                ];
                break;
              default: {
                const _exhaustive: never = joinDiff;
                void _exhaustive;
              }
            }

            // Iteration 4: detect and apply LIMIT edits. Coalesce has
            // no native `limit` config field for typical SQL nodes, so
            // we store LIMIT as the trailing clause of joinCondition
            // (Coalesce's renderer copies joinCondition verbatim into
            // the rendered SQL). Coordinated with iteration 2's
            // joinCondition update by composing on top of whichever
            // joinCondition value will be written.
            const baseForLimit = updatedSourceMapping
              ? (() => {
                  const sm = updatedSourceMapping[0];
                  if (!isPlainObject(sm)) { return existingJoinCondition; }
                  const j = isPlainObject(sm.join) ? sm.join : undefined;
                  return j && typeof j.joinCondition === "string" ? j.joinCondition : existingJoinCondition;
                })()
              : existingJoinCondition;
            limitDiff = diffLimit(params.sql, baseForLimit, {
              groupByAll: tailDiff.groupBy.kind === "identical"
                ? (isPlainObject(current.config) && current.config.groupByAll === true)
                : (tailDiff.groupBy.kind === "added"),
              orderby: tailDiff.orderBy.kind === "identical"
                ? (isPlainObject(current.config) && current.config.orderby === true)
                : (tailDiff.orderBy.kind === "added" || tailDiff.orderBy.kind === "changed"),
            });
            switch (limitDiff.kind) {
              case "identical":
                break;
              case "unsupported":
                // OFFSET / `LIMIT N, M` (MySQL row-range) — Coalesce's
                // joinCondition can't round-trip these. Reject the
                // whole apply rather than silently truncating.
                return buildJsonToolResponse(name, {
                  applied: false,
                  dryRun: params.dryRun ?? false,
                  inputKind,
                  diff,
                  joinDiff,
                  tailDiff,
                  limitDiff,
                  insertHeaderDiff,
                  warnings: inferenceWarnings,
                  error: `Cannot apply: ${limitDiff.reason}`,
                });
              case "added":
              case "changed":
                if (limitDiff.warnsClobberByTailClause) {
                  inferenceWarnings.push(
                    `LIMIT was added but the node has GROUP BY / ORDER BY auto-rendering enabled. `
                    + `Coalesce renders those AFTER the joinCondition, so the resulting SQL will be `
                    + `invalid (LIMIT before GROUP BY / ORDER BY). Disable groupByAll/orderby on the `
                    + `node, or move the LIMIT to a sibling Work node.`,
                  );
                }
                updatedSourceMapping = composeJoinConditionUpdate(
                  updatedSourceMapping ?? sourceMapping,
                  appendLimitToJoinCondition(baseForLimit, limitDiff.newLimit),
                );
                break;
              case "removed":
                updatedSourceMapping = composeJoinConditionUpdate(
                  updatedSourceMapping ?? sourceMapping,
                  stripTrailingLimit(baseForLimit),
                );
                break;
              default: {
                const _exhaustive: never = limitDiff;
                void _exhaustive;
              }
            }
          }
        }

        // Iteration 5: detect edits to the INSERT INTO header — target
        // identifier (database/locationName/name) and column-list
        // consistency check. DML-only, and skipped for MERGE shapes (the
        // MERGE INTO target is the same field but the column list isn't
        // present in MERGE syntax).
        if (inputKind === "dml" && !isMerge) {
          const selectColNames = parsed.map((p) => p.name);
          insertHeaderDiff = diffInsertHeader(
            params.sql,
            {
              database: typeof current.database === "string" ? current.database : "",
              locationName: typeof current.locationName === "string" ? current.locationName : "",
              name: typeof current.name === "string" ? current.name : "",
            },
            selectColNames,
          );
          switch (insertHeaderDiff.kind) {
            case "identical":
            case "notApplicable":
              break;
            case "malformedHeader":
              // Surface as a warning so the user knows their header
              // edit was ignored — silently flowing through as
              // notApplicable would mask the issue.
              inferenceWarnings.push(
                `INSERT header could not be parsed: ${insertHeaderDiff.reason} `
                + `Edit was ignored; column-level changes still applied.`,
              );
              break;
            case "columnListMismatch":
              // Reject the whole apply: the user's INSERT column list
              // is out-of-sync with the SELECT, which Coalesce can't
              // round-trip. Surface a clear message pointing at the
              // SELECT as the source of truth.
              return buildJsonToolResponse(name, {
                applied: false,
                dryRun: params.dryRun ?? false,
                inputKind,
                diff,
                joinDiff,
                tailDiff,
                limitDiff,
                insertHeaderDiff,
                warnings: inferenceWarnings,
                error: `Cannot apply: ${insertHeaderDiff.reason}`,
              });
            case "targetChanged":
              // Apply target rename / re-location at the top level.
              // Same shallow-copy pattern as the metadata + config
              // updates below; dependent nodes that reference this
              // node by id are unaffected (the id doesn't change).
              updatedTopLevel = {};
              for (const field of insertHeaderDiff.changedFields) {
                updatedTopLevel[field] = insertHeaderDiff.to[field];
              }
              // Format empty segments as `<empty>` so the user can see
              // when a part was already missing rather than being
              // collapsed silently (e.g. `DB..NAME` vs the misleading
              // `DB.NAME` that .filter(Boolean) would produce).
              {
                const fmt = (id: { database: string; locationName: string; name: string }) =>
                  [id.database, id.locationName, id.name]
                    .map((s) => s === "" ? "<empty>" : s)
                    .join(".");
                inferenceWarnings.push(
                  `INSERT target identifier changed (${insertHeaderDiff.changedFields.join(", ")}): `
                  + `${fmt(insertHeaderDiff.from)} → ${fmt(insertHeaderDiff.to)}. `
                  + `This renames/relocates the node; downstream references are unaffected `
                  + `(they use the node id, not the name).`,
                );
              }
              break;
            default: {
              const _exhaustive: never = insertHeaderDiff;
              void _exhaustive;
            }
          }
        }

        const hasColumnChanges =
          diff.typeChanged.length > 0
          || diff.removed.length > 0
          || diff.added.length > 0
          || diff.renamed.length > 0
          || diff.expressionChanged.length > 0;
        const hasJoinChange = joinDiff.kind === "whereOrJoinEdit";
        const hasTailChange = tailDiff.groupBy.kind !== "identical"
          || tailDiff.orderBy.kind !== "identical";
        const hasLimitChange = limitDiff.kind !== "identical";
        const hasInsertHeaderChange = insertHeaderDiff.kind === "targetChanged";

        if (
          !hasColumnChanges
          && !hasJoinChange
          && !hasTailChange
          && !hasLimitChange
          && !hasInsertHeaderChange
        ) {
          return buildJsonToolResponse(name, {
            applied: false,
            dryRun: params.dryRun ?? false,
            inputKind,
            diff,
            joinDiff,
            tailDiff,
            limitDiff,
            insertHeaderDiff,
            // Always include `warnings` (even when empty) so the response
            // shape is consistent across all three branches — downstream
            // consumers reading `response.warnings` get a stable
            // never-undefined array.
            warnings: inferenceWarnings,
            message: "No changes to apply — the SQL matches the node's current metadata.",
          });
        }

        // Apply column-level changes. For pure joinCondition / tail-clause
        // edits (no column changes) this is a no-op map of existing →
        // existing, but we still call applyColumnDiff so the column array
        // is rebuilt with parsed-SQL ordering when available.
        const updatedColumns = hasColumnChanges
          ? applyColumnDiff(parsed, existingColumns, diff, {
            addedColumnsByName,
            updatedSourcesByName,
          })
          : existingColumns;
        const updatedMetadata: Record<string, unknown> = { ...metadata, columns: updatedColumns };
        if (updatedSourceMapping) {
          updatedMetadata.sourceMapping = updatedSourceMapping;
        }
        const body: Record<string, unknown> = { ...current, metadata: updatedMetadata };
        if (updatedConfig) {
          body.config = updatedConfig;
        }
        if (updatedTopLevel) {
          // Apply iteration-5's database/locationName/name overrides
          // last so they take precedence over the spread-from-current.
          for (const [k, v] of Object.entries(updatedTopLevel)) {
            body[k] = v;
          }
        }

        if (params.dryRun) {
          return buildJsonToolResponse(name, {
            applied: false,
            dryRun: true,
            inputKind,
            diff,
            joinDiff,
            tailDiff,
            limitDiff,
            insertHeaderDiff,
            body,
            warnings: inferenceWarnings,
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
          inputKind,
          diff,
          joinDiff,
          tailDiff,
          limitDiff,
          insertHeaderDiff,
          warnings: inferenceWarnings,
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

/**
 * Compose a sourceMapping array update where the first entry's
 * `join.joinCondition` is replaced with `newJoinCondition`. Used by
 * iteration-4 LIMIT writes to compose on top of (potentially) iteration-2's
 * already-updated sourceMapping. Preserves all other entries / fields.
 */
function composeJoinConditionUpdate(
  sourceMapping: unknown[],
  newJoinCondition: string,
): unknown[] {
  const first = sourceMapping[0];
  const firstObj = isPlainObject(first) ? first : {};
  const firstJoin = isPlainObject(firstObj.join) ? firstObj.join : {};
  return [
    {
      ...firstObj,
      join: { ...firstJoin, joinCondition: newJoinCondition },
    },
    ...sourceMapping.slice(1),
  ];
}

/**
 * Flatten the cloud node's `metadata.sourceMapping[].aliases` blocks into a
 * single `alias → upstream nodeID` map. Multiple sourceMapping entries are
 * merged; collisions take the last-write-wins (rare in practice — collisions
 * mean the same alias points at different upstreams in different mappings).
 */
function extractSourceMappingAliases(
  metadata: Record<string, unknown>,
): Record<string, string> {
  const out: Record<string, string> = {};
  const sourceMapping = metadata.sourceMapping;
  if (!Array.isArray(sourceMapping)) { return out; }
  for (const entry of sourceMapping) {
    if (!isPlainObject(entry)) { continue; }
    const aliases = entry.aliases;
    if (!isPlainObject(aliases)) { continue; }
    for (const [k, v] of Object.entries(aliases)) {
      if (typeof v === "string" && v) { out[k] = v; }
    }
  }
  return out;
}

/**
 * Build a `name → columnID` lookup from a cloud node body, used to resolve
 * upstream column references during source-mapping inference. Tolerant of
 * the various shapes a fetched node body can take (cloud uses
 * `metadata.columns[].id`; the bridge sometimes hands back disk-shaped
 * `columnReference.columnCounter`).
 */
function indexUpstreamColumns(node: unknown): Record<string, string> {
  const out: Record<string, string> = {};
  if (!isPlainObject(node)) { return out; }
  const metadata = isPlainObject(node.metadata) ? node.metadata : {};
  const columns = Array.isArray(metadata.columns) ? metadata.columns : [];
  for (const col of columns) {
    if (!isPlainObject(col)) { continue; }
    const colName = typeof col.name === "string" ? col.name : undefined;
    if (!colName) { continue; }
    const id =
      typeof col.id === "string" ? col.id
      : typeof col.columnID === "string" ? col.columnID
      : isPlainObject(col.columnReference) && typeof col.columnReference.columnCounter === "string"
        ? col.columnReference.columnCounter
        : undefined;
    if (id) { out[colName.toUpperCase()] = id; }
  }
  return out;
}
