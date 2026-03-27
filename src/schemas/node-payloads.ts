import { z } from "zod";

export const StorageLocationInputSchema = z
  .object({
    locationName: z
      .string()
      .optional()
      .describe("Common storage location field used in hydrated node bodies."),
    name: z
      .string()
      .optional()
      .describe("Alternative storage location name field used by some node types."),
    database: z.string().optional().describe("Optional database for the storage location."),
    schema: z.string().optional().describe("Optional schema for the storage location."),
  })
  .passthrough();

const ColumnReferenceInputSchema = z
  .object({
    nodeID: z.string().optional(),
    columnID: z.string().optional(),
    columnName: z.string().optional(),
    stepCounter: z.string().optional(),
    columnCounter: z.string().optional(),
  })
  .passthrough();

const ColumnSourceInputSchema = z
  .object({
    name: z.string().optional(),
    transform: z.string().optional(),
    columnReferences: z.array(ColumnReferenceInputSchema).optional(),
  })
  .passthrough();

export const WorkspaceNodeColumnInputSchema = z
  .object({
    name: z.string().min(1).describe("Column name."),
    transform: z
      .string()
      .optional()
      .describe("Optional SQL expression or source reference for the column."),
    dataType: z.string().optional().describe("Optional Coalesce data type."),
    description: z.string().optional().describe("Optional column description."),
    nullable: z.boolean().optional().describe("Optional nullability flag."),
    columnID: z
      .string()
      .optional()
      .describe("Existing Coalesce column ID when preserving hydrated metadata."),
    columnReference: ColumnReferenceInputSchema
      .optional()
      .describe("Optional lineage reference preserved from hydrated node bodies."),
    sources: z
      .array(ColumnSourceInputSchema)
      .optional()
      .describe("Optional lineage/source entries used by hydrated metadata."),
    placement: z
      .union([z.string(), z.number()])
      .optional()
      .describe("Optional placement/order metadata."),
  })
  .passthrough();

const SourceMappingDependencyInputSchema = z
  .object({
    locationName: z.string().optional(),
    nodeName: z.string().optional(),
    nodeID: z.string().optional(),
  })
  .passthrough();

const SourceMappingJoinInputSchema = z
  .object({
    joinCondition: z
      .string()
      .optional()
      .describe("Optional FROM/JOIN/WHERE clause stored on the mapping entry."),
  })
  .passthrough();

const SourceMappingCustomSqlInputSchema = z
  .object({
    customSQL: z.string().optional(),
  })
  .passthrough();

const SourceMappingNoLinkRefInputSchema = z
  .union([z.string(), z.object({}).passthrough()]);

const SourceMappingInputSchema = z
  .object({
    name: z.string().optional(),
    aliases: z
      .record(z.string())
      .optional()
      .describe("Optional alias-to-node lookup map used by hydrated source mappings."),
    dependencies: z
      .array(SourceMappingDependencyInputSchema)
      .optional()
      .describe("Optional predecessor dependency records."),
    join: SourceMappingJoinInputSchema.optional(),
    customSQL: SourceMappingCustomSqlInputSchema.optional(),
    noLinkRefs: z.array(SourceMappingNoLinkRefInputSchema).optional(),
  })
  .passthrough();

export const WorkspaceNodeMetadataInputSchema = z
  .object({
    columns: z
      .array(WorkspaceNodeColumnInputSchema)
      .optional()
      .describe("Hydrated mapping-grid columns for the node."),
    sourceMapping: z
      .array(SourceMappingInputSchema)
      .optional()
      .describe("Hydrated source/join mapping entries for the node."),
    enabledColumnTestIDs: z
      .array(z.string())
      .optional()
      .describe("Enabled column test IDs preserved by the Coalesce PUT API."),
  })
  .passthrough();

export const NodeConfigInputSchema = z
  .object({
    preSQL: z.string().optional().describe("Optional pre-SQL hook."),
    postSQL: z.string().optional().describe("Optional post-SQL hook."),
    testsEnabled: z.boolean().optional().describe("Enable or disable node tests."),
    materializationType: z
      .string()
      .optional()
      .describe("Optional materialization type such as table or view."),
    insertStrategy: z
      .string()
      .optional()
      .describe("Optional insert strategy for nodes that support it."),
    truncateBefore: z
      .boolean()
      .optional()
      .describe("Optional truncate-before-load toggle."),
  })
  .passthrough();

export const ExternalColumnSchema = z.object({
  name: z.string().min(1).describe("Column name as it appears in the external table."),
  dataType: z.string().min(1).describe("Exact data type from the external system (e.g., NUMBER(38,0), VARCHAR(16777216), TIMESTAMP_NTZ(9))."),
  nullable: z.boolean().optional().describe("Whether the column is nullable. Defaults to true."),
  description: z.string().optional().describe("Optional column description."),
  transform: z
    .string()
    .optional()
    .describe("Optional SQL transform expression. When omitted, the tool auto-maps from a matching predecessor column by name. Columns with no match and no transform are flagged as needing one."),
});

export type ExternalColumnInput = z.infer<typeof ExternalColumnSchema>;

export const WorkspaceNodeWriteInputSchema = z
  .object({
    id: z.string().optional(),
    name: z.string().optional(),
    description: z.string().optional(),
    nodeType: z.string().optional(),
    table: z.string().optional(),
    database: z.string().optional(),
    schema: z.string().optional(),
    locationName: z.string().optional(),
    materializationType: z.string().optional(),
    storageLocations: z.array(StorageLocationInputSchema).optional(),
    config: NodeConfigInputSchema.optional(),
    metadata: WorkspaceNodeMetadataInputSchema.optional(),
  })
  .passthrough();
