/**
 * Bidirectional converter between the two Coalesce workspace-node serializations:
 *
 *   - **Cloud shape**: returned by `GET /api/v1/workspaces/{id}/nodes/{id}` and accepted
 *     by `POST/PATCH /api/v1/workspaces/{id}/nodes/{id}`. Top-level fields are flat
 *     (`database`, `locationName`, `materializationType`, …). Column-lineage refs use
 *     `nodeID` + `columnID`. Column lineage lives under `metadata.columns[].sources[]`.
 *
 *   - **Disk shape (V1)**: the YAML files coa reads from `nodes/*.yml`. Most fields are
 *     wrapped under `operation:`. Column-lineage refs use `stepCounter` + `columnCounter`.
 *     Column lineage lives under `operation.metadata.columns[].sourceColumnReferences[]`.
 *     Each column carries an explicit `columnReference: { columnCounter, stepCounter }`
 *     where `stepCounter` is the *parent node*'s `id`. Document header is `fileVersion: 1`.
 *
 * The two shapes describe the same underlying graph; the diff is naming + nesting.
 * Encoding it as a data-driven {@link FieldMapping} table means the walker can run in
 * either direction without duplicating logic, and we get the disk → cloud direction
 * (e.g. for "push my local edits to the workspace") nearly for free.
 *
 * **What the converter does NOT do:**
 *   - Generate new UUIDs for nodes/columns authored from scratch on disk. Round-tripping
 *     a node fetched from the cloud is safe (UUIDs are stable). Authoring fresh on disk
 *     and pushing up is a CREATE flow that this converter doesn't model — call
 *     `create_workspace_node_from_*` for that.
 *   - Synthesize a project (locations.yml, data.yml). That's a separate concern.
 *
 * Schema reference: see {@link WorkspaceNodeWriteInputSchema} in
 * src/schemas/node-payloads.ts for the canonical cloud body shape that matters here.
 */

import { isPlainObject } from "../../utils.js";

// ── Field-mapping types ────────────────────────────────────────────────────────

type Path = readonly string[];

/**
 * Describes how one field maps between the two shapes. Either side may be absent
 * (e.g. `fileVersion` is disk-only and constant; `lastModifiedAt` is cloud-only
 * and dropped on the way down).
 */
export interface FieldMapping {
  /** Path on the cloud side. Omit when the field is disk-only. */
  cloud?: Path;
  /** Path on the disk side. Omit when the field is cloud-only. */
  disk?: Path;
  /** Constant written to the disk side (cloud → disk only). E.g. `fileVersion: 1`. */
  diskConstant?: unknown;
  /**
   * Default written to disk when the cloud side has no value (cloud → disk only).
   * Distinct from `diskConstant`: the constant always wins; the default only fills gaps.
   */
  diskDefault?: unknown;
  /** Default written to the cloud side when the disk side is absent (disk → cloud only). */
  cloudDefault?: unknown;
  /** Drop a value from the disk side that's already implicit (disk → cloud round-trip). */
  diskOnly?: boolean;
  /**
   * For arrays of objects: walks each element with this nested mapping table. The
   * walker copies the array; children are transformed in place.
   */
  elementMap?: FieldMapping[];
  /**
   * Hook for fields that need a value derived from the parent context (cloud → disk only).
   * E.g. a column's `columnReference.stepCounter` is the *parent node*'s `id`.
   */
  diskFromContext?: (ctx: ConversionContext) => unknown;
}

interface ConversionContext {
  /** The id of the node currently being converted (used to fill `stepCounter`). */
  parentNodeId?: string;
}

// ── Top-level mapping: cloud body ↔ disk node YAML ─────────────────────────────

const COLUMN_REFERENCE_FIELD_MAP: FieldMapping[] = [
  { cloud: ["nodeID"], disk: ["stepCounter"] },
  { cloud: ["columnID"], disk: ["columnCounter"] },
  { cloud: ["columnName"], disk: ["columnName"] },
];

const COLUMN_SOURCE_FIELD_MAP: FieldMapping[] = [
  { cloud: ["transform"], disk: ["transform"], diskDefault: "", cloudDefault: "" },
  {
    cloud: ["columnReferences"],
    disk: ["columnReferences"],
    elementMap: COLUMN_REFERENCE_FIELD_MAP,
  },
  { cloud: ["name"], disk: ["name"] },
];

const COLUMN_FIELD_MAP: FieldMapping[] = [
  { cloud: ["name"], disk: ["name"] },
  { cloud: ["dataType"], disk: ["dataType"] },
  { cloud: ["description"], disk: ["description"], diskDefault: "", cloudDefault: "" },
  { cloud: ["nullable"], disk: ["nullable"], diskDefault: true, cloudDefault: true },
  { cloud: ["columnID"], disk: ["columnReference", "columnCounter"] },
  // The disk side also carries the parent node's id under columnReference.stepCounter.
  // The cloud has no equivalent because the relationship is implicit (a column belongs
  // to its containing node).
  {
    diskFromContext: (ctx) => ctx.parentNodeId,
    disk: ["columnReference", "stepCounter"],
  },
  // Cloud uses `sources[]`, disk uses `sourceColumnReferences[]`. Same content, renamed.
  {
    cloud: ["sources"],
    disk: ["sourceColumnReferences"],
    elementMap: COLUMN_SOURCE_FIELD_MAP,
  },
  { cloud: ["config"], disk: ["config"], diskDefault: {}, cloudDefault: {} },
  { cloud: ["transform"], disk: ["transform"] },
  { cloud: ["isPrimaryKey"], disk: ["isPrimaryKey"] },
  { cloud: ["isBusinessKey"], disk: ["isBusinessKey"] },
  { cloud: ["isChangeTracking"], disk: ["isChangeTracking"] },
  // Disk-only metadata block; cloud doesn't track per-column applied tests separately.
  { disk: ["appliedColumnTests"], diskDefault: {} },
];

const METADATA_FIELD_MAP: FieldMapping[] = [
  { cloud: ["columns"], disk: ["columns"], elementMap: COLUMN_FIELD_MAP },
  { cloud: ["sourceMapping"], disk: ["sourceMapping"] },
  { cloud: ["cteString"], disk: ["cteString"] },
  { cloud: ["appliedNodeTests"], disk: ["appliedNodeTests"], diskDefault: [] },
  { cloud: ["enabledColumnTestIDs"], disk: ["enabledColumnTestIDs"] },
];

const NODE_FIELD_MAP: FieldMapping[] = [
  { disk: ["fileVersion"], diskConstant: 1 },
  { cloud: ["id"], disk: ["id"] },
  { cloud: ["name"], disk: ["name"] },
  // Everything below moves under `operation:` on the disk side.
  { cloud: ["description"], disk: ["operation", "description"], diskDefault: "" },
  { cloud: ["nodeType"], disk: ["operation", "nodeType"] },
  { cloud: ["database"], disk: ["operation", "database"], diskDefault: "" },
  { cloud: ["schema"], disk: ["operation", "schema"] },
  { cloud: ["locationName"], disk: ["operation", "locationName"] },
  { cloud: ["materializationType"], disk: ["operation", "materializationType"] },
  { cloud: ["isMultisource"], disk: ["operation", "isMultisource"], diskDefault: false },
  { cloud: ["overrideSQL"], disk: ["operation", "overrideSQL"] },
  { cloud: ["table"], disk: ["operation", "table"] },
  { cloud: ["config"], disk: ["operation", "config"], diskDefault: {} },
  {
    cloud: ["metadata"],
    disk: ["operation", "metadata"],
    // `metadata` is itself an object — recurse via a virtual elementMap of length 1.
    // We use elementMap below for arrays; for nested objects we just rely on the
    // walker's path-traversal to set/get nested keys directly. The nested column
    // mapping is reached via the explicit `metadata.columns` rule above. Leave this
    // top-level entry untyped so non-mapped fields pass through.
  },
  // Disk-only default that disk YAML expects but cloud doesn't carry.
  { disk: ["operation", "deployEnabled"], diskDefault: true },
  // Cloud-only fields the disk doesn't need.
  { cloud: ["createdAt"] },
  { cloud: ["updatedAt"] },
  { cloud: ["deletedAt"] },
  { cloud: ["lastModifiedAt"] },
];

// `metadata` deserves its own pass because columns are an array of objects.
// The NODE_FIELD_MAP rule above intentionally omits `elementMap` so the metadata
// container itself passes through as a plain object; we then run METADATA_FIELD_MAP
// against `cloud.metadata` ↔ `disk.operation.metadata` separately.

// ── Walker ─────────────────────────────────────────────────────────────────────

export type Direction = "cloudToDisk" | "diskToCloud";

/** Get a value at a path; returns undefined if any segment is missing. */
function getAt(root: unknown, path: Path): unknown {
  let cur: unknown = root;
  for (const segment of path) {
    if (!isPlainObject(cur)) { return undefined; }
    cur = (cur as Record<string, unknown>)[segment];
  }
  return cur;
}

/** Set a value at a path, creating intermediate objects as needed. */
function setAt(root: Record<string, unknown>, path: Path, value: unknown): void {
  if (path.length === 0) { return; }
  let cur: Record<string, unknown> = root;
  for (let i = 0; i < path.length - 1; i++) {
    const segment = path[i];
    const next = cur[segment];
    if (!isPlainObject(next)) {
      const fresh: Record<string, unknown> = {};
      cur[segment] = fresh;
      cur = fresh;
    } else {
      cur = next as Record<string, unknown>;
    }
  }
  cur[path[path.length - 1]] = value;
}

/**
 * Apply a {@link FieldMapping} table to convert one shape into the other.
 *
 * The walker is intentionally lossy in one specific way: cloud-only fields without
 * a `disk` path (timestamps, server bookkeeping) drop on the way down to disk; disk-only
 * fields without a `cloud` path drop on the way up. Anything not in the table at all
 * is dropped — passthrough fields would defeat the schema-as-data goal. If something
 * round-trips poorly, add an explicit entry.
 */
function transform(
  input: Record<string, unknown>,
  table: FieldMapping[],
  direction: Direction,
  ctx: ConversionContext = {},
): Record<string, unknown> {
  const out: Record<string, unknown> = {};

  for (const mapping of table) {
    const sourcePath = direction === "cloudToDisk" ? mapping.cloud : mapping.disk;
    const targetPath = direction === "cloudToDisk" ? mapping.disk : mapping.cloud;

    // Disk-only constant (only applies cloud → disk).
    if (direction === "cloudToDisk" && mapping.diskConstant !== undefined && mapping.disk) {
      setAt(out, mapping.disk, mapping.diskConstant);
      continue;
    }

    // Synthesized from parent context (only applies cloud → disk).
    if (direction === "cloudToDisk" && mapping.diskFromContext && mapping.disk) {
      const value = mapping.diskFromContext(ctx);
      if (value !== undefined) {
        setAt(out, mapping.disk, value);
      }
      continue;
    }

    // Standard field-to-field copy.
    if (sourcePath && targetPath) {
      const value = getAt(input, sourcePath);
      if (value !== undefined) {
        if (mapping.elementMap && Array.isArray(value)) {
          // Recurse into each array element with the nested mapping.
          // Pass `parentNodeId` from the top-level node down to its columns so
          // `columnReference.stepCounter` can resolve.
          const childCtx: ConversionContext = {
            ...ctx,
            parentNodeId: ctx.parentNodeId ?? (input.id as string | undefined),
          };
          const transformed = value.map((entry) =>
            isPlainObject(entry) ? transform(entry, mapping.elementMap!, direction, childCtx) : entry,
          );
          setAt(out, targetPath, transformed);
        } else {
          setAt(out, targetPath, value);
        }
        continue;
      }

      // Source side has no value — fall back to direction-appropriate default.
      const fallback = direction === "cloudToDisk" ? mapping.diskDefault : mapping.cloudDefault;
      if (fallback !== undefined) {
        setAt(out, targetPath, fallback);
      }
    }
  }

  return out;
}

// ── Public converters ──────────────────────────────────────────────────────────

/**
 * Convert a workspace node fetched from the cloud (`get_workspace_node`) into a
 * disk-shape YAML object that coa's `nodes/*.yml` parser accepts. The output is a
 * plain object; serialize with {@link renderYaml} to write to disk.
 */
export function cloudNodeToDisk(cloudNode: Record<string, unknown>): Record<string, unknown> {
  const ctx: ConversionContext = {
    parentNodeId: typeof cloudNode.id === "string" ? cloudNode.id : undefined,
  };
  const top = transform(cloudNode, NODE_FIELD_MAP, "cloudToDisk", ctx);

  // The NODE_FIELD_MAP entry for `metadata` is intentionally untyped at the top
  // level (it's a container, not a leaf). Run METADATA_FIELD_MAP against the
  // metadata sub-tree explicitly so columns/sourceMapping/etc. are converted.
  const cloudMetadata = isPlainObject(cloudNode.metadata) ? cloudNode.metadata : undefined;
  if (cloudMetadata) {
    const diskMetadata = transform(cloudMetadata, METADATA_FIELD_MAP, "cloudToDisk", ctx);
    if (!isPlainObject(top.operation)) { top.operation = {}; }
    (top.operation as Record<string, unknown>).metadata = diskMetadata;
  }

  return top;
}

/**
 * Convert a disk-shape node (parsed from a `nodes/*.yml` file) into a cloud
 * workspace-node body suitable for `set_workspace_node` / `update_workspace_node`.
 *
 * Round-tripping a node that originated in the cloud preserves every UUID. Pushing
 * a node authored from scratch on disk requires a CREATE flow with new UUIDs;
 * use `create_workspace_node_from_*` for that case.
 */
export function diskNodeToCloud(diskNode: Record<string, unknown>): Record<string, unknown> {
  const top = transform(diskNode, NODE_FIELD_MAP, "diskToCloud");

  // Walk metadata explicitly, mirroring cloudNodeToDisk.
  const diskMetadata = getAt(diskNode, ["operation", "metadata"]);
  if (isPlainObject(diskMetadata)) {
    const cloudMetadata = transform(diskMetadata, METADATA_FIELD_MAP, "diskToCloud");
    top.metadata = cloudMetadata;
  }

  return top;
}

// ── Test hooks ─────────────────────────────────────────────────────────────────

// Exported for unit tests so we can verify each table independently.
export const __test = {
  NODE_FIELD_MAP,
  METADATA_FIELD_MAP,
  COLUMN_FIELD_MAP,
  COLUMN_SOURCE_FIELD_MAP,
  COLUMN_REFERENCE_FIELD_MAP,
  transform,
};
