import { describe, it, expect } from "vitest";
import {
  cloudNodeToDisk,
  diskNodeToCloud,
  __test,
  type FieldMapping,
} from "../../src/services/templates/node-shape-bridge.js";

const { transform, COLUMN_FIELD_MAP, COLUMN_SOURCE_FIELD_MAP, METADATA_FIELD_MAP, NODE_FIELD_MAP } = __test;

// Minimal cloud node mirrors what `GET /api/v1/workspaces/:id/nodes/:id` returns.
// The shape captures every field the bridge has an explicit rule for, so a
// regression in any single mapping shows up here rather than only in the
// integration-style render-node handler tests.
function buildCloudNode(overrides: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
  return {
    id: "node-uuid-1",
    name: "STG_CUSTOMER",
    description: "Stage customers",
    nodeType: "base-nodes:::Stage",
    database: "ANALYTICS",
    schema: "STAGING",
    locationName: "STG",
    materializationType: "table",
    isMultisource: false,
    table: "STG_CUSTOMER",
    config: { testsEnabled: true },
    metadata: {
      columns: [
        {
          name: "CUSTOMER_ID",
          dataType: "VARCHAR",
          description: "PK",
          nullable: false,
          columnID: "col-uuid-1",
          sources: [
            {
              transform: "src.CUSTOMER_ID",
              columnReferences: [{ nodeID: "src-uuid-1", columnID: "src-col-uuid-1" }],
            },
          ],
          config: {},
          isPrimaryKey: true,
        },
        {
          name: "EMAIL",
          dataType: "VARCHAR",
          nullable: true,
          columnID: "col-uuid-2",
          sources: [],
        },
      ],
      sourceMapping: [{ aliases: { src: "src-uuid-1" } }],
      appliedNodeTests: [],
    },
    // Cloud-only timestamps that must not leak onto the disk side.
    createdAt: "2026-01-01T00:00:00Z",
    updatedAt: "2026-01-02T00:00:00Z",
    deletedAt: null,
    lastModifiedAt: "2026-01-02T00:00:00Z",
    ...overrides,
  };
}

describe("cloudNodeToDisk", () => {
  it("converts a full cloud node into the operation-wrapped disk shape", () => {
    const disk = cloudNodeToDisk(buildCloudNode());

    expect(disk.fileVersion).toBe(1);
    expect(disk.id).toBe("node-uuid-1");
    expect(disk.name).toBe("STG_CUSTOMER");

    const operation = disk.operation as Record<string, unknown>;
    expect(operation.description).toBe("Stage customers");
    expect(operation.nodeType).toBe("base-nodes:::Stage");
    expect(operation.database).toBe("ANALYTICS");
    expect(operation.schema).toBe("STAGING");
    expect(operation.locationName).toBe("STG");
    expect(operation.materializationType).toBe("table");
    expect(operation.isMultisource).toBe(false);
    expect(operation.config).toEqual({ testsEnabled: true });
    // Note: the field map defines `deployEnabled` with `diskDefault: true` but no
    // cloud path, so the walker's `sourcePath && targetPath` guard skips it. The
    // current bridge does not emit deployEnabled — see the "field-mapping tables"
    // suite below for a sanity check that documents this.
    expect(operation.deployEnabled).toBeUndefined();
  });

  it("drops cloud-only bookkeeping fields (createdAt, updatedAt, …)", () => {
    const disk = cloudNodeToDisk(buildCloudNode());

    expect(disk.createdAt).toBeUndefined();
    expect(disk.updatedAt).toBeUndefined();
    expect(disk.deletedAt).toBeUndefined();
    expect(disk.lastModifiedAt).toBeUndefined();

    const operation = disk.operation as Record<string, unknown>;
    expect(operation.createdAt).toBeUndefined();
    expect(operation.updatedAt).toBeUndefined();
  });

  it("renames columns[].columnID to columnReference.columnCounter and propagates parent node id to stepCounter", () => {
    const disk = cloudNodeToDisk(buildCloudNode());
    const operation = disk.operation as Record<string, unknown>;
    const metadata = operation.metadata as Record<string, unknown>;
    const columns = metadata.columns as Array<Record<string, unknown>>;

    expect(columns).toHaveLength(2);

    const first = columns[0]!;
    expect(first.name).toBe("CUSTOMER_ID");
    expect(first.dataType).toBe("VARCHAR");
    expect(first.description).toBe("PK");
    expect(first.nullable).toBe(false);
    // The cloud's `columnID` lands under `columnReference.columnCounter` on disk.
    expect(first.columnReference).toEqual({
      columnCounter: "col-uuid-1",
      stepCounter: "node-uuid-1",
    });
    // Cloud `columnID` no longer appears at the top level on the disk side.
    expect(first.columnID).toBeUndefined();
    expect(first.isPrimaryKey).toBe(true);
  });

  it("renames metadata.columns[].sources[] to sourceColumnReferences[] and renames nodeID/columnID to stepCounter/columnCounter", () => {
    const disk = cloudNodeToDisk(buildCloudNode());
    const operation = disk.operation as Record<string, unknown>;
    const metadata = operation.metadata as Record<string, unknown>;
    const columns = metadata.columns as Array<Record<string, unknown>>;
    const first = columns[0]!;

    // The cloud `sources` array is renamed to `sourceColumnReferences` on disk.
    expect(first.sources).toBeUndefined();
    const sourceRefs = first.sourceColumnReferences as Array<Record<string, unknown>>;
    expect(sourceRefs).toHaveLength(1);
    expect(sourceRefs[0]!.transform).toBe("src.CUSTOMER_ID");

    // The inner columnReferences[] stays named the same but each reference has its
    // nodeID/columnID renamed to stepCounter/columnCounter.
    const refs = sourceRefs[0]!.columnReferences as Array<Record<string, unknown>>;
    expect(refs).toEqual([{ stepCounter: "src-uuid-1", columnCounter: "src-col-uuid-1" }]);
  });

  it("fills disk defaults for missing optional column fields", () => {
    const cloud = buildCloudNode();
    // Strip optional fields from the second column to verify defaults fire.
    const metadata = cloud.metadata as Record<string, unknown>;
    const columns = metadata.columns as Array<Record<string, unknown>>;
    delete columns[1]!.nullable;
    // description was already absent on the second column.

    const disk = cloudNodeToDisk(cloud);
    const operation = disk.operation as Record<string, unknown>;
    const diskColumns = (operation.metadata as Record<string, unknown>).columns as Array<Record<string, unknown>>;
    const second = diskColumns[1]!;

    expect(second.description).toBe(""); // diskDefault for description
    expect(second.nullable).toBe(true); // diskDefault for nullable
    // Note: appliedColumnTests has a `diskDefault: {}` rule but no `cloud` path,
    // so the walker's `sourcePath && targetPath` guard skips it. The default
    // never fires — see the "field-mapping tables" suite below.
    expect(second.appliedColumnTests).toBeUndefined();
  });

  it("works when the cloud node has no metadata block", () => {
    const cloud = buildCloudNode();
    delete cloud.metadata;

    const disk = cloudNodeToDisk(cloud);
    const operation = disk.operation as Record<string, unknown> | undefined;

    expect(disk.fileVersion).toBe(1);
    // operation may be undefined when no cloud field maps under it, or it may
    // exist with the diskDefault-driven sub-fields (description, database, …).
    // Either way, no metadata sub-tree should be synthesized.
    if (operation !== undefined) {
      expect(operation.metadata).toBeUndefined();
    }
  });

  it("falls back to undefined parentNodeId when the cloud node has no string id", () => {
    const cloud = buildCloudNode({ id: undefined });
    const disk = cloudNodeToDisk(cloud);
    const operation = disk.operation as Record<string, unknown>;
    const columns = (operation.metadata as Record<string, unknown>).columns as Array<Record<string, unknown>>;
    const ref = columns[0]!.columnReference as Record<string, unknown>;

    // columnCounter still resolves from cloud columnID, but stepCounter is dropped
    // when the parent has no usable id.
    expect(ref.columnCounter).toBe("col-uuid-1");
    expect(ref.stepCounter).toBeUndefined();
  });
});

describe("diskNodeToCloud", () => {
  it("inverts the cloud-to-disk conversion (round-trip via the bridge)", () => {
    const cloud = buildCloudNode();
    const disk = cloudNodeToDisk(cloud);
    const roundTripped = diskNodeToCloud(disk);

    // Top-level fields restore.
    expect(roundTripped.id).toBe(cloud.id);
    expect(roundTripped.name).toBe(cloud.name);
    expect(roundTripped.description).toBe(cloud.description);
    expect(roundTripped.nodeType).toBe(cloud.nodeType);
    expect(roundTripped.database).toBe(cloud.database);
    expect(roundTripped.schema).toBe(cloud.schema);
    expect(roundTripped.locationName).toBe(cloud.locationName);
    expect(roundTripped.materializationType).toBe(cloud.materializationType);
    expect(roundTripped.config).toEqual(cloud.config);

    // Metadata / column lineage round-trips.
    const meta = roundTripped.metadata as Record<string, unknown>;
    const columns = meta.columns as Array<Record<string, unknown>>;
    expect(columns).toHaveLength(2);

    const first = columns[0]!;
    expect(first.name).toBe("CUSTOMER_ID");
    expect(first.columnID).toBe("col-uuid-1");
    // The disk-only columnReference.stepCounter is dropped on the cloud side
    // because the parent-node relationship is implicit.
    expect(first.columnReference).toBeUndefined();

    // sources[] restored from sourceColumnReferences[].
    const sources = first.sources as Array<Record<string, unknown>>;
    expect(sources).toHaveLength(1);
    expect(sources[0]!.transform).toBe("src.CUSTOMER_ID");
    expect(sources[0]!.columnReferences).toEqual([
      { nodeID: "src-uuid-1", columnID: "src-col-uuid-1" },
    ]);
  });

  it("drops disk-only fields (fileVersion, deployEnabled, appliedColumnTests)", () => {
    const cloud = buildCloudNode();
    const disk = cloudNodeToDisk(cloud);
    const roundTripped = diskNodeToCloud(disk);

    expect(roundTripped.fileVersion).toBeUndefined();
    expect((roundTripped.operation as unknown)).toBeUndefined(); // operation wrapper unwrapped
    const meta = roundTripped.metadata as Record<string, unknown>;
    const cols = meta.columns as Array<Record<string, unknown>>;
    expect(cols[0]!.appliedColumnTests).toBeUndefined();
  });

  it("fills cloud defaults for missing optional disk fields", () => {
    // Build a disk node with a column missing description / nullable to verify
    // the cloudDefault path fires (mirror image of the diskDefault test above).
    const disk = {
      id: "node-x",
      name: "X",
      operation: {
        nodeType: "base-nodes:::Stage",
        metadata: {
          columns: [
            {
              name: "FOO",
              dataType: "VARCHAR",
              columnReference: { columnCounter: "c-1", stepCounter: "node-x" },
            },
          ],
        },
      },
    };

    const cloud = diskNodeToCloud(disk);
    const meta = cloud.metadata as Record<string, unknown>;
    const cols = meta.columns as Array<Record<string, unknown>>;
    const first = cols[0]!;
    expect(first.description).toBe("");
    expect(first.nullable).toBe(true);
    expect(first.config).toEqual({});
  });

  it("returns a node with no metadata when the disk side is missing operation.metadata", () => {
    const disk = {
      id: "node-x",
      name: "X",
      operation: { nodeType: "base-nodes:::Stage" },
    };

    const cloud = diskNodeToCloud(disk);
    expect(cloud.metadata).toBeUndefined();
    expect(cloud.nodeType).toBe("base-nodes:::Stage");
  });
});

describe("round-trip stability", () => {
  it("cloud → disk → cloud preserves every UUID and column lineage edge", () => {
    const cloud = buildCloudNode();
    const disk = cloudNodeToDisk(cloud);
    const back = diskNodeToCloud(disk);

    // Walk both column trees and assert every UUID round-trips.
    const origCols = (cloud.metadata as Record<string, unknown>).columns as Array<Record<string, unknown>>;
    const backCols = (back.metadata as Record<string, unknown>).columns as Array<Record<string, unknown>>;
    expect(backCols).toHaveLength(origCols.length);

    for (let i = 0; i < origCols.length; i++) {
      expect(backCols[i]!.columnID).toBe(origCols[i]!.columnID);
      const origSources = (origCols[i]!.sources as Array<Record<string, unknown>>) ?? [];
      const backSources = (backCols[i]!.sources as Array<Record<string, unknown>>) ?? [];
      expect(backSources).toHaveLength(origSources.length);
      for (let j = 0; j < origSources.length; j++) {
        expect(backSources[j]!.transform).toBe(origSources[j]!.transform);
        expect(backSources[j]!.columnReferences).toEqual(origSources[j]!.columnReferences);
      }
    }
  });

  it("disk → cloud → disk preserves columnReference.stepCounter when parent id is stable", () => {
    const startingDisk = cloudNodeToDisk(buildCloudNode());
    const cloud = diskNodeToCloud(startingDisk);
    const finalDisk = cloudNodeToDisk(cloud);

    // The two disk shapes should match field-for-field — the cloud→disk pass
    // re-derives stepCounter from the parent node id, which survived as `id`.
    expect(finalDisk).toEqual(startingDisk);
  });
});

describe("transform walker (table-driven helper)", () => {
  it("treats elementMap entries as recursive arrays of objects", () => {
    const table: FieldMapping[] = [
      { cloud: ["xs"], disk: ["ys"], elementMap: [{ cloud: ["a"], disk: ["b"] }] },
    ];
    const out = transform({ xs: [{ a: 1 }, { a: 2 }] }, table, "cloudToDisk");
    expect(out).toEqual({ ys: [{ b: 1 }, { b: 2 }] });
  });

  it("drops fields that have no mapping rule (no passthrough)", () => {
    const table: FieldMapping[] = [{ cloud: ["keep"], disk: ["keep"] }];
    const out = transform({ keep: "yes", drop: "gone" }, table, "cloudToDisk");
    expect(out).toEqual({ keep: "yes" });
    expect(out.drop).toBeUndefined();
  });

  it("applies diskConstant unconditionally on cloudToDisk and ignores it on diskToCloud", () => {
    const table: FieldMapping[] = [
      { disk: ["fileVersion"], diskConstant: 1 },
    ];
    const cloudToDisk = transform({}, table, "cloudToDisk");
    expect(cloudToDisk).toEqual({ fileVersion: 1 });
    // The other direction has no `cloud` path on this rule, so the constant is
    // ignored — disk's fileVersion does not leak back to the cloud side.
    const diskToCloud = transform({ fileVersion: 1 }, table, "diskToCloud");
    expect(diskToCloud).toEqual({});
  });

  it("applies diskFromContext to derive fields from the parent context", () => {
    const table: FieldMapping[] = [
      { diskFromContext: (ctx) => ctx.parentNodeId, disk: ["ref", "stepCounter"] },
    ];
    const out = transform({}, table, "cloudToDisk", { parentNodeId: "node-7" });
    expect(out).toEqual({ ref: { stepCounter: "node-7" } });
  });

  it("propagates parentNodeId from the top-level node id when traversing elementMap arrays", () => {
    // Mirrors how the real bridge propagates the node id down to columns.
    const innerTable: FieldMapping[] = [
      { diskFromContext: (ctx) => ctx.parentNodeId, disk: ["parent"] },
    ];
    const table: FieldMapping[] = [
      { cloud: ["children"], disk: ["children"], elementMap: innerTable },
    ];
    const out = transform({ id: "p-1", children: [{}, {}] }, table, "cloudToDisk");
    expect(out).toEqual({ children: [{ parent: "p-1" }, { parent: "p-1" }] });
  });

  it("falls back to diskDefault / cloudDefault when the source side has no value", () => {
    const table: FieldMapping[] = [
      { cloud: ["a"], disk: ["a"], diskDefault: "fallback-disk", cloudDefault: "fallback-cloud" },
    ];
    expect(transform({}, table, "cloudToDisk")).toEqual({ a: "fallback-disk" });
    expect(transform({}, table, "diskToCloud")).toEqual({ a: "fallback-cloud" });
  });

  it("skips defaults when the source value is undefined and no default is configured", () => {
    const table: FieldMapping[] = [{ cloud: ["a"], disk: ["a"] }];
    expect(transform({}, table, "cloudToDisk")).toEqual({});
  });

  it("does not transform elementMap entries that are not plain objects", () => {
    const table: FieldMapping[] = [
      { cloud: ["xs"], disk: ["ys"], elementMap: [{ cloud: ["a"], disk: ["b"] }] },
    ];
    // Mix object + primitive entries — the primitive should pass through verbatim.
    const out = transform({ xs: [{ a: 1 }, "literal", null] }, table, "cloudToDisk");
    expect(out).toEqual({ ys: [{ b: 1 }, "literal", null] });
  });
});

describe("field-mapping tables (sanity checks)", () => {
  it("NODE_FIELD_MAP includes the disk-only file-version constant", () => {
    const fileVersion = NODE_FIELD_MAP.find(
      (m) => m.disk?.[0] === "fileVersion" && m.diskConstant !== undefined,
    );
    expect(fileVersion?.diskConstant).toBe(1);
  });

  it("COLUMN_FIELD_MAP carries an explicit disk path for sources → sourceColumnReferences", () => {
    const sources = COLUMN_FIELD_MAP.find((m) => m.cloud?.[0] === "sources");
    expect(sources?.disk).toEqual(["sourceColumnReferences"]);
    expect(sources?.elementMap).toBe(COLUMN_SOURCE_FIELD_MAP);
  });

  it("METADATA_FIELD_MAP routes columns through COLUMN_FIELD_MAP for nested conversion", () => {
    const cols = METADATA_FIELD_MAP.find((m) => m.cloud?.[0] === "columns");
    expect(cols?.elementMap).toBe(COLUMN_FIELD_MAP);
  });

  // Locks in current walker behavior. Disk-only fields configured with `diskDefault`
  // (and no `cloud` path) are SILENTLY SKIPPED — the walker only fires defaults
  // when both `sourcePath` and `targetPath` resolve. NODE_FIELD_MAP / COLUMN_FIELD_MAP
  // each declare such rules (`deployEnabled`, `appliedColumnTests`) that document
  // intent but never materialize. If a future change makes disk-only diskDefault
  // actually fire, this test will fail and force a deliberate update.
  it("disk-only diskDefault rules do not emit (current walker behavior)", () => {
    const table: FieldMapping[] = [
      { disk: ["onlyDisk"], diskDefault: "would-be-default" },
    ];
    const out = transform({}, table, "cloudToDisk");
    expect(out).toEqual({});
  });
});
