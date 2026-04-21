import { describe, it, expect } from "vitest";
import {
  getNodeColumnCount,
  getNodeStorageLocationCount,
  getNodeConfigKeyCount,
  getRequestedNodeName,
  getRequestedColumnNames,
  getRequestedConfig,
  getRequestedLocationFields,
  getNodeColumnNames,
  getNodeDependencyNames,
  normalizeColumnName,
  normalizeDataType,
} from "../../src/services/workspace/node-inspection.js";

describe("getNodeColumnCount", () => {
  it("returns the count of columns in metadata", () => {
    expect(
      getNodeColumnCount({ metadata: { columns: [{ name: "A" }, { name: "B" }] } })
    ).toBe(2);
  });

  it("returns 0 when metadata is missing", () => {
    expect(getNodeColumnCount({})).toBe(0);
  });

  it("returns 0 when metadata is not a plain object", () => {
    expect(getNodeColumnCount({ metadata: "nope" })).toBe(0);
    expect(getNodeColumnCount({ metadata: null })).toBe(0);
  });

  it("returns 0 when metadata.columns is not an array", () => {
    expect(getNodeColumnCount({ metadata: { columns: "nope" } })).toBe(0);
  });
});

describe("getNodeStorageLocationCount", () => {
  it("returns array length", () => {
    expect(getNodeStorageLocationCount({ storageLocations: [1, 2, 3] })).toBe(3);
  });

  it("returns 0 when missing or non-array", () => {
    expect(getNodeStorageLocationCount({})).toBe(0);
    expect(getNodeStorageLocationCount({ storageLocations: "x" })).toBe(0);
  });
});

describe("getNodeConfigKeyCount", () => {
  it("counts top-level config keys", () => {
    expect(getNodeConfigKeyCount({ config: { a: 1, b: 2, c: 3 } })).toBe(3);
  });

  it("returns 0 when config is missing or not an object", () => {
    expect(getNodeConfigKeyCount({})).toBe(0);
    expect(getNodeConfigKeyCount({ config: null })).toBe(0);
    expect(getNodeConfigKeyCount({ config: [] })).toBe(0);
  });
});

describe("getRequestedNodeName", () => {
  it("returns trimmed-non-empty name", () => {
    expect(getRequestedNodeName({ name: "MY_NODE" })).toBe("MY_NODE");
  });

  it("returns undefined for empty / whitespace / non-string", () => {
    expect(getRequestedNodeName({})).toBeUndefined();
    expect(getRequestedNodeName({ name: "" })).toBeUndefined();
    expect(getRequestedNodeName({ name: "   " })).toBeUndefined();
    expect(getRequestedNodeName({ name: 123 })).toBeUndefined();
  });
});

describe("getRequestedColumnNames", () => {
  it("returns names of requested columns", () => {
    const changes = {
      metadata: {
        columns: [{ name: "A" }, { name: "B" }, { name: "C" }],
      },
    };
    expect(getRequestedColumnNames(changes)).toEqual(["A", "B", "C"]);
  });

  it("filters out malformed column entries", () => {
    const changes = {
      metadata: {
        columns: [
          { name: "OK" },
          { name: "" },
          { name: "   " },
          { name: 42 },
          "not-an-object",
          null,
        ],
      },
    };
    expect(getRequestedColumnNames(changes)).toEqual(["OK"]);
  });

  it("returns [] when metadata or columns missing", () => {
    expect(getRequestedColumnNames({})).toEqual([]);
    expect(getRequestedColumnNames({ metadata: {} })).toEqual([]);
    expect(getRequestedColumnNames({ metadata: { columns: "nope" } })).toEqual([]);
  });
});

describe("getRequestedConfig", () => {
  it("returns the config object when present", () => {
    const config = { a: 1 };
    expect(getRequestedConfig({ config })).toBe(config);
  });

  it("returns undefined when missing or wrong shape", () => {
    expect(getRequestedConfig({})).toBeUndefined();
    expect(getRequestedConfig({ config: null })).toBeUndefined();
    expect(getRequestedConfig({ config: [] })).toBeUndefined();
  });
});

describe("getRequestedLocationFields", () => {
  it("extracts only the location-related keys that are present", () => {
    const result = getRequestedLocationFields({
      database: "DB",
      schema: "PUBLIC",
      locationName: "PROD",
      otherField: "ignored",
    });
    expect(result).toEqual({ database: "DB", schema: "PUBLIC", locationName: "PROD" });
  });

  it("preserves explicit undefined when the key is present", () => {
    const result = getRequestedLocationFields({ database: undefined, schema: "PUBLIC" });
    expect(Object.prototype.hasOwnProperty.call(result, "database")).toBe(true);
    expect(result.database).toBeUndefined();
    expect(result.schema).toBe("PUBLIC");
  });

  it("returns empty object when no location keys are present", () => {
    expect(getRequestedLocationFields({ name: "X" })).toEqual({});
  });
});

describe("getNodeColumnNames", () => {
  it("returns names from metadata.columns", () => {
    expect(
      getNodeColumnNames({
        metadata: { columns: [{ name: "A" }, { name: "B" }] },
      })
    ).toEqual(["A", "B"]);
  });

  it("skips columns missing a string name", () => {
    expect(
      getNodeColumnNames({
        metadata: { columns: [{ name: "A" }, { name: 42 }, "no", null, { name: "B" }] },
      })
    ).toEqual(["A", "B"]);
  });

  it("returns empty array when metadata or columns missing", () => {
    expect(getNodeColumnNames({})).toEqual([]);
    expect(getNodeColumnNames({ metadata: {} })).toEqual([]);
    expect(getNodeColumnNames({ metadata: "nope" })).toEqual([]);
  });
});

describe("getNodeDependencyNames", () => {
  it("collects nodeName from sourceMapping[].dependencies[]", () => {
    const node = {
      metadata: {
        sourceMapping: [
          {
            dependencies: [{ nodeName: "STG_A" }, { nodeName: "STG_B" }],
          },
          {
            dependencies: [{ nodeName: "STG_C" }],
          },
        ],
      },
    };
    expect(getNodeDependencyNames(node)).toEqual(["STG_A", "STG_B", "STG_C"]);
  });

  it("skips malformed entries at every level", () => {
    const node = {
      metadata: {
        sourceMapping: [
          "not-an-object",
          null,
          { dependencies: "nope" },
          { dependencies: [null, "x", { nodeName: 42 }, { nodeName: "OK" }] },
        ],
      },
    };
    expect(getNodeDependencyNames(node)).toEqual(["OK"]);
  });

  it("returns empty array when metadata or sourceMapping missing", () => {
    expect(getNodeDependencyNames({})).toEqual([]);
    expect(getNodeDependencyNames({ metadata: {} })).toEqual([]);
    expect(getNodeDependencyNames({ metadata: { sourceMapping: "x" } })).toEqual([]);
  });
});

describe("normalizeColumnName", () => {
  it("trims and uppercases", () => {
    expect(normalizeColumnName("  customer_id  ")).toBe("CUSTOMER_ID");
    expect(normalizeColumnName("Already")).toBe("ALREADY");
  });
});

describe("normalizeDataType", () => {
  it("trims and uppercases", () => {
    expect(normalizeDataType("  varchar(255)  ")).toBe("VARCHAR(255)");
    expect(normalizeDataType("number")).toBe("NUMBER");
  });
});
