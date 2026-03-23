import { describe, it, expect, vi } from "vitest";
import { replaceWorkspaceNodeColumns } from "../../src/services/workspace/mutations.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ id: "new-node" }),
    put: vi.fn().mockResolvedValue({ id: "updated-node" }),
    delete: vi.fn(),
  };
}

describe("GROUP BY Metadata Stripping", () => {
  it("strips groupByColumns from metadata before sending to Coalesce API", async () => {
    const client = createMockClient();

    // Mock current node with groupByColumns in metadata (invalid for Coalesce API)
    client.get.mockResolvedValue({
      id: "node-1",
      name: "FCT_TABLE",
      nodeType: "Fact",
      metadata: {
        columns: [{ name: "OLD_COL" }],
        groupByColumns: [{ columnName: "CUSTOMER_ID" }], // This should be stripped
        sourceMapping: [{ name: "FCT_TABLE", dependencies: [] }],
      },
    });

    client.put.mockResolvedValue({ id: "node-1" });

    await replaceWorkspaceNodeColumns(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      columns: [
        { name: "CUSTOMER_ID", transform: '"TABLE"."CUSTOMER_ID"' },
        { name: "TOTAL_ORDERS", transform: 'COUNT(DISTINCT "TABLE"."ORDER_ID")' },
      ],
    });

    // Verify that groupByColumns was stripped from the PUT request
    expect(client.put).toHaveBeenCalled();
    const putCall = client.put.mock.calls[0];
    const updatedNode = putCall[1];

    expect(updatedNode.metadata.groupByColumns).toBeUndefined();
    expect(updatedNode.metadata.columns).toHaveLength(2);
    expect(updatedNode.metadata.sourceMapping).toBeDefined();

    // Passthrough transform "TABLE"."CUSTOMER_ID" should be stripped
    const customerCol = updatedNode.metadata.columns.find(
      (c: any) => c.name === "CUSTOMER_ID"
    );
    expect(customerCol.transform).toBeUndefined();

    // Actual transform COUNT(DISTINCT ...) should be preserved
    const ordersCol = updatedNode.metadata.columns.find(
      (c: any) => c.name === "TOTAL_ORDERS"
    );
    expect(ordersCol.transform).toBe('COUNT(DISTINCT "TABLE"."ORDER_ID")');
  });

  it("preserves other valid metadata fields while stripping groupByColumns", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "FCT_TABLE",
      nodeType: "Fact",
      metadata: {
        columns: [{ name: "OLD_COL" }],
        groupByColumns: [{ columnName: "INVALID" }],
        sourceMapping: [{ name: "FCT_TABLE", dependencies: [] }],
        customField: "should-be-preserved",
      },
    });

    client.put.mockResolvedValue({ id: "node-1" });

    await replaceWorkspaceNodeColumns(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      columns: [{ name: "NEW_COL", transform: '"TABLE"."COL"' }],
    });

    const putCall = client.put.mock.calls[0];
    const updatedNode = putCall[1];

    expect(updatedNode.metadata.groupByColumns).toBeUndefined();
    expect(updatedNode.metadata.sourceMapping).toBeDefined();
    // customField is not a valid Coalesce API metadata field — it should be stripped
    expect(updatedNode.metadata.customField).toBeUndefined();
    expect(updatedNode.metadata.columns).toHaveLength(1);
  });
});

describe("Passthrough Transform Stripping", () => {
  it("strips passthrough transforms in various formats", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "STG_TABLE",
      nodeType: "Stage",
      metadata: {
        columns: [],
        sourceMapping: [{ name: "STG_TABLE", dependencies: [] }],
      },
    });
    client.put.mockResolvedValue({ id: "node-1" });

    await replaceWorkspaceNodeColumns(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      columns: [
        // Passthrough: "ALIAS"."COL"
        { name: "TRUCK_ID", transform: '"SRC_TABLE"."TRUCK_ID"' },
        // Passthrough: {{ ref(...) }}."COL"
        { name: "MENU_ID", transform: '{{ ref(\'SRC_INGEST\', \'MENU\') }}."MENU_ID"' },
        // Passthrough: bare column name
        { name: "STATUS", transform: "STATUS" },
        // Passthrough: quoted bare name
        { name: "CITY", transform: '"CITY"' },
        // Actual transform: should be preserved
        { name: "UPPER_CITY", transform: 'UPPER("SRC"."CITY")' },
        // Actual transform: aggregate
        { name: "TOTAL", transform: 'SUM("SRC"."AMOUNT")' },
      ],
    });

    const putCall = client.put.mock.calls[0];
    const cols = putCall[1].metadata.columns;

    // Passthrough transforms should be stripped
    expect(cols.find((c: any) => c.name === "TRUCK_ID").transform).toBeUndefined();
    expect(cols.find((c: any) => c.name === "MENU_ID").transform).toBeUndefined();
    expect(cols.find((c: any) => c.name === "STATUS").transform).toBeUndefined();
    expect(cols.find((c: any) => c.name === "CITY").transform).toBeUndefined();

    // Actual transforms should be preserved
    expect(cols.find((c: any) => c.name === "UPPER_CITY").transform).toBe('UPPER("SRC"."CITY")');
    expect(cols.find((c: any) => c.name === "TOTAL").transform).toBe('SUM("SRC"."AMOUNT")');
  });

  it("does not strip transforms that reference a different column", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "STG_TABLE",
      nodeType: "Stage",
      metadata: {
        columns: [],
        sourceMapping: [{ name: "STG_TABLE", dependencies: [] }],
      },
    });
    client.put.mockResolvedValue({ id: "node-1" });

    await replaceWorkspaceNodeColumns(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      columns: [
        // References a DIFFERENT column name — not passthrough
        { name: "LOCATION_NAME", transform: '"SRC"."CITY"' },
      ],
    });

    const putCall = client.put.mock.calls[0];
    const cols = putCall[1].metadata.columns;

    expect(cols.find((c: any) => c.name === "LOCATION_NAME").transform).toBe('"SRC"."CITY"');
  });
});
