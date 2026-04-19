import { describe, it, expect, vi } from "vitest";
import {
  convertJoinToAggregation,
  applyJoinCondition,
} from "../../src/services/workspace/join-operations.js";

// Mock completeNodeConfiguration so tests don't need corpus/repo files.
vi.mock("../../src/services/config/intelligent.js", () => ({
  completeNodeConfiguration: vi.fn(
    async (client: any, params: { workspaceID: string; nodeID: string }) => {
      const node = await client.get(
        `/api/v1/workspaces/${params.workspaceID}/nodes/${params.nodeID}`
      );
      return {
        node,
        schemaSource: "corpus",
        classification: {
          required: [],
          conditionalRequired: [],
          optionalWithDefaults: [],
          contextual: [],
          columnSelectors: [],
        },
        context: {
          hasMultipleSources: false,
          hasAggregates: false,
          hasTimestampColumns: false,
          hasType2Pattern: false,
          materializationType: "table",
        },
        appliedConfig: {},
        configChanges: { required: {}, contextual: {}, preserved: {}, defaults: {} },
        columnAttributeChanges: { applied: [], reasoning: [] },
        reasoning: [],
        detectedPatterns: { candidateColumns: [] },
      };
    }
  ),
}));

type PutCall = [string, Record<string, unknown>];

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ id: "new-node" }),
    put: vi.fn().mockResolvedValue({ id: "updated-node" }),
    delete: vi.fn(),
  };
}

// -----------------------------------------------------------------------------
// Fixture builders
// -----------------------------------------------------------------------------

function buildMultiPredNode(overrides: Record<string, unknown> = {}) {
  return {
    id: "join-node",
    name: "INT_JOINED",
    nodeType: "View",
    config: {},
    metadata: {
      columns: [
        {
          name: "CUSTOMER_ID",
          dataType: "VARCHAR",
          sources: [
            { columnReferences: [{ nodeID: "pred-cust", columnName: "CUSTOMER_ID" }] },
          ],
        },
        {
          name: "ORDER_ID",
          dataType: "VARCHAR",
          sources: [
            { columnReferences: [{ nodeID: "pred-orders", columnName: "ORDER_ID" }] },
          ],
        },
      ],
      sourceMapping: [
        {
          name: "INT_JOINED",
          dependencies: [
            { locationName: "STAGING", nodeName: "STG_CUSTOMER" },
            { locationName: "STAGING", nodeName: "STG_ORDERS" },
          ],
          aliases: { STG_CUSTOMER: "pred-cust", STG_ORDERS: "pred-orders" },
          join: { joinCondition: "" },
          customSQL: { customSQL: "" },
          noLinkRefs: [],
        },
      ],
    },
    ...overrides,
  };
}

function buildSinglePredNode(overrides: Record<string, unknown> = {}) {
  return {
    id: "single-node",
    name: "FACT_FROM_STG",
    nodeType: "Fact",
    config: {},
    metadata: {
      columns: [
        {
          name: "CUSTOMER_ID",
          dataType: "VARCHAR",
          transform: '"STG_CUSTOMER"."CUSTOMER_ID"',
          sources: [
            { columnReferences: [{ nodeID: "pred-cust", columnName: "CUSTOMER_ID" }] },
          ],
        },
        {
          name: "AMOUNT",
          dataType: "NUMBER",
          transform: '"STG_CUSTOMER"."AMOUNT"',
          sources: [
            { columnReferences: [{ nodeID: "pred-cust", columnName: "AMOUNT" }] },
          ],
        },
      ],
      sourceMapping: [
        {
          name: "FACT_FROM_STG",
          dependencies: [{ locationName: "STAGING", nodeName: "STG_CUSTOMER" }],
          aliases: { STG_CUSTOMER: "pred-cust" },
          join: { joinCondition: "FROM {{ ref('STAGING', 'STG_CUSTOMER') }}" },
          customSQL: { customSQL: "" },
          noLinkRefs: [],
        },
      ],
    },
    ...overrides,
  };
}

function buildPredecessor(
  id: string,
  name: string,
  locationName: string | undefined,
  columns: Array<{ name: string; dataType?: string }>
) {
  const node: Record<string, unknown> = {
    id,
    name,
    nodeType: "Stage",
    config: {},
    metadata: {
      columns: columns.map((c) => ({
        name: c.name,
        dataType: c.dataType ?? "VARCHAR",
        sources: [],
      })),
      sourceMapping: [
        {
          name,
          dependencies: [],
          aliases: {},
          join: { joinCondition: "" },
          customSQL: { customSQL: "" },
          noLinkRefs: [],
        },
      ],
    },
  };
  if (locationName) {
    node.locationName = locationName;
    node.storageLocations = [{ name: locationName }];
  }
  return node;
}

function collectJoinConditions(putCalls: PutCall[]): string[] {
  const out: string[] = [];
  for (const call of putCalls) {
    const body = call[1];
    const meta = body.metadata as Record<string, unknown> | undefined;
    const sm = Array.isArray(meta?.sourceMapping) ? meta.sourceMapping : [];
    const first = sm[0] as Record<string, unknown> | undefined;
    const join = first?.join as Record<string, unknown> | undefined;
    const jc = typeof join?.joinCondition === "string" ? join.joinCondition : "";
    if (jc.length > 0) out.push(jc);
  }
  return out;
}

// =============================================================================
// convertJoinToAggregation
// =============================================================================

describe("convertJoinToAggregation", () => {
  it("single-predecessor aggregation appends GROUP BY to existing joinCondition", async () => {
    const client = createMockClient();
    const node = buildSinglePredNode();
    const predCust = buildPredecessor("pred-cust", "STG_CUSTOMER", "STAGING", [
      { name: "CUSTOMER_ID" },
      { name: "AMOUNT", dataType: "NUMBER" },
    ]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("single-node")) return Promise.resolve(node);
      if (path.includes("pred-cust")) return Promise.resolve(predCust);
      return Promise.resolve({ data: [] });
    });

    const result = await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "single-node",
      groupByColumns: ['"STG_CUSTOMER"."CUSTOMER_ID"'],
      aggregates: [
        { name: "TOTAL", function: "SUM", expression: '"STG_CUSTOMER"."AMOUNT"' },
      ],
    });

    expect(result.validation.valid).toBe(true);
    const allJCs = collectJoinConditions(client.put.mock.calls as PutCall[]);
    // At least one PUT must carry GROUP BY (the sourceMapping update) and
    // must NOT be a multi-predecessor JOIN (no INNER JOIN added by the
    // single-predecessor path).
    const jcWithGroupBy = allJCs.find((jc) => jc.includes("GROUP BY"));
    expect(jcWithGroupBy).toBeDefined();
    expect(jcWithGroupBy).toContain("FROM {{ ref('STAGING', 'STG_CUSTOMER') }}");
    expect(jcWithGroupBy).not.toContain("INNER JOIN");
  });

  it("skips predecessor fetch when maintainJoins: false", async () => {
    const client = createMockClient();
    const node = buildSinglePredNode();

    client.get.mockImplementation((path: string) => {
      if (path.includes("single-node")) return Promise.resolve(node);
      return Promise.resolve({ data: [] });
    });

    await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "single-node",
      groupByColumns: ['"STG_CUSTOMER"."CUSTOMER_ID"'],
      aggregates: [
        { name: "TOTAL", function: "SUM", expression: '"STG_CUSTOMER"."AMOUNT"' },
      ],
      maintainJoins: false,
    });

    // No predecessor fetch should have happened — only fetches for the node
    // itself (initial + post-update re-fetch + completion re-fetch).
    const predFetchCalls = (client.get.mock.calls as unknown[][]).filter(
      (call) => typeof call[0] === "string" && (call[0] as string).includes("pred-")
    );
    expect(predFetchCalls).toHaveLength(0);
  });

  it("sets isBusinessKey on GROUP BY columns and isChangeTracking on aggregate columns", async () => {
    const client = createMockClient();
    const node = buildMultiPredNode();
    const predCust = buildPredecessor("pred-cust", "STG_CUSTOMER", "STAGING", [
      { name: "CUSTOMER_ID" },
    ]);
    const predOrders = buildPredecessor("pred-orders", "STG_ORDERS", "STAGING", [
      { name: "ORDER_ID" },
      { name: "CUSTOMER_ID" },
      { name: "AMOUNT", dataType: "NUMBER" },
    ]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(node);
      if (path.includes("pred-cust")) return Promise.resolve(predCust);
      if (path.includes("pred-orders")) return Promise.resolve(predOrders);
      return Promise.resolve({ data: [] });
    });

    await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "join-node",
      groupByColumns: ['"STG_CUSTOMER"."CUSTOMER_ID"'],
      aggregates: [
        { name: "TOTAL", function: "SUM", expression: '"STG_ORDERS"."AMOUNT"' },
      ],
    });

    // Find the PUT call that wrote columns (not the one that wrote joinCondition)
    const columnsPutCall = (client.put.mock.calls as PutCall[]).find((call) => {
      const body = call[1];
      const meta = body.metadata as Record<string, unknown> | undefined;
      return Array.isArray(meta?.columns) && meta.columns.length > 0;
    });
    expect(columnsPutCall).toBeDefined();
    const cols = ((columnsPutCall as PutCall)[1].metadata as Record<string, unknown>)
      .columns as Array<Record<string, unknown>>;

    const bkCol = cols.find((c) => c.name === "CUSTOMER_ID");
    const ctCol = cols.find((c) => c.name === "TOTAL");
    expect(bkCol?.isBusinessKey).toBe(true);
    expect(bkCol?.isChangeTracking).toBeUndefined();
    expect(ctCol?.isChangeTracking).toBe(true);
    expect(ctCol?.isBusinessKey).toBeUndefined();
  });

  it("uses bare-name fallback join SQL when predecessors lack locationName", async () => {
    const client = createMockClient();
    const node = buildMultiPredNode();
    // Predecessors without locationName
    const predCust = buildPredecessor("pred-cust", "STG_CUSTOMER", undefined, [
      { name: "CUSTOMER_ID" },
    ]);
    const predOrders = buildPredecessor("pred-orders", "STG_ORDERS", undefined, [
      { name: "ORDER_ID" },
      { name: "CUSTOMER_ID" },
      { name: "AMOUNT", dataType: "NUMBER" },
    ]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(node);
      if (path.includes("pred-cust")) return Promise.resolve(predCust);
      if (path.includes("pred-orders")) return Promise.resolve(predOrders);
      return Promise.resolve({ data: [] });
    });

    const result = await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "join-node",
      groupByColumns: ["STG_CUSTOMER.CUSTOMER_ID"],
      aggregates: [
        { name: "TOTAL", function: "SUM", expression: "STG_ORDERS.AMOUNT" },
      ],
    });

    expect(
      result.joinSQL.warnings.some((w) => /locationName/i.test(w))
    ).toBe(true);
    // Fallback produces bare-name FROM (no {{ ref(...) }})
    expect(result.joinSQL.fullSQL).not.toContain("{{ ref(");
  });

  it("returns validation failure when an aggregate is actually a window function", async () => {
    const client = createMockClient();
    const node = buildSinglePredNode();
    const predCust = buildPredecessor("pred-cust", "STG_CUSTOMER", "STAGING", [
      { name: "CUSTOMER_ID" },
    ]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("single-node")) return Promise.resolve(node);
      if (path.includes("pred-cust")) return Promise.resolve(predCust);
      return Promise.resolve({ data: [] });
    });

    // ROW_NUMBER is classified as a window function by analyzeColumnsForGroupBy,
    // which is not GROUP BY-safe — must short-circuit before any mutation.
    const result = await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "single-node",
      groupByColumns: ['"STG_CUSTOMER"."CUSTOMER_ID"'],
      aggregates: [
        { name: "RN", function: "ROW_NUMBER", expression: "" },
      ],
    });

    expect(result.validation.valid).toBe(false);
    expect(result.configCompletionSkipped).toMatch(/not GROUP BY-safe/i);
    expect(client.put).not.toHaveBeenCalled();
  });

  it("throws when the node response is not an object", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue(null);

    await expect(
      convertJoinToAggregation(client as any, {
        workspaceID: "ws-1",
        nodeID: "bad-node",
        groupByColumns: ['"C"."ID"'],
        aggregates: [{ name: "CNT", function: "COUNT", expression: "*" }],
      })
    ).rejects.toThrow("Node response was not an object");
  });

  it("warns when the node has no sourceMapping entries (joinCondition not persisted)", async () => {
    const client = createMockClient();
    // Node with no sourceMapping at all
    const noMappingNode = {
      id: "no-mapping",
      name: "NO_SM",
      nodeType: "Fact",
      config: {},
      metadata: {
        columns: [
          {
            name: "CUSTOMER_ID",
            dataType: "VARCHAR",
            transform: '"STG_CUSTOMER"."CUSTOMER_ID"',
            sources: [],
          },
        ],
        sourceMapping: [],
      },
    };
    client.get.mockImplementation((path: string) => {
      if (path.includes("no-mapping")) return Promise.resolve(noMappingNode);
      return Promise.resolve({ data: [] });
    });

    const result = await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "no-mapping",
      groupByColumns: ['"STG_CUSTOMER"."CUSTOMER_ID"'],
      aggregates: [{ name: "CNT", function: "COUNT", expression: "*" }],
      maintainJoins: false,
    });

    expect(
      result.joinSQL.warnings.some((w) =>
        /no sourceMapping entries/i.test(w)
      )
    ).toBe(true);
  });
});

// =============================================================================
// applyJoinCondition
// =============================================================================

describe("applyJoinCondition", () => {
  it("throws when resolved predecessors fall below 2 due to missing locationName", async () => {
    const client = createMockClient();
    const joinNode = buildMultiPredNode();
    // Both predecessors missing locationName
    const predCust = buildPredecessor("pred-cust", "STG_CUSTOMER", undefined, [
      { name: "CUSTOMER_ID" },
    ]);
    const predOrders = buildPredecessor("pred-orders", "STG_ORDERS", undefined, [
      { name: "ORDER_ID" },
      { name: "CUSTOMER_ID" },
    ]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(joinNode);
      if (path.includes("pred-cust")) return Promise.resolve(predCust);
      if (path.includes("pred-orders")) return Promise.resolve(predOrders);
      return Promise.resolve({ data: [] });
    });

    await expect(
      applyJoinCondition(client as any, {
        workspaceID: "ws-1",
        nodeID: "join-node",
      })
    ).rejects.toThrow(/Could not resolve 2\+ predecessors/);
  });

  it("accepts WHERE clause that already starts with the WHERE keyword", async () => {
    const client = createMockClient();
    const joinNode = buildMultiPredNode();
    const predCust = buildPredecessor("pred-cust", "STG_CUSTOMER", "STAGING", [
      { name: "CUSTOMER_ID" },
      { name: "ACTIVE" },
    ]);
    const predOrders = buildPredecessor("pred-orders", "STG_ORDERS", "STAGING", [
      { name: "ORDER_ID" },
      { name: "CUSTOMER_ID" },
    ]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(joinNode);
      if (path.includes("pred-cust")) return Promise.resolve(predCust);
      if (path.includes("pred-orders")) return Promise.resolve(predOrders);
      return Promise.resolve({ data: [] });
    });

    const result = await applyJoinCondition(client as any, {
      workspaceID: "ws-1",
      nodeID: "join-node",
      whereClause: 'WHERE "STG_CUSTOMER"."ACTIVE" = TRUE',
    });

    // Should not double-prefix (no "WHERE WHERE")
    expect(result.joinCondition).toContain('WHERE "STG_CUSTOMER"."ACTIVE" = TRUE');
    expect(result.joinCondition).not.toContain("WHERE WHERE");
  });

  it("accepts QUALIFY clause that already starts with the QUALIFY keyword", async () => {
    const client = createMockClient();
    const joinNode = buildMultiPredNode();
    const predCust = buildPredecessor("pred-cust", "STG_CUSTOMER", "STAGING", [
      { name: "CUSTOMER_ID" },
    ]);
    const predOrders = buildPredecessor("pred-orders", "STG_ORDERS", "STAGING", [
      { name: "ORDER_ID" },
      { name: "CUSTOMER_ID" },
    ]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(joinNode);
      if (path.includes("pred-cust")) return Promise.resolve(predCust);
      if (path.includes("pred-orders")) return Promise.resolve(predOrders);
      return Promise.resolve({ data: [] });
    });

    const result = await applyJoinCondition(client as any, {
      workspaceID: "ws-1",
      nodeID: "join-node",
      qualifyClause: "QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts) = 1",
    });

    expect(result.joinCondition).toContain("QUALIFY ROW_NUMBER()");
    expect(result.joinCondition).not.toContain("QUALIFY QUALIFY");
  });

  it("warns and skips persistence when node has no sourceMapping entries", async () => {
    const client = createMockClient();
    const node = {
      id: "no-sm",
      name: "N",
      nodeType: "View",
      config: {},
      metadata: {
        columns: [
          {
            name: "ID",
            sources: [
              { columnReferences: [{ nodeID: "pred-cust", columnName: "ID" }] },
            ],
          },
          {
            name: "ORDER_ID",
            sources: [
              { columnReferences: [{ nodeID: "pred-orders", columnName: "ORDER_ID" }] },
            ],
          },
        ],
        sourceMapping: [],
      },
    };
    const predCust = buildPredecessor("pred-cust", "STG_CUSTOMER", "STAGING", [
      { name: "ID" },
    ]);
    const predOrders = buildPredecessor("pred-orders", "STG_ORDERS", "STAGING", [
      { name: "ORDER_ID" },
      { name: "ID" },
    ]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("no-sm")) return Promise.resolve(node);
      if (path.includes("pred-cust")) return Promise.resolve(predCust);
      if (path.includes("pred-orders")) return Promise.resolve(predOrders);
      return Promise.resolve({ data: [] });
    });

    const result = await applyJoinCondition(client as any, {
      workspaceID: "ws-1",
      nodeID: "no-sm",
    });

    expect(
      result.warnings.some((w) =>
        /no sourceMapping entries/i.test(w)
      )
    ).toBe(true);
    // put should NOT have been called — nothing to persist
    expect(client.put).not.toHaveBeenCalled();
  });

  it("throws when the node response is not an object", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue("not-an-object");

    await expect(
      applyJoinCondition(client as any, {
        workspaceID: "ws-1",
        nodeID: "bad",
      })
    ).rejects.toThrow("Node response was not an object");
  });
});
