import { describe, it, expect, vi } from "vitest";
import {
  createWorkspaceNodeFromScratch,
  createWorkspaceNodeFromPredecessor,
  buildUpdatedWorkspaceNodeBody,
  applyJoinCondition,
  convertJoinToAggregation,
} from "../../src/services/workspace/mutations.js";

// Mock completeNodeConfiguration so tests don't need corpus/repo files
vi.mock("../../src/services/config/intelligent.js", () => ({
  completeNodeConfiguration: vi.fn(async (client: any, params: { workspaceID: string; nodeID: string }) => {
    const node = await client.get(`/api/v1/workspaces/${params.workspaceID}/nodes/${params.nodeID}`);
    return {
      node,
      schemaSource: "corpus",
      classification: { required: [], conditionalRequired: [], optionalWithDefaults: [], contextual: [], columnSelectors: [] },
      context: { hasMultipleSources: false, hasAggregates: false, hasTimestampColumns: false, hasType2Pattern: false, materializationType: "table" },
      appliedConfig: {},
      configChanges: { required: {}, contextual: {}, preserved: {}, defaults: {} },
      columnAttributeChanges: { applied: [], reasoning: [] },
      reasoning: [],
      detectedPatterns: { candidateColumns: [] },
    };
  }),
}));

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ id: "new-node" }),
    put: vi.fn().mockResolvedValue({ id: "updated-node" }),
    delete: vi.fn(),
  };
}

describe("Node Type Format Validation", () => {
  describe("Package-prefixed node types", () => {
    it("accepts exact package-prefixed node type match", async () => {
      const client = createMockClient();
      client.post.mockResolvedValue({ id: "new-node", nodeType: "IncrementalLoading:::230" });
      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({
            data: [
              { id: "existing-1", nodeType: "Stage" },
              { id: "existing-2", nodeType: "IncrementalLoading:::230" },
            ],
          });
        }
        return Promise.resolve({
          id: "new-node",
          nodeType: "IncrementalLoading:::230",
          config: {},
        });
      });

      const result = await createWorkspaceNodeFromScratch(client as any, {
        workspaceID: "ws-1",
        nodeType: "IncrementalLoading:::230",
        completionLevel: "created",
        goal: "incremental load with high-water mark tracking",
      });

      expect(result).toHaveProperty("node");
      expect(client.post).toHaveBeenCalledWith("/api/v1/workspaces/ws-1/nodes", {
        nodeType: "IncrementalLoading:::230",
      });
    });

    it("accepts bare ID when a matching package-prefixed type is observed", async () => {
      const client = createMockClient();
      client.post.mockResolvedValue({ id: "new-node", nodeType: "230" });
      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({
            data: [
              { id: "existing-1", nodeType: "Stage" },
              { id: "existing-2", nodeType: "IncrementalLoading:::230" },
            ],
          });
        }
        return Promise.resolve({
          id: "new-node",
          nodeType: "230",
          config: {},
        });
      });

      const result = await createWorkspaceNodeFromScratch(client as any, {
        workspaceID: "ws-1",
        nodeType: "230",
        completionLevel: "created",
      });

      expect(result).toHaveProperty("node");
      expect(client.post).toHaveBeenCalledWith("/api/v1/workspaces/ws-1/nodes", {
        nodeType: "230",
      });
    });

    it("still attempts creation when no matching package-prefixed type is observed", async () => {
      const client = createMockClient();
      client.post.mockResolvedValue({ id: "new-node", nodeType: "999" });
      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({
            data: [
              { id: "existing-1", nodeType: "Stage" },
              { id: "existing-2", nodeType: "IncrementalLoading:::230" },
            ],
          });
        }
        return Promise.resolve({ id: "new-node", nodeType: "999", config: {} });
      });

      const result = await createWorkspaceNodeFromScratch(client as any, {
        workspaceID: "ws-1",
        nodeType: "999",
        completionLevel: "created",
      });

      expect(result).toHaveProperty("node");
      expect(client.post).toHaveBeenCalledWith("/api/v1/workspaces/ws-1/nodes", {
        nodeType: "999",
      });
    });

    it("accepts simple node type names", async () => {
      const client = createMockClient();
      client.post.mockResolvedValue({ id: "new-node", nodeType: "Stage" });
      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({
            data: [
              { id: "existing-1", nodeType: "Stage" },
              { id: "existing-2", nodeType: "persistentStage" },
            ],
          });
        }
        return Promise.resolve({
          id: "new-node",
          nodeType: "Stage",
          config: {},
        });
      });

      const result = await createWorkspaceNodeFromScratch(client as any, {
        workspaceID: "ws-1",
        nodeType: "Stage",
        completionLevel: "created",
      });

      expect(result).toHaveProperty("node");
      expect(client.post).toHaveBeenCalledWith("/api/v1/workspaces/ws-1/nodes", {
        nodeType: "Stage",
      });
    });
  });

  describe("buildUpdatedWorkspaceNodeBody preserves API-required fields", () => {
    it("preserves table and overrideSQL from current node", () => {
      const current = {
        id: "node-1",
        name: "MY_NODE",
        nodeType: "Stage",
        materializationType: "table",
        table: "MY_NODE",
        overrideSQL: false,
        config: {},
        metadata: { columns: [] },
      };

      const result = buildUpdatedWorkspaceNodeBody(current, {
        name: "RENAMED_NODE",
      });

      expect(result.table).toBe("MY_NODE");
      expect(result.overrideSQL).toBe(false);
      expect(result.name).toBe("RENAMED_NODE");
    });

    it("preserves columnID from existing columns matched by name", () => {
      const current = {
        id: "node-1",
        name: "MY_NODE",
        nodeType: "Stage",
        materializationType: "table",
        table: "MY_NODE",
        overrideSQL: false,
        config: {},
        metadata: {
          columns: [
            { name: "COL_A", columnID: "uuid-a", transform: "" },
            { name: "COL_B", columnID: "uuid-b", transform: "" },
          ],
        },
      };

      const result = buildUpdatedWorkspaceNodeBody(current, {
        metadata: {
          columns: [
            { name: "COL_A", transform: '"SRC"."COL_A"' },
            { name: "COL_B", transform: '"SRC"."COL_B"' },
            { name: "COL_NEW", transform: '"SRC"."COL_NEW"' },
          ],
        },
      });

      const metadata = result.metadata as Record<string, unknown>;
      const columns = metadata.columns as Array<Record<string, unknown>>;
      expect(columns).toHaveLength(3);
      expect(columns[0].columnID).toBe("uuid-a");
      expect(columns[1].columnID).toBe("uuid-b");
      expect(typeof columns[2].columnID).toBe("string"); // new column gets auto-generated UUID
    });

    it("does not overwrite columnID already present on new columns", () => {
      const current = {
        id: "node-1",
        name: "MY_NODE",
        nodeType: "Stage",
        materializationType: "table",
        config: {},
        metadata: {
          columns: [
            { name: "COL_A", columnID: "uuid-old", transform: "" },
          ],
        },
      };

      const result = buildUpdatedWorkspaceNodeBody(current, {
        metadata: {
          columns: [
            { name: "COL_A", columnID: "uuid-new", transform: '"SRC"."COL_A"' },
          ],
        },
      });

      const metadata = result.metadata as Record<string, unknown>;
      const columns = metadata.columns as Array<Record<string, unknown>>;
      expect(columns[0].columnID).toBe("uuid-new");
    });

    it("strips backslash-escaped quotes from transforms", () => {
      const current = {
        id: "node-1",
        name: "STG_CUSTOMER",
        nodeType: "Stage",
        materializationType: "table",
        table: "STG_CUSTOMER",
        overrideSQL: false,
        config: {},
        metadata: {
          columns: [
            {
              name: "CITY",
              columnID: "uuid-1",
              sources: [{ transform: "", columnReferences: [{ nodeID: "src-1" }] }],
            },
          ],
        },
      };

      // Agent over-escapes: \" instead of "
      const result = buildUpdatedWorkspaceNodeBody(current, {
        metadata: {
          columns: [
            { name: "CITY", transform: String.raw`UPPER(\"CUSTOMER_LOYALTY\".\"CITY\")` },
          ],
        },
      });

      const metadata = result.metadata as Record<string, unknown>;
      const columns = metadata.columns as Array<Record<string, unknown>>;
      const city = columns.find((c) => c.name === "CITY")!;
      const sources = city.sources as Array<Record<string, unknown>>;
      // Backslashes should be stripped — clean double quotes
      expect(sources[0].transform).toBe('UPPER("CUSTOMER_LOYALTY"."CITY")');
    });

    it("propagates non-passthrough transforms into inherited sources", () => {
      const current = {
        id: "node-1",
        name: "STG_CUSTOMER",
        nodeType: "Stage",
        materializationType: "table",
        table: "STG_CUSTOMER",
        overrideSQL: false,
        config: {},
        metadata: {
          columns: [
            {
              name: "CUSTOMER_ID",
              columnID: "uuid-1",
              sources: [{ transform: "", columnReferences: [{ nodeID: "src-1" }] }],
            },
            {
              name: "CITY",
              columnID: "uuid-2",
              sources: [{ transform: "", columnReferences: [{ nodeID: "src-1" }] }],
            },
            {
              name: "POSTAL_CODE",
              columnID: "uuid-3",
              sources: [{ transform: "", columnReferences: [{ nodeID: "src-1" }] }],
            },
          ],
        },
      };

      // Agent sends columns with transforms — UPPER() and LEFT() are real transforms
      const result = buildUpdatedWorkspaceNodeBody(current, {
        metadata: {
          columns: [
            { name: "CUSTOMER_ID", transform: '"CUSTOMER_LOYALTY"."CUSTOMER_ID"' },
            { name: "CITY", transform: 'UPPER("CUSTOMER_LOYALTY"."CITY")' },
            { name: "POSTAL_CODE", transform: 'LEFT("CUSTOMER_LOYALTY"."POSTAL_CODE", 5)' },
          ],
        },
      });

      const metadata = result.metadata as Record<string, unknown>;
      const columns = metadata.columns as Array<Record<string, unknown>>;

      // CUSTOMER_ID has a passthrough transform — should NOT propagate to sources
      const custId = columns.find((c) => c.name === "CUSTOMER_ID")!;
      const custIdSources = custId.sources as Array<Record<string, unknown>>;
      expect(custIdSources[0].transform).toBe(""); // passthrough stripped, original source preserved

      // CITY has UPPER() — should propagate to sources[0].transform
      const city = columns.find((c) => c.name === "CITY")!;
      const citySources = city.sources as Array<Record<string, unknown>>;
      expect(citySources[0].transform).toBe('UPPER("CUSTOMER_LOYALTY"."CITY")');

      // POSTAL_CODE has LEFT() — should propagate to sources[0].transform
      const postal = columns.find((c) => c.name === "POSTAL_CODE")!;
      const postalSources = postal.sources as Array<Record<string, unknown>>;
      expect(postalSources[0].transform).toBe('LEFT("CUSTOMER_LOYALTY"."POSTAL_CODE", 5)');
    });

    it("creates synthetic sources for computed columns with transforms but no predecessor match", () => {
      const current = {
        id: "node-1",
        name: "STG_CUSTOMER",
        nodeType: "Stage",
        materializationType: "table",
        table: "STG_CUSTOMER",
        overrideSQL: false,
        config: {},
        metadata: {
          columns: [
            {
              name: "E_MAIL",
              columnID: "uuid-1",
              sources: [{ transform: "", columnReferences: [{ nodeID: "src-1" }] }],
            },
            {
              name: "PHONE_NUMBER",
              columnID: "uuid-2",
              sources: [{ transform: "", columnReferences: [{ nodeID: "src-1" }] }],
            },
          ],
        },
      };

      // Agent sends columns including CONTACT_INFO — a computed column not on the predecessor
      const result = buildUpdatedWorkspaceNodeBody(current, {
        metadata: {
          columns: [
            { name: "E_MAIL" },
            { name: "PHONE_NUMBER" },
            { name: "CONTACT_INFO", transform: 'COALESCE("CUSTOMER_LOYALTY"."E_MAIL", "CUSTOMER_LOYALTY"."PHONE_NUMBER")' },
          ],
        },
      });

      const metadata = result.metadata as Record<string, unknown>;
      const columns = metadata.columns as Array<Record<string, unknown>>;

      // CONTACT_INFO has no predecessor match — should get synthetic sources with the transform
      const contactInfo = columns.find((c) => c.name === "CONTACT_INFO")!;
      expect(contactInfo.sources).toBeDefined();
      const sources = contactInfo.sources as Array<Record<string, unknown>>;
      expect(sources).toHaveLength(1);
      expect(sources[0].transform).toBe('COALESCE("CUSTOMER_LOYALTY"."E_MAIL", "CUSTOMER_LOYALTY"."PHONE_NUMBER")');
      expect(sources[0].columnReferences).toEqual([]);
    });

    it("always preserves overrideSQL from current even if body tries to set it", () => {
      const current = {
        id: "node-1",
        name: "MY_NODE",
        nodeType: "Stage",
        materializationType: "table",
        overrideSQL: false,
        config: {},
        metadata: { columns: [] },
      };

      // The assertNoSqlOverridePayload would block this at the tool level,
      // but buildUpdatedWorkspaceNodeBody ensures it's always from current
      const result = buildUpdatedWorkspaceNodeBody(current, {
        config: { someSetting: true },
      });

      expect(result.overrideSQL).toBe(false);
    });
  });

  describe("createWorkspaceNodeFromPredecessor with package-prefixed types", () => {
    it("accepts bare ID matching package-prefixed type", async () => {
      const client = createMockClient();
      client.post.mockResolvedValue({ id: "new-node", nodeType: "230" });
      client.get.mockImplementation((path: string) => {
        if (path === "/api/v1/workspaces/ws-1/nodes") {
          return Promise.resolve({
            data: [
              { id: "existing-1", nodeType: "Stage" },
              { id: "existing-2", nodeType: "IncrementalLoading:::230" },
            ],
          });
        }
        if (path === "/api/v1/workspaces/ws-1/nodes/pred-1") {
          return Promise.resolve({
            id: "pred-1",
            name: "SOURCE_TABLE",
            metadata: { columns: [{ name: "COL1" }] },
          });
        }
        return Promise.resolve({
          id: "new-node",
          name: "INC_TABLE",
          metadata: {
            columns: [
              {
                name: "COL1",
                sources: [{ columnReferences: [{ nodeID: "pred-1" }] }],
              },
            ],
            sourceMapping: [{ dependencies: [{ nodeName: "SOURCE_TABLE" }] }],
          },
        });
      });

      const result = await createWorkspaceNodeFromPredecessor(client as any, {
        workspaceID: "ws-1",
        nodeType: "230",
        predecessorNodeIDs: ["pred-1"],
      });

      expect(result).toHaveProperty("node");
      expect(client.post).toHaveBeenCalledWith("/api/v1/workspaces/ws-1/nodes", {
        nodeType: "230",
        predecessorNodeIDs: ["pred-1"],
      });
    });
  });
});

describe("applyJoinCondition", () => {
  function buildMultiPredecessorNode() {
    return {
      id: "join-node",
      name: "INT_JOINED",
      nodeType: "View",
      config: {},
      metadata: {
        columns: [
          {
            name: "CUSTOMER_ID",
            sources: [
              { columnReferences: [{ nodeID: "pred-cust", columnName: "CUSTOMER_ID" }] },
            ],
          },
          {
            name: "ORDER_ID",
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
    };
  }

  function buildPredecessorNode(name: string, locationName: string, columns: string[]) {
    return {
      id: `pred-${name.toLowerCase()}`,
      name,
      nodeType: "Stage",
      locationName,
      storageLocations: [{ name: locationName }],
      config: {},
      metadata: {
        columns: columns.map((c) => ({
          name: c,
          dataType: "VARCHAR",
          sources: [],
        })),
        sourceMapping: [{ name, dependencies: [], join: { joinCondition: "" }, customSQL: { customSQL: "" }, aliases: {}, noLinkRefs: [] }],
      },
    };
  }

  it("generates FROM/JOIN/ON with {{ ref() }} syntax for two predecessors", async () => {
    const client = createMockClient();
    const joinNode = buildMultiPredecessorNode();
    const custNode = buildPredecessorNode("STG_CUSTOMER", "STAGING", ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME"]);
    const ordersNode = buildPredecessorNode("STG_ORDERS", "STAGING", ["ORDER_ID", "CUSTOMER_ID", "ORDER_TOTAL"]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(joinNode);
      if (path.includes("pred-cust")) return Promise.resolve(custNode);
      if (path.includes("pred-orders")) return Promise.resolve(ordersNode);
      return Promise.resolve({ data: [] });
    });

    const result = await applyJoinCondition(client as any, {
      workspaceID: "ws-1",
      nodeID: "join-node",
    });

    expect(result.joinCondition).toContain("FROM {{ ref('STAGING', 'STG_CUSTOMER') }}");
    expect(result.joinCondition).toContain("INNER JOIN {{ ref('STAGING', 'STG_ORDERS') }}");
    expect(result.joinCondition).toContain('"STG_CUSTOMER"."CUSTOMER_ID" = "STG_ORDERS"."CUSTOMER_ID"');
    expect(result.predecessors).toHaveLength(2);
    expect(result.warnings).toHaveLength(0);

    // Verify it wrote the joinCondition to the node
    expect(client.put).toHaveBeenCalled();
  });

  it("uses specified joinType", async () => {
    const client = createMockClient();
    const joinNode = buildMultiPredecessorNode();
    const custNode = buildPredecessorNode("STG_CUSTOMER", "STAGING", ["CUSTOMER_ID", "FIRST_NAME"]);
    const ordersNode = buildPredecessorNode("STG_ORDERS", "STAGING", ["ORDER_ID", "CUSTOMER_ID"]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(joinNode);
      if (path.includes("pred-cust")) return Promise.resolve(custNode);
      if (path.includes("pred-orders")) return Promise.resolve(ordersNode);
      return Promise.resolve({ data: [] });
    });

    const result = await applyJoinCondition(client as any, {
      workspaceID: "ws-1",
      nodeID: "join-node",
      joinType: "LEFT JOIN",
    });

    expect(result.joinCondition).toContain("LEFT JOIN {{ ref('STAGING', 'STG_ORDERS') }}");
    expect(result.joinCondition).not.toContain("INNER JOIN");
  });

  it("appends WHERE and QUALIFY clauses", async () => {
    const client = createMockClient();
    const joinNode = buildMultiPredecessorNode();
    const custNode = buildPredecessorNode("STG_CUSTOMER", "STAGING", ["CUSTOMER_ID", "ACTIVE"]);
    const ordersNode = buildPredecessorNode("STG_ORDERS", "STAGING", ["ORDER_ID", "CUSTOMER_ID"]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(joinNode);
      if (path.includes("pred-cust")) return Promise.resolve(custNode);
      if (path.includes("pred-orders")) return Promise.resolve(ordersNode);
      return Promise.resolve({ data: [] });
    });

    const result = await applyJoinCondition(client as any, {
      workspaceID: "ws-1",
      nodeID: "join-node",
      whereClause: '"STG_CUSTOMER"."ACTIVE" = TRUE',
      qualifyClause: "ROW_NUMBER() OVER (PARTITION BY \"STG_CUSTOMER\".\"CUSTOMER_ID\" ORDER BY \"STG_ORDERS\".\"ORDER_ID\" DESC) = 1",
    });

    expect(result.joinCondition).toContain('WHERE "STG_CUSTOMER"."ACTIVE" = TRUE');
    expect(result.joinCondition).toContain("QUALIFY ROW_NUMBER()");
  });

  it("uses joinColumnOverrides for mismatched column names", async () => {
    const client = createMockClient();
    const joinNode = buildMultiPredecessorNode();
    const custNode = buildPredecessorNode("STG_CUSTOMER", "STAGING", ["CUST_ID", "FIRST_NAME"]);
    const ordersNode = buildPredecessorNode("STG_ORDERS", "STAGING", ["ORDER_ID", "CUSTOMER_ID"]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(joinNode);
      if (path.includes("pred-cust")) return Promise.resolve(custNode);
      if (path.includes("pred-orders")) return Promise.resolve(ordersNode);
      return Promise.resolve({ data: [] });
    });

    const result = await applyJoinCondition(client as any, {
      workspaceID: "ws-1",
      nodeID: "join-node",
      joinColumnOverrides: [{
        leftPredecessor: "STG_CUSTOMER",
        rightPredecessor: "STG_ORDERS",
        leftColumn: "CUST_ID",
        rightColumn: "CUSTOMER_ID",
      }],
    });

    expect(result.joinCondition).toContain('"STG_CUSTOMER"."CUST_ID" = "STG_ORDERS"."CUSTOMER_ID"');
  });

  it("throws when node has fewer than 2 predecessors", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({
      id: "single-pred-node",
      name: "STG_CUSTOMER",
      nodeType: "Stage",
      config: {},
      metadata: {
        columns: [
          {
            name: "CUSTOMER_ID",
            sources: [
              { columnReferences: [{ nodeID: "src-1", columnName: "CUSTOMER_ID" }] },
            ],
          },
        ],
        sourceMapping: [
          {
            name: "STG_CUSTOMER",
            dependencies: [{ locationName: "RAW", nodeName: "CUSTOMER" }],
            aliases: { CUSTOMER: "src-1" },
            join: { joinCondition: "" },
            customSQL: { customSQL: "" },
            noLinkRefs: [],
          },
        ],
      },
    });

    await expect(
      applyJoinCondition(client as any, {
        workspaceID: "ws-1",
        nodeID: "single-pred-node",
      })
    ).rejects.toThrow("2+ predecessors");
  });

  it("warns when predecessors have no common columns", async () => {
    const client = createMockClient();
    const joinNode = buildMultiPredecessorNode();
    const custNode = buildPredecessorNode("STG_CUSTOMER", "STAGING", ["FIRST_NAME", "LAST_NAME"]);
    const ordersNode = buildPredecessorNode("STG_ORDERS", "STAGING", ["ORDER_ID", "ORDER_TOTAL"]);

    client.get.mockImplementation((path: string) => {
      if (path.includes("join-node")) return Promise.resolve(joinNode);
      if (path.includes("pred-cust")) return Promise.resolve(custNode);
      if (path.includes("pred-orders")) return Promise.resolve(ordersNode);
      return Promise.resolve({ data: [] });
    });

    const result = await applyJoinCondition(client as any, {
      workspaceID: "ws-1",
      nodeID: "join-node",
    });

    expect(result.warnings.length).toBeGreaterThan(0);
    expect(result.warnings.some((w) => w.includes("No common columns"))).toBe(true);
  });
});

describe("convertJoinToAggregation", () => {
  it("writes generated joinCondition to node sourceMapping", async () => {
    const client = createMockClient();

    const joinNode = {
      id: "fact-node",
      name: "FACT_CLV",
      nodeType: "Fact",
      config: {},
      metadata: {
        columns: [
          { name: "CUSTOMER_ID", transform: '"STG_CUSTOMER"."CUSTOMER_ID"', dataType: "VARCHAR", sources: [] },
          { name: "ORDER_ID", transform: '"STG_ORDERS"."ORDER_ID"', dataType: "VARCHAR", sources: [] },
          { name: "ORDER_TOTAL", transform: '"STG_ORDERS"."ORDER_TOTAL"', dataType: "NUMBER", sources: [] },
        ],
        sourceMapping: [
          {
            name: "FACT_CLV",
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
    };

    const custNode = {
      id: "pred-cust",
      name: "STG_CUSTOMER",
      nodeType: "Stage",
      locationName: "STAGING",
      storageLocations: [{ name: "STAGING" }],
      config: {},
      metadata: {
        columns: [
          { name: "CUSTOMER_ID", dataType: "VARCHAR", sources: [] },
          { name: "FIRST_NAME", dataType: "VARCHAR", sources: [] },
        ],
        sourceMapping: [],
      },
    };

    const ordersNode = {
      id: "pred-orders",
      name: "STG_ORDERS",
      nodeType: "Stage",
      locationName: "STAGING",
      storageLocations: [{ name: "STAGING" }],
      config: {},
      metadata: {
        columns: [
          { name: "ORDER_ID", dataType: "VARCHAR", sources: [] },
          { name: "CUSTOMER_ID", dataType: "VARCHAR", sources: [] },
          { name: "ORDER_TOTAL", dataType: "NUMBER", sources: [] },
        ],
        sourceMapping: [],
      },
    };

    client.get.mockImplementation((path: string) => {
      if (path.includes("fact-node")) return Promise.resolve(joinNode);
      if (path.includes("pred-cust")) return Promise.resolve(custNode);
      if (path.includes("pred-orders")) return Promise.resolve(ordersNode);
      return Promise.resolve({ data: [] });
    });

    // replaceWorkspaceNodeColumns calls get then put; after that convertJoinToAggregation
    // re-fetches the node to get fresh sourceMapping. Mock put to return success.
    client.put.mockResolvedValue({ id: "fact-node" });

    const result = await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "fact-node",
      groupByColumns: ['"STG_CUSTOMER"."CUSTOMER_ID"'],
      aggregates: [
        { name: "TOTAL_ORDERS", function: "COUNT", expression: 'DISTINCT "STG_ORDERS"."ORDER_ID"' },
        { name: "LIFETIME_VALUE", function: "SUM", expression: '"STG_ORDERS"."ORDER_TOTAL"' },
      ],
    });

    // Verify that put was called with a joinCondition containing FROM/JOIN/ON/GROUP BY
    const putCalls = client.put.mock.calls;
    const joinConditionCall = putCalls.find(
      (call: unknown[]) => {
        const body = call[1] as Record<string, unknown>;
        const meta = body?.metadata as Record<string, unknown> | undefined;
        const sm = Array.isArray(meta?.sourceMapping) ? meta.sourceMapping : [];
        const firstMapping = sm[0] as Record<string, unknown> | undefined;
        const joinObj = firstMapping?.join as Record<string, unknown> | undefined;
        const jc = typeof joinObj?.joinCondition === "string" ? joinObj.joinCondition : "";
        return jc.includes("GROUP BY") && jc.includes("INNER JOIN");
      }
    );

    expect(joinConditionCall).toBeDefined();
    const writtenBody = (joinConditionCall as unknown[])[1] as Record<string, unknown>;
    const writtenSM = (writtenBody.metadata as Record<string, unknown>).sourceMapping as unknown[];
    const writtenJC = ((writtenSM[0] as Record<string, unknown>).join as Record<string, unknown>).joinCondition as string;
    expect(writtenJC).toContain("{{ ref('STAGING', 'STG_CUSTOMER') }}");
    expect(writtenJC).toContain("{{ ref('STAGING', 'STG_ORDERS') }}");
    expect(writtenJC).toContain("CUSTOMER_ID");
    expect(writtenJC).toContain("GROUP BY");
    expect(result.joinSQL).toBeDefined();
    expect(result.groupByAnalysis).toBeDefined();
  });
});

describe("createWorkspaceNodeFromPredecessor single-call workflow", () => {
  function buildPredecessorNodeResponse(nodeID: string, name: string, columns: string[]) {
    return {
      id: nodeID,
      name,
      nodeType: "Stage",
      locationName: "STAGING",
      config: {},
      metadata: {
        columns: columns.map((colName) => ({
          name: colName,
          dataType: "VARCHAR",
          columnID: `col-${colName.toLowerCase()}`,
          sources: [{ columnReferences: [{ nodeID }], transform: `"${name}"."${colName}"` }],
          columnReference: { stepCounter: nodeID, columnCounter: `col-${colName.toLowerCase()}` },
        })),
        sourceMapping: [
          {
            name,
            dependencies: [{ locationName: "STAGING", nodeName: name }],
            aliases: { [name]: nodeID },
            join: { joinCondition: `FROM {{ ref('STAGING', '${name}') }}` },
          },
        ],
      },
    };
  }

  function buildCreatedNodeResponse(predNode: ReturnType<typeof buildPredecessorNodeResponse>, newID: string) {
    return {
      id: newID,
      name: predNode.name,
      nodeType: "Stage",
      config: {},
      metadata: {
        columns: predNode.metadata.columns.map((col: any) => ({
          ...col,
          columnID: `new-${col.name.toLowerCase()}`,
          columnReference: { stepCounter: newID, columnCounter: `new-${col.name.toLowerCase()}` },
        })),
        sourceMapping: [
          {
            name: predNode.name,
            dependencies: [{ locationName: "STAGING", nodeName: predNode.name }],
            aliases: { [predNode.name]: predNode.id },
            join: { joinCondition: `FROM {{ ref('STAGING', '${predNode.name}') }}` },
          },
        ],
      },
    };
  }

  it("creates a node with columns and whereCondition in a single call", async () => {
    const client = createMockClient();
    const pred = buildPredecessorNodeResponse("pred-1", "LOCATION", ["LOCATION_ID", "CITY", "COUNTRY"]);
    const created = buildCreatedNodeResponse(pred, "new-node");

    client.post.mockResolvedValue({ id: "new-node" });
    let putCallCount = 0;
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes/pred-1") return Promise.resolve(pred);
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node") return Promise.resolve(created);
      if (path === "/api/v1/workspaces/ws-1/nodes") return Promise.resolve({ data: [{ nodeType: "Stage" }] });
      return Promise.resolve({ data: [] });
    });
    client.put.mockImplementation(() => {
      putCallCount++;
      return Promise.resolve(created);
    });

    const result = await createWorkspaceNodeFromPredecessor(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      predecessorNodeIDs: ["pred-1"],
      changes: { name: "STG_LOCATION" },
      columns: [
        { name: "LOCATION_ID" },
        { name: "CITY", transform: "UPPER(\"LOCATION\".\"CITY\")" },
        { name: "COUNTRY" },
      ],
      whereCondition: "\"LOCATION\".\"LOCATION_ID\" IS NOT NULL",
    });

    expect(result).toHaveProperty("node");
    // Should have: 1 PUT for changes (name), 1 PUT for replaceWorkspaceNodeColumns, 1 PUT for config completion
    expect(putCallCount).toBeGreaterThanOrEqual(2);
  });

  it("creates an aggregation node with groupByColumns and aggregates in a single call", async () => {
    const client = createMockClient();
    const pred = buildPredecessorNodeResponse("pred-1", "STG_ORDER_HEADER", ["ORDER_ID", "ORDER_CURRENCY", "ORDER_AMOUNT"]);
    const created = buildCreatedNodeResponse(pred, "new-node");

    client.post.mockResolvedValue({ id: "new-node" });
    client.get.mockImplementation((path: string) => {
      if (path === "/api/v1/workspaces/ws-1/nodes/pred-1") return Promise.resolve(pred);
      if (path === "/api/v1/workspaces/ws-1/nodes/new-node") return Promise.resolve(created);
      if (path === "/api/v1/workspaces/ws-1/nodes") return Promise.resolve({ data: [{ nodeType: "Stage" }] });
      return Promise.resolve({ data: [] });
    });
    client.put.mockResolvedValue(created);

    const result = await createWorkspaceNodeFromPredecessor(client as any, {
      workspaceID: "ws-1",
      nodeType: "Stage",
      predecessorNodeIDs: ["pred-1"],
      changes: { name: "STG_ORDER_TOTALS" },
      groupByColumns: ["\"STG_ORDER_HEADER\".\"ORDER_ID\"", "\"STG_ORDER_HEADER\".\"ORDER_CURRENCY\""],
      aggregates: [
        { name: "TOTAL_ORDER_AMOUNT", function: "SUM", expression: "\"STG_ORDER_HEADER\".\"ORDER_AMOUNT\"" },
      ],
    });

    expect(result).toHaveProperty("node");
    expect(result).toHaveProperty("groupByAnalysis");
    expect(result).toHaveProperty("joinSQL");
  });

  it("rejects columns combined with groupByColumns", async () => {
    const client = createMockClient();

    await expect(
      createWorkspaceNodeFromPredecessor(client as any, {
        workspaceID: "ws-1",
        nodeType: "Stage",
        predecessorNodeIDs: ["pred-1"],
        columns: [{ name: "COL1" }],
        groupByColumns: ["COL1"],
        aggregates: [{ name: "TOTAL", function: "SUM", expression: "COL2" }],
      })
    ).rejects.toThrow("Cannot provide both");
  });

  it("rejects aggregates without groupByColumns", async () => {
    const client = createMockClient();

    await expect(
      createWorkspaceNodeFromPredecessor(client as any, {
        workspaceID: "ws-1",
        nodeType: "Stage",
        predecessorNodeIDs: ["pred-1"],
        aggregates: [{ name: "TOTAL", function: "SUM", expression: "COL2" }],
      })
    ).rejects.toThrow("'aggregates' requires 'groupByColumns'");
  });

  it("rejects groupByColumns without aggregates", async () => {
    const client = createMockClient();

    await expect(
      createWorkspaceNodeFromPredecessor(client as any, {
        workspaceID: "ws-1",
        nodeType: "Stage",
        predecessorNodeIDs: ["pred-1"],
        groupByColumns: ["COL1"],
      })
    ).rejects.toThrow("'groupByColumns' requires 'aggregates'");
  });

  it("rejects whereCondition with groupByColumns", async () => {
    const client = createMockClient();

    await expect(
      createWorkspaceNodeFromPredecessor(client as any, {
        workspaceID: "ws-1",
        nodeType: "Stage",
        predecessorNodeIDs: ["pred-1"],
        groupByColumns: ["COL1"],
        aggregates: [{ name: "TOTAL", function: "SUM", expression: "COL2" }],
        whereCondition: "COL1 IS NOT NULL",
      })
    ).rejects.toThrow("'whereCondition' cannot be combined with");
  });
});
