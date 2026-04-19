import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../src/coalesce/api/nodes.js", () => ({
  listWorkspaceNodes: vi.fn(),
  getWorkspaceNode: vi.fn(),
}));

vi.mock("../../src/services/workspace/mutations.js", () => ({
  listWorkspaceNodeTypes: vi.fn(),
}));

import {
  resolveSqlRefsToWorkspaceNodes,
  getSourceNodesByID,
  getWorkspaceNodeTypeInventory,
  applyWorkspaceNodeTypeValidation,
} from "../../src/services/pipelines/workspace-resolution.js";
import { listWorkspaceNodes, getWorkspaceNode } from "../../src/coalesce/api/nodes.js";
import { listWorkspaceNodeTypes } from "../../src/services/workspace/mutations.js";
import type { CoalesceClient } from "../../src/client.js";
import type {
  PipelinePlan,
  ParsedSqlSourceRef,
  WorkspaceNodeTypeInventory,
} from "../../src/services/pipelines/planning-types.js";
import { CoalesceApiError } from "../../src/client.js";

const mockListWorkspaceNodes = vi.mocked(listWorkspaceNodes);
const mockGetWorkspaceNode = vi.mocked(getWorkspaceNode);
const mockListWorkspaceNodeTypes = vi.mocked(listWorkspaceNodeTypes);

function createMockClient(): CoalesceClient {
  return {} as CoalesceClient;
}

function makeRef(overrides: Partial<ParsedSqlSourceRef> = {}): ParsedSqlSourceRef {
  return {
    locationName: "",
    nodeName: "STG_ORDERS",
    alias: null,
    nodeID: null,
    sourceStyle: "table_name",
    locationCandidates: [],
    relationStart: 0,
    relationEnd: 10,
    ...overrides,
  };
}

function makeEmptyPlan(overrides: Partial<PipelinePlan> = {}): PipelinePlan {
  return {
    version: 1,
    intent: "sql",
    status: "ready",
    workspaceID: "ws-1",
    platform: null,
    goal: null,
    sql: null,
    nodes: [],
    assumptions: [],
    openQuestions: [],
    warnings: [],
    supportedNodeTypes: [],
    ...overrides,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
});

// ---------------------------------------------------------------------------
// resolveSqlRefsToWorkspaceNodes
// ---------------------------------------------------------------------------
describe("resolveSqlRefsToWorkspaceNodes", () => {
  const client = createMockClient();
  const wsID = "ws-1";

  it("returns an open question when refs array is empty", async () => {
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, []);
    expect(result.openQuestions.length).toBe(1);
    expect(result.openQuestions[0]).toMatch(/upstream Coalesce node/i);
    expect(result.refs).toEqual([]);
    expect(result.predecessorNodes).toEqual({});
  });

  it("resolves a single exact match by name", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [{ id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" }],
    });
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-1",
      name: "STG_ORDERS",
      nodeType: "Stage",
      locationName: "RAW",
    });

    const ref = makeRef();
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBe("n-1");
    expect(ref.locationName).toBe("RAW");
    expect(result.openQuestions).toEqual([]);
    expect(result.predecessorNodes["n-1"]).toBeDefined();
  });

  it("produces an open question when no node matches", async () => {
    mockListWorkspaceNodes.mockResolvedValue({ data: [] });

    const ref = makeRef({ nodeName: "NONEXISTENT" });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBeNull();
    expect(result.openQuestions.length).toBe(1);
    expect(result.openQuestions[0]).toMatch(/Could not resolve/);
  });

  it("resolves with location hint when multiple names match", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" },
        { id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: "CURATED" },
      ],
    });
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-1",
      name: "STG_ORDERS",
      nodeType: "Stage",
      locationName: "RAW",
    });

    const ref = makeRef({ locationName: "RAW", locationCandidates: [] });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBe("n-1");
    expect(result.openQuestions).toEqual([]);
  });

  it("uses locationCandidates when locationName is empty", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" },
        { id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: "CURATED" },
      ],
    });
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-2",
      name: "STG_ORDERS",
      nodeType: "Stage",
      locationName: "CURATED",
    });

    const ref = makeRef({ locationName: "", locationCandidates: ["CURATED"] });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBe("n-2");
    expect(ref.locationName).toBe("CURATED");
    expect(result.openQuestions).toEqual([]);
  });

  it("produces an open question when multiple location-hinted matches exist", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" },
        { id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" },
      ],
    });

    const ref = makeRef({ locationName: "RAW" });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBeNull();
    expect(result.openQuestions.length).toBe(1);
    expect(result.openQuestions[0]).toMatch(/Multiple workspace nodes matched/);
  });

  it("falls back to detailed fetch when multiple names match without location hints", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" },
        { id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: "CURATED" },
      ],
    });
    // No location hints — falls through to multi-match branch,
    // getWorkspaceNode fetches details for each
    mockGetWorkspaceNode
      .mockResolvedValueOnce({ id: "n-1", name: "STG_ORDERS", locationName: "RAW" })
      .mockResolvedValueOnce({ id: "n-2", name: "STG_ORDERS", locationName: "CURATED" });

    const ref = makeRef({ locationCandidates: [] });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    // Without any location hint at all, ambiguity produces an open question
    expect(result.openQuestions.length).toBe(1);
    expect(result.openQuestions[0]).toMatch(/Multiple workspace nodes named/);
  });

  it("resolves ambiguous names via detailed fetch when location candidate matches one", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
        { id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
      ],
    });
    // Index entries have null locationName → no index-level hint match → falls to detailed fetch
    mockGetWorkspaceNode
      .mockResolvedValueOnce({ id: "n-1", name: "STG_ORDERS", locationName: "RAW" })
      .mockResolvedValueOnce({ id: "n-2", name: "STG_ORDERS", locationName: "CURATED" });

    const ref = makeRef({ locationCandidates: ["RAW"] });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBe("n-1");
    expect(result.openQuestions).toEqual([]);
  });

  it("warns and produces open question when detailed fetch fails for ambiguous matches", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
        { id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
      ],
    });
    mockGetWorkspaceNode.mockRejectedValue(new Error("network timeout"));

    const ref = makeRef({ locationCandidates: ["RAW"] });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBeNull();
    expect(result.warnings.length).toBeGreaterThan(0);
    expect(result.warnings[0]).toMatch(/Could not fetch details/);
    expect(result.openQuestions.length).toBe(1);
  });

  it("rethrows non-recoverable errors (401/403) from detailed fetch", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
        { id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
      ],
    });
    mockGetWorkspaceNode.mockRejectedValue(
      new CoalesceApiError("Unauthorized", 401, "GET", "/api/v1/workspaces/ws-1/nodes/n-1")
    );

    const ref = makeRef({ locationCandidates: ["RAW"] });
    await expect(
      resolveSqlRefsToWorkspaceNodes(client, wsID, [ref])
    ).rejects.toThrow(CoalesceApiError);
  });

  it("case-insensitive name matching via normalizeSqlIdentifier", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [{ id: "n-1", name: "stg_orders", nodeType: "Stage", locationName: "RAW" }],
    });
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-1",
      name: "stg_orders",
      locationName: "RAW",
    });

    const ref = makeRef({ nodeName: "STG_ORDERS" });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBe("n-1");
    expect(result.openQuestions).toEqual([]);
  });

  it("nullifies nodeID when coalesce_ref predecessor location doesn't match", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [{ id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" }],
    });
    // Predecessor fetch returns a different location than what the ref requested
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-1",
      name: "STG_ORDERS",
      locationName: "CURATED",
    });

    const ref = makeRef({
      sourceStyle: "coalesce_ref",
      locationName: "RAW",
    });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBeNull();
    expect(result.openQuestions.length).toBe(1);
    expect(result.openQuestions[0]).toMatch(/not the requested location/);
  });

  it("backfills locationName from index entry when ref has none (single match)", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [{ id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" }],
    });
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-1",
      name: "STG_ORDERS",
      locationName: "RAW",
    });

    const ref = makeRef({ locationName: "" });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    // Single match → locationName backfilled from index entry during name resolution
    expect(ref.nodeID).toBe("n-1");
    expect(ref.locationName).toBe("RAW");
    expect(result.openQuestions).toEqual([]);
  });

  it("warns when predecessor fetch fails but does not block resolution", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [{ id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" }],
    });
    mockGetWorkspaceNode.mockRejectedValue(new Error("timeout"));

    const ref = makeRef();
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    // nodeID was set during name resolution (single match), but predecessor fetch failed
    expect(ref.nodeID).toBe("n-1");
    expect(result.warnings.length).toBe(1);
    expect(result.warnings[0]).toMatch(/Could not fetch predecessor/);
    expect(result.predecessorNodes["n-1"]).toBeUndefined();
  });

  it("warns when predecessor fetch returns non-object", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [{ id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" }],
    });
    mockGetWorkspaceNode.mockResolvedValue("not-an-object");

    const ref = makeRef();
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(result.warnings.length).toBe(1);
    expect(result.warnings[0]).toMatch(/did not return an object/);
  });

  it("coalesce_ref with matching location on ambiguous multi-match uses detailed fetch", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
        { id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
      ],
    });
    mockGetWorkspaceNode
      .mockResolvedValueOnce({ id: "n-1", name: "STG_ORDERS", locationName: "CURATED" })
      .mockResolvedValueOnce({ id: "n-2", name: "STG_ORDERS", locationName: "RAW" });

    const ref = makeRef({
      sourceStyle: "coalesce_ref",
      locationName: "RAW",
    });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    // Exact location match on detailed fetch → n-2
    expect(ref.nodeID).toBe("n-2");
    expect(result.openQuestions).toEqual([]);
  });

  it("coalesce_ref with no matching location on detailed fetch produces open question", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
        { id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: null },
      ],
    });
    mockGetWorkspaceNode
      .mockResolvedValueOnce({ id: "n-1", name: "STG_ORDERS", locationName: "CURATED" })
      .mockResolvedValueOnce({ id: "n-2", name: "STG_ORDERS", locationName: "ARCHIVE" });

    const ref = makeRef({
      sourceStyle: "coalesce_ref",
      locationName: "RAW",
    });
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBeNull();
    expect(result.openQuestions.length).toBe(1);
    expect(result.openQuestions[0]).toMatch(/none matched the requested location/);
  });
});

// ---------------------------------------------------------------------------
// Pagination edge case (listAllWorkspaceNodes)
// ---------------------------------------------------------------------------
describe("resolveSqlRefsToWorkspaceNodes pagination", () => {
  const client = createMockClient();
  const wsID = "ws-1";

  it("follows pagination cursors across multiple pages", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({
        data: [{ id: "n-1", name: "NODE_A", nodeType: "Stage", locationName: "RAW" }],
        next: "cursor-2",
      })
      .mockResolvedValueOnce({
        data: [{ id: "n-2", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" }],
      });
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-2",
      name: "STG_ORDERS",
      locationName: "RAW",
    });

    const ref = makeRef();
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBe("n-2");
    expect(mockListWorkspaceNodes).toHaveBeenCalledTimes(2);
    expect(result.openQuestions).toEqual([]);
  });

  it("throws on repeated pagination cursor (infinite loop guard)", async () => {
    mockListWorkspaceNodes
      .mockResolvedValueOnce({
        data: [{ id: "n-1", name: "NODE_A", nodeType: "Stage", locationName: "RAW" }],
        next: "same-cursor",
      })
      .mockResolvedValueOnce({
        data: [{ id: "n-2", name: "NODE_B", nodeType: "Stage", locationName: "RAW" }],
        next: "same-cursor",
      });

    const ref = makeRef();
    await expect(
      resolveSqlRefsToWorkspaceNodes(client, wsID, [ref])
    ).rejects.toThrow(/repeated cursor/);
  });

  it("skips malformed items in pagination data", async () => {
    mockListWorkspaceNodes.mockResolvedValue({
      data: [
        { id: "n-1", name: "STG_ORDERS", nodeType: "Stage", locationName: "RAW" },
        { id: null, name: null }, // malformed — should be skipped
        "not-an-object", // skipped
      ],
    });
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-1",
      name: "STG_ORDERS",
      locationName: "RAW",
    });

    const ref = makeRef();
    const result = await resolveSqlRefsToWorkspaceNodes(client, wsID, [ref]);

    expect(ref.nodeID).toBe("n-1");
    expect(result.openQuestions).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// getSourceNodesByID
// ---------------------------------------------------------------------------
describe("getSourceNodesByID", () => {
  const client = createMockClient();
  const wsID = "ws-1";

  it("resolves a single source node with location", async () => {
    mockGetWorkspaceNode.mockResolvedValue({
      id: "n-1",
      name: "STG_ORDERS",
      locationName: "RAW",
      nodeType: "Stage",
    });

    const result = await getSourceNodesByID(client, wsID, ["n-1"]);

    expect(result.sourceRefs.length).toBe(1);
    expect(result.sourceRefs[0]!.nodeID).toBe("n-1");
    expect(result.sourceRefs[0]!.locationName).toBe("RAW");
    expect(result.predecessorNodes["n-1"]).toBeDefined();
    expect(result.openQuestions).toEqual([]);
  });

  it("produces an open question when node is not an object", async () => {
    mockGetWorkspaceNode.mockResolvedValue("not-an-object");

    const result = await getSourceNodesByID(client, wsID, ["n-1"]);

    expect(result.sourceRefs.length).toBe(0);
    expect(result.openQuestions.length).toBe(1);
    expect(result.openQuestions[0]).toMatch(/Could not read source node/);
  });

  it("produces an open question when node has no usable name", async () => {
    mockGetWorkspaceNode.mockResolvedValue({ id: "n-1", name: "", locationName: "RAW" });

    const result = await getSourceNodesByID(client, wsID, ["n-1"]);

    expect(result.sourceRefs.length).toBe(0);
    expect(result.openQuestions.length).toBe(1);
    expect(result.openQuestions[0]).toMatch(/does not have a usable name/);
  });

  it("produces an open question when node has no locationName", async () => {
    mockGetWorkspaceNode.mockResolvedValue({ id: "n-1", name: "STG_ORDERS" });

    const result = await getSourceNodesByID(client, wsID, ["n-1"]);

    expect(result.sourceRefs.length).toBe(1);
    expect(result.sourceRefs[0]!.locationName).toBe("UNKNOWN_LOCATION");
    expect(result.openQuestions.length).toBe(1);
    expect(result.openQuestions[0]).toMatch(/does not expose locationName/);
  });

  it("handles multiple source nodes with mixed success", async () => {
    mockGetWorkspaceNode
      .mockResolvedValueOnce({ id: "n-1", name: "STG_ORDERS", locationName: "RAW" })
      .mockResolvedValueOnce("not-an-object");

    const result = await getSourceNodesByID(client, wsID, ["n-1", "n-2"]);

    expect(result.sourceRefs.length).toBe(1);
    expect(result.openQuestions.length).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// getWorkspaceNodeTypeInventory
// ---------------------------------------------------------------------------
describe("getWorkspaceNodeTypeInventory", () => {
  const client = createMockClient();
  const wsID = "ws-1";

  it("returns inventory from successful API call", async () => {
    mockListWorkspaceNodeTypes.mockResolvedValue({
      nodeTypes: ["Stage", "Dimension"],
      counts: { Stage: 5, Dimension: 3 },
      total: 8,
    });

    const result = await getWorkspaceNodeTypeInventory(client, wsID);

    expect(result.nodeTypes).toEqual(["Stage", "Dimension"]);
    expect(result.total).toBe(8);
    expect(result.warnings).toEqual([]);
  });

  it("returns empty inventory with warning on recoverable error", async () => {
    mockListWorkspaceNodeTypes.mockRejectedValue(new Error("timeout"));

    const result = await getWorkspaceNodeTypeInventory(client, wsID);

    expect(result.nodeTypes).toEqual([]);
    expect(result.total).toBe(0);
    expect(result.warnings.length).toBe(1);
    expect(result.warnings[0]).toMatch(/could not be fetched/);
  });

  it("rethrows non-recoverable API errors", async () => {
    mockListWorkspaceNodeTypes.mockRejectedValue(
      new CoalesceApiError("Forbidden", 403, "GET", "/api/v1/workspaces/ws-1/nodeTypes")
    );

    await expect(getWorkspaceNodeTypeInventory(client, wsID)).rejects.toThrow(CoalesceApiError);
  });
});

// ---------------------------------------------------------------------------
// applyWorkspaceNodeTypeValidation
// ---------------------------------------------------------------------------
describe("applyWorkspaceNodeTypeValidation", () => {
  it("does nothing when inventory is empty (total=0)", () => {
    const plan = makeEmptyPlan();
    const inventory: WorkspaceNodeTypeInventory = {
      nodeTypes: [],
      counts: {},
      total: 0,
      warnings: ["some warning"],
    };

    applyWorkspaceNodeTypeValidation(plan, inventory);

    // Inventory warnings are always pushed
    expect(plan.warnings).toEqual(["some warning"]);
    // But no missing-type check when total=0
    expect(plan.status).toBe("ready");
  });

  it("does not warn when all node types are present", () => {
    const plan = makeEmptyPlan({
      nodes: [
        {
          planNodeID: "p-1",
          name: "STG_ORDERS",
          nodeType: "Stage",
          predecessorNodeIDs: [],
          predecessorPlanNodeIDs: [],
          predecessorNodeNames: [],
          description: null,
          sql: null,
          selectItems: [],
          outputColumnNames: [],
          configOverrides: {},
          sourceRefs: [],
          joinCondition: null,
          location: {},
          requiresFullSetNode: false,
        },
      ],
    });
    const inventory: WorkspaceNodeTypeInventory = {
      nodeTypes: ["Stage", "Dimension"],
      counts: { Stage: 5, Dimension: 3 },
      total: 8,
      warnings: [],
    };

    applyWorkspaceNodeTypeValidation(plan, inventory);

    expect(plan.status).toBe("ready");
    expect(plan.warnings).toEqual([]);
  });

  it("warns and sets needs_clarification when a node type is missing", () => {
    const plan = makeEmptyPlan({
      nodes: [
        {
          planNodeID: "p-1",
          name: "STG_ORDERS",
          nodeType: "IncrementalLoading:::230",
          predecessorNodeIDs: [],
          predecessorPlanNodeIDs: [],
          predecessorNodeNames: [],
          description: null,
          sql: null,
          selectItems: [],
          outputColumnNames: [],
          configOverrides: {},
          sourceRefs: [],
          joinCondition: null,
          location: {},
          requiresFullSetNode: false,
        },
      ],
    });
    const inventory: WorkspaceNodeTypeInventory = {
      nodeTypes: ["Stage", "Dimension"],
      counts: { Stage: 5, Dimension: 3 },
      total: 8,
      warnings: [],
    };

    applyWorkspaceNodeTypeValidation(plan, inventory);

    expect(plan.status).toBe("needs_clarification");
    expect(plan.warnings.length).toBe(1);
    expect(plan.warnings[0]).toMatch(/IncrementalLoading:::230/);
  });

  it("matches package-prefixed type by bare ID", () => {
    const plan = makeEmptyPlan({
      nodes: [
        {
          planNodeID: "p-1",
          name: "STG_ORDERS",
          nodeType: "230",
          predecessorNodeIDs: [],
          predecessorPlanNodeIDs: [],
          predecessorNodeNames: [],
          description: null,
          sql: null,
          selectItems: [],
          outputColumnNames: [],
          configOverrides: {},
          sourceRefs: [],
          joinCondition: null,
          location: {},
          requiresFullSetNode: false,
        },
      ],
    });
    const inventory: WorkspaceNodeTypeInventory = {
      nodeTypes: ["Stage", "IncrementalLoading:::230"],
      counts: { Stage: 5, "IncrementalLoading:::230": 2 },
      total: 7,
      warnings: [],
    };

    applyWorkspaceNodeTypeValidation(plan, inventory);

    // Bare "230" matches "IncrementalLoading:::230" via matchesObservedNodeType
    expect(plan.status).toBe("ready");
    expect(plan.warnings).toEqual([]);
  });

  it("includes requestedNodeType in validation", () => {
    const plan = makeEmptyPlan();
    const inventory: WorkspaceNodeTypeInventory = {
      nodeTypes: ["Stage"],
      counts: { Stage: 5 },
      total: 5,
      warnings: [],
    };

    applyWorkspaceNodeTypeValidation(plan, inventory, "MissingType");

    expect(plan.status).toBe("needs_clarification");
    expect(plan.warnings.length).toBe(1);
    expect(plan.warnings[0]).toMatch(/MissingType/);
  });

  it("skips requestedNodeType when it is empty or whitespace", () => {
    const plan = makeEmptyPlan();
    const inventory: WorkspaceNodeTypeInventory = {
      nodeTypes: ["Stage"],
      counts: { Stage: 5 },
      total: 5,
      warnings: [],
    };

    applyWorkspaceNodeTypeValidation(plan, inventory, "  ");

    expect(plan.status).toBe("ready");
    expect(plan.warnings).toEqual([]);
  });
});
