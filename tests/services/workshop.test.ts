import { describe, it, expect, vi, afterEach, beforeEach } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  openWorkshop,
  workshopInstruct,
  getWorkshopStatus,
  workshopClose,
  loadSession,
  deleteSession,
} from "../../src/services/pipelines/workshop.js";
import { createMockClient } from "../helpers/fixtures.js";

// Mock completeNodeConfiguration so intent parsing doesn't need corpus/repo files
vi.mock("../../src/services/config/intelligent.js", () => ({
  completeNodeConfiguration: vi.fn(async () => ({})),
}));

// Per-file cache dir so workshop session JSON files don't collide with
// sibling tests running in parallel. getCacheDir() resolves via
// COALESCE_CACHE_DIR; without this isolation, all workshop tests share
// `<cwd>/coalesce_transform_mcp_data_cache/workshops/` and produce EINVAL
// races under vitest's parallel file pool.
let workshopTestCacheDir: string;
const savedCacheDir = process.env.COALESCE_CACHE_DIR;

beforeEach(() => {
  workshopTestCacheDir = mkdtempSync(join(tmpdir(), "workshop-cache-test-"));
  process.env.COALESCE_CACHE_DIR = workshopTestCacheDir;
});

afterEach(() => {
  if (savedCacheDir === undefined) {
    delete process.env.COALESCE_CACHE_DIR;
  } else {
    process.env.COALESCE_CACHE_DIR = savedCacheDir;
  }
  rmSync(workshopTestCacheDir, { recursive: true, force: true });
});

function setupMockClient(nodes: Array<{ id: string; name: string; locationName: string }> = []) {
  const client = createMockClient();
  client.get.mockImplementation((path: string) => {
    if (path.match(/\/nodes$/) && !path.includes("/nodes/")) {
      return Promise.resolve({
        data: nodes.map((n) => ({
          id: n.id,
          name: n.name,
          nodeType: "Stage",
          locationName: n.locationName,
        })),
      });
    }
    // Individual node lookup
    for (const n of nodes) {
      if (path.includes(`/nodes/${n.id}`)) {
        return Promise.resolve({
          id: n.id,
          name: n.name,
          nodeType: "Stage",
          metadata: { columns: [{ name: "ID", dataType: "VARCHAR" }] },
        });
      }
    }
    return Promise.resolve({ data: [] });
  });
  return client;
}

describe("Pipeline Workshop", () => {
  const sessionIDs: string[] = [];

  afterEach(() => {
    vi.restoreAllMocks();
    // Clean up any sessions created during tests
    for (const id of sessionIDs.splice(0, sessionIDs.length)) {
      deleteSession(id);
    }
  });

  describe("openWorkshop", () => {
    it("creates a new session with workspace entities", async () => {
      const client = setupMockClient([
        { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
        { id: "n2", name: "ORDERS", locationName: "RAW" },
      ]);

      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      expect(session.sessionID).toBeTruthy();
      expect(session.workspaceID).toBe("ws-1");
      expect(session.resolvedEntities).toHaveLength(2);
      expect(session.nodes).toHaveLength(0);
    });

    it("bootstraps with initial intent", async () => {
      const client = setupMockClient([
        { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
        { id: "n2", name: "ORDERS", locationName: "RAW" },
      ]);

      const session = await openWorkshop(client as any, {
        workspaceID: "ws-1",
        intent: "join CUSTOMERS and ORDERS on CUSTOMER_ID",
      });
      sessionIDs.push(session.sessionID);

      expect(session.nodes.length).toBeGreaterThanOrEqual(1);
      expect(session.history).toHaveLength(1);
    });

    it("persists session to disk", async () => {
      const client = setupMockClient();
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      const loaded = loadSession(session.sessionID);
      expect(loaded).not.toBeNull();
      expect(loaded!.workspaceID).toBe("ws-1");
    });
  });

  describe("workshopInstruct", () => {
    it("adds a stage node", async () => {
      const client = setupMockClient([
        { id: "n1", name: "PAYMENTS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "stage PAYMENTS",
      });

      expect(result.action).toBe("added_nodes");
      expect(result.changes.length).toBeGreaterThan(0);
      expect(result.currentPlan.length).toBeGreaterThanOrEqual(1);
    });

    it("adds a join node with entities resolved from session", async () => {
      const client = setupMockClient([
        { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
        { id: "n2", name: "ORDERS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "join CUSTOMERS and ORDERS on CUSTOMER_ID",
      });

      expect(result.action).toBe("added_nodes");
      const joinNode = result.currentPlan.find(
        (n) => n.predecessorIDs.length >= 2
      );
      expect(joinNode).toBeDefined();
      expect(joinNode!.joinCondition).toContain("CUSTOMER_ID");
    });

    it("renames a node", async () => {
      const client = setupMockClient([
        { id: "n1", name: "PAYMENTS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "stage PAYMENTS",
      });

      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "rename STG_PAYMENTS to STG_PAYMENT_HISTORY",
      });

      expect(result.action).toBe("renamed");
      expect(result.currentPlan.some((n) => n.name === "STG_PAYMENT_HISTORY")).toBe(true);
    });

    it("removes a node", async () => {
      const client = setupMockClient([
        { id: "n1", name: "PAYMENTS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "stage PAYMENTS",
      });

      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "remove STG_PAYMENTS",
      });

      expect(result.action).toBe("removed");
      expect(result.currentPlan).toHaveLength(0);
    });

    it("adds a filter", async () => {
      const client = setupMockClient([
        { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "stage CUSTOMERS",
      });

      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "add filter for IS_ACTIVE = true",
      });

      expect(result.action).toBe("added_filter");
      const node = result.currentPlan[result.currentPlan.length - 1]!;
      expect(node.filters.length).toBeGreaterThan(0);
    });

    it("adds a column", async () => {
      const client = setupMockClient([
        { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "stage CUSTOMERS",
      });

      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "add column FULL_NAME",
      });

      expect(result.action).toBe("added_column");
      const node = result.currentPlan[result.currentPlan.length - 1]!;
      expect(node.columns).toContain("FULL_NAME");
    });

    it("removes a column", async () => {
      const client = setupMockClient([
        { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "stage CUSTOMERS",
      });
      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "add column TEMP_COL",
      });

      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "remove column TEMP_COL",
      });

      expect(result.action).toBe("removed_column");
      const node = result.currentPlan[result.currentPlan.length - 1]!;
      expect(node.columns).not.toContain("TEMP_COL");
    });

    it("changes join key", async () => {
      const client = setupMockClient([
        { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
        { id: "n2", name: "ORDERS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "join CUSTOMERS and ORDERS on CUSTOMER_ID",
      });

      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "change the join key to ORDER_ID",
      });

      expect(result.action).toBe("updated_join");
      const joinNode = result.currentPlan.find((n) => n.joinCondition !== null);
      expect(joinNode).toBeDefined();
      expect(joinNode!.joinCondition).toContain("ORDER_ID");
    });

    it("returns clarification for vague instructions", async () => {
      const client = setupMockClient();
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "do something with the data",
      });

      expect(result.action).toBe("clarification_needed");
      expect(result.openQuestions.length).toBeGreaterThan(0);
    });

    it("throws for non-existent session", async () => {
      const client = setupMockClient();

      await expect(
        workshopInstruct(client as any, {
          sessionID: "nonexistent",
          instruction: "stage CUSTOMERS",
        })
      ).rejects.toThrow("not found");
    });

    it("records history for each instruction", async () => {
      const client = setupMockClient([
        { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "stage CUSTOMERS",
      });
      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "add filter for IS_ACTIVE = true",
      });

      const status = getWorkshopStatus(session.sessionID);
      expect(status!.history).toHaveLength(2);
    });
  });

  describe("workshopClose", () => {
    it("closes and removes session", async () => {
      const client = setupMockClient();
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });

      const result = workshopClose(session.sessionID);

      expect(result.closed).toBe(true);
      expect(getWorkshopStatus(session.sessionID)).toBeNull();
    });

    it("warns about uncreated nodes", async () => {
      const client = setupMockClient([
        { id: "n1", name: "PAYMENTS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });

      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "stage PAYMENTS",
      });

      const result = workshopClose(session.sessionID);

      expect(result.closed).toBe(true);
      expect(result.message).toContain("not created");
    });

    it("returns false for non-existent session", () => {
      const result = workshopClose("nonexistent");
      expect(result.closed).toBe(false);
    });
  });

  describe("multi-step workflow", () => {
    it("builds a join then aggregation incrementally", async () => {
      const client = setupMockClient([
        { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
        { id: "n2", name: "ORDERS", locationName: "RAW" },
      ]);
      const session = await openWorkshop(client as any, { workspaceID: "ws-1" });
      sessionIDs.push(session.sessionID);

      // Step 1: Join
      await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "join CUSTOMERS and ORDERS on CUSTOMER_ID",
      });

      // Step 2: Aggregate
      const result = await workshopInstruct(client as any, {
        sessionID: session.sessionID,
        instruction: "aggregate total REVENUE by REGION",
      });

      expect(result.currentPlan.length).toBeGreaterThanOrEqual(2);

      // Verify the aggregation node references the join node
      const aggNode = result.currentPlan.find((n) => n.aggregates.length > 0);
      expect(aggNode).toBeDefined();
      expect(aggNode!.groupByColumns).toContain("REGION");
    });
  });
});
