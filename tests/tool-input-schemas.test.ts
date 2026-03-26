import { describe, it, expect, vi } from "vitest";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerNodeTools } from "../src/mcp/nodes.js";
import { registerPipelineTools } from "../src/mcp/pipelines.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

function getToolParamsSchema(
  toolSpy: ReturnType<typeof vi.spyOn>,
  toolName: string
): z.ZodObject<z.ZodRawShape> {
  const toolCall = toolSpy.mock.calls.find((call) => call[0] === toolName);
  const shape = toolCall?.[2] as z.ZodRawShape | undefined;

  if (!shape) {
    throw new Error(`Tool ${toolName} was not registered`);
  }

  return z.object(shape);
}

describe("Node Tool Input Schemas", () => {
  it("requires structured objects for scratch-node payload fields", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "tool");

    registerNodeTools(server, createMockClient() as any);

    const schema = getToolParamsSchema(toolSpy, "create-workspace-node-from-scratch");

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        nodeType: "base-nodes:::Stage",
        storageLocations: [{ locationName: "DEV" }],
        config: { testsEnabled: true, preSQL: "SELECT 1" },
        metadata: {
          columns: [{ name: "CUSTOMER_ID", dataType: "VARCHAR" }],
          sourceMapping: [{ name: "STG_CUSTOMER", dependencies: [{ nodeName: "CUSTOMER" }] }],
        },
        changes: { name: "STG_CUSTOMER", database: "ANALYTICS" },
      }).success
    ).toBe(true);

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        nodeType: "base-nodes:::Stage",
        storageLocations: ["DEV"],
      }).success
    ).toBe(false);

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        nodeType: "base-nodes:::Stage",
        config: "not-an-object",
      }).success
    ).toBe(false);

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        nodeType: "base-nodes:::Stage",
        metadata: { columns: ["CUSTOMER_ID"] },
      }).success
    ).toBe(false);
  });

  it("requires structured node bodies for set/update tools", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "tool");

    registerNodeTools(server, createMockClient() as any);

    const setSchema = getToolParamsSchema(toolSpy, "set-workspace-node");
    const updateSchema = getToolParamsSchema(toolSpy, "update-workspace-node");

    expect(
      setSchema.safeParse({
        workspaceID: "ws-1",
        nodeID: "node-1",
        body: {
          name: "STG_CUSTOMER",
          config: { postSQL: "" },
          metadata: {
            columns: [{ name: "CUSTOMER_ID", nullable: false }],
          },
        },
      }).success
    ).toBe(true);

    expect(
      setSchema.safeParse({
        workspaceID: "ws-1",
        nodeID: "node-1",
        body: "full-body-text-blob",
      }).success
    ).toBe(false);

    expect(
      updateSchema.safeParse({
        workspaceID: "ws-1",
        nodeID: "node-1",
        changes: {
          description: "Updated description",
          metadata: {
            columns: [{ name: "CUSTOMER_ID", transform: "\"SRC\".\"CUSTOMER_ID\"" }],
          },
        },
      }).success
    ).toBe(true);

    expect(
      updateSchema.safeParse({
        workspaceID: "ws-1",
        nodeID: "node-1",
        changes: "patch-text-blob",
      }).success
    ).toBe(false);
  });

  it("requires column objects for replace-workspace-node-columns", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "tool");

    registerNodeTools(server, createMockClient() as any);

    const schema = getToolParamsSchema(toolSpy, "replace-workspace-node-columns");

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        nodeID: "node-1",
        columns: [
          { name: "CUSTOMER_ID", transform: "\"SRC\".\"CUSTOMER_ID\"" },
          { name: "COUNTRY", description: "Country name" },
        ],
        additionalChanges: {
          name: "STG_CUSTOMER_FILTERED",
          config: { testsEnabled: false },
        },
      }).success
    ).toBe(true);

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        nodeID: "node-1",
        columns: ["CUSTOMER_ID"],
      }).success
    ).toBe(false);

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        nodeID: "node-1",
        columns: [{ name: "CUSTOMER_ID" }],
        additionalChanges: "rename-node",
      }).success
    ).toBe(false);
  });
});

describe("Pipeline Tool Input Schemas", () => {
  it("requires structured config overrides for planner/create tools", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "tool");

    registerPipelineTools(server, createMockClient() as any);

    const planSchema = getToolParamsSchema(toolSpy, "plan-pipeline");
    const createFromSqlSchema = getToolParamsSchema(toolSpy, "create-pipeline-from-sql");

    expect(
      planSchema.safeParse({
        workspaceID: "ws-1",
        goal: "Build a customer stage",
        configOverrides: { testsEnabled: true, preSQL: "SELECT 1" },
      }).success
    ).toBe(true);

    expect(
      planSchema.safeParse({
        workspaceID: "ws-1",
        goal: "Build a customer stage",
        configOverrides: "testsEnabled=true",
      }).success
    ).toBe(false);

    expect(
      createFromSqlSchema.safeParse({
        workspaceID: "ws-1",
        sql: "select * from customer",
        configOverrides: { materializationType: "table" },
      }).success
    ).toBe(true);

    expect(
      createFromSqlSchema.safeParse({
        workspaceID: "ws-1",
        sql: "select * from customer",
        configOverrides: ["table"],
      }).success
    ).toBe(false);
  });

  it("requires a structured plan object for create-pipeline-from-plan", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "tool");

    registerPipelineTools(server, createMockClient() as any);

    const schema = getToolParamsSchema(toolSpy, "create-pipeline-from-plan");

    const validPlan = {
      version: 1,
      intent: "goal",
      status: "ready",
      workspaceID: "ws-1",
      platform: null,
      goal: "Build a customer stage",
      sql: null,
      nodes: [
        {
          planNodeID: "plan-1",
          name: "STG_CUSTOMER",
          nodeType: "base-nodes:::Stage",
          nodeTypeFamily: "stage",
          predecessorNodeIDs: ["src-1"],
          predecessorPlanNodeIDs: [],
          predecessorNodeNames: ["CUSTOMER"],
          description: null,
          sql: null,
          selectItems: [],
          outputColumnNames: ["CUSTOMER_ID"],
          configOverrides: { testsEnabled: true },
          sourceRefs: [
            {
              locationName: "RAW",
              nodeName: "CUSTOMER",
              alias: null,
              nodeID: "src-1",
            },
          ],
          joinCondition: null,
          location: {},
          requiresFullSetNode: false,
        },
      ],
      assumptions: [],
      openQuestions: [],
      warnings: [],
      supportedNodeTypes: ["base-nodes:::Stage"],
    };

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        plan: validPlan,
      }).success
    ).toBe(true);

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        plan: "cached-plan-path-or-text",
      }).success
    ).toBe(false);
  });
});
