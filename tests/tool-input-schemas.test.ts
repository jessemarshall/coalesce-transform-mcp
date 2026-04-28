import { describe, it, expect, vi } from "vitest";
import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { defineNodeTools } from "../src/mcp/nodes.js";
import { definePipelineTools } from "../src/mcp/pipelines.js";
import { defineCoaTools } from "../src/mcp/coa.js";
import { defineEnvironmentTools } from "../src/mcp/environments.js";
import { defineWorkspaceTools } from "../src/mcp/workspaces.js";
import { defineProjectTools } from "../src/mcp/projects.js";
import { defineJobTools } from "../src/mcp/jobs.js";
import { defineUserTools } from "../src/mcp/users.js";
import { defineGitAccountTools } from "../src/mcp/git-accounts.js";
import { defineSubgraphTools } from "../src/mcp/subgraphs.js";
import { defineCacheTools } from "../src/mcp/cache.js";
import { defineWorkshopTools } from "../src/mcp/workshop.js";
import { defineLineageTools } from "../src/mcp/lineage.js";
import { defineRunTools } from "../src/mcp/runs.js";
import { defineRenderNodeTools } from "../src/mcp/render-node.js";
import { registerRunAndWait } from "../src/workflows/run-and-wait.js";
import { defineGetEnvironmentOverview } from "../src/workflows/get-environment-overview.js";
import { defineGetEnvironmentHealth } from "../src/workflows/get-environment-health.js";
import { defineGetRunDetails } from "../src/workflows/get-run-details.js";

const VALID_PIPELINE_PLAN = {
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
  const schema = toolCall?.[1]?.inputSchema as z.ZodObject<z.ZodRawShape> | undefined;

  if (!schema) {
    throw new Error(`Tool ${toolName} was not registered`);
  }

  return schema;
}

describe("Node Tool Input Schemas", () => {
  it("requires structured objects for scratch-node payload fields", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");

    defineNodeTools(server, createMockClient() as any).forEach(t => server.registerTool(...t));

    const schema = getToolParamsSchema(toolSpy, "create_workspace_node_from_scratch");

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
    const toolSpy = vi.spyOn(server, "registerTool");

    defineNodeTools(server, createMockClient() as any).forEach(t => server.registerTool(...t));

    const setSchema = getToolParamsSchema(toolSpy, "set_workspace_node");
    const updateSchema = getToolParamsSchema(toolSpy, "update_workspace_node");

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
    const toolSpy = vi.spyOn(server, "registerTool");

    defineNodeTools(server, createMockClient() as any).forEach(t => server.registerTool(...t));

    const schema = getToolParamsSchema(toolSpy, "replace_workspace_node_columns");

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

describe("COA Cloud Tool Input Schemas", () => {
  it("rejects empty environmentID for coa_plan, coa_deploy, and coa_refresh", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");

    defineCoaTools(server).forEach(t => server.registerTool(...t));

    for (const toolName of ["coa_plan", "coa_deploy", "coa_refresh"]) {
      const schema = getToolParamsSchema(toolSpy, toolName);
      const result = schema.safeParse({ environmentID: "" });
      expect(result.success, `${toolName} should reject empty environmentID`).toBe(false);
    }
  });
});

describe("Required-string validation across MCP tools", () => {
  // Locks in the contract that required ID/name params reject empty strings at
  // the schema layer instead of forwarding `""` to the Coalesce API where it
  // surfaces as a confusing 4xx. Covers read/write/destructive tools across
  // every tool family that registers required string params.
  it("rejects empty required IDs across read/write/destructive tools", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");

    const client = createMockClient() as any;
    defineEnvironmentTools(server, client).forEach(t => server.registerTool(...t));
    defineWorkspaceTools(server, client).forEach(t => server.registerTool(...t));
    defineProjectTools(server, client).forEach(t => server.registerTool(...t));
    defineJobTools(server, client).forEach(t => server.registerTool(...t));
    defineUserTools(server, client).forEach(t => server.registerTool(...t));
    defineGitAccountTools(server, client).forEach(t => server.registerTool(...t));
    defineSubgraphTools(server, client).forEach(t => server.registerTool(...t));
    defineCacheTools(server, client).forEach(t => server.registerTool(...t));
    defineNodeTools(server, client).forEach(t => server.registerTool(...t));
    defineWorkshopTools(server, client).forEach(t => server.registerTool(...t));
    definePipelineTools(server, client).forEach(t => server.registerTool(...t));
    defineLineageTools(server, client).forEach(t => server.registerTool(...t));
    defineRunTools(server, client).forEach(t => server.registerTool(...t));
    defineRenderNodeTools(server, client).forEach(t => server.registerTool(...t));
    defineGetEnvironmentOverview(server, client).forEach(t => server.registerTool(...t));
    defineGetEnvironmentHealth(server, client).forEach(t => server.registerTool(...t));
    defineGetRunDetails(server, client).forEach(t => server.registerTool(...t));
    defineCoaTools(server).forEach(t => server.registerTool(...t));
    registerRunAndWait(server, client);

    const cases: Array<{ tool: string; input: Record<string, unknown> }> = [
      { tool: "get_environment", input: { environmentID: "" } },
      { tool: "create_environment", input: { projectID: "", name: "qa" } },
      { tool: "create_environment", input: { projectID: "p1", name: "" } },
      { tool: "delete_environment", input: { environmentID: "", confirmed: true } },
      { tool: "get_workspace", input: { workspaceID: "" } },
      { tool: "get_project", input: { projectID: "" } },
      { tool: "update_project", input: { projectID: "" } },
      { tool: "update_project", input: { projectID: "p1", name: "" } },
      { tool: "delete_project", input: { projectID: "", confirmed: true } },
      { tool: "list_environment_jobs", input: { environmentID: "" } },
      { tool: "create_workspace_job", input: { workspaceID: "", name: "j", includeSelector: "", excludeSelector: "" } },
      { tool: "create_workspace_job", input: { workspaceID: "ws", name: "", includeSelector: "", excludeSelector: "" } },
      { tool: "get_environment_job", input: { environmentID: "", jobID: "j" } },
      { tool: "get_environment_job", input: { environmentID: "e", jobID: "" } },
      { tool: "update_workspace_job", input: { workspaceID: "", jobID: "j", name: "j", includeSelector: "", excludeSelector: "" } },
      { tool: "update_workspace_job", input: { workspaceID: "ws", jobID: "", name: "j", includeSelector: "", excludeSelector: "" } },
      { tool: "update_workspace_job", input: { workspaceID: "ws", jobID: "j", name: "", includeSelector: "", excludeSelector: "" } },
      { tool: "delete_workspace_job", input: { workspaceID: "", jobID: "j", confirmed: true } },
      { tool: "get_user_roles", input: { userID: "" } },
      { tool: "set_org_role", input: { userID: "u", role: "" } },
      { tool: "set_project_role", input: { userID: "u", projectID: "", role: "admin" } },
      { tool: "set_env_role", input: { userID: "u", environmentID: "", role: "admin" } },
      { tool: "delete_project_role", input: { userID: "", projectID: "p1", confirmed: true } },
      { tool: "delete_project_role", input: { userID: "u", projectID: "", confirmed: true } },
      { tool: "delete_env_role", input: { userID: "", environmentID: "e", confirmed: true } },
      { tool: "get_git_account", input: { gitAccountID: "" } },
      { tool: "create_git_account", input: { name: "", gitUsername: "u", gitAuthorName: "a", gitAuthorEmail: "a@b", gitToken: "t" } },
      { tool: "create_git_account", input: { name: "n", gitUsername: "", gitAuthorName: "a", gitAuthorEmail: "a@b", gitToken: "t" } },
      { tool: "update_git_account", input: { gitAccountID: "" } },
      { tool: "delete_git_account", input: { gitAccountID: "", confirmed: true } },
      { tool: "get_workspace_subgraph", input: { workspaceID: "ws", subgraphID: "" } },
      { tool: "create_workspace_subgraph", input: { workspaceID: "ws", name: "", steps: [] } },
      { tool: "create_workspace_subgraph", input: { workspaceID: "", name: "n", steps: [] } },
      { tool: "create_workspace_subgraph", input: { workspaceID: "ws", name: "n", steps: [""] } },
      { tool: "update_workspace_subgraph", input: { workspaceID: "", subgraphID: "sg-1", name: "n", steps: ["n-1"] } },
      { tool: "update_workspace_subgraph", input: { workspaceID: "ws", subgraphID: "sg-1", name: "", steps: ["n-1"] } },
      { tool: "delete_workspace_subgraph", input: { workspaceID: "", subgraphID: "sg-1", confirmed: true } },
      { tool: "cache_workspace_nodes", input: { workspaceID: "" } },
      { tool: "cache_environment_nodes", input: { environmentID: "" } },
      { tool: "list_environment_nodes", input: { environmentID: "" } },
      { tool: "list_workspace_nodes", input: { workspaceID: "" } },
      { tool: "get_environment_node", input: { environmentID: "", nodeID: "n" } },
      { tool: "get_workspace_node", input: { workspaceID: "ws", nodeID: "" } },
      { tool: "create_workspace_node_from_scratch", input: { workspaceID: "", nodeType: "base-nodes:::Stage" } },
      { tool: "create_workspace_node_from_scratch", input: { workspaceID: "ws", nodeType: "" } },
      { tool: "create_workspace_node_from_predecessor", input: { workspaceID: "", nodeType: "base-nodes:::Stage", predecessorNodeIDs: ["n-1"] } },
      { tool: "set_workspace_node", input: { workspaceID: "", nodeID: "n", body: {} } },
      { tool: "set_workspace_node", input: { workspaceID: "ws", nodeID: "", body: {} } },
      { tool: "update_workspace_node", input: { workspaceID: "", nodeID: "n", changes: {} } },
      { tool: "update_workspace_node", input: { workspaceID: "ws", nodeID: "", changes: {} } },
      { tool: "replace_workspace_node_columns", input: { workspaceID: "", nodeID: "n", columns: [{ name: "C" }] } },
      { tool: "replace_workspace_node_columns", input: { workspaceID: "ws", nodeID: "", columns: [{ name: "C" }] } },
      { tool: "delete_workspace_node", input: { workspaceID: "ws", nodeID: "", confirmed: true } },
      { tool: "complete_node_configuration", input: { workspaceID: "", nodeID: "n" } },
      { tool: "pipeline_workshop_open", input: { workspaceID: "" } },
      { tool: "pipeline_workshop_instruct", input: { sessionID: "", instruction: "go" } },
      { tool: "pipeline_workshop_instruct", input: { sessionID: "s", instruction: "" } },
      { tool: "pipeline_workshop_close", input: { sessionID: "" } },
      { tool: "plan_pipeline", input: { workspaceID: "" } },
      { tool: "create_pipeline_from_sql", input: { workspaceID: "", sql: "select 1" } },
      { tool: "create_pipeline_from_sql", input: { workspaceID: "ws", sql: "" } },
      { tool: "build_pipeline_from_intent", input: { workspaceID: "", intent: "stage customers" } },
      { tool: "build_pipeline_from_intent", input: { workspaceID: "ws", intent: "" } },
      { tool: "create_pipeline_from_plan", input: { workspaceID: "", plan: VALID_PIPELINE_PLAN } },
      { tool: "parse_sql_structure", input: { sql: "" } },
      { tool: "review_pipeline", input: { workspaceID: "" } },
      { tool: "select_pipeline_node_type", input: { workspaceID: "", sourceCount: 0 } },
      // RunDetailsSchema empty-string rejection — locks in the field-level
      // .min(1, "...when provided") in src/coalesce/run-schemas.ts so a
      // future refactor can't silently re-allow empty strings to flow into
      // the .refine() truthy check below it.
      { tool: "start_run", input: { runDetails: { environmentID: "", workspaceID: "ws-1" }, confirmRunAllNodes: true } },
      { tool: "start_run", input: { runDetails: { environmentID: "env-1", workspaceID: "" }, confirmRunAllNodes: true } },
      { tool: "start_run", input: { runDetails: { environmentID: "env-1", jobID: "" }, confirmRunAllNodes: true } },
      { tool: "run_and_wait", input: { runDetails: { environmentID: "", workspaceID: "ws-1" }, confirmRunAllNodes: true } },
      { tool: "run_and_wait", input: { runDetails: { environmentID: "env-1", jobID: "" }, confirmRunAllNodes: true } },
      { tool: "get_upstream_nodes", input: { workspaceID: "", nodeID: "n-1" } },
      { tool: "get_upstream_nodes", input: { workspaceID: "ws-1", nodeID: "" } },
      { tool: "get_downstream_nodes", input: { workspaceID: "", nodeID: "n-1" } },
      { tool: "get_column_lineage", input: { workspaceID: "ws-1", nodeID: "n-1", columnID: "" } },
      { tool: "analyze_impact", input: { workspaceID: "", nodeID: "n-1" } },
      { tool: "propagate_column_change", input: { workspaceID: "", nodeID: "n-1", columnID: "c-1", changes: { columnName: "X" }, confirmed: true } },
      { tool: "search_workspace_content", input: { workspaceID: "", query: "x" } },
      { tool: "audit_documentation_coverage", input: { workspaceID: "" } },
      { tool: "serialize_workspace_node_to_disk_yaml", input: { workspaceID: "", nodeID: "n-1" } },
      { tool: "serialize_workspace_node_to_disk_yaml", input: { workspaceID: "ws-1", nodeID: "" } },
      { tool: "apply_sql_to_workspace_node", input: { workspaceID: "", nodeID: "n-1", sql: "select 1" } },
      { tool: "apply_sql_to_workspace_node", input: { workspaceID: "ws-1", nodeID: "", sql: "select 1" } },
      // parse_disk_node_to_workspace_body: optional fields must reject empty
      // values when provided. An empty `yaml: ""` previously slipped past the
      // `.refine()` (since `data.yaml !== undefined`) and produced a confusing
      // "Provide either yaml or diskNode" error from the handler. An empty
      // `diskNode: {}` previously fed an empty object into diskNodeToCloud.
      { tool: "parse_disk_node_to_workspace_body", input: { yaml: "" } },
      { tool: "parse_disk_node_to_workspace_body", input: { diskNode: {} } },
      // Workflow tools — get_environment_overview / get_environment_health /
      // get_run_details. Their handlers call validatePathSegment, which throws
      // at runtime, but the schema-layer rejection is what makes the empty-
      // string contract consistent across every tool family.
      { tool: "get_environment_overview", input: { environmentID: "" } },
      { tool: "get_environment_health", input: { environmentID: "" } },
      { tool: "get_run_details", input: { runID: "" } },
      // coa_describe topic must not be empty — `coa describe ""` would shell
      // out with no topic and surface a confusing CLI error instead of a clear
      // tool-side validation message.
      { tool: "coa_describe", input: { topic: "" } },
      { tool: "coa_describe", input: { topic: "selectors", subtopic: "" } },
    ];

    for (const { tool, input } of cases) {
      const schema = getToolParamsSchema(toolSpy, tool);
      const result = schema.safeParse(input);
      expect(result.success, `${tool} should reject input ${JSON.stringify(input)}`).toBe(false);
    }
  });

  // Sanity check: the same tools accept non-empty IDs so we don't lock in over-strict schemas.
  it("accepts non-empty required IDs (no over-rejection)", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");

    const client = createMockClient() as any;
    defineEnvironmentTools(server, client).forEach(t => server.registerTool(...t));
    defineWorkspaceTools(server, client).forEach(t => server.registerTool(...t));

    expect(getToolParamsSchema(toolSpy, "get_environment").safeParse({ environmentID: "env-1" }).success).toBe(true);
    expect(getToolParamsSchema(toolSpy, "get_workspace").safeParse({ workspaceID: "ws-1" }).success).toBe(true);
  });
});

describe("Pipeline Tool Input Schemas", () => {
  it("requires structured config overrides for planner/create tools", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");

    definePipelineTools(server, createMockClient() as any).forEach(t => server.registerTool(...t));

    const planSchema = getToolParamsSchema(toolSpy, "plan_pipeline");
    const createFromSqlSchema = getToolParamsSchema(toolSpy, "create_pipeline_from_sql");

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
    const toolSpy = vi.spyOn(server, "registerTool");

    definePipelineTools(server, createMockClient() as any).forEach(t => server.registerTool(...t));

    const schema = getToolParamsSchema(toolSpy, "create_pipeline_from_plan");

    expect(
      schema.safeParse({
        workspaceID: "ws-1",
        plan: VALID_PIPELINE_PLAN,
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
