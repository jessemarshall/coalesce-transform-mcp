/**
 * Live API integration test — calls every registered MCP tool against a real
 * Coalesce environment.
 *
 * Required env vars (suite skips if any are missing):
 *   COALESCE_ACCESS_TOKEN, TEST_WORKSPACE_ID, TEST_ENVIRONMENT_ID, TEST_NODE_ID
 *
 * Optional env vars (individual tests skip if missing):
 *   TEST_JOB_ID, TEST_RUN_ID, TEST_PROJECT_ID
 *   SNOWFLAKE_USERNAME, SNOWFLAKE_KEY_PAIR_KEY, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
 *   COALESCE_BASE_URL, COALESCE_REPO_PATH
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { validateConfig, createClient } from "../../src/client.js";
import {
  createCoalesceMcpServer,
  SERVER_NAME,
} from "../../src/server.js";
import { LoopbackTransport } from "../helpers/mcp-harness.js";

// ── Gate flags ──────────────────────────────────────────────────────────────

const HAS_REQUIRED = !!(
  process.env.COALESCE_ACCESS_TOKEN &&
  process.env.TEST_WORKSPACE_ID &&
  process.env.TEST_ENVIRONMENT_ID &&
  process.env.TEST_NODE_ID
);
const HAS_SNOWFLAKE = !!(
  process.env.SNOWFLAKE_USERNAME &&
  process.env.SNOWFLAKE_KEY_PAIR_KEY &&
  process.env.SNOWFLAKE_WAREHOUSE &&
  process.env.SNOWFLAKE_ROLE
);
const HAS_REPO = !!process.env.COALESCE_REPO_PATH;

// ── Env-var helpers ─────────────────────────────────────────────────────────

const WORKSPACE_ID = process.env.TEST_WORKSPACE_ID!;
const ENVIRONMENT_ID = process.env.TEST_ENVIRONMENT_ID!;
const NODE_ID = process.env.TEST_NODE_ID!;
const JOB_ID = process.env.TEST_JOB_ID;
const RUN_ID = process.env.TEST_RUN_ID;
const PROJECT_ID = process.env.TEST_PROJECT_ID;

// ── Assertion helpers ───────────────────────────────────────────────────────

type ToolResult = Awaited<ReturnType<Client["callTool"]>>;

function getToolText(response: ToolResult): string {
  const first = response.content?.[0];
  if (first && "text" in first) return first.text as string;
  return JSON.stringify(response.content);
}

function assertToolSuccess(response: ToolResult, toolName: string) {
  if (response.isError) {
    throw new Error(`${toolName} returned error: ${getToolText(response)}`);
  }
  expect(response.content).toBeDefined();
  expect(response.content!.length).toBeGreaterThan(0);
}

function assertToolSuccessOrExpected(
  response: ToolResult,
  toolName: string,
  allowedPatterns: string[],
) {
  if (response.isError) {
    const text = getToolText(response);
    const isExpected = allowedPatterns.some((p) => text.includes(p));
    if (!isExpected) {
      throw new Error(`${toolName} unexpected error: ${text}`);
    }
    return;
  }
  expect(response.content).toBeDefined();
  expect(response.content!.length).toBeGreaterThan(0);
}

function parseStructured(response: ToolResult): Record<string, unknown> {
  if (response.structuredContent) {
    return response.structuredContent as Record<string, unknown>;
  }
  const text = getToolText(response);
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

// ── Main suite ──────────────────────────────────────────────────────────────

describe.skipIf(!HAS_REQUIRED)("Live API — All MCP Tools", { timeout: 60_000 }, () => {
  let mcpClient: Client;
  let closeHarness: () => Promise<void>;

  // Shared state populated by discovery phase
  const discovered: Record<string, string | undefined> = {};
  // Resources created by write tests, for cleanup
  const created: Record<string, string | undefined> = {};
  // Track which tools we tested
  const testedTools = new Set<string>();

  // ── Setup & teardown ────────────────────────────────────────────────────

  beforeAll(async () => {
    const config = validateConfig();
    const apiClient = createClient(config);
    const server = createCoalesceMcpServer(apiClient);

    const serverTransport = new LoopbackTransport();
    const clientTransport = new LoopbackTransport();
    serverTransport.peer = clientTransport;
    clientTransport.peer = serverTransport;

    mcpClient = new Client(
      { name: `${SERVER_NAME}-live-test`, version: "0.0.1" },
      { capabilities: {} },
    );

    await server.connect(serverTransport);
    await mcpClient.connect(clientTransport);

    closeHarness = async () => {
      await Promise.allSettled([mcpClient.close(), server.close()]);
    };
  });

  afterAll(async () => {
    console.log(`\n✅ Tools tested: ${testedTools.size}`);
    await closeHarness?.();
  });

  async function callTool(name: string, args: Record<string, unknown> = {}) {
    testedTools.add(name);
    return mcpClient.callTool({ name, arguments: args });
  }

  // ── Phase 1: Discovery ────────────────────────────────────────────────

  describe("Phase 1 — Discovery", { timeout: 60_000 }, () => {
    it("list_workspaces", async () => {
      const res = await callTool("list_workspaces");
      assertToolSuccess(res, "list_workspaces");
    });

    it("list_environments", async () => {
      const res = await callTool("list_environments");
      assertToolSuccess(res, "list_environments");
    });

    it("list_workspace_nodes — discover columnID", async () => {
      const res = await callTool("list_workspace_nodes", {
        workspaceID: WORKSPACE_ID,
        limit: 10,
      });
      assertToolSuccess(res, "list_workspace_nodes");
      // Try to discover a column ID from the first node
      const data = parseStructured(res);
      const nodes = (data.data ?? data) as any[];
      if (nodes?.[0]?.id) {
        discovered.firstNodeID = nodes[0].id;
      }
    });

    it("list_environment_nodes", async () => {
      const res = await callTool("list_environment_nodes", {
        environmentID: ENVIRONMENT_ID,
        limit: 10,
      });
      assertToolSuccess(res, "list_environment_nodes");
      const data = parseStructured(res);
      const nodes = (data.data ?? data) as any[];
      if (nodes?.[0]?.id) {
        discovered.envNodeID = nodes[0].id;
      }
    });

    it("list_environment_jobs", async () => {
      const res = await callTool("list_environment_jobs", {
        environmentID: ENVIRONMENT_ID,
      });
      assertToolSuccess(res, "list_environment_jobs");
      const data = parseStructured(res);
      const jobs = (data.data ?? data) as any[];
      if (jobs?.[0]?.id) {
        discovered.jobID = jobs[0].id;
      }
    });

    it("list_workspace_jobs", async () => {
      const res = await callTool("list_workspace_jobs", {
        workspaceID: WORKSPACE_ID,
      });
      assertToolSuccess(res, "list_workspace_jobs");
    });

    it("list_runs", async () => {
      const res = await callTool("list_runs", { limit: 5 });
      assertToolSuccess(res, "list_runs");
      const data = parseStructured(res);
      const runs = (data.data ?? data) as any[];
      if (runs?.[0]?.id) {
        discovered.runID = String(runs[0].id);
        discovered.runCounter = runs[0].runCounter != null
          ? String(runs[0].runCounter)
          : undefined;
      }
    });

    it("list_projects", async () => {
      const res = await callTool("list_projects");
      assertToolSuccess(res, "list_projects");
      const data = parseStructured(res);
      const projects = (data.data ?? data) as any[];
      if (projects?.[0]?.id) {
        discovered.projectID = projects[0].id;
      }
    });

    it("list_org_users", async () => {
      const res = await callTool("list_org_users");
      assertToolSuccess(res, "list_org_users");
      const data = parseStructured(res);
      const users = (data.data ?? data) as any[];
      if (users?.[0]?.id) {
        discovered.userID = users[0].id;
      }
    });

    it("list_git_accounts", async () => {
      const res = await callTool("list_git_accounts");
      assertToolSuccess(res, "list_git_accounts");
      const data = parseStructured(res);
      const accounts = (data.data ?? data) as any[];
      if (accounts?.[0]?.id) {
        discovered.gitAccountID = accounts[0].id;
      }
    });

    it("list_workspace_subgraphs", async () => {
      const res = await callTool("list_workspace_subgraphs", {
        workspaceID: WORKSPACE_ID,
      });
      assertToolSuccess(res, "list_workspace_subgraphs");
      const data = parseStructured(res);
      const subgraphs = (data.data ?? data) as any[];
      if (subgraphs?.[0]?.id) {
        discovered.subgraphID = subgraphs[0].id;
      }
    });
  });

  // ── Phase 2: Read-Only Tools ──────────────────────────────────────────

  describe("Phase 2 — Read-Only Tools", { timeout: 60_000 }, () => {
    // Workspace & Environment
    it("get_workspace", async () => {
      const res = await callTool("get_workspace", { workspaceID: WORKSPACE_ID });
      assertToolSuccess(res, "get_workspace");
    });

    it("get_environment", async () => {
      const res = await callTool("get_environment", { environmentID: ENVIRONMENT_ID });
      assertToolSuccess(res, "get_environment");
    });

    it("preview_deployment", async () => {
      const res = await callTool("preview_deployment", {
        workspaceID: WORKSPACE_ID,
        environmentID: ENVIRONMENT_ID,
      });
      assertToolSuccessOrExpected(res, "preview_deployment", [
        "not found",
        "No changes",
      ]);
    });

    it("get_environment_overview", async () => {
      const res = await callTool("get_environment_overview", {
        environmentID: ENVIRONMENT_ID,
      });
      assertToolSuccess(res, "get_environment_overview");
    });

    it("get_environment_health", { timeout: 120_000 }, async () => {
      const res = await callTool("get_environment_health", {
        environmentID: ENVIRONMENT_ID,
      });
      assertToolSuccess(res, "get_environment_health");
    });

    // Nodes
    it("get_workspace_node", async () => {
      const res = await callTool("get_workspace_node", {
        workspaceID: WORKSPACE_ID,
        nodeID: NODE_ID,
      });
      assertToolSuccess(res, "get_workspace_node");
      // Discover a columnID for lineage tests
      const data = parseStructured(res);
      const columns = (data as any)?.metadata?.columns ?? (data as any)?.columns;
      if (Array.isArray(columns) && columns[0]?.columnReference?.columnCounter) {
        discovered.columnID = columns[0].columnReference.columnCounter;
      } else if (Array.isArray(columns) && columns[0]?.id) {
        discovered.columnID = columns[0].id;
      }
    });

    it("get_environment_node", async () => {
      const envNode = discovered.envNodeID;
      if (!envNode) return; // skip if no env nodes
      const res = await callTool("get_environment_node", {
        environmentID: ENVIRONMENT_ID,
        nodeID: envNode,
      });
      assertToolSuccess(res, "get_environment_node");
    });

    it("analyze_workspace_patterns", async () => {
      const res = await callTool("analyze_workspace_patterns", {
        workspaceID: WORKSPACE_ID,
      });
      assertToolSuccess(res, "analyze_workspace_patterns");
    });

    it("list_workspace_node_types", async () => {
      const res = await callTool("list_workspace_node_types", {
        workspaceID: WORKSPACE_ID,
      });
      assertToolSuccess(res, "list_workspace_node_types");
      // Store first node type for write phase
      const data = parseStructured(res);
      const types = (data.data ?? data) as any[];
      if (Array.isArray(types) && types.length > 0) {
        // nodeTypes could be strings or objects
        const first = typeof types[0] === "string" ? types[0] : types[0]?.nodeType ?? types[0]?.name;
        if (first) discovered.nodeType = first;
      }
    });

    // Jobs
    it("get_environment_job", async () => {
      const jid = JOB_ID ?? discovered.jobID;
      if (!jid) return;
      const res = await callTool("get_environment_job", {
        environmentID: ENVIRONMENT_ID,
        jobID: jid,
      });
      assertToolSuccess(res, "get_environment_job");
    });

    // Runs
    it("get_run", async () => {
      const rid = RUN_ID ?? discovered.runID;
      if (!rid) return;
      const res = await callTool("get_run", { runID: rid });
      assertToolSuccessOrExpected(res, "get_run", ["not found"]);
    });

    it("get_run_results", async () => {
      const rid = RUN_ID ?? discovered.runID;
      if (!rid) return;
      const res = await callTool("get_run_results", { runID: rid });
      assertToolSuccessOrExpected(res, "get_run_results", [
        "not found",
        "No results",
        "no results",
      ]);
    });

    it("run_status", async () => {
      // run_status expects runCounter as z.number(). Only use values we know are numeric.
      const raw = discovered.runCounter ?? RUN_ID;
      if (!raw) return;
      const counter = Number(raw);
      if (!Number.isFinite(counter)) return; // skip if RUN_ID is non-numeric (e.g. UUID)
      const res = await callTool("run_status", { runCounter: counter });
      assertToolSuccessOrExpected(res, "run_status", ["not found"]);
    });

    it("get_run_details", async () => {
      const rid = RUN_ID ?? discovered.runID;
      if (!rid) return;
      const res = await callTool("get_run_details", { runID: rid });
      assertToolSuccessOrExpected(res, "get_run_details", ["not found"]);
    });

    it("diagnose_run_failure", async () => {
      const rid = RUN_ID ?? discovered.runID;
      if (!rid) return;
      const res = await callTool("diagnose_run_failure", { runID: rid });
      assertToolSuccessOrExpected(res, "diagnose_run_failure", [
        "not found",
        "not failed",
        "No failure",
        "completed",
        "cancelled",
        "running",
        "waiting",
      ]);
    });

    // Projects
    it("get_project", async () => {
      const pid = PROJECT_ID ?? discovered.projectID;
      if (!pid) return;
      const res = await callTool("get_project", { projectID: pid });
      assertToolSuccess(res, "get_project");
    });

    // Users
    it("get_user_roles", async () => {
      const uid = discovered.userID;
      if (!uid) return;
      const res = await callTool("get_user_roles", { userID: uid });
      assertToolSuccess(res, "get_user_roles");
    });

    it("list_user_roles", async () => {
      const res = await callTool("list_user_roles");
      assertToolSuccess(res, "list_user_roles");
    });

    // Git accounts
    it("get_git_account", async () => {
      const gid = discovered.gitAccountID;
      if (!gid) return;
      const res = await callTool("get_git_account", { gitAccountID: gid });
      assertToolSuccess(res, "get_git_account");
    });

    // Subgraphs
    it("get_workspace_subgraph", async () => {
      const sid = discovered.subgraphID;
      if (!sid) return;
      const res = await callTool("get_workspace_subgraph", {
        workspaceID: WORKSPACE_ID,
        subgraphID: sid,
      });
      assertToolSuccess(res, "get_workspace_subgraph");
    });

    // Pipelines (read-only)
    it("plan_pipeline", { timeout: 120_000 }, async () => {
      const res = await callTool("plan_pipeline", {
        workspaceID: WORKSPACE_ID,
        goal: "Create a staging node for integration test validation",
      });
      assertToolSuccess(res, "plan_pipeline");
      // Store full structured response — plan_pipeline returns plan fields at top level,
      // not nested under a `plan` key
      const data = parseStructured(res);
      if (data && typeof data === "object" && "status" in (data as object)) {
        discovered.planResponseJSON = JSON.stringify(data);
      }
    });

    it("parse_sql_structure", async () => {
      const res = await callTool("parse_sql_structure", {
        sql: "SELECT id, name FROM customers WHERE active = true",
      });
      assertToolSuccess(res, "parse_sql_structure");
    });

    it("select_pipeline_node_type", async () => {
      const res = await callTool("select_pipeline_node_type", {
        workspaceID: WORKSPACE_ID,
        goal: "staging layer for raw data",
      });
      assertToolSuccess(res, "select_pipeline_node_type");
    });

    it("review_pipeline", async () => {
      const res = await callTool("review_pipeline", {
        workspaceID: WORKSPACE_ID,
      });
      assertToolSuccess(res, "review_pipeline");
    });

    // Lineage
    it("get_upstream_nodes", { timeout: 120_000 }, async () => {
      const res = await callTool("get_upstream_nodes", {
        workspaceID: WORKSPACE_ID,
        nodeID: NODE_ID,
      });
      assertToolSuccess(res, "get_upstream_nodes");
    });

    it("get_downstream_nodes", { timeout: 120_000 }, async () => {
      const res = await callTool("get_downstream_nodes", {
        workspaceID: WORKSPACE_ID,
        nodeID: NODE_ID,
      });
      assertToolSuccess(res, "get_downstream_nodes");
    });

    it("get_column_lineage", { timeout: 120_000 }, async () => {
      const colID = discovered.columnID;
      if (!colID) return;
      const res = await callTool("get_column_lineage", {
        workspaceID: WORKSPACE_ID,
        nodeID: NODE_ID,
        columnID: colID,
      });
      assertToolSuccessOrExpected(res, "get_column_lineage", [
        "not found",
        "No column",
      ]);
    });

    it("analyze_impact", { timeout: 120_000 }, async () => {
      const res = await callTool("analyze_impact", {
        workspaceID: WORKSPACE_ID,
        nodeID: NODE_ID,
      });
      assertToolSuccess(res, "analyze_impact");
    });

    // Cache tools
    it("cache_workspace_nodes", async () => {
      const res = await callTool("cache_workspace_nodes", {
        workspaceID: WORKSPACE_ID,
      });
      assertToolSuccess(res, "cache_workspace_nodes");
    });

    it("cache_environment_nodes", async () => {
      const res = await callTool("cache_environment_nodes", {
        environmentID: ENVIRONMENT_ID,
      });
      assertToolSuccess(res, "cache_environment_nodes");
    });

    it("cache_runs", async () => {
      const res = await callTool("cache_runs");
      assertToolSuccess(res, "cache_runs");
    });

    it("cache_org_users", async () => {
      const res = await callTool("cache_org_users");
      assertToolSuccess(res, "cache_org_users");
    });

    it("clear_data_cache", async () => {
      const res = await callTool("clear_data_cache", { confirmed: true });
      assertToolSuccess(res, "clear_data_cache");
    });

    // Corpus tools (local snapshot — may not exist)
    it("search_node_type_variants", async () => {
      const res = await callTool("search_node_type_variants", {});
      // Graceful: snapshot may not exist
      assertToolSuccessOrExpected(res, "search_node_type_variants", [
        "snapshot not found",
        "not found",
        "unreadable",
      ]);
      if (!res.isError) {
        const data = parseStructured(res);
        const variants = (data.matches ?? data.variants ?? data.data) as any[];
        if (Array.isArray(variants) && variants[0]?.variantKey) {
          discovered.variantKey = variants[0].variantKey;
        }
      }
    });

    it("get_node_type_variant", async () => {
      const key = discovered.variantKey;
      if (!key) return;
      const res = await callTool("get_node_type_variant", { variantKey: key });
      assertToolSuccessOrExpected(res, "get_node_type_variant", [
        "not found",
        "unreadable",
      ]);
    });

    it("generate_set_workspace_node_template_from_variant", async () => {
      const key = discovered.variantKey;
      if (!key) return;
      const res = await callTool("generate_set_workspace_node_template_from_variant", {
        variantKey: key,
        allowPartial: true,
      });
      assertToolSuccessOrExpected(
        res,
        "generate_set_workspace_node_template_from_variant",
        ["not found", "unreadable", "not supported", "partial"],
      );
    });

    // Repo-backed tools
    it.skipIf(!HAS_REPO)("list_repo_packages", async () => {
      const res = await callTool("list_repo_packages");
      assertToolSuccess(res, "list_repo_packages");
    });

    it.skipIf(!HAS_REPO)("list_repo_node_types", async () => {
      const res = await callTool("list_repo_node_types");
      assertToolSuccess(res, "list_repo_node_types");
    });

    it.skipIf(!HAS_REPO)("get_repo_node_type_definition", async () => {
      const res = await callTool("get_repo_node_type_definition", {
        nodeType: "Stage",
      });
      assertToolSuccessOrExpected(res, "get_repo_node_type_definition", [
        "not found",
      ]);
    });

    it.skipIf(!HAS_REPO)("generate_set_workspace_node_template", async () => {
      const res = await callTool("generate_set_workspace_node_template", {
        nodeType: "Stage",
      });
      assertToolSuccessOrExpected(res, "generate_set_workspace_node_template", [
        "not found",
      ]);
    });
  });

  // ── Phase 3: Write Tools ──────────────────────────────────────────────

  describe("Phase 3 — Write Tools", { timeout: 60_000 }, () => {
    const ts = Date.now();

    // Project lifecycle
    it("create_project", async () => {
      const res = await callTool("create_project", {
        name: `mcp-live-test-${ts}`,
      });
      assertToolSuccess(res, "create_project");
      const data = parseStructured(res);
      created.projectID = (data as any)?.id ?? (data as any)?.data?.id;
    });

    it("update_project", async () => {
      if (!created.projectID) return;
      const res = await callTool("update_project", {
        projectID: created.projectID,
        description: "Integration test project",
      });
      assertToolSuccess(res, "update_project");
    });

    // Environment lifecycle
    it("create_environment", async () => {
      const pid = created.projectID ?? PROJECT_ID ?? discovered.projectID;
      if (!pid) return;
      const res = await callTool("create_environment", {
        projectID: pid,
        name: `mcp-test-env-${ts}`,
      });
      assertToolSuccess(res, "create_environment");
      const data = parseStructured(res);
      created.environmentID = (data as any)?.id ?? (data as any)?.data?.id;
    });

    it("update_environment", async () => {
      if (!created.environmentID) return;
      const res = await callTool("update_environment", {
        environmentID: created.environmentID,
        name: `mcp-test-env-updated-${ts}`,
      });
      assertToolSuccess(res, "update_environment");
    });

    // Node lifecycle
    it("create_workspace_node_from_scratch", async () => {
      const nodeType = discovered.nodeType ?? "Stage";
      const res = await callTool("create_workspace_node_from_scratch", {
        workspaceID: WORKSPACE_ID,
        nodeType,
        name: `MCP_TEST_SCRATCH_${ts}`,
        completionLevel: "named",
      });
      assertToolSuccess(res, "create_workspace_node_from_scratch");
      const data = parseStructured(res);
      created.scratchNodeID = (data as any)?.id ?? (data as any)?.nodeID ?? (data as any)?.data?.id;
    });

    it("update_workspace_node", async () => {
      if (!created.scratchNodeID) return;
      const res = await callTool("update_workspace_node", {
        workspaceID: WORKSPACE_ID,
        nodeID: created.scratchNodeID,
        changes: { description: "Integration test node" },
      });
      assertToolSuccess(res, "update_workspace_node");
    });

    it("set_workspace_node", async () => {
      if (!created.scratchNodeID) return;
      const res = await callTool("set_workspace_node", {
        workspaceID: WORKSPACE_ID,
        nodeID: created.scratchNodeID,
        body: { description: "Set via integration test" },
      });
      assertToolSuccess(res, "set_workspace_node");
    });

    it("replace_workspace_node_columns", async () => {
      if (!created.scratchNodeID) return;
      const res = await callTool("replace_workspace_node_columns", {
        workspaceID: WORKSPACE_ID,
        nodeID: created.scratchNodeID,
        columns: [
          {
            name: "TEST_COL",
            dataType: "VARCHAR(100)",
            nullable: true,
          },
        ],
      });
      assertToolSuccess(res, "replace_workspace_node_columns");
    });

    it("complete_node_configuration", async () => {
      if (!created.scratchNodeID) return;
      const res = await callTool("complete_node_configuration", {
        workspaceID: WORKSPACE_ID,
        nodeID: created.scratchNodeID,
      });
      assertToolSuccessOrExpected(res, "complete_node_configuration", [
        "No repo",
        "no repo",
        "not found",
        "not configured",
        "COALESCE_REPO_PATH",
      ]);
    });

    it("create_workspace_node_from_predecessor", async () => {
      const nodeType = discovered.nodeType ?? "Stage";
      const res = await callTool("create_workspace_node_from_predecessor", {
        workspaceID: WORKSPACE_ID,
        nodeType,
        predecessorNodeIDs: [NODE_ID],
        changes: { name: `MCP_TEST_PRED_${ts}` },
      });
      assertToolSuccess(res, "create_workspace_node_from_predecessor");
      const data = parseStructured(res);
      created.predecessorNodeID = (data as any)?.id ?? (data as any)?.nodeID ?? (data as any)?.data?.id;
    });

    // Job lifecycle
    it("create_workspace_job", async () => {
      const res = await callTool("create_workspace_job", {
        workspaceID: WORKSPACE_ID,
        name: `mcp-test-job-${ts}`,
        includeSelector: "",
        excludeSelector: "",
      });
      assertToolSuccess(res, "create_workspace_job");
      const data = parseStructured(res);
      created.jobID = (data as any)?.id ?? (data as any)?.data?.id;
    });

    it("update_workspace_job", async () => {
      if (!created.jobID) return;
      const res = await callTool("update_workspace_job", {
        workspaceID: WORKSPACE_ID,
        jobID: created.jobID,
        name: `mcp-test-job-upd-${ts}`,
        includeSelector: "",
        excludeSelector: "",
      });
      assertToolSuccess(res, "update_workspace_job");
    });

    // Subgraph lifecycle
    it("create_workspace_subgraph", async () => {
      const nodeForSubgraph = created.scratchNodeID ?? NODE_ID;
      const res = await callTool("create_workspace_subgraph", {
        workspaceID: WORKSPACE_ID,
        name: `mcp-test-subgraph-${ts}`,
        steps: [nodeForSubgraph],
      });
      assertToolSuccess(res, "create_workspace_subgraph");
      const data = parseStructured(res);
      created.subgraphID = (data as any)?.id ?? (data as any)?.data?.id;
    });

    it("update_workspace_subgraph", async () => {
      if (!created.subgraphID) return;
      const nodeForSubgraph = created.scratchNodeID ?? NODE_ID;
      const res = await callTool("update_workspace_subgraph", {
        workspaceID: WORKSPACE_ID,
        subgraphID: created.subgraphID,
        name: `mcp-test-subgraph-upd-${ts}`,
        steps: [nodeForSubgraph],
      });
      assertToolSuccess(res, "update_workspace_subgraph");
    });

    // Git account lifecycle
    it("create_git_account", async () => {
      const res = await callTool("create_git_account", {
        name: `mcp-test-git-${ts}`,
      });
      assertToolSuccess(res, "create_git_account");
      const data = parseStructured(res);
      created.gitAccountID = (data as any)?.id ?? (data as any)?.data?.id;
    });

    it("update_git_account", async () => {
      if (!created.gitAccountID) return;
      const res = await callTool("update_git_account", {
        gitAccountID: created.gitAccountID,
        name: `mcp-test-git-upd-${ts}`,
      });
      assertToolSuccess(res, "update_git_account");
    });

    // User role tools (use created project/env so we don't modify real permissions)
    it("set_project_role", async () => {
      const uid = discovered.userID;
      const pid = created.projectID;
      if (!uid || !pid) return;
      const res = await callTool("set_project_role", {
        userID: uid,
        projectID: pid,
        role: "viewer",
      });
      assertToolSuccessOrExpected(res, "set_project_role", [
        "not found",
        "permission",
      ]);
    });

    it("set_env_role", async () => {
      const uid = discovered.userID;
      const eid = created.environmentID;
      if (!uid || !eid) return;
      const res = await callTool("set_env_role", {
        userID: uid,
        environmentID: eid,
        role: "viewer",
      });
      assertToolSuccessOrExpected(res, "set_env_role", [
        "not found",
        "permission",
      ]);
    });

    // Workshop lifecycle
    it("pipeline_workshop_open", { timeout: 120_000 }, async () => {
      const res = await callTool("pipeline_workshop_open", {
        workspaceID: WORKSPACE_ID,
      });
      assertToolSuccess(res, "pipeline_workshop_open");
      const data = parseStructured(res);
      created.workshopSessionID = (data as any)?.sessionID ?? (data as any)?.id;
    });

    it("pipeline_workshop_instruct", { timeout: 120_000 }, async () => {
      if (!created.workshopSessionID) return;
      const res = await callTool("pipeline_workshop_instruct", {
        sessionID: created.workshopSessionID,
        instruction: "Add a staging node called TEST_WORKSHOP_NODE",
      });
      assertToolSuccess(res, "pipeline_workshop_instruct");
    });

    it("get_pipeline_workshop_status", async () => {
      if (!created.workshopSessionID) return;
      const res = await callTool("get_pipeline_workshop_status", {
        sessionID: created.workshopSessionID,
      });
      assertToolSuccess(res, "get_pipeline_workshop_status");
    });

    it("pipeline_workshop_close", async () => {
      if (!created.workshopSessionID) return;
      const res = await callTool("pipeline_workshop_close", {
        sessionID: created.workshopSessionID,
      });
      assertToolSuccess(res, "pipeline_workshop_close");
    });

    // Pipeline write tools (dry run to avoid actual mutations)
    it("create_pipeline_from_sql — dryRun", { timeout: 120_000 }, async () => {
      const res = await callTool("create_pipeline_from_sql", {
        workspaceID: WORKSPACE_ID,
        sql: "SELECT id, name FROM customers",
        dryRun: true,
      });
      // dryRun still returns STOP_AND_CONFIRM or a preview — both are valid
      assertToolSuccessOrExpected(res, "create_pipeline_from_sql", [
        "STOP_AND_CONFIRM",
        "confirm",
      ]);
    });

    it("build_pipeline_from_intent — dryRun", { timeout: 120_000 }, async () => {
      const res = await callTool("build_pipeline_from_intent", {
        workspaceID: WORKSPACE_ID,
        intent: "Create a staging node for customer data",
        dryRun: true,
      });
      assertToolSuccessOrExpected(res, "build_pipeline_from_intent", [
        "STOP_AND_CONFIRM",
        "confirm",
      ]);
    });

    it("create_pipeline_from_plan — dryRun", { timeout: 120_000 }, async () => {
      // plan_pipeline returns plan fields at top level (not nested under `plan`).
      // create_pipeline_from_plan expects `plan` to match PipelinePlanSchema.strict(),
      // so we must extract only the schema-compatible fields.
      const PLAN_SCHEMA_KEYS = [
        "version", "intent", "status", "workspaceID", "platform", "goal", "sql",
        "nodes", "assumptions", "openQuestions", "warnings", "supportedNodeTypes",
        "nodeTypeSelection", "cteNodeSummary", "STOP_AND_CONFIRM",
      ] as const;

      function extractPlan(raw: unknown): Record<string, unknown> | undefined {
        if (!raw || typeof raw !== "object") return undefined;
        const src = raw as Record<string, unknown>;
        if (!("version" in src) || !("intent" in src)) return undefined;
        const plan: Record<string, unknown> = {};
        for (const key of PLAN_SCHEMA_KEYS) {
          if (key in src) plan[key] = src[key];
        }
        return plan;
      }

      let plan: Record<string, unknown> | undefined;
      if (discovered.planResponseJSON) {
        try {
          plan = extractPlan(JSON.parse(discovered.planResponseJSON));
        } catch {
          plan = undefined;
        }
      }
      if (!plan) {
        // Call plan_pipeline to get a fresh plan
        const planRes = await callTool("plan_pipeline", {
          workspaceID: WORKSPACE_ID,
          goal: "Dry-run test: create staging node",
        });
        if (!planRes.isError) {
          plan = extractPlan(parseStructured(planRes));
        }
      }
      if (!plan) return; // can't test without a plan

      const res = await callTool("create_pipeline_from_plan", {
        workspaceID: WORKSPACE_ID,
        plan,
        dryRun: true,
      });
      assertToolSuccessOrExpected(res, "create_pipeline_from_plan", [
        "STOP_AND_CONFIRM",
        "confirm",
      ]);
    });

    // Run tools (require Snowflake credentials)
    it.skipIf(!HAS_SNOWFLAKE)("start_run", { timeout: 120_000 }, async () => {
      const jid = JOB_ID ?? discovered.jobID;
      if (!jid) return;
      const res = await callTool("start_run", {
        runDetails: {
          environmentID: ENVIRONMENT_ID,
          jobID: jid,
        },
      });
      assertToolSuccessOrExpected(res, "start_run", [
        "STOP_AND_CONFIRM",
        "confirm",
        "credentials",
        "Snowflake",
      ]);
      const data = parseStructured(res);
      if ((data as any)?.runCounter) {
        created.runCounter = String((data as any).runCounter);
      }
    });

    it.skipIf(!HAS_SNOWFLAKE)("run_and_wait", { timeout: 300_000 }, async () => {
      const jid = JOB_ID ?? discovered.jobID;
      if (!jid) return;
      const res = await callTool("run_and_wait", {
        runDetails: {
          environmentID: ENVIRONMENT_ID,
          jobID: jid,
        },
      });
      assertToolSuccessOrExpected(res, "run_and_wait", [
        "STOP_AND_CONFIRM",
        "confirm",
        "credentials",
        "Snowflake",
      ]);
    });

    it.skipIf(!HAS_SNOWFLAKE)("retry_run", { timeout: 120_000 }, async () => {
      const rid = RUN_ID ?? discovered.runID;
      if (!rid) return;
      const res = await callTool("retry_run", {
        runDetails: { runID: rid },
      });
      assertToolSuccessOrExpected(res, "retry_run", [
        "not found",
        "credentials",
        "Snowflake",
        "cannot retry",
        "Cannot retry",
        "not failed",
      ]);
    });

    it.skipIf(!HAS_SNOWFLAKE)("retry_and_wait", { timeout: 300_000 }, async () => {
      const rid = RUN_ID ?? discovered.runID;
      if (!rid) return;
      const res = await callTool("retry_and_wait", {
        runDetails: { runID: rid },
      });
      assertToolSuccessOrExpected(res, "retry_and_wait", [
        "not found",
        "credentials",
        "Snowflake",
        "cannot retry",
        "Cannot retry",
        "not failed",
      ]);
    });

    it.skipIf(!HAS_SNOWFLAKE)("cancel_run", async () => {
      const counter = created.runCounter;
      if (!counter) return;
      const res = await callTool("cancel_run", {
        runID: counter,
        environmentID: ENVIRONMENT_ID,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "cancel_run", [
        "not found",
        "not running",
        "already",
        "Cannot cancel",
      ]);
    });
  });

  // ── Phase 4: Destructive Cleanup ──────────────────────────────────────

  describe("Phase 4 — Destructive Cleanup", { timeout: 60_000 }, () => {
    it("delete_workspace_node (scratch)", async () => {
      if (!created.scratchNodeID) return;
      const res = await callTool("delete_workspace_node", {
        workspaceID: WORKSPACE_ID,
        nodeID: created.scratchNodeID,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "delete_workspace_node", ["not found"]);
    });

    it("delete_workspace_node (predecessor)", async () => {
      if (!created.predecessorNodeID) return;
      const res = await callTool("delete_workspace_node", {
        workspaceID: WORKSPACE_ID,
        nodeID: created.predecessorNodeID,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "delete_workspace_node", ["not found"]);
    });

    it("delete_workspace_job", async () => {
      if (!created.jobID) return;
      const res = await callTool("delete_workspace_job", {
        workspaceID: WORKSPACE_ID,
        jobID: created.jobID,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "delete_workspace_job", ["not found"]);
    });

    it("delete_workspace_subgraph", async () => {
      if (!created.subgraphID) return;
      const res = await callTool("delete_workspace_subgraph", {
        workspaceID: WORKSPACE_ID,
        subgraphID: created.subgraphID,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "delete_workspace_subgraph", ["not found"]);
    });

    it("delete_git_account", async () => {
      if (!created.gitAccountID) return;
      const res = await callTool("delete_git_account", {
        gitAccountID: created.gitAccountID,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "delete_git_account", ["not found"]);
    });

    it("delete_project_role", async () => {
      const uid = discovered.userID;
      const pid = created.projectID;
      if (!uid || !pid) return;
      const res = await callTool("delete_project_role", {
        userID: uid,
        projectID: pid,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "delete_project_role", [
        "not found",
        "permission",
      ]);
    });

    it("delete_env_role", async () => {
      const uid = discovered.userID;
      const eid = created.environmentID;
      if (!uid || !eid) return;
      const res = await callTool("delete_env_role", {
        userID: uid,
        environmentID: eid,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "delete_env_role", [
        "not found",
        "permission",
      ]);
    });

    it("delete_environment", async () => {
      if (!created.environmentID) return;
      const res = await callTool("delete_environment", {
        environmentID: created.environmentID,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "delete_environment", ["not found"]);
    });

    it("delete_project", async () => {
      if (!created.projectID) return;
      const res = await callTool("delete_project", {
        projectID: created.projectID,
        confirmed: true,
      });
      assertToolSuccessOrExpected(res, "delete_project", ["not found"]);
    });
  });
});
