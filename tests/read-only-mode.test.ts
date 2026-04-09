import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { InMemoryTaskStore, InMemoryTaskMessageQueue } from "../src/tasks/store.js";
import { registerServerSurface, isReadOnlyMode } from "../src/server.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

function createServer(): McpServer {
  return new McpServer(
    { name: "test", version: "0.0.1" },
    {
      capabilities: { tasks: { requests: { tools: { call: {} } } } },
      taskStore: new InMemoryTaskStore(),
      taskMessageQueue: new InMemoryTaskMessageQueue(),
    }
  );
}

// Tools that should always be visible (read-only + cache snapshots)
const READ_ONLY_TOOLS = [
  "list_environments",
  "get_environment",
  "list_workspaces",
  "get_workspace",
  "list_environment_nodes",
  "list_workspace_nodes",
  "get_environment_node",
  "get_workspace_node",
  "analyze_workspace_patterns",
  "list_workspace_node_types",
  "list_environment_jobs",
  "get_environment_job",
  "list_workspace_subgraphs",
  "get_workspace_subgraph",
  "list_runs",
  "get_run",
  "get_run_results",
  "diagnose_run_failure",
  "run_status",
  "list_projects",
  "get_project",
  "list_git_accounts",
  "get_git_account",
  "list_org_users",
  "get_user_roles",
  "list_user_roles",
  "parse_sql_structure",
  "select_pipeline_node_type",
  "plan_pipeline",
  "review_pipeline",
  "get_pipeline_workshop_status",
  "search_node_type_variants",
  "get_node_type_variant",
  "generate_set_workspace_node_template_from_variant",
  "list_repo_packages",
  "list_repo_node_types",
  "get_repo_node_type_definition",
  "generate_set_workspace_node_template",
  "cache_workspace_nodes",
  "cache_environment_nodes",
  "cache_runs",
  "cache_org_users",
  "get_run_details",
  "get_environment_overview",
  "get_environment_health",
  "get_upstream_nodes",
  "get_downstream_nodes",
  "get_column_lineage",
  "analyze_impact",
  "search_workspace_content",
  "audit_documentation_coverage",
  "personalize_skills",
];

// Tools that should be hidden in read-only mode
const WRITE_TOOLS = [
  "create_environment",
  "delete_environment",
  "create_workspace_node_from_scratch",
  "create_workspace_node_from_predecessor",
  "create_node_from_external_schema",
  "set_workspace_node",
  "update_workspace_node",
  "replace_workspace_node_columns",
  "convert_join_to_aggregation",
  "apply_join_condition",
  "complete_node_configuration",
  "delete_workspace_node",
  "create_workspace_job",
  "update_workspace_job",
  "delete_workspace_job",
  "create_workspace_subgraph",
  "update_workspace_subgraph",
  "delete_workspace_subgraph",
  "retry_run",
  "cancel_run",
  "create_project",
  "update_project",
  "delete_project",
  "create_git_account",
  "update_git_account",
  "delete_git_account",
  "set_org_role",
  "set_project_role",
  "delete_project_role",
  "set_env_role",
  "delete_env_role",
  "create_pipeline_from_plan",
  "create_pipeline_from_sql",
  "build_pipeline_from_intent",
  "pipeline_workshop_open",
  "pipeline_workshop_instruct",
  "pipeline_workshop_close",
  "clear_data_cache",
  "propagate_column_change",
];

// Task-based tools — all write operations
const WRITE_TASK_TOOLS = ["start_run", "run_and_wait", "retry_and_wait"];

describe("Read-Only Mode", () => {
  const originalEnv = process.env.COALESCE_MCP_READ_ONLY;

  afterEach(() => {
    if (originalEnv === undefined) {
      delete process.env.COALESCE_MCP_READ_ONLY;
    } else {
      process.env.COALESCE_MCP_READ_ONLY = originalEnv;
    }
  });

  describe("isReadOnlyMode()", () => {
    it("returns true when COALESCE_MCP_READ_ONLY is 'true'", () => {
      process.env.COALESCE_MCP_READ_ONLY = "true";
      expect(isReadOnlyMode()).toBe(true);
    });

    it("returns false when COALESCE_MCP_READ_ONLY is unset", () => {
      delete process.env.COALESCE_MCP_READ_ONLY;
      expect(isReadOnlyMode()).toBe(false);
    });

    it("returns false when COALESCE_MCP_READ_ONLY is 'false'", () => {
      process.env.COALESCE_MCP_READ_ONLY = "false";
      expect(isReadOnlyMode()).toBe(false);
    });

    it("returns false for non-exact values like 'TRUE' or '1'", () => {
      process.env.COALESCE_MCP_READ_ONLY = "TRUE";
      expect(isReadOnlyMode()).toBe(false);
      process.env.COALESCE_MCP_READ_ONLY = "1";
      expect(isReadOnlyMode()).toBe(false);
    });
  });

  describe("when COALESCE_MCP_READ_ONLY=true", () => {
    beforeEach(() => {
      process.env.COALESCE_MCP_READ_ONLY = "true";
    });

    it("registers only read-only tools", () => {
      const server = createServer();
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      registerServerSurface(server, client as any);

      const registeredNames = toolSpy.mock.calls.map(
        (call: unknown[]) => call[0] as string
      );

      for (const name of READ_ONLY_TOOLS) {
        expect(registeredNames, `expected ${name} to be registered`).toContain(name);
      }
    });

    it("hides all write/mutation tools", () => {
      const server = createServer();
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      registerServerSurface(server, client as any);

      const registeredNames = toolSpy.mock.calls.map(
        (call: unknown[]) => call[0] as string
      );

      for (const name of WRITE_TOOLS) {
        expect(registeredNames, `expected ${name} to be hidden`).not.toContain(name);
      }
    });

    it("hides write task-based tools", () => {
      const server = createServer();
      const client = createMockClient();

      registerServerSurface(server, client as any);

      const registeredTools = (server as any)._registeredTools;
      const allToolNames = Object.keys(registeredTools);

      for (const name of WRITE_TASK_TOOLS) {
        expect(allToolNames, `expected task tool ${name} to be hidden`).not.toContain(name);
      }
    });

    it("keeps cache snapshot tools visible", () => {
      const server = createServer();
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      registerServerSurface(server, client as any);

      const registeredNames = toolSpy.mock.calls.map(
        (call: unknown[]) => call[0] as string
      );

      const cacheSnapshots = [
        "cache_workspace_nodes",
        "cache_environment_nodes",
        "cache_runs",
        "cache_org_users",
      ];

      for (const name of cacheSnapshots) {
        expect(registeredNames, `expected ${name} to remain visible`).toContain(name);
      }
    });
  });

  describe("when COALESCE_MCP_READ_ONLY is unset", () => {
    beforeEach(() => {
      delete process.env.COALESCE_MCP_READ_ONLY;
    });

    it("registers all tools including write tools", () => {
      const server = createServer();
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      registerServerSurface(server, client as any);

      const registeredNames = toolSpy.mock.calls.map(
        (call: unknown[]) => call[0] as string
      );

      for (const name of WRITE_TOOLS) {
        expect(registeredNames, `expected ${name} to be registered`).toContain(name);
      }
    });

    it("registers task-based write tools", () => {
      const server = createServer();
      const client = createMockClient();

      registerServerSurface(server, client as any);

      const registeredTools = (server as any)._registeredTools;
      const allToolNames = Object.keys(registeredTools);

      for (const name of WRITE_TASK_TOOLS) {
        expect(allToolNames, `expected task tool ${name} to be registered`).toContain(name);
      }
    });
  });

  describe("when COALESCE_MCP_READ_ONLY=false", () => {
    beforeEach(() => {
      process.env.COALESCE_MCP_READ_ONLY = "false";
    });

    it("registers all tools including write tools", () => {
      const server = createServer();
      const toolSpy = vi.spyOn(server, "registerTool");
      const client = createMockClient();

      registerServerSurface(server, client as any);

      const registeredNames = toolSpy.mock.calls.map(
        (call: unknown[]) => call[0] as string
      );

      for (const name of WRITE_TOOLS) {
        expect(registeredNames, `expected ${name} to be registered`).toContain(name);
      }
    });
  });
});
