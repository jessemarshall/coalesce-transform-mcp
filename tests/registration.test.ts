import { describe, it, expect, vi, beforeEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerServerSurface } from "../src/server.js";

vi.mock("node:child_process", async (importOriginal) => {
  const original = await importOriginal<typeof import("node:child_process")>();
  return {
    ...original,
    execFileSync: vi.fn(),
  };
});

import { execFileSync } from "node:child_process";
const mockExecFileSync = vi.mocked(execFileSync);

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

// Cortex tools are conditionally registered based on whether the cortex CLI is installed.
const CORTEX_TOOL_NAMES = [
  "explore_data_source",
  "query_snowflake",
  "search_snowflake_objects",
  "list_snowflake_connections",
];
const BASE_TOOL_COUNT = 84;

describe("Tool Registration", () => {
  let server: McpServer;
  let toolSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    server = new McpServer({ name: "test", version: "0.0.1" });
    toolSpy = vi.spyOn(server, "registerTool");
    vi.clearAllMocks();
    toolSpy = vi.spyOn(server, "registerTool");
  });

  it("registers base tools when cortex is not installed", async () => {
    mockExecFileSync.mockImplementation(() => {
      throw new Error("command not found");
    });
    const client = createMockClient();
    registerServerSurface(server, client as any);

    const toolNames = toolSpy.mock.calls.map(
      (call: unknown[]) => call[0] as string
    );

    expect(toolSpy).toHaveBeenCalledTimes(BASE_TOOL_COUNT);

    // Core tools present
    expect(toolNames).toContain("list_workspaces");
    expect(toolNames).toContain("get_workspace");
    expect(toolNames).toContain("list_environment_jobs");
    expect(toolNames).toContain("list_workspace_jobs");
    expect(toolNames).toContain("list_workspace_subgraphs");
    expect(toolNames).toContain("update_environment");
    expect(toolNames).toContain("get_environment_job");
    expect(toolNames).toContain("list_environments");
    expect(toolNames).toContain("get_environment");
    expect(toolNames).toContain("list_environment_nodes");
    expect(toolNames).toContain("update_workspace_node");
    expect(toolNames).toContain("create_workspace_node_from_scratch");
    expect(toolNames).toContain("create_workspace_node_from_predecessor");
    expect(toolNames).toContain("apply_join_condition");
    expect(toolNames).toContain("plan_pipeline");
    expect(toolNames).toContain("create_pipeline_from_plan");
    expect(toolNames).toContain("build_pipeline_from_intent");
    expect(toolNames).toContain("start_run");
    expect(toolNames).toContain("cancel_run");
    expect(toolNames).toContain("run_and_wait");
    expect(toolNames).toContain("get_run_details");
    expect(toolNames).toContain("delete_project");
    expect(toolNames).toContain("set_org_role");
    expect(toolNames).toContain("delete_git_account");
    expect(toolNames).toContain("search_node_type_variants");
    expect(toolNames).toContain("generate_set_workspace_node_template_from_variant");
    expect(toolNames).toContain("list_repo_packages");
    expect(toolNames).toContain("list_repo_node_types");
    expect(toolNames).toContain("get_repo_node_type_definition");
    expect(toolNames).toContain("generate_set_workspace_node_template");
    expect(toolNames).toContain("cache_workspace_nodes");
    expect(toolNames).toContain("cache_environment_nodes");
    expect(toolNames).toContain("cache_runs");
    expect(toolNames).toContain("cache_org_users");
    expect(toolNames).toContain("clear_data_cache");
    expect(toolNames).toContain("analyze_workspace_patterns");
    expect(toolNames).toContain("diagnose_run_failure");
    expect(toolNames).toContain("review_pipeline");
    expect(toolNames).toContain("pipeline_workshop_open");
    expect(toolNames).toContain("pipeline_workshop_instruct");
    expect(toolNames).toContain("pipeline_workshop_status");
    expect(toolNames).toContain("pipeline_workshop_close");
    expect(toolNames).toContain("list_workspace_node_types");
    expect(toolNames).toContain("complete_node_configuration");

    // Cortex tools NOT registered
    for (const name of CORTEX_TOOL_NAMES) {
      expect(toolNames).not.toContain(name);
    }

    const clearCacheCall = toolSpy.mock.calls.find(
      (call: unknown[]) => call[0] === "clear_data_cache"
    );
    expect(clearCacheCall).toBeDefined();
    expect(clearCacheCall?.[1]).toMatchObject({
      annotations: {
      readOnlyHint: false,
      idempotentHint: false,
      destructiveHint: true,
      },
    });
  });

  it("registers cortex tools when cortex is installed", async () => {
    mockExecFileSync.mockReturnValue(Buffer.from("1.2.3"));
    const client = createMockClient();
    registerServerSurface(server, client as any);

    const toolNames = toolSpy.mock.calls.map(
      (call: unknown[]) => call[0] as string
    );

    expect(toolSpy).toHaveBeenCalledTimes(
      BASE_TOOL_COUNT + CORTEX_TOOL_NAMES.length
    );

    for (const name of CORTEX_TOOL_NAMES) {
      expect(toolNames).toContain(name);
    }
  });
});
