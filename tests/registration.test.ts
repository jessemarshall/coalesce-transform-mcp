import { describe, it, expect, vi, beforeEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerServerSurface } from "../src/server.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

describe("Tool Registration", () => {
  let server: McpServer;
  let toolSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    server = new McpServer({ name: "test", version: "0.0.1" });
    toolSpy = vi.spyOn(server, "registerTool");
  });

  it("registers all tools", async () => {
    const client = createMockClient();
    registerServerSurface(server, client as any);

    // Verify expected tool names are registered
    const toolNames = toolSpy.mock.calls.map(
      (call: unknown[]) => call[0] as string
    );

    expect(toolSpy).toHaveBeenCalledTimes(77);
    expect(toolNames).toContain("coalesce_list_workspaces");
    expect(toolNames).toContain("coalesce_get_workspace");
    expect(toolNames).toContain("coalesce_list_environment_jobs");
    expect(toolNames).toContain("coalesce_list_workspace_jobs");
    expect(toolNames).toContain("coalesce_list_workspace_subgraphs");
    expect(toolNames).toContain("coalesce_update_environment");
    expect(toolNames).toContain("coalesce_get_environment_job");
    expect(toolNames).toContain("coalesce_list_environments");
    expect(toolNames).toContain("coalesce_get_environment");
    expect(toolNames).toContain("coalesce_list_environment_nodes");
    expect(toolNames).toContain("coalesce_update_workspace_node");
    expect(toolNames).toContain("coalesce_create_workspace_node_from_scratch");
    expect(toolNames).toContain("coalesce_create_workspace_node_from_predecessor");
    expect(toolNames).toContain("coalesce_apply_join_condition");
    expect(toolNames).toContain("coalesce_plan_pipeline");
    expect(toolNames).toContain("coalesce_create_pipeline_from_plan");
    expect(toolNames).toContain("coalesce_start_run");
    expect(toolNames).toContain("coalesce_cancel_run");
    expect(toolNames).toContain("coalesce_run_and_wait");
    expect(toolNames).toContain("coalesce_get_run_details");
    expect(toolNames).toContain("coalesce_delete_project");
    expect(toolNames).toContain("coalesce_set_org_role");
    expect(toolNames).toContain("coalesce_delete_git_account");
    expect(toolNames).toContain("coalesce_search_node_type_variants");
    expect(toolNames).toContain("coalesce_generate_set_workspace_node_template_from_variant");
    expect(toolNames).toContain("coalesce_list_repo_packages");
    expect(toolNames).toContain("coalesce_list_repo_node_types");
    expect(toolNames).toContain("coalesce_get_repo_node_type_definition");
    expect(toolNames).toContain("coalesce_generate_set_workspace_node_template");
    expect(toolNames).toContain("coalesce_cache_workspace_nodes");
    expect(toolNames).toContain("coalesce_cache_environment_nodes");
    expect(toolNames).toContain("coalesce_cache_runs");
    expect(toolNames).toContain("coalesce_cache_org_users");
    expect(toolNames).toContain("coalesce_clear_data_cache");
    expect(toolNames).toContain("coalesce_analyze_workspace_patterns");
    expect(toolNames).toContain("coalesce_list_workspace_node_types");
    expect(toolNames).toContain("coalesce_complete_node_configuration");

    const clearCacheCall = toolSpy.mock.calls.find(
      (call: unknown[]) => call[0] === "coalesce_clear_data_cache"
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
});
