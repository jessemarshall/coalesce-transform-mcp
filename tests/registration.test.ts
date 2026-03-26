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
    toolSpy = vi.spyOn(server, "tool");
  });

  it("registers all tools", async () => {
    const client = createMockClient();
    registerServerSurface(server, client as any);

    // Verify expected tool names are registered
    const toolNames = toolSpy.mock.calls.map(
      (call: unknown[]) => call[0] as string
    );

    expect(toolSpy).toHaveBeenCalledTimes(72); // Removed deprecated create-workspace-node
    expect(toolNames).toContain("list-jobs");
    expect(toolNames).toContain("list-environments");
    expect(toolNames).toContain("get-environment");
    expect(toolNames).toContain("list-environment-nodes");
    expect(toolNames).toContain("update-workspace-node");
    expect(toolNames).toContain("create-workspace-node-from-scratch");
    expect(toolNames).toContain("create-workspace-node-from-predecessor");
    expect(toolNames).toContain("apply-join-condition");
    expect(toolNames).toContain("plan-pipeline");
    expect(toolNames).toContain("create-pipeline-from-plan");
    expect(toolNames).toContain("start-run");
    expect(toolNames).toContain("cancel-run");
    expect(toolNames).toContain("run-and-wait");
    expect(toolNames).toContain("get-run-details");
    expect(toolNames).toContain("delete-project");
    expect(toolNames).toContain("set-org-role");
    expect(toolNames).toContain("delete-git-account");
    expect(toolNames).toContain("search-node-type-variants");
    expect(toolNames).toContain("generate-set-workspace-node-template-from-variant");
    expect(toolNames).toContain("list-repo-packages");
    expect(toolNames).toContain("list-repo-node-types");
    expect(toolNames).toContain("get-repo-node-type-definition");
    expect(toolNames).toContain("generate-set-workspace-node-template");
    expect(toolNames).toContain("cache-workspace-nodes");
    expect(toolNames).toContain("cache-environment-nodes");
    expect(toolNames).toContain("cache-runs");
    expect(toolNames).toContain("cache-org-users");
    expect(toolNames).toContain("clear_coalesce_transform_mcp_data_cache");
    expect(toolNames).toContain("analyze-workspace-patterns");
    expect(toolNames).toContain("list-workspace-node-types");
    expect(toolNames).toContain("complete-node-configuration");

    const clearCacheCall = toolSpy.mock.calls.find(
      (call: unknown[]) => call[0] === "clear_coalesce_transform_mcp_data_cache"
    );
    expect(clearCacheCall).toBeDefined();
    expect(clearCacheCall?.[3]).toMatchObject({
      readOnlyHint: false,
      idempotentHint: false,
      destructiveHint: true,
    });
  });
});
