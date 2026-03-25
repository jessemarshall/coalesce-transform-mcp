import { describe, it, expect, vi, beforeEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

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

    const { registerEnvironmentTools } = await import(
      "../src/mcp/environments.js"
    );
    const { registerNodeTools } = await import("../src/mcp/nodes.js");
    const { registerPipelineTools } = await import("../src/mcp/pipelines.js");
    const { registerRunTools } = await import("../src/mcp/runs.js");
    const { registerProjectTools } = await import("../src/mcp/projects.js");
    const { registerGitAccountTools } = await import(
      "../src/mcp/git-accounts.js"
    );
    const { registerUserTools } = await import("../src/mcp/users.js");
    const { registerNodeTypeCorpusTools } = await import(
      "../src/mcp/node-type-corpus.js"
    );
    const { registerRepoNodeTypeTools } = await import(
      "../src/mcp/repo-node-types.js"
    );
    const { registerJobTools } = await import("../src/mcp/jobs.js");
    const { registerSubgraphTools } = await import(
      "../src/mcp/subgraphs.js"
    );
    const { registerCacheTools } = await import(
      "../src/mcp/cache.js"
    );
    const { registerRunAndWait } = await import(
      "../src/workflows/run-and-wait.js"
    );
    const { registerRetryAndWait } = await import(
      "../src/workflows/retry-and-wait.js"
    );
    const { registerGetRunDetails } = await import(
      "../src/workflows/get-run-details.js"
    );
    const { registerGetEnvironmentOverview } = await import(
      "../src/workflows/get-environment-overview.js"
    );

    registerEnvironmentTools(server, client as any);
    registerNodeTools(server, client as any);
    registerPipelineTools(server, client as any);
    registerRunTools(server, client as any);
    registerProjectTools(server, client as any);
    registerGitAccountTools(server, client as any);
    registerUserTools(server, client as any);
    registerNodeTypeCorpusTools(server, client as any);
    registerRepoNodeTypeTools(server, client as any);
    registerJobTools(server, client as any);
    registerSubgraphTools(server, client as any);
    registerCacheTools(server, client as any);
    registerRunAndWait(server, client as any);
    registerRetryAndWait(server, client as any);
    registerGetRunDetails(server, client as any);
    registerGetEnvironmentOverview(server, client as any);

    // Verify expected tool names are registered
    const toolNames = toolSpy.mock.calls.map(
      (call: unknown[]) => call[0] as string
    );

    expect(toolSpy).toHaveBeenCalledTimes(71); // Removed deprecated create-workspace-node
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
  });
});
