import { describe, it, expect, vi, beforeEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerJobTools } from "../../src/mcp/jobs.js";
import { registerSubgraphTools } from "../../src/mcp/subgraphs.js";
import { registerGitAccountTools } from "../../src/mcp/git-accounts.js";
import { registerUserTools } from "../../src/mcp/users.js";
import { registerRunTools } from "../../src/mcp/runs.js";
import { registerCacheTools } from "../../src/mcp/cache.js";
import { registerProjectTools } from "../../src/mcp/projects.js";
import { registerEnvironmentTools } from "../../src/mcp/environments.js";
import { registerNodeTools } from "../../src/mcp/nodes.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ ok: true }),
    put: vi.fn().mockResolvedValue({ ok: true }),
    patch: vi.fn().mockResolvedValue({ ok: true }),
    delete: vi.fn().mockResolvedValue({ message: "deleted" }),
  };
}

function extractHandler<T extends object>(
  spy: ReturnType<typeof vi.spyOn<McpServer, "registerTool">>,
  toolName: string
): (params: T, extra?: unknown) => Promise<{ content: Array<{ text: string }>; isError?: boolean; structuredContent?: unknown }> {
  const call = spy.mock.calls.find((c) => c[0] === toolName);
  if (!call) throw new Error(`Tool "${toolName}" was not registered`);
  return call[2] as any;
}

beforeEach(() => {
  vi.resetAllMocks();
});

describe("Destructive tool confirmation gating", () => {
  /**
   * Each test verifies that a destructive tool returns STOP_AND_CONFIRM
   * when called without confirmed=true, and does NOT call the underlying
   * API. This ensures an LLM agent cannot execute destructive operations
   * without explicit user confirmation.
   */

  it("delete_workspace_job returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerJobTools(server, client as any);

    const handler = extractHandler<{ workspaceID: string; jobID: string; confirmed?: boolean }>(spy, "delete_workspace_job");
    const result = await handler({ workspaceID: "ws-1", jobID: "job-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
    expect(client.delete).not.toHaveBeenCalled();
  });

  it("delete_workspace_job executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerJobTools(server, client as any);

    const handler = extractHandler<{ workspaceID: string; jobID: string; confirmed?: boolean }>(spy, "delete_workspace_job");
    const result = await handler({ workspaceID: "ws-1", jobID: "job-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    expect(client.delete).toHaveBeenCalled();
  });

  it("delete_workspace_subgraph returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerSubgraphTools(server, client as any);

    const handler = extractHandler<{ workspaceID: string; subgraphID: string; confirmed?: boolean }>(spy, "delete_workspace_subgraph");
    const result = await handler({ workspaceID: "ws-1", subgraphID: "sg-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
    expect(client.delete).not.toHaveBeenCalled();
  });

  it("delete_git_account returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerGitAccountTools(server, client as any);

    const handler = extractHandler<{ gitAccountID: string; confirmed?: boolean }>(spy, "delete_git_account");
    const result = await handler({ gitAccountID: "ga-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
    expect(client.delete).not.toHaveBeenCalled();
  });

  it("delete_project_role returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerUserTools(server, client as any);

    const handler = extractHandler<{ userID: string; projectID: string; confirmed?: boolean }>(spy, "delete_project_role");
    const result = await handler({ userID: "u-1", projectID: "proj-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
    expect(client.delete).not.toHaveBeenCalled();
  });

  it("delete_env_role returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerUserTools(server, client as any);

    const handler = extractHandler<{ userID: string; environmentID: string; confirmed?: boolean }>(spy, "delete_env_role");
    const result = await handler({ userID: "u-1", environmentID: "env-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
    expect(client.delete).not.toHaveBeenCalled();
  });

  it("cancel_run returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerRunTools(server, client as any);

    const handler = extractHandler<{ runID: string; environmentID: string; confirmed?: boolean }>(spy, "cancel_run");
    const result = await handler({ runID: "401", environmentID: "env-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
    expect(client.post).not.toHaveBeenCalledWith(
      "/scheduler/cancelRun",
      expect.anything()
    );
  });

  it("cancel_run executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerRunTools(server, client as any);

    process.env.COALESCE_ORG_ID = "org-1";

    const handler = extractHandler<{ runID: string; environmentID: string; confirmed?: boolean }>(spy, "cancel_run");
    const result = await handler({ runID: "401", environmentID: "env-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    expect(client.post).toHaveBeenCalledWith("/scheduler/cancelRun", expect.objectContaining({
      runID: "401",
      environmentID: "env-1",
    }));

    delete process.env.COALESCE_ORG_ID;
  });

  it("clear_data_cache returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerCacheTools(server, client as any);

    const handler = extractHandler<{ confirmed?: boolean }>(spy, "clear_data_cache");
    const result = await handler({});

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
  });

  it("delete_workspace_subgraph executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerSubgraphTools(server, client as any);

    const handler = extractHandler<{ workspaceID: string; subgraphID: string; confirmed?: boolean }>(spy, "delete_workspace_subgraph");
    const result = await handler({ workspaceID: "ws-1", subgraphID: "sg-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    expect(client.delete).toHaveBeenCalled();
  });

  it("delete_git_account executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerGitAccountTools(server, client as any);

    const handler = extractHandler<{ gitAccountID: string; confirmed?: boolean }>(spy, "delete_git_account");
    const result = await handler({ gitAccountID: "ga-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    expect(client.delete).toHaveBeenCalled();
  });

  it("delete_project_role executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerUserTools(server, client as any);

    const handler = extractHandler<{ userID: string; projectID: string; confirmed?: boolean }>(spy, "delete_project_role");
    const result = await handler({ userID: "u-1", projectID: "proj-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    expect(client.delete).toHaveBeenCalled();
  });

  it("delete_env_role executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerUserTools(server, client as any);

    const handler = extractHandler<{ userID: string; environmentID: string; confirmed?: boolean }>(spy, "delete_env_role");
    const result = await handler({ userID: "u-1", environmentID: "env-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    expect(client.delete).toHaveBeenCalled();
  });

  it("clear_data_cache executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerCacheTools(server, client as any);

    const handler = extractHandler<{ confirmed?: boolean }>(spy, "clear_data_cache");
    const result = await handler({ confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    // Tool proceeds past confirmation — deleted value depends on whether cache dir exists
    expect(data).toHaveProperty("deleted");
  });

  // Verify the 3 pre-existing guarded tools still gate correctly

  it("delete_workspace_node returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerNodeTools(server, client as any);

    const handler = extractHandler<{ workspaceID: string; nodeID: string; confirmed?: boolean }>(spy, "delete_workspace_node");
    const result = await handler({ workspaceID: "ws-1", nodeID: "node-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
    expect(client.delete).not.toHaveBeenCalled();
  });

  it("delete_workspace_node executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerNodeTools(server, client as any);

    const handler = extractHandler<{ workspaceID: string; nodeID: string; confirmed?: boolean }>(spy, "delete_workspace_node");
    const result = await handler({ workspaceID: "ws-1", nodeID: "node-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    expect(client.delete).toHaveBeenCalled();
  });

  it("delete_project returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerProjectTools(server, client as any);

    const handler = extractHandler<{ projectID: string; confirmed?: boolean }>(spy, "delete_project");
    const result = await handler({ projectID: "proj-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
    expect(client.delete).not.toHaveBeenCalled();
  });

  it("delete_environment returns STOP_AND_CONFIRM when not confirmed", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerEnvironmentTools(server, client as any);

    const handler = extractHandler<{ environmentID: string; confirmed?: boolean }>(spy, "delete_environment");
    const result = await handler({ environmentID: "env-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.executed).toBe(false);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain("confirmed=true");
    expect(client.delete).not.toHaveBeenCalled();
  });

  it("delete_project executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerProjectTools(server, client as any);

    const handler = extractHandler<{ projectID: string; confirmed?: boolean }>(spy, "delete_project");
    const result = await handler({ projectID: "proj-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    expect(client.delete).toHaveBeenCalled();
  });

  it("delete_environment executes when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    registerEnvironmentTools(server, client as any);

    const handler = extractHandler<{ environmentID: string; confirmed?: boolean }>(spy, "delete_environment");
    const result = await handler({ environmentID: "env-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeUndefined();
    expect(client.delete).toHaveBeenCalled();
  });
});
