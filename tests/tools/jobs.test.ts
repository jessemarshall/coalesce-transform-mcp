import { describe, it, expect, vi } from "vitest";
import type { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { defineJobTools } from "../../src/mcp/jobs.js";
import { CoalesceApiError } from "../../src/client.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

function extractHandler<T extends object>(
  spy: ReturnType<typeof vi.spyOn<McpServer, "registerTool">>,
  toolName: string
): (params: T) => Promise<{ content: Array<{ text: string }>; isError?: boolean; structuredContent?: unknown }> {
  const call = spy.mock.calls.find((c) => c[0] === toolName);
  if (!call) throw new Error(`Tool "${toolName}" was not registered`);
  return call[2] as any;
}

function extractInputSchema(
  spy: ReturnType<typeof vi.spyOn<McpServer, "registerTool">>,
  toolName: string
): z.ZodTypeAny {
  const call = spy.mock.calls.find((c) => c[0] === toolName);
  if (!call) throw new Error(`Tool "${toolName}" was not registered`);
  return (call[1] as { inputSchema: z.ZodTypeAny }).inputSchema;
}

describe("Job Tools", () => {
  it("registers all job tools without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    defineJobTools(server, client as any);
    expect(true).toBe(true);
  });

  it("list-environment-jobs scans sequential IDs and collects found jobs", async () => {
    const client = createMockClient();
    // Job at ID 3 exists, all others 404
    client.get.mockImplementation(async (path: string) => {
      if (path === "/api/v1/environments/env-1/jobs/3") {
        return { id: "3", name: "Nightly" };
      }
      throw new CoalesceApiError("Not found", 404);
    });

    const { listEnvironmentJobs } = await import("../../src/coalesce/api/jobs.js");
    const result = await listEnvironmentJobs(client as any, { environmentID: "env-1" }) as { data: unknown[] };

    expect(result.data).toHaveLength(1);
    expect(result.data[0]).toEqual({ id: "3", name: "Nightly" });
  });

  it("list-environment-jobs returns empty data when no jobs exist", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Not found", 404));

    const { listEnvironmentJobs } = await import("../../src/coalesce/api/jobs.js");
    const result = await listEnvironmentJobs(client as any, { environmentID: "env-1" }) as { data: unknown[] };

    expect(result.data).toHaveLength(0);
  });

  it("list-environment-jobs propagates non-404 errors", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Forbidden", 403));

    const { listEnvironmentJobs } = await import("../../src/coalesce/api/jobs.js");
    await expect(listEnvironmentJobs(client as any, { environmentID: "env-1" })).rejects.toThrow("Forbidden");
  });

  it("get-job calls GET /api/v1/environments/{environmentID}/jobs/{jobID}", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "job-1", name: "Nightly" });

    const { getEnvironmentJob } = await import("../../src/coalesce/api/jobs.js");
    const result = await getEnvironmentJob(client as any, { environmentID: "env-1", jobID: "job-1" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/environments/env-1/jobs/job-1", {});
    expect(result).toEqual({ id: "job-1", name: "Nightly" });
  });

  it("create-workspace-job calls POST /api/v1/workspaces/{workspaceID}/jobs", async () => {
    const client = createMockClient();
    client.post.mockResolvedValue({ id: "job-2" });

    const { createWorkspaceJob } = await import("../../src/coalesce/api/jobs.js");
    await createWorkspaceJob(client as any, {
      workspaceID: "ws-1",
      name: "Nightly",
      includeSelector: "{ location: ETL name: * }",
      excludeSelector: "",
    });

    expect(client.post).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/jobs",
      { name: "Nightly", includeSelector: "{ location: ETL name: * }", excludeSelector: "" }
    );
  });

  it("update-workspace-job calls PUT /api/v1/workspaces/{workspaceID}/jobs/{jobID}", async () => {
    const client = createMockClient();
    client.put.mockResolvedValue({ id: "job-1" });

    const { updateWorkspaceJob } = await import("../../src/coalesce/api/jobs.js");
    await updateWorkspaceJob(client as any, {
      workspaceID: "ws-1",
      jobID: "job-1",
      name: "Nightly v2",
      includeSelector: "{ location: ETL name: * }",
      excludeSelector: "",
    });

    expect(client.put).toHaveBeenCalledWith(
      "/api/v1/workspaces/ws-1/jobs/job-1",
      { name: "Nightly v2", includeSelector: "{ location: ETL name: * }", excludeSelector: "" }
    );
  });

  it("delete-workspace-job calls DELETE /api/v1/workspaces/{workspaceID}/jobs/{jobID}", async () => {
    const client = createMockClient();
    client.delete.mockResolvedValue({});

    const { deleteWorkspaceJob } = await import("../../src/coalesce/api/jobs.js");
    await deleteWorkspaceJob(client as any, { workspaceID: "ws-1", jobID: "job-1" });

    expect(client.delete).toHaveBeenCalledWith("/api/v1/workspaces/ws-1/jobs/job-1");
  });
});

describe("delete_workspace_job — resolve + confirmMessage handler logic", () => {
  // The destructive-confirmation suite covers the gating behavior. These tests
  // cover the resolve hook + confirmMessage details that are unique to jobs.ts:
  // the hook fetches via getWorkspaceJob (workspace-scoped, not env-scoped) and
  // the message label degrades from `"<name>" (<id>)` to `"<id>"` when the API
  // response has no usable name.

  it("surfaces the resolved job name in the confirmation message", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "job-1", name: "NIGHTLY_REFRESH" });
    defineJobTools(server, client as any).forEach(t => server.registerTool(...t));

    const handler = extractHandler<{ workspaceID: string; jobID: string; confirmed?: boolean }>(spy, "delete_workspace_job");
    const result = await handler({ workspaceID: "ws-1", jobID: "job-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    expect(data.STOP_AND_CONFIRM).toContain('"NIGHTLY_REFRESH"');
    expect(data.STOP_AND_CONFIRM).toContain("(job-1)");
    expect(data.STOP_AND_CONFIRM).toContain('workspace "ws-1"');
    expect(data.preview?.primary?.type).toBe("workspace_job");
    expect(data.preview?.primary?.name).toBe("NIGHTLY_REFRESH");
    expect(data.preview?.primary?.id).toBe("job-1");
    expect(client.delete).not.toHaveBeenCalled();
  });

  it("falls back to the jobID label when the resolved job has no name field", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    // API returns a job object but no name/label/displayName — extractEntityName returns undefined.
    client.get.mockResolvedValue({ id: "job-orphan" });
    defineJobTools(server, client as any).forEach(t => server.registerTool(...t));

    const handler = extractHandler<{ workspaceID: string; jobID: string; confirmed?: boolean }>(spy, "delete_workspace_job");
    const result = await handler({ workspaceID: "ws-1", jobID: "job-orphan" });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.STOP_AND_CONFIRM).toBeDefined();
    // No quoted name, falls back to bare ID.
    expect(data.STOP_AND_CONFIRM).toContain('"job-orphan"');
    expect(data.STOP_AND_CONFIRM).not.toContain("(job-orphan)");
    expect(data.preview?.primary?.name).toBeUndefined();
  });

  it("blocks the delete when getWorkspaceJob 404s even with confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Job not found", 404));
    defineJobTools(server, client as any).forEach(t => server.registerTool(...t));

    const handler = extractHandler<{ workspaceID: string; jobID: string; confirmed?: boolean }>(spy, "delete_workspace_job");
    const result = await handler({ workspaceID: "ws-1", jobID: "phantom-job", confirmed: true });

    expect(client.delete).not.toHaveBeenCalled();
    expect(result.isError).toBe(true);
    const text = result.content[0]!.text;
    expect(text).toContain("Refusing to run delete_workspace_job");
    expect(text).toContain("Job not found");
  });

  it("includes the resolved preview in the success response when confirmed=true", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "job-1", name: "NIGHTLY_REFRESH" });
    client.delete.mockResolvedValue({ message: "deleted" });
    defineJobTools(server, client as any).forEach(t => server.registerTool(...t));

    const handler = extractHandler<{ workspaceID: string; jobID: string; confirmed?: boolean }>(spy, "delete_workspace_job");
    const result = await handler({ workspaceID: "ws-1", jobID: "job-1", confirmed: true });

    const data = JSON.parse(result.content[0]!.text);
    expect(client.delete).toHaveBeenCalledWith("/api/v1/workspaces/ws-1/jobs/job-1");
    expect(data.resolvedTargets?.primary?.name).toBe("NIGHTLY_REFRESH");
    expect(data.resolvedTargets?.primary?.id).toBe("job-1");
    expect(data.resolvedTargets?.primary?.type).toBe("workspace_job");
  });

  it("resolve hook fetches against the WORKSPACE jobs endpoint (not the environment one)", async () => {
    // Regression guard: the previous draft of this tool used getEnvironmentJob
    // for resolution, which 404s for workspace-side IDs. Lock the correct path.
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "job-1", name: "NIGHTLY" });
    defineJobTools(server, client as any).forEach(t => server.registerTool(...t));

    const handler = extractHandler<{ workspaceID: string; jobID: string; confirmed?: boolean }>(spy, "delete_workspace_job");
    await handler({ workspaceID: "ws-7", jobID: "job-1" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/workspaces/ws-7/jobs/job-1", {});
  });
});

describe("list_job_nodes — schema-level jobID/jobName refine", () => {
  it("rejects input with neither jobID nor jobName", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    defineJobTools(server, createMockClient() as any).forEach(t => server.registerTool(...t));

    const schema = extractInputSchema(spy, "list_job_nodes");
    const result = schema.safeParse({ workspaceID: "ws-1" });
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(JSON.stringify(result.error.issues)).toContain(
        "Either jobID or jobName is required"
      );
    }
  });

  it("accepts jobID alone", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    defineJobTools(server, createMockClient() as any).forEach(t => server.registerTool(...t));

    const schema = extractInputSchema(spy, "list_job_nodes");
    expect(schema.safeParse({ workspaceID: "ws-1", jobID: "job-1" }).success).toBe(true);
  });

  it("accepts jobName alone", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    defineJobTools(server, createMockClient() as any).forEach(t => server.registerTool(...t));

    const schema = extractInputSchema(spy, "list_job_nodes");
    expect(schema.safeParse({ workspaceID: "ws-1", jobName: "Nightly" }).success).toBe(true);
  });

  it("rejects empty-string jobID/jobName as if they were absent", () => {
    // An empty optional string would otherwise satisfy the refine's
    // Boolean(v.jobID) check via Boolean("") === false, but only because
    // .min(1, "...when provided") rejects it at the field level first.
    // Lock both layers in.
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const spy = vi.spyOn(server, "registerTool");
    defineJobTools(server, createMockClient() as any).forEach(t => server.registerTool(...t));

    const schema = extractInputSchema(spy, "list_job_nodes");
    expect(schema.safeParse({ workspaceID: "ws-1", jobID: "" }).success).toBe(false);
    expect(schema.safeParse({ workspaceID: "ws-1", jobName: "" }).success).toBe(false);
    expect(schema.safeParse({ workspaceID: "ws-1", jobID: "", jobName: "" }).success).toBe(false);
  });
});
