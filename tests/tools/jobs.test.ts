import { describe, it, expect, vi } from "vitest";
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
