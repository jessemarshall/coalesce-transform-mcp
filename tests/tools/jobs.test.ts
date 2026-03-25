import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerJobTools } from "../../src/mcp/jobs.js";
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
    registerJobTools(server, client as any);
    expect(true).toBe(true);
  });

  it("list-jobs calls GET /api/v1/environments/{environmentID}/jobs", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "job-1", name: "Nightly" }] });

    const { listEnvironmentJobs } = await import("../../src/coalesce/api/jobs.js");
    const result = await listEnvironmentJobs(client as any, { environmentID: "env-1" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/environments/env-1/jobs", {});
    expect(result).toEqual({ data: [{ id: "job-1", name: "Nightly" }] });
  });

  it("list-jobs passes pagination params", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [] });

    const { listEnvironmentJobs } = await import("../../src/coalesce/api/jobs.js");
    await listEnvironmentJobs(client as any, { environmentID: "env-1", limit: 10, orderBy: "name" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/environments/env-1/jobs", {
      limit: 10,
      orderBy: "name",
    });
  });

  it("list-jobs throws CoalesceApiError from data-access layer", async () => {
    const client = createMockClient();
    client.get.mockRejectedValue(new CoalesceApiError("Not found", 404));

    const { listEnvironmentJobs } = await import("../../src/coalesce/api/jobs.js");
    await expect(listEnvironmentJobs(client as any, { environmentID: "bad" })).rejects.toThrow("Not found");
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
