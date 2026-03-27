import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  getRunDetails,
  registerGetRunDetails,
} from "../../src/workflows/get-run-details.js";
import { POSTMAN_RUN_DETAILS_RESPONSE } from "../fixtures/postman-examples.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ ok: true }),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("get-run-details workflow", () => {
  it("registers without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerGetRunDetails(server, client as any);
    expect(true).toBe(true);
  });

  it("calls both GET endpoints in parallel and returns combined result", async () => {
    const client = createMockClient();
    const runData = POSTMAN_RUN_DETAILS_RESPONSE;
    const resultsData = { results: [{ nodeID: "n1", status: "success" }] };

    client.get
      .mockImplementation((path: string) => {
        if (path === "/api/v1/runs/0") return Promise.resolve(runData);
        if (path === "/api/v1/runs/0/results") return Promise.resolve(resultsData);
        return Promise.resolve({});
      });

    const result = await getRunDetails(client as any, { runID: "0" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/runs/0");
    expect(client.get).toHaveBeenCalledWith("/api/v1/runs/0/results");
    expect(client.get).toHaveBeenCalledTimes(2);
    expect(result).toEqual({ run: runData, results: resultsData });
  });

  it("returns run data with resultsError when results endpoint fails", async () => {
    const client = createMockClient();
    const runData = POSTMAN_RUN_DETAILS_RESPONSE;

    client.get
      .mockImplementation((path: string) => {
        if (path === "/api/v1/runs/0") return Promise.resolve(runData);
        if (path === "/api/v1/runs/0/results") return Promise.reject(new Error("Resource not found"));
        return Promise.resolve({});
      });

    const result = await getRunDetails(client as any, { runID: "0" });

    expect(result.run).toEqual(runData);
    expect(result.results).toBeNull();
    expect(result.resultsError).toEqual({ message: "Resource not found" });
  });
});
