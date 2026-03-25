import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  listGitAccounts,
  getGitAccount,
  createGitAccount,
  updateGitAccount,
  deleteGitAccount,
} from "../../src/coalesce/api/git-accounts.js";
import { registerGitAccountTools } from "../../src/mcp/git-accounts.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ ok: true }),
    put: vi.fn().mockResolvedValue({ ok: true }),
    patch: vi.fn().mockResolvedValue({ ok: true }),
    delete: vi.fn().mockResolvedValue({ message: "Operation completed successfully" }),
  };
}

describe("Git Account Tools", () => {
  it("registers all 5 git account tools without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerGitAccountTools(server, client as any);
    expect(true).toBe(true);
  });

  it("listGitAccounts calls GET /api/v1/gitAccounts", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ data: [{ id: "ga-1" }] });

    const result = await listGitAccounts(client as any);

    expect(client.get).toHaveBeenCalledWith("/api/v1/gitAccounts", {});
    expect(result).toEqual({ data: [{ id: "ga-1" }] });
  });

  it("getGitAccount calls GET /api/v1/gitAccounts/{gitAccountID}", async () => {
    const client = createMockClient();
    client.get.mockResolvedValue({ id: "ga-1", name: "My Git Account" });

    const result = await getGitAccount(client as any, { gitAccountID: "ga-1" });

    expect(client.get).toHaveBeenCalledWith("/api/v1/gitAccounts/ga-1", {});
    expect(result).toEqual({ id: "ga-1", name: "My Git Account" });
  });

  it("createGitAccount calls POST /api/v1/gitAccounts with body", async () => {
    const client = createMockClient();
    const body = { name: "New Git Account" };
    client.post.mockResolvedValue({ id: "ga-2", name: "New Git Account" });

    const result = await createGitAccount(client as any, { body });

    expect(client.post).toHaveBeenCalledWith("/api/v1/gitAccounts", body, undefined);
    expect(result).toEqual({ id: "ga-2", name: "New Git Account" });
  });

  it("updateGitAccount calls PATCH /api/v1/gitAccounts/{gitAccountID} with body", async () => {
    const client = createMockClient();
    const body = { name: "Updated Git Account" };
    client.patch.mockResolvedValue({ id: "ga-1", name: "Updated Git Account" });

    const result = await updateGitAccount(client as any, { gitAccountID: "ga-1", body });

    expect(client.patch).toHaveBeenCalledWith("/api/v1/gitAccounts/ga-1", body, {});
    expect(result).toEqual({ id: "ga-1", name: "Updated Git Account" });
  });

  it("deleteGitAccount calls DELETE /api/v1/gitAccounts/{gitAccountID}", async () => {
    const client = createMockClient();
    client.delete.mockResolvedValue({ message: "Operation completed successfully" });

    const result = await deleteGitAccount(client as any, { gitAccountID: "ga-1" });

    expect(client.delete).toHaveBeenCalledWith("/api/v1/gitAccounts/ga-1", undefined);
    expect(result).toEqual({ message: "Operation completed successfully" });
  });
});
