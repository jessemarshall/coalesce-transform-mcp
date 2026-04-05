import { describe, it, expect, vi } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import {
  listGitAccounts,
  getGitAccount,
  createGitAccount,
  updateGitAccount,
  deleteGitAccount,
} from "../../src/coalesce/api/git-accounts.js";
import * as gitAccountsApi from "../../src/coalesce/api/git-accounts.js";
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

function extractHandler<T extends object>(
  spy: ReturnType<typeof vi.spyOn<McpServer, "registerTool">>,
  toolName: string
): (params: T, extra?: unknown) => Promise<{ content: Array<{ text: string }>; isError?: boolean }> {
  const call = spy.mock.calls.find((c) => c[0] === toolName);
  if (!call) throw new Error(`Tool "${toolName}" was not registered`);
  return call[2] as (params: T, extra?: unknown) => Promise<{ content: Array<{ text: string }>; isError?: boolean }>;
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

  // --- Handler-level tests ---

  describe("create_git_account handler", () => {
    it("maps name to gitAccountName in the API call body", async () => {
      const createSpy = vi.spyOn(gitAccountsApi, "createGitAccount").mockResolvedValue({ id: "ga-new" } as any);
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      registerGitAccountTools(server, client as any);

      const handler = extractHandler<{
        name: string;
        gitUsername: string;
        gitAuthorName: string;
        gitAuthorEmail: string;
        gitToken: string;
        provider?: string;
        accountOwner?: string;
      }>(spy, "create_git_account");

      await handler({
        name: "My Account",
        gitUsername: "jmarshall",
        gitAuthorName: "Jesse",
        gitAuthorEmail: "jesse@example.com",
        gitToken: "ghp_abc123",
      });

      expect(createSpy).toHaveBeenCalledWith(expect.anything(), {
        body: {
          gitAccountName: "My Account",
          gitUsername: "jmarshall",
          gitAuthorName: "Jesse",
          gitAuthorEmail: "jesse@example.com",
          gitToken: "ghp_abc123",
        },
        accountOwner: undefined,
      });
      createSpy.mockRestore();
    });

    it("passes accountOwner separately from the body", async () => {
      const createSpy = vi.spyOn(gitAccountsApi, "createGitAccount").mockResolvedValue({ id: "ga-new" } as any);
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      registerGitAccountTools(server, client as any);

      const handler = extractHandler<{
        name: string;
        gitUsername: string;
        gitAuthorName: string;
        gitAuthorEmail: string;
        gitToken: string;
        accountOwner?: string;
      }>(spy, "create_git_account");

      await handler({
        name: "My Account",
        gitUsername: "jmarshall",
        gitAuthorName: "Jesse",
        gitAuthorEmail: "jesse@example.com",
        gitToken: "ghp_abc123",
        accountOwner: "user-42",
      });

      expect(createSpy).toHaveBeenCalledWith(expect.anything(), {
        body: expect.objectContaining({ gitAccountName: "My Account" }),
        accountOwner: "user-42",
      });
      // accountOwner should NOT be in the body
      const callBody = createSpy.mock.calls[0]![1].body;
      expect(callBody).not.toHaveProperty("accountOwner");
      createSpy.mockRestore();
    });
  });

  describe("update_git_account handler", () => {
    it("maps name to gitAccountName in the PATCH body", async () => {
      const updateSpy = vi.spyOn(gitAccountsApi, "updateGitAccount").mockResolvedValue({ id: "ga-1" } as any);
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      registerGitAccountTools(server, client as any);

      const handler = extractHandler<{
        gitAccountID: string;
        name?: string;
        gitUsername?: string;
        accountOwner?: string;
      }>(spy, "update_git_account");

      await handler({
        gitAccountID: "ga-1",
        name: "Renamed Account",
        gitUsername: "newuser",
      });

      expect(updateSpy).toHaveBeenCalledWith(expect.anything(), {
        gitAccountID: "ga-1",
        body: {
          gitAccountName: "Renamed Account",
          gitUsername: "newuser",
        },
        accountOwner: undefined,
      });
      updateSpy.mockRestore();
    });

    it("omits gitAccountName from body when name is not provided", async () => {
      const updateSpy = vi.spyOn(gitAccountsApi, "updateGitAccount").mockResolvedValue({ id: "ga-1" } as any);
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      registerGitAccountTools(server, client as any);

      const handler = extractHandler<{
        gitAccountID: string;
        name?: string;
        gitToken?: string;
        accountOwner?: string;
      }>(spy, "update_git_account");

      await handler({
        gitAccountID: "ga-1",
        gitToken: "ghp_newtoken",
      });

      expect(updateSpy).toHaveBeenCalledWith(expect.anything(), {
        gitAccountID: "ga-1",
        body: { gitToken: "ghp_newtoken" },
        accountOwner: undefined,
      });
      const callBody = updateSpy.mock.calls[0]![1].body;
      expect(callBody).not.toHaveProperty("gitAccountName");
      expect(callBody).not.toHaveProperty("name");
      updateSpy.mockRestore();
    });

    it("passes accountOwner separately from the body", async () => {
      const updateSpy = vi.spyOn(gitAccountsApi, "updateGitAccount").mockResolvedValue({ id: "ga-1" } as any);
      const server = new McpServer({ name: "test", version: "0.0.1" });
      const spy = vi.spyOn(server, "registerTool");
      const client = createMockClient();
      registerGitAccountTools(server, client as any);

      const handler = extractHandler<{
        gitAccountID: string;
        name?: string;
        accountOwner?: string;
      }>(spy, "update_git_account");

      await handler({
        gitAccountID: "ga-1",
        name: "Updated",
        accountOwner: "user-99",
      });

      expect(updateSpy).toHaveBeenCalledWith(expect.anything(), {
        gitAccountID: "ga-1",
        body: { gitAccountName: "Updated" },
        accountOwner: "user-99",
      });
      updateSpy.mockRestore();
    });
  });
});
