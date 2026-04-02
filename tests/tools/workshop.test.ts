import { describe, it, expect, vi, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerWorkshopTools } from "../../src/mcp/workshop.js";
import {
  openWorkshop,
  workshopInstruct,
  getWorkshopStatus,
  workshopClose,
  deleteSession,
} from "../../src/services/pipelines/workshop.js";
import { createMockClient } from "../helpers/fixtures.js";

vi.mock("../../src/services/pipelines/workshop.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../src/services/pipelines/workshop.js")>();
  return {
    ...actual,
    openWorkshop: vi.fn(),
    workshopInstruct: vi.fn(),
    getWorkshopStatus: vi.fn(),
    workshopClose: vi.fn(),
  };
});

function makeServer() {
  return new McpServer({ name: "test", version: "0.0.1" });
}

function getToolHandler(
  server: McpServer,
  spy: ReturnType<typeof vi.spyOn>,
  toolName: string
) {
  const call = spy.mock.calls.find((c) => c[0] === toolName);
  return call?.[2] as ((params: Record<string, unknown>) => Promise<unknown>) | undefined;
}

describe("Workshop Tools", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("registers all 4 workshop tools without throwing", () => {
    const server = makeServer();
    const client = createMockClient();
    expect(() => registerWorkshopTools(server, client as any)).not.toThrow();
  });

  it("pipeline_workshop_open calls openWorkshop and returns its result", async () => {
    const server = makeServer();
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    const fakeSession = {
      sessionID: "abc123",
      workspaceID: "ws-1",
      createdAt: "2026-01-01T00:00:00Z",
      updatedAt: "2026-01-01T00:00:00Z",
      nodes: [],
      history: [],
      resolvedEntities: [],
      openQuestions: [],
      warnings: [],
    };
    vi.mocked(openWorkshop).mockResolvedValue(fakeSession as any);

    registerWorkshopTools(server, client as any);

    const handler = getToolHandler(server, spy, "pipeline_workshop_open");
    expect(handler).toBeDefined();

    const result = await handler!({ workspaceID: "ws-1" }) as any;
    expect(openWorkshop).toHaveBeenCalledWith(client, { workspaceID: "ws-1", intent: undefined });
    expect(result.content[0].text).toContain("abc123");
  });

  it("pipeline_workshop_open rejects invalid workspaceID", async () => {
    const server = makeServer();
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    registerWorkshopTools(server, client as any);

    const handler = getToolHandler(server, spy, "pipeline_workshop_open");
    const result = await handler!({ workspaceID: "../escape" }) as any;
    expect(result.isError).toBe(true);
    expect(openWorkshop).not.toHaveBeenCalled();
  });

  it("pipeline_workshop_instruct calls workshopInstruct and returns its result", async () => {
    const server = makeServer();
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    const fakeResult = {
      sessionID: "abc123",
      action: "added_nodes",
      changes: ["Added stage node STG_CUSTOMERS"],
      currentPlan: [],
      openQuestions: [],
      warnings: [],
    };
    vi.mocked(workshopInstruct).mockResolvedValue(fakeResult as any);

    registerWorkshopTools(server, client as any);

    const handler = getToolHandler(server, spy, "pipeline_workshop_instruct");
    expect(handler).toBeDefined();

    const result = await handler!({ sessionID: "abc123", instruction: "stage the customers table" }) as any;
    expect(workshopInstruct).toHaveBeenCalledWith(client, {
      sessionID: "abc123",
      instruction: "stage the customers table",
    });
    expect(result.content[0].text).toContain("added_nodes");
  });

  it("pipeline_workshop_status returns session state when session exists", async () => {
    const server = makeServer();
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    const fakeSession = {
      sessionID: "abc123",
      workspaceID: "ws-1",
      createdAt: "2026-01-01T00:00:00Z",
      updatedAt: "2026-01-01T00:00:00Z",
      nodes: [],
      history: [],
      resolvedEntities: [],
    };
    vi.mocked(getWorkshopStatus).mockReturnValue(fakeSession as any);

    registerWorkshopTools(server, client as any);

    const handler = getToolHandler(server, spy, "pipeline_workshop_status");
    expect(handler).toBeDefined();

    const result = await handler!({ sessionID: "abc123" }) as any;
    expect(result.content[0].text).toContain("abc123");
    expect(result.content[0].text).not.toContain('"error"');
  });

  it("pipeline_workshop_status returns error object when session not found", async () => {
    const server = makeServer();
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    vi.mocked(getWorkshopStatus).mockReturnValue(null);

    registerWorkshopTools(server, client as any);

    const handler = getToolHandler(server, spy, "pipeline_workshop_status");
    const result = await handler!({ sessionID: "missing" }) as any;
    expect(result.content[0].text).toContain("error");
    expect(result.content[0].text).toContain("missing");
  });

  it("pipeline_workshop_close calls workshopClose and returns its result", async () => {
    const server = makeServer();
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    vi.mocked(workshopClose).mockReturnValue({ closed: true, message: "Session closed." });

    registerWorkshopTools(server, client as any);

    const handler = getToolHandler(server, spy, "pipeline_workshop_close");
    expect(handler).toBeDefined();

    const result = await handler!({ sessionID: "abc123" }) as any;
    expect(workshopClose).toHaveBeenCalledWith("abc123");
    expect(result.content[0].text).toContain("closed");
  });

  it("pipeline_workshop_open passes intent when provided", async () => {
    const server = makeServer();
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    vi.mocked(openWorkshop).mockResolvedValue({
      sessionID: "def456",
      workspaceID: "ws-1",
      createdAt: "2026-01-01T00:00:00Z",
      updatedAt: "2026-01-01T00:00:00Z",
      nodes: [],
      history: [],
      resolvedEntities: [],
      openQuestions: [],
      warnings: [],
    } as any);

    registerWorkshopTools(server, client as any);

    const handler = getToolHandler(server, spy, "pipeline_workshop_open");
    await handler!({ workspaceID: "ws-1", intent: "join customers and orders" });

    expect(openWorkshop).toHaveBeenCalledWith(client, {
      workspaceID: "ws-1",
      intent: "join customers and orders",
    });
  });

  it("pipeline_workshop_instruct surfaces errors via isError response", async () => {
    const server = makeServer();
    const spy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    vi.mocked(workshopInstruct).mockRejectedValue(new Error("Session not found"));

    registerWorkshopTools(server, client as any);

    const handler = getToolHandler(server, spy, "pipeline_workshop_instruct");
    const result = await handler!({ sessionID: "bad", instruction: "do something" }) as any;
    expect(result.isError).toBe(true);
    expect(result.content[0].text).toContain("Session not found");
  });
});
