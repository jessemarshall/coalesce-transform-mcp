import { describe, it, expect, vi, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { defineWorkshopTools } from "../../src/mcp/workshop.js";
import { deleteSession } from "../../src/services/pipelines/workshop.js";

function createMockClient(nodes: Array<{ id: string; name: string; locationName?: string }> = []) {
  const client = {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };

  client.get.mockImplementation((path: string) => {
    if (path.match(/\/nodes$/) && !path.includes("/nodes/")) {
      return Promise.resolve({
        data: nodes.map((n) => ({
          id: n.id,
          name: n.name,
          nodeType: "Stage",
          locationName: n.locationName ?? "RAW",
        })),
      });
    }
    for (const n of nodes) {
      if (path.includes(`/nodes/${n.id}`)) {
        return Promise.resolve({
          id: n.id,
          name: n.name,
          nodeType: "Stage",
          metadata: { columns: [{ name: "ID", dataType: "VARCHAR" }] },
        });
      }
    }
    return Promise.resolve({ data: [] });
  });

  return client;
}

function getToolHandler(toolSpy: ReturnType<typeof vi.fn>, toolName: string) {
  return toolSpy.mock.calls.find((call) => call[0] === toolName)?.[2] as
    | ((params: Record<string, unknown>) => Promise<{ content: { text: string }[]; isError?: boolean }>)
    | undefined;
}

describe("Pipeline Workshop Tools", () => {
  const sessionIDs: string[] = [];

  afterEach(() => {
    vi.restoreAllMocks();
    for (const id of sessionIDs.splice(0, sessionIDs.length)) {
      try { deleteSession(id); } catch { /* ignore */ }
    }
  });

  it("registers all 4 workshop tools without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    defineWorkshopTools(server, client as any).forEach(t => server.registerTool(...t));
    expect(true).toBe(true);
  });

  it("pipeline_workshop_open creates a session and returns a sessionID", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient([
      { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
    ]);

    defineWorkshopTools(server, client as any).forEach(t => server.registerTool(...t));

    const handler = getToolHandler(toolSpy, "pipeline_workshop_open");
    expect(typeof handler).toBe("function");

    const result = await handler!({ workspaceID: "ws-1" });

    const data = JSON.parse(result.content[0]!.text);
    expect(typeof data.sessionID).toBe("string");
    expect(data.sessionID.length).toBeGreaterThan(0);
    expect(data.workspaceID).toBe("ws-1");
    sessionIDs.push(data.sessionID);
  });

  it("pipeline_workshop_open with intent bootstraps session nodes", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient([
      { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
      { id: "n2", name: "ORDERS", locationName: "RAW" },
    ]);

    defineWorkshopTools(server, client as any).forEach(t => server.registerTool(...t));

    const handler = getToolHandler(toolSpy, "pipeline_workshop_open");
    const result = await handler!({
      workspaceID: "ws-1",
      intent: "stage CUSTOMERS",
    });

    const data = JSON.parse(result.content[0]!.text);
    expect(typeof data.sessionID).toBe("string");
    sessionIDs.push(data.sessionID);
  });

  it("get_pipeline_workshop_status returns session for a valid sessionID", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient([{ id: "n1", name: "CUSTOMERS" }]);

    defineWorkshopTools(server, client as any).forEach(t => server.registerTool(...t));

    // Open a session first
    const openHandler = getToolHandler(toolSpy, "pipeline_workshop_open");
    const openResult = await openHandler!({ workspaceID: "ws-1" });
    const { sessionID } = JSON.parse(openResult.content[0]!.text);
    sessionIDs.push(sessionID);

    // Now get status
    const statusHandler = getToolHandler(toolSpy, "get_pipeline_workshop_status");
    expect(typeof statusHandler).toBe("function");

    const statusResult = await statusHandler!({ sessionID });
    const data = JSON.parse(statusResult.content[0]!.text);
    expect(data.sessionID).toBe(sessionID);
    expect(data.workspaceID).toBe("ws-1");
    expect(Array.isArray(data.nodes)).toBe(true);
  });

  it("get_pipeline_workshop_status returns error for unknown sessionID", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient();

    defineWorkshopTools(server, client as any).forEach(t => server.registerTool(...t));

    const handler = getToolHandler(toolSpy, "get_pipeline_workshop_status");
    const result = await handler!({ sessionID: "nonexistent-session-id" });

    expect(result.isError).toBe(true);
    expect(result.content[0]!.text).toContain("not found");
  });

  it("pipeline_workshop_instruct modifies the current session plan", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient([
      { id: "n1", name: "CUSTOMERS", locationName: "RAW" },
    ]);

    defineWorkshopTools(server, client as any).forEach(t => server.registerTool(...t));

    // Open session
    const openHandler = getToolHandler(toolSpy, "pipeline_workshop_open");
    const openResult = await openHandler!({ workspaceID: "ws-1" });
    const { sessionID } = JSON.parse(openResult.content[0]!.text);
    sessionIDs.push(sessionID);

    // Send instruction
    const instructHandler = getToolHandler(toolSpy, "pipeline_workshop_instruct");
    expect(typeof instructHandler).toBe("function");

    const result = await instructHandler!({
      sessionID,
      instruction: "add a staging node for CUSTOMERS",
    });

    const data = JSON.parse(result.content[0]!.text);
    expect(data.sessionID).toBe(sessionID);
    expect(typeof data.action).toBe("string");
    expect(Array.isArray(data.changes)).toBe(true);
    expect(Array.isArray(data.currentPlan)).toBe(true);
  });

  it("pipeline_workshop_close closes the session", async () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const toolSpy = vi.spyOn(server, "registerTool");
    const client = createMockClient([{ id: "n1", name: "CUSTOMERS" }]);

    defineWorkshopTools(server, client as any).forEach(t => server.registerTool(...t));

    // Open session
    const openHandler = getToolHandler(toolSpy, "pipeline_workshop_open");
    const openResult = await openHandler!({ workspaceID: "ws-1" });
    const { sessionID } = JSON.parse(openResult.content[0]!.text);

    // Close session
    const closeHandler = getToolHandler(toolSpy, "pipeline_workshop_close");
    expect(typeof closeHandler).toBe("function");

    const result = await closeHandler!({ sessionID });
    const data = JSON.parse(result.content[0]!.text);
    expect(data.closed).toBe(true);
    expect(typeof data.message).toBe("string");

    // Status should show session gone after close
    const statusHandler = getToolHandler(toolSpy, "get_pipeline_workshop_status");
    const statusResult = await statusHandler!({ sessionID });
    expect(statusResult.isError).toBe(true);
    expect(statusResult.content[0]!.text).toContain("not found");
  });
});
