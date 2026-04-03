import { describe, it, expect, vi, beforeEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { requireDestructiveConfirmation } from "../../src/services/shared/elicitation.js";

function createMockServer(supportsElicitation: boolean) {
  const server = {
    server: {
      getClientCapabilities: vi.fn().mockReturnValue(
        supportsElicitation ? { elicitation: { form: true } } : {}
      ),
      elicitInput: vi.fn(),
    },
  } as unknown as McpServer;
  return server;
}

describe("requireDestructiveConfirmation", () => {
  it("returns null when confirmed=true (already approved)", async () => {
    const server = createMockServer(false);
    const result = await requireDestructiveConfirmation(
      server,
      "delete_project",
      "This will delete the project.",
      true,
    );
    expect(result).toBeNull();
    expect(server.server.getClientCapabilities).not.toHaveBeenCalled();
  });

  it("returns STOP_AND_CONFIRM when client does not support elicitation", async () => {
    const server = createMockServer(false);
    const result = await requireDestructiveConfirmation(
      server,
      "delete_project",
      "This will delete the project.",
      undefined,
    );

    expect(result).not.toBeNull();
    const content = result!.structuredContent as Record<string, unknown>;
    expect(content.executed).toBe(false);
    expect(content.STOP_AND_CONFIRM).toContain("STOP");
    expect(content.STOP_AND_CONFIRM).toContain("delete_project");
    expect(content.STOP_AND_CONFIRM).toContain("confirmed=true");
  });

  it("includes extra fields in STOP_AND_CONFIRM response", async () => {
    const server = createMockServer(false);
    const result = await requireDestructiveConfirmation(
      server,
      "propagate_column_change",
      "This will update downstream nodes.",
      undefined,
      { nodeID: "n1", downstreamCount: 5 },
    );

    const content = result!.structuredContent as Record<string, unknown>;
    expect(content.nodeID).toBe("n1");
    expect(content.downstreamCount).toBe(5);
  });

  it("returns null when user accepts elicitation", async () => {
    const server = createMockServer(true);
    (server.server.elicitInput as ReturnType<typeof vi.fn>).mockResolvedValue({
      action: "accept",
      content: { confirmed: true },
    });

    const result = await requireDestructiveConfirmation(
      server,
      "delete_project",
      "This will delete the project.",
      undefined,
    );

    expect(result).toBeNull();
    expect(server.server.elicitInput).toHaveBeenCalledWith(
      expect.objectContaining({
        message: "This will delete the project.",
        requestedSchema: expect.objectContaining({
          type: "object",
          properties: expect.objectContaining({
            confirmed: expect.objectContaining({ type: "boolean" }),
          }),
        }),
      })
    );
  });

  it("returns cancelled response when user declines elicitation", async () => {
    const server = createMockServer(true);
    (server.server.elicitInput as ReturnType<typeof vi.fn>).mockResolvedValue({
      action: "accept",
      content: { confirmed: false },
    });

    const result = await requireDestructiveConfirmation(
      server,
      "delete_environment",
      "This will delete the environment.",
      undefined,
    );

    expect(result).not.toBeNull();
    const content = result!.structuredContent as Record<string, unknown>;
    expect(content.executed).toBe(false);
    expect(content.cancelled).toBe(true);
    expect(content.reason).toContain("declined");
  });

  it("returns cancelled response when user cancels elicitation dialog", async () => {
    const server = createMockServer(true);
    (server.server.elicitInput as ReturnType<typeof vi.fn>).mockResolvedValue({
      action: "cancel",
      content: {},
    });

    const result = await requireDestructiveConfirmation(
      server,
      "delete_workspace_node",
      "This will delete the node.",
      undefined,
    );

    expect(result).not.toBeNull();
    const content = result!.structuredContent as Record<string, unknown>;
    expect(content.executed).toBe(false);
    expect(content.cancelled).toBe(true);
    expect(content.reason).toContain("cancelled");
  });

  it("returns cancelled response when user declines elicitation dialog", async () => {
    const server = createMockServer(true);
    (server.server.elicitInput as ReturnType<typeof vi.fn>).mockResolvedValue({
      action: "decline",
      content: {},
    });

    const result = await requireDestructiveConfirmation(
      server,
      "delete_workspace_node",
      "This will delete the node.",
      undefined,
    );

    expect(result).not.toBeNull();
    const content = result!.structuredContent as Record<string, unknown>;
    expect(content.executed).toBe(false);
    expect(content.cancelled).toBe(true);
    expect(content.reason).toContain("declined");
  });

  it("does not call elicitInput when confirmed=true", async () => {
    const server = createMockServer(true);
    await requireDestructiveConfirmation(
      server,
      "delete_project",
      "This will delete the project.",
      true,
    );
    expect(server.server.elicitInput).not.toHaveBeenCalled();
  });
});
