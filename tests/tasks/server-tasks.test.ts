import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { createCoalesceMcpServer } from "../../src/server.js";
import { setupSnowflakeEnv } from "../fixtures/snowflake-env.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue({ runCounter: 0 }),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("server tasks capability", () => {
  const originalEnv = process.env;
  let env: ReturnType<typeof setupSnowflakeEnv>;

  beforeEach(() => {
    env = setupSnowflakeEnv(originalEnv);
  });

  afterEach(() => {
    env.cleanup();
  });

  it("creates server with tasks capability declared", () => {
    const client = createMockClient();
    const server = createCoalesceMcpServer(client as any);
    expect(server).toBeDefined();
  });

  it("registers task-based run_and_wait, retry_and_wait, and start_run tools", () => {
    const client = createMockClient();
    const server = createCoalesceMcpServer(client as any);

    const registeredTools = (server as any)._registeredTools;
    const toolNames = Object.keys(registeredTools);
    expect(toolNames).toContain("run_and_wait");
    expect(toolNames).toContain("retry_and_wait");
    expect(toolNames).toContain("start_run");
  });

  it("still registers non-task run tools (list_runs, get_run, etc.)", () => {
    const client = createMockClient();
    const server = createCoalesceMcpServer(client as any);

    const registeredTools = (server as any)._registeredTools;
    const toolNames = Object.keys(registeredTools);
    expect(toolNames).toContain("list_runs");
    expect(toolNames).toContain("get_run");
    expect(toolNames).toContain("run_status");
    expect(toolNames).toContain("retry_run");
    expect(toolNames).toContain("cancel_run");
  });
});
