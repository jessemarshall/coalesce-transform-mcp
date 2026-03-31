import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { InMemoryTaskStore, InMemoryTaskMessageQueue } from "../../src/tasks/store.js";
import { registerRetryAndWaitTask } from "../../src/tasks/retry-and-wait-task.js";
import { POSTMAN_RERUN_RESPONSE } from "../fixtures/postman-examples.js";
import { setupSnowflakeEnv } from "../fixtures/snowflake-env.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue(POSTMAN_RERUN_RESPONSE),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

function createTaskServer() {
  const taskStore = new InMemoryTaskStore();
  const taskMessageQueue = new InMemoryTaskMessageQueue();
  const server = new McpServer(
    { name: "test", version: "0.0.1" },
    {
      capabilities: { tasks: { requests: { tools: { call: {} } } } },
      taskStore,
      taskMessageQueue,
    }
  );
  return { server, taskStore };
}

describe("retry_and_wait task tool", () => {
  const originalEnv = process.env;
  let env: ReturnType<typeof setupSnowflakeEnv>;

  beforeEach(() => {
    env = setupSnowflakeEnv(originalEnv);
  });

  afterEach(() => {
    env.cleanup();
  });

  it("registers as a task tool without throwing", () => {
    const { server } = createTaskServer();
    const client = createMockClient();
    registerRetryAndWaitTask(server, client as any);
    expect(true).toBe(true);
  });
});
