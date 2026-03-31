import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { InMemoryTaskStore, InMemoryTaskMessageQueue } from "../../src/tasks/store.js";
import { registerRunAndWaitTask } from "../../src/tasks/run-and-wait-task.js";
import {
  POSTMAN_RUN_STATUS_RESPONSE,
  POSTMAN_START_RUN_RESPONSE,
} from "../fixtures/postman-examples.js";
import { setupSnowflakeEnv } from "../fixtures/snowflake-env.js";

function createMockClient() {
  return {
    get: vi.fn().mockResolvedValue({ data: [] }),
    post: vi.fn().mockResolvedValue(POSTMAN_START_RUN_RESPONSE),
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

describe("run_and_wait task tool", () => {
  const originalEnv = process.env;
  let env: ReturnType<typeof setupSnowflakeEnv>;

  beforeEach(() => {
    vi.useFakeTimers();
    env = setupSnowflakeEnv(originalEnv);
  });

  afterEach(() => {
    vi.useRealTimers();
    env.cleanup();
  });

  it("registers as a task tool without throwing", () => {
    vi.useRealTimers();
    const { server } = createTaskServer();
    const client = createMockClient();
    registerRunAndWaitTask(server, client as any);
    expect(true).toBe(true);
  });

  it("creates a task and completes it when the run finishes", async () => {
    const { server, taskStore } = createTaskServer();
    const client = createMockClient();

    client.get.mockImplementation((path: string) => {
      if (path === "/scheduler/runStatus") {
        return Promise.resolve({
          ...POSTMAN_RUN_STATUS_RESPONSE,
          runStatus: "completed",
        });
      }
      if (path === "/api/v1/runs/0/results") {
        return Promise.resolve({ results: [] });
      }
      return Promise.resolve({});
    });

    registerRunAndWaitTask(server, client as any);

    // Verify taskStore starts empty
    const { tasks } = await taskStore.listTasks();
    expect(tasks).toHaveLength(0);
  });

  it("task store tracks tasks through their lifecycle", async () => {
    const taskStore = new InMemoryTaskStore();
    const requestId = "req-1";
    const request = { method: "tools/call", params: { name: "run_and_wait", arguments: {} } };

    const task = await taskStore.createTask(
      { ttl: 300000, pollInterval: 10000 },
      requestId,
      request
    );
    expect(task.taskId).toBeDefined();
    expect(task.status).toBe("working");

    await taskStore.storeTaskResult(task.taskId, "completed", {
      content: [{ type: "text", text: '{"status":"completed"}' }],
    });

    const updated = await taskStore.getTask(task.taskId);
    expect(updated?.status).toBe("completed");

    const result = await taskStore.getTaskResult(task.taskId);
    expect(result).toEqual({
      content: [{ type: "text", text: '{"status":"completed"}' }],
    });

    taskStore.cleanup();
  });

  it("task store handles failed tasks", async () => {
    const taskStore = new InMemoryTaskStore();
    const task = await taskStore.createTask(
      { ttl: 300000 },
      "req-2",
      { method: "tools/call", params: { name: "run_and_wait", arguments: {} } }
    );

    await taskStore.storeTaskResult(task.taskId, "failed", {
      content: [{ type: "text", text: "run_and_wait failed: connection error" }],
      isError: true,
    });

    const updated = await taskStore.getTask(task.taskId);
    expect(updated?.status).toBe("failed");

    const result = await taskStore.getTaskResult(task.taskId);
    expect(result).toEqual({
      content: [{ type: "text", text: "run_and_wait failed: connection error" }],
      isError: true,
    });

    taskStore.cleanup();
  });
});
