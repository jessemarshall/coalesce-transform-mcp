import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { registerRunAndWaitTask } from "./run-and-wait-task.js";
import { registerRetryAndWaitTask } from "./retry-and-wait-task.js";
import { registerStartRunTask } from "./start-run-task.js";

export { InMemoryTaskStore, InMemoryTaskMessageQueue } from "./store.js";

export function registerTaskTools(
  server: McpServer,
  client: CoalesceClient
): void {
  registerRunAndWaitTask(server, client);
  registerRetryAndWaitTask(server, client);
  registerStartRunTask(server, client);
}
