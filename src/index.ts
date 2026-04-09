#!/usr/bin/env node
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { validateConfig, createClient } from "./client.js";
import { createCoalesceMcpServer } from "./server.js";

const config = validateConfig();
const client = createClient(config);
const server = createCoalesceMcpServer(client);

// Start server
const transport = new StdioServerTransport();
await server.connect(transport);
