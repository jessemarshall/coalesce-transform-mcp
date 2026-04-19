import { describe, it, expect, vi, beforeEach } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { InMemoryTaskStore, InMemoryTaskMessageQueue } from "../src/tasks/store.js";
import { registerServerSurface, SERVER_INSTRUCTIONS } from "../src/server.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  };
}

/**
 * Task-based tools are registered via `registerToolTask`, which the
 * `registerTool` spy does not catch — we assert them separately from the
 * registered-tool snapshot below.
 */
const TASK_TOOL_NAMES = ["start_run", "run_and_wait", "retry_and_wait"];

/**
 * Tools whose prior presence we want to guard against regressions for — they
 * were removed deliberately and should not silently reappear. Kept as an
 * explicit negative list so the snapshot doesn't have to justify absences.
 */
const FORBIDDEN_TOOL_NAMES = [
  // Cloud-facing coa_list_* were removed in favor of first-class REST tools.
  "coa_list_environments",
  "coa_list_environment_nodes",
  "coa_list_runs",
  // Cortex tools removed — users go via the cortex CLI directly.
  "explore_data_source",
  "query_snowflake",
  "search_snowflake_objects",
  "list_snowflake_connections",
];

describe("Tool Registration", () => {
  let server: McpServer;
  let toolSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    server = new McpServer(
      { name: "test", version: "0.0.1" },
      {
        capabilities: { tasks: { requests: { tools: { call: {} } } } },
        taskStore: new InMemoryTaskStore(),
        taskMessageQueue: new InMemoryTaskMessageQueue(),
      }
    );
    toolSpy = vi.spyOn(server, "registerTool");
  });

  it("registers the expected set of tools (snapshot)", async () => {
    const client = createMockClient();
    registerServerSurface(server, client as any);

    const toolNames = toolSpy.mock.calls
      .map((call: unknown[]) => call[0] as string)
      .sort();

    // Snapshot of the full registered-tool set. Adding or removing a tool will
    // fail this test with a clear diff — update via `vitest -u` once the
    // addition/removal is intentional. Prefers a snapshot over a hardcoded
    // count so we catch silent tool-set drift, not just total count changes.
    expect(toolNames).toMatchSnapshot("registered tool names");

    // Task-based tools live outside the registerTool spy path.
    const registeredTools = (server as any)._registeredTools;
    const allToolNames = Object.keys(registeredTools);
    for (const name of TASK_TOOL_NAMES) {
      expect(allToolNames).toContain(name);
    }

    // Regression guards: tools that were removed on purpose should stay gone.
    for (const name of FORBIDDEN_TOOL_NAMES) {
      expect(toolNames).not.toContain(name);
    }

    // Narrow annotation check — `clear_data_cache` is the canonical example of
    // a destructive, non-idempotent local tool. If this shape drifts, the tool
    // helper wiring has changed in a way the snapshot won't catch.
    const clearCacheCall = toolSpy.mock.calls.find(
      (call: unknown[]) => call[0] === "clear_data_cache"
    );
    expect(clearCacheCall).toBeDefined();
    expect(clearCacheCall?.[1]).toMatchObject({
      annotations: {
        readOnlyHint: false,
        idempotentHint: false,
        destructiveHint: true,
      },
    });
  });
});

describe("Server Instructions", () => {
  it("contains tool category sections for client-side navigation", () => {
    const categories = [
      "Discovery",
      "Pipeline building",
      "Node editing",
      "Execution",
      "Node type discovery",
      "Lineage",
      "Caching",
      "Users and admin",
      "Customization",
    ];
    for (const category of categories) {
      expect(SERVER_INSTRUCTIONS).toContain(category);
    }
  });

  it("contains typical workflow sequences", () => {
    expect(SERVER_INSTRUCTIONS).toContain("TYPICAL WORKFLOWS:");
    expect(SERVER_INSTRUCTIONS).toContain("Explore a workspace:");
    expect(SERVER_INSTRUCTIONS).toContain("Build a pipeline:");
    expect(SERVER_INSTRUCTIONS).toContain("Run a job:");
    expect(SERVER_INSTRUCTIONS).toContain("Diagnose a failure:");
    expect(SERVER_INSTRUCTIONS).toContain("Audit a workspace:");
    expect(SERVER_INSTRUCTIONS).toContain("Prepare for deployment:");
  });

  it("contains key operational rules", () => {
    expect(SERVER_INSTRUCTIONS).toContain("plan_pipeline");
    expect(SERVER_INSTRUCTIONS).toContain("Resolve IDs before mutating");
    expect(SERVER_INSTRUCTIONS).toContain("warning, validation, resultsError");
    expect(SERVER_INSTRUCTIONS).toContain("coalesce://context/*");
  });

  it("references representative tools from each category", () => {
    const tools = [
      "list_workspaces",
      "list_workspace_nodes",
      "plan_pipeline",
      "create_pipeline_from_plan",
      "update_workspace_node",
      "run_and_wait",
      "get_upstream_nodes",
      "cache_workspace_nodes",
      "diagnose_run_failure",
      "personalize_skills",
    ];
    for (const tool of tools) {
      expect(SERVER_INSTRUCTIONS).toContain(tool);
    }
  });
});
