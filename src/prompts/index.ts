import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

export function registerPrompts(server: McpServer): void {
  server.registerPrompt(
    "coalesce-start-here",
    {
      title: "Coalesce Start Here",
      description:
        "Discover projects, workspaces, environments, jobs, and node IDs before calling mutating tools.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "Start with discovery before mutation. Use list-projects(includeWorkspaces=true) or get-project(includeWorkspaces=true) to resolve workspace IDs, list-environments for environment IDs, list-jobs for job IDs, and list-workspace-nodes or get-workspace-node before editing node bodies. Read coalesce://context/id-discovery and coalesce://context/tool-usage for the detailed lookup patterns.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "safe-pipeline-planning",
    {
      title: "Safe Pipeline Planning",
      description:
        "Planner-first pipeline workflow, including review and approval before any workspace mutation.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "Always call plan-pipeline before create-pipeline-from-plan or create-pipeline-from-sql. If the planner returns status needs_clarification, stop and address openQuestions and warnings first. If it returns status ready, present the planned nodes, exact nodeType values, transforms, and filters to the user and wait for explicit approval before creating anything. Review coalesce://context/pipeline-workflows and coalesce://context/tool-usage for the mandatory planner-first sequence.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "run-operations-guide",
    {
      title: "Run Operations Guide",
      description:
        "Choose the right run helper and interpret run statuses, results, warnings, and timeouts correctly.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "Use run-and-wait when the user wants a final outcome in one call, retry-and-wait for immediate reruns of failed runs, run-status for live scheduler polling, and get-run-details when you need metadata plus results together. Treat waitingToRun and running as non-terminal, and completed, failed, and canceled as terminal. Inspect validation, warning, resultsError, incomplete, and timedOut fields before reporting success. See coalesce://context/run-operations for the full lifecycle.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "large-result-handling",
    {
      title: "Large Result Handling",
      description:
        "Use cache tools and coalesce://cache resource URIs when payloads are too large to return inline.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "Large JSON responses may be returned as cache metadata with a coalesce://cache/... resource URI instead of full inline payloads. Read the referenced resource rather than assuming the JSON is embedded in the tool result. When you know a large snapshot is needed, prefer explicit cache tools like cache-workspace-nodes, cache-environment-nodes, cache-runs, or cache-org-users so the artifact can be reused. See coalesce://context/tool-usage for paging and cache-handling guidance.",
          },
        },
      ],
    })
  );
}
