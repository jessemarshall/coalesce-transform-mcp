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
              "Start with discovery before mutation. Use coalesce_list_workspaces to resolve workspace IDs, coalesce_list_environments for environment IDs, coalesce_list_environment_jobs for job IDs, and coalesce_list_workspace_nodes or coalesce_get_workspace_node before editing node bodies. Read coalesce://context/id-discovery and coalesce://context/tool-usage for the detailed lookup patterns.",
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
              "Always call coalesce_plan_pipeline before coalesce_create_pipeline_from_plan or coalesce_create_pipeline_from_sql. If the planner returns status needs_clarification, stop and address openQuestions and warnings first. If it returns status ready, present the planned nodes, exact nodeType values, transforms, and filters to the user and wait for explicit approval before creating anything. Review coalesce://context/pipeline-workflows and coalesce://context/tool-usage for the mandatory planner-first sequence.",
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
              "Use coalesce_run_and_wait when the user wants a final outcome in one call, coalesce_retry_and_wait for immediate reruns of failed runs, coalesce_run_status for live scheduler polling, and coalesce_get_run_details when you need metadata plus results together. Treat waitingToRun and running as non-terminal, and completed, failed, and canceled as terminal. Inspect validation, warning, resultsError, incomplete, and timedOut fields before reporting success. See coalesce://context/run-operations for the full lifecycle.",
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
              "Large JSON responses may be returned as cache metadata with a coalesce://cache/... resource URI instead of full inline payloads. Read the referenced resource rather than assuming the JSON is embedded in the tool result. When you know a large snapshot is needed, prefer explicit cache tools like coalesce_cache_workspace_nodes, coalesce_cache_environment_nodes, coalesce_cache_runs, or coalesce_cache_org_users so the artifact can be reused. See coalesce://context/tool-usage for paging and cache-handling guidance.",
          },
        },
      ],
    })
  );
}
