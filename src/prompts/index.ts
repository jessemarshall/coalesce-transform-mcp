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
              "Start with discovery before mutation. Use list_workspaces to resolve workspace IDs, list_environments for environment IDs, list_environment_jobs for job IDs, and list_workspace_nodes or get_workspace_node before editing node bodies. Read coalesce://context/id-discovery and coalesce://context/tool-usage for the detailed lookup patterns.",
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
              "Always call plan_pipeline before create_pipeline_from_plan or create_pipeline_from_sql. If the planner returns status needs_clarification, stop and address openQuestions and warnings first. If it returns status ready, present the planned nodes, exact nodeType values, transforms, and filters to the user and wait for explicit approval before creating anything. Review coalesce://context/pipeline-workflows and coalesce://context/tool-usage for the mandatory planner-first sequence.",
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
              "Use run_and_wait when the user wants a final outcome in one call, retry_and_wait for immediate reruns of failed runs, run_status for live scheduler polling, and get_run_details when you need metadata plus results together. Treat waitingToRun and running as non-terminal, and completed, failed, and canceled as terminal. Inspect validation, warning, resultsError, incomplete, and timedOut fields before reporting success. See coalesce://context/run-operations for the full lifecycle.",
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
              "Large JSON responses may be returned as cache metadata with a coalesce://cache/... resource URI instead of full inline payloads. Read the referenced resource rather than assuming the JSON is embedded in the tool result. When you know a large snapshot is needed, prefer explicit cache tools like cache_workspace_nodes, cache_environment_nodes, cache_runs, or cache_org_users so the artifact can be reused. See coalesce://context/tool-usage for paging and cache-handling guidance.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "diagnose-failing-node",
    {
      title: "Diagnose Failing Node",
      description:
        "Step-by-step workflow for diagnosing why a node failed in a run.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "To diagnose a failing node, follow this sequence:\n" +
              "1. Identify the environment and recent runs: list_environments → list_environment_jobs → get_run_details (for the most recent failed run)\n" +
              "2. Run diagnose_run_failure with the failed run ID to get structured root-cause analysis\n" +
              "3. Check the node definition: get_workspace_node to see the SQL, config, and column setup\n" +
              "4. Check upstream dependencies: get_upstream_nodes to see if a parent node failed first (cascade failures are common)\n" +
              "5. If the error is SQL-related, review the node SQL against the platform SQL rules (read coalesce://context/sql-snowflake, sql-databricks, or sql-bigquery as appropriate)\n" +
              "6. If the error is config-related, use complete_node_configuration to check for missing required fields\n" +
              "Present your diagnosis with: root cause, affected nodes, and specific fix recommendations.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "prepare-for-deployment",
    {
      title: "Prepare for Deployment",
      description:
        "Guided workflow to assess workspace readiness before deploying to an environment.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "To prepare a workspace for deployment, follow this sequence:\n" +
              "1. Discover the workspace: list_workspaces → get_workspace to confirm the target\n" +
              "2. Profile the workspace: analyze_workspace_patterns for a compact overview of node counts, types, subgraphs, and methodology\n" +
              "3. Review pipeline quality: review_pipeline to check for redundant nodes, missing joins, layer violations, naming issues, and optimization opportunities\n" +
              "4. Check documentation coverage: audit_documentation_coverage to identify undocumented nodes and columns\n" +
              "5. Verify lineage integrity: get_upstream_nodes and get_downstream_nodes on leaf nodes to confirm the dependency graph is complete\n" +
              "6. Identify the target environment: list_environments to find the deployment target\n" +
              "7. Check environment health: get_environment_health for the target environment to see current node run status\n" +
              "Present a deployment readiness report with: workspace summary, quality findings, documentation gaps, and any blockers.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "audit-workspace",
    {
      title: "Audit Workspace",
      description:
        "Comprehensive workspace audit covering structure, quality, documentation, and lineage.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "To audit a workspace comprehensively, follow this sequence:\n" +
              "1. Discover the workspace: list_workspaces → list_workspace_nodes (cache with cache_workspace_nodes for large workspaces)\n" +
              "2. Profile structure: analyze_workspace_patterns for node counts, types, subgraphs, and methodology detection\n" +
              "3. Review pipeline quality: review_pipeline to check structural issues, naming, and optimization opportunities\n" +
              "4. Audit documentation: audit_documentation_coverage for documentation coverage statistics and specific gaps\n" +
              "5. Search for potential issues: search_workspace_content to look for patterns like hardcoded values, TODO comments, or deprecated references\n" +
              "6. Check lineage: use get_upstream_nodes on leaf nodes to verify the full dependency chain is intact\n" +
              "Present a structured audit report with sections for: structure overview, quality findings (by severity), documentation coverage, and prioritized recommendations.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "column-change-workflow",
    {
      title: "Column Change Workflow",
      description:
        "Safely rename or retype a column across the pipeline with impact analysis and propagation.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "To safely rename or retype a column across the pipeline, follow this sequence:\n" +
              "1. Identify the source node and column: get_workspace_node to see the column's current name and data type\n" +
              "2. Analyze impact first: analyze_impact with the nodeID and columnID to see every downstream node and column that will be affected\n" +
              "3. Review the impact: present the list of affected nodes to the user and get explicit confirmation before proceeding\n" +
              "4. Propagate the change: propagate_column_change with confirmed: true to update all downstream column references. This is a destructive operation — it modifies multiple nodes via the API. The tool requires confirmed: true to proceed.\n" +
              "5. Verify the result: check the propagation result for errors, partial failures, and skipped nodes\n" +
              "6. If partial failure occurred: review the snapshotPath in the result for the pre-mutation state of affected nodes. Manually correct any inconsistencies using update_workspace_node\n" +
              "Always analyze_impact before propagate_column_change. Never skip the impact analysis step.",
          },
        },
      ],
    })
  );
}
