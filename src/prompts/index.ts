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
    "cross-server-workflow",
    {
      title: "Cross-Server Workflow",
      description:
        "Patterns for combining this MCP with Snowflake, Fivetran, dbt, or Catalog MCPs for end-to-end data workflows.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "This MCP manages Coalesce transform definitions — not warehouse data, ingestion, or catalog metadata. " +
              "For cross-server workflows:\n" +
              "- Pre-run validation: Fivetran MCP (check sync status) → this MCP (run_and_wait) → Snowflake MCP (validate output)\n" +
              "- Impact analysis: this MCP (analyze_impact) → Catalog MCP (check downstream consumers beyond Coalesce)\n" +
              "- Debugging: Snowflake MCP (find bad data) → this MCP (trace upstream lineage) → Fivetran MCP (check source sync)\n" +
              "Lineage tools here cover Coalesce nodes only. For end-to-end lineage across ingestion, transform, and consumption, combine with the Catalog MCP.\n" +
              "Read coalesce://context/ecosystem-boundaries for full details on scope boundaries and handoff patterns.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "coalesce-setup",
    {
      title: "Coalesce Setup",
      description:
        "Guided first-time setup. Credentials can come from a `COALESCE_ACCESS_TOKEN` env var OR from `~/.coa/config` — whichever the user prefers. Use this the first time a user connects the MCP to a new machine.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "You are guiding the user through first-time Coalesce MCP setup. The server accepts credentials from two sources:\n" +
              "  (a) `~/.coa/config` — the same INI file the `coa` CLI uses. Good for users who already run `coa` locally; avoids duplicating credentials.\n" +
              "  (b) MCP-client env vars (`COALESCE_ACCESS_TOKEN`, `SNOWFLAKE_*`). Good for first-time users or CI, no file required.\n" +
              "Env wins when both are set. The goal is to get `diagnose_setup` reporting `ready: true`.\n\n" +
              "Do not assume any state — call diagnose_setup first and base every subsequent step on its output. After the user completes an action, call diagnose_setup again before moving on.\n\n" +
              "SEQUENCE:\n\n" +
              "0. Baseline. Call diagnose_setup. If `ready` is true, tell the user they're already set up and stop. Otherwise present the current state compactly: the `coaConfig` block (file status + active profile + presentKeys), then accessToken (with its `source`) and snowflakeCreds (with its per-field `sources` map). Those source tags tell the user exactly where each value came from — env, profile:default, etc.\n\n" +
              "1. Access token. If accessToken.status is 'missing' or 'invalid':\n" +
              "   - 'missing' AND coaConfig.status === 'missing-file': offer the user the two paths. Env-var path: generate a token from Deploy → User Settings, then add `\"env\": { \"COALESCE_ACCESS_TOKEN\": \"<token>\" }` to their MCP client config. Profile path: create `~/.coa/config` with the INI shape shown in the README (or run `npx @coalescesoftware/coa describe config` for the canonical reference). Either works; env wins if both are set.\n" +
              "   - 'missing' but coaConfig.status === 'ok' with profileExists === true: the profile is loaded but has no `token=` field. Tell the user to add one, or set COALESCE_ACCESS_TOKEN in their MCP client env.\n" +
              "   - 'missing' but coaConfig.status === 'ok' with profileExists === false: COALESCE_PROFILE points at a profile that isn't in the file. Show `availableProfiles` from the diagnose output; have the user either switch COALESCE_PROFILE or add the missing section to ~/.coa/config.\n" +
              "   - 'invalid' (401/403): the token was rejected. Show the `source` from diagnose_setup so the user knows whether to fix the env var or the profile's `token` field. Generate a fresh token from Deploy → User Settings.\n" +
              "   - IMPORTANT: env-wins precedence. If `accessToken.source === 'env'` but the user expected their profile to supply it, there's a stale env var shadowing it. Point at the `source` field.\n" +
              "   - Wait for diagnose_setup to return accessToken.status === 'ok' before moving on.\n\n" +
              "2. Snowflake credentials (only if the user needs run tools or `coa_create`/`coa_run`). If snowflakeCreds.status is 'missing' or 'invalid':\n" +
              "   - For run tools the user needs SNOWFLAKE_USERNAME, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE, plus either SNOWFLAKE_KEY_PAIR_KEY (path to PEM) or SNOWFLAKE_PAT. These come from env OR the matching `snowflake*` keys in ~/.coa/config.\n" +
              "   - PATs are env-only. The MCP doesn't read the profile's `snowflakePassword` field (that's COA's Basic-auth mechanism, different from our PAT flow).\n" +
              "   - If the user doesn't use run tools today, they can skip this step — read-only Cloud REST tools work fine without Snowflake creds.\n\n" +
              "3. Repo path (optional). Only needed for repo-backed node-type lookup and local coa_* tools (coa_doctor, coa_validate, coa_create, coa_run, coa_plan).\n" +
              "   - Skip this if repoPath.status is already 'ok' or the user doesn't need those tools yet.\n" +
              "   - Otherwise: ask for the project's git URL (list_git_accounts may help). Tell them to `git clone <url> <target>`, then either add `\"COALESCE_REPO_PATH\": \"<absolute-path>\"` to their MCP client env block OR add `repoPath=<absolute-path>` to their profile in ~/.coa/config, restart the MCP client, and re-run diagnose_setup.\n" +
              "   - Target state: repoPath.isCoaProject === true and coaDoctor.status === 'ok' (or 'skipped' if the project doesn't yet have workspaces.yml).\n\n" +
              "RULES:\n" +
              "- Never skip diagnose_setup between steps. Actual state drifts from assumed state easily, especially across MCP-client restarts.\n" +
              "- Never write to the user's shell profile or ~/.coa/config yourself. Give them the exact text to paste; they apply it.\n" +
              "- Never ask the user for their token, PAT, or passphrase in chat. Secrets belong in ~/.coa/config or the MCP client env block — never in the conversation transcript.\n" +
              "- Each restart of the MCP client ends this conversation's access to the new values. The user may need to /coalesce-setup again after each restart; diagnose_setup will confirm which step they're resuming.\n" +
              "- COA cloud commands (coa_list_environments, coa_deploy, coa_refresh) read the same ~/.coa/config. If the user has a populated profile, those tools work automatically; pass `profile` as a tool arg only when they want a non-default one.\n" +
              "- Read coalesce://context/overview for broader context if the user asks.\n\n" +
              "Start now by calling diagnose_setup.",
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
