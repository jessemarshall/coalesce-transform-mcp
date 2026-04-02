# Tool Usage Patterns

## Core Rules

- **Snowflake data questions are out of scope for this server.** This server manages node *definitions* (what's designed), not live warehouse data (what exists). If the user asks about data by database or schema name (e.g. "what tables are in JESSE_DEV?", "show me rows from ANALYTICS.SALES"), use a Snowflake-capable tool if one is available.
- Only call tools that actually exist in this MCP server. Do not invent tool names.
- Prefer the smallest sufficient tool: list for discovery, get for detail, workflow helpers for multi-step operations.
- If the request is exploratory or ambiguous, ask clarifying questions before mutating anything.
- Before mutating a node, read the current state unless a helper already does that for you.
- Treat arrays as full-replacement fields unless a tool explicitly documents merge semantics. This is especially important for `metadata.columns`.
- When a helper returns `validation`, `warning`, `resultsError`, or `incomplete`, inspect those fields before continuing.
- Before declaring work complete, verify real outcomes by re-reading the saved node and checking `validation` fields.

## Discovery Patterns

### Find Projects and Workspaces

- `list_projects` or `get_project` to discover project structure
- For workspace IDs: `list_workspaces()` for all workspaces (returns workspaces with their `projectID`), or `get_project({ projectID, includeWorkspaces: true })` for project-scoped
- For broader ID resolution guidance, see `coalesce://context/id-discovery`

### List vs Get

- **List** endpoints for discovery and broad state: `list_environments`, `list_projects`, `list_runs`, `list_workspace_nodes`, `list_environment_nodes`
- **Get** endpoints when you have an ID and need the full object: `get_environment`, `get_project`, `get_run`, `get_workspace_node`, `get_environment_node`

### Pagination

- Large list endpoints may be paginated. Do not assume one page is complete if `next` or paging cursors are present.
- Prefer discovery first, then targeted reads.
- Large JSON responses may be auto-cached to `coalesce_transform_mcp_data_cache/auto-cache/`; the tool returns cache metadata plus a `coalesce://cache/...` resource URI and `resource_link` instead of the full payload.
- Use explicit cache tools for large lists: `cache_workspace_nodes`, `cache_environment_nodes`, `cache_runs`, `cache_org_users`.

### Large Workspace Analysis

- `analyze_workspace_patterns` for a compact workspace profile (naming, packages, methodology conventions)
- `cache_workspace_nodes` when the full payload should be written to `coalesce_transform_mcp_data_cache/nodes/` for reuse
- For architecture guidance, see `coalesce://context/data-engineering-principles`

## Mandatory: Plan Before Creating Nodes

**NEVER skip `plan_pipeline` before creating nodes.** Do not hardcode node types like "Stage", "65", or any other ID.

1. Call `plan_pipeline` with `goal`, `sourceNodeIDs`, and `repoPath` — it discovers and ranks all node types
2. Use the `nodeType` from the plan result when calling creation tools — the planner already excludes specialized types (Dynamic Table, Incremental, Materialized View, etc.) unless your context explicitly requires them

**Common mistake**: Picking node types by ID or name without calling `plan_pipeline`. This leads to wrong types (e.g., Dynamic Tables for batch ETL) because the agent has no visibility into what types are available or appropriate.

## Repo-Backed and Corpus Workflows

Before creating nodes with complex metadata:

1. Check observed types: `list_workspace_node_types` (see `coalesce://context/node-type-corpus`)
2. If a local repo is available: install the package, commit, update clone, use `list_repo_node_types` / `get_repo_node_type_definition` / `generate_set_workspace_node_template` with `repoPath` or `COALESCE_REPO_PATH`
3. If the repo lacks the definition: use `search_node_type_variants` / `get_node_type_variant` / `generate_set_workspace_node_template_from_variant`
4. Adapt the pattern with user-specific data
5. Create with `create_workspace_node_from_predecessor` or pipeline tools

## Parallel vs Sequential Tool Use

### Safe to Parallelize

- Independent reads
- Multiple list/get calls for unrelated objects
- Discovery reads across several predecessor nodes

### Usually Sequential

- Dependent node creation (upstream before downstream)
- Create then validate flows
- Update flows where the next step depends on the saved body
- Run lifecycle steps

### Independent Writes

Parallelize only when writes truly do not depend on each other and you verify each result afterward. When in doubt, prefer sequential.

## Storage and SQL Context

- Use the actual node body to determine storage/location fields when writing SQL references
- Determine the platform with `coalesce://context/sql-platform-selection`, then read the matching dialect resource
- Cross-check `{{ ref() }}` assumptions against `coalesce://context/storage-mappings`
- For full-body edits, cross-check `coalesce://context/node-payloads` and `coalesce://context/hydrated-metadata`

## Capabilities and Boundaries

Your capabilities are defined by the tools in this MCP server:

- **Tool names**: Use exact tool names as listed. If an operation is not available as a tool, it falls outside your scope.
- **Source nodes**: Read-only. You can read them and use as predecessors, but cannot modify their columns or config.
- **Array fields**: Always send the full accumulated array. The server replaces — it does not merge.
- **Helper warnings**: `warning`, `validation`, and `resultsError` fields are actionable. A successful call with warnings may not be fully correct.
- **Node readiness**: A node is fully usable only when `validation` confirms coverage and correctness.

## Lineage Tools

- **First call is slow**: `get_upstream_nodes`, `get_downstream_nodes`, `analyze_impact`, and `propagate_column_change` all build a full lineage cache on first use (fetches every node with `detail=true`). Expect latency on the first call per workspace; subsequent calls within the same session reuse the cache.
- **Exploration vs impact**: Use `get_upstream_nodes` / `get_downstream_nodes` when you need to trace the graph. Use `analyze_impact` when you want to understand what breaks if a specific node or column changes — it returns affected downstream counts without traversal lists.
- **`propagate_column_change` is destructive and expensive**: It PUTs updated node bodies for every downstream node. Always confirm with the user before calling it. Requires at least one of `columnName` or `dataType` in `changes` — an empty `changes: {}` is rejected.
- **Node ID required, not name**: All lineage tools take a `nodeID`, not a node name. Resolve the ID first with `list_workspace_nodes` or `get_workspace_node`.

## Related Resources

- `coalesce://context/pipeline-workflows` — building pipelines end-to-end
- `coalesce://context/node-operations` — editing nodes after creation
- `coalesce://context/node-creation-decision-tree` — which tool to use
