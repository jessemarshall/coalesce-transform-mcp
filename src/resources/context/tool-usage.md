# Tool Usage Patterns

## Core Rules

- Only call tools that actually exist in this MCP server. Do not invent tool names.
- Prefer the smallest sufficient tool: list for discovery, get for detail, workflow helpers for multi-step operations.
- If the request is exploratory or ambiguous, ask clarifying questions before mutating anything.
- Before mutating a node, read the current state unless a helper already does that for you.
- Treat arrays as full-replacement fields unless a tool explicitly documents merge semantics. This is especially important for `metadata.columns`.
- When a helper returns `validation`, `warning`, `resultsError`, or `incomplete`, inspect those fields before continuing.
- Before declaring work complete, verify real outcomes by re-reading the saved node and checking `validation` fields.

## Discovery Patterns

### Find Projects and Workspaces

- `list-projects` or `get-project` to discover project structure
- For workspace IDs: `list-projects({ includeWorkspaces: true })` or `get-project({ projectID, includeWorkspaces: true })`
- For broader ID resolution guidance, see `coalesce://context/id-discovery`

### List vs Get

- **List** endpoints for discovery and broad state: `list-environments`, `list-projects`, `list-runs`, `list-workspace-nodes`, `list-environment-nodes`
- **Get** endpoints when you have an ID and need the full object: `get-environment`, `get-project`, `get-run`, `get-workspace-node`, `get-environment-node`

### Pagination

- Large list endpoints may be paginated. Do not assume one page is complete if `next` or paging cursors are present.
- Prefer discovery first, then targeted reads.
- Large JSON responses may be auto-cached to `data/auto-cache/`; the tool returns cache metadata plus the file path instead of the full payload.
- Use explicit cache tools for large lists: `cache-workspace-nodes`, `cache-environment-nodes`, `cache-runs`, `cache-org-users`.

### Large Workspace Analysis

- `analyze-workspace-patterns` for a compact workspace profile (naming, packages, methodology conventions)
- `cache-workspace-nodes` when the full payload should be written to `data/nodes/` for reuse
- For architecture guidance, see `coalesce://context/data-engineering-principles`

## Mandatory: Plan Before Creating Nodes

**NEVER skip `plan-pipeline` before creating nodes.** Do not hardcode node types like "Stage", "65", or any other ID.

1. Call `plan-pipeline` with `goal`, `sourceNodeIDs`, and `repoPath` — it discovers and ranks all node types
2. Use the `nodeType` from the plan result when calling creation tools
3. If the plan suggests a specialized type (Dynamic Table, Incremental, etc.) but you're doing standard batch ETL, use Stage or Work instead

**Common mistake**: Picking node types by ID or name without calling `plan-pipeline`. This leads to wrong types (e.g., Dynamic Tables for batch ETL) because the agent has no visibility into what types are available or appropriate.

## Repo-Backed and Corpus Workflows

Before creating nodes with complex metadata:

1. Check observed types: `list-workspace-node-types` (see `coalesce://context/node-type-corpus`)
2. If a local repo is available: install the package, commit, update clone, use `list-repo-node-types` / `get-repo-node-type-definition` / `generate-set-workspace-node-template` with `repoPath` or `COALESCE_REPO_PATH`
3. If the repo lacks the definition: use `search-node-type-variants` / `get-node-type-variant` / `generate-set-workspace-node-template-from-variant`
4. Adapt the pattern with user-specific data
5. Create with `create-workspace-node-from-predecessor` or pipeline tools

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

## Related Resources

- `coalesce://context/pipeline-workflows` — building pipelines end-to-end
- `coalesce://context/node-operations` — editing nodes after creation
- `coalesce://context/node-creation-decision-tree` — which tool to use
