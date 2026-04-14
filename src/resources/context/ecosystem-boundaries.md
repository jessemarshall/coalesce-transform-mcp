# Ecosystem Boundaries

## What This MCP Covers

The Coalesce Transform MCP manages **transform definitions and workspace configuration** within the Coalesce platform:

- Workspace nodes: create, edit, delete, configure, and inspect node definitions
- Pipeline building: plan, create, and review multi-node transform pipelines
- Runs: start, poll, diagnose, and retry deploy/refresh jobs
- Lineage: trace upstream/downstream dependencies at node and column level within a workspace
- Node types: discover, search, and inspect repo-backed and workspace node type definitions
- Configuration: intelligent auto-completion, schema resolution, storage mappings

It does **not** manage:

- Warehouse data (tables, schemas, row counts, sample data)
- Ingestion/extraction pipelines (connectors, syncs, sources)
- Data catalog or governance metadata
- Cross-platform lineage spanning ingestion -> transform -> consumption

## Adjacent Data Engineering MCPs

When a user's request falls outside this server's scope, direct them to the appropriate MCP:

### Snowflake MCP
- **Scope:** Warehouse queries, table DDL, row-level data inspection, Cortex AI features
- **When to use:** User asks about actual table data, wants to run SQL against the warehouse, needs schema introspection from the warehouse side, or wants to validate that a Coalesce-deployed node produced the expected output
- **Handoff pattern:** After deploying via `run_and_wait`, use Snowflake MCP to query the resulting table

### Fivetran MCP
- **Scope:** Ingestion pipeline management, connector configuration, sync status, destination setup
- **When to use:** User asks about source data freshness, connector health, or wants to trigger/check an ingestion sync
- **Handoff pattern:** Check Fivetran sync status before running a Coalesce refresh to ensure source data is current

### dbt MCP
- **Scope:** dbt model management, documentation generation, model lineage, SQL compilation
- **When to use:** User works in a dbt-based project alongside Coalesce, or asks about dbt models, tests, or documentation
- **Note:** Coalesce and dbt serve overlapping transform roles — users typically use one or the other, not both for the same tables

### Coalesce Catalog MCP *(planned — not yet available)*
- **Scope:** Data catalog search, governance metadata, end-to-end lineage visualization
- **When to use:** User needs lineage that spans beyond Coalesce (ingestion -> transform -> consumption), catalog search across the full data stack, or governance/compliance metadata
- **Handoff pattern:** Lineage from `get_upstream_nodes`/`get_downstream_nodes` covers Coalesce nodes only. When the Catalog MCP becomes available, combine with its results for full-stack lineage
- **Status:** This MCP is planned but not yet published. Cross-server workflow patterns referencing it describe future capabilities

## Cross-Server Workflow Patterns

### Pre-run validation
1. **Fivetran MCP** -> Check that source connectors completed their latest sync
2. **This MCP** -> `run_and_wait` to execute the Coalesce refresh job
3. **Snowflake MCP** -> Query the output tables to validate results

### Impact analysis across the stack
1. **This MCP** -> `analyze_impact` to identify downstream Coalesce nodes affected by a change
2. **Catalog MCP** *(when available)* -> Check if downstream consumers (dashboards, reports) are affected beyond Coalesce

### Debugging a data quality issue
1. **Snowflake MCP** -> Identify the problematic data in the warehouse
2. **This MCP** -> `get_upstream_nodes` and `get_workspace_node` to trace the transform logic
3. **Fivetran MCP** -> Check if the issue originated in the source data or ingestion

## Lineage Scope

The lineage tools in this MCP (`get_upstream_nodes`, `get_downstream_nodes`, `get_column_lineage`, `analyze_impact`) operate **within a single Coalesce workspace**. They trace dependencies between Coalesce nodes only.

For lineage that crosses system boundaries (e.g., from a Fivetran source -> Coalesce Stage -> Coalesce Dimension -> a BI dashboard), a Catalog MCP would provide the broader view. When the Coalesce Catalog MCP becomes available, this MCP's lineage data can be a precise input into that broader graph, but it does not extend beyond Coalesce node boundaries on its own.
