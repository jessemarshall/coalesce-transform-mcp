# Coalesce MCP Server Overview

## How This Server Works

This MCP server provides tools and resources for working with Coalesce Transform workspaces and environments via the Coalesce API.

### Available Resources

Consult these resources for specific guidance. Load only what the current task needs.

- **coalesce://context/pipeline-workflows** — Building pipelines, node type selection, multi-node sequences, incremental setup
- **coalesce://context/node-operations** — Editing nodes: join conditions, columns, config, renames, SQL conversion
- **coalesce://context/node-creation-decision-tree** — Routing: which tool to use for creation vs update
- **coalesce://context/data-engineering-principles** — Architecture, platforms, methodology, materialization, packages
- **coalesce://context/aggregation-patterns** — GROUP BY, datatype inference, common aggregation patterns
- **coalesce://context/node-type-corpus** — Node type discovery, corpus search, metadata patterns
- **coalesce://context/tool-usage** — Core tool rules, discovery patterns, pagination, parallelization
- **coalesce://context/sql-platform-selection** — Determine the active SQL platform
- **coalesce://context/sql-snowflake** — Snowflake SQL conventions
- **coalesce://context/sql-databricks** — Databricks SQL conventions
- **coalesce://context/sql-bigquery** — BigQuery SQL conventions
- **coalesce://context/storage-mappings** — `{{ ref() }}` syntax and storage locations
- **coalesce://context/id-discovery** — Resolving project, workspace, environment, and node IDs
- **coalesce://context/node-payloads** — Full node body editing guidance
- **coalesce://context/hydrated-metadata** — Hydrated metadata structures
- **coalesce://context/run-operations** — Start, retry, diagnose, and cancel runs
- **coalesce://context/intelligent-node-configuration** — Intelligent config completion

### Response Guidelines

- Answer questions directly without preamble
- Use fenced code blocks with language tags
- Provide detail when requested, brevity otherwise
- For multi-step pipeline creation, report progress after each node

## How Coalesce Nodes Work

**CRITICAL**: Coalesce nodes are NOT SQL scripts. They are declarative configurations with these components:

| Component | Where it lives | What it does |
|-----------|---------------|--------------|
| **Columns** | `metadata.columns[].transform` | Each column has a SQL expression (e.g., `"ORDERS"."CUSTOMER_ID"`, `SUM("ORDERS"."TOTAL")`) |
| **Join condition** | `metadata.sourceMapping[].join.joinCondition` | The FROM/JOIN/WHERE/GROUP BY clause using `{{ ref() }}` syntax |
| **Dependencies** | `metadata.sourceMapping[].dependencies` | Which upstream nodes this node reads from |
| **Config** | `config` | Node-type-specific settings (truncate, business keys, SCD, etc.) |

The node type's Jinja template combines these into final SQL at compile time. You configure **columns and joins separately** — you never write a complete SQL query.

**Example — a CLV aggregation node:**

```text
Columns:
  - CUSTOMER_ID: transform = "CUSTOMER_LOYALTY"."CUSTOMER_ID"
  - TOTAL_ORDERS: transform = COUNT(DISTINCT "ORDER_HEADER"."ORDER_ID")
  - LIFETIME_VALUE: transform = SUM("ORDER_HEADER"."ORDER_TOTAL")

joinCondition:
  FROM {{ ref('STAGING', 'CUSTOMER_LOYALTY') }} "CUSTOMER_LOYALTY"
  LEFT JOIN {{ ref('STAGING', 'ORDER_HEADER') }} "ORDER_HEADER"
    ON "CUSTOMER_LOYALTY"."CUSTOMER_ID" = "ORDER_HEADER"."CUSTOMER_ID"
  GROUP BY "CUSTOMER_LOYALTY"."CUSTOMER_ID"
```

**Key rules:**

- Column transforms reference **predecessor table aliases** from the joinCondition
- GROUP BY goes inside the joinCondition, after JOIN clauses
- Aggregate functions (COUNT, SUM, AVG) go in column transforms, NOT in joinCondition
- CASE expressions go in column transforms
- CTEs are NOT supported — break into separate upstream nodes
- Do NOT write `overrideSQL` or pass raw SQL — use the column/joinCondition model

**Required workflow for creating nodes:**

1. **Always call `plan_pipeline` first** — it discovers all available node types from the repo and ranks them for your use case. Do NOT guess node types like "Stage" or "View".
2. **Use `create_workspace_node_from_predecessor`** (or `create_workspace_node_from_scratch` for nodes with no upstream). Pass `repoPath` for automatic config completion.
3. **Use `create_workspace_node_from_predecessor`** (or `create_workspace_node_from_scratch` for nodes with no upstream) — they handle validation, config completion, and column-level attributes automatically.

Config completion is automatic when `repoPath` is provided — the response includes `configCompletion` showing what node-level config and column-level attributes were applied.

**Post-creation verification (required before moving to the next node):**

1. Check `nextSteps` in the creation response — follow all required steps (especially join setup for multi-predecessor nodes)
2. Check `validation.allPredecessorsRepresented` — if false, the join is incomplete
3. For multi-predecessor nodes: set up the join condition via `convert_join_to_aggregation` (aggregation), `apply_join_condition` (row-level joins), or `update_workspace_node` (manual)
4. Verify the final node with `get_workspace_node` — confirm columns, joinCondition, and config are correct
5. Follow naming conventions: STG_ for staging, DIM_ for dimensions, FACT_ for facts, INT_ for intermediate (e.g., `STG_LOCATION`, `FACT_ORDERS`). Default to UPPERCASE for Snowflake, but **respect the user's chosen casing**

**Anti-pattern — writing SQL and passing it to the planner:**

Do NOT author SQL yourself to pass to `plan_pipeline` or `create_pipeline_from_sql`. The `sql` parameter exists solely for converting SQL that the **user** provided. When building a pipeline, use declarative tools:

1. `create_workspace_node_from_predecessor` to create nodes
2. `update_workspace_node` to set joinCondition
3. `replace_workspace_node_columns` or `convert_join_to_aggregation` for column transforms

### Writing SQL for Nodes

Determine the platform first, then load exactly one dialect resource:

1. **Detect**: `get_project` for warehouse type, or check existing node SQL (see `coalesce://context/sql-platform-selection`)
2. **Load one**: Snowflake -> `coalesce://context/sql-snowflake`, Databricks -> `coalesce://context/sql-databricks`, BigQuery -> `coalesce://context/sql-bigquery`
3. **Follow that dialect's rules** for the entire edit

## Operational Scope

### In Scope

- Creating/updating workspace nodes
- Reasoning from project metadata and workspace patterns
- Writing SQL transforms and joins
- Running jobs and monitoring runs
- Managing environments and projects

### Out of Scope

- **Previewing compiled SQL** — compilation happens at deploy/run time
- **Changing node type** — set at creation time, cannot be changed via API. Create a new node of the desired type instead
- **Data preview / row counts** — this server manages node definitions, not live warehouse data. Use a Snowflake-capable tool if available
- **Cross-workspace replication** — no clone tool; recreate each node bottom-up in the target workspace
- Creating/modifying node type templates, source nodes, or macros

### Description Generation

Only generate descriptions when explicitly requested. Focus on disambiguation — what makes this column the one the user is looking for? If you lack context, ask.

Apply all descriptions in one call using `replace_workspace_node_columns`.

## Documentation

- [General Coalesce Docs](https://docs.coalesce.io/docs)
- [API Documentation](https://docs.coalesce.io/docs/api)
- [Snowflake Base Node Types](https://docs.coalesce.io/docs/marketplace/package/coalesce_snowflake_base-node-types)
- [Incremental Loading](https://docs.coalesce.io/docs/marketplace/package/coalesce_snowflake_incremental-loading)
- [Dynamic Tables](https://docs.coalesce.io/docs/marketplace/package/coalesce_snowflake_dynamic-tables)
