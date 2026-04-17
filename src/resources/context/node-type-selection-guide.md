# Node Type Selection Guide

When creating pipeline nodes, choose the node type based on the actual purpose of the node — not the SQL pattern.

## General-Purpose Node Types (use for most transforms)

### Stage / Work
- **Purpose**: General-purpose intermediate processing. Handles single-source, multi-source joins, GROUP BY, UNION, filters, transforms. These are interchangeable for most patterns.
- **Materialization**: Table or View
- **Use for**: Column renames, type casts, WHERE filters, GROUP BY aggregations, multi-table JOINs, UNION/UNION ALL, any transformation without special requirements
- **Default choice**: When in doubt, use Stage or Work

### View
- **Purpose**: Virtual table with no physical storage. Recalculates on every query.
- **Materialization**: View only
- **Use for**: Lightweight projections, secure views, cost savings when recomputation is OK
- **Avoid when**: Downstream queries are performance-critical or aggregations are expensive

## Dimensional Modeling Node Types (only when explicitly building a dimensional model)

### Dimension
- **Purpose**: Descriptive business context (customers, products, locations). Requires business keys. Supports SCD Type 1/2.
- **Materialization**: Table or View
- **Use ONLY when**: Building a star/snowflake schema, node is named dim_ or dimension_, SCD tracking is needed
- **NOT for**: Generic GROUP BY, aggregations, staging, or CTE decomposition

### Fact
- **Purpose**: Business measures (revenue, quantity, cost) at a defined grain. Requires business keys.
- **Materialization**: Table or View
- **Use ONLY when**: Building a fact table in a dimensional model, node is named fct_ or fact_
- **NOT for**: Any GROUP BY or SUM — those are transforms, not fact tables

### Factless Fact
- **Purpose**: Record events without numeric measures (attendance, eligibility)
- **Use ONLY when**: Event tracking without measures in a dimensional model

## Change Tracking Node Types

### Persistent Stage
- **Purpose**: CDC / change tracking with business keys. Type 1/Type 2 history.
- **Materialization**: Table only
- **Use ONLY when**: Goal explicitly mentions CDC, change tracking, or history tracking
- **NOT for**: Simple staging, general transforms, CTE decomposition

## Data Vault Node Types

### Hub / Satellite / Link
- **Use ONLY when**: Explicitly building a Data Vault model
- **NOT for**: General-purpose joins or transforms

## Specialized Materialization Patterns (only when explicitly requested)

### Dynamic Tables
- **Purpose**: Snowflake-managed declarative refresh with lag-based orchestration
- **Use when**: Near-real-time / continuous refresh, streaming-like pipelines, replacing complex Streams+Tasks
- **NOT for**: Batch ETL, scheduled runs, CTE decomposition, cost-sensitive workloads
- **Key difference**: Snowflake manages the refresh DAG automatically. You specify a lag (e.g., 5 minutes) and Snowflake keeps data fresh within that window. This adds continuous compute cost.

### Incremental Load
- **Purpose**: Process only new/modified records via high-water mark comparison
- **Use when**: Large tables where full refresh is too expensive, append-only sources
- **NOT for**: Full-refresh staging, CTE decomposition, small-to-medium tables

### Materialized View
- **Purpose**: Pre-computed aggregations that auto-refresh when base data changes
- **Use when**: Expensive aggregations that need to stay current, single-source only
- **NOT for**: Multi-source joins, standard transforms

### Deferred Merge
- **Purpose**: Snowflake Streams + scheduled merge tasks for high-frequency ingestion
- **Use when**: High-frequency data ingestion where immediate merge is too expensive
- **NOT for**: Batch ETL, standard staging

### Tasks / DAG
- **Purpose**: Snowflake scheduled or DAG-based orchestration
- **Use when**: Building scheduled task workflows
- **NOT for**: Data transformation nodes

## Decision Rules

1. **CTE decomposition**: Each CTE becomes a Stage or Work node. Never Dimension, Fact, or specialized types unless the user explicitly names it that way.
2. **GROUP BY / SUM / COUNT**: These are transforms. Use Stage/Work. Only use Fact/Dimension if building a dimensional model.
3. **Multi-source JOIN**: Use Stage or Work — both handle joins via sourceMapping.
4. **Name prefix**: `stg_` → Stage, `wrk_` → Work, `dim_` → Dimension, `fct_` → Fact, `vw_` → View. Follow the prefix.
5. **No explicit requirement**: Default to Stage for single-source, Work for multi-source.
6. **Specialized types require explicit user request**: Dynamic Tables, Incremental Load, Materialized View, Deferred Merge, and Tasks are specialized materialization patterns. **NEVER auto-select these.** The user must explicitly ask for continuous refresh, incremental processing, or the specific pattern by name.

## Common Mistakes

| Mistake | Why It's Wrong | Correct Approach |
|---------|---------------|-----------------|
| Using Dynamic Tables for batch ETL | Dynamic Tables add continuous compute cost and are for near-real-time refresh | Use Stage or Work with table materialization |
| Picking node type by ID (e.g., "65") without `plan_pipeline` | The agent doesn't know what the ID maps to or if it's appropriate | Always call `plan_pipeline` first |
| Using Dimension/Fact for GROUP BY | GROUP BY is a transform, not a dimensional model | Use Stage or Work |
| Skipping `plan_pipeline` and guessing "Stage" | May miss better options or select an inappropriate type | Always call `plan_pipeline` with `repoPath` |
| Using Incremental Load for small tables | Incremental overhead isn't worth it for fast full refreshes | Use Stage with full refresh |
| Picking Persistent Stage (PSTG) because the user said "staging" | PSTG is for CDC / SCD / change tracking — not general staging. The short name `PSTG` is a false friend for `STG`. | Use Stage (STG) for general staging. Only use Persistent Stage if the human explicitly asks for CDC, change tracking, SCD, historical snapshots, or names Persistent Stage / PSTG directly. |
