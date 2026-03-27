# Data Engineering Principles for Coalesce

## How to Use This Guide

When to consult:
- Before creating workspace nodes that need architecture decisions
- When evaluating existing workspace structure or methodology
- When choosing materialization strategies

Application pattern:
- If the user already specified the exact node type, use it and skip analysis.
- Use `coalesce_analyze_workspace_patterns` for a compact inline profile of workspace conventions.
- Use `coalesce_cache_workspace_nodes` when the full node list should bypass chat context or be reused.
- If workspace differs from recommendations, inform user with rationale.
- If workspace aligns, proceed with existing pattern.

For node type selection by pipeline layer, see `coalesce://context/pipeline-workflows`.

## Platform Awareness

Coalesce supports multiple data platforms. Materialization strategies, cost models, and features differ significantly. Determine the platform before recommending materialization.

To detect: check `coalesce://context/sql-platform-selection`, inspect existing node configurations, or ask.

### Snowflake

- Compute model: Per-second compute billing
- Key features: Transient tables (no Fail-safe, lower cost for staging), Dynamic Tables (declarative, auto-refreshing), Streams/Tasks (CDC), materialized views
- Staging best practice: Transient tables for bronze/silver layers
- Incremental: MERGE with high-water mark, Streams for CDC
- Config indicators: `insertStrategy`, `truncateBefore`, `materializationType`

### Databricks

- Compute model: DBU-based billing, Photon engine
- Key features: Delta format (time travel, OPTIMIZE, ZORDER), Delta Live Tables (DLT), streaming tables, Unity Catalog
- Staging best practice: Delta tables with OPTIMIZE for large staging
- Incremental: Delta MERGE, APPLY CHANGES (DLT), streaming tables
- Note: Views are logical views over Delta tables; materialized views less common than Snowflake/BigQuery

### BigQuery

- Compute model: Per-bytes-scanned (on-demand) or slot-based (reservations). Views are expensive because each query rescans underlying data.
- Key features: Partitioned tables, clustered tables, materialized views (auto-refreshing), table snapshots, expiration policies
- Staging best practice: Partitioned tables with expiration; clustering on high-cardinality filter columns
- Incremental: MERGE with partition pruning, streaming inserts
- IMPORTANT: Avoid views for repeatedly queried or large-scan nodes. Prefer materialized views or tables.

## Workspace Pattern Analysis

### Package Detection

Scan node types for package prefixes:
- `base-nodes:::*` -> base-nodes package observed
- `custom-package:::*` -> custom package observed
- No prefix -> built-in type

Presence indicates observed usage, not exhaustive inventory. Absence doesn't prove a package is unavailable.

Package categories:
- **base-nodes**: Enhanced Stage, View, Dimension, Fact, Work
- **Specialized**: Data Vault, Kimball extensions, semantic layers
- **Platform-specific**: Databricks DLT, BigQuery advanced, Dynamic Tables, Streams/Tasks
- **Data quality**: Incremental loading, test filtering, validation
- **Built-in**: Stage, View, Dimension, Fact, persistentStage (no prefix)

### DAG Topology Analysis

| Layer | DAG Signature | Typical Names |
|-------|--------------|---------------|
| Bronze/Landing | 0 predecessors or source predecessors | RAW_*, SRC_*, LANDING_* |
| Silver/Staging | 1-2 predecessors from bronze | STG_*, STAGE_*, CLEAN_* |
| Intermediate | Mid-pipeline (has predecessors AND consumers) | INT_*, WORK_*, TRANSFORM_* |
| Gold/Mart | Multiple predecessors, few/no consumers | DIM_*, FACT_*, FCT_*, MART_* |

### Methodology Detection

**Kimball**: DIM_*/FACT_* separation, facts with 3+ dimension predecessors, star/snowflake topology. SCD indicators: EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT columns.

**Data Vault 2.0**: Hub (few columns, business key + metadata, many downstream satellites), Satellite (many columns, single hub predecessor, HASH_DIFF), Link (multiple hub predecessors). May use specialized packages from github.com/coalesceio.

**dbt-Style**: stg_ -> int_ -> fct_/dim_ naming, heavy View usage in intermediate layer, selective materialization.

**Mixed/Unclear**: Default to staging -> mart pattern. Don't force methodology on established workspaces.

## Materialization Strategies

### Table (Full Refresh)

When: Data changes significantly each run, staging/bronze layer, dimensions with low update frequency.
Trade-offs: Simple, predictable, consistent. Higher compute for large datasets.

- Snowflake: `truncateBefore: true`, `materializationType: "table"`. Transient tables for staging.
- BigQuery: Cost-effective even for large datasets (bytes written, not row count).
- Databricks: Delta tables with ACID guarantees.

### View

When: Intermediate transforms, low query frequency, always-fresh data, simple transforms without aggregation.

IMPORTANT: `View` node types can ONLY materialize as views. Cannot convert to tables. For aggregations or frequently queried nodes, use `Dimension`, `Fact`, `Stage`, or `Work`.

- Snowflake: Each query consumes compute. Acceptable for low-frequency.
- BigQuery: Expensive — every query rescans and bills per bytes. Avoid for frequent queries or large tables.
- Databricks: Benefits from Delta caching but recomputes on each query.

### Incremental (Merge/Append)

When: Large datasets, time-series/event data, fact tables with high volume, staging with clear update patterns.
Trade-offs: Efficient, faster runs. More complex, risk of drift.

- Snowflake: MERGE with high-water mark, `insertStrategy: "MERGE"`. Streams/Tasks for CDC.
- BigQuery: MERGE with partition pruning (always partition incremental tables).
- Databricks: Delta MERGE with schema evolution. APPLY CHANGES in DLT.

### Dynamic / Auto-Refreshing

- Snowflake Dynamic Tables: Declarative SQL, auto-refresh based on lag target
- BigQuery Materialized Views: Auto-refresh with smart tuning (single-table aggregations)
- Databricks DLT: Declarative pipeline definitions with automatic refresh

Check for platform-specific packages (Dynamic-Table-Nodes, databricks-DLT).

### Layer-Specific Defaults

- Bronze -> Tables (preserve raw; Snowflake: transient; BigQuery: partitioned with expiration)
- Silver -> Tables for small, incremental for large (BigQuery: always partition)
- Intermediate -> Views (BigQuery: materialize if queried by multiple downstream nodes)
- Gold Dimensions -> Tables (small, need persistence)
- Gold Facts -> Incremental tables (large, time-series)
- Metrics -> Tables via `Dimension` or `Fact`; consider materialized views for single-table aggregations

## Dependency Management

### Healthy DAG Patterns

- **Fan-out** (1 -> many): One staging feeds multiple downstream. Promotes reusability.
- **Fan-in** (many -> 1): Multiple sources join into one. Sweet spot: 2-4 predecessors.
- **Linear chains** (1 -> 1 -> 1): Acceptable at 3-5 steps if each adds clear value.

### Problematic Patterns

- **Excessive fan-in** (>5 predecessors): Break into intermediate nodes.
- **Deep chains** (>6 steps): Consolidate or use views.
- **Circular dependencies**: Break by extracting shared logic upstream.
- **Cross-layer skips**: Gold reading Bronze directly. Route through proper layers.

### Warnings to Surface

- predecessorNodeIDs.length > 5 -> warn about excessive fan-in
- 7th node in linear chain -> warn about deep chain
- Cross-layer skip -> warn about layer violation

## Package Recommendations

### Base Nodes

If workspace has NO base-nodes types: "The base-nodes package offers enhanced Stage, View, Dimension, and Fact. Install via Build Settings > Packages."
If workspace HAS base-nodes types: use base-nodes versions by default.

### Common Packages

| Package | When to Recommend | Node Types |
|---------|------------------|------------|
| Incremental-Nodes | Large fact tables, "incremental"/"delta" mentions | Incremental Load, Test Passed/Failed Records, Looped Load |
| Dynamic Tables | Auto-refreshing aggregations (Snowflake) | Dynamic Table Work, Dimension |
| Materialized Views | Simple single-table aggregations (Snowflake) | Materialized View |
| Streams/Tasks | CDC, event-driven (Snowflake) | Stream, Task |
| DLT | Declarative pipelines (Databricks) | DLT nodes |

### Recommendation Logic

1. Match existing workspace patterns first
2. Fresh workspace with built-in types -> soft recommend base-nodes
3. Specialized need -> point to specific package, remind "Build Settings > Packages to install"
4. Never assume installed: "If you have the package..." not "Use the node type"
5. Don't push packages when built-in types work fine

## Related Resources

- `coalesce://context/pipeline-workflows` — node type selection by layer, pipeline building
- `coalesce://context/node-type-corpus` — node type discovery and corpus search
- `coalesce://context/sql-platform-selection` — platform detection
