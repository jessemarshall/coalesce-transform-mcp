# Pipeline Building Workflows

## Quick Reference

| User says | Node type | Action |
|-----------|-----------|--------|
| "stage my X data" | `Stage` | `coalesce_create_workspace_node_from_predecessor` with source node |
| "create a dimension for X" | `Dimension` | `coalesce_create_workspace_node_from_predecessor` with staging node |
| "build a fact table from X and Y" | `Fact` | `coalesce_create_workspace_node_from_predecessor` with both, then `coalesce_convert_join_to_aggregation` |
| "create a view for X" | `View` | `coalesce_create_workspace_node_from_predecessor` with upstream node |
| "join X and Y" | `View` | `coalesce_create_workspace_node_from_predecessor` with both, then apply join condition |
| "build an incremental pipeline" | `Incremental Load` | See "Incremental Pipeline Setup" below |
| "create an empty node" | Any | `coalesce_create_workspace_node_from_scratch` with `name` and `metadata.columns` |

Use this table as a heuristic for likely node families, but still call `coalesce_plan_pipeline` before creating anything so the exact `nodeType` is confirmed from the repo/workspace context. Use the full workflow whenever the request is ambiguous or involves multiple layers.

## Choosing the Right Node Type

**Step 1: Check available types**

```javascript
coalesce_list_workspace_node_types({ workspaceID })
```

Prefer types already in use. If `base-nodes:::Stage` etc. are observed, use those over built-in types. If a recommended type is not observed, tell the user it may need installation via Build Settings > Packages.

**Step 2: Read source columns**

```javascript
coalesce_get_workspace_node({ workspaceID, nodeID: "source-id" })
```

Look for timestamp columns (UPDATED_AT, CREATED_AT) for incremental loading, business keys (CUSTOMER_ID) for merge keys, and SCD columns (EFFECTIVE_FROM, IS_CURRENT) for dimension tracking.

**Step 3: Select by pipeline layer**

| Layer | Default | Use Instead When |
|-------|---------|-----------------|
| Staging | `Stage` | Source has timestamps AND is large -> `Incremental Load` (requires package) |
| Intermediate | `View` | Expensive computation -> `Stage` or `Work` |
| Dimension (gold) | `Dimension` | Has SCD columns -> configure SCD Type 2 |
| Fact (gold) | `Fact` | Large append-heavy data -> incremental config |
| Metrics | `View` | Complex/frequently queried -> `Fact` |

IMPORTANT: `View` types can ONLY materialize as views. For aggregations queried repeatedly, use `Dimension` or `Fact`. See `coalesce://context/data-engineering-principles` for platform-specific materialization guidance.

Node type format: `"Stage"` (simple) or `"IncrementalLoading:::230"` (PackageName:::NodeTypeID). Prefer the package-prefixed format when known.

## Pipeline Building Sequence

### Step 1: Discover the Workspace

```javascript
coalesce_list_workspaces()
```

### Step 2: Find Source Nodes

```javascript
coalesce_list_workspace_nodes({ workspaceID, detail: true })
```

Note node IDs, names, and `locationName` (needed for `{{ ref() }}` syntax).

### Step 3: Plan to discover the correct node type

**Always call `coalesce_plan_pipeline` before creating nodes.** The planner scans the repo for all committed node type definitions, scores them against your use case, and returns the best match. Do not guess node types — use what the planner recommends.

```javascript
coalesce_plan_pipeline({
  workspaceID,
  goal: "stage customer data",
  sourceNodeIDs: ["source-id-1"],
  repoPath: "/path/to/repo"  // or rely on COALESCE_REPO_PATH
})
```

The response includes:

- `nodeTypeSelection.consideredNodeTypes` — ranked candidates with scores and reasons
- `supportedNodeTypes` — types that support automatic creation
- `nodes[].nodeType` — the recommended type for each planned node

**If the user provides SQL:** Pass it directly to the planner.

```javascript
coalesce_plan_pipeline({ workspaceID, sql: "<USER-PROVIDED SQL HERE>" })
```

Never author SQL yourself to pass to these tools.

### Step 4: Review and Execute

- Plan returns `status: "ready"` -> use the recommended node type to create, or call `coalesce_create_pipeline_from_plan`
- Plan returns `status: "needs_clarification"` -> address `openQuestions`, then re-plan

### Step 5: Create Nodes with the planned node type

Use `coalesce_create_workspace_node_from_predecessor` with the node type from the plan. **Always pass `repoPath`** so config completion runs automatically:

```javascript
coalesce_create_workspace_node_from_predecessor({
  workspaceID,
  nodeType: "base-nodes:::Stage",  // from coalesce_plan_pipeline result
  predecessorNodeIDs: ["source-id-1"],
  changes: { name: "STG_CUSTOMER" },
  repoPath: "/path/to/repo"  // enables automatic config completion
})
```

**Always use `coalesce_create_workspace_node_from_predecessor` or `coalesce_create_workspace_node_from_scratch`** — they handle validation, config completion, and column-level attributes automatically.

For joins and aggregations:

```javascript
coalesce_convert_join_to_aggregation({
  workspaceID, nodeID: "join-node-id",
  groupByColumns: [...], aggregates: [...],
  maintainJoins: true,
  repoPath: "/path/to/repo"
})
```

### Step 6: Verify Config Completion

Config is applied automatically when `repoPath` is provided. Check the `configCompletion` field in the response:

- `configCompletion.configReview.status` — `complete`, `needs_attention`, or `incomplete`
- `configCompletion.configReview.summary` — human-readable status summary
- `configCompletion.configReview.missingRequired` — required fields/columnSelectors still unset
- `configCompletion.configReview.warnings` — issues needing manual review (e.g., missing business keys on dimension nodes)
- `configCompletion.configReview.suggestions` — optional improvements (e.g., change tracking, materialization)
- `configCompletion.appliedConfig` — node-level config values that were set
- `configCompletion.columnAttributeChanges.applied` — column-level attributes (isBusinessKey, etc.)
- `configCompletion.reasoning` — why each decision was made

**Action required when `configReview.status` is not `complete`:**

- `incomplete` — required fields are missing. Set them via `coalesce_update_workspace_node` or `coalesce_replace_workspace_node_columns`.
- `needs_attention` — warnings need manual review (e.g., set `isBusinessKey` on the correct columns).

If `configCompletionSkipped` appears instead, call `coalesce_complete_node_configuration` with `repoPath` to retry.

### Step 7: Follow-Up Edits

Use `coalesce_update_workspace_node` for post-creation changes.

## Multi-Node Pipelines

Create nodes bottom-up — upstream before downstream. Each step uses node IDs from the previous step.

**Example: join two sources then aggregate**

```javascript
// 0. Plan to discover correct node types for each layer
coalesce_plan_pipeline({
  workspaceID,
  goal: "stage customer and orders, then build CLV fact",
  sourceNodeIDs: ["source-customer-id", "source-orders-id"],
  repoPath: "/path/to/repo"
})
// -> nodeTypeSelection shows e.g. "base-nodes:::Stage" for staging,
//    "base-nodes:::Fact" for the fact layer

// 1. Stage each source using the planned node type (independent — parallelize)
coalesce_create_workspace_node_from_predecessor({
  workspaceID, nodeType: "base-nodes:::Stage",  // from plan
  predecessorNodeIDs: ["source-customer-id"],
  changes: { name: "STG_CUSTOMER" },
  repoPath: "/path/to/repo"  // auto-completes config
})
// -> "stg-cust-id" + configCompletion shows applied config

coalesce_create_workspace_node_from_predecessor({
  workspaceID, nodeType: "base-nodes:::Stage",  // from plan
  predecessorNodeIDs: ["source-orders-id"],
  changes: { name: "STG_ORDERS" },
  repoPath: "/path/to/repo"
})
// -> "stg-orders-id" + configCompletion shows applied config

// 2. Create join/fact node with planned fact type
coalesce_create_workspace_node_from_predecessor({
  workspaceID, nodeType: "base-nodes:::Fact",  // from plan
  predecessorNodeIDs: ["stg-cust-id", "stg-orders-id"],
  changes: { name: "FACT_CLV" },
  repoPath: "/path/to/repo"
})
// -> "fact-clv-id", joinSuggestions with common columns, configCompletion

// 3. Aggregate with automatic JOIN ON generation
coalesce_convert_join_to_aggregation({
  workspaceID, nodeID: "fact-clv-id",
  groupByColumns: ['"STG_CUSTOMER"."CUSTOMER_ID"'],
  aggregates: [
    { name: "TOTAL_ORDERS", function: "COUNT", expression: 'DISTINCT "STG_ORDERS"."ORDER_ID"' },
    { name: "LIFETIME_VALUE", function: "SUM", expression: '"STG_ORDERS"."ORDER_TOTAL"' }
  ],
  maintainJoins: true,
  repoPath: "/path/to/repo"
})
```

**Verification after each step:** Check `validation.allPredecessorsRepresented`, `validation.autoPopulatedColumns`, `warning`, and `joinSuggestions` before proceeding.

**If a node is created with 0 columns:** The predecessor may have no columns, the IDs were wrong, or the node type doesn't auto-inherit. Try a projection-capable type (Stage, View, Work) or add columns with `coalesce_replace_workspace_node_columns`.

### UNION / Multi-Source Nodes

```javascript
coalesce_create_workspace_node_from_predecessor({
  workspaceID, nodeType: "Stage",
  predecessorNodeIDs: ["stg-orders-us-id", "stg-orders-eu-id"],
  changes: { name: "STG_ORDERS_ALL" }
})

coalesce_update_workspace_node({
  workspaceID, nodeID: "new-node-id",
  changes: { config: { insertStrategy: "UNION ALL" } }
})
```

Values: `"UNION"` (dedup), `"UNION ALL"` (keep all), `"INSERT"` (sequential, default).

## Incremental Pipeline Setup

**With the Incremental-Nodes package** (check `coalesce_list_workspace_node_types` for `IncrementalLoading:::230`):

```javascript
// 1. Create the node
coalesce_create_workspace_node_from_predecessor({
  workspaceID, nodeType: "IncrementalLoading:::230",
  predecessorNodeIDs: ["source-orders-id"],
  changes: { name: "INC_ORDERS" }
})

// 2. Configure incremental settings
coalesce_update_workspace_node({
  workspaceID, nodeID: "inc-orders-id",
  changes: {
    config: {
      filterBasedOnPersistentTable: true,
      persistentTableLocationName: "STAGING",
      persistentTableName: "INC_ORDERS",
      incrementalLoadColumn: "UPDATED_AT"
    }
  }
})
```

How it works: reads MAX of the high-water mark column from the target, filters source to rows above that value, INSERTs new rows. Any MERGE/upsert/SCD logic happens downstream in Dimension or Fact nodes.

**Without the package** — use a regular Stage with a MAX subquery in joinCondition:

```javascript
coalesce_update_workspace_node({
  workspaceID, nodeID: "new-node-id",
  changes: {
    config: { truncateBefore: false },
    metadata: {
      sourceMapping: [{
        name: "STG_ORDERS_INCREMENTAL",
        dependencies: [{ locationName: "RAW", nodeName: "ORDERS" }],
        join: {
          joinCondition: 'FROM {{ ref(\'RAW\', \'ORDERS\') }} "ORDERS"\nWHERE "ORDERS"."UPDATED_AT" > (\n  SELECT COALESCE(MAX("UPDATED_AT"), \'1900-01-01\')\n  FROM {{ ref_no_link(\'STAGING\', \'STG_ORDERS_INCREMENTAL\') }}\n)'
        },
        customSQL: { customSQL: "" }, aliases: {}, noLinkRefs: []
      }]
    }
  }
})
```

Use `{{ ref_no_link() }}` for the self-reference to avoid circular DAG dependencies.

## Data Engineering Best Practices

### Naming Conventions

Follow layer-appropriate naming to keep pipelines readable:

| Layer | Convention | Examples |
|-------|-----------|----------|
| Staging | `STG_<SOURCE>` | `STG_CUSTOMERS`, `STG_ORDERS` |
| Intermediate | `INT_<PURPOSE>` or `WRK_<PURPOSE>` | `INT_ORDER_ENRICHMENT`, `WRK_CUSTOMER_DEDUP` |
| Dimension | `DIM_<ENTITY>` | `DIM_CUSTOMER`, `DIM_PRODUCT` |
| Fact | `FACT_<PROCESS>` or `FCT_<PROCESS>` | `FACT_SALES`, `FCT_CLV` |
| View | `V_<PURPOSE>` | `V_ACTIVE_CUSTOMERS` |
| Hub | `HUB_<KEY>` | `HUB_CUSTOMER` |
| Satellite | `SAT_<HUB>_<CONTEXT>` | `SAT_CUSTOMER_DETAILS` |
| Link | `LNK_<RELATIONSHIP>` | `LNK_CUSTOMER_ORDER` |

For Snowflake, UPPERCASE node names are conventional (`STG_LOCATION`) since unquoted identifiers are uppercase. For Databricks/BigQuery, lowercase is typical. **Always respect the user's chosen casing** — if they provide or create a node with a specific case, preserve it exactly.

### Join Verification Checklist

After creating a multi-predecessor node, ALWAYS:

1. **Review `joinSuggestions`** — the response shows common columns between predecessors. Confirm these are the correct business keys for the join.
2. **Choose the right join type:**
   - `INNER JOIN` — only matching rows (use when every record must exist in both tables)
   - `LEFT JOIN` — keep all rows from the primary table (use when the left table is the "driver" and right table may have missing matches)
   - `FULL OUTER JOIN` — keep all rows from both tables (rare, use for reconciliation)
3. **Verify join cardinality:** Joining a 1M-row table to a 10M-row table on a non-unique key causes fan-out (row multiplication). Ensure at least one side of the join is unique on the join key.
4. **Set the join condition** — call `coalesce_convert_join_to_aggregation` (for aggregation), `coalesce_apply_join_condition` (for row-level joins), or `coalesce_update_workspace_node` (for full manual control)
5. **Verify columns** — call `coalesce_get_workspace_node` to confirm the final column list and transforms are correct

### Fact Table Grain

When building fact tables, define the **grain** (the set of columns that uniquely identifies each row):

- Grain columns become your `groupByColumns` in `coalesce_convert_join_to_aggregation`
- Mark grain columns as `isBusinessKey: true`
- All other columns should be aggregates (COUNT, SUM, AVG, etc.)
- If unsure about the grain, ask the user: "What uniquely identifies each row in this fact table?"

Example: A sales fact table might have grain = `[CUSTOMER_ID, ORDER_DATE, PRODUCT_ID]` with measures `QUANTITY`, `REVENUE`, `DISCOUNT_AMOUNT`.

### Post-Creation Verification

After creating each node, verify before moving to the next:

1. **Check `nextSteps`** in the creation response — follow all required steps
2. **Check `validation.allPredecessorsRepresented`** — if false, predecessors are missing from column sources
3. **Check `configCompletion`** — verify applied config and column attributes make sense
4. **For multi-predecessor nodes:** Confirm the join condition was set (call `coalesce_get_workspace_node` to verify `metadata.sourceMapping[].join.joinCondition` is not empty)
5. **For aggregation nodes:** Verify GROUP BY is valid (`validation.valid: true` from `coalesce_convert_join_to_aggregation`)

### Materialization Strategy

Choose materialization based on the node's role and query patterns:

- **Staging/Bronze:** Always `table` (preserve raw data; Snowflake: use transient tables)
- **Intermediate:** `view` for lightweight transforms; `table` for expensive computations
- **Dimensions:** `table` (small, queried repeatedly, need persistence)
- **Facts:** `table` with incremental loading for large volumes
- **Metrics/aggregations queried frequently:** `table` via Dimension or Fact (NEVER `view` for repeated aggregation queries)

IMPORTANT: `View` node types can ONLY materialize as views. If you need a table, use `Dimension`, `Fact`, `Stage`, or `Work`.

## After Building the Pipeline

1. **Deploy**: `coalesce_start_run` with `runType: "deploy"`
2. **Run**: `coalesce_start_run` with `runType: "refresh"`
3. **Monitor**: `coalesce_run_status` or `coalesce_run_and_wait`
4. **Troubleshoot**: `coalesce_get_run_results` for errors, `coalesce_retry_run` to re-run

Scheduling is configured via Jobs in the Coalesce UI. Trigger existing jobs with `coalesce_start_run` and `jobID`. See `coalesce://context/run-operations` for full guidance.

## Related Resources

- `coalesce://context/node-creation-decision-tree` — routing: which tool to use
- `coalesce://context/node-operations` — editing nodes after creation
- `coalesce://context/data-engineering-principles` — architecture and materialization
- `coalesce://context/aggregation-patterns` — GROUP BY, datatype inference
