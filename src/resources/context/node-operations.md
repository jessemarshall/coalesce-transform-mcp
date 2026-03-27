# Node Operations

How to edit existing workspace nodes: join conditions, columns, config, renames, and SQL conversion.

## Applying Join Conditions

When `create_workspace_node_from_predecessor` returns `joinSuggestions` for a multi-predecessor node, you MUST set the join condition — without it the node fails at compile time.

> **Platform note**: Examples use Snowflake double-quote syntax (`"TABLE"."COLUMN"`). For Databricks use backticks, for BigQuery use backticks with project.dataset prefix. Determine platform first via `coalesce://context/sql-platform-selection`.

**Option A — Automatic (recommended for aggregations):**

Call `convert_join_to_aggregation` with `maintainJoins: true`. It reads predecessors, finds common columns, and generates the full JOIN ON clause automatically.

**Option B — Automatic (recommended for row-level joins):**

Call `apply_join_condition` — it reads predecessors, finds common columns, generates FROM/JOIN/ON with `{{ ref() }}` syntax, and writes the joinCondition to the node automatically:

```javascript
apply_join_condition({
  workspaceID, nodeID: "join-node-id",
  joinType: "LEFT JOIN"  // defaults to INNER JOIN
})
```

For mismatched column names across predecessors:

```javascript
apply_join_condition({
  workspaceID, nodeID: "join-node-id",
  joinType: "LEFT JOIN",
  joinColumnOverrides: [{
    leftPredecessor: "STG_CUSTOMER",
    rightPredecessor: "STG_ORDERS",
    leftColumn: "CUST_ID",
    rightColumn: "CUSTOMER_ID"
  }]
})
```

**Option C — Manual (when you need full control):**

Read each predecessor to get its `locationName`, then build the join condition manually:

```javascript
update_workspace_node({
  workspaceID, nodeID: "join-node-id",
  changes: {
    metadata: {
      sourceMapping: [{
        name: "JOIN_NODE_NAME",
        dependencies: [
          { locationName: "STAGING", nodeName: "STG_CUSTOMER" },
          { locationName: "STAGING", nodeName: "STG_ORDERS" }
        ],
        join: {
          joinCondition: 'FROM {{ ref(\'STAGING\', \'STG_CUSTOMER\') }} "STG_CUSTOMER"\nJOIN {{ ref(\'STAGING\', \'STG_ORDERS\') }} "STG_ORDERS"\n  ON "STG_CUSTOMER"."CUSTOMER_ID" = "STG_ORDERS"."CUSTOMER_ID"'
        },
        customSQL: { customSQL: "" }, aliases: {}, noLinkRefs: []
      }]
    }
  }
})
```

Always read `locationName` from `get_workspace_node` — never hardcode it.

**3+ table joins**: Chain JOIN clauses in the same joinCondition string. All dependencies go in one sourceMapping entry. Use `joinSuggestions` which returns common columns for each predecessor PAIR.

**Changing join type**: Read the existing sourceMapping, modify the JOIN keyword (e.g., `JOIN` -> `LEFT JOIN`), write back using read-modify-write pattern.

## Column Operations

### Replacing All Columns

Use `replace_workspace_node_columns`:

```javascript
replace_workspace_node_columns({
  workspaceID, nodeID: "node-id",
  columns: [
    { name: "CUSTOMER_ID" },  // passthrough — omit transform
    { name: "TOTAL_ORDERS", transform: 'COUNT(DISTINCT "STG_ORDER"."ORDER_ID")' }
  ]
})
```

### Adding a Column

`metadata.columns` is a full-replacement array. Read the current columns, append the new one, send the full array:

```javascript
replace_workspace_node_columns({
  workspaceID, nodeID: "node-id",
  columns: [...existingColumns, { name: "DISCOUNT_AMOUNT", transform: '"STG_ORDERS"."ORDER_TOTAL" * 0.1' }]
})
```

Only include `transform` on the new column if it has an actual transformation. Passthrough columns from `existingColumns` already have their transforms set by Coalesce.

### Resetting Columns to Match a Predecessor

Build passthrough columns from the predecessor — omit `transform` since these are all passthroughs:

```javascript
const resetColumns = predecessorColumns.map(col => ({
  name: col.name, dataType: col.dataType
}));
replace_workspace_node_columns({ workspaceID, nodeID: "node-id", columns: resetColumns })
```

Do NOT copy raw column objects — their `columnReference`, `sources`, and `columnID` belong to the predecessor.

### Custom Column Transforms

The `transform` field is the SQL expression for the column's SELECT clause.

**Passthrough columns**: If a column has no transformation (just passes through from the predecessor), **omit the `transform` field entirely**. Coalesce auto-populates passthrough transforms. Only specify `transform` when you are applying an actual transformation (UPPER, CAST, CASE, arithmetic, aggregation, etc.).

**Use hardcoded aliases, not `{{ ref() }}`**: In the `transform` field, use the table alias directly (e.g., `"STG_ORDERS"."PRICE"`). Do NOT use `{{ ref() }}` syntax in transforms — that syntax is for `joinCondition` only. The alias comes from the node's `joinCondition` (e.g., `FROM {{ ref('STAGING', 'STG_ORDERS') }} "STG_ORDERS"` means the alias is `"STG_ORDERS"`).

**Finding the correct alias**: Read the node and inspect `metadata.sourceMapping[].join.joinCondition`. If it contains `FROM {{ ref('RAW', 'CUSTOMER') }} "CUSTOMER"`, the alias is `"CUSTOMER"` (the source name), NOT the current node's name.

Common patterns:

```javascript
// Passthrough — omit transform entirely
{ name: "CUSTOMER_ID" }
// Computed
{ name: "LINE_TOTAL", transform: '"STG_ORDERS"."PRICE" * "STG_ORDERS"."QUANTITY"', dataType: "NUMBER(38,4)" }
// Type cast
{ name: "ORDER_DATE", transform: 'CAST("STG_ORDERS"."ORDER_DATE_STR" AS DATE)', dataType: "DATE" }
// Conditional
{ name: "ORDER_STATUS", transform: 'CASE WHEN "STG_ORDERS"."IS_CANCELLED" = TRUE THEN \'CANCELLED\' ELSE \'ACTIVE\' END', dataType: "VARCHAR" }
// String transform
{ name: "CITY", transform: 'UPPER("STG_LOCATION"."CITY")' }
```

**Aggregate vs scalar**: If a column uses an aggregate function, the joinCondition MUST include GROUP BY. Use `convert_join_to_aggregation` or add GROUP BY manually. Scalar transforms (CASE, CAST, arithmetic) work without GROUP BY.

### Bulk Column Operations

When renaming columns across a pipeline:

1. List all nodes and find affected ones
2. Map the dependency chain (upstream rename breaks downstream transforms)
3. Update bottom-up — upstream first, then fix downstream `transform` references and `joinCondition` ON clauses

IMPORTANT: Coalesce does NOT cascade column renames. You must update every downstream reference manually.

## Adding WHERE, QUALIFY, or GROUP BY

These go in the joinCondition, not in column transforms. Always use read-modify-write:

```javascript
// 1. Read current sourceMapping
get_workspace_node({ workspaceID, nodeID: "node-id" })

// 2. Append clause to existing joinCondition string
// e.g., add WHERE: existingJoinCondition + '\nWHERE "STG_ORDERS"."ORDER_DATE" >= \'2024-01-01\''
// e.g., add QUALIFY: existingJoinCondition + '\nQUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1'

// 3. Write back complete sourceMapping
update_workspace_node({
  workspaceID, nodeID: "node-id",
  changes: { metadata: { sourceMapping: [modifiedSourceMapping] } }
})
```

Preserve all existing fields (name, dependencies, aliases, customSQL, noLinkRefs) — only modify `join.joinCondition`.

## Column-Level Attributes (columnSelector)

Some node type config items are column-level, not node-level. They have `"type": "columnSelector"` in the node type definition.

**How to discover:** Look up the definition with `get_repo_node_type_definition`. Find items with `"type": "columnSelector"` and note the `attributeName`.

**How to set:**

```javascript
update_workspace_node({
  workspaceID, nodeID: "node-id",
  changes: {
    metadata: {
      columns: [
        { name: "CUSTOMER_ID", isBusinessKey: true, ... },
        { name: "ORDER_TOTAL", isChangeTracking: true, ... }
      ]
    }
  }
})
```

Common attributes:

| Node Type | attributeName | Purpose |
|-----------|--------------|---------|
| Dimension | `isBusinessKey` | Natural key column(s) |
| Dimension | `isChangeTracking` | Columns monitored for SCD Type 2 |
| Persistent Stage | `isBusinessKey` | Record uniqueness key |
| Persistent Stage | `isChangeTracking` | Columns to detect changes |
| Fact | `isBusinessKey` | Degenerate dimension / merge key |

Always look up actual attribute names from the node type definition — they vary by package.

## Common Config Fields

Set via `update_workspace_node({ changes: { config: { ... } } })`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `truncateBefore` | boolean | true (Stage) | Truncate before insert (full reload) |
| `testsEnabled` | boolean | true (Stage) | Enable data quality tests |
| `insertStrategy` | string | "INSERT" | Multi-source: "INSERT", "UNION", "UNION ALL" |
| `selectDistinct` | boolean | false | Apply DISTINCT to SELECT |
| `groupByAll` | boolean | false | GROUP BY ALL (mutually exclusive with selectDistinct) |
| `preSQL` | string | "" | SQL before main insert |
| `postSQL` | string | "" | SQL after main insert |

For node-type-specific fields, use `get_repo_node_type_definition` or inspect the definition in the repo under `nodeTypes/`.

## Adding a Predecessor to an Existing Node

1. Read the existing node to get current sourceMapping and columns
2. Read the new predecessor to get its columns and `locationName`
3. Update sourceMapping with the new dependency and extend joinCondition:

```javascript
update_workspace_node({
  workspaceID, nodeID: "existing-join-id",
  changes: {
    metadata: {
      sourceMapping: [{
        ...existingSourceMapping,
        dependencies: [...existingDependencies, { locationName: "STAGING", nodeName: "STG_RETURNS" }],
        join: {
          joinCondition: existingJoinCondition + '\nJOIN {{ ref(\'STAGING\', \'STG_RETURNS\') }} "STG_RETURNS"\n  ON "STG_ORDERS"."ORDER_ID" = "STG_RETURNS"."ORDER_ID"'
        }
      }]
    }
  }
})
```

4. Add columns from the new predecessor using `replace_workspace_node_columns`

Note: This updates metadata but does NOT create the DAG edge. The DAG link is established at creation time. The node may need to be recreated with the full set of `predecessorNodeIDs` if a true DAG predecessor is needed.

## Rename Safety

Renaming via `update_workspace_node({ changes: { name: "NEW_NAME" } })` updates the node itself (including its sourceMapping entry name), but does NOT update downstream nodes. After renaming:

1. Find downstream nodes whose `metadata.sourceMapping[].dependencies[].nodeName` references the old name
2. Update each downstream node's dependencies `nodeName` and `{{ ref() }}` calls in joinCondition
3. Update column transforms referencing the old name as a table alias

The same cascade applies to location changes.

## Duplicate Node Names

Nodes in the same storage location must have distinct names — duplicate names make `{{ ref() }}` ambiguous. Nodes in different locations CAN share a name.

## SQL-to-Graph Conversion

When a user pastes SQL for conversion into Coalesce nodes:

### SQL with CTEs

Create each CTE as a separate node bottom-up. Use `View` or `Work` for intermediate CTEs, the target type for the final query.

### Raw SQL (no ref syntax)

1. Identify table names in the SQL
2. Match to workspace nodes via `list_workspace_nodes` (case-insensitive)
3. Get each node's `locationName` via `get_workspace_node`
4. Pass the user's exact SQL unchanged to `plan_pipeline` or `create_pipeline_from_sql`
5. Do NOT rewrite table names into `{{ ref('LOCATION', 'NODE') }}` syntax for these pipeline tools — the planner resolves references automatically

### Large Queries (many columns)

Create the node first with `create_workspace_node_from_predecessor`, then set columns with `replace_workspace_node_columns`. Always send the full column array — arrays are replaced, not merged.

## Debugging Incorrect Data

When output data looks wrong after a successful run:

1. Read the node — check column `transform` expressions, joinCondition (join type, ON conditions), and dependencies
2. Common issues:
   - **Values too high**: Missing/incorrect JOIN ON (cartesian product), or LEFT JOIN that should be INNER
   - **Missing rows**: INNER JOIN filtering unmatched rows, or overly restrictive WHERE
   - **Duplicates**: Missing DISTINCT, wrong GROUP BY, or fan-out from one-to-many join
   - **NULLs**: LEFT JOIN on non-matching column, or missing COALESCE
3. Compare with predecessors to verify column names and data types match

## Exploring a Workspace

- **Summary**: `analyze_workspace_patterns` — package adoption, layers, methodology
- **All nodes**: `list_workspace_nodes` with `detail: true`
- **Specific node**: `get_workspace_node` — full body with columns, config, sourceMapping
- **Node types**: `list_workspace_node_types` — distinct types observed
- **Workspaces**: `list_workspaces` or `list_workspaces({ projectID })`
- **Environments**: `list_environments` — deployment targets (DEV, QA, PROD)

**Large workspaces (100+ nodes)**: Use `list_workspace_nodes` WITHOUT `detail` first, find target by name, then `get_workspace_node` on its ID. Use `cache_workspace_nodes` for repeated searches.

**Tracing lineage**: Read the node, find predecessors in `metadata.sourceMapping[].dependencies`, recurse until you reach nodes with no dependencies. For column-level lineage, inspect `metadata.columns[].sources[].columnReferences`.

**Downstream impact**: List workspace nodes and check which reference the target node in their dependencies. Recurse for the full impact chain.

## Related Resources

- `coalesce://context/pipeline-workflows` — building pipelines end-to-end
- `coalesce://context/node-creation-decision-tree` — which tool to use
- `coalesce://context/aggregation-patterns` — GROUP BY, datatype inference
- `coalesce://context/node-payloads` — full node body editing
- `coalesce://context/hydrated-metadata` — advanced metadata structures
