# Aggregation and GROUP BY Patterns

## Automatic JOIN ON Generation

When creating or converting multi-predecessor nodes, the system analyzes common columns between predecessors and generates JOIN ON clauses.

1. **Column Analysis**: Compares columns from each predecessor pair
2. **Normalization**: Case-insensitive column name matching
3. **SQL Generation**: Produces FROM/JOIN/ON clauses

Example: predecessors ORDERS (ORDER_ID, CUSTOMER_ID) and CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME) produce:

```sql
FROM "ORDERS"
INNER JOIN "CUSTOMERS"
  ON "ORDERS"."CUSTOMER_ID" = "CUSTOMERS"."CUSTOMER_ID"
```

## Automatic Datatype Inference

The system infers datatypes from transform expressions:

| Transform Pattern | Inferred Datatype |
|-------------------|-------------------|
| `COUNT(...)` | `NUMBER` |
| `SUM(...)` | `NUMBER(38,4)` |
| `AVG(...)` | `NUMBER(38,4)` |
| `MIN/MAX(..._TS)` | `TIMESTAMP_NTZ(9)` |
| `MIN/MAX(..._DATE)` | `DATE` |
| `DATEDIFF(...)` | `NUMBER` |
| `CURRENT_DATE` | `DATE` |
| `CURRENT_TIMESTAMP` | `TIMESTAMP_NTZ(9)` |
| `ROW_NUMBER()` | `NUMBER` |
| `CONCAT(...)` | `VARCHAR` |

## GROUP BY Analysis and Validation

The system automatically detects aggregate functions, identifies non-aggregate columns needing GROUP BY, validates coverage, and generates the GROUP BY clause.

### Detection Rules

**Aggregate functions** (column goes into aggregate list):
`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`, `LISTAGG`, `ARRAY_AGG`

**Window functions** (column goes into aggregate list):
`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LEAD`, `LAG`, `FIRST_VALUE`, `LAST_VALUE`

**Non-aggregate columns** (must be in GROUP BY):
Simple references (`"TABLE"."COLUMN"`), expressions without aggregation (`UPPER("TABLE"."NAME")`)

### Validation

Non-aggregate columns are automatically included in GROUP BY, and pure-aggregate queries (all columns use aggregate/window functions) are valid without GROUP BY — the entire result set is a single group.

## Common Patterns

### Customer Lifetime Metrics

```javascript
{
  groupByColumns: ['"ORDERS"."CUSTOMER_ID"'],
  aggregates: [
    { name: "TOTAL_ORDERS", function: "COUNT", expression: 'DISTINCT "ORDERS"."ORDER_ID"' },
    { name: "LIFETIME_VALUE", function: "SUM", expression: '"ORDERS"."ORDER_TOTAL"' },
    { name: "AVG_ORDER_VALUE", function: "AVG", expression: '"ORDERS"."ORDER_TOTAL"' },
    { name: "FIRST_ORDER_DATE", function: "MIN", expression: '"ORDERS"."ORDER_TS"' },
    { name: "LAST_ORDER_DATE", function: "MAX", expression: '"ORDERS"."ORDER_TS"' },
    { name: "DAYS_SINCE_LAST", function: "DATEDIFF", expression: 'day, MAX("ORDERS"."ORDER_TS"), CURRENT_DATE()' }
  ]
}
```

### Daily Sales Summary

```javascript
{
  groupByColumns: [
    'DATE_TRUNC(\'day\', "ORDERS"."ORDER_TS")',
    '"ORDERS"."LOCATION_ID"'
  ],
  aggregates: [
    { name: "DAILY_ORDERS", function: "COUNT", expression: 'DISTINCT "ORDERS"."ORDER_ID"' },
    { name: "DAILY_REVENUE", function: "SUM", expression: '"ORDER_DETAIL"."LINE_TOTAL"' },
    { name: "AVG_ORDER_SIZE", function: "AVG", expression: '"ORDER_DETAIL"."QUANTITY"' }
  ]
}
```

### Product Category Performance

```javascript
{
  groupByColumns: ['"PRODUCTS"."CATEGORY"', '"PRODUCTS"."SUBCATEGORY"'],
  aggregates: [
    { name: "TOTAL_SALES", function: "SUM", expression: '"ORDERS"."AMOUNT"' },
    { name: "UNITS_SOLD", function: "SUM", expression: '"ORDERS"."QUANTITY"' },
    { name: "UNIQUE_CUSTOMERS", function: "COUNT", expression: 'DISTINCT "ORDERS"."CUSTOMER_ID"' },
    { name: "AVG_PRICE", function: "AVG", expression: '"ORDERS"."UNIT_PRICE"' }
  ]
}
```

## groupByColumns is Analysis Data Only

**CRITICAL**: The `groupByColumns` field returned by `convert_join_to_aggregation` is for analysis only. It must NEVER be included in node metadata sent to the Coalesce API.

The Coalesce API rejects it with: `"request/body must NOT have additional properties"`

Our tools (`convert_join_to_aggregation`, `replace_workspace_node_columns`, `update_workspace_node`) automatically strip `groupByColumns` from metadata. But if you call `set_workspace_node` with a body containing `groupByColumns` in metadata, you'll get an error.

## Automatic Config Completion

`convert_join_to_aggregation` automatically completes config fields after transformation:

### Column-Level Attributes

- `isBusinessKey: true` on GROUP BY columns (dimensions)
- `isChangeTracking: true` on aggregate columns (measures)

These are column-level attributes set directly on each column object. See `coalesce://context/node-operations` for details on columnSelector attributes.

### Node-Type-Aware Config

Automatically set based on context:

- `selectDistinct: false` (incompatible with aggregates)
- `truncateBefore: false` (table materialization default)
- `insertStrategy`: based on multi-source detection

See `coalesce://context/intelligent-node-configuration` for complete details.

## Tips

1. Always use fully-qualified column names: `"TABLE"."COLUMN"`
2. Check `groupByClause` in the response for the generated GROUP BY
3. Use `maintainJoins: true` for automatic JOIN ON generation
4. Let datatype inference work — don't manually specify unless needed
5. Review `joinSQL.fullSQL` for the generated SQL
6. Never include groupByColumns in metadata sent to the API

## Related Resources

- `coalesce://context/pipeline-workflows` — using aggregation in pipelines
- `coalesce://context/node-operations` — column-level attributes, config fields
- `coalesce://context/intelligent-node-configuration` — config completion details
