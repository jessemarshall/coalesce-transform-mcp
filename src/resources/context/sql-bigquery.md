# SQL Rules: BigQuery

This guidance is based on the AI runtime platform rules for BigQuery and the related Coalesce SQL rendering behavior.

## Default SQL Style

- Prefer lowercase identifiers.
- Use backticks to quote identifiers when needed.
- For raw physical references, prefer fully qualified `project.dataset.table` paths and quote the entire path with one pair of backticks when quoting is necessary.
- Avoid `SELECT *`; prefer explicit column lists.

## Coalesce `ref()` Syntax

- In Coalesce node SQL, node-to-node dependencies should stay in logical ref form:

```jinja
FROM {{ ref('sample', 'nation') }} nation
LEFT JOIN {{ ref('storage_location', 'stg_foo') }} stg_foo
  ON nation.id = stg_foo.nation_id
```

- The SQL processing layer treats `ref()` arguments as exact `locationName` and `nodeName` values.
- Keep `ref()` arguments aligned with the saved Coalesce names, even when the rest of the SQL follows BigQuery lowercase conventions.
- For more on logical locations, use `coalesce://context/storage-mappings`.

## Physical Object Names

- When Coalesce renders physical database and schema locations for BigQuery, generated physical references may appear in backtick form such as `` `project`.`dataset`. ``
- Preserve that generated style when editing SQL that already includes physical references from Coalesce metadata or templates.

## Common Source-Backed Functions

- String: `split`, `substr`, `upper`, `lower`, `regexp_extract`, `replace`
- Date: `date_trunc`, `timestamp_trunc`, `date_add`, `date_sub`, `timestamp_add`, `timestamp_diff`
- Aggregate: `sum`, `count`, `avg`, `min`, `max`, `any_value`, `string_agg`, `array_agg`

## Avoid

- Do not replace Coalesce `ref()` references with raw project.dataset.table paths unless the user explicitly wants physical SQL.
- Do not switch BigQuery identifier quoting to Snowflake-style double quotes.
- Do not introduce wildcard projections when explicit columns are practical.
