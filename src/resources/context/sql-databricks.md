# SQL Rules: Databricks

This guidance is based on the AI runtime platform rules for Databricks and the related Coalesce SQL rendering behavior.

## Default SQL Style

- Table references should use backticks rather than double quotes.
- Use lowercase identifiers for SQL you introduce.
- Use clear naming prefixes such as `stg_`, `dim_`, and `fct_` when naming new objects.

## Coalesce `ref()` Syntax

- Coalesce node-to-node SQL should use logical refs:

```jinja
FROM {{ ref('sample', 'nation') }} `nation`
JOIN {{ ref('storage_location', 'stg_foo') }} `stg_foo`
```

- The SQL processing layer treats `ref()` arguments as exact `locationName` and `nodeName` values.
- Keep `ref()` arguments aligned with the saved Coalesce names, even when the rest of the SQL follows Databricks lowercase conventions.
- For more on logical locations, use `coalesce://context/storage-mappings`.

## Physical Object Names

- When Coalesce renders physical database and schema locations for Databricks, object names appear in backtick form such as `` `catalog`.`schema`. `` or `` `db`.`schema`. ``
- Preserve that generated style when editing SQL that already includes physical references from Coalesce metadata or templates.

## Common Source-Backed Functions

- String: `split`, `regexp_extract`, `initcap`, `translate`, `reverse`
- Date: `date_trunc`, `add_months`, `months_between`, `next_day`, `current_date`
- Array: `explode`, `array_contains`, `size`, `slice`, `posexplode`
- Aggregate: `collect_list`, `collect_set`, `count_distinct`, `percentile_approx`, `any_value`

## Avoid

- Do not rewrite `ref()` arguments into physical warehouse paths.
- Do not use Snowflake double quotes for Databricks identifiers.
- Do not mix uppercase Snowflake-style aliasing with Databricks lowercase conventions unless the saved names require it.
