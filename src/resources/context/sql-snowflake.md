# SQL Rules: Snowflake

This guidance is based on the AI runtime platform rules for Snowflake and the related Coalesce SQL rendering behavior.

## Default SQL Style

- Use uppercase unquoted identifiers by default — this applies to **node names**, column names, aliases, and all SQL identifiers.
- Node names become Snowflake table/view names. Always use UPPERCASE: `STG_LOCATION`, `FACT_ORDERS`, `DIM_CUSTOMER`.
- Use double quotes only when you must preserve exact case or quote a reserved word.
- Column names and aliases should normally be uppercase and unquoted.
- Table aliases should normally be uppercase and unquoted.
- Avoid backticks, single-quoted identifiers, or mixed quoting styles.

## Coalesce `ref()` Syntax

- Coalesce node-to-node SQL should use logical refs:

```jinja
FROM {{ ref("SAMPLE", "NATION") }} N
LEFT JOIN {{ ref("STORAGE_LOCATION", "STG_FOO") }} F
  ON N.ID = F.NATION_ID
```

- The SQL processing layer treats `ref()` arguments as exact `locationName` and `nodeName` values.
- Keep `ref()` arguments aligned with the saved Coalesce names, even when the rest of the SQL follows Snowflake casing preferences.
- For more on logical locations, use `coalesce://context/storage-mappings`.

## Physical Object Names

- When Coalesce renders physical database and schema locations, Snowflake object names may appear in double-quoted form such as `"DB"."SCHEMA".`
- Preserve that generated style when editing SQL that already includes physical object references from Coalesce metadata or templates.

## Common Source-Backed Functions

- String: `SPLIT`, `SUBSTR`, `UPPER`, `LOWER`, `REGEXP_SUBSTR`
- Date: `DATEADD`, `DATEDIFF`, `TO_DATE`, `DATE_TRUNC`
- Aggregate: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `MEDIAN`

## Avoid

- Do not rewrite `ref()` arguments into raw warehouse object paths.
- Do not switch Snowflake identifier quoting to backticks.
- Do not introduce mixed-case quoted identifiers unless preserving exact case is required.
