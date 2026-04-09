# Storage Locations and References

This guidance is based on the AI runtime storage-context section and the related Coalesce ref-handling code.

## What Storage Locations Mean

- Storage locations are short logical names that map to `DATABASE.SCHEMA` pairs where tables live.
- Users may have many storage locations configured in a workspace.
- Any node can be manually configured to live somewhere other than the obvious default, so always pay attention to the existing node storage locations when writing joins or downstream refs.

## Default Placement Matters

- New nodes normally default to the storage location specified by their node type.
- That default matters when you create multiple nodes at once or in parallel because downstream refs may need to target the new node's default location, not the source node's location.

## Source Example

- You create a chain `A -> B -> C`
- `A` already exists in `SOURCE_A`
- `B` is created and defaults to `USER_WORKSPACE`
- `C` is created at the same time and also defaults to `USER_WORKSPACE`

That means:

- `B` should reference `A` with `{{ ref('SOURCE_A', 'ORDERS') }}`
- `C` should reference `B` with `{{ ref('USER_WORKSPACE', 'NODE_B') }}`

## `ref()` Contract

- Coalesce node SQL uses logical refs in this shape:

```jinja
FROM {{ ref('LOCATION_NAME', 'NODE_NAME') }}
```

- The SQL processing layer extracts refs into exact `locationName` and `nodeName` pairs.
- Keep `ref()` arguments aligned with the saved Coalesce names.

## Practical Rules

- Treat `locationName` as logical Coalesce state and `database` or `schema` as the resolved physical target behind it.
- When chaining node creation, confirm where the upstream node was actually placed before referencing it.
- If storage mappings are missing, fix that first instead of guessing raw warehouse paths.

## Avoid

- Do not guess logical location names from database or schema values.
- Do not assume new nodes land in the same location as their predecessors.
- Do not normalize `ref()` arguments to warehouse casing rules.
