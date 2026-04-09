# Hydrated Metadata

Use this resource when the user wants to provide or edit raw node `metadata`, `config`, or `storageLocations`.

## What This Covers

Coalesce hydrated node bodies commonly include:
- `metadata.columns`
- `sources`
- `config`
- `storageLocations`

The exact structure varies by node type and configuration.

## Practical Summary

### `metadata.columns`

Columns map to the Mapping Grid in Coalesce.

Common fields on a column include:
- `id`
- `name`
- `dataType`
- `description`
- `nullable`
- `defaultValue`
- `tests`
- `transform`

Columns can also contain lineage-related source information.

### `sources`

Hydrated source metadata can include:
- source `name`
- source `columns`
- source `join`
- `dependencies`

This is useful for multisource and join-oriented nodes.

### `config`

Hydrated config can contain both simple scalar fields and nested structures.

Examples include:
- `preSQL`
- `postSQL`
- `insertStrategy`
- booleans
- dropdown values
- nested tabular config items
- config entries that themselves reference column-like objects

Treat config as node-type-specific. Preserve unknown keys unless the user intends to replace them.

### `storageLocations`

Hydrated storage locations are arrays of objects with fields such as:
- `database`
- `schema`
- `name`

Do not assume storage location names can be normalized for SQL style. They must match the actual Coalesce objects.

## Editing Rules

- Prefer `update_workspace_node` for partial changes.
- Treat arrays as full-replacement fields (see `coalesce://context/node-payloads` for array safety details).
- Preserve existing hydrated structures you are not intentionally changing.
- When working from scratch, provide `metadata.columns` explicitly if the user expects a configured node.

## When To Use Raw Hydrated Input

Use raw hydrated structures when:
- the user already knows the exact payload shape they want
- the node type has custom nested config
- you need to preserve advanced lineage or source structures

If the user only wants a normal create/update flow, prefer the higher-level helpers and simpler fields first.

## Official Reference

See the Coalesce documentation for the fuller field inventory:
- [Hydrated Metadata](https://docs.coalesce.io/docs/build-your-pipeline/user-defined-nodes/hydrated-metadata)
- [Hydrated Metadata Reference](https://docs.coalesce.io/docs/build-your-pipeline/user-defined-nodes/hydrated-metadata-reference)

## Related Resources

- `coalesce://context/node-payloads`
- `coalesce://context/storage-mappings`
