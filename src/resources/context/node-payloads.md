# Node Payloads

Use this resource when reading or writing full workspace node bodies.

## Core Rule

Treat workspace node bodies as structured objects with important differences between top-level fields, `metadata`, `config`, and arrays.

## Common Payload Areas

### Top-Level Fields

Common top-level fields include:
- `id`
- `name`
- `description`
- `nodeType`
- `database`
- `schema`
- `locationName`
- `storageLocations`
- `config`
- `metadata`

Not every node type uses every field.

Project policy:
- Do not send `overrideSQL`
- Do not send `override.*`
- If a node definition mentions `overrideSQLToggle`, treat it as disallowed and omit it from writes

### `metadata`

`metadata` usually contains node-shape details such as:
- `columns`
- lineage/source structures
- mapping-related structures

This is where many node-specific editing mistakes happen.

### `config`

`config` is node-type-specific operational configuration.

Examples include:
- `preSQL`
- `postSQL`
- `insertStrategy`
- node-type-specific dropdown or tabular settings

Do not assume the same config keys exist across node types.

### `storageLocations` and top-level location fields

Some nodes use `storageLocations`.
Some also expose location fields at the top level such as:
- `database`
- `schema`
- `locationName`

Verify the saved node body instead of assuming one location representation is always present.

## Array Safety

Arrays are replace-on-write unless the tool says otherwise.

This is especially important for:
- `metadata.columns`
- other metadata arrays
- `storageLocations`

If you send only the new array items, you will usually replace the old array.

## Tool Guidance

For tool selection and helper preference guidance, see `coalesce://context/node-creation-decision-tree`.

### Prefer `update-workspace-node`

Use it for most partial node edits because it:
- reads the current node first
- deep-merges object fields
- writes the merged full body back

### Use `set-workspace-node` carefully

Use `set-workspace-node` only when:
- you intend a full replacement
- you already have the complete final node body

### Scratch creation

For scratch creation guidance (use cases, completion levels, expected fields), see `coalesce://context/pipeline-workflows`.

## Validation Rules

After helper-based writes, inspect:
- `validation`
- `warning`

Do not assume a node is fully ready unless the helper says the requested completion was satisfied.

## Rename Safety

If a node name changes:
- verify the saved node body still looks correct
- verify related mapping/source fields still make sense
- do not assume downstream references were updated automatically

## Related Resources

- `coalesce://context/hydrated-metadata`
- `coalesce://context/storage-mappings`
- `coalesce://context/node-creation-decision-tree`
