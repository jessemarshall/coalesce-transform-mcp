# Node Type Discovery and Corpus

## Prefer Repo-Backed Discovery First

**BEFORE creating or editing nodes:**
1. Determine which node types are already observed in the workspace
2. If a local committed repo is available, use repo-backed discovery with explicit `repoPath` or the `COALESCE_REPO_PATH` fallback
3. Use the corpus only when the repo is unavailable or lacks the committed definition

Repo-backed workflow:
- Install the package in Coalesce
- Commit the workspace branch
- Update the local repo clone to that branch
- Use `coalesce_list_repo_packages`, `coalesce_list_repo_node_types`, `coalesce_get_repo_node_type_definition`, or `coalesce_generate_set_workspace_node_template`
- Prefer explicit `repoPath` on repo-aware calls; when omitted, tools fall back to `COALESCE_REPO_PATH`
- There is no server-wide repo mapping in v1

## Repo-Aware Tools

### `coalesce_list_repo_packages`
Inspect committed `packages/*.yml`
Use this to discover exact package aliases and see which enabled node-type IDs have committed definitions

### `coalesce_list_repo_node_types`
List exact resolvable node-type identifiers from committed `nodeTypes/`
Use this to confirm the exact direct identifier or `alias:::id` value before generating templates

### `coalesce_get_repo_node_type_definition`
Resolve one exact node type from the committed repo
Returns the parsed outer definition plus raw and parsed `metadata.nodeMetadataSpec`

### `coalesce_generate_set_workspace_node_template`
Generate a `coalesce_set_workspace_node` body template from either:
- a raw definition object
- a committed repo definition resolved by `repoPath` + `nodeType`

In repo mode, this preserves the exact resolved node type, including package-backed identifiers like `alias:::id`

## Corpus Tools

### `coalesce_search_node_type_variants`
Search for node type families (e.g., "stage", "dimension")
Returns variants with their config schema and metadata examples

### `coalesce_get_node_type_variant`
Get exact variant definition by key
Use this to get the authoritative structure for a node type

### `coalesce_list_workspace_node_types` *(NEW)*
Scan workspace nodes and return distinct observed node types
Use this to inspect current workspace usage and exact identifiers before recommending
The response includes `basis: "observed_nodes"` to make the contract explicit

## Node-Type-Specific Patterns

Project policy:
- Never recommend or set SQL override fields
- Ignore or remove `overrideSQLToggle`, `overrideSQL`, and `override.*` when reading node-type definitions
- Prefer native node configuration and metadata patterns instead of SQL override

The corpus contains node-type-specific patterns and configurations. These vary by:
- Package source (Coalesce built-in vs community packages)
- Node type family (Stage, Dimension, Fact, View, etc.)
- Package version and variant

**When to consult corpus patterns:**
- Creating nodes with complex metadata (sources, joins, config)
- Applying node-type-specific logic (materialization, caching, partitioning)
- Understanding required vs optional config fields
- Adapting SQL patterns for specific node types

The corpus provides real-world examples from actual node type source code, including:
- Metadata structure (columns, sources, mappings)
- Config field schemas and valid values
- SQL patterns and Jinja syntax specific to that node type
- Storage location and reference patterns

## Column-Level Attributes (columnSelector)

Node type definitions contain config items with `"type": "columnSelector"`. These are **column-level attributes** — boolean flags set directly on individual column objects in `metadata.columns`, NOT in the node-level `config` object.

**How it works:**

1. The node type definition (in `nodeTypes/` in the local repo) has a config item like:
   ```json
   { "displayName": "Business Key", "attributeName": "isBusinessKey", "type": "columnSelector" }
   ```
2. To activate it, set the `attributeName` as a boolean on the column:
   ```json
   { "name": "CUSTOMER_ID", "dataType": "NUMBER(38,0)", "isBusinessKey": true, "transform": "..." }
   ```
3. The node type's Jinja templates reference it: `columns | selectattr('isBusinessKey')`

**Discovery workflow:**

1. Use `coalesce_get_repo_node_type_definition` to read the node type definition from the local repo
2. Find config items where `type` is `"columnSelector"`
3. The `attributeName` field tells you what property to set on columns
4. Set `attributeName: true` on the appropriate columns via `coalesce_update_workspace_node` or `coalesce_replace_workspace_node_columns`

**Important:** Attribute names vary by node type and package. Always look them up in the actual node type definition — do not guess or hardcode attribute names.

## Workflow

1. **Discover observed types:** `coalesce_list_workspace_node_types`
2. **Check if recommended type is already observed:** Compare to workspace types
3. **If unobserved:** Do not claim it is unavailable; confirm installation in the UI before proceeding
4. **If a committed local repo is available:** Use repo-aware tools with `repoPath` or `COALESCE_REPO_PATH`
5. **If repo resolution fails or the definition is missing:** Use `coalesce_search_node_type_variants` and `coalesce_get_node_type_variant`
6. **Adapt example:** Replace placeholder values with user-specific data
7. **Create/update:** Use appropriate tool with correct structure

## Node Type Availability

If a recommended node type is not observed in current workspace nodes:
1. Do not claim the type is unavailable
2. Explain that the workspace scan only reflects existing nodes, not a true installed-type registry
3. Confirm package installation in the Coalesce UI when the exact availability is uncertain
4. Prefer repo-backed discovery to recover the exact identifier before proceeding

## Node Type Format

Node types can appear in two formats:

1. **Simple format:** Direct node type names without package prefix
   - Examples: `"Stage"`, `"persistentStage"`, `"View"`
   - Used for built-in node types or custom node types in the repo

2. **Package-prefixed format:** Package name followed by `:::` and node type ID
   - Examples: `"IncrementalLoading:::230"`, `"Databricks-Incremental-nodes:::278"`
   - Used for package-installed node types from published packages
   - The format is: `"PackageName:::NodeTypeID"`

### Creating Nodes with Package-Prefixed Types

When creating nodes, you can use **either format**:

- Full format: `nodeType: "IncrementalLoading:::230"`
- Bare ID: `nodeType: "230"` (will match any package with that ID)

Prefer the full package-prefixed format when you know it. Using just the numeric ID (e.g., `"230"`) is safest when a matching package-prefixed type is already observed in workspace nodes.

### Discovering Node Type Format

Use `coalesce_list_workspace_node_types` to see the exact format of observed node types. The response will show package-prefixed types like `"IncrementalLoading:::230"` when those identifiers are already present in current workspace nodes.

## Template Usage

### Template Generation Hierarchy

1. **For committed local repo definitions:** Use `coalesce_generate_set_workspace_node_template` in repo mode
   - Resolves the exact committed definition from `repoPath` or `COALESCE_REPO_PATH`
   - Preserves exact direct or package-backed node-type identifiers
   - Best choice when the local repo contains the definition

2. **For known corpus variants:** Use `coalesce_get_node_type_variant`
   - Returns exact variant definition from the committed corpus snapshot
   - Best fallback when repo-backed resolution is unavailable

3. **For generating templates from corpus variants:** Use `coalesce_generate_set_workspace_node_template_from_variant`
   - Converts a corpus variant into an editable YAML-friendly template
   - Use this when the repo does not contain the committed definition

4. **For discovery:** Search first, then get variant
   - Use `coalesce_search_node_type_variants` to find the right fallback variant
   - Then use `coalesce_get_node_type_variant` or `coalesce_generate_set_workspace_node_template_from_variant`
