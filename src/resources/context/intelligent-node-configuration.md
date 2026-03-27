# Intelligent Node Configuration

## Overview

The intelligent node configuration system automatically completes all required and contextual config fields based on node type schemas and node analysis.

## Tools

### `complete_node_configuration`

Standalone tool that completes config for any workspace node.

**Input:**
- `workspaceID`: Workspace containing the node
- `nodeID`: Node to configure
- `repoPath`: (Optional) Path to committed repo for accurate schema resolution

**Process:**
1. Fetches current node
2. Resolves node type schema (repo → corpus)
3. Analyzes node context (columns, sources, joins)
4. Classifies config fields (required, conditional, optional, contextual)
5. Applies intelligence rules
6. Updates node with complete config

**Output:**

- `node`: Updated node with complete config
- `configChanges`: What was changed (required, contextual, defaults, preserved)
- `configReview`: Status summary of the node's configuration
  - `status`: `complete` | `needs_attention` | `incomplete`
  - `summary`: Human-readable status description
  - `missingRequired`: Required fields or columnSelectors still unset
  - `warnings`: Issues needing manual review (e.g., missing business keys on dimension/fact nodes)
  - `suggestions`: Optional improvements (e.g., change tracking, materialization changes)
- `columnAttributeChanges`: Column-level attributes applied (isBusinessKey, isChangeTracking)
- `reasoning`: Why each decision was made

### `convert_join_to_aggregation`

Enhanced with automatic config completion. After transformation, automatically calls `complete_node_configuration` to fill all remaining config fields.

**New output field:** `configCompletion` with changes and analysis

## Intelligence Rules

### Multi-Source Strategy

**Trigger:** Node has multiple sources

**Action:**

- If aggregates present: `insertStrategy: "UNION"` (deduplication)
- If no aggregates: `insertStrategy: "UNION ALL"` (performance)

### Aggregation Compatibility

**Trigger:** Node has aggregate columns (COUNT, SUM, AVG, etc.)

**Action:**

- `selectDistinct: false` (incompatible with aggregates)

### View Materialization

**Trigger:** Node materialized as a view without aggregates

**Action:**

- `selectDistinct: false` (default; set to true only if deduplication is needed)

### Table Materialization

**Trigger:** Node materialized as a table

**Action:**

- `truncateBefore: false` (safe default to preserve existing data)

### Timestamp Column Detection

**Trigger:** Columns matching `*_TS`, `*_DATE`, `*_TIMESTAMP` patterns

**Action:**

- Documents candidates in `detectedPatterns.candidateColumns`
- Does NOT auto-enable `lastModifiedComparison` (user choice)
- If a table has NO timestamp/date columns, a reasoning note suggests adding audit columns

### Type 2 SCD Detection

**Trigger:** Columns include START_DATE/EFFECTIVE_DATE, END_DATE/EXPIRY_DATE, and IS_CURRENT/CURRENT_FLAG

**Action:**

- Documents detection in reasoning
- Does NOT auto-enable SCD config (requires verification)

## Schema Resolution

Priority order:
1. **Repo-backed** (if `repoPath` provided) - most accurate
2. **Corpus** (fallback) - standard Coalesce node types
3. **Error** (if neither available)

## Preservation Rules

**Never overwrite:**
- Existing non-null config values (except required fields)
- User-set fields take precedence

**Always set:**
- Required fields (even if overwriting)
- Aggregation-specific fields from transformation

**Smart merge:**
- If field is empty/null, set it
- If field is default and context suggests better value, update it

## Usage Examples

### Complete Existing Node

```typescript
await completeNodeConfiguration(client, {
  workspaceID: "ws-123",
  nodeID: "dim-customers",
  repoPath: "/path/to/repo"
});
```

### Transform with Auto-Config

```typescript
await convertJoinToAggregation(client, {
  workspaceID: "ws-123",
  nodeID: "fact-orders",
  groupByColumns: ['"ORDERS"."CUSTOMER_ID"'],
  aggregates: [
    { name: "TOTAL", function: "COUNT", expression: "*" }
  ],
  maintainJoins: true,
  repoPath: "/path/to/repo"
});

// Returns fully transformed AND configured node
```

## Troubleshooting

**"Cannot resolve node type schema":**
- Ensure package is committed to repo or available in corpus
- Check nodeType format (may need package prefix like "PackageName:::ID")

**"Required field could not be auto-determined":**
- Some required fields need manual input
- Check `configReview.missingRequired` for details

**Config not as expected:**
- Check `configChanges.preserved` — may have existing values that weren't overwritten
- Check `configReview.warnings` and `configReview.suggestions` for actionable guidance
- Verify node context (sources, columns, materialization type)
