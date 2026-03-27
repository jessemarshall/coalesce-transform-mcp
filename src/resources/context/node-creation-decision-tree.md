# Node Creation Decision Tree

Use this resource before creating or heavily editing workspace nodes.

## Before You Start

**Always check observed node types first:**

1. Call `coalesce_list_workspace_node_types` to discover which node types are already observed in current workspace nodes
2. Do not treat that scan as a true installed-type registry; if the desired node type is unobserved, confirm installation in the Coalesce UI before proceeding
3. If a local committed repo is available, install the package in Coalesce, commit the workspace branch, update the local clone, then use repo-aware tools with explicit `repoPath` or the `COALESCE_REPO_PATH` fallback
4. If the repo does not contain the committed definition, consult `coalesce://context/node-type-corpus` for fallback patterns and metadata structure

---

## Choose the Right Tool

### Step 1: Always plan first to discover the correct node type

**Before creating any node**, call `coalesce_plan_pipeline` to discover and rank available node types from the repo:

```javascript
coalesce_plan_pipeline({
  workspaceID,
  goal: "stage truck data",
  sourceNodeIDs: ["source-id-1"],
  repoPath: "/path/to/repo"  // or rely on COALESCE_REPO_PATH
})
```

The planner scans the repo for all committed node type definitions, scores them against your use case, and returns `nodeTypeSelection.consideredNodeTypes` with the best match. **Use the `nodeType` from the plan** when calling `coalesce_create_workspace_node_from_predecessor` — do not guess node types like "Stage" or "View".

If the user provides SQL, pass it directly:

```javascript
coalesce_plan_pipeline({ workspaceID, sql: "<USER-PROVIDED SQL HERE>" })
```

Do NOT author SQL yourself to pass to these tools.

### Step 2: Create the node with the planned node type

Use `coalesce_create_workspace_node_from_predecessor` with the node type from the plan:

```javascript
coalesce_create_workspace_node_from_predecessor({
  workspaceID,
  nodeType: "base-nodes:::Stage",  // from coalesce_plan_pipeline result
  predecessorNodeIDs: ["source-id-1"],
  changes: { name: "STG_CUSTOMER" }
})
```

Good fit:
- stage from a source
- join from multiple upstream nodes
- transform node that should inherit upstream columns

After the call:
- inspect `validation`
- inspect `joinSuggestions`
- inspect `nodeTypeValidation.warning` — if present, the node type may be wrong for this use case
- stop if `warning` is present
- for validation field details, see `coalesce://context/node-payloads`

For joins and aggregations, follow up with `coalesce_convert_join_to_aggregation`.

**IMPORTANT — Column handling in `changes`:**

When creating from predecessor, columns are **auto-populated** with proper source linkage (`sources`, `columnReference`). Only include columns in `changes.metadata.columns` that have **actual transforms** (UPPER, CAST, CASE, aggregation, derived expressions). Do NOT include passthrough columns — they already exist with correct source references.

If the user's SQL has 10 columns but only 3 have transforms, create the node first (columns auto-populate), then use `coalesce_replace_workspace_node_columns` to add the 3 transformed columns plus any new derived columns. This preserves source linkage for all passthrough columns.

### If the user provides SQL to convert

- prefer `coalesce_plan_pipeline` with the user's SQL to preview the plan first
- prefer `coalesce_create_pipeline_from_sql` with the user's SQL for one-step conversion
- prefer `coalesce_create_pipeline_from_plan` when a plan has already been reviewed and approved

Do NOT author SQL yourself to pass to these tools.

### If the node already exists and only part of it should change

Use `coalesce_update_workspace_node`.

Good fit:
- descriptions
- config updates
- top-level location fields
- full replacement of `metadata.columns`

### If you intentionally want to replace the full node body

Use `coalesce_set_workspace_node`.

Only do this when you already have the exact full node body to persist.

## Multi-Predecessor and Join Requests

When the user wants a join:

1. Read the predecessor nodes if you need more context.
2. Use `coalesce_create_workspace_node_from_predecessor`.
3. Inspect `joinSuggestions` for common column names.
4. Confirm `validation.allPredecessorsRepresented` and inspect `validation.predecessorCoverage` before assuming the join node is ready.

**CRITICAL: The node is NOT complete after step 2.** Multi-predecessor nodes are created with columns but NO join condition. You MUST complete the join setup:

5. **Review `nextSteps`** in the response — it contains context-aware guidance for your specific node.
6. **Set up the join condition** by calling one of:
   - `coalesce_convert_join_to_aggregation` — for GROUP BY / fact table / aggregation use cases
   - `coalesce_apply_join_condition` — for row-level joins (auto-generates FROM/JOIN/ON with `{{ ref() }}` syntax)
   - `coalesce_update_workspace_node` — to set joinCondition manually when you need full control
7. **Verify the join** — call `coalesce_get_workspace_node` to confirm the joinCondition is set and columns are correct.

**Join type selection:**

| Scenario | Join Type | When to Use |
|----------|-----------|-------------|
| Every record must exist in both tables | `INNER JOIN` | Matching orders to known customers |
| Keep all from primary, allow nulls from secondary | `LEFT JOIN` | All customers, even those with no orders |
| Keep all from both, allow nulls on either side | `FULL OUTER JOIN` | Reconciliation between two systems |

**Join key verification:**
- Use **business keys** (e.g., CUSTOMER_ID, ORDER_NUMBER), not surrogate keys
- Confirm at least one side of the join is unique on the join key to avoid fan-out (row multiplication)
- If join keys have different names across predecessors (e.g., `CUST_ID` vs `CUSTOMER_ID`), the agent must map them explicitly

## Node Configuration Is Automatic

When you use `coalesce_create_workspace_node_from_predecessor` or `coalesce_create_workspace_node_from_scratch` with `repoPath`, node configuration is completed automatically:

- Node-level config defaults are applied from the node type definition
- Column-level attributes (`isBusinessKey`, `isChangeTracking`, etc.) are inferred and set
- The `configCompletion` field in the response shows exactly what was applied

If `repoPath` is not provided or config completion fails, the response includes `configCompletionSkipped` — call `coalesce_complete_node_configuration` with `repoPath` to retry.

**Always use `coalesce_create_workspace_node_from_predecessor` or `coalesce_create_workspace_node_from_scratch`** — they handle validation and config completion automatically.

## Storage and SQL Follow-Up

After creation:

1. Determine the SQL platform with `coalesce://context/sql-platform-selection`.
2. Verify storage and `{{ ref(...) }}` assumptions with `coalesce://context/storage-mappings`.
3. For payload-heavy edits, use:
  - `coalesce://context/node-payloads`
  - `coalesce://context/hydrated-metadata`

## Related Resources

- `coalesce://context/pipeline-workflows`
- `coalesce://context/node-operations`
- `coalesce://context/data-engineering-principles`
- `coalesce://context/node-payloads`
