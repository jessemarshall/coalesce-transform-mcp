# Run Diagnostics Guide

This guide explains how to use the `diagnose-run-failure` tool to analyze and fix failed Coalesce runs.

## When to Use

Use `diagnose-run-failure` when:
- A run has failed and the user wants to understand why
- The user reports errors after a deploy or refresh
- You need to determine whether to retry or fix before re-running
- Multiple nodes failed and you need to prioritize fixes

Do NOT use when:
- The run is still in progress — use `run-status` instead
- You just need raw run data — use `get-run-details` instead
- The user wants to start a new run — use `run-and-wait` instead

## How It Works

The tool:
1. Fetches run metadata and per-node results in parallel
2. Classifies each node failure into a category
3. Generates specific fix suggestions per failure
4. Produces prioritized recommendations

## Failure Categories

| Category | What It Means | Common Causes |
|---|---|---|
| `sql_error` | Invalid SQL in node definition | Bad syntax in transforms, missing commas, unclosed parens |
| `reference_error` | Broken reference to another object | Source node not deployed, node renamed, ref() typo |
| `missing_object` | Table/schema/database doesn't exist | Not yet deployed, wrong storage location |
| `permission_error` | Snowflake access denied | Wrong role, warehouse suspended, missing grants |
| `data_type_error` | Type mismatch or cast failure | VARCHAR in numeric column, bad date format |
| `timeout` | Query ran too long | Large table scan, missing filters, small warehouse |
| `configuration_error` | Node config issue | Missing materialization, duplicate columns, bad location |
| `network_error` | Connection or API failure | Transient — usually safe to retry |
| `unknown` | Unclassified error | Check raw error message for details |

## Typical Workflow

```
1. User: "My run failed"
2. Agent: Use list-runs to find the failed run ID
3. Agent: Call diagnose-run-failure with the run ID
4. Agent: Present the diagnosis to the user:
   - Which nodes failed and why
   - Specific fix suggestions
   - Whether to fix first or retry
5. Agent: Apply fixes using node mutation tools
6. Agent: Use retry-run to re-execute failed nodes
```

## Reading the Output

### Summary Section
Shows total/succeeded/failed/skipped counts. A high failure ratio (>50%) usually means a systemic issue like bad credentials or a missing upstream dependency.

### Failures Array
Each entry contains:
- `nodeID` / `nodeName` — which node failed
- `category` — the classified failure type
- `errorMessage` — the raw error from Coalesce/Snowflake
- `suggestedFixes` — actionable steps to resolve

### Recommendations
Prioritized list of actions. Address permission and reference errors first — they often cascade and cause downstream failures.

## Integration with Other Tools

After diagnosing:
- Use `get-workspace-node` to inspect a failed node's transforms and config
- Use `update-workspace-node` to fix column transforms or join conditions
- Use `complete-node-configuration` to fill in missing config
- Use `apply-join-condition` to fix broken join references
- Use `retry-run` to re-execute only the failed nodes
