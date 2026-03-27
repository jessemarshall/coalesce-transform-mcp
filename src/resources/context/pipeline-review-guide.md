# Pipeline Review Guide

This guide explains how to use the `review-pipeline` tool to analyze existing Coalesce pipelines and identify improvement opportunities.

## When to Use

Use `review-pipeline` when:
- A user asks "how can I improve my pipeline?"
- You want to audit a workspace for quality issues before a release
- After building a pipeline, to verify it follows best practices
- When debugging performance issues or unexpected behavior

Do NOT use when:
- You need to create new nodes — use `plan-pipeline` or `build-pipeline-from-intent`
- You need to fix a specific node — use `update-workspace-node` directly
- You need to diagnose a run failure — use `diagnose-run-failure`

## What It Analyzes

### Critical Findings (must fix)
- **Missing join condition**: Multi-predecessor nodes without FROM/JOIN/ON defined. These will fail at run time.

### Warnings (should fix)
- **Redundant passthrough**: Single-predecessor nodes where every column is a passthrough — adds a layer with no value.
- **Node type mismatch**: View type used for multi-source joins (performance risk), or Dimension/Fact type at staging layer (semantic mismatch).
- **Orphan nodes**: Disconnected from the pipeline (no predecessors, no successors).

### Suggestions (nice to have)
- **Layer violations**: Skipping from bronze directly to mart without staging/intermediate layers.
- **Deep chains**: Pipelines 8+ nodes deep that increase deployment complexity.
- **High fan-out**: Nodes with 10+ downstream dependents — changes cascade widely.
- **Naming inconsistencies**: Mixed case, missing layer prefixes for the detected methodology.
- **Unused columns**: Over half the columns not referenced by any downstream node.

## Reading the Output

### Findings Array
Each finding includes:
- `severity`: critical, warning, or suggestion
- `category`: the type of issue detected
- `nodeID` / `nodeName`: which node is affected
- `message`: what's wrong
- `suggestion`: how to fix it

### Graph Stats
- `maxDepth`: longest chain from root to leaf
- `rootNodes`: nodes with no predecessors (sources)
- `leafNodes`: nodes with no successors (terminal outputs)
- `avgFanOut`: average number of downstream dependents per node

### Methodology Detection
The tool detects the workspace methodology (Kimball, Data Vault, dbt-style, mixed) and tailors naming checks accordingly.

## Scoping the Review

By default, the tool reviews all nodes but only fetches full detail for the first 50. For large workspaces:

1. **Use subgraph scoping**: Get node IDs from `list-workspace-subgraphs` and pass them as `nodeIDs`
2. **Focus on a pipeline section**: Pass specific node IDs for the pipeline you want to review

## Typical Workflow

```
1. User: "Can you review my pipeline?"
2. Agent: Call review-pipeline with the workspace ID
3. Agent: Present findings grouped by severity
4. Agent: For critical issues, offer to fix them:
   - Missing join: use apply-join-condition
   - Redundant passthrough: suggest removal
   - Type mismatch: suggest update-workspace-node
5. Agent: For warnings/suggestions, explain trade-offs
```

## Integration with Other Tools

After reviewing:
- Use `get-workspace-node` to inspect flagged nodes in detail
- Use `apply-join-condition` to fix missing join conditions
- Use `update-workspace-node` to fix node types or configurations
- Use `complete-node-configuration` to fill missing config
- Use `analyze-workspace-patterns` for a higher-level workspace profile
