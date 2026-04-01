# MCP Lineage & Impact Tools — Build Progress

## Status: Complete

## Step 1: Lineage Cache Service
- [x] Create src/services/lineage/ directory
- [x] lineage-cache.ts — paginated fetch with detail=true, skipParsing=false
- [x] Store in Map<nodeID, node>
- [x] Build 4 indexes: node→upstream, node→downstream, column→source, column→downstream
- [x] Reuse existing cache_workspace_nodes data if available
- [x] Progress notifications every 500 nodes
- [x] TTL-based cache invalidation (default 30 min)

## Step 2: Tools
- [x] get_upstream_nodes(workspaceID, nodeID) — full depth, no limit
- [x] get_downstream_nodes(workspaceID, nodeID) — full depth, no limit
- [x] get_column_lineage(workspaceID, nodeID, columnID) — full path upstream + downstream
- [x] analyze_impact(workspaceID, nodeID, columnID?) — what breaks, grouped by depth, critical path
- [x] propagate_column_change(workspaceID, nodeID, columnID, changes) — update all downstream columns (write tool)

## Step 3: Registration
- [x] All read tools registered as read-only (compatible with COALESCE_MCP_READ_ONLY)
- [x] propagate_column_change registered as write tool (hidden in read-only mode)
- [x] Added to README tool reference

## Step 4: Tests
- [x] Cache building with mock data
- [x] Index correctness
- [x] Full-depth traversal (25-node chain)
- [x] Column lineage multi-hop
- [x] Impact analysis node + column level
- [x] Propagation updates downstream correctly
- [x] Progress notifications firing
- [x] Cache reuse from snapshots
- [x] Diamond dependency pattern
- [x] Paginated API responses

## Step 5: Ship
- [x] Branch feature/lineage-tools off main
- [x] PR → develop, code review, merge

## API Notes
- Must use skipParsing=false for resolved column references
- total field is count returned per page, not workspace total
- Paginate via next cursor until null
- detail=true includes full column/sourceMapping data
