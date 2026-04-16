import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import { getCacheBaseDir } from "../cache-dir.js";
import {
  buildJsonToolResponse,
  getToolOutputSchema,
  handleToolError,
  DESTRUCTIVE_ANNOTATIONS,
  READ_ONLY_ANNOTATIONS,
  validatePathSegment,
  type ToolDefinition,
} from "../coalesce/types.js";
import { requireDestructiveConfirmation } from "../services/shared/elicitation.js";
import {
  buildLineageCache,
  walkUpstream,
  walkDownstream,
  walkColumnLineage,
  analyzeNodeImpact,
  propagateColumnChange,
  searchWorkspaceContent,
  auditDocumentationCoverage,
  type SearchField,
} from "../services/lineage/lineage-cache.js";
import {
  createWorkflowProgressReporter,
  type WorkflowProgressExtra,
} from "../workflows/progress.js";

export function defineLineageTools(
  server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  // --- get_upstream_nodes ---
  [
    "get_upstream_nodes",
    {
      title: "Get Upstream Nodes",
      description: [
        "Walk the full upstream dependency graph for a node and return every ancestor with its depth level.",
        "",
        "Args:",
        "  workspaceID: Workspace to query",
        "  nodeID: Starting node whose upstream lineage to trace",
        "",
        "Returns: Array of ancestor nodes with nodeID, nodeName, nodeType, and depth (1 = direct parent).",
        "Traverses the entire graph with no depth limit. Nodes are deduplicated.",
        "",
        "Requires a lineage cache — will fetch all workspace nodes with detail=true on first call (may take a moment for large workspaces). Subsequent calls use the cached graph (default TTL: 30 min).",
      ].join("\n"),
      inputSchema: z.object({
        workspaceID: z.string().describe("Workspace ID"),
        nodeID: z.string().describe("Node ID to trace upstream from"),
      }),
      outputSchema: getToolOutputSchema("get_upstream_nodes"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params, extra) => {
      try {
        validatePathSegment(params.workspaceID, "workspaceID");
        validatePathSegment(params.nodeID, "nodeID");

        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );

        const cache = await buildLineageCache(client, params.workspaceID, {
          reportProgress: progressReporter,
        });

        if (!cache.nodes.has(params.nodeID)) {
          throw new Error(
            `Node ${params.nodeID} not found in workspace ${params.workspaceID}. Available nodes: ${cache.nodes.size}. Ensure the node ID is correct.`
          );
        }

        const ancestors = walkUpstream(cache, params.nodeID);
        const node = cache.nodes.get(params.nodeID)!;

        return buildJsonToolResponse("get_upstream_nodes", {
          nodeID: params.nodeID,
          nodeName: node.name,
          nodeType: node.nodeType,
          totalAncestors: ancestors.length,
          ancestors,
        }, { workspaceID: params.workspaceID });
      } catch (error) {
        return handleToolError(error);
      }
    }
  ],

  // --- get_downstream_nodes ---
  [
    "get_downstream_nodes",
    {
      title: "Get Downstream Nodes",
      description: [
        "Walk the full downstream dependency graph for a node and return every dependent with its depth level.",
        "",
        "Args:",
        "  workspaceID: Workspace to query",
        "  nodeID: Starting node whose downstream dependents to trace",
        "",
        "Returns: Array of dependent nodes with nodeID, nodeName, nodeType, and depth (1 = direct child).",
        "Traverses the entire graph with no depth limit. Nodes are deduplicated.",
        "",
        "Requires a lineage cache — will fetch all workspace nodes with detail=true on first call.",
      ].join("\n"),
      inputSchema: z.object({
        workspaceID: z.string().describe("Workspace ID"),
        nodeID: z.string().describe("Node ID to trace downstream from"),
      }),
      outputSchema: getToolOutputSchema("get_downstream_nodes"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params, extra) => {
      try {
        validatePathSegment(params.workspaceID, "workspaceID");
        validatePathSegment(params.nodeID, "nodeID");

        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );

        const cache = await buildLineageCache(client, params.workspaceID, {
          reportProgress: progressReporter,
        });

        if (!cache.nodes.has(params.nodeID)) {
          throw new Error(
            `Node ${params.nodeID} not found in workspace ${params.workspaceID}. Available nodes: ${cache.nodes.size}. Ensure the node ID is correct.`
          );
        }

        const dependents = walkDownstream(cache, params.nodeID);
        const node = cache.nodes.get(params.nodeID)!;

        return buildJsonToolResponse("get_downstream_nodes", {
          nodeID: params.nodeID,
          nodeName: node.name,
          nodeType: node.nodeType,
          totalDependents: dependents.length,
          dependents,
        }, { workspaceID: params.workspaceID });
      } catch (error) {
        return handleToolError(error);
      }
    }
  ],

  // --- get_column_lineage ---
  [
    "get_column_lineage",
    {
      title: "Get Column Lineage",
      description: [
        "Trace a specific column through the entire pipeline — upstream to its sources and downstream to every column that depends on it.",
        "",
        "Args:",
        "  workspaceID: Workspace to query",
        "  nodeID: Node that contains the column",
        "  columnID: Column ID to trace",
        "",
        "Returns: Array of column lineage entries with nodeID, nodeName, nodeType, columnID, columnName, direction (upstream/downstream), and depth.",
        "Uses column-level references (metadata.columns[].sources[].columnReferences[]) to trace the full path.",
        "",
        "Requires a lineage cache — will fetch all workspace nodes with detail=true on first call.",
      ].join("\n"),
      inputSchema: z.object({
        workspaceID: z.string().describe("Workspace ID"),
        nodeID: z.string().describe("Node ID containing the column"),
        columnID: z.string().describe("Column ID to trace lineage for"),
      }),
      outputSchema: getToolOutputSchema("get_column_lineage"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params, extra) => {
      try {
        validatePathSegment(params.workspaceID, "workspaceID");
        validatePathSegment(params.nodeID, "nodeID");

        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );

        const cache = await buildLineageCache(client, params.workspaceID, {
          reportProgress: progressReporter,
        });

        const node = cache.nodes.get(params.nodeID);
        if (!node) {
          throw new Error(
            `Node ${params.nodeID} not found in workspace ${params.workspaceID}. Ensure the node ID is correct.`
          );
        }

        const col = node.columns.find((c) => c.id === params.columnID);
        if (!col) {
          const available = node.columns.map((c) => `${c.id} (${c.name})`).join(", ");
          throw new Error(
            `Column ${params.columnID} not found on node ${params.nodeID} (${node.name}). Available columns: ${available || "none"}`
          );
        }

        const lineage = walkColumnLineage(cache, params.nodeID, params.columnID);
        const upstream = lineage.filter((e) => e.direction === "upstream");
        const downstream = lineage.filter((e) => e.direction === "downstream");

        return buildJsonToolResponse("get_column_lineage", {
          nodeID: params.nodeID,
          nodeName: node.name,
          columnID: params.columnID,
          columnName: col.name,
          totalUpstream: upstream.length,
          totalDownstream: downstream.length,
          upstream,
          downstream,
        }, { workspaceID: params.workspaceID });
      } catch (error) {
        return handleToolError(error);
      }
    }
  ],

  // --- analyze_impact ---
  [
    "analyze_impact",
    {
      title: "Analyze Impact",
      description: [
        "Analyze the downstream impact of changing a node or a specific column.",
        "",
        "Args:",
        "  workspaceID: Workspace to query",
        "  nodeID: Node to analyze impact for",
        "  columnID: (optional) Specific column — if omitted, analyzes impact of the entire node",
        "",
        "Returns: Impacted node count, impacted column count, nodes grouped by depth level, and the critical path (longest dependency chain from source to leaf).",
        "Without columnID: shows all downstream nodes and all columns across them that depend on any column of this node.",
        "With columnID: shows only nodes/columns that specifically depend on that column.",
        "",
        "Requires a lineage cache — will fetch all workspace nodes with detail=true on first call.",
      ].join("\n"),
      inputSchema: z.object({
        workspaceID: z.string().describe("Workspace ID"),
        nodeID: z.string().describe("Node ID to analyze impact for"),
        columnID: z.string().optional().describe("Optional column ID — if provided, only analyzes impact of that specific column"),
      }),
      outputSchema: getToolOutputSchema("analyze_impact"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params, extra) => {
      try {
        validatePathSegment(params.workspaceID, "workspaceID");
        validatePathSegment(params.nodeID, "nodeID");

        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );

        const cache = await buildLineageCache(client, params.workspaceID, {
          reportProgress: progressReporter,
        });

        if (!cache.nodes.has(params.nodeID)) {
          throw new Error(
            `Node ${params.nodeID} not found in workspace ${params.workspaceID}. Available nodes: ${cache.nodes.size}. Ensure the node ID is correct.`
          );
        }

        const result = analyzeNodeImpact(cache, params.nodeID, params.columnID);

        return buildJsonToolResponse("analyze_impact", result, {
          workspaceID: params.workspaceID,
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  ],

  // --- propagate_column_change ---
  [
    "propagate_column_change",
    {
      title: "Propagate Column Change",
      description: [
        "⚠️ WRITE operation — Updates all downstream columns that depend on a source column.",
        "Use this after renaming a column or changing its data type to propagate the change through the entire pipeline.",
        "",
        "Args:",
        "  workspaceID: Workspace to modify",
        "  nodeID: Node containing the source column",
        "  columnID: Column ID that was changed",
        "  changes: Object with optional columnName and/or dataType to propagate",
        "",
        "Returns: Pre-mutation snapshot summary (column-level changes), snapshotPath to a disk file with full node bodies, list of updated nodes/columns, total count, and any errors encountered.",
        "The disk snapshot at snapshotPath captures each downstream node's complete nodeBody before mutation, enabling manual reversal of partial failures via set_workspace_node.",
        "Each downstream node is fetched, its column updated, and the full node PUT back via API.",
        "The lineage cache is invalidated after propagation.",
        "",
        "Requires a lineage cache — will fetch all workspace nodes with detail=true on first call.",
        "Note: Propagation targets are determined from the cached lineage graph (up to 30 min old).",
        "Downstream nodes added after the cache was built will not be included. Refresh lineage first",
        "if the workspace structure has changed recently.",
      ].join("\n"),
      inputSchema: z.object({
        workspaceID: z.string().describe("Workspace ID"),
        nodeID: z.string().describe("Node ID containing the source column"),
        columnID: z.string().describe("Column ID that was changed"),
        changes: z.object({
          columnName: z.string().optional().describe("New column name to propagate"),
          dataType: z.string().optional().describe("New data type to propagate"),
        }).refine(
          (c) => c.columnName !== undefined || c.dataType !== undefined,
          { message: "At least one of columnName or dataType is required" }
        ).describe("Changes to propagate — at least one of columnName or dataType required"),
        confirmed: z
          .boolean()
          .optional()
          .describe("Set to true after the user explicitly confirms the propagation. Required because this operation modifies multiple downstream nodes."),
      }),
      outputSchema: getToolOutputSchema("propagate_column_change"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params, extra) => {
      try {
        validatePathSegment(params.workspaceID, "workspaceID");
        validatePathSegment(params.nodeID, "nodeID");

        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );

        const cache = await buildLineageCache(client, params.workspaceID, {
          reportProgress: progressReporter,
        });

        // Validate node + column exist before asking for confirmation
        const node = cache.nodes.get(params.nodeID);
        if (!node) {
          throw new Error(`Node ${params.nodeID} not found in lineage cache`);
        }
        const sourceCol = node.columns.find((c) => c.id === params.columnID);
        if (!sourceCol) {
          throw new Error(`Column ${params.columnID} not found on node ${params.nodeID} (${node.name})`);
        }

        // Use column-level lineage (not node-level) for an accurate count
        const downstreamColumns = walkColumnLineage(cache, params.nodeID, params.columnID)
          .filter((e) => e.direction === "downstream");
        const changeDesc = [
          params.changes.columnName ? `rename to "${params.changes.columnName}"` : null,
          params.changes.dataType ? `change type to ${params.changes.dataType}` : null,
        ].filter(Boolean).join(" and ");

        const approvalResponse = await requireDestructiveConfirmation(
          server,
          "propagate_column_change",
          `This will update column references across ${downstreamColumns.length} downstream node(s) of "${node.name}" (${changeDesc}). This modifies node bodies via the API and cannot be easily undone.`,
          params.confirmed,
          { nodeID: params.nodeID, columnID: params.columnID, downstreamCount: downstreamColumns.length },
        );
        if (approvalResponse) return approvalResponse;

        const result = await propagateColumnChange(
          client,
          cache,
          params.workspaceID,
          params.nodeID,
          params.columnID,
          params.changes,
          progressReporter,
          getCacheBaseDir(),
        );

        const response = buildJsonToolResponse("propagate_column_change", result, {
          workspaceID: params.workspaceID,
        });
        if (result.partialFailure) {
          return {
            ...response,
            isError: true,
            content: [
              {
                type: "text" as const,
                text: `⚠️ PARTIAL FAILURE: ${result.totalUpdated} node(s) were updated before a write failed. ${result.skippedNodes?.length ?? 0} node(s) were skipped. ${result.errors.length} total error(s). The workspace is in an inconsistent state.${result.snapshotPath ? ` Full pre-mutation node bodies are saved at ${result.snapshotPath} — each entry contains the complete nodeBody that can be PUT back via set_workspace_node to reverse the change.` : ""} Review updatedNodes, skippedNodes, and errors to determine which nodes need manual correction.`,
              },
              ...(Array.isArray(response.content) ? response.content : []),
            ],
          };
        }
        if (result.errors.length > 0) {
          if (result.rolledBack) {
            return {
              ...response,
              isError: true,
              content: [
                {
                  type: "text" as const,
                  text: "Propagation failed after a write error, and any earlier successful writes were rolled back. No downstream updates remain applied.",
                },
                ...(Array.isArray(response.content) ? response.content : []),
              ],
            };
          }
          return { ...response, isError: true };
        }
        return response;
      } catch (error) {
        return handleToolError(error);
      }
    }
  ],

  // --- search_workspace_content ---
  [
    "search_workspace_content",
    {
      title: "Search Workspace Content",
      description: [
        "Search across node names, SQL, column names, descriptions, and config values in a workspace using the lineage cache as the data source.",
        "",
        "Args:",
        "  workspaceID: Workspace to search",
        "  query: Text to search for (case-insensitive substring match)",
        "  fields: (optional) Array of fields to search — any of: name, nodeType, sql, columnName, columnDataType, description, config. Defaults to all fields.",
        "  nodeType: (optional) Filter results to a specific node type",
        "  limit: (optional) Max results to return (1-200, default 50)",
        "",
        "Returns: Matching nodes with the fields that matched and content snippets.",
        "Efficient for large workspaces — searches the in-memory cache instead of making per-node API calls.",
        "",
        "Requires a populated lineage cache — will fetch all workspace nodes with detail=true on first call (may take a moment for large workspaces). Subsequent calls use the cached data (default TTL: 30 min).",
      ].join("\n"),
      inputSchema: z.object({
        workspaceID: z.string().describe("Workspace ID"),
        query: z.string().min(1).describe("Search text (case-insensitive)"),
        fields: z
          .array(z.enum(["name", "nodeType", "sql", "columnName", "columnDataType", "description", "config"]))
          .optional()
          .describe("Fields to search — defaults to all if omitted"),
        nodeType: z.string().optional().describe("Filter to a specific node type"),
        limit: z
          .number()
          .int()
          .min(1)
          .max(200)
          .optional()
          .describe("Max results (default 50)"),
      }),
      outputSchema: getToolOutputSchema("search_workspace_content"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params, extra) => {
      try {
        validatePathSegment(params.workspaceID, "workspaceID");

        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );

        const cache = await buildLineageCache(client, params.workspaceID, {
          reportProgress: progressReporter,
        });

        const result = searchWorkspaceContent(cache, {
          query: params.query,
          fields: params.fields as SearchField[] | undefined,
          nodeType: params.nodeType,
          limit: params.limit,
        });

        return buildJsonToolResponse("search_workspace_content", result, {
          workspaceID: params.workspaceID,
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  ],

  // --- audit_documentation_coverage ---
  [
    "audit_documentation_coverage",
    {
      title: "Audit Documentation Coverage",
      description: [
        "Scan all nodes and columns in a workspace and report documentation coverage statistics.",
        "",
        "Args:",
        "  workspaceID: Workspace to audit",
        "",
        "Returns: Total and documented counts for nodes and columns, percentage coverage,",
        "and lists of undocumented nodes and columns (column list capped at 200).",
        "A node is 'documented' if it has a non-empty description. A column is 'documented'",
        "if its metadata.columns[] entry has a non-empty description field.",
        "",
        "Requires a lineage cache — will fetch all workspace nodes with detail=true on first call",
        "(may take a moment for large workspaces). Subsequent calls use the cached graph (default TTL: 30 min).",
      ].join("\n"),
      inputSchema: z.object({
        workspaceID: z.string().describe("Workspace ID to audit"),
      }),
      outputSchema: getToolOutputSchema("audit_documentation_coverage"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params, extra) => {
      try {
        validatePathSegment(params.workspaceID, "workspaceID");

        const progressReporter = createWorkflowProgressReporter(
          extra as WorkflowProgressExtra | undefined
        );

        const cache = await buildLineageCache(client, params.workspaceID, {
          reportProgress: progressReporter,
        });

        const result = auditDocumentationCoverage(cache);

        return buildJsonToolResponse("audit_documentation_coverage", result, {
          workspaceID: params.workspaceID,
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  ],
  ];
}
