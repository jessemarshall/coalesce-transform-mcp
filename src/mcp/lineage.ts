import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  buildJsonToolResponse,
  getToolOutputSchema,
  handleToolError,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  validatePathSegment,
} from "../coalesce/types.js";
import {
  buildLineageCache,
  walkUpstream,
  walkDownstream,
  walkColumnLineage,
  analyzeNodeImpact,
  propagateColumnChange,
} from "../services/lineage/lineage-cache.js";
import {
  createWorkflowProgressReporter,
  type WorkflowProgressExtra,
} from "../workflows/progress.js";
import { isPlainObject } from "../utils.js";

export function registerLineageTools(
  server: McpServer,
  client: CoalesceClient
): void {
  // --- get_upstream_nodes ---
  server.registerTool(
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
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  // --- get_downstream_nodes ---
  server.registerTool(
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
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  // --- get_column_lineage ---
  server.registerTool(
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
        });
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  // --- analyze_impact ---
  server.registerTool(
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

        return buildJsonToolResponse("analyze_impact", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  // --- propagate_column_change ---
  server.registerTool(
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
        "Returns: List of updated nodes/columns, total count, and any errors encountered.",
        "Each downstream node is fetched, its column updated, and the full node PUT back via API.",
        "The lineage cache is invalidated after propagation.",
        "",
        "Requires a lineage cache — will fetch all workspace nodes with detail=true on first call.",
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
      }),
      outputSchema: getToolOutputSchema("propagate_column_change"),
      annotations: WRITE_ANNOTATIONS,
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

        const result = await propagateColumnChange(
          client,
          cache,
          params.workspaceID,
          params.nodeID,
          params.columnID,
          params.changes,
          progressReporter
        );

        const response = buildJsonToolResponse("propagate_column_change", result);
        if (
          isPlainObject(result) &&
          Array.isArray(result.errors) &&
          result.errors.length > 0 &&
          Array.isArray(result.updatedNodes) &&
          result.updatedNodes.length === 0
        ) {
          return { ...response, isError: true };
        }
        return response;
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
