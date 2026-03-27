import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  listEnvironmentNodes,
  listWorkspaceNodes,
  getEnvironmentNode,
  getWorkspaceNode,
  setWorkspaceNode,
  deleteWorkspaceNode,
} from "../coalesce/api/nodes.js";
import {
  updateWorkspaceNode,
  buildUpdatedWorkspaceNodeBody,
  replaceWorkspaceNodeColumns,
  createWorkspaceNodeFromScratch,
  createWorkspaceNodeFromPredecessor,
  convertJoinToAggregation,
  applyJoinCondition,
  listWorkspaceNodeTypes,
} from "../services/workspace/mutations.js";
import { completeNodeConfiguration } from "../services/config/intelligent.js";
import { assertNoSqlOverridePayload } from "../services/policies/sql-override.js";
import { buildWorkspaceProfile } from "../services/workspace/analysis.js";
import { fetchAllWorkspaceNodes, toNodeSummaries } from "../services/cache/snapshots.js";
import {
  NodeConfigInputSchema,
  StorageLocationInputSchema,
  WorkspaceNodeColumnInputSchema,
  WorkspaceNodeMetadataInputSchema,
  WorkspaceNodeWriteInputSchema,
} from "../schemas/node-payloads.js";
import {
  PaginationParams,
  buildJsonToolResponse,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  IDEMPOTENT_WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  handleToolError,
} from "../coalesce/types.js";

export function registerNodeTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.tool(
    "list-environment-nodes",
    "List all nodes in a Coalesce environment",
    PaginationParams.extend({
      environmentID: z.string().describe("The environment ID"),
      detail: z.boolean().optional().describe("Include full node details in response"),
    }).shape,
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listEnvironmentNodes(client, params);
        return buildJsonToolResponse("list-environment-nodes", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "list-workspace-nodes",
    "List all nodes in a Coalesce workspace. To find workspace IDs, use list-workspaces.",
    PaginationParams.extend({
      workspaceID: z.string().describe("The workspace ID"),
      detail: z.boolean().optional().describe("Include full node details in response"),
    }).shape,
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listWorkspaceNodes(client, params);
        return buildJsonToolResponse("list-workspace-nodes", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-environment-node",
    "Get details of a specific node in a Coalesce environment",
    {
      environmentID: z.string().describe("The environment ID"),
      nodeID: z.string().describe("The node ID"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getEnvironmentNode(client, params);
        return buildJsonToolResponse("get-environment-node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "get-workspace-node",
    "Get details of a specific node in a Coalesce workspace. To find workspace IDs, use list-workspaces.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeID: z.string().describe("The node ID"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await getWorkspaceNode(client, params);
        return buildJsonToolResponse("get-workspace-node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "create-workspace-node-from-scratch",
    "Create a workspace node from scratch with NO predecessors. Only use this when the node truly has no upstream nodes — for example, a standalone utility node. If the node has ANY upstream/source nodes, use create-workspace-node-from-predecessor instead.\n\nREQUIRED: Before calling this tool, call plan-pipeline with goal + repoPath to discover the correct nodeType. Do not guess or hardcode node types — the planner ranks all available types and returns the best match.\n\nSPECIALIZED TYPES WARNING: Do NOT use Dynamic Tables, Incremental Load, Materialized View, or other specialized types unless the user explicitly requests that pattern (e.g., 'near-real-time refresh', 'incremental processing'). For standard batch ETL, CTE decomposition, and general transforms, use Stage or Work. The response includes nodeTypeValidation.warning if a specialized pattern was detected without matching context.\n\nDefaults to completionLevel='configured', which REQUIRES both `name` and `metadata.columns` to be provided. If you don't have column definitions yet, set completionLevel to 'created' or 'named' instead.\n\nAUTOMATIC CONFIG: When repoPath is provided, this tool automatically runs intelligent config completion after creation — reading the node type definition, setting node-level config defaults, and applying column-level attributes. The configCompletion result shows what was applied.\n\nDo not use overrideSQL or override.* fields; SQL override is disallowed in this project.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeType: z.string().describe("The type of node to create. IMPORTANT: Call plan-pipeline first to discover and rank available node types — use the nodeType from its result. Format: 'PackageName:::ID' for package types (e.g., 'base-nodes:::Stage') or simple name ('Stage') for built-in types. Always prefer the package-prefixed format returned by plan-pipeline."),
      completionLevel: z
        .enum(["created", "named", "configured"])
        .optional()
        .describe("How complete the node should be before the tool returns. Defaults to configured."),
      name: z
        .string()
        .optional()
        .describe("Optional node name to apply after creation."),
      description: z
        .string()
        .optional()
        .describe("Optional node description to apply after creation."),
      storageLocations: z
        .array(StorageLocationInputSchema)
        .optional()
        .describe("Optional storageLocations array to apply after creation."),
      config: NodeConfigInputSchema
        .optional()
        .describe("Optional config object to apply after creation."),
      metadata: WorkspaceNodeMetadataInputSchema
        .optional()
        .describe("Optional metadata object to apply after creation, including metadata.columns."),
      changes: WorkspaceNodeWriteInputSchema
        .optional()
        .describe("Optional additional partial fields to merge after the node is created and fetched."),
      repoPath: z
        .string()
        .optional()
        .describe("Path to local Coalesce repository for automatic config completion after creation."),
      goal: z
        .string()
        .optional()
        .describe("The goal or intent for this node (e.g., 'deduplicate customer records', 'aggregate daily sales'). Used to validate that the chosen nodeType is appropriate for the task. Same value you would pass to plan-pipeline."),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await createWorkspaceNodeFromScratch(client, params);
        return buildJsonToolResponse("create-workspace-node-from-scratch", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "set-workspace-node",
    "Replace all fields of a workspace node (full update). To find workspace IDs, use list-workspaces.\n\nDo not include overrideSQL or override.* fields; they are auto-preserved from the existing node. SQL override is disallowed in this project.\n\nPrefer update-workspace-node for partial changes. This tool fetches the current node to preserve API-required fields (table, overrideSQL, columnIDs).",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeID: z.string().describe("The node ID"),
      body: WorkspaceNodeWriteInputSchema.describe(
        "Complete node data to set. Common fields include name, description, nodeType, table, database, schema, locationName, storageLocations, config, and metadata. Do not include overrideSQL — it is auto-preserved."
      ),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        // Deep-check for override fields the agent may have included
        assertNoSqlOverridePayload(params.body, "set-workspace-node body");

        // Fetch current node to preserve API-required fields
        const current = await getWorkspaceNode(client, {
          workspaceID: params.workspaceID,
          nodeID: params.nodeID,
        });

        // Route through the shared merge+validate+clean path that ensures
        // all API-required fields (table, overrideSQL, dataType, columnID,
        // nullable, description, enabledColumnTestIDs) are present.
        const body = buildUpdatedWorkspaceNodeBody(current, params.body);

        // Preserve database/schema from current node if not provided
        if (typeof current === "object" && current !== null && !Array.isArray(current)) {
          const currentObj = current as Record<string, unknown>;
          for (const field of ["database", "schema"]) {
            if (!(field in body) && field in currentObj) {
              body[field] = currentObj[field];
            }
          }
        }

        const result = await setWorkspaceNode(client, {
          workspaceID: params.workspaceID,
          nodeID: params.nodeID,
          body,
        });
        return buildJsonToolResponse("set-workspace-node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "update-workspace-node",
    "Safely update selected fields of a workspace node by fetching the current node, applying partial changes, then writing back the full merged body. Object fields are deep-merged; arrays replace the existing array when provided. To find workspace IDs, use list-workspaces.\n\nDo not use overrideSQL or override.* fields; SQL override is disallowed in this project.\n\nNOTE: Arrays (like metadata.columns) are replaced, not merged. For complex column transformations (e.g., converting from join to aggregation), consider using replace-workspace-node-columns instead.\n\nFor guidance on SQL platforms and tool usage patterns, see resources: coalesce://context/sql-platform-selection, coalesce://context/tool-usage",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeID: z.string().describe("The node ID"),
      changes: WorkspaceNodeWriteInputSchema.describe(
        "Partial node fields to update. Common fields include name, description, database, schema, locationName, storageLocations, config, and metadata. Object fields are deep-merged; arrays replace the existing array when provided."
      ),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await updateWorkspaceNode(client, params);
        return buildJsonToolResponse("update-workspace-node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "replace-workspace-node-columns",
    "Replace all columns in a workspace node with a new set of columns, optionally applying a WHERE filter and additional changes in a single call.\n\nDo not use overrideSQL or override.* fields; SQL override is disallowed in this project.\n\nUse this when:\n- Applying column transforms (UPPER, LEFT, COALESCE, etc.) after node creation\n- Adding WHERE filters at the same time as column transforms\n- Converting from a simple join to GROUP BY aggregation\n- Completely replacing column definitions with aggregate functions\n\nPrefer this over separate update-workspace-node calls. Combine column replacement + WHERE filter in one call.\n\nExample: Apply transforms and filter in one call:\n{\n  columns: [\n    { name: 'CUSTOMER_ID', transform: '\"CUSTOMER_LOYALTY\".\"CUSTOMER_ID\"' },\n    { name: 'CITY', transform: 'UPPER(\"CUSTOMER_LOYALTY\".\"CITY\")' },\n    { name: 'CONTACT_INFO', transform: 'COALESCE(\"CUSTOMER_LOYALTY\".\"E_MAIL\", \"CUSTOMER_LOYALTY\".\"PHONE_NUMBER\")' }\n  ],\n  whereCondition: '\"CUSTOMER_LOYALTY\".\"CUSTOMER_ID\" IS NOT NULL AND (\"CUSTOMER_LOYALTY\".\"E_MAIL\" IS NOT NULL OR \"CUSTOMER_LOYALTY\".\"PHONE_NUMBER\" IS NOT NULL)'\n}\n\nIMPORTANT: Use whereCondition for WHERE filters — do NOT construct {{ ref() }} syntax yourself. The FROM clause is already set up from node creation. The whereCondition is appended to the existing joinCondition automatically.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeID: z.string().describe("The node ID"),
      columns: z.array(WorkspaceNodeColumnInputSchema).describe(
        "Complete new columns array to replace metadata.columns. Each column should include name and may include transform, dataType, description, nullable, sources, and other hydrated metadata fields."
      ),
      whereCondition: z
        .string()
        .optional()
        .describe("Optional WHERE filter to append to the node's existing joinCondition. Just provide the condition — do NOT include the WHERE keyword or construct {{ ref() }} syntax. The FROM clause is already set from node creation. Example: '\"LOCATION\".\"LOCATION_ID\" IS NOT NULL AND \"LOCATION\".\"LOCATION_ID\" != 0'"),
      additionalChanges: WorkspaceNodeWriteInputSchema
        .optional()
        .describe("Optional additional fields to update, such as name, description, config, or metadata. Object fields are deep-merged; arrays are replaced. Do NOT include metadata.sourceMapping or customSQL — use whereCondition for WHERE filters, apply-join-condition for join setup, or convert-join-to-aggregation for GROUP BY patterns."),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await replaceWorkspaceNodeColumns(client, params);
        return buildJsonToolResponse("replace-workspace-node-columns", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "convert-join-to-aggregation",
    "Convert an existing join node into an aggregated fact table with GROUP BY. This is the REQUIRED follow-up after creating a multi-predecessor node — it completes the join setup.\n\nThis tool automatically:\n- Generates JOIN ON clauses from common columns between predecessors\n- Writes the complete FROM/JOIN/ON/GROUP BY clause to the node's joinCondition (no separate update needed)\n- Replaces columns with GROUP BY dimensions + aggregate measures\n- Infers datatypes from transform functions (COUNT → NUMBER, SUM → NUMBER(38,4), etc.)\n- Sets column-level attributes (isBusinessKey on GROUP BY columns, isChangeTracking on aggregates)\n- Validates that all non-aggregate columns are in GROUP BY\n- Runs intelligent config completion\n\nUse this to transform a simple join (row-level) into an aggregated fact table (summary-level).\n\nExample: Convert order detail join to customer metrics:\n{\n  workspaceID: \"1\",\n  nodeID: \"fact-node-id\",\n  groupByColumns: ['\"STG_ORDER_HEADER\".\"CUSTOMER_ID\"'],\n  aggregates: [\n    { name: \"TOTAL_ORDERS\", function: \"COUNT\", expression: 'DISTINCT \"STG_ORDER_HEADER\".\"ORDER_ID\"' },\n    { name: \"LIFETIME_VALUE\", function: \"SUM\", expression: '\"STG_ORDER_HEADER\".\"ORDER_TOTAL\"' },\n    { name: \"AVG_ORDER_VALUE\", function: \"AVG\", expression: '\"STG_ORDER_HEADER\".\"ORDER_TOTAL\"' }\n  ],\n  joinType: \"INNER JOIN\"\n}\n\nThe response includes:\n- Updated node with new columns and joinCondition already written\n- Generated JOIN SQL with GROUP BY\n- GROUP BY analysis and validation\n- Warnings if GROUP BY is invalid\n- Config completion results",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeID: z.string().describe("The node ID of the join to convert"),
      groupByColumns: z
        .array(z.string())
        .describe("Columns to group by (dimensions). Use fully-qualified names like '\"TABLE\".\"COLUMN\"'."),
      aggregates: z
        .array(
          z.object({
            name: z.string().describe("Column name for the aggregate"),
            function: z.string().describe("Aggregate function: COUNT, SUM, AVG, MIN, MAX, etc."),
            expression: z.string().describe("Expression to aggregate (e.g., 'DISTINCT \"TABLE\".\"COLUMN\"')"),
            description: z.string().optional().describe("Optional column description"),
          })
        )
        .describe("Aggregate columns with their functions and expressions"),
      joinType: z
        .enum(["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"])
        .optional()
        .describe("Type of JOIN to use. Defaults to INNER JOIN."),
      maintainJoins: z
        .boolean()
        .optional()
        .describe("If true (default), analyzes predecessors, generates JOIN SQL, and writes the joinCondition to the node. If false, only replaces columns with aggregates without generating joins."),
      repoPath: z
        .string()
        .optional()
        .describe("Optional path to local Coalesce repository for intelligent config completion"),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await convertJoinToAggregation(client, params);
        return buildJsonToolResponse("convert-join-to-aggregation", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "apply-join-condition",
    "Auto-generate and write a FROM/JOIN/ON clause for a multi-predecessor node. Use this for ROW-LEVEL joins (no aggregation). For aggregation joins with GROUP BY, use convert-join-to-aggregation instead.\n\nThis tool automatically:\n- Reads the node to discover its predecessors from sourceMapping dependencies\n- Fetches each predecessor to get locationName and column names\n- Finds common columns between predecessor pairs for JOIN ON clauses\n- Generates FROM/JOIN/ON with proper {{ ref() }} syntax\n- Writes the joinCondition to the node's sourceMapping\n- Returns the generated joinCondition, joinSuggestions, and any warnings\n\nUse after create-workspace-node-from-predecessor when building row-level joins (e.g., enrichment joins, lookups, denormalization).\n\nFor column name mismatches across predecessors, use joinColumnOverrides to map them explicitly.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeID: z.string().describe("The node ID of the multi-predecessor node"),
      joinType: z
        .enum(["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"])
        .optional()
        .describe("Type of JOIN to use between predecessors. Defaults to INNER JOIN."),
      whereClause: z
        .string()
        .optional()
        .describe("Optional WHERE clause to append after the JOIN (without the WHERE keyword)."),
      qualifyClause: z
        .string()
        .optional()
        .describe("Optional QUALIFY clause to append (without the QUALIFY keyword)."),
      joinColumnOverrides: z
        .array(
          z.object({
            leftPredecessor: z.string().describe("Name of the left predecessor node"),
            rightPredecessor: z.string().describe("Name of the right predecessor node"),
            leftColumn: z.string().describe("Column name in the left predecessor"),
            rightColumn: z.string().describe("Column name in the right predecessor"),
          })
        )
        .optional()
        .describe("Explicit column mappings for joins when column names differ across predecessors. Overrides auto-detected common columns for the specified predecessor pair."),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await applyJoinCondition(client, params);
        return buildJsonToolResponse("apply-join-condition", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "create-workspace-node-from-predecessor",
    "Create a workspace node from one or more predecessor nodes, fetch it, and verify that columns were auto-populated from those predecessors before applying any optional changes.\n\nSINGLE-CALL WORKFLOW: You can create a node AND apply column transforms, WHERE filters, or aggregation in one call:\n- columns + whereCondition: Replace auto-populated columns with specific transforms and add a WHERE filter — no separate replace-workspace-node-columns needed\n- groupByColumns + aggregates: Convert to an aggregation node with GROUP BY — no separate convert-join-to-aggregation needed\nThese are mutually exclusive: use columns OR groupByColumns+aggregates, not both.\n\nREQUIRED: Before calling this tool, call `plan-pipeline` with `goal`, `sourceNodeIDs`, and `repoPath` to discover and rank available node types. Use the `nodeType` from the plan result — do NOT guess or hardcode node types like 'Stage', 'View', or numeric IDs like '65'. The planner scans all committed node type definitions and scores them against your use case.\n\nSPECIALIZED TYPES WARNING: Do NOT use Dynamic Tables, Incremental Load, Materialized View, or other specialized types unless the user explicitly requests that pattern (e.g., 'near-real-time refresh', 'continuous refresh', 'incremental processing'). For standard batch ETL, CTE decomposition, and general transforms, use Stage or Work. The response includes nodeTypeValidation.warning if a specialized pattern was detected without matching context — always check this field.\n\nJOIN INTELLIGENCE: For multi-predecessor nodes (joins), this tool automatically:\n- Analyzes common columns between each predecessor pair\n- Returns `joinSuggestions` with normalized column names and their left/right counterparts\n- Reports which predecessors are represented in the resulting column references\n- Warns if any predecessor is missing from the auto-populated columns\n\nAUTOMATIC CONFIG: When repoPath is provided, this tool automatically runs intelligent config completion after creation — reading the node type definition, setting node-level config defaults, and applying column-level attributes (isBusinessKey, isChangeTracking, etc.). The configCompletion result shows what was applied.\n\nDo not use overrideSQL or override.* fields; SQL override is disallowed in this project.\n\nFor guidance on node types, storage locations, and SQL patterns, see resources: coalesce://context/data-engineering-principles, coalesce://context/storage-mappings, coalesce://context/sql-platform-selection",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeType: z.string().describe("The type of node to create. IMPORTANT: Call plan-pipeline first to discover and rank available node types — use the nodeType from its result. Format: 'PackageName:::ID' for package types (e.g., 'base-nodes:::Stage') or simple name ('Stage') for built-in types. Always prefer the package-prefixed format returned by plan-pipeline."),
      predecessorNodeIDs: z
        .array(z.string())
        .min(1)
        .describe("One or more predecessor node IDs to link to the new node"),
      changes: WorkspaceNodeWriteInputSchema
        .optional()
        .describe("Optional partial fields to apply after successful auto-population validation, such as name, description, config, metadata, database, schema, or locationName."),
      columns: z.array(WorkspaceNodeColumnInputSchema)
        .optional()
        .describe("Replace auto-populated columns with these specific columns and transforms. Mutually exclusive with groupByColumns/aggregates."),
      whereCondition: z
        .string()
        .optional()
        .describe("WHERE filter to append to the joinCondition (without the WHERE keyword). Only valid with columns, not with groupByColumns/aggregates."),
      groupByColumns: z
        .array(z.string())
        .optional()
        .describe("GROUP BY columns for aggregation. Must be provided with aggregates. Mutually exclusive with columns."),
      aggregates: z
        .array(
          z.object({
            name: z.string().describe("Output column name for the aggregate"),
            function: z.string().describe("Aggregate function: COUNT, SUM, AVG, MIN, MAX, etc."),
            expression: z.string().describe("Expression to aggregate"),
            description: z.string().optional().describe("Column description"),
          })
        )
        .optional()
        .describe("Aggregate columns. Must be provided with groupByColumns. Mutually exclusive with columns."),
      joinType: z
        .enum(["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"])
        .optional()
        .describe("JOIN type for multi-predecessor aggregation nodes. Defaults to INNER JOIN."),
      repoPath: z
        .string()
        .optional()
        .describe("Path to local Coalesce repository for automatic config completion after creation."),
      goal: z
        .string()
        .optional()
        .describe("The goal or intent for this node (e.g., 'deduplicate customer records', 'join orders with customers'). Used to validate that the chosen nodeType is appropriate for the task. Same value you would pass to plan-pipeline."),
    },
    WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await createWorkspaceNodeFromPredecessor(client, params);
        return buildJsonToolResponse("create-workspace-node-from-predecessor", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "analyze-workspace-patterns",
    "Analyze workspace node patterns to detect package adoption, pipeline layers, data modeling methodology, and generate recommendations. Results are returned as a workspace profile summary. This tool paginates through the full workspace node list. If you want a reusable local snapshot instead of inline data, use `cache-workspace-nodes`.\n\nThis tool examines existing workspace nodes to understand conventions before creating new nodes.",
    {
      workspaceID: z.string().describe("The workspace ID to analyze"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const nodesResponse = await fetchAllWorkspaceNodes(client, {
          workspaceID: params.workspaceID,
          detail: false,
        });
        const nodes = toNodeSummaries(nodesResponse.items);

        const profile = buildWorkspaceProfile(params.workspaceID, nodes);
        return buildJsonToolResponse("analyze-workspace-patterns", profile);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "list-workspace-node-types",
    "List distinct node types observed in current workspace nodes. This scans existing nodes only; it is not a true installed-type registry. Use it to inspect current workspace usage.\n\nWARNING: Do NOT use these values directly as the nodeType parameter for create-workspace-node-from-predecessor or create-workspace-node-from-scratch. The observed values may be bare numeric IDs (e.g. '31') that differ from the proper package-prefixed format (e.g. 'base-nodes:::Stage'). Always call plan-pipeline first to discover the correct nodeType — it ranks repo-backed and observed types and returns the properly formatted identifier.",
    {
      workspaceID: z.string().describe("The workspace ID")
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const result = await listWorkspaceNodeTypes(client, params);
        return buildJsonToolResponse("list-workspace-node-types", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "delete-workspace-node",
    "Delete a node from a Coalesce workspace. This is a destructive operation — the node and all its configuration will be permanently removed. To find workspace IDs, use list-workspaces.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeID: z.string().describe("The node ID to delete"),
    },
    DESTRUCTIVE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await deleteWorkspaceNode(client, params);
        return buildJsonToolResponse("delete-workspace-node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.tool(
    "complete-node-configuration",
    "Intelligently complete a node's configuration by analyzing its context, classifying config fields, and applying best-practice rules. Returns updated node with applied config and detailed reasoning.",
    {
      workspaceID: z.string().describe("The workspace ID"),
      nodeID: z.string().describe("The node ID to configure"),
      repoPath: z.string().optional().describe("Optional path to local Coalesce repository for schema resolution"),
    },
    IDEMPOTENT_WRITE_ANNOTATIONS,
    async (params) => {
      try {
        const result = await completeNodeConfiguration(client, params);
        return buildJsonToolResponse("complete-node-configuration", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
