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
  getToolOutputSchema,
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
  server.registerTool(
    "list_environment_nodes",
    {
      title: "List Environment Nodes",
      description:
        "List all nodes deployed in a Coalesce environment.\n\nArgs:\n  - environmentID (string, required): The environment ID\n  - detail (boolean, optional): Include full node details\n  - limit, startingFrom, orderBy, orderByDirection: Pagination\n\nReturns:\n  { data: Node[], next?: string, total?: number }",
      inputSchema: PaginationParams.extend({
        environmentID: z.string().describe("The environment ID"),
        detail: z.boolean().optional().describe("Include full node details in response"),
      }),
      outputSchema: getToolOutputSchema("list_environment_nodes"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listEnvironmentNodes(client, params);
        return buildJsonToolResponse("list_environment_nodes", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "list_workspace_nodes",
    {
      title: "List Workspace Nodes",
      description:
        "List all nodes in a Coalesce workspace. Use list_workspaces to find workspace IDs.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - detail (boolean, optional): Include full node details\n  - limit, startingFrom, orderBy, orderByDirection: Pagination\n\nReturns:\n  { data: Node[], next?: string, total?: number }",
      inputSchema: PaginationParams.extend({
        workspaceID: z.string().describe("The workspace ID"),
        detail: z.boolean().optional().describe("Include full node details in response"),
      }),
      outputSchema: getToolOutputSchema("list_workspace_nodes"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listWorkspaceNodes(client, params);
        return buildJsonToolResponse("list_workspace_nodes", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_environment_node",
    {
      title: "Get Environment Node",
      description:
        "Get details of a specific node deployed in an environment.\n\nArgs:\n  - environmentID (string, required): The environment ID\n  - nodeID (string, required): The node ID\n\nReturns:\n  Full node object with columns, config, metadata, and deployment state.",
      inputSchema: z.object({
        environmentID: z.string().describe("The environment ID"),
        nodeID: z.string().describe("The node ID"),
      }),
      outputSchema: getToolOutputSchema("get_environment_node"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getEnvironmentNode(client, params);
        return buildJsonToolResponse("get_environment_node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "get_workspace_node",
    {
      title: "Get Workspace Node",
      description:
        "Get details of a specific node in a workspace. Use list_workspaces to find workspace IDs.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - nodeID (string, required): The node ID\n\nReturns:\n  Full workspace node with columns, transforms, joins, config, and metadata.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        nodeID: z.string().describe("The node ID"),
      }),
      outputSchema: getToolOutputSchema("get_workspace_node"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await getWorkspaceNode(client, params);
        return buildJsonToolResponse("get_workspace_node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "create_workspace_node_from_scratch",
    {
      title: "Create Workspace Node from Scratch",
      description:
        "Create a workspace node from scratch with NO predecessors. Only use this when the node truly has no upstream nodes — for example, a standalone utility node. If the node has ANY upstream/source nodes, use create_workspace_node_from_predecessor instead.\n\nREQUIRED: Before calling this tool, call plan_pipeline with goal + repoPath to discover the correct nodeType. Do not guess or hardcode node types — the planner ranks all available types and returns the best match.\n\nSPECIALIZED TYPES WARNING: Do NOT use Dynamic Tables, Incremental Load, Materialized View, or other specialized types unless the user explicitly requests that pattern (e.g., 'near-real-time refresh', 'incremental processing'). For standard batch ETL, CTE decomposition, and general transforms, use Stage or Work. The response includes nodeTypeValidation.warning if a specialized pattern was detected without matching context.\n\nDefaults to completionLevel='configured', which REQUIRES both `name` and `metadata.columns` to be provided. If you don't have column definitions yet, set completionLevel to 'created' or 'named' instead.\n\nAUTOMATIC CONFIG: When repoPath is provided, this tool automatically runs intelligent config completion after creation — reading the node type definition, setting node-level config defaults, and applying column-level attributes. The configCompletion result shows what was applied.\n\nDo not use overrideSQL or override.* fields; SQL override is disallowed in this project.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        nodeType: z.string().describe("The type of node to create. IMPORTANT: Call plan_pipeline first to discover and rank available node types — use the nodeType from its result. Format: 'PackageName:::ID' for package types (e.g., 'base-nodes:::Stage') or simple name ('Stage') for built-in types. Always prefer the package-prefixed format returned by plan_pipeline."),
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
          .describe("The goal or intent for this node (e.g., 'deduplicate customer records', 'aggregate daily sales'). Used to validate that the chosen nodeType is appropriate for the task. Same value you would pass to plan_pipeline."),
      }),
      outputSchema: getToolOutputSchema("create_workspace_node_from_scratch"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await createWorkspaceNodeFromScratch(client, params);
        return buildJsonToolResponse("create_workspace_node_from_scratch", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "set_workspace_node",
    {
      title: "Set Workspace Node",
      description:
        "Update a workspace node's full body. Reads current state, merges changes, validates, and writes back.\n\nThis is the primary mutation tool for workspace nodes. It handles column linkage preservation, passthrough transform stripping, required API field injection, and metadata cleaning automatically.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - nodeID (string, required): The node ID\n  - body (object, required): Fields to update — name, description, nodeType, config, metadata.columns, etc.\n\nReturns:\n  { nodeID, created, warning?, validation?, configCompletion? }\n\nDo NOT set overrideSQL or metadata.sourceMapping through this tool. Use apply_join_condition or convert_join_to_aggregation for join/aggregation changes.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        nodeID: z.string().describe("The node ID"),
        body: WorkspaceNodeWriteInputSchema.describe(
          "Complete node data to set. Common fields include name, description, nodeType, table, database, schema, locationName, storageLocations, config, and metadata. Do not include overrideSQL — it is auto-preserved."
        ),
      }),
      outputSchema: getToolOutputSchema("set_workspace_node"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        // Deep-check for override fields the agent may have included
        assertNoSqlOverridePayload(params.body, "set_workspace_node body");

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
        return buildJsonToolResponse("set_workspace_node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "update_workspace_node",
    {
      title: "Update Workspace Node",
      description:
        "Safely update selected fields of a workspace node by fetching the current node, applying partial changes, then writing back the full merged body. Object fields are deep-merged; arrays replace the existing array when provided. Use list_workspaces to find workspace IDs.\n\nDo not use overrideSQL or override.* fields; SQL override is disallowed in this project.\n\nNOTE: Arrays (like metadata.columns) are replaced, not merged. For complex column transformations (e.g., converting from join to aggregation), consider using replace_workspace_node_columns instead.\n\nFor guidance on SQL platforms and tool usage patterns, see resources: coalesce://context/sql-platform-selection, coalesce://context/tool-usage",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        nodeID: z.string().describe("The node ID"),
        changes: WorkspaceNodeWriteInputSchema.describe(
          "Partial node fields to update. Common fields include name, description, database, schema, locationName, storageLocations, config, and metadata. Object fields are deep-merged; arrays replace the existing array when provided."
        ),
      }),
      outputSchema: getToolOutputSchema("update_workspace_node"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await updateWorkspaceNode(client, params);
        return buildJsonToolResponse("update_workspace_node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "replace_workspace_node_columns",
    {
      title: "Replace Workspace Node Columns",
      description:
        "Replace all columns in a workspace node with a new set of columns, optionally applying a WHERE filter and additional changes in a single call.\n\nDo not use overrideSQL or override.* fields; SQL override is disallowed in this project.\n\nUse this when:\n- Applying column transforms (UPPER, LEFT, COALESCE, etc.) after node creation\n- Adding WHERE filters at the same time as column transforms\n- Converting from a simple join to GROUP BY aggregation\n- Completely replacing column definitions with aggregate functions\n\nPrefer this over separate update_workspace_node calls. Combine column replacement + WHERE filter in one call.\n\nExample: Apply transforms and filter in one call:\n{\n  columns: [\n    { name: 'CUSTOMER_ID', transform: '\"CUSTOMER_LOYALTY\".\"CUSTOMER_ID\"' },\n    { name: 'CITY', transform: 'UPPER(\"CUSTOMER_LOYALTY\".\"CITY\")' },\n    { name: 'CONTACT_INFO', transform: 'COALESCE(\"CUSTOMER_LOYALTY\".\"E_MAIL\", \"CUSTOMER_LOYALTY\".\"PHONE_NUMBER\")' }\n  ],\n  whereCondition: '\"CUSTOMER_LOYALTY\".\"CUSTOMER_ID\" IS NOT NULL AND (\"CUSTOMER_LOYALTY\".\"E_MAIL\" IS NOT NULL OR \"CUSTOMER_LOYALTY\".\"PHONE_NUMBER\" IS NOT NULL)'\n}\n\nIMPORTANT: Use whereCondition for WHERE filters — do NOT construct {{ ref() }} syntax yourself. The FROM clause is already set up from node creation. The whereCondition is appended to the existing joinCondition automatically.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - nodeID (string, required): The node ID\n  - columns (array, required): Complete new columns array\n  - whereCondition (string, optional): WHERE filter to append\n  - additionalChanges (object, optional): Additional fields to update\n\nReturns:\n  { nodeID, warning?, validation? }",
      inputSchema: z.object({
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
          .describe("Optional additional fields to update, such as name, description, config, or metadata. Object fields are deep-merged; arrays are replaced. Do NOT include metadata.sourceMapping or customSQL — use whereCondition for WHERE filters, apply_join_condition for join setup, or convert_join_to_aggregation for GROUP BY patterns."),
      }),
      outputSchema: getToolOutputSchema("replace_workspace_node_columns"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await replaceWorkspaceNodeColumns(client, params);
        return buildJsonToolResponse("replace_workspace_node_columns", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "convert_join_to_aggregation",
    {
      title: "Convert Join to Aggregation",
      description:
        "Convert an existing join node into an aggregated fact table with GROUP BY. This is the REQUIRED follow-up after creating a multi-predecessor node — it completes the join setup.\n\nThis tool automatically:\n- Generates JOIN ON clauses from common columns between predecessors\n- Writes the complete FROM/JOIN/ON/GROUP BY clause to the node's joinCondition (no separate update needed)\n- Replaces columns with GROUP BY dimensions + aggregate measures\n- Infers datatypes from transform functions (COUNT → NUMBER, SUM → NUMBER(38,4), etc.)\n- Sets column-level attributes (isBusinessKey on GROUP BY columns, isChangeTracking on aggregates)\n- Validates that all non-aggregate columns are in GROUP BY\n- Runs intelligent config completion\n\nUse this to transform a simple join (row-level) into an aggregated fact table (summary-level).\n\nExample: Convert order detail join to customer metrics:\n{\n  workspaceID: \"1\",\n  nodeID: \"fact-node-id\",\n  groupByColumns: ['\"STG_ORDER_HEADER\".\"CUSTOMER_ID\"'],\n  aggregates: [\n    { name: \"TOTAL_ORDERS\", function: \"COUNT\", expression: 'DISTINCT \"STG_ORDER_HEADER\".\"ORDER_ID\"' },\n    { name: \"LIFETIME_VALUE\", function: \"SUM\", expression: '\"STG_ORDER_HEADER\".\"ORDER_TOTAL\"' },\n    { name: \"AVG_ORDER_VALUE\", function: \"AVG\", expression: '\"STG_ORDER_HEADER\".\"ORDER_TOTAL\"' }\n  ],\n  joinType: \"INNER JOIN\"\n}\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - nodeID (string, required): The node ID of the join to convert\n  - groupByColumns (string[], required): Columns to group by (dimensions)\n  - aggregates (array, required): Aggregate columns with functions and expressions\n  - joinType (string, optional): JOIN type (default: INNER JOIN)\n  - maintainJoins (boolean, optional): Generate JOINs (default: true)\n  - repoPath (string, optional): Local repo path for config completion\n\nReturns:\n  Updated node with new columns, joinCondition, GROUP BY analysis, and config completion results.",
      inputSchema: z.object({
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
      }),
      outputSchema: getToolOutputSchema("convert_join_to_aggregation"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await convertJoinToAggregation(client, params);
        return buildJsonToolResponse("convert_join_to_aggregation", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "apply_join_condition",
    {
      title: "Apply Join Condition",
      description:
        "Write a FROM/JOIN/ON clause to a workspace node's sourceMapping.join.joinCondition by analyzing predecessor columns and generating the join automatically.\n\nUse this for multi-predecessor nodes where you need to combine data via JOIN. The tool inspects predecessor columns, finds common column names for ON conditions, and writes the full joinCondition to the node.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - nodeID (string, required): The node ID of the multi-predecessor node\n  - joinType (enum, optional): 'INNER JOIN' | 'LEFT JOIN' | 'RIGHT JOIN' | 'FULL OUTER JOIN'. Defaults to INNER JOIN\n  - whereClause (string, optional): WHERE filter to append after the JOIN (without the WHERE keyword)\n  - qualifyClause (string, optional): QUALIFY clause to append (without the QUALIFY keyword)\n  - joinColumnOverrides (array, optional): Explicit column mappings when column names differ across predecessors. Each entry: { leftPredecessor, rightPredecessor, leftColumn, rightColumn }\n\nReturns:\n  { nodeID, joinCondition, warning?, validation? }",
      inputSchema: z.object({
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
      }),
      outputSchema: getToolOutputSchema("apply_join_condition"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await applyJoinCondition(client, params);
        return buildJsonToolResponse("apply_join_condition", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "create_workspace_node_from_predecessor",
    {
      title: "Create Workspace Node from Predecessor",
      description:
        "Create a workspace node from one or more predecessor nodes, fetch it, and verify that columns were auto-populated from those predecessors before applying any optional changes.\n\nSINGLE-CALL WORKFLOW: You can create a node AND apply column transforms, WHERE filters, or aggregation in one call:\n- columns + whereCondition: Replace auto-populated columns with specific transforms and add a WHERE filter — no separate replace_workspace_node_columns needed\n- groupByColumns + aggregates: Convert to an aggregation node with GROUP BY — no separate convert_join_to_aggregation needed\nThese are mutually exclusive: use columns OR groupByColumns+aggregates, not both.\n\nREQUIRED: Before calling this tool, call `plan_pipeline` with `goal`, `sourceNodeIDs`, and `repoPath` to discover and rank available node types. Use the `nodeType` from the plan result — do NOT guess or hardcode node types like 'Stage', 'View', or numeric IDs like '65'. The planner scans all committed node type definitions and scores them against your use case.\n\nSPECIALIZED TYPES WARNING: Do NOT use Dynamic Tables, Incremental Load, Materialized View, or other specialized types unless the user explicitly requests that pattern (e.g., 'near-real-time refresh', 'continuous refresh', 'incremental processing'). For standard batch ETL, CTE decomposition, and general transforms, use Stage or Work. The response includes nodeTypeValidation.warning if a specialized pattern was detected without matching context — always check this field.\n\nJOIN INTELLIGENCE: For multi-predecessor nodes (joins), this tool automatically:\n- Analyzes common columns between each predecessor pair\n- Returns `joinSuggestions` with normalized column names and their left/right counterparts\n- Reports which predecessors are represented in the resulting column references\n- Warns if any predecessor is missing from the auto-populated columns\n\nAUTOMATIC CONFIG: When repoPath is provided, this tool automatically runs intelligent config completion after creation — reading the node type definition, setting node-level config defaults, and applying column-level attributes (isBusinessKey, isChangeTracking, etc.). The configCompletion result shows what was applied.\n\nDo not use overrideSQL or override.* fields; SQL override is disallowed in this project.\n\nFor guidance on node types, storage locations, and SQL patterns, see resources: coalesce://context/data-engineering-principles, coalesce://context/storage-mappings, coalesce://context/sql-platform-selection\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - nodeType (string, required): Node type from plan_pipeline\n  - predecessorNodeIDs (string[], required): One or more predecessor node IDs\n  - changes (object, optional): Partial fields to apply after creation\n  - columns (array, optional): Replace auto-populated columns (mutually exclusive with groupByColumns)\n  - whereCondition (string, optional): WHERE filter (only with columns)\n  - groupByColumns (string[], optional): GROUP BY columns (with aggregates)\n  - aggregates (array, optional): Aggregate columns (with groupByColumns)\n  - joinType (string, optional): JOIN type for multi-predecessor aggregation\n  - repoPath (string, optional): Local repo path for config completion\n  - goal (string, optional): Intent for node type validation\n\nReturns:\n  { nodeID, created, joinSuggestions?, nodeTypeValidation?, configCompletion? }",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        nodeType: z.string().describe("The type of node to create. IMPORTANT: Call plan_pipeline first to discover and rank available node types — use the nodeType from its result. Format: 'PackageName:::ID' for package types (e.g., 'base-nodes:::Stage') or simple name ('Stage') for built-in types. Always prefer the package-prefixed format returned by plan_pipeline."),
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
          .describe("The goal or intent for this node (e.g., 'deduplicate customer records', 'join orders with customers'). Used to validate that the chosen nodeType is appropriate for the task. Same value you would pass to plan_pipeline."),
      }),
      outputSchema: getToolOutputSchema("create_workspace_node_from_predecessor"),
      annotations: WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await createWorkspaceNodeFromPredecessor(client, params);
        return buildJsonToolResponse("create_workspace_node_from_predecessor", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "analyze_workspace_patterns",
    {
      title: "Analyze Workspace Patterns",
      description:
        "Analyze workspace node patterns to detect package adoption, pipeline layers, data modeling methodology, and generate recommendations.\n\nThis tool examines existing workspace nodes to understand conventions before creating new nodes. Results are returned as a workspace profile summary. If you want a reusable local snapshot instead of inline data, use cache_workspace_nodes.\n\nArgs:\n  - workspaceID (string, required): The workspace ID to analyze\n\nReturns:\n  Workspace profile with package adoption, pipeline layers, naming conventions, and recommendations.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID to analyze"),
      }),
      outputSchema: getToolOutputSchema("analyze_workspace_patterns"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const nodesResponse = await fetchAllWorkspaceNodes(client, {
          workspaceID: params.workspaceID,
          detail: false,
        });
        const nodes = toNodeSummaries(nodesResponse.items);

        const profile = buildWorkspaceProfile(params.workspaceID, nodes);
        return buildJsonToolResponse("analyze_workspace_patterns", profile);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "list_workspace_node_types",
    {
      title: "List Workspace Node Types",
      description:
        "List distinct node types observed in current workspace nodes. This scans existing nodes only; it is not a true installed-type registry.\n\nWARNING: Do NOT use these values directly as the nodeType parameter for create_workspace_node_from_predecessor or create_workspace_node_from_scratch. The observed values may be bare numeric IDs (e.g. '31') that differ from the proper package-prefixed format (e.g. 'base-nodes:::Stage'). Always call plan_pipeline first to discover the correct nodeType.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n\nReturns:\n  { nodeTypes: { id, name, count }[] }",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
      }),
      outputSchema: getToolOutputSchema("list_workspace_node_types"),
      annotations: READ_ONLY_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await listWorkspaceNodeTypes(client, params);
        return buildJsonToolResponse("list_workspace_node_types", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "delete_workspace_node",
    {
      title: "Delete Workspace Node",
      description:
        "Permanently delete a workspace node. Destructive — check for downstream dependencies first.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - nodeID (string, required): The node ID\n\nReturns:\n  Confirmation message.",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        nodeID: z.string().describe("The node ID to delete"),
      }),
      outputSchema: getToolOutputSchema("delete_workspace_node"),
      annotations: DESTRUCTIVE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await deleteWorkspaceNode(client, params);
        return buildJsonToolResponse("delete_workspace_node", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "complete_node_configuration",
    {
      title: "Complete Node Configuration",
      description:
        "Run intelligent configuration completion on a workspace node. Analyzes the node's type definition, existing config, and column layout to fill in required and recommended configuration fields.\n\nBoth creation tools call this internally, but you can invoke it separately after manual edits.\n\nArgs:\n  - workspaceID (string, required): The workspace ID\n  - nodeID (string, required): The node ID\n  - repoPath (string, optional): Local repo path for type definition lookup\n\nReturns:\n  { nodeID, configCompletion? | configCompletionSkipped? }",
      inputSchema: z.object({
        workspaceID: z.string().describe("The workspace ID"),
        nodeID: z.string().describe("The node ID to configure"),
        repoPath: z.string().optional().describe("Optional path to local Coalesce repository for schema resolution"),
      }),
      outputSchema: getToolOutputSchema("complete_node_configuration"),
      annotations: IDEMPOTENT_WRITE_ANNOTATIONS,
    },
    async (params) => {
      try {
        const result = await completeNodeConfiguration(client, params);
        return buildJsonToolResponse("complete_node_configuration", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
