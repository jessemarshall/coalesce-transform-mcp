import { type CoalesceClient } from "../../client.js";
import { getWorkspaceNode } from "../../coalesce/api/nodes.js";
import { isPlainObject } from "../../utils.js";
import { normalizeColumnName } from "./node-inspection.js";
import type { ConfigCompletionResult } from "../config/intelligent.js";
import {
  buildPredecessorSummary,
  buildJoinSuggestions,
  generateJoinSQL,
  generateRefJoinSQL,
  inferDatatype,
  analyzeColumnsForGroupBy,
  extractPredecessorNodeIDs,
  extractPredecessorRefInfo,
  appendWhereToJoinCondition,
  type PredecessorSummary,
  type JoinSuggestion,
  type GroupByAnalysis,
  type ColumnTransform,
  type PredecessorRefInfo,
} from "./join-helpers.js";
import {
  tryCompleteNodeConfiguration,
  buildUpdatedWorkspaceNodeBody,
} from "./node-update-helpers.js";
import {
  updateWorkspaceNode,
  replaceWorkspaceNodeColumns,
} from "./mutations.js";

export async function convertJoinToAggregation(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    groupByColumns: string[];
    aggregates: Array<{
      name: string;
      function: string;
      expression: string;
      description?: string;
    }>;
    joinType?: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN";
    maintainJoins?: boolean;
    repoPath?: string;
  }
): Promise<{
  node: unknown;
  joinSQL: {
    fromClause: string;
    joinClauses: string[];
    fullSQL: string;
    warnings: string[];
  };
  groupByAnalysis: GroupByAnalysis;
  validation: {
    valid: boolean;
    warnings: string[];
  };
  configCompletion?: ConfigCompletionResult;
  configCompletionSkipped?: string;
}> {
  // Get the current node to analyze predecessors
  const current = await getWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
  });

  if (!isPlainObject(current)) {
    throw new Error("Node response was not an object");
  }

  const metadata = isPlainObject(current.metadata) ? current.metadata : {};
  const predecessorNodeIDs = extractPredecessorNodeIDs(metadata);

  // Fetch predecessor nodes to build join suggestions and ref info
  const predecessorNodes: PredecessorSummary[] = [];
  const predecessorRefInfos: PredecessorRefInfo[] = [];
  const shouldMaintainJoins = params.maintainJoins !== false; // default true
  if (shouldMaintainJoins && predecessorNodeIDs.length > 0) {
    const fetched = await Promise.all(
      predecessorNodeIDs.map(async (nodeID) => ({
        nodeID,
        node: await getWorkspaceNode(client, { workspaceID: params.workspaceID, nodeID }),
      }))
    );
    for (const { nodeID, node: predecessor } of fetched) {
      if (isPlainObject(predecessor)) {
        predecessorNodes.push(buildPredecessorSummary(nodeID, predecessor));
        const refInfo = extractPredecessorRefInfo(nodeID, predecessor);
        if (refInfo) {
          predecessorRefInfos.push(refInfo);
        }
      }
    }
  }

  // Generate JOIN SQL with {{ ref() }} syntax if maintaining joins
  const joinSuggestions = buildJoinSuggestions(predecessorNodes);
  let joinSQL: { fromClause: string; joinClauses: string[]; fullSQL: string; warnings: string[] };
  if (predecessorRefInfos.length >= 2) {
    joinSQL = generateRefJoinSQL(
      predecessorRefInfos,
      joinSuggestions,
      params.joinType || "INNER JOIN"
    );
  } else {
    // Fallback: predecessors missing locationName — use bare-name join SQL
    const bareJoin = generateJoinSQL(joinSuggestions, params.joinType || "INNER JOIN");
    joinSQL = {
      fromClause: bareJoin.fromClause,
      joinClauses: bareJoin.joinClauses.map((jc) =>
        `${jc.type} ${jc.rightTableAlias}\n  ON ${jc.onConditions.join("\n  AND ")}`
      ),
      fullSQL: bareJoin.fullSQL,
      warnings: predecessorNodeIDs.length >= 2
        ? ["Predecessors are missing locationName — generated join uses bare table names instead of {{ ref() }} syntax. Set locationName on predecessor nodes for proper Coalesce references."]
        : [],
    };
  }

  // Build a lookup map of existing column datatypes from the current node
  // so GROUP BY pass-through columns can inherit their predecessor's dataType
  const existingColumns = Array.isArray(metadata.columns) ? metadata.columns : [];
  const existingDataTypeByName = new Map<string, string>();
  for (const col of existingColumns) {
    if (isPlainObject(col) && typeof col.name === "string" && typeof col.dataType === "string") {
      existingDataTypeByName.set(normalizeColumnName(col.name), col.dataType);
    }
  }

  // Build columns: group by columns + aggregates
  const columns: ColumnTransform[] = [];

  // Add GROUP BY columns
  for (const groupByCol of params.groupByColumns) {
    const colName = groupByCol.split(".").pop()?.replace(/"/g, "") || groupByCol;
    const inferredDatatype = inferDatatype(groupByCol)
      ?? existingDataTypeByName.get(normalizeColumnName(colName))
      ?? "VARCHAR";
    columns.push({
      name: colName,
      transform: groupByCol,
      dataType: inferredDatatype,
    });
  }

  // Add aggregate columns
  for (const agg of params.aggregates) {
    const transform = `${agg.function}(${agg.expression})`;
    const inferredDatatype = inferDatatype(transform) ?? "VARCHAR";
    columns.push({
      name: agg.name,
      transform,
      dataType: inferredDatatype,
      description: agg.description,
    });
  }

  // Analyze GROUP BY requirements
  const groupByAnalysis = analyzeColumnsForGroupBy(columns);

  const warnings: string[] = [];
  if (!groupByAnalysis.validation.valid) {
    warnings.push(...groupByAnalysis.validation.errors);
  }

  // Derive business key and change tracking column names
  // Business key = GROUP BY columns (dimensions)
  // Change tracking = aggregate columns (measures that change over time)
  const businessKeyColumnNames = new Set(
    params.groupByColumns.map(
      (col) => col.split(".").pop()?.replace(/"/g, "") || col
    )
  );
  const changeTrackingColumnNames = new Set(
    params.aggregates.map((agg) => agg.name)
  );

  // Convert columns to metadata format with column-level attributes
  // columnSelector attributes (isBusinessKey, isChangeTracking) are set directly
  // on each column object — this is how Coalesce node type definitions work
  const metadataColumns = columns.map((col) => {
    const metadataCol: Record<string, unknown> = {
      name: col.name,
      dataType: col.dataType ?? "VARCHAR",
      transform: col.transform,
      nullable: true,
    };
    if (col.description) {
      metadataCol.description = col.description;
    }
    if (businessKeyColumnNames.has(col.name)) {
      metadataCol.isBusinessKey = true;
    }
    if (changeTrackingColumnNames.has(col.name)) {
      metadataCol.isChangeTracking = true;
    }
    return metadataCol;
  });

  // Replace columns with aggregation columns
  const updated = await replaceWorkspaceNodeColumns(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
    columns: metadataColumns,
  });

  // Write the generated JOIN SQL and/or GROUP BY directly to the node's sourceMapping.
  // Re-fetch the node to get fresh sourceMapping after column replacement.
  const hasJoinSQL = joinSQL.fullSQL.length > 0;
  const hasGroupBy = groupByAnalysis.groupByColumns.length > 0;

  if (hasJoinSQL || hasGroupBy) {
    const freshNode = await getWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: params.nodeID,
    });
    const freshMetadata = isPlainObject(freshNode) && isPlainObject(freshNode.metadata)
      ? freshNode.metadata
      : {};
    const freshSourceMapping = Array.isArray(freshMetadata.sourceMapping)
      ? freshMetadata.sourceMapping
      : [];
    const firstEntry = freshSourceMapping.find(isPlainObject);
    if (!firstEntry) {
      joinSQL.warnings.push(
        "Could not write joinCondition — node has no sourceMapping entries. " +
        "The generated SQL is returned but was not persisted to the node."
      );
    } else {
      const existingJoin = isPlainObject(firstEntry.join) ? firstEntry.join : {};
      const existingJoinCondition = typeof existingJoin.joinCondition === "string"
        ? existingJoin.joinCondition.trim()
        : "";

      let fullJoinCondition: string;
      if (hasJoinSQL) {
        // Multi-predecessor: use generated FROM/JOIN + GROUP BY
        const groupByClause = hasGroupBy
          ? `\nGROUP BY ${groupByAnalysis.groupByColumns.join(", ")}`
          : "";
        fullJoinCondition = joinSQL.fullSQL + groupByClause;
      } else {
        // Single-predecessor aggregation: append GROUP BY to existing joinCondition
        const groupByClause = `\nGROUP BY ${groupByAnalysis.groupByColumns.join(", ")}`;
        fullJoinCondition = existingJoinCondition.length > 0
          ? existingJoinCondition + groupByClause
          : groupByClause.trim();
      }

      const updatedSourceMapping = freshSourceMapping.map((entry) =>
        entry === firstEntry
          ? { ...firstEntry, join: { ...existingJoin, joinCondition: fullJoinCondition } }
          : entry
      );
      await updateWorkspaceNode(client, {
        workspaceID: params.workspaceID,
        nodeID: params.nodeID,
        changes: {
          metadata: {
            sourceMapping: updatedSourceMapping,
          },
        },
      });
    }
  }

  // Complete configuration with intelligent rules (best-effort)
  const completion = await tryCompleteNodeConfiguration(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
    repoPath: params.repoPath,
  });

  return {
    node: completion.configCompletion?.node ?? updated,
    joinSQL,
    groupByAnalysis,
    validation: {
      valid: groupByAnalysis.validation.valid && warnings.length === 0,
      warnings,
    },
    ...completion,
  };
}

export async function applyJoinCondition(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    joinType?: "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN";
    whereClause?: string;
    qualifyClause?: string;
    joinColumnOverrides?: Array<{
      leftPredecessor: string;
      rightPredecessor: string;
      leftColumn: string;
      rightColumn: string;
    }>;
  }
): Promise<{
  joinCondition: string;
  joinSuggestions: JoinSuggestion[];
  predecessors: Array<{ nodeID: string; nodeName: string; locationName: string; columnCount: number }>;
  warnings: string[];
}> {
  const joinType = params.joinType ?? "INNER JOIN";

  // Fetch the node to extract predecessors
  const current = await getWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
  });
  if (!isPlainObject(current)) {
    throw new Error("Node response was not an object");
  }

  const metadata = isPlainObject(current.metadata) ? current.metadata : {};
  const sourceMapping = Array.isArray(metadata.sourceMapping) ? metadata.sourceMapping : [];
  const predecessorNodeIDs = extractPredecessorNodeIDs(metadata);

  if (predecessorNodeIDs.length < 2) {
    throw new Error(
      "apply_join_condition requires a node with 2+ predecessors. " +
      "This node has " + predecessorNodeIDs.length + " predecessor(s). " +
      "For single-predecessor nodes, set the joinCondition directly via update_workspace_node."
    );
  }

  // Fetch all predecessors in parallel to get their names, locationNames, and columns
  const predecessorRefInfos: PredecessorRefInfo[] = [];
  const predecessorSummaries: PredecessorSummary[] = [];
  const warnings: string[] = [];

  const fetchedPredecessors = await Promise.all(
    predecessorNodeIDs.map(async (nodeID) => ({
      nodeID,
      node: await getWorkspaceNode(client, { workspaceID: params.workspaceID, nodeID }),
    }))
  );

  for (const { nodeID, node: predecessor } of fetchedPredecessors) {
    if (!isPlainObject(predecessor)) {
      warnings.push(`Could not fetch predecessor ${nodeID}`);
      continue;
    }
    const refInfo = extractPredecessorRefInfo(nodeID, predecessor);
    if (!refInfo) {
      const name = typeof predecessor.name === "string" ? predecessor.name : nodeID;
      warnings.push(
        `Predecessor "${name}" is missing locationName. ` +
        `Set it in the Coalesce UI or via update_workspace_node before applying joins.`
      );
      continue;
    }
    predecessorRefInfos.push(refInfo);
    predecessorSummaries.push(buildPredecessorSummary(nodeID, predecessor));
  }

  if (predecessorRefInfos.length < 2) {
    throw new Error(
      "Could not resolve 2+ predecessors with valid name and locationName. " +
      "Ensure all predecessor nodes have a locationName set."
    );
  }

  // Build join suggestions from common columns
  const joinSuggestions = buildJoinSuggestions(predecessorSummaries);

  // Generate FROM/JOIN/ON with {{ ref() }} syntax
  const joinResult = generateRefJoinSQL(
    predecessorRefInfos,
    joinSuggestions,
    joinType,
    params.joinColumnOverrides
  );
  warnings.push(...joinResult.warnings);

  // Build full joinCondition: FROM/JOIN + WHERE + QUALIFY
  const parts = [joinResult.fullSQL];
  if (params.whereClause) {
    const trimmedWhere = params.whereClause.trim();
    const whereStr = /^where\b/i.test(trimmedWhere)
      ? trimmedWhere
      : `WHERE ${trimmedWhere}`;
    parts.push(whereStr);
  }
  if (params.qualifyClause) {
    const trimmedQualify = params.qualifyClause.trim();
    const qualifyStr = /^qualify\b/i.test(trimmedQualify)
      ? trimmedQualify
      : `QUALIFY ${trimmedQualify}`;
    parts.push(qualifyStr);
  }

  const fullJoinCondition = parts.join("\n");

  // Write to node's sourceMapping
  const firstEntry = sourceMapping.find(isPlainObject);
  if (!firstEntry) {
    warnings.push(
      "Could not write joinCondition — node has no sourceMapping entries. " +
      "The generated SQL is returned but was not persisted to the node."
    );
  } else {
    const updatedSourceMapping = sourceMapping.map((entry) =>
      entry === firstEntry
        ? {
            ...firstEntry,
            join: {
              ...(isPlainObject(firstEntry.join) ? firstEntry.join : {}),
              joinCondition: fullJoinCondition,
            },
          }
        : entry
    );
    await updateWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: params.nodeID,
      changes: {
        metadata: {
          sourceMapping: updatedSourceMapping,
        },
      },
    });
  }

  return {
    joinCondition: fullJoinCondition,
    joinSuggestions,
    predecessors: predecessorRefInfos.map((p) => ({
      nodeID: p.nodeID,
      nodeName: p.nodeName,
      locationName: p.locationName,
      columnCount: p.columnNames.length,
    })),
    warnings,
  };
}
