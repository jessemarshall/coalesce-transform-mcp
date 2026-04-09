import type {
  PipelinePlan,
  CteNodeSummary,
} from "./planning-types.js";
import {
  isIdentifierChar,
  scanTopLevel,
} from "./sql-tokenizer.js";
import { type PipelineNodeTypeSelection } from "./node-type-selection.js";
import { escapeRegExp } from "./sql-utils.js";
import { type ParsedCte, classifyCtePattern, isAggregateFn } from "./cte-parsing.js";

/**
 * Build a per-CTE instruction block that tells the agent exactly what transforms
 * and filters to apply for this CTE.
 */
function buildCteNodeInstruction(cte: ParsedCte, nodeType: string): string {
  const lines: string[] = [];
  lines.push(`## ${cte.name}`);
  lines.push(`- nodeType: "${nodeType}"`);

  if (cte.sourceTable) {
    lines.push(`- source: ${cte.sourceTable}`);
  }

  const transforms = cte.columns.filter((c) => c.isTransform);
  const passthroughCols = cte.columns.filter((c) => !c.isTransform);

  if (cte.hasGroupBy) {
    lines.push(`- AGGREGATION NODE: pass groupByColumns + aggregates directly to create_workspace_node_from_predecessor (single call)`);
  } else if (cte.columns.length > 0) {
    lines.push(`- Pass columns array + whereCondition directly to create_workspace_node_from_predecessor (single call)`);
  }

  if (transforms.length > 0) {
    lines.push(`- Column transforms:`);
    for (const col of transforms) {
      lines.push(`  - ${col.outputName}: ${col.expression}`);
    }
  }

  if (passthroughCols.length > 0) {
    lines.push(`- Passthrough columns: ${passthroughCols.map((c) => c.outputName).join(", ")}`);
  }

  if (cte.columns.length > 0) {
    lines.push(`- ONLY keep these ${cte.columns.length} columns: ${cte.columns.map((c) => c.outputName).join(", ")}`);
  }

  if (cte.whereClause) {
    lines.push(`- WHERE filter (pass as whereCondition — do NOT construct {{ ref() }}): ${cte.whereClause}`);
  }

  if (cte.hasJoin) {
    lines.push(`- Has JOIN — use apply_join_condition or update_workspace_node for join setup`);
  }

  return lines.join("\n");
}

/**
 * When the user's SQL contains CTEs, return a plan that instructs the agent
 * to break each CTE into a separate Coalesce node using the declarative tools.
 * CTEs are not supported in Coalesce — each CTE should be its own node.
 *
 * The plan includes per-CTE structured data: column transforms, WHERE clauses,
 * source tables, and which columns to keep/remove.
 */
export function buildCtePlan(
  params: {
    workspaceID: string;
    goal?: string;
    sql?: string;
    targetName?: string;
  },
  ctes: ParsedCte[],
  nodeTypeSelections: {
    staging: PipelineNodeTypeSelection;
    multiSource: PipelineNodeTypeSelection;
    aggregation: PipelineNodeTypeSelection;
  }
): PipelinePlan {
  const stagingType = nodeTypeSelections.staging.selectedNodeType ?? "Stage";
  const multiSourceType = nodeTypeSelections.multiSource.selectedNodeType ?? stagingType;
  const aggregationType = nodeTypeSelections.aggregation.selectedNodeType ?? stagingType;

  const typeMap: Record<string, string> = {
    staging: stagingType,
    multiSource: multiSourceType,
    aggregation: aggregationType,
  };

  // Build per-CTE instructions
  const cteInstructions: string[] = [];
  for (const cte of ctes) {
    const pattern = classifyCtePattern(cte);
    const nodeType = typeMap[pattern]!;
    cteInstructions.push(buildCteNodeInstruction(cte, nodeType));
  }

  // Detect if any CTE references another CTE (pipeline dependency)
  const cteNameSet = new Set(ctes.map((c) => c.name));
  const cteDependencies: string[] = [];
  for (const cte of ctes) {
    const deps = ctes
      .filter((other) => other.name !== cte.name && new RegExp(`\\b${escapeRegExp(other.name)}\\b`, "iu").test(cte.body))
      .map((other) => other.name);
    if (deps.length > 0) {
      cteDependencies.push(`${cte.name} depends on: ${deps.join(", ")}`);
    }
  }

  // Detect the final SELECT after all CTEs
  const finalSelectNote = extractFinalSelectFromCteQuery(params.sql ?? "", cteNameSet);

  const allTransformCount = ctes.reduce(
    (sum, cte) => sum + cte.columns.filter((c) => c.isTransform).length,
    0
  );
  const allFilterCount = ctes.filter((c) => c.whereClause).length;

  // Build structured per-CTE summary for easy agent consumption
  const cteNodeSummary: CteNodeSummary[] = ctes.map((cte) => {
    const pattern = classifyCtePattern(cte);
    const nodeType = typeMap[pattern]!;
    const transforms = cte.columns.filter((c) => c.isTransform);

    const summary: CteNodeSummary = {
      name: cte.name,
      nodeType,
      pattern,
      sourceTable: cte.sourceTable,
      columnCount: cte.columns.length,
      transforms: transforms.map((c) => ({ column: c.outputName, expression: c.expression })),
      passthroughColumns: cte.columns.filter((c) => !c.isTransform).map((c) => c.outputName),
      whereFilter: cte.whereClause,
      hasGroupBy: cte.hasGroupBy,
      hasJoin: cte.hasJoin,
      dependsOn: ctes
        .filter((other) => other.name !== cte.name && new RegExp(`\\b${escapeRegExp(other.name)}\\b`, "iu").test(cte.body))
        .map((other) => other.name),
    };

    // Add structured params for single-call creation
    if (cte.hasGroupBy && cte.columns.length > 0) {
      const groupByCols: string[] = [];
      const aggCols: Array<{ name: string; function: string; expression: string }> = [];
      for (const col of cte.columns) {
        const aggMatch = col.expression.match(/^(\w+)\s*\((.*)\)$/s);
        if (col.isTransform && aggMatch && isAggregateFn(aggMatch[1]!)) {
          aggCols.push({
            name: col.outputName,
            function: aggMatch[1]!.toUpperCase(),
            expression: aggMatch[2]!.trim(),
          });
        } else {
          groupByCols.push(col.expression);
        }
      }
      if (groupByCols.length > 0 && aggCols.length > 0) {
        summary.groupByColumnsParam = groupByCols;
        summary.aggregatesParam = aggCols;
      }
    } else if (cte.columns.length > 0 && !cte.hasJoin) {
      summary.columnsParam = cte.columns.map((c) => ({
        name: c.outputName,
        ...(c.isTransform ? { transform: c.expression } : {}),
      }));
    }

    return summary;
  });

  return {
    version: 1,
    intent: "sql",
    status: "needs_clarification",
    STOP_AND_CONFIRM: `STOP. Present the pipeline summary to the user in a table format and ask for confirmation BEFORE creating any nodes. For EACH node in cteNodeSummary, display: name, the EXACT nodeType string (e.g. "Coalesce-Base-Node-Types:::Stage"), pattern, transforms, and whereFilter. Use the cteNodeSummary array — do NOT paraphrase or simplify the nodeType values. Do NOT proceed until the user explicitly approves.`,
    workspaceID: params.workspaceID,
    platform: null,
    goal: params.goal ?? null,
    sql: params.sql ?? null,
    nodes: [],
    cteNodeSummary,
    assumptions: [
      `Parsed ${ctes.length} CTEs with ${allTransformCount} column transforms and ${allFilterCount} WHERE filters.`,
      `Staging and aggregation CTEs: 1 call per node. Multi-source JOIN CTEs: 2 calls (create + apply_join_condition).`,
    ],
    openQuestions: [
      `STOP: Present this pipeline summary to the user and ask "Should I proceed with creating these ${ctes.length} nodes?" Do NOT create nodes until the user confirms.`,
      `This SQL uses CTEs (WITH ... AS), which Coalesce does not support as a single node. Each CTE must become a separate node.`,
      `--- PER-CTE INSTRUCTIONS ---\n\n${cteInstructions.join("\n\n")}`,
      ...(cteDependencies.length > 0
        ? [`CTE dependencies (create in order):\n${cteDependencies.map((d) => `  - ${d}`).join("\n")}`]
        : []),
      ...(finalSelectNote ? [finalSelectNote] : []),
      `Node type guidance (do NOT use list_workspace_node_types):\n` +
        `- Staging CTEs (single-source): nodeType "${stagingType}"\n` +
        `- Join/transform CTEs (multi-source): nodeType "${multiSourceType}"\n` +
        `- Aggregation CTEs (GROUP BY): nodeType "${aggregationType}"`,
      `Workflow per CTE:\n` +
        `create_workspace_node_from_predecessor accepts columns, whereCondition, groupByColumns, and aggregates directly:\n` +
        `- For staging/transform CTEs (single-source): 1 call — pass columns (from cteNodeSummary.columnsParam) + whereCondition\n` +
        `- For GROUP BY CTEs: 1 call — pass groupByColumns (from cteNodeSummary.groupByColumnsParam) + aggregates (from cteNodeSummary.aggregatesParam)\n` +
        `- For multi-source JOIN CTEs: 2 calls — first create_workspace_node_from_predecessor with columns + whereCondition, then apply_join_condition to set up FROM/JOIN/ON\n` +
        `- Do NOT construct {{ ref() }} syntax — the FROM clause and joins are auto-generated\n` +
        `- Pass repoPath to each call for automatic config completion`,
    ],
    warnings: [
      `SQL contains ${ctes.length} CTEs: ${ctes.map((c) => c.name).join(", ")}. Each must be a separate Coalesce node.` +
        (allTransformCount > 0 ? ` ${allTransformCount} column transforms detected.` : ``),
    ],
    supportedNodeTypes: nodeTypeSelections.staging.supportedNodeTypes.length > 0
      ? nodeTypeSelections.staging.supportedNodeTypes
      : [stagingType],
    nodeTypeSelection: nodeTypeSelections.staging,
  };
}

function extractFinalSelectFromCteQuery(sql: string, cteNames: Set<string>): string | null {
  const trimmed = sql.trim();
  let lastSelectIdx = -1;

  scanTopLevel(trimmed, (_char, index, parenDepth) => {
    if (
      parenDepth === 0 &&
      trimmed.slice(index, index + 6).toUpperCase() === "SELECT" &&
      !isIdentifierChar(trimmed[index - 1]) &&
      !isIdentifierChar(trimmed[index + 6])
    ) {
      lastSelectIdx = index;
    }
    return true;
  });

  if (lastSelectIdx < 0) return null;

  const finalSelect = trimmed.slice(lastSelectIdx).trim();
  const referencedCtes = [...cteNames].filter((name) =>
    new RegExp(`\\b${escapeRegExp(name)}\\b`, "i").test(finalSelect)
  );

  if (referencedCtes.length === 0) return null;

  const selectStarFromOne =
    referencedCtes.length === 1 &&
    /^SELECT\s+\*\s+FROM\s+\w+\s*;?\s*$/i.test(finalSelect);

  if (selectStarFromOne) {
    return (
      `Final SELECT is just \`SELECT * FROM ${referencedCtes[0]}\` — this is redundant. ` +
      `The last CTE node (${referencedCtes[0]}) already represents the final output. ` +
      `Do NOT create an additional node for this.`
    );
  }

  return (
    `Final output query references: ${referencedCtes.join(", ")}. ` +
    `Create a final node with these as predecessors. ` +
    `The final SELECT is:\n${finalSelect.slice(0, 500)}${finalSelect.length > 500 ? "..." : ""}`
  );
}
