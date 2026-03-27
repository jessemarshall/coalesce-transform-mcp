import type { CoalesceClient } from "../../client.js";
import { getWorkspaceNode } from "../../coalesce/api/nodes.js";
import { resolveNodeTypeSchema } from "./schema-resolver.js";
import { analyzeNodeContext, type NodeContext } from "./context-analyzer.js";
import { classifyConfigFields, type ClassifiedFields } from "./field-classifier.js";
import { applyIntelligenceRules } from "./rules.js";
import { updateWorkspaceNode } from "../workspace/mutations.js";
import { isPlainObject } from "../../utils.js";
import { NODE_TYPE_INTENT, type NodeTypeIntent } from "../pipelines/node-type-intent.js";
import { inferFamily, type PipelineNodeTypeFamily } from "../pipelines/node-type-selection.js";

export interface ConfigReview {
  status: "complete" | "needs_attention" | "incomplete";
  summary: string;
  missingRequired: string[];
  warnings: string[];
  suggestions: string[];
}

export interface ConfigCompletionResult {
  node: unknown;
  schemaSource: "repo" | "corpus";
  classification: {
    required: string[];
    conditionalRequired: string[];
    optionalWithDefaults: string[];
    contextual: string[];
    columnSelectors: Array<{
      attributeName: string;
      displayName: string | undefined;
      isRequired: boolean;
    }>;
  };
  context: {
    hasMultipleSources: boolean;
    hasAggregates: boolean;
    hasTimestampColumns: boolean;
    hasType2Pattern: boolean;
    materializationType: "table" | "view";
  };
  appliedConfig: Record<string, unknown>;
  configChanges: {
    required: Record<string, unknown>;
    contextual: Record<string, unknown>;
    preserved: Record<string, unknown>;
    defaults: Record<string, unknown>;
  };
  columnAttributeChanges: {
    applied: Array<{ columnName: string; attribute: string; value: boolean }>;
    reasoning: string[];
  };
  reasoning: string[];
  detectedPatterns: {
    candidateColumns: string[];
  };
  configReview: ConfigReview;
}

function getNodeMetadataColumns(node: Record<string, unknown>): Array<Record<string, unknown>> {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  const columns = metadata?.["columns"];
  if (!Array.isArray(columns)) {
    return [];
  }
  return columns.filter(isPlainObject);
}

/**
 * Intelligently determines which columns should receive a columnSelector attribute.
 * Returns a map of columnName → true for columns that should be marked.
 */
function inferColumnSelectorAssignments(
  attributeName: string,
  columns: Array<Record<string, unknown>>,
  context: ReturnType<typeof analyzeNodeContext>
): { assignments: Map<string, boolean>; reasoning: string } {
  const assignments = new Map<string, boolean>();

  switch (attributeName) {
    case "isBusinessKey": {
      // Use columns already marked, or fall back to detected business key candidates
      const alreadyMarked = columns.filter((c) => c.isBusinessKey === true);
      if (alreadyMarked.length > 0) {
        for (const col of alreadyMarked) {
          if (typeof col.name === "string") {
            assignments.set(col.name, true);
          }
        }
        return { assignments, reasoning: `Preserved ${alreadyMarked.length} existing isBusinessKey column(s)` };
      }

      // Infer from column name patterns
      for (const candidateName of context.columnPatterns.businessKeys) {
        assignments.set(candidateName, true);
      }
      if (assignments.size > 0) {
        return { assignments, reasoning: `Inferred isBusinessKey from ID/KEY/CODE column name patterns: ${[...assignments.keys()].join(", ")}` };
      }
      return { assignments, reasoning: "No business key candidates detected — set isBusinessKey manually on the appropriate column(s)" };
    }

    case "isChangeTracking": {
      // Use columns already marked, or fall back to detected candidates
      const alreadyMarked = columns.filter((c) => c.isChangeTracking === true);
      if (alreadyMarked.length > 0) {
        for (const col of alreadyMarked) {
          if (typeof col.name === "string") {
            assignments.set(col.name, true);
          }
        }
        return { assignments, reasoning: `Preserved ${alreadyMarked.length} existing isChangeTracking column(s)` };
      }

      // For change tracking, mark non-key mutable columns
      for (const candidateName of context.columnPatterns.changeTrackingCandidates) {
        assignments.set(candidateName, true);
      }
      if (assignments.size > 0) {
        return { assignments, reasoning: `Inferred isChangeTracking for non-key columns: ${[...assignments.keys()].join(", ")}` };
      }
      return { assignments, reasoning: "No change tracking candidates detected" };
    }

    default: {
      // For unknown columnSelector attributes, preserve existing values only
      const alreadyMarked = columns.filter((c) => c[attributeName] === true);
      for (const col of alreadyMarked) {
        if (typeof col.name === "string") {
          assignments.set(col.name, true);
        }
      }
      return {
        assignments,
        reasoning: alreadyMarked.length > 0
          ? `Preserved ${alreadyMarked.length} existing ${attributeName} column(s)`
          : `Unknown columnSelector '${attributeName}' — skipped automatic assignment. Set manually if needed.`,
      };
    }
  }
}

function inferFamilyFromNodeType(nodeType: string): PipelineNodeTypeFamily {
  return inferFamily([nodeType]);
}

/**
 * Build a config review that summarizes the state of the node's configuration
 * based on the node type's intent, what fields were filled, and what's still missing.
 */
function buildConfigReview(
  nodeType: string,
  appliedConfig: Record<string, unknown>,
  classification: ClassifiedFields,
  context: NodeContext,
  columnAttributeApplied: Array<{ columnName: string; attribute: string; value: boolean }>
): ConfigReview {
  const missingRequired: string[] = [];
  const warnings: string[] = [];
  const suggestions: string[] = [];

  // Check for missing required fields that weren't filled
  for (const fieldName of classification.required) {
    if (!(fieldName in appliedConfig) || appliedConfig[fieldName] === undefined) {
      missingRequired.push(fieldName);
    }
  }

  // Intent-aware checks based on node type family
  const family = inferFamilyFromNodeType(nodeType);
  const intent: NodeTypeIntent = NODE_TYPE_INTENT[family];

  // Check column-level attributes
  const hasBusinessKeySelector = classification.columnSelectors.some(
    (s) => s.attributeName === "isBusinessKey"
  );
  const businessKeysApplied = columnAttributeApplied.filter(
    (a) => a.attribute === "isBusinessKey" && a.value
  );

  if (intent.requiresSemanticConfig) {
    // Node types that require semantic config need business keys
    if (hasBusinessKeySelector && businessKeysApplied.length === 0 && context.columnPatterns.businessKeys.length === 0) {
      warnings.push(
        `${family} nodes require business keys but none were detected or set. ` +
        `Set isBusinessKey: true on the appropriate column(s) via replace_workspace_node_columns.`
      );
    }

    // Dimension/Persistent Stage with no change tracking
    const hasChangeTrackingSelector = classification.columnSelectors.some(
      (s) => s.attributeName === "isChangeTracking"
    );
    if (hasChangeTrackingSelector) {
      const changeTrackingApplied = columnAttributeApplied.filter(
        (a) => a.attribute === "isChangeTracking" && a.value
      );
      if (changeTrackingApplied.length === 0 && context.columnPatterns.changeTrackingCandidates.length === 0) {
        suggestions.push(
          `${family} supports change tracking but no isChangeTracking columns were detected. ` +
          `If CDC is needed, set isChangeTracking: true on mutable columns.`
        );
      }
    }
  }

  // Check for required columnSelectors that have no assignments
  for (const selector of classification.columnSelectors) {
    if (selector.isRequired) {
      const applied = columnAttributeApplied.filter(
        (a) => a.attribute === selector.attributeName && a.value
      );
      if (applied.length === 0) {
        missingRequired.push(`columnSelector:${selector.attributeName}`);
      }
    }
  }

  // Materialization-specific suggestions
  if (context.materializationType === "view" && context.hasAggregates) {
    suggestions.push(
      "This view contains aggregations that recalculate on every query. " +
      "Consider table materialization if performance is important."
    );
  }

  // Determine overall status
  let status: ConfigReview["status"];
  if (missingRequired.length > 0) {
    status = "incomplete";
  } else if (warnings.length > 0) {
    status = "needs_attention";
  } else {
    status = "complete";
  }

  // Build summary
  let summary: string;
  if (status === "complete") {
    summary = `Config is complete. All required fields are set.`;
    if (suggestions.length > 0) {
      summary += ` ${suggestions.length} optional suggestion(s) available.`;
    }
  } else if (status === "needs_attention") {
    summary = `Config has ${warnings.length} warning(s) that may need manual review.`;
  } else {
    summary = `Config is incomplete — ${missingRequired.length} required field(s) missing: ${missingRequired.join(", ")}.`;
  }

  return { status, summary, missingRequired, warnings, suggestions };
}

export async function completeNodeConfiguration(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    repoPath?: string;
  }
): Promise<ConfigCompletionResult> {
  // Step 1: Fetch node
  const node = await getWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
  });

  if (!isPlainObject(node)) {
    throw new Error("Node response was not an object");
  }

  const nodeType = typeof node.nodeType === "string" ? node.nodeType : "";
  if (!nodeType) {
    throw new Error("Node has no nodeType");
  }

  // Step 2: Resolve schema
  const schemaResolution = await resolveNodeTypeSchema(nodeType, params.repoPath);

  // Step 3: Analyze context
  const context = analyzeNodeContext(node);

  // Step 4: Classify fields (now separates columnSelector items)
  const classification = classifyConfigFields(schemaResolution.schema.config);

  // Step 5: Apply intelligence rules (get suggestions)
  const rulesResult = applyIntelligenceRules(context);

  // Step 6: Build config changes from rules and schema defaults
  const existingConfig = isPlainObject(node.config) ? node.config : {};

  const requiredChanges: Record<string, unknown> = {};
  const contextualChanges: Record<string, unknown> = {};
  const preservedFields: Record<string, unknown> = {};
  const defaultChanges: Record<string, unknown> = {};

  // Apply required field defaults from schema
  for (const fieldName of classification.required) {
    if (!(fieldName in existingConfig)) {
      for (const group of schemaResolution.schema.config) {
        for (const item of group.items) {
          if (item.attributeName === fieldName && item.default !== undefined) {
            requiredChanges[fieldName] = item.default;
          }
        }
      }
    } else {
      preservedFields[fieldName] = existingConfig[fieldName];
    }
  }

  // Apply schema defaults for optional fields that aren't set yet
  for (const fieldName of classification.optionalWithDefaults) {
    if (!(fieldName in existingConfig)) {
      for (const group of schemaResolution.schema.config) {
        for (const item of group.items) {
          if (item.attributeName === fieldName && item.default !== undefined) {
            defaultChanges[fieldName] = item.default;
          }
        }
      }
    }
  }

  // Apply contextual suggestions from rules
  for (const [key, value] of Object.entries(rulesResult.suggestions)) {
    if (classification.contextual.includes(key) || classification.optionalWithDefaults.includes(key)) {
      contextualChanges[key] = value;
    }
  }

  // Merge all config changes (contextual overrides defaults when both apply)
  const appliedConfig: Record<string, unknown> = {
    ...existingConfig,
    ...defaultChanges,
    ...requiredChanges,
    ...contextualChanges,
  };

  // Step 7: Handle columnSelector attributes (column-level)
  const columns = getNodeMetadataColumns(node);
  const columnAttributeApplied: ConfigCompletionResult["columnAttributeChanges"]["applied"] = [];
  const columnAttributeReasoning: string[] = [];
  let columnsModified = false;

  for (const selector of classification.columnSelectors) {
    const { assignments, reasoning } = inferColumnSelectorAssignments(
      selector.attributeName,
      columns,
      context
    );
    columnAttributeReasoning.push(`${selector.attributeName}: ${reasoning}`);

    for (const [colName, value] of assignments) {
      const col = columns.find(
        (c) => typeof c.name === "string" && c.name === colName
      );
      if (col && col[selector.attributeName] !== value) {
        col[selector.attributeName] = value;
        columnsModified = true;
        columnAttributeApplied.push({
          columnName: colName,
          attribute: selector.attributeName,
          value,
        });
      }
    }
  }

  // Step 8: Update node with config changes and/or column attribute changes
  const hasConfigChanges =
    Object.keys(requiredChanges).length > 0 ||
    Object.keys(contextualChanges).length > 0 ||
    Object.keys(defaultChanges).length > 0;

  let updatedNode: unknown = node;
  if (hasConfigChanges || columnsModified) {
    const changes: Record<string, unknown> = {};
    if (hasConfigChanges) {
      changes.config = appliedConfig;
    }
    if (columnsModified) {
      changes.metadata = { columns };
    }
    updatedNode = await updateWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: params.nodeID,
      changes,
    });
  }

  // Step 9: Detect candidate columns for reporting
  const candidateColumns = columns
    .filter((col) => {
      const name = typeof col.name === "string" ? col.name : "";
      return /_(ID|KEY|CODE|NUM)$/i.test(name) || /^(ID|KEY|CODE)_/i.test(name);
    })
    .flatMap((col) => (typeof col.name === "string" ? [col.name] : []));

  // Step 10: Build config review — summarizes what's set, what's missing, and what needs attention
  const configReview = buildConfigReview(
    nodeType,
    appliedConfig,
    classification,
    context,
    columnAttributeApplied
  );

  return {
    node: updatedNode,
    schemaSource: schemaResolution.source,
    classification,
    context: {
      hasMultipleSources: context.hasMultipleSources,
      hasAggregates: context.hasAggregates,
      hasTimestampColumns: context.hasTimestampColumns,
      hasType2Pattern: context.hasType2Pattern,
      materializationType: context.materializationType,
    },
    appliedConfig,
    configChanges: {
      required: requiredChanges,
      contextual: contextualChanges,
      preserved: preservedFields,
      defaults: defaultChanges,
    },
    columnAttributeChanges: {
      applied: columnAttributeApplied,
      reasoning: columnAttributeReasoning,
    },
    reasoning: rulesResult.reasoning,
    detectedPatterns: {
      candidateColumns,
    },
    configReview,
  };
}
