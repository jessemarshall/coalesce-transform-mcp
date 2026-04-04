import { isPlainObject } from "../../utils.js";
import type { PlannedSelectItem, PlannedPipelineNode } from "./planning-types.js";
import { normalizeSqlIdentifier, deepClone, getUniqueSourceDependencies } from "./sql-parsing.js";

export function getColumnNamesFromNode(node: Record<string, unknown>): string[] {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!Array.isArray(metadata?.columns)) {
    return [];
  }

  return metadata.columns.flatMap((column) => {
    if (!isPlainObject(column) || typeof column.name !== "string") {
      return [];
    }
    return [column.name];
  });
}

export function getNodeColumnArray(node: Record<string, unknown>): Record<string, unknown>[] {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!Array.isArray(metadata?.columns)) {
    return [];
  }
  return metadata.columns.filter(isPlainObject);
}

export function getColumnSourceNodeIDs(column: Record<string, unknown>): string[] {
  if (!Array.isArray(column.sources)) {
    return [];
  }
  const ids = new Set<string>();
  for (const source of column.sources) {
    if (!isPlainObject(source) || !Array.isArray(source.columnReferences)) {
      continue;
    }
    for (const ref of source.columnReferences) {
      if (isPlainObject(ref) && typeof ref.nodeID === "string") {
        ids.add(ref.nodeID);
      }
    }
  }
  return Array.from(ids);
}

export function findMatchingBaseColumn(
  node: Record<string, unknown>,
  selectItem: PlannedSelectItem
): Record<string, unknown> | null {
  if (!selectItem.sourceColumnName) return null;
  const normalizedTargetName = normalizeSqlIdentifier(selectItem.sourceColumnName);
  for (const column of getNodeColumnArray(node)) {
    if (
      typeof column.name !== "string" ||
      normalizeSqlIdentifier(column.name) !== normalizedTargetName
    ) {
      continue;
    }

    const sourceNodeIDs = getColumnSourceNodeIDs(column);
    if (selectItem.sourceNodeID && sourceNodeIDs.includes(selectItem.sourceNodeID)) {
      return deepClone(column);
    }
    if (!selectItem.sourceNodeID) {
      return deepClone(column);
    }
  }

  return null;
}

export function renameSourceMappingEntries(
  node: Record<string, unknown>,
  newName: string
): Record<string, unknown> {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!metadata || !Array.isArray(metadata.sourceMapping)) {
    return node;
  }

  const previousName =
    typeof node.name === "string" && node.name.trim().length > 0 ? node.name : null;
  const updateSingleUnnamedMapping = previousName === null && metadata.sourceMapping.length === 1;

  return {
    ...node,
    metadata: {
      ...metadata,
      sourceMapping: metadata.sourceMapping.map((entry) => {
        if (!isPlainObject(entry)) {
          return entry;
        }
        const shouldRename =
          (previousName !== null && entry.name === previousName) ||
          updateSingleUnnamedMapping;
        if (!shouldRename) {
          return entry;
        }
        return {
          ...entry,
          name: newName,
        };
      }),
    },
  };
}

export function buildStageSourceMappingFromPlan(
  currentNode: Record<string, unknown>,
  nodePlan: PlannedPipelineNode
): Record<string, unknown>[] {
  const metadata = isPlainObject(currentNode.metadata) ? currentNode.metadata : undefined;
  const existingEntry =
    metadata && Array.isArray(metadata.sourceMapping)
      ? metadata.sourceMapping.find(isPlainObject)
      : undefined;

  const aliases: Record<string, string> = {};
  for (const ref of nodePlan.sourceRefs) {
    if (!ref.nodeID) {
      continue;
    }
    const alias = ref.alias ?? ref.nodeName;
    if (nodePlan.sourceRefs.length > 1 || ref.alias) {
      aliases[alias] = ref.nodeID;
    }
  }

  return [
    {
      ...(isPlainObject(existingEntry) ? existingEntry : {}),
      aliases,
      customSQL: {
        ...(isPlainObject(existingEntry) && isPlainObject(existingEntry.customSQL)
          ? existingEntry.customSQL
          : {}),
        customSQL: "",
      },
      dependencies: getUniqueSourceDependencies(nodePlan.sourceRefs),
      join: {
        ...(isPlainObject(existingEntry) && isPlainObject(existingEntry.join)
          ? existingEntry.join
          : {}),
        joinCondition: nodePlan.joinCondition ?? "",
      },
      name: nodePlan.name,
      noLinkRefs:
        isPlainObject(existingEntry) && Array.isArray(existingEntry.noLinkRefs)
          ? existingEntry.noLinkRefs
          : [],
    },
  ];
}
