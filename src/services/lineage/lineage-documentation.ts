import type { LineageCacheEntry } from "./lineage-cache.js";
import { isPlainObject } from "../../utils.js";

const MAX_UNDOCUMENTED_COLUMNS = 200;

export type UndocumentedNode = {
  nodeID: string;
  nodeName: string;
  nodeType: string;
};

export type UndocumentedColumn = {
  nodeID: string;
  nodeName: string;
  columnID: string;
  columnName: string;
};

export type DocumentationAuditResult = {
  workspaceID: string;
  auditedAt: string;
  totalNodes: number;
  documentedNodes: number;
  undocumentedNodes: number;
  nodeDocumentationPercent: number;
  totalColumns: number;
  documentedColumns: number;
  undocumentedColumns: number;
  columnDocumentationPercent: number;
  undocumentedNodeList: UndocumentedNode[];
  undocumentedColumnList: UndocumentedColumn[];
  truncatedColumns: boolean;
};

function hasNonEmptyDescription(value: unknown): boolean {
  return typeof value === "string" && value.trim().length > 0;
}

export function auditDocumentationCoverage(
  cache: LineageCacheEntry
): DocumentationAuditResult {
  let totalNodes = 0;
  let documentedNodes = 0;
  let totalColumns = 0;
  let documentedColumns = 0;
  const undocumentedNodeList: UndocumentedNode[] = [];
  const undocumentedColumnList: UndocumentedColumn[] = [];

  for (const node of cache.nodes.values()) {
    totalNodes++;

    // Check node-level description (raw.description OR raw.metadata.description,
    // matching lineage-search.ts extractDescription logic)
    const metaObj = isPlainObject(node.raw.metadata) ? node.raw.metadata : {};
    if (hasNonEmptyDescription(node.raw.description) || hasNonEmptyDescription(metaObj.description)) {
      documentedNodes++;
    } else {
      undocumentedNodeList.push({
        nodeID: node.id,
        nodeName: node.name,
        nodeType: node.nodeType,
      });
    }

    // Check column-level descriptions via raw metadata
    const rawColumns = Array.isArray(metaObj.columns)
      ? metaObj.columns
      : [];

    for (const col of node.columns) {
      totalColumns++;

      // Find the matching raw column to check its description
      const rawCol = rawColumns.find((rc: unknown) => {
        if (!isPlainObject(rc)) return false;
        const rcId =
          typeof rc.columnID === "string"
            ? rc.columnID
            : typeof rc.id === "string"
              ? rc.id
              : undefined;
        return rcId === col.id;
      });

      if (rawCol && isPlainObject(rawCol) && hasNonEmptyDescription(rawCol.description)) {
        documentedColumns++;
      } else if (undocumentedColumnList.length < MAX_UNDOCUMENTED_COLUMNS) {
        undocumentedColumnList.push({
          nodeID: node.id,
          nodeName: node.name,
          columnID: col.id,
          columnName: col.name,
        });
      }
    }
  }

  const undocumentedNodes = totalNodes - documentedNodes;
  const undocumentedColumnsCount = totalColumns - documentedColumns;

  return {
    workspaceID: cache.workspaceID,
    auditedAt: new Date().toISOString(),
    totalNodes,
    documentedNodes,
    undocumentedNodes,
    nodeDocumentationPercent:
      totalNodes > 0
        ? Math.round((documentedNodes / totalNodes) * 10000) / 100
        : 0,
    totalColumns,
    documentedColumns,
    undocumentedColumns: undocumentedColumnsCount,
    columnDocumentationPercent:
      totalColumns > 0
        ? Math.round((documentedColumns / totalColumns) * 10000) / 100
        : 0,
    undocumentedNodeList,
    undocumentedColumnList,
    truncatedColumns: undocumentedColumnsCount > MAX_UNDOCUMENTED_COLUMNS,
  };
}
