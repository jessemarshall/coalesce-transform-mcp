/**
 * Pure read-only helpers for extracting properties from untyped workspace node objects.
 */

import { isPlainObject } from "../../utils.js";

export function getNodeColumnCount(node: Record<string, unknown>): number {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  return Array.isArray(metadata?.columns) ? metadata.columns.length : 0;
}

export function getNodeStorageLocationCount(node: Record<string, unknown>): number {
  return Array.isArray(node.storageLocations) ? node.storageLocations.length : 0;
}

export function getNodeConfigKeyCount(node: Record<string, unknown>): number {
  return isPlainObject(node.config) ? Object.keys(node.config).length : 0;
}

export function getRequestedNodeName(changes: Record<string, unknown>): string | undefined {
  return typeof changes.name === "string" && changes.name.trim().length > 0
    ? changes.name
    : undefined;
}

export function getRequestedColumnNames(changes: Record<string, unknown>): string[] {
  const metadata = isPlainObject(changes.metadata) ? changes.metadata : undefined;
  if (!metadata || !Array.isArray(metadata.columns)) {
    return [];
  }

  const names: string[] = [];
  for (const column of metadata.columns) {
    if (isPlainObject(column) && typeof column.name === "string" && column.name.trim().length > 0) {
      names.push(column.name);
    }
  }
  return names;
}

export function getRequestedConfig(changes: Record<string, unknown>): Record<string, unknown> | undefined {
  return isPlainObject(changes.config) ? changes.config : undefined;
}

export function getRequestedLocationFields(
  changes: Record<string, unknown>
): Record<string, unknown> {
  const requested: Record<string, unknown> = {};
  for (const key of ["database", "schema", "locationName"]) {
    if (Object.prototype.hasOwnProperty.call(changes, key)) {
      requested[key] = changes[key];
    }
  }
  return requested;
}

export function getNodeColumnNames(node: Record<string, unknown>): string[] {
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

export function getNodeDependencyNames(node: Record<string, unknown>): string[] {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!Array.isArray(metadata?.sourceMapping)) {
    return [];
  }

  return metadata.sourceMapping.flatMap((mapping) => {
    if (!isPlainObject(mapping) || !Array.isArray(mapping.dependencies)) {
      return [];
    }

    return mapping.dependencies.flatMap((dependency) => {
      if (!isPlainObject(dependency) || typeof dependency.nodeName !== "string") {
        return [];
      }
      return [dependency.nodeName];
    });
  });
}

export function normalizeColumnName(name: string): string {
  return name.trim().toUpperCase();
}

export function normalizeDataType(dt: string): string {
  return dt.trim().toUpperCase();
}
