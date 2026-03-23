import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { isPlainObject } from "../../utils.js";

export type NodeTypeCorpusSupportStatus = "supported" | "partial" | "parse_error";

export type NodeTypeCorpusOccurrence = {
  packageName: string;
  nodeTypeDirName: string;
  nodeTypeDirPath: string;
  definitionPath: string;
  createPath: string;
  runPath: string;
};

export type NodeTypeCorpusDefinitionSummary = {
  capitalized: string | null;
  short: string | null;
  plural: string | null;
  tagColor: string | null;
  deployStrategy: string | null;
  configGroupCount: number;
  configItemCount: number;
};

export type NodeTypeCorpusVariant = {
  variantKey: string;
  normalizedFamily: string;
  packageNames: string[];
  occurrenceCount: number;
  occurrences: NodeTypeCorpusOccurrence[];
  definitionHash: string;
  createHash: string;
  runHash: string;
  primitiveSignature: string[];
  controlSignature: string[];
  unsupportedPrimitives: string[];
  supportStatus: NodeTypeCorpusSupportStatus;
  definitionSummary: NodeTypeCorpusDefinitionSummary;
  outerDefinition: {
    fileVersion: unknown;
    id: string | null;
    isDisabled: boolean | null;
    name: string | null;
    type: string | null;
  };
  nodeMetadataSpec: string;
  nodeDefinition: Record<string, unknown> | null;
  parseError: string | null;
};

export type NodeTypeCorpusSnapshot = {
  generatedAt: string;
  sourceRoot: string;
  packageCount: number;
  definitionCount: number;
  uniqueVariantCount: number;
  uniqueNormalizedFamilyCount: number;
  supportedVariantCount: number;
  partialVariantCount: number;
  parseErrorVariantCount: number;
  variants: NodeTypeCorpusVariant[];
};

let cachedSnapshot: NodeTypeCorpusSnapshot | null = null;

function getSnapshotPath(): string {
  return fileURLToPath(
    new URL("../../generated/node-type-corpus.json", import.meta.url)
  );
}

export function loadNodeTypeCorpusSnapshot(): NodeTypeCorpusSnapshot {
  if (cachedSnapshot) {
    return cachedSnapshot;
  }

  const filePath = getSnapshotPath();
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(filePath, "utf8"));
  } catch {
    throw new Error(
      `Node type corpus snapshot not found or unreadable at ${filePath}. Run npm run import:node-type-corpus before building.`
    );
  }

  if (!isPlainObject(parsed) || !Array.isArray(parsed.variants)) {
    throw new Error(`Node type corpus snapshot is invalid at ${filePath}.`);
  }

  cachedSnapshot = parsed as NodeTypeCorpusSnapshot;
  return cachedSnapshot;
}
