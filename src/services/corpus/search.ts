import type {
  NodeTypeCorpusSnapshot,
  NodeTypeCorpusVariant,
  NodeTypeCorpusSupportStatus,
} from "./loader.js";
import { filterSqlOverrideControls } from "../policies/sql-override.js";

export function summarizeNodeTypeCorpus(snapshot: NodeTypeCorpusSnapshot) {
  const familyCounts = new Map<string, number>();
  for (const variant of snapshot.variants) {
    familyCounts.set(
      variant.normalizedFamily,
      (familyCounts.get(variant.normalizedFamily) ?? 0) + 1
    );
  }

  const topFamilies = Array.from(familyCounts.entries())
    .sort((left, right) =>
      right[1] === left[1]
        ? left[0].localeCompare(right[0])
        : right[1] - left[1]
    )
    .slice(0, 20)
    .map(([normalizedFamily, variantCount]) => ({
      normalizedFamily,
      variantCount,
    }));

  return {
    generatedAt: snapshot.generatedAt,
    sourceRoot: snapshot.sourceRoot,
    packageCount: snapshot.packageCount,
    definitionCount: snapshot.definitionCount,
    uniqueVariantCount: snapshot.uniqueVariantCount,
    uniqueNormalizedFamilyCount: snapshot.uniqueNormalizedFamilyCount,
    supportedVariantCount: snapshot.supportedVariantCount,
    partialVariantCount: snapshot.partialVariantCount,
    parseErrorVariantCount: snapshot.parseErrorVariantCount,
    topFamilies,
  };
}

export function buildVariantSummary(variant: NodeTypeCorpusVariant) {
  return {
    variantKey: variant.variantKey,
    normalizedFamily: variant.normalizedFamily,
    packageNames: variant.packageNames,
    occurrenceCount: variant.occurrenceCount,
    supportStatus: variant.supportStatus,
    unsupportedPrimitives: filterSqlOverrideControls(variant.unsupportedPrimitives),
    primitiveSignature: filterSqlOverrideControls(variant.primitiveSignature),
    controlSignature: filterSqlOverrideControls(variant.controlSignature),
    definitionSummary: variant.definitionSummary,
  };
}

export function searchNodeTypeCorpusVariants(
  snapshot: NodeTypeCorpusSnapshot,
  params: {
    normalizedFamily?: string;
    packageName?: string;
    primitive?: string;
    supportStatus?: NodeTypeCorpusSupportStatus;
    limit?: number;
  }
) {
  const limit = Math.max(1, Math.min(params.limit ?? 25, 200));
  const normalizedFamilyFilter = params.normalizedFamily?.trim().toLowerCase();
  const packageNameFilter = params.packageName?.trim().toLowerCase();
  const primitiveFilter = params.primitive?.trim().toLowerCase();

  const matches = snapshot.variants.filter((variant) => {
    const familyMatches =
      !normalizedFamilyFilter ||
      variant.normalizedFamily.toLowerCase() === normalizedFamilyFilter;
    const packageMatches =
      !packageNameFilter ||
      variant.packageNames.some(
        (packageName) => packageName.toLowerCase() === packageNameFilter
      );
    const primitiveMatches =
      !primitiveFilter ||
      variant.primitiveSignature.some(
        (primitive) => primitive.toLowerCase() === primitiveFilter
      );
    const supportMatches =
      !params.supportStatus || variant.supportStatus === params.supportStatus;

    return familyMatches && packageMatches && primitiveMatches && supportMatches;
  });

  return {
    matchedCount: matches.length,
    returnedCount: Math.min(matches.length, limit),
    matches: matches.slice(0, limit).map(buildVariantSummary),
  };
}

export function getNodeTypeCorpusVariant(
  snapshot: NodeTypeCorpusSnapshot,
  variantKey: string
): NodeTypeCorpusVariant {
  const variant = snapshot.variants.find((entry) => entry.variantKey === variantKey);
  if (!variant) {
    throw new Error(`No node type corpus variant found for variantKey ${variantKey}.`);
  }
  return variant;
}
