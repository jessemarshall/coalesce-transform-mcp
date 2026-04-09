import {
  resolveRepoNodeType,
  type ParsedRepo,
  type RepoNodeTypeResolution,
} from "../repo/parser.js";
import { buildSetWorkspaceNodeTemplateFromDefinition } from "../templates/nodes.js";
import { isPlainObject } from "../../utils.js";
import {
  inferFamily,
  isAutoExecutableFamily,
  matchesNodeTypeIdentity,
  type PipelineNodeTypeFamily,
  type InternalPipelineNodeTypeCandidate,
} from "./node-type-selection.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function getString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function compareStrings(left: string, right: string): number {
  return left.localeCompare(right, undefined, {
    numeric: true,
    sensitivity: "case",
  });
}

function getDefinitionConfigItems(nodeDefinition: Record<string, unknown>): Record<string, unknown>[] {
  const groups = Array.isArray(nodeDefinition.config)
    ? nodeDefinition.config.filter(isPlainObject)
    : [];
  return groups.flatMap((group) =>
    Array.isArray(group.items) ? group.items.filter(isPlainObject) : []
  );
}

function analyzeDefinition(nodeDefinition: Record<string, unknown> | null): {
  semanticSignals: string[];
  missingDefaultFields: string[];
} {
  if (!nodeDefinition) {
    return {
      semanticSignals: [],
      missingDefaultFields: [],
    };
  }

  const semanticSignals = new Set<string>();
  const missingDefaultFields = new Set<string>();

  for (const item of getDefinitionConfigItems(nodeDefinition)) {
    const label =
      getString(item.attributeName) ??
      getString(item.displayName) ??
      getString(item.type) ??
      "unknown";
    const normalizedLabel = label.toLowerCase();
    const itemType = getString(item.type) ?? "";
    const hasDefault = Object.prototype.hasOwnProperty.call(item, "default");

    if (
      /(business.?key|surrogate|scd|effective|current.?flag|grain|hash|hub|satellite|link|fact|dimension|merge.?key)/u.test(
        normalizedLabel
      )
    ) {
      semanticSignals.add(label);
    }

    if (
      !hasDefault &&
      itemType !== "materializationSelector" &&
      itemType !== "multisourceToggle" &&
      itemType !== "overrideSQLToggle"
    ) {
      missingDefaultFields.add(label);
    }
  }

  return {
    semanticSignals: Array.from(semanticSignals).sort(compareStrings),
    missingDefaultFields: Array.from(missingDefaultFields).sort(compareStrings),
  };
}

// ---------------------------------------------------------------------------
// Repo resolution collection
// ---------------------------------------------------------------------------

export function collectRepoResolutions(parsedRepo: ParsedRepo): RepoNodeTypeResolution[] {
  const resolutions: RepoNodeTypeResolution[] = [];

  for (const [id, matches] of parsedRepo.nodeTypesByID.entries()) {
    if (matches.length !== 1) {
      continue;
    }
    resolutions.push(resolveRepoNodeType(parsedRepo, id));
  }

  for (const packageRecord of parsedRepo.packages) {
    const packageMatches = parsedRepo.packagesByAlias.get(packageRecord.alias) ?? [];
    if (packageMatches.length !== 1) {
      continue;
    }
    for (const definitionID of packageRecord.resolvedDefinitionIDs) {
      resolutions.push(
        resolveRepoNodeType(parsedRepo, `${packageRecord.alias}:::${definitionID}`)
      );
    }
  }

  return resolutions.sort((left, right) =>
    compareStrings(left.resolvedNodeType, right.resolvedNodeType)
  );
}

// ---------------------------------------------------------------------------
// Candidate builders
// ---------------------------------------------------------------------------

export function buildRepoCandidate(
  resolution: RepoNodeTypeResolution,
  workspaceNodeTypes: string[],
  workspaceNodeTypeCounts: Record<string, number>
): InternalPipelineNodeTypeCandidate {
  const generated = resolution.nodeTypeRecord.nodeDefinition
    ? buildSetWorkspaceNodeTemplateFromDefinition(
        resolution.nodeTypeRecord.nodeDefinition,
        { nodeType: resolution.resolvedNodeType }
      )
    : undefined;
  const displayName =
    getString(resolution.nodeTypeRecord.outerDefinition.name) ??
    generated?.definitionSummary.capitalized ??
    null;
  const shortName = generated?.definitionSummary.short ?? null;
  const family = inferFamily(
    [
      resolution.resolvedNodeType,
      resolution.nodeTypeRecord.dirName,
      displayName ?? "",
      shortName ?? "",
      generated?.definitionSummary.capitalized ?? "",
    ].filter((value) => value.length > 0)
  );
  const insights = analyzeDefinition(resolution.nodeTypeRecord.nodeDefinition);
  const observedInWorkspace = workspaceNodeTypes.some((nodeType) =>
    matchesNodeTypeIdentity(nodeType, resolution.resolvedNodeType)
  );
  const workspaceUsageCount = workspaceNodeTypes.reduce((sum, nodeType) => {
    if (!matchesNodeTypeIdentity(nodeType, resolution.resolvedNodeType)) {
      return sum;
    }
    return sum + (workspaceNodeTypeCounts[nodeType] ?? 0);
  }, 0);
  const autoExecutable =
    (isAutoExecutableFamily(family) ||
      (family === "unknown" &&
        insights.semanticSignals.length === 0 &&
        insights.missingDefaultFields.length === 0 &&
        !!generated)) &&
    !generated?.warnings.some((warning) => warning.includes("does not map cleanly"));

  const reasons: string[] = [];
  if (resolution.usageCount > 0) {
    reasons.push(`used ${resolution.usageCount} time(s) in committed nodes`);
  }
  if (observedInWorkspace) {
    reasons.push("already observed in current workspace nodes");
  }
  if (displayName) {
    reasons.push(`definition resolves to ${displayName}`);
  }

  return {
    nodeType: resolution.resolvedNodeType,
    displayName,
    shortName,
    family,
    usageCount: resolution.usageCount,
    workspaceUsageCount,
    observedInWorkspace,
    autoExecutable,
    semanticSignals: insights.semanticSignals,
    missingDefaultFields: insights.missingDefaultFields,
    templateWarnings: generated?.warnings ?? [],
    templateDefaults: generated
      ? {
          inferredTopLevelFields: generated.inferredTopLevelFields,
          inferredConfig: generated.inferredConfig,
        }
      : undefined,
    score: resolution.usageCount * 20 + workspaceUsageCount * 10 + (generated ? 5 : 0),
    reasons,
    source: "repo",
    resolutionKind: resolution.resolutionKind,
    ...(resolution.resolutionKind === "package"
      ? { packageAlias: resolution.packageAlias }
      : {}),
  };
}

export function buildWorkspaceCandidate(
  nodeType: string,
  workspaceNodeTypeCounts: Record<string, number>
): InternalPipelineNodeTypeCandidate {
  const family = inferFamily([nodeType]);
  return {
    nodeType,
    displayName: nodeType,
    shortName: null,
    family,
    usageCount: 0,
    workspaceUsageCount: workspaceNodeTypeCounts[nodeType] ?? 0,
    observedInWorkspace: true,
    autoExecutable: isAutoExecutableFamily(family),
    semanticSignals: [],
    missingDefaultFields: [],
    templateWarnings: [],
    score: (workspaceNodeTypeCounts[nodeType] ?? 0) * 10,
    reasons: ["observed in current workspace nodes"],
    source: "workspace",
  };
}
