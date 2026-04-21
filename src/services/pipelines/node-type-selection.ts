import {
  parseRepo,
  type ParsedRepo,
  type RepoNodeTypeResolution,
} from "../repo/parser.js";
import { resolveOptionalRepoPathInput } from "../repo/path.js";
import { scoreCandidate, challengeCandidate, rankCandidates } from "./node-type-scoring.js";
import { collectRepoResolutions, buildRepoCandidate, buildWorkspaceCandidate } from "./node-type-candidates.js";

// ---------------------------------------------------------------------------
// Exported types
// ---------------------------------------------------------------------------

export type PipelineTemplateDefaults = {
  inferredTopLevelFields: Record<string, unknown>;
  inferredConfig: Record<string, unknown>;
};

export const PIPELINE_NODE_TYPE_FAMILIES = [
  "stage",
  "persistent-stage",
  "view",
  "work",
  "dimension",
  "fact",
  "hub",
  "satellite",
  "link",
  "unknown",
] as const;

export type PipelineNodeTypeFamily = (typeof PIPELINE_NODE_TYPE_FAMILIES)[number];

export type InternalPipelineNodeTypeCandidate = {
  nodeType: string;
  displayName: string | null;
  shortName: string | null;
  family: PipelineNodeTypeFamily;
  usageCount: number;
  workspaceUsageCount: number;
  observedInWorkspace: boolean;
  autoExecutable: boolean;
  semanticSignals: string[];
  missingDefaultFields: string[];
  templateWarnings: string[];
  templateDefaults?: PipelineTemplateDefaults;
  score: number;
  reasons: string[];
  source: "repo" | "workspace";
  resolutionKind?: "direct" | "package";
  packageAlias?: string;
};

export type PipelineNodeTypeSelectionCandidate = {
  nodeType: string;
  displayName: string | null;
  shortName: string | null;
  family: PipelineNodeTypeFamily;
  usageCount: number;
  workspaceUsageCount: number;
  observedInWorkspace: boolean;
  autoExecutable: boolean;
  score: number;
  reasons: string[];
};

export type PipelineNodeTypeSelection = {
  strategy: "explicit" | "repo-ranked" | "workspace-ranked" | "fallback";
  selectedNodeType: string | null;
  selectedDisplayName: string | null;
  selectedShortName: string | null;
  selectedFamily: PipelineNodeTypeFamily | null;
  confidence: "high" | "medium" | "low";
  autoExecutable: boolean;
  supportedNodeTypes: string[];
  repoPath: string | null;
  resolvedRepoPath: string | null;
  repoWarnings: string[];
  workspaceObservedNodeTypes: string[];
  consideredNodeTypes: PipelineNodeTypeSelectionCandidate[];
};

export type PipelineNodeTypeSelectionResult = {
  selectedCandidate: {
    nodeType: string;
    displayName: string | null;
    shortName: string | null;
    family: PipelineNodeTypeFamily;
    autoExecutable: boolean;
    semanticSignals: string[];
    missingDefaultFields: string[];
    templateWarnings: string[];
    templateDefaults?: PipelineTemplateDefaults;
  } | null;
  selection: PipelineNodeTypeSelection;
  warnings: string[];
};

export type PipelineNodeTypeSelectionContext = {
  explicitNodeType?: string;
  goal?: string;
  targetName?: string;
  sql?: string;
  sourceCount: number;
  workspaceNodeTypes?: string[];
  workspaceNodeTypeCounts?: Record<string, number>;
  repoPath?: string;
  /** Structural hint: does the SQL/transform contain JOINs? */
  hasJoin?: boolean;
  /** Structural hint: does the SQL/transform contain GROUP BY? */
  hasGroupBy?: boolean;
  /** Structural hint: are business keys explicitly defined? */
  hasBusinessKeys?: boolean;
};

// ---------------------------------------------------------------------------
// Shared helpers (used by scoring / candidates / external consumers)
// ---------------------------------------------------------------------------

function nodeTypeID(nodeType: string): string {
  const delimiterIndex = nodeType.indexOf(":::");
  return delimiterIndex === -1 ? nodeType : nodeType.slice(delimiterIndex + 3);
}

/** Node type IDs excluded from selection — these are not valid pipeline transform types. */
const EXCLUDED_NODE_TYPE_IDS = new Set(["SQL", "Source"]);

function isExcludedNodeTypeID(nodeType: string): boolean {
  return EXCLUDED_NODE_TYPE_IDS.has(nodeTypeID(nodeType));
}

function isExcludedByInputMode(resolution: RepoNodeTypeResolution): boolean {
  if (resolution.nodeTypeRecord.outerDefinition.inputMode === "sql") {
    return true;
  }
  const nodeDefinition = resolution.nodeTypeRecord.nodeDefinition;
  if (nodeDefinition && typeof nodeDefinition.inputMode === "string") {
    return nodeDefinition.inputMode === "sql";
  }
  return false;
}

function isDisabledNodeType(resolution: RepoNodeTypeResolution): boolean {
  return resolution.nodeTypeRecord.outerDefinition.isDisabled === true;
}

export function matchesNodeTypeIdentity(left: string, right: string): boolean {
  return left === right || nodeTypeID(left) === nodeTypeID(right);
}

export function isAutoExecutableFamily(family: PipelineNodeTypeFamily): boolean {
  return (
    family === "stage" ||
    family === "persistent-stage" ||
    family === "view" ||
    family === "work"
  );
}

export function inferFamily(signals: string[]): PipelineNodeTypeFamily {
  const combined = signals
    .filter((value) => value.trim().length > 0)
    .join(" ")
    .toLowerCase();

  if (/(^|[\s_-])persistent\s*stage([\s_-]|$)|persistentstage/u.test(combined)) {
    return "persistent-stage";
  }
  if (/(^|[\s_-])stage([\s_-]|$)|\bstg\b/u.test(combined)) {
    return "stage";
  }
  if (/(^|[\s_-])view([\s_-]|$)|\bvw\b/u.test(combined)) {
    return "view";
  }
  if (/(^|[\s_-])work([\s_-]|$)|\bwrk\b|\bcwrk\b/u.test(combined)) {
    return "work";
  }
  if (/(^|[\s_-])dimension([\s_-]|$)|\bdim\b/u.test(combined)) {
    return "dimension";
  }
  if (/(^|[\s_-])fact([\s_-]|$)|\bfct\b/u.test(combined)) {
    return "fact";
  }
  if (/(^|[\s_-])hub([\s_-]|$)/u.test(combined)) {
    return "hub";
  }
  if (/(^|[\s_-])satellite([\s_-]|$)|(^|[\s_-])sat([\s_-]|$)/u.test(combined)) {
    return "satellite";
  }
  if (/(^|[\s_-])link([\s_-]|$)/u.test(combined)) {
    return "link";
  }

  return "unknown";
}

// ---------------------------------------------------------------------------
// Main selection orchestrator
// ---------------------------------------------------------------------------

export function selectPipelineNodeType(
  context: PipelineNodeTypeSelectionContext
): PipelineNodeTypeSelectionResult {
  const warnings: string[] = [];
  const workspaceNodeTypes = context.workspaceNodeTypes ?? [];
  const workspaceNodeTypeCounts = context.workspaceNodeTypeCounts ?? {};
  const repoPath = resolveOptionalRepoPathInput(context.repoPath);

  let parsedRepo: ParsedRepo | undefined;
  if (repoPath) {
    try {
      parsedRepo = parseRepo(repoPath);
    } catch (error) {
      warnings.push(
        error instanceof Error
          ? error.message
          : `Repo-backed planning could not parse ${repoPath}.`
      );
    }
  }

  const candidates: InternalPipelineNodeTypeCandidate[] = [];
  const seen = new Set<string>();
  const excludedByInputMode = new Set<string>();

  if (parsedRepo) {
    for (const resolution of collectRepoResolutions(parsedRepo)) {
      if (isExcludedNodeTypeID(resolution.resolvedNodeType) || isExcludedByInputMode(resolution) || isDisabledNodeType(resolution)) {
        excludedByInputMode.add(resolution.resolvedNodeType);
        seen.add(resolution.resolvedNodeType);
        continue;
      }
      const candidate = buildRepoCandidate(
        resolution,
        workspaceNodeTypes,
        workspaceNodeTypeCounts
      );
      if (seen.has(candidate.nodeType)) {
        continue;
      }
      seen.add(candidate.nodeType);
      candidates.push(scoreCandidate(candidate, context));
    }
  }

  for (const nodeType of workspaceNodeTypes) {
    if (
      seen.has(nodeType) ||
      isExcludedNodeTypeID(nodeType) ||
      Array.from(excludedByInputMode).some((excluded) => matchesNodeTypeIdentity(excluded, nodeType))
    ) {
      continue;
    }
    seen.add(nodeType);
    candidates.push(scoreCandidate(buildWorkspaceCandidate(nodeType, workspaceNodeTypeCounts), context));
  }

  // === DELIBERATIVE SELECTION: Match → Rank → Challenge → Repeat (twice) ===
  const contextText = [context.goal, context.targetName].filter(Boolean).join(" ");

  let sorted = rankCandidates(candidates);
  const challengeLog: string[] = [];

  for (let round = 1; round <= 2; round++) {
    const top = sorted[0] ?? null;
    if (!top) break;

    const challenges = challengeCandidate(top, contextText);
    if (challenges.length > 0) {
      challengeLog.push(`Round ${round}: challenged "${top.nodeType}" (${top.displayName ?? top.family}) — ${challenges.join("; ")}`);
      top.score = -Infinity;
      top.reasons.push(...challenges.map((c) => `CHALLENGED: ${c}`));
      sorted = rankCandidates(sorted);
    } else {
      challengeLog.push(`Round ${round}: "${top.nodeType}" (${top.displayName ?? top.family}) passed challenge`);
      break;
    }
  }

  let selected: InternalPipelineNodeTypeCandidate | null = sorted[0] ?? null;
  if (selected && selected.score === -Infinity) {
    selected = sorted.find((c) => c.score > -Infinity) ?? null;
  }

  let strategy: PipelineNodeTypeSelection["strategy"] = parsedRepo
    ? "repo-ranked"
    : workspaceNodeTypes.length > 0
      ? "workspace-ranked"
      : "fallback";

  const isExcludedExplicit = context.explicitNodeType
    ? isExcludedNodeTypeID(context.explicitNodeType) ||
      excludedByInputMode.has(context.explicitNodeType) ||
      Array.from(excludedByInputMode).some((excluded) =>
        matchesNodeTypeIdentity(excluded, context.explicitNodeType!)
      )
    : false;

  if (context.explicitNodeType) {
    if (isExcludedExplicit) {
      warnings.push(
        `targetNodeType "${context.explicitNodeType}" is excluded because it relies on raw SQL override, which is disallowed in this project. Use a declarative node type (Stage, View, Dimension, Fact, etc.) instead.`
      );
    } else {
      const explicitMatch =
        sorted.find((candidate) => candidate.nodeType === context.explicitNodeType) ??
        sorted.find((candidate) =>
          matchesNodeTypeIdentity(candidate.nodeType, context.explicitNodeType!)
        ) ??
        null;
      if (explicitMatch) {
        selected = explicitMatch;
        strategy = "explicit";
      } else {
        warnings.push(
          `targetNodeType ${context.explicitNodeType} could not be matched to repo-backed or observed workspace node types.`
        );
      }
    }
  }

  if (!selected && context.explicitNodeType && !isExcludedExplicit) {
    const explicitFamily = inferFamily([context.explicitNodeType]);
    selected = {
      ...buildWorkspaceCandidate(context.explicitNodeType, workspaceNodeTypeCounts),
      family: explicitFamily,
      observedInWorkspace: false,
      reasons: ["provided as an explicit targetNodeType override"],
      score: 500,
    };
  }

  if (challengeLog.length > 0) {
    warnings.push(...challengeLog);
  }

  const nextBest = sorted.find((candidate) => candidate.nodeType !== selected?.nodeType && candidate.score > -Infinity) ?? null;
  const gap = selected ? selected.score - (nextBest?.score ?? 0) : 0;
  const confidence: PipelineNodeTypeSelection["confidence"] =
    selected && selected.autoExecutable && gap >= 40
      ? "high"
      : selected && gap >= 15
        ? "medium"
        : "low";

  const selection: PipelineNodeTypeSelection = {
    strategy,
    selectedNodeType: selected?.nodeType ?? null,
    selectedDisplayName: selected?.displayName ?? null,
    selectedShortName: selected?.shortName ?? null,
    selectedFamily: selected?.family ?? null,
    confidence,
    autoExecutable: selected?.autoExecutable ?? false,
    supportedNodeTypes: sorted
      .filter((candidate) => candidate.autoExecutable)
      .map((candidate) => candidate.nodeType)
      .slice(0, 10),
    repoPath: repoPath ?? null,
    resolvedRepoPath: parsedRepo?.summary.resolvedRepoPath ?? null,
    repoWarnings: parsedRepo?.summary.warnings ?? [],
    workspaceObservedNodeTypes: workspaceNodeTypes,
    consideredNodeTypes: sorted.slice(0, 10).map((candidate) => ({
      nodeType: candidate.nodeType,
      displayName: candidate.displayName,
      shortName: candidate.shortName,
      family: candidate.family,
      usageCount: candidate.usageCount,
      workspaceUsageCount: candidate.workspaceUsageCount,
      observedInWorkspace: candidate.observedInWorkspace,
      autoExecutable: candidate.autoExecutable,
      score: candidate.score,
      reasons: candidate.reasons,
    })),
  };

  return {
    selectedCandidate: selected
      ? {
          nodeType: selected.nodeType,
          displayName: selected.displayName,
          shortName: selected.shortName,
          family: selected.family,
          autoExecutable: selected.autoExecutable,
          semanticSignals: selected.semanticSignals,
          missingDefaultFields: selected.missingDefaultFields,
          templateWarnings: selected.templateWarnings,
          templateDefaults: selected.templateDefaults,
        }
      : null,
    selection,
    warnings,
  };
}
