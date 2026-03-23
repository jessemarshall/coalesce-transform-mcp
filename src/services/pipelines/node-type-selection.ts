import {
  parseRepo,
  resolveRepoNodeType,
  type ParsedRepo,
  type RepoNodeTypeResolution,
} from "../repo/parser.js";
import { resolveOptionalRepoPathInput } from "../repo/path.js";
import { buildSetWorkspaceNodeTemplateFromDefinition } from "../templates/nodes.js";
import { isPlainObject } from "../../utils.js";
import { NODE_TYPE_INTENT, hasAntiSignal, detectSpecializedPatternPenalty, detectSpecializedPatternMatch } from "./node-type-intent.js";

export type PipelineTemplateDefaults = {
  inferredTopLevelFields: Record<string, unknown>;
  inferredConfig: Record<string, unknown>;
};

export type PipelineNodeTypeFamily =
  | "stage"
  | "persistent-stage"
  | "view"
  | "work"
  | "dimension"
  | "fact"
  | "hub"
  | "satellite"
  | "link"
  | "unknown";

type InternalPipelineNodeTypeCandidate = {
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

type PipelineNodeTypeSelectionContext = {
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

function getString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function compareStrings(left: string, right: string): number {
  return left.localeCompare(right, undefined, {
    numeric: true,
    sensitivity: "case",
  });
}

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
  // inputMode can be in outerDefinition (top-level) or nodeDefinition (nodeMetadataSpec)
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

function matchesNodeTypeIdentity(left: string, right: string): boolean {
  return left === right || nodeTypeID(left) === nodeTypeID(right);
}

function collectRepoResolutions(parsedRepo: ParsedRepo): RepoNodeTypeResolution[] {
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

function isAutoExecutableFamily(family: PipelineNodeTypeFamily): boolean {
  return (
    family === "stage" ||
    family === "persistent-stage" ||
    family === "view" ||
    family === "work"
  );
}

/**
 * Node type category — the real decision.
 *
 * Stage and Work are interchangeable for all general-purpose patterns.
 * The only meaningful distinction is between categories, not between
 * stage and work within general-purpose.
 */
type NodeTypeCategory =
  | "general-purpose"   // stage / work (interchangeable)
  | "view"              // no materialization
  | "persistent"        // CDC, change tracking
  | "dimensional"       // dimension, fact — requires explicit intent
  | "data-vault";       // hub, satellite, link — requires explicit intent

const CATEGORY_FAMILIES: Record<NodeTypeCategory, PipelineNodeTypeFamily[]> = {
  "general-purpose": ["stage", "work"],
  "view": ["view"],
  "persistent": ["persistent-stage"],
  "dimensional": ["dimension", "fact"],
  "data-vault": ["hub", "satellite", "link"],
};

/**
 * Determine the desired category from context.
 *
 * Decision tree:
 * 1. Explicit dimensional/data-vault/CDC/view intent → that category
 * 2. Strong signal from name (dim_, fct_, hub_, etc.) → that category
 * 3. Otherwise → general-purpose (stage/work, pick by workspace usage)
 *
 * Stage vs Work within general-purpose: prefer whichever the workspace
 * already uses more, or default to stage.
 */
function buildUseCaseContext(context: PipelineNodeTypeSelectionContext): {
  desiredFamilies: PipelineNodeTypeFamily[];
  category: NodeTypeCategory;
  dimensionalModeling: boolean;
  multiSource: boolean;
} {
  const freeText = [context.goal, context.targetName].filter(Boolean).join(" ").toLowerCase();
  const multiSource = context.sourceCount > 1;

  // Dimensional modeling requires explicit intent — not just GROUP BY
  const dimensionalModeling =
    /\bdimension(al)?\s+model/u.test(freeText) ||
    /\bstar\s+schema\b/u.test(freeText) ||
    /\bsnowflake\s+schema\b/u.test(freeText);

  // Data Vault requires explicit intent
  const dataVaultIntent = /\bdata\s*vault\b/u.test(freeText);

  // CDC / persistent stage requires explicit intent
  const persistentIntent =
    /\bpersistent\s*stage\b/u.test(freeText) ||
    /\bcdc\b/u.test(freeText) ||
    /\bchange\s*track/u.test(freeText);

  // View requires explicit intent
  const viewIntent =
    /\bview\b/u.test(freeText) ||
    /\bno\s+materialization/u.test(freeText) ||
    /\bvirtual\s+table/u.test(freeText);

  // Check name-based strong signals ONLY for specialized categories
  // (not for stage/work — those are general-purpose regardless of name)
  const targetName = (context.targetName ?? "").toLowerCase();
  const combinedText = `${targetName} ${freeText}`;

  const specializedSignalChecks: { category: NodeTypeCategory; families: PipelineNodeTypeFamily[] }[] = [
    { category: "persistent", families: ["persistent-stage"] },
    { category: "dimensional", families: ["dimension", "fact"] },
    { category: "data-vault", families: ["hub", "satellite", "link"] },
    { category: "view", families: ["view"] },
  ];

  // 1. Explicit intent from goal text
  if (dataVaultIntent) {
    return { desiredFamilies: ["hub", "satellite", "link"], category: "data-vault", dimensionalModeling, multiSource };
  }
  if (dimensionalModeling) {
    return { desiredFamilies: ["dimension", "fact"], category: "dimensional", dimensionalModeling, multiSource };
  }
  if (persistentIntent) {
    return { desiredFamilies: ["persistent-stage", "stage"], category: "persistent", dimensionalModeling, multiSource };
  }
  if (viewIntent && !context.hasJoin && !context.hasGroupBy) {
    return { desiredFamilies: ["view", "stage"], category: "view", dimensionalModeling, multiSource };
  }

  // 2. Strong signal from name (only for specialized families)
  for (const { category, families } of specializedSignalChecks) {
    for (const family of families) {
      const intent = NODE_TYPE_INTENT[family];
      if (intent.strongSignals.test(combinedText)) {
        return { desiredFamilies: families, category, dimensionalModeling, multiSource };
      }
    }
  }

  // 3. General-purpose — stage and work are interchangeable.
  // Tiebreaker priority:
  //   a) Workspace pattern — which does the workspace already use more?
  //   b) Default to stage (base node type package always has Stage)
  const counts = context.workspaceNodeTypeCounts ?? {};
  const stageUsage = Object.entries(counts)
    .filter(([nodeType]) => inferFamily([nodeType]) === "stage")
    .reduce((sum, [, count]) => sum + count, 0);
  const workUsage = Object.entries(counts)
    .filter(([nodeType]) => inferFamily([nodeType]) === "work")
    .reduce((sum, [, count]) => sum + count, 0);

  const desiredFamilies: PipelineNodeTypeFamily[] =
    workUsage > stageUsage
      ? ["work", "stage", "view"]
      : ["stage", "work", "view"];

  return { desiredFamilies, category: "general-purpose", dimensionalModeling, multiSource };
}

function familyScore(
  candidate: InternalPipelineNodeTypeCandidate,
  useCase: { desiredFamilies: PipelineNodeTypeFamily[]; category: NodeTypeCategory }
): { score: number; reasons: string[] } {
  const { desiredFamilies, category } = useCase;

  if (desiredFamilies.length === 0) {
    return { score: 0, reasons: [] };
  }

  // For general-purpose category, stage and work get the same top score.
  // They're interchangeable — the tiebreaker is workspace usage.
  // Candidates observed in the workspace get a bonus to prefer established patterns.
  if (category === "general-purpose") {
    if (candidate.family === "stage" || candidate.family === "work") {
      const workspaceBonus = candidate.observedInWorkspace ? 20 : 0;
      const reasons = [`general-purpose ${candidate.family} node — fits standard transforms`];
      if (workspaceBonus > 0) {
        reasons.push("preferred — already used in this workspace");
      }
      return {
        score: 120 + workspaceBonus,
        reasons,
      };
    }
    if (candidate.family === "view") {
      return {
        score: 60,
        reasons: ["view is acceptable for general-purpose transforms"],
      };
    }
    return { score: 0, reasons: [] };
  }

  // For specialized categories, rank by position in desired families list
  if (desiredFamilies.includes(candidate.family)) {
    const position = desiredFamilies.indexOf(candidate.family);
    const score = position === 0 ? 120 : 60;
    return {
      score,
      reasons: [`matches the ${category} category (${candidate.family})`],
    };
  }

  // General-purpose families are always a fallback for specialized categories
  if (candidate.family === "stage" || candidate.family === "work") {
    return {
      score: 25,
      reasons: [`general-purpose fallback for ${category} category`],
    };
  }

  return { score: 0, reasons: [] };
}

function scoreCandidate(
  candidate: InternalPipelineNodeTypeCandidate,
  context: PipelineNodeTypeSelectionContext
): InternalPipelineNodeTypeCandidate {
  const reasons = [...candidate.reasons];
  let score = candidate.score;
  const useCase = buildUseCaseContext(context);

  if (context.explicitNodeType) {
    if (candidate.nodeType === context.explicitNodeType) {
      score += 1000;
      reasons.push("matches the explicit targetNodeType override");
    } else if (matchesNodeTypeIdentity(candidate.nodeType, context.explicitNodeType)) {
      score += 900;
      reasons.push("matches the explicit targetNodeType ID");
    } else {
      score -= 200;
    }
  }

  const familyMatch = familyScore(candidate, useCase);
  score += familyMatch.score;
  reasons.push(...familyMatch.reasons);

  if (useCase.multiSource && isAutoExecutableFamily(candidate.family)) {
    score += 15;
    reasons.push("fits a multisource projection workflow");
  }
  if (useCase.dimensionalModeling && (candidate.family === "dimension" || candidate.family === "fact")) {
    score += 40;
    reasons.push(`${candidate.family} is designed for dimensional modeling with business keys`);
  }
  if (!useCase.multiSource && (candidate.family === "stage" || candidate.family === "work")) {
    score += 10;
    reasons.push("general-purpose node for single-source transforms");
  }

  // Anti-signal penalty: if this family's anti-signals match the context, penalize it.
  // Prevents dimension/fact from being chosen for generic transforms.
  const contextText = [context.goal, context.targetName].filter(Boolean).join(" ");
  if (contextText.length > 0 && hasAntiSignal(candidate.family, contextText)) {
    score -= 30;
    reasons.push(`context suggests this is not a ${candidate.family} use case`);
  }

  // Semantic config penalty: types that require business keys, SCD, etc.
  // get penalized when there's no dimensional modeling intent
  const intent = NODE_TYPE_INTENT[candidate.family];
  if (intent.requiresSemanticConfig && !useCase.dimensionalModeling) {
    score -= 15;
    reasons.push(`${candidate.family} requires semantic config (business keys, SCD) — no dimensional modeling intent detected`);
  }

  // Specialized materialization patterns: Dynamic Tables, Incremental Loads, etc.
  // Decision is binary from node-type-intent.ts:
  //   - If context explicitly requests the pattern (contextRequired matches) → keep it, add bonus
  //   - If context doesn't request it → mark as not applicable (score = -Infinity)
  // This is the same logic as validateNodeTypeChoice() at creation time.
  const candidateSignals = [candidate.nodeType, candidate.displayName ?? "", candidate.shortName ?? ""].join(" ");
  const specializedResult = detectSpecializedPatternPenalty(candidateSignals, contextText);
  if (specializedResult) {
    // Context doesn't match — this specialized type is not appropriate
    score = -Infinity;
    reasons.push(`not applicable: ${specializedResult.reason}`);
  } else {
    // Check if context positively matches a specialized pattern (context requested it)
    const positiveMatch = detectSpecializedPatternMatch(candidateSignals, contextText);
    if (positiveMatch) {
      score += 50;
      reasons.push(`context explicitly requests ${positiveMatch} pattern`);
    }
  }

  // Hard exclusion: data-vault package types are NEVER selected unless
  // the context explicitly requests data vault. These types serve a fundamentally
  // different modeling paradigm and should not appear in standard pipelines.
  if (
    !useCase.dimensionalModeling &&
    candidate.family !== "hub" && candidate.family !== "satellite" && candidate.family !== "link" &&
    (
      (candidate.packageAlias && /data.vault/iu.test(candidate.packageAlias)) ||
      /data.vault/iu.test(candidate.nodeType)
    )
  ) {
    const hasDataVaultIntent = /\bdata\s*vault\b/iu.test(contextText);
    if (!hasDataVaultIntent) {
      score = -Infinity;
      reasons.push(`data vault package type excluded — no data vault intent in context`);
      return { ...candidate, score, reasons: Array.from(new Set(reasons)) };
    }
  }

  // Non-base type exclusion for general-purpose selections.
  // Priority: workspace pattern > base node type package > 4 defaults.
  // Types NOT from the base package are EXCLUDED unless observed in workspace.
  if (useCase.category === "general-purpose" && candidate.source === "repo") {
    const isBasePackage = candidate.packageAlias && /base.node.type/iu.test(candidate.packageAlias);
    if (isBasePackage) {
      score += 15;
      reasons.push("from base node type package — preferred default");
    } else if (!candidate.observedInWorkspace) {
      // Non-base, non-workspace types are excluded for general-purpose transforms.
      // Only types from the base node type package or already in use in the workspace
      // are eligible for standard staging/transform/join operations.
      score = -Infinity;
      const source = candidate.packageAlias
        ? `non-base package "${candidate.packageAlias}"`
        : `custom repo type "${candidate.nodeType}"`;
      reasons.push(`excluded: ${source} — not observed in workspace. Use base node types or workspace-established types.`);
      return { ...candidate, score, reasons: Array.from(new Set(reasons)) };
    }
  }

  // Penalize "Copy of" types — these are user-cloned definitions.
  // The original base type should be preferred unless explicitly requested.
  if (
    candidate.displayName &&
    /\bcopy\s+of\b/iu.test(candidate.displayName) &&
    !context.explicitNodeType
  ) {
    score -= 30;
    reasons.push(`"${candidate.displayName}" is a cloned type — prefer the original`);
  }

  // Unknown family types should never beat known general-purpose types.
  // If inferFamily couldn't classify it, it's likely a custom/specialized type
  // that shouldn't be auto-selected for standard transforms.
  if (candidate.family === "unknown" && !context.explicitNodeType) {
    score -= 50;
    reasons.push("unknown node type family — cannot verify suitability for this use case");
  }

  if (candidate.autoExecutable) {
    score += 25;
    reasons.push("supports template-based automatic creation");
  } else {
    score -= 25;
    reasons.push("likely needs extra semantic configuration before automatic creation");
  }

  if (candidate.missingDefaultFields.length > 0) {
    score -= candidate.missingDefaultFields.length * 8;
    reasons.push(
      `has config fields without defaults: ${candidate.missingDefaultFields.join(", ")}`
    );
  }

  if (candidate.semanticSignals.length > 0) {
    score -= candidate.semanticSignals.length * 6;
    reasons.push(
      `exposes semantic config signals: ${candidate.semanticSignals.join(", ")}`
    );
  }

  if (candidate.templateWarnings.length > 0) {
    score -= Math.min(candidate.templateWarnings.length, 3) * 3;
  }

  return {
    ...candidate,
    score,
    reasons: Array.from(new Set(reasons)),
  };
}

function buildRepoCandidate(
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
    reasons.push(`used ${resolution.usageCount} time(s) in committed nodes/`);
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

function buildWorkspaceCandidate(
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

/**
 * Sort candidates by score descending (highest first).
 */
function rankCandidates(
  candidates: InternalPipelineNodeTypeCandidate[]
): InternalPipelineNodeTypeCandidate[] {
  return [...candidates].sort((a, b) => b.score - a.score);
}

/**
 * Challenge the top-ranked candidate against the intent corpus.
 * Returns an array of challenge reasons. Empty = candidate passed.
 *
 * Checks:
 * 1. Anti-signals from the intent doc match the context
 * 2. Specialized pattern not requested by context
 * 3. Requires semantic config but no dimensional modeling intent
 * 4. Another family's strong signal matches context but this candidate doesn't belong to it
 * 5. doNotUseWhen entries match the context
 */
function challengeCandidate(
  candidate: InternalPipelineNodeTypeCandidate,
  context: PipelineNodeTypeSelectionContext,
  contextText: string
): string[] {
  const challenges: string[] = [];
  const intent = NODE_TYPE_INTENT[candidate.family];
  const contextLower = contextText.toLowerCase();

  // 1. Anti-signals: the intent doc says this family should NOT be used for this context
  if (intent.antiSignals !== null && intent.antiSignals.test(contextLower)) {
    challenges.push(
      `${candidate.family} anti-signal matched — intent says not for this context`
    );
  }

  // 2. Specialized pattern penalty: candidate is a specialized type but context doesn't request it
  const candidateSignals = [
    candidate.nodeType,
    candidate.displayName ?? "",
    candidate.shortName ?? "",
  ].join(" ");
  const specializedPenalty = detectSpecializedPatternPenalty(candidateSignals, contextText);
  if (specializedPenalty) {
    challenges.push(specializedPenalty.reason);
  }

  // 3. Requires semantic config (business keys, SCD) but context has no dimensional modeling intent
  if (intent.requiresSemanticConfig) {
    const hasDimensionalIntent =
      /\bdimension(al)?\s+model/u.test(contextLower) ||
      /\bstar\s+schema\b/u.test(contextLower) ||
      /\bsnowflake\s+schema\b/u.test(contextLower) ||
      /\bdata\s*vault\b/u.test(contextLower);
    // Also check if context has a strong signal for THIS family (e.g. name starts with dim_)
    const hasOwnStrongSignal = intent.strongSignals.test(contextLower);
    if (!hasDimensionalIntent && !hasOwnStrongSignal) {
      challenges.push(
        `${candidate.family} requires semantic config (business keys, SCD) but no dimensional modeling intent detected`
      );
    }
  }

  // 4. Another family has a strong signal match but this candidate is a different CATEGORY
  // Stage and work are interchangeable — don't challenge one for the other.
  const generalPurposeFamilies = new Set<PipelineNodeTypeFamily>(["stage", "work"]);
  const signalCheckOrder: PipelineNodeTypeFamily[] = [
    "persistent-stage", "dimension", "fact", "hub", "satellite", "link",
    "view", "work", "stage",
  ];
  for (const family of signalCheckOrder) {
    if (family === candidate.family) continue;
    // Skip stage↔work challenges — they're the same category
    if (generalPurposeFamilies.has(family) && generalPurposeFamilies.has(candidate.family)) continue;
    const otherIntent = NODE_TYPE_INTENT[family];
    if (otherIntent.strongSignals.test(contextLower)) {
      challenges.push(
        `context has a strong signal for ${family} but candidate is ${candidate.family}`
      );
      break; // One mismatch is enough
    }
  }

  // 5. doNotUseWhen: check if any anti-pattern descriptions match the context
  for (const antiPattern of intent.doNotUseWhen) {
    const antiLower = antiPattern.toLowerCase();
    // Extract key phrases from the doNotUseWhen text and check against context
    // Only trigger for phrases that are specific enough (> 10 chars, not generic advice)
    if (antiLower.length > 10) {
      // Check for CTE decomposition anti-pattern
      if (/cte\s+decomposition/u.test(antiLower) && /cte\s+decomposition/u.test(contextLower)) {
        challenges.push(`intent says do not use ${candidate.family} for CTE decomposition`);
      }
      // Check for batch ETL anti-pattern
      if (/batch\s+etl/u.test(antiLower) && /batch\s+etl/u.test(contextLower)) {
        challenges.push(`intent says do not use ${candidate.family} for batch ETL`);
      }
      // Check for general/simple transforms anti-pattern
      if (/general.purpose|simple\s+stag/u.test(antiLower) && /general|simple|basic/u.test(contextLower)) {
        challenges.push(`intent says do not use ${candidate.family} for general-purpose transforms`);
      }
    }
  }

  // 6. Package-level challenge: non-base packages for general-purpose context
  // Data vault, functional, and other specialized packages should not be selected
  // for standard staging/transform/join operations — unless already in workspace.
  if (
    candidate.packageAlias &&
    !/base.node.type/iu.test(candidate.packageAlias) &&
    !candidate.observedInWorkspace &&
    /batch\s+etl|staging|transform|general/iu.test(contextLower)
  ) {
    challenges.push(
      `from specialized package "${candidate.packageAlias}" — not appropriate for general-purpose transforms`
    );
  }

  // 7. "Copy of" types should be challenged in favor of originals
  if (candidate.displayName && /\bcopy\s+of\b/iu.test(candidate.displayName)) {
    challenges.push(
      `"${candidate.displayName}" is a cloned type — original should be preferred`
    );
  }

  return challenges;
}

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
  // Two rounds of scoring + challenge to ensure the best type is selected.
  // The challenge step uses the intent doc to verify the top candidate is appropriate.

  const contextText = [context.goal, context.targetName].filter(Boolean).join(" ");

  let sorted = rankCandidates(candidates);
  const challengeLog: string[] = [];

  for (let round = 1; round <= 2; round++) {
    const top = sorted[0] ?? null;
    if (!top) break;

    const challenges = challengeCandidate(top, context, contextText);
    if (challenges.length > 0) {
      challengeLog.push(`Round ${round}: challenged "${top.nodeType}" (${top.displayName ?? top.family}) — ${challenges.join("; ")}`);
      // Disqualify the top candidate and re-rank
      top.score = -Infinity;
      top.reasons.push(...challenges.map((c) => `CHALLENGED: ${c}`));
      sorted = rankCandidates(sorted);
    } else {
      challengeLog.push(`Round ${round}: "${top.nodeType}" (${top.displayName ?? top.family}) passed challenge`);
      break; // Candidate passed — no need for another round
    }
  }

  let selected: InternalPipelineNodeTypeCandidate | null = sorted[0] ?? null;
  // Skip candidates that were disqualified
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
