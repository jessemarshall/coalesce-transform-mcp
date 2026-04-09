import { NODE_TYPE_INTENT, hasAntiSignal, detectSpecializedPatternPenalty, detectSpecializedPatternMatch } from "./node-type-intent.js";
import {
  inferFamily,
  isAutoExecutableFamily,
  matchesNodeTypeIdentity,
  type PipelineNodeTypeFamily,
  type InternalPipelineNodeTypeCandidate,
  type PipelineNodeTypeSelectionContext,
} from "./node-type-selection.js";

// ---------------------------------------------------------------------------
// Node type category — the real decision
// ---------------------------------------------------------------------------

type NodeTypeCategory =
  | "general-purpose"   // stage / work (interchangeable)
  | "view"              // no materialization
  | "persistent"        // CDC, change tracking
  | "dimensional"       // dimension, fact — requires explicit intent
  | "data-vault";       // hub, satellite, link — requires explicit intent

// ---------------------------------------------------------------------------
// Use-case context
// ---------------------------------------------------------------------------

export function buildUseCaseContext(context: PipelineNodeTypeSelectionContext): {
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

// ---------------------------------------------------------------------------
// Family scoring
// ---------------------------------------------------------------------------

function familyScore(
  candidate: InternalPipelineNodeTypeCandidate,
  useCase: { desiredFamilies: PipelineNodeTypeFamily[]; category: NodeTypeCategory }
): { score: number; reasons: string[] } {
  const { desiredFamilies, category } = useCase;

  if (desiredFamilies.length === 0) {
    return { score: 0, reasons: [] };
  }

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

// ---------------------------------------------------------------------------
// Candidate scoring
// ---------------------------------------------------------------------------

export function scoreCandidate(
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

  // Anti-signal penalty
  const contextText = [context.goal, context.targetName].filter(Boolean).join(" ");
  if (contextText.length > 0 && hasAntiSignal(candidate.family, contextText)) {
    score -= 30;
    reasons.push(`context suggests this is not a ${candidate.family} use case`);
  }

  // Semantic config penalty
  const intent = NODE_TYPE_INTENT[candidate.family];
  if (intent.requiresSemanticConfig && !useCase.dimensionalModeling) {
    score -= 15;
    reasons.push(`${candidate.family} requires semantic config (business keys, SCD) — no dimensional modeling intent detected`);
  }

  // Specialized materialization patterns
  const candidateSignals = [candidate.nodeType, candidate.displayName ?? "", candidate.shortName ?? ""].join(" ");
  const specializedResult = detectSpecializedPatternPenalty(candidateSignals, contextText);
  if (specializedResult) {
    score = -Infinity;
    reasons.push(`not applicable: ${specializedResult.reason}`);
  } else {
    const positiveMatch = detectSpecializedPatternMatch(candidateSignals, contextText);
    if (positiveMatch) {
      score += 50;
      reasons.push(`context explicitly requests ${positiveMatch} pattern`);
    }
  }

  // Hard exclusion: data-vault package types
  if (
    useCase.category !== "data-vault" &&
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

  // Non-base type exclusion for general-purpose selections
  if (useCase.category === "general-purpose" && candidate.source === "repo") {
    const isBasePackage = candidate.packageAlias && /base.node.type/iu.test(candidate.packageAlias);
    if (isBasePackage) {
      score += 15;
      reasons.push("from base node type package — preferred default");
    } else if (!candidate.observedInWorkspace) {
      score = -Infinity;
      const source = candidate.packageAlias
        ? `non-base package "${candidate.packageAlias}"`
        : `custom repo type "${candidate.nodeType}"`;
      reasons.push(`excluded: ${source} — not observed in workspace. Use base node types or workspace-established types.`);
      return { ...candidate, score, reasons: Array.from(new Set(reasons)) };
    }
  }

  // Penalize "Copy of" types
  if (
    candidate.displayName &&
    /\bcopy\s+of\b/iu.test(candidate.displayName) &&
    !context.explicitNodeType
  ) {
    score -= 30;
    reasons.push(`"${candidate.displayName}" is a cloned type — prefer the original`);
  }

  // Unknown family types penalty
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

// ---------------------------------------------------------------------------
// Candidate challenge
// ---------------------------------------------------------------------------

export function challengeCandidate(
  candidate: InternalPipelineNodeTypeCandidate,
  context: PipelineNodeTypeSelectionContext,
  contextText: string
): string[] {
  const challenges: string[] = [];
  const intent = NODE_TYPE_INTENT[candidate.family];
  const contextLower = contextText.toLowerCase();

  // 1. Anti-signals
  if (intent.antiSignals !== null && intent.antiSignals.test(contextLower)) {
    challenges.push(
      `${candidate.family} anti-signal matched — intent says not for this context`
    );
  }

  // 2. Specialized pattern penalty
  const candidateSignals = [
    candidate.nodeType,
    candidate.displayName ?? "",
    candidate.shortName ?? "",
  ].join(" ");
  const specializedPenalty = detectSpecializedPatternPenalty(candidateSignals, contextText);
  if (specializedPenalty) {
    challenges.push(specializedPenalty.reason);
  }

  // 3. Requires semantic config but no dimensional modeling intent
  if (intent.requiresSemanticConfig) {
    const hasDimensionalIntent =
      /\bdimension(al)?\s+model/u.test(contextLower) ||
      /\bstar\s+schema\b/u.test(contextLower) ||
      /\bsnowflake\s+schema\b/u.test(contextLower) ||
      /\bdata\s*vault\b/u.test(contextLower);
    const hasOwnStrongSignal = intent.strongSignals.test(contextLower);
    if (!hasDimensionalIntent && !hasOwnStrongSignal) {
      challenges.push(
        `${candidate.family} requires semantic config (business keys, SCD) but no dimensional modeling intent detected`
      );
    }
  }

  // 4. Another family has a strong signal match but this candidate is a different CATEGORY
  const generalPurposeFamilies = new Set<PipelineNodeTypeFamily>(["stage", "work"]);
  const signalCheckOrder: PipelineNodeTypeFamily[] = [
    "persistent-stage", "dimension", "fact", "hub", "satellite", "link",
    "view", "work", "stage",
  ];
  for (const family of signalCheckOrder) {
    if (family === candidate.family) continue;
    if (generalPurposeFamilies.has(family) && generalPurposeFamilies.has(candidate.family)) continue;
    const otherIntent = NODE_TYPE_INTENT[family];
    if (otherIntent.strongSignals.test(contextLower)) {
      challenges.push(
        `context has a strong signal for ${family} but candidate is ${candidate.family}`
      );
      break;
    }
  }

  // 5. doNotUseWhen
  for (const antiPattern of intent.doNotUseWhen) {
    const antiLower = antiPattern.toLowerCase();
    if (antiLower.length > 10) {
      if (/cte\s+decomposition/u.test(antiLower) && /cte\s+decomposition/u.test(contextLower)) {
        challenges.push(`intent says do not use ${candidate.family} for CTE decomposition`);
      }
      if (/batch\s+etl/u.test(antiLower) && /batch\s+etl/u.test(contextLower)) {
        challenges.push(`intent says do not use ${candidate.family} for batch ETL`);
      }
      if (/general.purpose|simple\s+stag/u.test(antiLower) && /general|simple|basic/u.test(contextLower)) {
        challenges.push(`intent says do not use ${candidate.family} for general-purpose transforms`);
      }
    }
  }

  // 6. Package-level challenge
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

  // 7. "Copy of" types
  if (candidate.displayName && /\bcopy\s+of\b/iu.test(candidate.displayName)) {
    challenges.push(
      `"${candidate.displayName}" is a cloned type — original should be preferred`
    );
  }

  return challenges;
}

// ---------------------------------------------------------------------------
// Ranking
// ---------------------------------------------------------------------------

export function rankCandidates(
  candidates: InternalPipelineNodeTypeCandidate[]
): InternalPipelineNodeTypeCandidate[] {
  return [...candidates].sort((a, b) => b.score - a.score);
}
