/**
 * Node Type Intent Corpus
 *
 * Authoritative reference for what each Coalesce node type family is designed for,
 * derived from official Coalesce node type package READMEs.
 *
 * Used by the scoring logic to make informed decisions about which node type
 * fits a given use case — instead of relying on name heuristics alone.
 */

import type { PipelineNodeTypeFamily } from "./node-type-selection.js";

export type NodeTypeIntent = {
  family: PipelineNodeTypeFamily;
  /** What this node type is designed for */
  purpose: string;
  /** What materialization it creates */
  materialization: string[];
  /** When to use this type — positive signals */
  useWhen: string[];
  /** When NOT to use this type — anti-patterns */
  doNotUseWhen: string[];
  /** Whether it requires semantic config (business keys, SCD, etc.) */
  requiresSemanticConfig: boolean;
  /** Whether it handles multi-source (joins, unions) */
  supportsMultiSource: boolean;
  /** Keywords in goal/name that strongly indicate this type */
  strongSignals: RegExp;
  /** Keywords that should NOT trigger this type */
  antiSignals: RegExp | null;
};

/**
 * Specialized materialization patterns that exist across families.
 * These are NOT families — they modify how a family-level node is materialized.
 * Scoring penalizes these when the context doesn't explicitly request them.
 */
export type SpecializedPattern = {
  name: string;
  /** Regex to detect this pattern in candidate name/displayName */
  detect: RegExp;
  /** Regex the context must match to avoid penalty */
  contextRequired: RegExp;
  /** Scoring penalty when context doesn't match */
  penalty: number;
  /** Why this pattern exists and when to use it */
  purpose: string;
  /** When NOT to use this pattern */
  doNotUseWhen: string[];
};

/**
 * Intent corpus indexed by family.
 *
 * Source: Coalesce node type package READMEs
 * - Coalesce-Base-Node-Types
 * - Coalesce-Base-Node-Types---Advanced-Deploy
 * - Dynamic-Table-Nodes
 * - Incremental-Nodes
 * - Materialized-View-Node
 * - functional-node-types
 * - create-alter-node-types
 */
export const NODE_TYPE_INTENT: Record<PipelineNodeTypeFamily, NodeTypeIntent> = {
  stage: {
    family: "stage",
    purpose: "General-purpose intermediate staging for raw data processing before transformation. The default workhorse node type.",
    materialization: ["table", "view"],
    useWhen: [
      "Single-source SELECT/WHERE transforms",
      "Column renames, type casts, filters",
      "Landing raw data into a staging layer",
      "Any general-purpose transformation without special requirements",
      "GROUP BY aggregations (Stage/Work handle these natively)",
      "Multi-source joins (Stage/Work both support joins via sourceMapping)",
    ],
    doNotUseWhen: [
      "You need change tracking / CDC (use Persistent Stage)",
      "You need SCD Type 1/2 with business keys (use Dimension)",
      "You need no materialization at all (use View)",
    ],
    requiresSemanticConfig: false,
    supportsMultiSource: true,
    strongSignals: /\bstag(e|ing)\b|\bstg[_-]|\bstage[_-]|\bbronze\b|\blanding\b|\bsilver\b/u,
    antiSignals: null,
  },

  work: {
    family: "work",
    purpose: "Intermediary processing node for joins, transforms, and multi-source operations. Interchangeable with Stage for most patterns.",
    materialization: ["table", "view"],
    useWhen: [
      "Multi-source joins (INNER, LEFT, RIGHT, FULL OUTER)",
      "Intermediate transforms between staging and mart layers",
      "UNION / UNION ALL operations",
      "Any general-purpose transformation",
      "GROUP BY aggregations",
    ],
    doNotUseWhen: [
      "You need change tracking / CDC (use Persistent Stage)",
      "You need SCD Type 1/2 with business keys (use Dimension)",
    ],
    requiresSemanticConfig: false,
    supportsMultiSource: true,
    strongSignals: /\bwork\b|\bwork[_-]|\bwrk[_-]|\bintermediate\b|\btransform\b/u,
    antiSignals: null,
  },

  view: {
    family: "view",
    purpose: "Virtual table with no physical materialization. Query recalculates on every access. Good for lightweight transforms or cost savings.",
    materialization: ["view"],
    useWhen: [
      "No materialization needed (virtual table)",
      "Lightweight transforms or simple projections",
      "Secure views for data access control",
      "Cost optimization when re-computation is acceptable",
    ],
    doNotUseWhen: [
      "Performance-critical queries that run frequently",
      "Large aggregations that are expensive to recompute",
      "You need persistent storage for downstream consumers",
    ],
    requiresSemanticConfig: false,
    supportsMultiSource: true,
    strongSignals: /\bview\b|\bview[_-]|\bvw[_-]|\bsecure\s*view\b/u,
    antiSignals: null,
  },

  "persistent-stage": {
    family: "persistent-stage",
    purpose: "Maintain data persistence across execution cycles with change tracking. Supports business keys and Type 1/Type 2 CDC.",
    materialization: ["table"],
    useWhen: [
      "Change data capture (CDC) is required",
      "Track historical changes using business keys",
      "Type 1 or Type 2 slowly changing data",
      "Data persistence across multiple execution cycles",
    ],
    doNotUseWhen: [
      "Simple staging without change tracking — use Stage/Work",
      "General-purpose transforms — use Stage/Work",
      "No business key is defined",
      "Batch ETL where you just need TRUNCATE+INSERT",
    ],
    requiresSemanticConfig: true,
    supportsMultiSource: true,
    strongSignals: /\bpersistent\s*stage\b|\bcdc\b|\bchange\s*track/u,
    antiSignals: /\bstaging\s+layer\b|\bgeneral\b|\bsimple\b/u,
  },

  dimension: {
    family: "dimension",
    purpose: "Store descriptive business context (customers, products, locations). Requires business keys. Supports SCD Type 1/2, zero key records.",
    materialization: ["table", "view"],
    useWhen: [
      "Building a dimensional model (star/snowflake schema)",
      "Descriptive entity tables (customers, products, locations, employees)",
      "SCD Type 1 or Type 2 tracking on business entities",
      "Node name explicitly starts with dim_ or dimension_",
    ],
    doNotUseWhen: [
      "Just doing a GROUP BY — that's a transform, use Stage/Work",
      "No business key is defined",
      "Generic data processing or staging",
      "SQL has aggregation functions but no dimensional modeling intent",
      "CTE decomposition — CTEs become Stage/Work nodes, not Dimensions",
    ],
    requiresSemanticConfig: true,
    supportsMultiSource: true,
    strongSignals: /\bdimension\b|\bdimension[_-]|\bdim[_-]|\bscd\b|\bslowly\s*changing/u,
    antiSignals: /\bstaging\b|\btransform\b|\bintermediate\b/u,
  },

  fact: {
    family: "fact",
    purpose: "Aggregate measures and numerical business data (sales, costs, profits). Requires business keys. Part of dimensional modeling.",
    materialization: ["table", "view"],
    useWhen: [
      "Building a fact table in a dimensional model",
      "Storing business measures (revenue, quantity, cost)",
      "Node name explicitly starts with fct_ or fact_",
      "Grain-level transactional data with foreign keys to dimensions",
    ],
    doNotUseWhen: [
      "Just doing a GROUP BY or SUM — that's a transform, use Stage/Work",
      "No business key or grain is defined",
      "Generic aggregation or intermediate processing",
      "CTE decomposition — aggregation CTEs become Stage/Work, not Facts",
    ],
    requiresSemanticConfig: true,
    supportsMultiSource: true,
    strongSignals: /\bfact\b|\bfact[_-]|\bfct[_-]|\bgrain\b|\bmeasure\b.*\bdimensional\b/u,
    antiSignals: /\bstaging\b|\btransform\b|\bintermediate\b/u,
  },

  hub: {
    family: "hub",
    purpose: "Data Vault hub entity. Stores unique business keys for core business concepts.",
    materialization: ["table"],
    useWhen: [
      "Building a Data Vault model",
      "Hub entity with unique business keys",
    ],
    doNotUseWhen: [
      "Not building a Data Vault",
      "General-purpose transforms",
    ],
    requiresSemanticConfig: true,
    supportsMultiSource: false,
    strongSignals: /\bhub\b|\bhub[_-]|\bdata\s*vault\b/u,
    antiSignals: null,
  },

  satellite: {
    family: "satellite",
    purpose: "Data Vault satellite. Stores descriptive attributes and change history for a hub.",
    materialization: ["table"],
    useWhen: [
      "Building a Data Vault model",
      "Satellite with descriptive attributes linked to a hub",
    ],
    doNotUseWhen: [
      "Not building a Data Vault",
      "General-purpose transforms",
    ],
    requiresSemanticConfig: true,
    supportsMultiSource: false,
    strongSignals: /\bsatellite\b|\bsat[_-]/u,
    antiSignals: null,
  },

  link: {
    family: "link",
    purpose: "Data Vault link. Stores relationships between two or more hubs.",
    materialization: ["table"],
    useWhen: [
      "Building a Data Vault model",
      "Link entity connecting multiple hubs",
    ],
    doNotUseWhen: [
      "Not building a Data Vault",
      "General-purpose joins (use Work/Stage)",
    ],
    requiresSemanticConfig: true,
    supportsMultiSource: true,
    strongSignals: /\blink[_-]|\bdata\s*vault.*link/u,
    antiSignals: null,
  },

  unknown: {
    family: "unknown",
    purpose: "Unrecognized node type family. May be a custom or specialized type.",
    materialization: ["table", "view"],
    useWhen: [],
    doNotUseWhen: [
      "A known family matches the use case",
    ],
    requiresSemanticConfig: false,
    supportsMultiSource: false,
    strongSignals: /(?!)/u, // never matches
    antiSignals: null,
  },
};

/**
 * Specialized materialization patterns that cross-cut families.
 *
 * These detect node types with specialized materialization behavior
 * (Dynamic Tables, Incremental Loads, Materialized Views, etc.)
 * and penalize them when the context doesn't explicitly call for that pattern.
 *
 * Source: Coalesce node type package READMEs
 */
export const SPECIALIZED_PATTERNS: SpecializedPattern[] = [
  {
    name: "Dynamic Table",
    detect: /dynamic\s*table|dt[_\s-]/u,
    contextRequired: /dynamic\s*table|auto[\s-]*refresh|continuous[\s-]*refresh|near[\s-]*real[\s-]*time|low[\s-]*latency/u,
    penalty: 50,
    purpose: "Snowflake Dynamic Tables with declarative orchestration and automatic lag-based refresh. Snowflake manages the refresh DAG — no manual scheduling needed.",
    doNotUseWhen: [
      "Batch ETL — scheduled runs where you control when data refreshes",
      "Cost-sensitive workloads — DTs incur continuous compute for refresh monitoring",
      "One-time or ad-hoc transforms — no ongoing refresh needed",
      "Standard staging/transform pipelines — use Stage/Work with table materialization",
      "CTE decomposition — CTEs are batch patterns, not streaming",
    ],
  },
  {
    name: "Incremental Load",
    detect: /incremental\s*load|looped\s*load|grouped\s*incremental/u,
    contextRequired: /incremental|high[\s-]*water[\s-]*mark|append[\s-]*only|delta[\s-]*load/u,
    penalty: 50,
    purpose: "Process only new/modified records by comparing against a persistent table using high-water mark tracking. For large tables where full refresh is too expensive.",
    doNotUseWhen: [
      "Full refresh is acceptable (most staging tables)",
      "Source is small enough for TRUNCATE+INSERT",
      "CTE decomposition — CTEs represent full-refresh batch logic",
    ],
  },
  {
    name: "Deferred Merge",
    detect: /deferred\s*merge|append\s*stream|delta\s*stream/u,
    contextRequired: /deferred\s*merge|stream|merge\s*task|high[\s-]*frequency\s*ingestion/u,
    penalty: 50,
    purpose: "Capture incremental changes via Snowflake Streams with scheduled merge tasks. For high-frequency ingestion where immediate merge is too expensive.",
    doNotUseWhen: [
      "Batch ETL with scheduled full or incremental loads",
      "Standard staging/transform pipelines",
      "CTE decomposition",
    ],
  },
  {
    name: "Materialized View",
    detect: /materialized\s*view/u,
    contextRequired: /materialized\s*view|pre[\s-]*compute|expensive\s*aggregat/u,
    penalty: 40,
    purpose: "Snowflake Materialized Views — pre-computed query results that auto-refresh when base data changes. Single-source only. No GROUP BY ALL.",
    doNotUseWhen: [
      "Multi-source joins (materialized views are single-source only)",
      "Standard transforms — use Stage/Work",
      "Views as source (not supported)",
    ],
  },
  {
    name: "Task/DAG",
    detect: /\btask\b|dag\s*root/u,
    contextRequired: /\btask\b|\bdag\b|\bschedul(e|ing)\b|\bcron\b/u,
    penalty: 50,
    purpose: "Snowflake Tasks for scheduled or DAG-based orchestration. Creates task objects, not tables.",
    doNotUseWhen: [
      "Building data transformation nodes",
      "Standard staging/transform pipelines",
    ],
  },
  {
    name: "Data Quality",
    detect: /data\s*quality|dmf|data\s*profil/u,
    contextRequired: /data\s*quality|dmf|profil|metric\s*function/u,
    penalty: 50,
    purpose: "Data quality monitoring (DMF) or statistical profiling. Creates monitoring metadata, not transformation tables.",
    doNotUseWhen: [
      "Building data transformation pipelines",
      "Standard staging/transform/mart patterns",
    ],
  },
  {
    name: "Functional (Date/Time Dimension, Pivot, etc.)",
    detect: /\bpivot\b|\bunpivot\b|match[\s_]recognize|recursive\s*cte|date\s*dimension|time\s*dimension/u,
    contextRequired: /\bpivot\b|\bunpivot\b|match[\s_]recognize|recursive|date\s*dim|time\s*dim|calendar/u,
    penalty: 40,
    purpose: "Specialized transformation patterns: Pivot/Unpivot rows↔columns, Match Recognize for pattern detection, Recursive CTE for hierarchies, Date/Time Dimension generators.",
    doNotUseWhen: [
      "Standard SELECT/WHERE/JOIN transforms",
      "CTE decomposition (unless the CTE itself is a PIVOT/UNPIVOT)",
    ],
  },
];

/**
 * Check whether a use case matches a family's anti-signals.
 * Anti-signals indicate the family should NOT be used for this context.
 */
export function hasAntiSignal(family: PipelineNodeTypeFamily, text: string): boolean {
  const intent = NODE_TYPE_INTENT[family];
  return intent.antiSignals !== null && intent.antiSignals.test(text.toLowerCase());
}

/**
 * Check whether a use case matches a family's strong signals.
 */
export function hasStrongSignal(family: PipelineNodeTypeFamily, text: string): boolean {
  return NODE_TYPE_INTENT[family].strongSignals.test(text.toLowerCase());
}

/**
 * Detect if a candidate matches a specialized pattern where the context
 * does NOT request it. Returns the penalty to apply if found, or null otherwise.
 */
export function detectSpecializedPatternPenalty(
  candidateSignals: string,
  contextText: string
): { penalty: number; reason: string } | null {
  const lower = candidateSignals.toLowerCase();
  const contextLower = contextText.toLowerCase();

  for (const pattern of SPECIALIZED_PATTERNS) {
    if (pattern.detect.test(lower) && !pattern.contextRequired.test(contextLower)) {
      return {
        penalty: pattern.penalty,
        reason: `${pattern.name} pattern not requested — ${pattern.doNotUseWhen[0] ?? "use standard node types instead"}`,
      };
    }
  }

  return null;
}

/**
 * Detect if a candidate matches a specialized pattern AND the context
 * explicitly requests it. Returns the pattern name if it's a positive match.
 */
export function detectSpecializedPatternMatch(
  candidateSignals: string,
  contextText: string
): string | null {
  const lower = candidateSignals.toLowerCase();
  const contextLower = contextText.toLowerCase();

  for (const pattern of SPECIALIZED_PATTERNS) {
    if (pattern.detect.test(lower) && pattern.contextRequired.test(contextLower)) {
      return pattern.name;
    }
  }

  return null;
}
