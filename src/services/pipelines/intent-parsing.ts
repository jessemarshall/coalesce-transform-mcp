// ---------------------------------------------------------------------------
// Intent parsing — pure text analysis, no API calls
// ---------------------------------------------------------------------------

export type IntentOperation = "stage" | "join" | "aggregate" | "union";

export type IntentColumn = {
  name: string;
  aggregateFunction: string | null;
  expression: string | null;
};

export type IntentStep = {
  operation: IntentOperation;
  entityNames: string[];
  targetName: string | null;
  columns: IntentColumn[];
  groupByColumns: string[];
  filters: string[];
  joinKey: string | null;
  joinType: "INNER" | "LEFT" | "FULL OUTER" | null;
};

export type ParsedIntent = {
  steps: IntentStep[];
  rawIntent: string;
  warnings: string[];
  openQuestions: string[];
};

// ---------------------------------------------------------------------------
// Patterns
// ---------------------------------------------------------------------------

const AGGREGATE_PATTERNS: Array<{
  pattern: RegExp;
  fn: string;
}> = [
  { pattern: /\b(?:total|sum(?:\s+of)?)\s+(\w+)/gi, fn: "SUM" },
  { pattern: /\b(?:count(?:\s+of)?)\s+(\w+)/gi, fn: "COUNT" },
  { pattern: /\b(?:average|avg(?:\s+of)?)\s+(\w+)/gi, fn: "AVG" },
  { pattern: /\b(?:max(?:imum)?(?:\s+of)?)\s+(\w+)/gi, fn: "MAX" },
  { pattern: /\b(?:min(?:imum)?(?:\s+of)?)\s+(\w+)/gi, fn: "MIN" },
];

const JOIN_KEYWORDS = /\b(?:combine|join|merge|link|connect|match)\b/i;
const AGGREGATE_KEYWORDS = /\b(?:aggregate|group|sum|total|count|average|avg|rollup|summarize|summarise)\b/i;
const FILTER_KEYWORDS = /\b(?:filter|where|only|exclude|remove|active|inactive)\b/i;
const UNION_KEYWORDS = /\b(?:union|stack|append|combine\s+all)\b/i;
const STAGE_KEYWORDS = /\b(?:stage|load|ingest|source|land|raw)\b/i;

const GROUP_BY_PATTERN = /\b(?:(?:group|aggregate|summarize|summarise|rollup)\s+by|per|by)\s+([\w\s,]+?)(?:\s+(?:and|then|with|from|where|filter)|$)/gi;

const FILTER_PATTERN = /\b(?:filter(?:ed)?(?:\s+(?:to|for|by|on))?|where|only(?:\s+(?:include|keep|show))?)\s+([\w\s=<>!']+?)(?:\s+(?:and\s+(?:group|aggregate|join)|then|from)|$)/gi;

// Matches "on COLUMN_NAME" or "using COLUMN_NAME" near join context
const JOIN_ON_PATTERN = /\b(?:(?:left|right|inner|full|outer|cross)\s+)?(?:join|combine|merge|link|connect|match)\s+[\w_]+\s+(?:and|with|to)\s+[\w_]+\s+(?:on|using)\s+([\w_]+)/gi;

const ENTITY_PATTERNS = [
  // "join X and Y", "combine X with Y", "left join X and Y"
  /\b(?:(?:left|right|inner|full|outer|cross)\s+)?(?:join|combine|merge|link|connect|match)\s+([\w_]+)\s+(?:and|with|to)\s+([\w_]+)/gi,
  // "from X and Y"
  /\bfrom\s+([\w_]+)\s+(?:and|,)\s+([\w_]+)/gi,
  // "stage/load/ingest X" — single entity after staging verb
  /\b(?:stage|load|ingest)\s+(?:the\s+)?(?:raw\s+)?([\w_]+)/gi,
  // standalone table-like names (2+ uppercase chars with underscores)
  /\b([A-Z][A-Z0-9_]{2,})\b/g,
];

// ---------------------------------------------------------------------------
// Extraction helpers
// ---------------------------------------------------------------------------

function extractEntityNames(intent: string): string[] {
  const entities = new Set<string>();

  for (const pattern of ENTITY_PATTERNS.slice(0, 3)) {
    let match: RegExpExecArray | null;
    const re = new RegExp(pattern.source, pattern.flags);
    while ((match = re.exec(intent)) !== null) {
      if (match[1]) entities.add(match[1].toUpperCase());
      if (match[2]) entities.add(match[2].toUpperCase());
    }
  }

  if (entities.size > 0) {
    return Array.from(entities);
  }

  const uppercasePattern = ENTITY_PATTERNS[3]!;
  let match: RegExpExecArray | null;
  const re = new RegExp(uppercasePattern.source, uppercasePattern.flags);
  const STOP_WORDS = new Set([
    "SUM", "COUNT", "AVG", "MAX", "MIN", "GROUP", "TOTAL", "FILTER",
    "WHERE", "JOIN", "AND", "FROM", "INTO", "INNER", "LEFT", "FULL",
    "OUTER", "UNION", "ALL", "SELECT", "WITH", "THE", "FOR",
  ]);
  while ((match = re.exec(intent)) !== null) {
    if (match[1] && !STOP_WORDS.has(match[1])) {
      entities.add(match[1]);
    }
  }

  return Array.from(entities);
}

function extractAggregateColumns(intent: string): IntentColumn[] {
  const columns: IntentColumn[] = [];
  const seen = new Set<string>();

  for (const { pattern, fn } of AGGREGATE_PATTERNS) {
    let match: RegExpExecArray | null;
    const re = new RegExp(pattern.source, pattern.flags);
    while ((match = re.exec(intent)) !== null) {
      const colName = match[1]?.toUpperCase();
      if (colName && !seen.has(`${fn}:${colName}`)) {
        seen.add(`${fn}:${colName}`);
        columns.push({
          name: `${fn}_${colName}`,
          aggregateFunction: fn,
          expression: `${fn}(${colName})`,
        });
      }
    }
  }

  return columns;
}

function extractGroupByColumns(intent: string): string[] {
  const columns: string[] = [];
  const seen = new Set<string>();

  let match: RegExpExecArray | null;
  const re = new RegExp(GROUP_BY_PATTERN.source, GROUP_BY_PATTERN.flags);
  while ((match = re.exec(intent)) !== null) {
    if (match[1]) {
      const parts = match[1].split(/[,\s]+/).filter((p) => p.length > 0);
      for (const part of parts) {
        const col = part.toUpperCase().replace(/[^A-Z0-9_]/g, "");
        if (col.length > 0 && !seen.has(col)) {
          seen.add(col);
          columns.push(col);
        }
      }
    }
  }

  return columns;
}

function extractFilters(intent: string): string[] {
  const filters: string[] = [];

  let match: RegExpExecArray | null;
  const re = new RegExp(FILTER_PATTERN.source, FILTER_PATTERN.flags);
  while ((match = re.exec(intent)) !== null) {
    if (match[1]) {
      const filter = match[1].trim();
      if (filter.length > 2) {
        filters.push(filter);
      }
    }
  }

  return filters;
}

function extractJoinKey(intent: string): string | null {
  let match: RegExpExecArray | null;
  const re = new RegExp(JOIN_ON_PATTERN.source, JOIN_ON_PATTERN.flags);
  while ((match = re.exec(intent)) !== null) {
    if (match[1]) {
      return match[1].toUpperCase();
    }
  }
  return null;
}

function detectJoinType(intent: string): "INNER" | "LEFT" | "FULL OUTER" | null {
  if (/\bleft\s+(?:outer\s+)?join\b/i.test(intent)) return "LEFT";
  if (/\bfull\s+(?:outer\s+)?join\b/i.test(intent)) return "FULL OUTER";
  if (/\b(?:inner\s+)?join\b/i.test(intent)) return "INNER";
  if (JOIN_KEYWORDS.test(intent)) return "INNER";
  return null;
}

// ---------------------------------------------------------------------------
// Main parser
// ---------------------------------------------------------------------------

export function parseIntent(intentText: string): ParsedIntent {
  const warnings: string[] = [];
  const openQuestions: string[] = [];
  const steps: IntentStep[] = [];

  const entityNames = extractEntityNames(intentText);
  const hasJoin = JOIN_KEYWORDS.test(intentText);
  const hasAggregate = AGGREGATE_KEYWORDS.test(intentText);
  const hasFilter = FILTER_KEYWORDS.test(intentText);
  const hasUnion = UNION_KEYWORDS.test(intentText);
  const hasStage = STAGE_KEYWORDS.test(intentText) && !hasJoin && !hasAggregate;

  if (entityNames.length === 0) {
    openQuestions.push(
      "Could not identify source tables or nodes from the description. " +
      "Please mention the table/node names explicitly (e.g., 'combine CUSTOMERS and ORDERS')."
    );
  }

  // Build steps based on detected operations
  if (hasUnion) {
    steps.push({
      operation: "union",
      entityNames,
      targetName: null,
      columns: [],
      groupByColumns: [],
      filters: [],
      joinKey: null,
      joinType: null,
    });
  } else if (hasJoin && entityNames.length < 2) {
    openQuestions.push(
      `A join operation requires at least two source tables, but only ${entityNames.length === 0 ? "none were" : `"${entityNames[0]}" was`} found. ` +
      `Please mention both tables (e.g., 'join CUSTOMERS and ORDERS on CUSTOMER_ID').`
    );
  } else if (hasJoin && entityNames.length >= 2) {
    const joinKey = extractJoinKey(intentText);
    const joinType = detectJoinType(intentText);

    steps.push({
      operation: "join",
      entityNames,
      targetName: null,
      columns: [],
      groupByColumns: [],
      filters: [],
      joinKey,
      joinType,
    });

    if (!joinKey) {
      openQuestions.push(
        `What column should be used to join ${entityNames.join(" and ")}? ` +
        `(e.g., 'join on CUSTOMER_ID')`
      );
    }
  } else if (hasStage || (!hasJoin && !hasAggregate && entityNames.length > 0)) {
    for (const entityName of entityNames) {
      steps.push({
        operation: "stage",
        entityNames: [entityName],
        targetName: null,
        columns: [],
        groupByColumns: [],
        filters: [],
        joinKey: null,
        joinType: null,
      });
    }
  }

  // Add aggregate step if detected
  if (hasAggregate) {
    const aggregateColumns = extractAggregateColumns(intentText);
    const groupByColumns = extractGroupByColumns(intentText);

    if (groupByColumns.length === 0 && aggregateColumns.length > 0) {
      openQuestions.push(
        "Aggregation detected but no GROUP BY columns found. " +
        "Which columns should be used for grouping? (e.g., 'group by REGION, CATEGORY')"
      );
    }

    const aggEntities = steps.length > 0
      ? [] // will reference previous step's output
      : entityNames;

    steps.push({
      operation: "aggregate",
      entityNames: aggEntities,
      targetName: null,
      columns: aggregateColumns,
      groupByColumns,
      filters: [],
      joinKey: null,
      joinType: null,
    });
  }

  // Add filter to the last step if detected
  if (hasFilter && steps.length > 0) {
    const filters = extractFilters(intentText);
    const lastStep = steps[steps.length - 1]!;
    lastStep.filters.push(...filters);
  }

  if (steps.length === 0) {
    openQuestions.push(
      "Could not determine what pipeline operations to perform. " +
      "Please describe the transformation (e.g., 'join CUSTOMERS and ORDERS on CUSTOMER_ID, then aggregate total REVENUE by REGION')."
    );
  }

  return {
    steps,
    rawIntent: intentText,
    warnings,
    openQuestions,
  };
}
