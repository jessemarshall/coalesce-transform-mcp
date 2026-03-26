import { z } from "zod";
import { randomUUID } from "node:crypto";
import type { CoalesceClient } from "../../client.js";
import { validatePathSegment } from "../../coalesce/types.js";
import {
  getWorkspaceNode,
  listWorkspaceNodes,
} from "../../coalesce/api/nodes.js";
import { listWorkspaceNodeTypes } from "../workspace/mutations.js";
import { isPlainObject, uniqueInOrder } from "../../utils.js";
import { NodeConfigInputSchema } from "../../schemas/node-payloads.js";
import {
  selectPipelineNodeType,
  type PipelineNodeTypeFamily,
  type PipelineNodeTypeSelection,
  type PipelineTemplateDefaults,
} from "./node-type-selection.js";

type PipelineIntent = "sql" | "goal";
type PipelineStatus = "ready" | "needs_clarification";
type PipelineNodeType = string;

type PlannedSelectItemKind = "column" | "expression";

type PlannedSelectItem = {
  expression: string;
  outputName: string | null;
  sourceNodeAlias: string | null;
  sourceNodeName: string | null;
  sourceNodeID: string | null;
  sourceColumnName: string | null;
  kind: PlannedSelectItemKind;
  supported: boolean;
  reason?: string;
};

type PlannedSourceRef = {
  locationName: string;
  nodeName: string;
  alias: string | null;
  nodeID: string | null;
};

export type PlannedPipelineNode = {
  planNodeID: string;
  name: string;
  nodeType: PipelineNodeType;
  nodeTypeFamily?: PipelineNodeTypeFamily | null;
  predecessorNodeIDs: string[];
  predecessorPlanNodeIDs: string[];
  predecessorNodeNames: string[];
  description: string | null;
  sql: string | null;
  selectItems: PlannedSelectItem[];
  outputColumnNames: string[];
  configOverrides: Record<string, unknown>;
  sourceRefs: PlannedSourceRef[];
  joinCondition: string | null;
  location: {
    locationName?: string;
    database?: string;
    schema?: string;
  };
  requiresFullSetNode: boolean;
  templateDefaults?: PipelineTemplateDefaults;
};

type PipelinePlan = {
  version: 1;
  intent: PipelineIntent;
  status: PipelineStatus;
  workspaceID: string;
  platform: string | null;
  goal: string | null;
  sql: string | null;
  nodes: PlannedPipelineNode[];
  assumptions: string[];
  openQuestions: string[];
  warnings: string[];
  supportedNodeTypes: PipelineNodeType[];
  nodeTypeSelection?: PipelineNodeTypeSelection;
  cteNodeSummary?: CteNodeSummary[];
  STOP_AND_CONFIRM?: string;
};

type CteNodeSummary = {
  name: string;
  nodeType: string;
  pattern: "staging" | "multiSource" | "aggregation";
  sourceTable: string | null;
  columnCount: number;
  transforms: Array<{ column: string; expression: string }>;
  passthroughColumns: string[];
  whereFilter: string | null;
  hasGroupBy: boolean;
  hasJoin: boolean;
  dependsOn: string[];
  /** Structured columns for single-call creation (non-GROUP-BY CTEs) */
  columnsParam?: Array<{ name: string; transform?: string }>;
  /** GROUP BY column expressions for single-call aggregation */
  groupByColumnsParam?: string[];
  /** Aggregate columns for single-call aggregation */
  aggregatesParam?: Array<{ name: string; function: string; expression: string }>;
};

const PlannedSelectItemSchema = z
  .object({
    expression: z.string(),
    outputName: z.string().nullable(),
    sourceNodeAlias: z.string().nullable(),
    sourceNodeName: z.string().nullable(),
    sourceNodeID: z.string().nullable(),
    sourceColumnName: z.string().nullable(),
    kind: z.enum(["column", "expression"]),
    supported: z.boolean(),
    reason: z.string().optional(),
  })
  .strict();

const PlannedPipelineNodeSchema = z
  .object({
    planNodeID: z.string(),
    name: z.string(),
    nodeType: z.string(),
    nodeTypeFamily: z
      .enum([
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
      ])
      .nullable()
      .optional(),
    predecessorNodeIDs: z.array(z.string()),
    predecessorPlanNodeIDs: z.array(z.string()),
    predecessorNodeNames: z.array(z.string()),
    description: z.string().nullable(),
    sql: z.string().nullable(),
    selectItems: z.array(PlannedSelectItemSchema),
    outputColumnNames: z.array(z.string()),
    configOverrides: NodeConfigInputSchema,
    sourceRefs: z.array(
      z
        .object({
          locationName: z.string(),
          nodeName: z.string(),
          alias: z.string().nullable(),
          nodeID: z.string().nullable(),
        })
        .strict()
    ),
    joinCondition: z.string().nullable(),
    location: z
      .object({
        locationName: z.string().optional(),
        database: z.string().optional(),
        schema: z.string().optional(),
      })
      .strict(),
    requiresFullSetNode: z.boolean(),
    templateDefaults: z
      .object({
        inferredTopLevelFields: z.record(z.unknown()),
        inferredConfig: NodeConfigInputSchema,
      })
      .strict()
      .optional(),
  })
  .strict();

export const PipelinePlanSchema = z
  .object({
    version: z.literal(1),
    intent: z.enum(["sql", "goal"]),
    status: z.enum(["ready", "needs_clarification"]),
    workspaceID: z.string(),
    platform: z.string().nullable(),
    goal: z.string().nullable(),
    sql: z.string().nullable(),
    nodes: z.array(PlannedPipelineNodeSchema),
    assumptions: z.array(z.string()),
    openQuestions: z.array(z.string()),
    warnings: z.array(z.string()),
    supportedNodeTypes: z.array(z.string()),
    nodeTypeSelection: z.record(z.unknown()).optional(),
    cteNodeSummary: z.array(z.record(z.unknown())).optional(),
    STOP_AND_CONFIRM: z.string().optional(),
  })
  .strict();

type WorkspaceNodeIndexEntry = {
  id: string;
  name: string;
  nodeType: string | null;
  locationName: string | null;
};

type ResolvedSqlRef = {
  locationName: string;
  nodeName: string;
  alias: string | null;
  nodeID: string | null;
};

type ParsedSqlSourceRef = ResolvedSqlRef & {
  sourceStyle: "coalesce_ref" | "table_name";
  locationCandidates: string[];
  relationStart: number;
  relationEnd: number;
};

type SqlParseResult = {
  refs: ParsedSqlSourceRef[];
  selectItems: PlannedSelectItem[];
  warnings: string[];
};

type WorkspaceNodeTypeInventory = {
  nodeTypes: string[];
  counts: Record<string, number>;
  total: number;
  warnings: string[];
};

const WORKSPACE_NODE_PAGE_LIMIT = 200;
export const DEFAULT_STAGE_CONFIG: Record<string, unknown> = {
  postSQL: "",
  preSQL: "",
  testsEnabled: true,
};

export function normalizeSqlIdentifier(identifier: string): string {
  return identifier.trim().replace(/^["`[]|["`\]]$/g, "").toUpperCase();
}

export function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

export function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

export function buildSourceDependencyKey(
  locationName: string | null | undefined,
  nodeName: string
): string {
  return `${normalizeSqlIdentifier(locationName ?? "")}::${normalizeSqlIdentifier(nodeName)}`;
}

export function getUniqueSourceDependencies(
  sourceRefs: Array<{ locationName: string; nodeName: string }>
): Array<{ locationName: string; nodeName: string }> {
  const seen = new Set<string>();
  const dependencies: Array<{ locationName: string; nodeName: string }> = [];

  for (const ref of sourceRefs) {
    const key = buildSourceDependencyKey(ref.locationName, ref.nodeName);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    dependencies.push({
      locationName: ref.locationName,
      nodeName: ref.nodeName,
    });
  }

  return dependencies;
}

function isIdentifierChar(char: string | undefined): boolean {
  return !!char && /[A-Za-z0-9_$]/.test(char);
}

function stripIdentifierQuotes(identifier: string): string {
  const trimmed = identifier.trim();
  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith("`") && trimmed.endsWith("`")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"))
  ) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function findTopLevelKeywordIndex(sql: string, keyword: string, startIndex = 0): number {
  const lowerKeyword = keyword.toLowerCase();
  let parenDepth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inBracket = false;

  for (let index = startIndex; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1];

    if (inSingleQuote) {
      if (char === "'" && next === "'") {
        index += 1;
      } else if (char === "'") {
        inSingleQuote = false;
      }
      continue;
    }
    if (inDoubleQuote) {
      if (char === '"') {
        inDoubleQuote = false;
      }
      continue;
    }
    if (inBacktick) {
      if (char === "`") {
        inBacktick = false;
      }
      continue;
    }
    if (inBracket) {
      if (char === "]") {
        inBracket = false;
      }
      continue;
    }

    if (char === "'") {
      inSingleQuote = true;
      continue;
    }
    if (char === '"') {
      inDoubleQuote = true;
      continue;
    }
    if (char === "`") {
      inBacktick = true;
      continue;
    }
    if (char === "[") {
      inBracket = true;
      continue;
    }
    if (char === "(") {
      parenDepth += 1;
      continue;
    }
    if (char === ")" && parenDepth > 0) {
      parenDepth -= 1;
      continue;
    }
    if (parenDepth !== 0) {
      continue;
    }

    if (
      sql.slice(index, index + lowerKeyword.length).toLowerCase() === lowerKeyword &&
      !isIdentifierChar(sql[index - 1]) &&
      !isIdentifierChar(sql[index + lowerKeyword.length])
    ) {
      return index;
    }
  }

  return -1;
}

function splitTopLevel(value: string, delimiter: string): string[] {
  const parts: string[] = [];
  let current = "";
  let parenDepth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inBracket = false;

  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];
    const next = value[index + 1];

    if (inSingleQuote) {
      current += char;
      if (char === "'" && next === "'") {
        current += next;
        index += 1;
      } else if (char === "'") {
        inSingleQuote = false;
      }
      continue;
    }
    if (inDoubleQuote) {
      current += char;
      if (char === '"') {
        inDoubleQuote = false;
      }
      continue;
    }
    if (inBacktick) {
      current += char;
      if (char === "`") {
        inBacktick = false;
      }
      continue;
    }
    if (inBracket) {
      current += char;
      if (char === "]") {
        inBracket = false;
      }
      continue;
    }

    if (char === "'") {
      inSingleQuote = true;
      current += char;
      continue;
    }
    if (char === '"') {
      inDoubleQuote = true;
      current += char;
      continue;
    }
    if (char === "`") {
      inBacktick = true;
      current += char;
      continue;
    }
    if (char === "[") {
      inBracket = true;
      current += char;
      continue;
    }
    if (char === "(") {
      parenDepth += 1;
      current += char;
      continue;
    }
    if (char === ")" && parenDepth > 0) {
      parenDepth -= 1;
      current += char;
      continue;
    }
    if (char === delimiter && parenDepth === 0) {
      parts.push(current.trim());
      current = "";
      continue;
    }

    current += char;
  }

  if (current.trim().length > 0) {
    parts.push(current.trim());
  }

  return parts;
}

function splitTopLevelWhitespace(value: string): string[] {
  const parts: string[] = [];
  let current = "";
  let parenDepth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inBracket = false;

  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];
    const next = value[index + 1];

    if (inSingleQuote) {
      current += char;
      if (char === "'" && next === "'") {
        current += next;
        index += 1;
      } else if (char === "'") {
        inSingleQuote = false;
      }
      continue;
    }
    if (inDoubleQuote) {
      current += char;
      if (char === '"') {
        inDoubleQuote = false;
      }
      continue;
    }
    if (inBacktick) {
      current += char;
      if (char === "`") {
        inBacktick = false;
      }
      continue;
    }
    if (inBracket) {
      current += char;
      if (char === "]") {
        inBracket = false;
      }
      continue;
    }

    if (char === "'") {
      inSingleQuote = true;
      current += char;
      continue;
    }
    if (char === '"') {
      inDoubleQuote = true;
      current += char;
      continue;
    }
    if (char === "`") {
      inBacktick = true;
      current += char;
      continue;
    }
    if (char === "[") {
      inBracket = true;
      current += char;
      continue;
    }
    if (char === "(") {
      parenDepth += 1;
      current += char;
      continue;
    }
    if (char === ")" && parenDepth > 0) {
      parenDepth -= 1;
      current += char;
      continue;
    }
    if (/\s/u.test(char) && parenDepth === 0) {
      if (current.trim().length > 0) {
        parts.push(current.trim());
        current = "";
      }
      continue;
    }

    current += char;
  }

  if (current.trim().length > 0) {
    parts.push(current.trim());
  }

  return parts;
}

function skipWhitespace(value: string, index: number): number {
  let nextIndex = index;
  while (nextIndex < value.length && /\s/u.test(value[nextIndex] ?? "")) {
    nextIndex += 1;
  }
  return nextIndex;
}

function matchesKeywordAt(value: string, index: number, keyword: string): boolean {
  return (
    value.slice(index, index + keyword.length).toLowerCase() === keyword &&
    !isIdentifierChar(value[index - 1]) &&
    !isIdentifierChar(value[index + keyword.length])
  );
}

function extractSelectClause(sql: string): string | null {
  const selectIndex = findTopLevelKeywordIndex(sql, "select");
  if (selectIndex < 0) {
    return null;
  }
  const fromIndex = findTopLevelKeywordIndex(sql, "from", selectIndex + 6);
  if (fromIndex < 0) {
    return null;
  }
  return sql.slice(selectIndex + 6, fromIndex).trim();
}

function extractFromClause(sql: string): string | null {
  const selectIndex = findTopLevelKeywordIndex(sql, "select");
  if (selectIndex < 0) {
    return null;
  }
  const fromIndex = findTopLevelKeywordIndex(sql, "from", selectIndex + 6);
  if (fromIndex < 0) {
    return null;
  }
  return sql
    .slice(fromIndex)
    .trim()
    .replace(/;+\s*$/u, "");
}

function extractTopLevelSourceSegments(
  fromClause: string
): Array<{ text: string; relationStart: number; relationEnd: number }> {
  const segments: Array<{ text: string; relationStart: number; relationEnd: number }> = [];
  let parenDepth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inBracket = false;
  let captureStart: number | null = null;

  const pushSegment = (endIndex: number) => {
    if (captureStart === null) {
      return;
    }
    let trimmedEnd = endIndex;
    while (trimmedEnd > captureStart && /\s/u.test(fromClause[trimmedEnd - 1] ?? "")) {
      trimmedEnd -= 1;
    }
    if (trimmedEnd > captureStart) {
      segments.push({
        text: fromClause.slice(captureStart, trimmedEnd),
        relationStart: captureStart,
        relationEnd: trimmedEnd,
      });
    }
  };

  for (let index = 0; index < fromClause.length; index += 1) {
    const char = fromClause[index];
    const next = fromClause[index + 1];

    if (inSingleQuote) {
      if (char === "'" && next === "'") {
        index += 1;
      } else if (char === "'") {
        inSingleQuote = false;
      }
      continue;
    }
    if (inDoubleQuote) {
      if (char === '"') {
        inDoubleQuote = false;
      }
      continue;
    }
    if (inBacktick) {
      if (char === "`") {
        inBacktick = false;
      }
      continue;
    }
    if (inBracket) {
      if (char === "]") {
        inBracket = false;
      }
      continue;
    }

    if (char === "'") {
      inSingleQuote = true;
      continue;
    }
    if (char === '"') {
      inDoubleQuote = true;
      continue;
    }
    if (char === "`") {
      inBacktick = true;
      continue;
    }
    if (char === "[") {
      inBracket = true;
      continue;
    }
    if (char === "(") {
      parenDepth += 1;
      continue;
    }
    if (char === ")" && parenDepth > 0) {
      parenDepth -= 1;
      continue;
    }
    if (parenDepth !== 0) {
      continue;
    }

    if (captureStart === null) {
      if (matchesKeywordAt(fromClause, index, "from")) {
        captureStart = skipWhitespace(fromClause, index + 4);
        index += 3;
        continue;
      }
      if (matchesKeywordAt(fromClause, index, "join")) {
        captureStart = skipWhitespace(fromClause, index + 4);
        index += 3;
        continue;
      }
      if (char === ",") {
        captureStart = skipWhitespace(fromClause, index + 1);
      }
      continue;
    }

    if (char === ",") {
      pushSegment(index);
      captureStart = skipWhitespace(fromClause, index + 1);
      continue;
    }

    const terminator =
      matchesKeywordAt(fromClause, index, "join")
        ? "join"
        : matchesKeywordAt(fromClause, index, "left")
          ? "left"
          : matchesKeywordAt(fromClause, index, "right")
            ? "right"
            : matchesKeywordAt(fromClause, index, "inner")
              ? "inner"
              : matchesKeywordAt(fromClause, index, "full")
                ? "full"
                : matchesKeywordAt(fromClause, index, "cross")
                  ? "cross"
                  : matchesKeywordAt(fromClause, index, "natural")
                    ? "natural"
                    : matchesKeywordAt(fromClause, index, "on")
                      ? "on"
                      : matchesKeywordAt(fromClause, index, "using")
                        ? "using"
                        : matchesKeywordAt(fromClause, index, "where")
                          ? "where"
                          : matchesKeywordAt(fromClause, index, "group")
                            ? "group"
                            : matchesKeywordAt(fromClause, index, "order")
                              ? "order"
                              : matchesKeywordAt(fromClause, index, "having")
                                ? "having"
                                : matchesKeywordAt(fromClause, index, "limit")
                                  ? "limit"
                                  : matchesKeywordAt(fromClause, index, "qualify")
                                    ? "qualify"
                                    : matchesKeywordAt(fromClause, index, "union")
                                      ? "union"
                                      : null;

    if (!terminator) {
      continue;
    }

    pushSegment(index);
    captureStart =
      terminator === "join"
        ? skipWhitespace(fromClause, index + terminator.length)
        : null;
    index += terminator.length - 1;
  }

  pushSegment(fromClause.length);
  return segments;
}

function isSupportedIdentifierToken(token: string): boolean {
  return (
    /^[A-Za-z_][\w$]*$/u.test(token) ||
    /^"[^"]+"$/u.test(token) ||
    /^`[^`]+`$/u.test(token) ||
    /^\[[^\]]+\]$/u.test(token)
  );
}

function parseSqlSourceSegment(
  segment: { text: string; relationStart: number; relationEnd: number }
): ParsedSqlSourceRef | null {
  const trimmedSegment = segment.text.trim();
  if (trimmedSegment.length === 0) {
    return null;
  }

  let relationText: string;
  let aliasTokens: string[];
  if (trimmedSegment.startsWith("{{")) {
    const closingIndex = trimmedSegment.indexOf("}}");
    if (closingIndex < 0) {
      return null;
    }
    relationText = trimmedSegment.slice(0, closingIndex + 2);
    aliasTokens = splitTopLevelWhitespace(trimmedSegment.slice(closingIndex + 2).trim());
  } else {
    const tokens = splitTopLevelWhitespace(trimmedSegment);
    if (tokens.length === 0) {
      return null;
    }
    relationText = tokens[0]!;
    aliasTokens = tokens.slice(1);
  }

  const alias =
    aliasTokens[0]?.toLowerCase() === "as"
      ? (aliasTokens[1] ? stripIdentifierQuotes(aliasTokens[1]) : null)
      : aliasTokens[0]
        ? stripIdentifierQuotes(aliasTokens[0])
        : null;

  const refMatch = relationText.match(
    /^\{\{\s*ref\(\s*(['"])([^'"]+)\1\s*,\s*(['"])([^'"]+)\3\s*\)\s*\}\}$/iu
  );
  if (refMatch) {
    return {
      locationName: refMatch[2] ?? "",
      nodeName: refMatch[4] ?? "",
      alias,
      nodeID: null,
      sourceStyle: "coalesce_ref",
      locationCandidates: refMatch[2] ? [refMatch[2]] : [],
      relationStart: segment.relationStart,
      relationEnd: segment.relationStart + relationText.length,
    };
  }

  if (relationText.startsWith("(")) {
    return null;
  }

  const parts = splitTopLevel(relationText, ".").map((part) => part.trim());
  if (
    parts.length === 0 ||
    parts.some((part) => part.length === 0 || !isSupportedIdentifierToken(part))
  ) {
    return null;
  }

  const normalizedParts = parts.map(stripIdentifierQuotes);
  const nodeName = normalizedParts[normalizedParts.length - 1] ?? "";

  return {
    locationName: "",
    nodeName,
    alias,
    nodeID: null,
    sourceStyle: "table_name",
    locationCandidates: normalizedParts.slice(0, -1).reverse(),
    relationStart: segment.relationStart,
    relationEnd: segment.relationStart + relationText.length,
  };
}

function parseSqlSourceRefs(sql: string): ParsedSqlSourceRef[] {
  const fromClause = extractFromClause(sql);
  if (!fromClause) {
    return [];
  }

  return extractTopLevelSourceSegments(fromClause)
    .map(parseSqlSourceSegment)
    .filter((ref): ref is ParsedSqlSourceRef => ref !== null);
}

function splitExpressionAlias(rawItem: string): { expression: string; outputName: string | null } {
  const asMatch = rawItem.match(
    /^(.*?)(?:\s+AS\s+)([A-Za-z_][\w$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])$/i
  );
  if (asMatch) {
    return {
      expression: asMatch[1]?.trim() ?? rawItem.trim(),
      outputName: stripIdentifierQuotes(asMatch[2] ?? ""),
    };
  }

  const bareAliasMatch = rawItem.match(
    /^(.*?)(?:\s+)([A-Za-z_][\w$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])$/
  );
  if (bareAliasMatch) {
    const candidateExpression = bareAliasMatch[1]?.trim() ?? rawItem.trim();
    if (candidateExpression.includes(".") || candidateExpression.includes("(")) {
      return {
        expression: candidateExpression,
        outputName: stripIdentifierQuotes(bareAliasMatch[2] ?? ""),
      };
    }
  }

  return {
    expression: rawItem.trim(),
    outputName: null,
  };
}

function parseDirectColumnExpression(expression: string): {
  sourceNodeAlias: string | null;
  sourceColumnName: string;
} | null {
  const trimmed = expression.trim();
  if (trimmed === "*") {
    return null;
  }

  const parts = splitTopLevel(trimmed, ".").map((part) => part.trim());
  if (
    parts.length === 0 ||
    parts.some((part) => part.length === 0 || !isSupportedIdentifierToken(part))
  ) {
    return null;
  }

  return {
    sourceNodeAlias:
      parts.length >= 2 ? stripIdentifierQuotes(parts[parts.length - 2] ?? "") : null,
    sourceColumnName: stripIdentifierQuotes(parts[parts.length - 1] ?? ""),
  };
}

function parseWildcardExpression(expression: string): {
  sourceNodeAlias: string | null;
} | null {
  const trimmed = expression.trim();
  if (trimmed === "*") {
    return { sourceNodeAlias: null };
  }
  const parts = splitTopLevel(trimmed, ".").map((part) => part.trim());
  if (
    parts.length < 2 ||
    parts[parts.length - 1] !== "*" ||
    parts.slice(0, -1).some((part) => part.length === 0 || !isSupportedIdentifierToken(part))
  ) {
    return null;
  }
  return {
    sourceNodeAlias: stripIdentifierQuotes(parts[parts.length - 2] ?? ""),
  };
}

function listToQuestion(values: string[]): string {
  return values.join(", ");
}

function parseSqlSelectItems(sql: string, refs: ParsedSqlSourceRef[]): SqlParseResult {
  const warnings: string[] = [];
  const refsByAlias = new Map<string, ParsedSqlSourceRef>();
  for (const ref of refs) {
    refsByAlias.set(normalizeSqlIdentifier(ref.alias ?? ref.nodeName), ref);
  }

  const selectClause = extractSelectClause(sql);
  if (!selectClause) {
    return {
      refs,
      selectItems: [],
      warnings: ["Could not find a top-level SELECT ... FROM clause in the SQL."],
    };
  }

  const rawItems = splitTopLevel(selectClause, ",");
  const selectItems: PlannedSelectItem[] = [];

  for (const rawItem of rawItems) {
    const { expression, outputName } = splitExpressionAlias(rawItem);
    const wildcard = parseWildcardExpression(expression);
    if (wildcard) {
      if (wildcard.sourceNodeAlias === null && refs.length !== 1) {
        selectItems.push({
          expression,
          outputName: null,
          sourceNodeAlias: null,
          sourceNodeName: null,
          sourceNodeID: null,
          sourceColumnName: null,
          kind: "expression",
          supported: false,
          reason: "Unqualified * is only supported when exactly one predecessor ref is present.",
        });
        continue;
      }

      const ref =
        wildcard.sourceNodeAlias === null
          ? refs[0] ?? null
          : refsByAlias.get(normalizeSqlIdentifier(wildcard.sourceNodeAlias)) ?? null;
      if (!ref) {
        selectItems.push({
          expression,
          outputName: null,
          sourceNodeAlias: wildcard.sourceNodeAlias,
          sourceNodeName: null,
          sourceNodeID: null,
          sourceColumnName: null,
          kind: "expression",
          supported: false,
          reason: "Wildcard source alias could not be resolved to a predecessor ref.",
        });
        continue;
      }

      // Wildcards are expanded later after predecessor nodes are fetched.
      selectItems.push({
        expression,
        outputName: null,
        sourceNodeAlias: wildcard.sourceNodeAlias ?? ref.alias ?? ref.nodeName,
        sourceNodeName: ref.nodeName,
        sourceNodeID: ref.nodeID,
        sourceColumnName: "*",
        kind: "expression",
        supported: true,
      });
      continue;
    }

    const directColumn = parseDirectColumnExpression(expression);
    if (!directColumn) {
      // Expression is not a direct column reference - it's a computed expression
      // Support it if it has an output name (alias)
      if (outputName === null) {
        selectItems.push({
          expression,
          outputName: null,
          sourceNodeAlias: null,
          sourceNodeName: null,
          sourceNodeID: null,
          sourceColumnName: null,
          kind: "expression",
          supported: false,
          reason: "Computed expressions require an alias (e.g., CASE ... END AS column_name)",
        });
        continue;
      }

      // Computed expression with alias - supported
      selectItems.push({
        expression,
        outputName,
        sourceNodeAlias: null,
        sourceNodeName: null,
        sourceNodeID: null,
        sourceColumnName: null,
        kind: "expression",
        supported: true,
      });
      continue;
    }

    const ref =
      directColumn.sourceNodeAlias === null
        ? refs.length === 1
          ? refs[0] ?? null
          : null
        : refsByAlias.get(normalizeSqlIdentifier(directColumn.sourceNodeAlias)) ?? null;
    if (!ref) {
      selectItems.push({
        expression,
        outputName: outputName ?? directColumn.sourceColumnName,
        sourceNodeAlias: directColumn.sourceNodeAlias,
        sourceNodeName: null,
        sourceNodeID: null,
        sourceColumnName: directColumn.sourceColumnName,
        kind: "column",
        supported: false,
        reason:
          directColumn.sourceNodeAlias === null
            ? "Unqualified columns are only supported when exactly one predecessor ref is present."
            : `The source alias ${directColumn.sourceNodeAlias} did not match a predecessor ref.`,
      });
      continue;
    }

    selectItems.push({
      expression,
      outputName: outputName ?? directColumn.sourceColumnName,
      sourceNodeAlias: directColumn.sourceNodeAlias ?? ref.alias ?? ref.nodeName,
      sourceNodeName: ref.nodeName,
      sourceNodeID: ref.nodeID,
      sourceColumnName: directColumn.sourceColumnName,
      kind: "column",
      supported: true,
    });
  }

  if (selectItems.length === 0) {
    warnings.push("The SQL SELECT clause did not produce any supported projected columns.");
  }

  return { refs, selectItems, warnings };
}

async function listAllWorkspaceNodes(
  client: CoalesceClient,
  workspaceID: string
): Promise<WorkspaceNodeIndexEntry[]> {
  const nodes: WorkspaceNodeIndexEntry[] = [];
  const seenCursors = new Set<string>();
  let next: string | undefined;
  let isFirstPage = true;

  while (isFirstPage || next) {
    const response = await listWorkspaceNodes(client, {
      workspaceID,
      limit: WORKSPACE_NODE_PAGE_LIMIT,
      orderBy: "id",
      ...(next ? { startingFrom: next } : {}),
    });

    if (!isPlainObject(response)) {
      throw new Error("Workspace node list response was not an object");
    }

    if (Array.isArray(response.data)) {
      for (const item of response.data) {
        if (!isPlainObject(item) || typeof item.id !== "string" || typeof item.name !== "string") {
          continue;
        }
        nodes.push({
          id: item.id,
          name: item.name,
          nodeType: typeof item.nodeType === "string" ? item.nodeType : null,
          locationName:
            typeof item.locationName === "string" ? item.locationName : null,
        });
      }
    }

    const responseNext =
      typeof response.next === "string" && response.next.trim().length > 0
        ? response.next
        : undefined;
    if (responseNext) {
      if (seenCursors.has(responseNext)) {
        throw new Error(`Workspace node pagination repeated cursor ${responseNext}`);
      }
      seenCursors.add(responseNext);
    }

    next = responseNext;
    isFirstPage = false;
  }

  return nodes;
}

function getNodeLocationName(node: Record<string, unknown>): string | null {
  if (typeof node.locationName === "string" && node.locationName.trim().length > 0) {
    return node.locationName;
  }
  return null;
}

async function resolveSqlRefsToWorkspaceNodes(
  client: CoalesceClient,
  workspaceID: string,
  refs: ParsedSqlSourceRef[]
): Promise<{
  refs: ParsedSqlSourceRef[];
  openQuestions: string[];
  warnings: string[];
  predecessorNodes: Record<string, Record<string, unknown>>;
}> {
  const warnings: string[] = [];
  const openQuestions: string[] = [];
  const predecessorNodes: Record<string, Record<string, unknown>> = {};

  if (refs.length === 0) {
    openQuestions.push(
      "Which upstream Coalesce node(s) should this pipeline build from? Use a top-level FROM/JOIN that names existing workspace nodes (raw table names or {{ ref('LOCATION', 'NODE') }} syntax), or provide sourceNodeIDs."
    );
    return { refs, openQuestions, warnings, predecessorNodes };
  }

  const workspaceNodes = await listAllWorkspaceNodes(client, workspaceID);
  const nodesByNormalizedName = new Map<string, WorkspaceNodeIndexEntry[]>();
  for (const node of workspaceNodes) {
    const normalized = normalizeSqlIdentifier(node.name);
    const existing = nodesByNormalizedName.get(normalized) ?? [];
    existing.push(node);
    nodesByNormalizedName.set(normalized, existing);
  }

  for (const ref of refs) {
    const matches =
      nodesByNormalizedName.get(normalizeSqlIdentifier(ref.nodeName)) ?? [];
    if (matches.length === 0) {
      openQuestions.push(
        `Could not resolve the SQL source ${ref.nodeName} to a workspace node ID in workspace ${workspaceID}.`
      );
      continue;
    }

    const locationHints = [
      ...(ref.locationName ? [ref.locationName] : []),
      ...ref.locationCandidates,
    ].map(normalizeSqlIdentifier);
    const hintedMatches =
      locationHints.length > 0
        ? matches.filter(
            (entry) =>
              entry.locationName &&
              locationHints.includes(normalizeSqlIdentifier(entry.locationName))
          )
        : [];

    if (hintedMatches.length === 1) {
      ref.nodeID = hintedMatches[0]?.id ?? null;
      if (!ref.locationName && hintedMatches[0]?.locationName) {
        ref.locationName = hintedMatches[0].locationName;
      }
      continue;
    }
    if (hintedMatches.length > 1) {
      openQuestions.push(
        `Multiple workspace nodes matched the SQL source ${ref.nodeName}. Resolve the exact node before creation.`
      );
      continue;
    }

    if (matches.length === 1) {
      ref.nodeID = matches[0]?.id ?? null;
      if (!ref.locationName && matches[0]?.locationName) {
        ref.locationName = matches[0].locationName;
      }
      continue;
    }

    if (matches.length > 1) {
      const detailedMatches = await Promise.all(
        matches.map(async (match) => {
          const node = await getWorkspaceNode(client, {
            workspaceID,
            nodeID: match.id,
          });
          return {
            match,
            node: isPlainObject(node) ? node : null,
          };
        })
      );
      const exactLocationMatches =
        locationHints.length > 0
          ? detailedMatches.filter(
              (candidate) =>
                candidate.node &&
                getNodeLocationName(candidate.node) &&
                locationHints.includes(
                  normalizeSqlIdentifier(getNodeLocationName(candidate.node) ?? "")
                )
            )
          : [];
      if (exactLocationMatches.length === 1) {
        ref.nodeID = exactLocationMatches[0]?.match.id ?? null;
        if (!ref.locationName) {
          ref.locationName = getNodeLocationName(exactLocationMatches[0]?.node ?? {}) ?? "";
        }
        continue;
      }
      if (exactLocationMatches.length > 1) {
        openQuestions.push(
          `Multiple workspace nodes matched the SQL source ${ref.nodeName}. Resolve the exact node before creation.`
        );
        continue;
      }

      if (ref.sourceStyle === "coalesce_ref" && ref.locationName) {
        openQuestions.push(
          `Workspace nodes named ${ref.nodeName} were found, but none matched the requested location ${ref.locationName}.`
        );
        continue;
      }

      openQuestions.push(
        `Multiple workspace nodes named ${ref.nodeName} were found. Qualify the SQL source more clearly or provide sourceNodeIDs before creation.`
      );
      continue;
    }
  }

  for (const ref of refs) {
    if (!ref.nodeID) {
      continue;
    }
    const predecessor = await getWorkspaceNode(client, {
      workspaceID,
      nodeID: ref.nodeID,
    });
    if (!isPlainObject(predecessor)) {
      warnings.push(`Resolved predecessor ${ref.nodeName} did not return an object body.`);
      continue;
    }
    const predecessorLocationName = getNodeLocationName(predecessor);
    if (
      ref.sourceStyle === "coalesce_ref" &&
      predecessorLocationName &&
      normalizeSqlIdentifier(predecessorLocationName) !==
        normalizeSqlIdentifier(ref.locationName)
    ) {
      ref.nodeID = null;
      openQuestions.push(
        `Resolved node ${ref.nodeName} is in location ${predecessorLocationName}, not the requested location ${ref.locationName}.`
      );
      continue;
    }
    if (!ref.locationName && predecessorLocationName) {
      ref.locationName = predecessorLocationName;
    }
    predecessorNodes[ref.nodeID] = predecessor;
  }

  return { refs, openQuestions, warnings, predecessorNodes };
}

function buildJoinConditionFromSql(
  sql: string,
  refs: ParsedSqlSourceRef[]
): string | null {
  const fromClause = extractFromClause(sql);
  if (!fromClause) {
    return null;
  }

  let joinCondition = fromClause;
  for (const ref of [...refs]
    .filter((candidate) => candidate.sourceStyle === "table_name" && candidate.locationName)
    .sort((left, right) => right.relationStart - left.relationStart)) {
    const replacement = `{{ ref('${ref.locationName}', '${ref.nodeName}') }}`;
    joinCondition =
      joinCondition.slice(0, ref.relationStart) +
      replacement +
      joinCondition.slice(ref.relationEnd);
  }

  return joinCondition;
}

export function getColumnNamesFromNode(node: Record<string, unknown>): string[] {
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

function buildSelectItemsFromSourceNode(
  sourceNodeID: string,
  sourceNodeName: string,
  node: Record<string, unknown>
): PlannedSelectItem[] {
  return getColumnNamesFromNode(node).map((columnName) => ({
    expression: `${sourceNodeName}.${columnName}`,
    outputName: columnName,
    sourceNodeAlias: sourceNodeName,
    sourceNodeName,
    sourceNodeID,
    sourceColumnName: columnName,
    kind: "column",
    supported: true,
  }));
}

async function getSourceNodesByID(
  client: CoalesceClient,
  workspaceID: string,
  sourceNodeIDs: string[]
): Promise<{
  sourceRefs: PlannedSourceRef[];
  predecessorNodes: Record<string, Record<string, unknown>>;
  openQuestions: string[];
  warnings: string[];
}> {
  const sourceRefs: PlannedSourceRef[] = [];
  const predecessorNodes: Record<string, Record<string, unknown>> = {};
  const openQuestions: string[] = [];
  const warnings: string[] = [];

  for (const sourceNodeID of sourceNodeIDs) {
    const node = await getWorkspaceNode(client, {
      workspaceID,
      nodeID: sourceNodeID,
    });
    if (!isPlainObject(node)) {
      openQuestions.push(
        `Could not read source node ${sourceNodeID} in workspace ${workspaceID}.`
      );
      continue;
    }
    if (typeof node.name !== "string" || node.name.trim().length === 0) {
      openQuestions.push(`Source node ${sourceNodeID} does not have a usable name.`);
      continue;
    }
    const locationName = getNodeLocationName(node);
    if (!locationName) {
      openQuestions.push(
        `Source node ${node.name} does not expose locationName. Clarify the Coalesce location before generating ref() SQL for this pipeline.`
      );
    }

    predecessorNodes[sourceNodeID] = node;
    sourceRefs.push({
      locationName: locationName ?? "UNKNOWN_LOCATION",
      nodeName: node.name,
      alias: node.name,
      nodeID: sourceNodeID,
    });
  }

  return {
    sourceRefs,
    predecessorNodes,
    openQuestions,
    warnings,
  };
}

function expandWildcardSelectItems(
  selectItems: PlannedSelectItem[],
  refs: ResolvedSqlRef[],
  predecessorNodes: Record<string, Record<string, unknown>>
): PlannedSelectItem[] {
  const expanded: PlannedSelectItem[] = [];

  for (const item of selectItems) {
    if (item.sourceColumnName !== "*" || !item.supported) {
      expanded.push(item);
      continue;
    }

    const ref =
      item.sourceNodeID
        ? refs.find((candidate) => candidate.nodeID === item.sourceNodeID) ?? null
        : refs.find(
            (candidate) =>
              normalizeSqlIdentifier(candidate.alias ?? candidate.nodeName) ===
              normalizeSqlIdentifier(item.sourceNodeAlias ?? "")
          ) ?? null;
    if (!ref?.nodeID) {
      expanded.push({
        ...item,
        supported: false,
        reason: "Wildcard source could not be resolved to a concrete predecessor node.",
      });
      continue;
    }

    const predecessor = predecessorNodes[ref.nodeID];
    if (!predecessor) {
      expanded.push({
        ...item,
        supported: false,
        reason: "Wildcard source predecessor body was not available for column expansion.",
      });
      continue;
    }

    const columnNames = getColumnNamesFromNode(predecessor);
    if (columnNames.length === 0) {
      expanded.push({
        ...item,
        supported: false,
        reason: "Wildcard source predecessor has no columns to expand.",
      });
      continue;
    }

    for (const columnName of columnNames) {
      expanded.push({
        expression:
          item.sourceNodeAlias && item.sourceNodeAlias.length > 0
            ? `${item.sourceNodeAlias}.${columnName}`
            : columnName,
        outputName: columnName,
        sourceNodeAlias: item.sourceNodeAlias,
        sourceNodeName: item.sourceNodeName,
        sourceNodeID: ref.nodeID,
        sourceColumnName: columnName,
        kind: "column",
        supported: true,
      });
    }
  }

  return expanded;
}

function buildDefaultNodePrefix(
  nodeTypeFamily: PipelineNodeTypeFamily | null | undefined,
  shortName: string | null | undefined
): string {
  if (shortName && shortName.trim().length > 0) {
    return shortName.trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_");
  }

  switch (nodeTypeFamily) {
    case "stage":
      return "STG";
    case "persistent-stage":
      return "PSTG";
    case "view":
      return "VW";
    case "work":
      return "WRK";
    case "dimension":
      return "DIM";
    case "fact":
      return "FACT";
    case "hub":
      return "HUB";
    case "satellite":
      return "SAT";
    case "link":
      return "LNK";
    default:
      return "NODE";
  }
}

function buildDefaultNodeName(
  targetName: string | undefined,
  refs: Array<ResolvedSqlRef | PlannedSourceRef>,
  nodeTypeFamily?: PipelineNodeTypeFamily | null,
  shortName?: string | null
): string {
  if (targetName && targetName.trim().length > 0) {
    return targetName.trim();
  }

  const prefix = buildDefaultNodePrefix(nodeTypeFamily, shortName);
  const firstRef = refs[0];
  if (!firstRef) {
    return `${prefix}_NEW_PIPELINE`;
  }

  const stripped = firstRef.nodeName.replace(
    /^(SRC[_-]?|STG[_-]?|DIM[_-]?|FACT[_-]?|FCT[_-]?|INT[_-]?|WORK[_-]?|VW[_-]?)/i,
    ""
  );
  return `${prefix}_${stripped}`.toUpperCase().replace(/__+/g, "_");
}

function matchesObservedNodeType(
  requestedNodeType: string,
  observedNodeTypes: string[]
): boolean {
  const requestedID = requestedNodeType.includes(":::")
    ? requestedNodeType.split(":::")[1] ?? requestedNodeType
    : requestedNodeType;

  return observedNodeTypes.some((observed) => {
    if (observed === requestedNodeType) {
      return true;
    }
    const observedID = observed.includes(":::") ? observed.split(":::")[1] ?? observed : observed;
    return observedID === requestedID;
  });
}

async function getWorkspaceNodeTypeInventory(
  client: CoalesceClient,
  workspaceID: string
): Promise<WorkspaceNodeTypeInventory> {
  try {
    const result = await listWorkspaceNodeTypes(client, { workspaceID });
    return {
      nodeTypes: result.nodeTypes ?? [],
      counts: result.counts ?? {},
      total: result.total ?? 0,
      warnings: [],
    };
  } catch {
    return {
      nodeTypes: [],
      counts: {},
      total: 0,
      warnings: [
        `Observed workspace node types could not be fetched for workspace ${workspaceID}. ` +
          `Use list-workspace-node-types or cache-workspace-nodes to inspect current workspace usage and confirm installation before execution.`,
      ],
    };
  }
}

function applyWorkspaceNodeTypeValidation(
  plan: PipelinePlan,
  inventory: WorkspaceNodeTypeInventory,
  requestedNodeType?: string
): void {
  plan.warnings.push(...inventory.warnings);

  if (inventory.total === 0) {
    return;
  }

  const recommendedTypes: string[] = (plan.nodes ?? [])
    .map((node) => node.nodeType)
    .filter((nodeType) => typeof nodeType === "string" && nodeType.length > 0);

  if (requestedNodeType && requestedNodeType.trim().length > 0) {
    recommendedTypes.push(requestedNodeType);
  }

  const missingTypes = Array.from(new Set(recommendedTypes)).filter(
    (nodeType) => !matchesObservedNodeType(nodeType, inventory.nodeTypes)
  );

  if (missingTypes.length > 0) {
    plan.warnings.push(
      `The following node types were not observed in current workspace nodes: ${missingTypes.join(
        ", "
      )}. This observation is based on existing nodes, not a true installed-type registry. Confirm installation in Coalesce before creating nodes of these types.`
    );
    plan.status = "needs_clarification";
  }
}

function buildPlanFromSql(
  params: {
    workspaceID: string;
    goal?: string;
    sql: string;
    targetName?: string;
    description?: string;
    targetNodeType?: string;
    configOverrides?: Record<string, unknown>;
    nodeTypeSelection: PipelineNodeTypeSelection;
    selectedNodeType?: {
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
    location?: {
      locationName?: string;
      database?: string;
      schema?: string;
    };
  },
  parseResult: SqlParseResult,
  predecessorNodes: Record<string, Record<string, unknown>>,
  openQuestions: string[],
  warnings: string[]
): PipelinePlan {
  const nodeType =
    params.selectedNodeType?.nodeType ?? params.targetNodeType ?? "Stage";
  const planOpenQuestions = [...openQuestions];
  if (!params.selectedNodeType) {
    warnings.push(
      `No ranked node type candidate was available, so planning fell back to ${nodeType}.`
    );
  } else if (!params.selectedNodeType.autoExecutable) {
    warnings.push(
      `Planner selected node type ${nodeType}, but it likely needs additional semantic configuration before automatic creation.`
    );
    if (params.selectedNodeType.semanticSignals.length > 0) {
      planOpenQuestions.push(
        `Confirm the required configuration for ${nodeType}: ${params.selectedNodeType.semanticSignals.join(
          ", "
        )}.`
      );
    }
    if (params.selectedNodeType.missingDefaultFields.length > 0) {
      planOpenQuestions.push(
        `Provide values for ${nodeType} config fields without defaults: ${params.selectedNodeType.missingDefaultFields.join(
          ", "
        )}.`
      );
    }
  }

  const expandedSelectItems = expandWildcardSelectItems(
    parseResult.selectItems,
    parseResult.refs,
    predecessorNodes
  );
  const unsupportedItems = expandedSelectItems.filter((item) => !item.supported);
  if (unsupportedItems.length > 0) {
    for (const item of unsupportedItems) {
      warnings.push(
        item.reason
          ? `${item.expression}: ${item.reason}`
          : `${item.expression}: unsupported SQL projection in v1`
      );
    }
  }

  const supportedOutputColumnCount = expandedSelectItems.filter(
    (item) => item.supported && item.outputName
  ).length;
  if (
    parseResult.warnings.some((warning) =>
      warning.includes("Could not find a top-level SELECT ... FROM clause")
    )
  ) {
    planOpenQuestions.push(
      "Provide a top-level SELECT ... FROM query using direct column projections before creating this pipeline."
    );
  } else if (supportedOutputColumnCount === 0) {
    planOpenQuestions.push(
      "Specify at least one supported projected column before creating this pipeline."
    );
  }

  const predecessorNodeIDs = uniqueInOrder(parseResult.refs.flatMap((ref) =>
    ref.nodeID ? [ref.nodeID] : []
  ));
  const predecessorNodeNames = parseResult.refs.map((ref) => ref.nodeName);

  const ready =
    (params.selectedNodeType?.autoExecutable ?? true) &&
    predecessorNodeIDs.length > 0 &&
    supportedOutputColumnCount > 0 &&
    unsupportedItems.length === 0 &&
    parseResult.warnings.length === 0 &&
    planOpenQuestions.length === 0;

  const name = buildDefaultNodeName(
    params.targetName,
    parseResult.refs,
    params.selectedNodeType?.family ?? null,
    params.selectedNodeType?.shortName ?? null
  );
  const plan: PipelinePlan = {
    version: 1,
    intent: "sql",
    status: ready ? "ready" : "needs_clarification",
    workspaceID: params.workspaceID,
    platform: null,
    goal: params.goal ?? null,
    sql: params.sql,
    nodes: [
      {
        planNodeID: "node-1",
        name,
        nodeType,
        nodeTypeFamily: params.selectedNodeType?.family ?? null,
        predecessorNodeIDs,
        predecessorPlanNodeIDs: [],
        predecessorNodeNames,
        description: params.description ?? null,
        sql: params.sql,
        selectItems: expandedSelectItems,
        outputColumnNames: expandedSelectItems.flatMap((item) =>
          item.outputName ? [item.outputName] : []
        ),
        configOverrides: params.configOverrides ? deepClone(params.configOverrides) : {},
        sourceRefs: parseResult.refs.map((ref) => ({
          locationName: ref.locationName,
          nodeName: ref.nodeName,
          alias: ref.alias,
          nodeID: ref.nodeID,
        })),
        joinCondition: buildJoinConditionFromSql(params.sql, parseResult.refs),
        location: params.location ?? {},
        requiresFullSetNode: true,
        ...(params.selectedNodeType?.templateDefaults
          ? { templateDefaults: params.selectedNodeType.templateDefaults }
          : {}),
      },
    ],
    assumptions: [
      `Planner ${params.nodeTypeSelection.strategy} selected ${nodeType} from repo/workspace candidates.`,
      "The generated plan uses create-workspace-node-from-predecessor followed by set-workspace-node when the selected type is projection-capable.",
    ],
    openQuestions: planOpenQuestions,
    warnings: [...parseResult.warnings, ...warnings],
    supportedNodeTypes:
      params.nodeTypeSelection.supportedNodeTypes.length > 0
        ? params.nodeTypeSelection.supportedNodeTypes
        : [nodeType],
    nodeTypeSelection: params.nodeTypeSelection,
  };

  return plan;
}

/**
 * Parsed CTE with name and body SQL.
 */
type ParsedCte = {
  name: string;
  body: string;
  columns: CteColumn[];
  whereClause: string | null;
  sourceTable: string | null;
  hasGroupBy: boolean;
  hasJoin: boolean;
};

type CteColumn = {
  outputName: string;
  expression: string;
  isTransform: boolean;
};

/**
 * Extract CTEs with their bodies from SQL.
 * Handles nested parentheses to find each CTE body.
 */
function extractCtes(sql: string): ParsedCte[] {
  const trimmed = sql.trim();
  if (!/^with\b/i.test(trimmed)) {
    return [];
  }

  const ctes: ParsedCte[] = [];
  // Match CTE header: WITH name AS ( or , name AS (
  const pattern = /(?:^with|,)\s+([A-Za-z_][\w$]*|"[^"]+"|`[^`]+`)\s+AS\s*\(/gi;
  const matches = [...trimmed.matchAll(pattern)];

  for (let i = 0; i < matches.length; i++) {
    const match = matches[i]!;
    const rawName = match[1] ? stripIdentifierQuotes(match[1]) : null;
    if (!rawName) continue;

    const name = rawName.toUpperCase();
    // Find the CTE body: start after the opening '(' of "AS ("
    const bodyStart = match.index! + match[0].length;
    const body = extractParenBody(trimmed, bodyStart);

    if (body) {
      const columns = parseCteColumns(body);
      const whereClause = extractCteWhereClause(body);
      const sourceTable = extractCteSourceTable(body);
      const hasGroupBy = /\bGROUP\s+BY\b/i.test(body);
      const hasJoin = /\bJOIN\b/i.test(body);
      ctes.push({ name, body, columns, whereClause, sourceTable, hasGroupBy, hasJoin });
    } else {
      ctes.push({ name, body: "", columns: [], whereClause: null, sourceTable: null, hasGroupBy: false, hasJoin: false });
    }
  }

  return ctes;
}

/**
 * Extract the body between balanced parentheses.
 * `startIndex` should be the position right after the opening '('.
 */
function extractParenBody(sql: string, startIndex: number): string | null {
  let depth = 1;
  let i = startIndex;
  while (i < sql.length && depth > 0) {
    const ch = sql[i];
    if (ch === "'") {
      // Skip string literal
      i++;
      while (i < sql.length && sql[i] !== "'") {
        if (sql[i] === "'" && i + 1 < sql.length && sql[i + 1] === "'") i++; // escaped quote
        i++;
      }
    } else if (ch === "(") {
      depth++;
    } else if (ch === ")") {
      depth--;
      if (depth === 0) {
        return sql.slice(startIndex, i).trim();
      }
    }
    i++;
  }
  return null;
}

/**
 * Parse a CTE body's SELECT list into columns with transform detection.
 *
 * Handles `SELECT * FROM (subquery) WHERE ...` by recursing into the subquery.
 */
function parseCteColumns(body: string): CteColumn[] {
  const selectClause = extractSelectClause(body);
  if (!selectClause) return [];

  const rawItems = splitTopLevel(selectClause, ",");

  // Detect "SELECT * FROM (subquery)" — recurse into the subquery
  if (rawItems.length === 1 && /^\*$/.test(rawItems[0]!.trim())) {
    const subqueryBody = extractSubqueryFromFrom(body);
    if (subqueryBody) {
      return parseCteColumns(subqueryBody);
    }
    return [];
  }

  const columns: CteColumn[] = [];

  for (const rawItem of rawItems) {
    const { expression, outputName } = splitExpressionAlias(rawItem);
    const trimmedExpr = expression.trim();

    // Skip wildcards
    if (/^\*$/.test(trimmedExpr) || /\.\*$/.test(trimmedExpr)) continue;

    const bareColName = extractBareColumnName(trimmedExpr)?.toUpperCase() ?? null;
    const colName = (outputName?.toUpperCase() ?? bareColName);
    if (!colName) continue;

    // Detect transforms: anything that isn't a simple column reference,
    // OR a column rename (AS alias differs from the source column name).
    // Renames need a transform so preserveColumnLinkage can match by the NEW name
    // and propagate the expression into sources[*].transform.
    const isRename = outputName !== null && bareColName !== null && outputName.toUpperCase() !== bareColName;
    const isTransform = !isSimpleColumnRef(trimmedExpr) || isRename;

    columns.push({
      outputName: colName,
      expression: trimmedExpr,
      isTransform,
    });
  }

  return columns;
}

/**
 * Extract the subquery body from `FROM (subquery)`.
 * Returns the SQL inside the parentheses, or null if FROM doesn't start with a subquery.
 */
function extractSubqueryFromFrom(sql: string): string | null {
  const fromIndex = findTopLevelKeywordIndex(sql, "from");
  if (fromIndex < 0) return null;
  const afterFrom = sql.slice(fromIndex + 4).trimStart();
  if (!afterFrom.startsWith("(")) return null;
  return extractParenBody(afterFrom, 1);
}

/**
 * Check if an expression is a simple column reference (no transform needed).
 * Simple: `col`, `"col"`, `table.col`, `table."col"`, `"table"."col"`
 */
function isSimpleColumnRef(expr: string): boolean {
  // Simple: identifier or qualified identifier (with optional quotes)
  return /^(?:[A-Za-z_][\w$]*|"[^"]+")(?:\.(?:[A-Za-z_][\w$]*|"[^"]+"))?$/.test(expr.trim());
}

/**
 * Extract a bare column name from a simple reference like `table.col` or `col`.
 */
function extractBareColumnName(expr: string): string | null {
  const match = expr.trim().match(/(?:.*\.)?([A-Za-z_][\w$]*|"[^"]+")$/);
  if (!match?.[1]) return null;
  return stripIdentifierQuotes(match[1]);
}

/**
 * Extract WHERE clause from a CTE body (ignoring subqueries).
 */
function extractCteWhereClause(body: string): string | null {
  // Find WHERE that's not inside parentheses
  const upperBody = body.toUpperCase();
  let depth = 0;
  for (let i = 0; i < body.length; i++) {
    if (body[i] === "(") depth++;
    else if (body[i] === ")") depth--;
    else if (depth === 0 && upperBody.startsWith("WHERE", i)) {
      // Check it's a word boundary
      const before = i > 0 ? body[i - 1] : " ";
      const after = i + 5 < body.length ? body[i + 5] : " ";
      if (/\s/.test(before) && /\s/.test(after)) {
        // Extract until GROUP BY, ORDER BY, HAVING, LIMIT, QUALIFY, or end
        const rest = body.slice(i + 5);
        const endMatch = rest.search(/\b(?:GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|QUALIFY)\b/i);
        const clause = endMatch >= 0 ? rest.slice(0, endMatch).trim() : rest.trim();
        return clause || null;
      }
    }
  }
  return null;
}

const AGGREGATE_FUNCTIONS = new Set([
  "COUNT", "SUM", "AVG", "MIN", "MAX",
  "LISTAGG", "ARRAY_AGG", "MEDIAN", "MODE",
  "STDDEV", "VARIANCE", "ANY_VALUE",
  "COUNT_IF", "SUM_IF", "AVG_IF",
  "APPROX_COUNT_DISTINCT", "HLL",
]);

function isAggregateFn(name: string): boolean {
  return AGGREGATE_FUNCTIONS.has(name.toUpperCase());
}

/**
 * Extract the main source table from a CTE body's FROM clause.
 */
function extractCteSourceTable(body: string): string | null {
  const match = body.match(/\bFROM\s+([A-Za-z_][\w$.]*(?:\.[A-Za-z_][\w$]*)*)/i);
  return match?.[1]?.toUpperCase() ?? null;
}

/**
 * Classify a CTE's pattern to pick the right node type.
 */
function classifyCtePattern(cte: ParsedCte): "staging" | "multiSource" | "aggregation" {
  if (cte.hasGroupBy) return "aggregation";
  if (cte.hasJoin) return "multiSource";
  return "staging";
}

/**
 * Build a per-CTE instruction block that tells the agent exactly what transforms
 * and filters to apply for this CTE.
 */
function buildCteNodeInstruction(cte: ParsedCte, nodeType: string): string {
  const lines: string[] = [];
  lines.push(`## ${cte.name}`);
  lines.push(`- nodeType: "${nodeType}"`);

  if (cte.sourceTable) {
    lines.push(`- source: ${cte.sourceTable}`);
  }

  const transforms = cte.columns.filter((c) => c.isTransform);
  const passthroughCols = cte.columns.filter((c) => !c.isTransform);

  if (cte.hasGroupBy) {
    lines.push(`- AGGREGATION NODE: pass groupByColumns + aggregates directly to create-workspace-node-from-predecessor (single call)`);
  } else if (cte.columns.length > 0) {
    lines.push(`- Pass columns array + whereCondition directly to create-workspace-node-from-predecessor (single call)`);
  }

  if (transforms.length > 0) {
    lines.push(`- Column transforms:`);
    for (const col of transforms) {
      lines.push(`  - ${col.outputName}: ${col.expression}`);
    }
  }

  if (passthroughCols.length > 0) {
    lines.push(`- Passthrough columns: ${passthroughCols.map((c) => c.outputName).join(", ")}`);
  }

  if (cte.columns.length > 0) {
    lines.push(`- ONLY keep these ${cte.columns.length} columns: ${cte.columns.map((c) => c.outputName).join(", ")}`);
  }

  if (cte.whereClause) {
    lines.push(`- WHERE filter (pass as whereCondition — do NOT construct {{ ref() }}): ${cte.whereClause}`);
  }

  if (cte.hasJoin) {
    lines.push(`- Has JOIN — use apply-join-condition or update-workspace-node for join setup`);
  }

  return lines.join("\n");
}

/**
 * When the user's SQL contains CTEs, return a plan that instructs the agent
 * to break each CTE into a separate Coalesce node using the declarative tools.
 * CTEs are not supported in Coalesce — each CTE should be its own node.
 *
 * The plan includes per-CTE structured data: column transforms, WHERE clauses,
 * source tables, and which columns to keep/remove.
 */
function buildCtePlan(
  params: {
    workspaceID: string;
    goal?: string;
    sql?: string;
    targetName?: string;
  },
  ctes: ParsedCte[],
  nodeTypeSelections: {
    staging: PipelineNodeTypeSelection;
    multiSource: PipelineNodeTypeSelection;
    aggregation: PipelineNodeTypeSelection;
  }
): PipelinePlan {
  const stagingType = nodeTypeSelections.staging.selectedNodeType ?? "Stage";
  const multiSourceType = nodeTypeSelections.multiSource.selectedNodeType ?? stagingType;
  const aggregationType = nodeTypeSelections.aggregation.selectedNodeType ?? stagingType;

  const typeMap: Record<string, string> = {
    staging: stagingType,
    multiSource: multiSourceType,
    aggregation: aggregationType,
  };

  // Build per-CTE instructions
  const cteInstructions: string[] = [];
  for (const cte of ctes) {
    const pattern = classifyCtePattern(cte);
    const nodeType = typeMap[pattern]!;
    cteInstructions.push(buildCteNodeInstruction(cte, nodeType));
  }

  // Detect if any CTE references another CTE (pipeline dependency)
  const cteNameSet = new Set(ctes.map((c) => c.name));
  const cteDependencies: string[] = [];
  for (const cte of ctes) {
    const deps = ctes
      .filter((other) => other.name !== cte.name && cte.body.toUpperCase().includes(other.name))
      .map((other) => other.name);
    if (deps.length > 0) {
      cteDependencies.push(`${cte.name} depends on: ${deps.join(", ")}`);
    }
  }

  // Detect the final SELECT after all CTEs
  const finalSelectNote = extractFinalSelectFromCteQuery(params.sql ?? "", cteNameSet);

  const allTransformCount = ctes.reduce(
    (sum, cte) => sum + cte.columns.filter((c) => c.isTransform).length,
    0
  );
  const allFilterCount = ctes.filter((c) => c.whereClause).length;

  // Build structured per-CTE summary for easy agent consumption
  // Includes columnsParam / groupByColumnsParam / aggregatesParam for single-call creation
  const cteNodeSummary: CteNodeSummary[] = ctes.map((cte) => {
    const pattern = classifyCtePattern(cte);
    const nodeType = typeMap[pattern]!;
    const transforms = cte.columns.filter((c) => c.isTransform);

    const summary: CteNodeSummary = {
      name: cte.name,
      nodeType,
      pattern,
      sourceTable: cte.sourceTable,
      columnCount: cte.columns.length,
      transforms: transforms.map((c) => ({ column: c.outputName, expression: c.expression })),
      passthroughColumns: cte.columns.filter((c) => !c.isTransform).map((c) => c.outputName),
      whereFilter: cte.whereClause,
      hasGroupBy: cte.hasGroupBy,
      hasJoin: cte.hasJoin,
      dependsOn: ctes
        .filter((other) => other.name !== cte.name && cte.body.toUpperCase().includes(other.name))
        .map((other) => other.name),
    };

    // Add structured params for single-call creation
    if (cte.hasGroupBy && cte.columns.length > 0) {
      // GROUP BY CTEs: split columns into group-by (passthrough) and aggregates (transforms with agg functions)
      const groupByCols: string[] = [];
      const aggCols: Array<{ name: string; function: string; expression: string }> = [];
      for (const col of cte.columns) {
        const aggMatch = col.expression.match(/^(\w+)\s*\((.*)\)$/s);
        if (col.isTransform && aggMatch && isAggregateFn(aggMatch[1]!)) {
          aggCols.push({
            name: col.outputName,
            function: aggMatch[1]!.toUpperCase(),
            expression: aggMatch[2]!.trim(),
          });
        } else {
          // Non-aggregate columns in a GROUP BY CTE are the GROUP BY dimensions
          groupByCols.push(col.expression);
        }
      }
      if (groupByCols.length > 0 && aggCols.length > 0) {
        summary.groupByColumnsParam = groupByCols;
        summary.aggregatesParam = aggCols;
      }
    } else if (cte.columns.length > 0 && !cte.hasJoin) {
      // Only set columnsParam for single-source CTEs where expressions can be passed directly.
      // Multi-source JOIN CTEs have SQL aliases (soh.*, sl.*) that don't map to Coalesce node names —
      // the agent must translate these to "NODE_NAME"."COLUMN" format.
      summary.columnsParam = cte.columns.map((c) => ({
        name: c.outputName,
        ...(c.isTransform ? { transform: c.expression } : {}),
      }));
    }

    return summary;
  });

  return {
    version: 1,
    intent: "sql",
    status: "needs_clarification",
    STOP_AND_CONFIRM: `STOP. Present the pipeline summary to the user in a table format and ask for confirmation BEFORE creating any nodes. For EACH node in cteNodeSummary, display: name, the EXACT nodeType string (e.g. "Coalesce-Base-Node-Types:::Stage"), pattern, transforms, and whereFilter. Use the cteNodeSummary array — do NOT paraphrase or simplify the nodeType values. Do NOT proceed until the user explicitly approves.`,
    workspaceID: params.workspaceID,
    platform: null,
    goal: params.goal ?? null,
    sql: params.sql ?? null,
    nodes: [],
    cteNodeSummary,
    assumptions: [
      `Parsed ${ctes.length} CTEs with ${allTransformCount} column transforms and ${allFilterCount} WHERE filters.`,
      `Staging and aggregation CTEs: 1 call per node. Multi-source JOIN CTEs: 2 calls (create + apply-join-condition).`,
    ],
    openQuestions: [
      `STOP: Present this pipeline summary to the user and ask "Should I proceed with creating these ${ctes.length} nodes?" Do NOT create nodes until the user confirms.`,
      `This SQL uses CTEs (WITH ... AS), which Coalesce does not support as a single node. Each CTE must become a separate node.`,
      `--- PER-CTE INSTRUCTIONS ---\n\n${cteInstructions.join("\n\n")}`,
      ...(cteDependencies.length > 0
        ? [`CTE dependencies (create in order):\n${cteDependencies.map((d) => `  - ${d}`).join("\n")}`]
        : []),
      ...(finalSelectNote ? [finalSelectNote] : []),
      `Node type guidance (do NOT use list-workspace-node-types):\n` +
        `- Staging CTEs (single-source): nodeType "${stagingType}"\n` +
        `- Join/transform CTEs (multi-source): nodeType "${multiSourceType}"\n` +
        `- Aggregation CTEs (GROUP BY): nodeType "${aggregationType}"`,
      `Workflow per CTE:\n` +
        `create-workspace-node-from-predecessor accepts columns, whereCondition, groupByColumns, and aggregates directly:\n` +
        `- For staging/transform CTEs (single-source): 1 call — pass columns (from cteNodeSummary.columnsParam) + whereCondition\n` +
        `- For GROUP BY CTEs: 1 call — pass groupByColumns (from cteNodeSummary.groupByColumnsParam) + aggregates (from cteNodeSummary.aggregatesParam)\n` +
        `- For multi-source JOIN CTEs: 2 calls — first create-workspace-node-from-predecessor with columns + whereCondition, then apply-join-condition to set up FROM/JOIN/ON\n` +
        `- Do NOT construct {{ ref() }} syntax — the FROM clause and joins are auto-generated\n` +
        `- Pass repoPath to each call for automatic config completion`,
    ],
    warnings: [
      `SQL contains ${ctes.length} CTEs: ${ctes.map((c) => c.name).join(", ")}. Each must be a separate Coalesce node.` +
        (allTransformCount > 0 ? ` ${allTransformCount} column transforms detected.` : ``),
    ],
    supportedNodeTypes: nodeTypeSelections.staging.supportedNodeTypes.length > 0
      ? nodeTypeSelections.staging.supportedNodeTypes
      : [stagingType],
    nodeTypeSelection: nodeTypeSelections.staging,
  };
}

/**
 * Extract information about the final SELECT after all CTEs.
 */
function extractFinalSelectFromCteQuery(sql: string, cteNames: Set<string>): string | null {
  // Find the final SELECT that comes after the last CTE
  // It's the SELECT that's not inside any CTE body
  const trimmed = sql.trim();
  // Find last top-level SELECT
  let lastSelectIdx = -1;
  let depth = 0;
  const upper = trimmed.toUpperCase();
  for (let i = 0; i < trimmed.length; i++) {
    if (trimmed[i] === "(") depth++;
    else if (trimmed[i] === ")") depth--;
    else if (depth === 0 && upper.startsWith("SELECT", i)) {
      const before = i > 0 ? trimmed[i - 1] : " ";
      if (/[\s,)]/.test(before) || i === 0) {
        lastSelectIdx = i;
      }
    }
  }

  if (lastSelectIdx < 0) return null;

  const finalSelect = trimmed.slice(lastSelectIdx).trim();
  // Check which CTEs the final SELECT references
  const referencedCtes = [...cteNames].filter((name) =>
    new RegExp(`\\b${name}\\b`, "i").test(finalSelect)
  );

  if (referencedCtes.length === 0) return null;

  // Check if the final SELECT is just `SELECT * FROM single_cte` — redundant
  const selectStarFromOne =
    referencedCtes.length === 1 &&
    /^SELECT\s+\*\s+FROM\s+\w+\s*;?\s*$/i.test(finalSelect);

  if (selectStarFromOne) {
    return (
      `Final SELECT is just \`SELECT * FROM ${referencedCtes[0]}\` — this is redundant. ` +
      `The last CTE node (${referencedCtes[0]}) already represents the final output. ` +
      `Do NOT create an additional node for this.`
    );
  }

  return (
    `Final output query references: ${referencedCtes.join(", ")}. ` +
    `Create a final node with these as predecessors. ` +
    `The final SELECT is:\n${finalSelect.slice(0, 500)}${finalSelect.length > 500 ? "..." : ""}`
  );
}

export async function planPipeline(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    goal?: string;
    sql?: string;
    targetName?: string;
    targetNodeType?: string;
    description?: string;
    configOverrides?: Record<string, unknown>;
    locationName?: string;
    database?: string;
    schema?: string;
    sourceNodeIDs?: string[];
    repoPath?: string;
  }
): Promise<PipelinePlan> {
  const location = {
    ...(params.locationName ? { locationName: params.locationName } : {}),
    ...(params.database ? { database: params.database } : {}),
    ...(params.schema ? { schema: params.schema } : {}),
  };
  const workspaceNodeTypeInventory = await getWorkspaceNodeTypeInventory(
    client,
    params.workspaceID
  );

  if (params.sql && params.sql.trim().length > 0) {
    // Detect CTEs — Coalesce does not support CTEs. Each CTE should be a separate node.
    const ctes = extractCtes(params.sql);
    if (ctes.length > 0) {
      // Evaluate each layer pattern independently.
      // Goals explicitly mention "batch ETL CTE decomposition" so that specialized
      // patterns (Dynamic Tables, Incremental, etc.) are properly excluded by the scorer.
      const sharedContext = {
        workspaceNodeTypes: workspaceNodeTypeInventory.nodeTypes,
        workspaceNodeTypeCounts: workspaceNodeTypeInventory.counts,
        repoPath: params.repoPath,
      };
      const userGoal = params.goal ? ` for ${params.goal}` : "";
      const stagingSelection = selectPipelineNodeType({
        ...sharedContext,
        explicitNodeType: params.targetNodeType,
        goal: `batch ETL CTE decomposition — staging layer${userGoal}. Use Stage or Work node type.`,
        sourceCount: 1,
        hasJoin: false,
        hasGroupBy: false,
      });
      const multiSourceSelection = selectPipelineNodeType({
        ...sharedContext,
        explicitNodeType: params.targetNodeType,
        goal: `batch ETL CTE decomposition — join transform${userGoal}. Use Stage, Work, or View node type.`,
        sourceCount: 3,
        hasJoin: true,
        hasGroupBy: false,
      });
      const aggregationSelection = selectPipelineNodeType({
        ...sharedContext,
        explicitNodeType: params.targetNodeType,
        goal: `batch ETL CTE decomposition — aggregation transform${userGoal}. Use Stage or Work node type.`,
        sourceCount: 1,
        hasJoin: false,
        hasGroupBy: true,
      });
      const ctePlan = buildCtePlan(params, ctes, {
        staging: stagingSelection.selection,
        multiSource: multiSourceSelection.selection,
        aggregation: aggregationSelection.selection,
      });
      applyWorkspaceNodeTypeValidation(
        ctePlan,
        workspaceNodeTypeInventory,
        params.targetNodeType
      );
      return ctePlan;
    }

    const parseResult = parseSqlSelectItems(params.sql, parseSqlSourceRefs(params.sql));
    const {
      refs,
      predecessorNodes,
      openQuestions,
      warnings,
    } = await resolveSqlRefsToWorkspaceNodes(
      client,
      params.workspaceID,
      parseResult.refs
    );
    const selectionResult = selectPipelineNodeType({
      explicitNodeType: params.targetNodeType,
      goal: params.goal,
      targetName: params.targetName,
      sql: params.sql,
      sourceCount: refs.length,
      workspaceNodeTypes: workspaceNodeTypeInventory.nodeTypes,
      workspaceNodeTypeCounts: workspaceNodeTypeInventory.counts,
      repoPath: params.repoPath,
    });
    const plan = buildPlanFromSql(
      {
        workspaceID: params.workspaceID,
        goal: params.goal,
        sql: params.sql,
        targetName: params.targetName,
        description: params.description,
        targetNodeType: params.targetNodeType,
        configOverrides: params.configOverrides,
        nodeTypeSelection: selectionResult.selection,
        selectedNodeType: selectionResult.selectedCandidate,
        location,
      },
      { ...parseResult, refs },
      predecessorNodes,
      openQuestions,
      [...warnings, ...selectionResult.warnings]
    );
    applyWorkspaceNodeTypeValidation(
      plan,
      workspaceNodeTypeInventory,
      params.targetNodeType
    );

    return plan;
  }

  if (params.sourceNodeIDs && params.sourceNodeIDs.length > 0) {
    const {
      sourceRefs,
      predecessorNodes,
      openQuestions,
      warnings,
    } = await getSourceNodesByID(client, params.workspaceID, params.sourceNodeIDs);
    const multiSource = sourceRefs.length > 1;
    const singleSource = sourceRefs.length === 1;
    const selectionResult = selectPipelineNodeType({
      explicitNodeType: params.targetNodeType,
      goal: params.goal,
      targetName: params.targetName,
      sourceCount: sourceRefs.length,
      workspaceNodeTypes: workspaceNodeTypeInventory.nodeTypes,
      workspaceNodeTypeCounts: workspaceNodeTypeInventory.counts,
      repoPath: params.repoPath,
    });
    const selectedNodeType =
      selectionResult.selectedCandidate?.nodeType ??
      params.targetNodeType ??
      "Stage";

    if (singleSource) {
      const sourceRef = sourceRefs[0]!;
      const predecessor = predecessorNodes[sourceRef.nodeID!];
      const selectItems = buildSelectItemsFromSourceNode(
        sourceRef.nodeID!,
        sourceRef.alias ?? sourceRef.nodeName,
        predecessor
      );
      const ready =
        (selectionResult.selectedCandidate?.autoExecutable ?? true) &&
        openQuestions.length === 0 &&
        selectItems.length > 0;
      const planWarnings = [...warnings, ...selectionResult.warnings];
      const planOpenQuestions = [...openQuestions];
      if (selectionResult.selectedCandidate && !selectionResult.selectedCandidate.autoExecutable) {
        planWarnings.push(
          `Planner selected node type ${selectedNodeType}, but it likely needs additional semantic configuration before automatic creation.`
        );
        if (selectionResult.selectedCandidate.semanticSignals.length > 0) {
          planOpenQuestions.push(
            `Confirm the required configuration for ${selectedNodeType}: ${selectionResult.selectedCandidate.semanticSignals.join(
              ", "
            )}.`
          );
        }
        if (selectionResult.selectedCandidate.missingDefaultFields.length > 0) {
          planOpenQuestions.push(
            `Provide values for ${selectedNodeType} config fields without defaults: ${selectionResult.selectedCandidate.missingDefaultFields.join(
              ", "
            )}.`
          );
        }
      }

      const plan: PipelinePlan = {
        version: 1,
        intent: "goal",
        status: ready ? "ready" : "needs_clarification",
        workspaceID: params.workspaceID,
        platform: null,
        goal: params.goal ?? null,
        sql: null,
        nodes: [
          {
            planNodeID: "node-1",
            name: buildDefaultNodeName(params.targetName, [
              {
                locationName: sourceRef.locationName,
                nodeName: sourceRef.nodeName,
                alias: sourceRef.alias,
                nodeID: sourceRef.nodeID,
              },
            ], selectionResult.selectedCandidate?.family ?? null, selectionResult.selectedCandidate?.shortName ?? null),
            nodeType: selectedNodeType,
            nodeTypeFamily: selectionResult.selectedCandidate?.family ?? null,
            predecessorNodeIDs: [sourceRef.nodeID!],
            predecessorPlanNodeIDs: [],
            predecessorNodeNames: [sourceRef.nodeName],
            description: params.description ?? null,
            sql: null,
            selectItems,
            outputColumnNames: selectItems.flatMap((item) =>
              item.outputName ? [item.outputName] : []
            ),
            configOverrides: params.configOverrides
              ? deepClone(params.configOverrides)
              : {},
            sourceRefs,
            joinCondition: `FROM {{ ref('${sourceRef.locationName}', '${sourceRef.nodeName}') }} "${sourceRef.alias ?? sourceRef.nodeName}"`,
            location,
            requiresFullSetNode: true,
            ...(selectionResult.selectedCandidate?.templateDefaults
              ? { templateDefaults: selectionResult.selectedCandidate.templateDefaults }
              : {}),
          },
        ],
        assumptions: [
          `Planner ${selectionResult.selection.strategy} selected ${selectedNodeType} from repo/workspace candidates.`,
          "Goal-driven planning uses a pass-through projection from the supplied source node IDs when the selected type is projection-capable.",
          "Review the generated plan before execution if the goal implies filters, joins, or computed columns.",
        ],
        openQuestions: planOpenQuestions,
        warnings: planWarnings,
        supportedNodeTypes:
          selectionResult.selection.supportedNodeTypes.length > 0
            ? selectionResult.selection.supportedNodeTypes
            : [selectedNodeType],
        nodeTypeSelection: selectionResult.selection,
      };
      applyWorkspaceNodeTypeValidation(
        plan,
        workspaceNodeTypeInventory,
        params.targetNodeType
      );

      return plan;
    }

    const multiSourceWarnings = [...warnings, ...selectionResult.warnings];
    const multiSourceOpenQuestions = [
      ...openQuestions,
      ...(multiSource
        ? [
            `How should these sources be joined or filtered: ${sourceRefs
              .map((ref) => ref.nodeName)
              .join(", ")}?`,
          ]
        : []),
    ];
    if (selectionResult.selectedCandidate && !selectionResult.selectedCandidate.autoExecutable) {
      multiSourceWarnings.push(
        `Planner selected node type ${selectedNodeType}, but it likely needs additional semantic configuration before automatic creation.`
      );
      if (selectionResult.selectedCandidate.semanticSignals.length > 0) {
        multiSourceOpenQuestions.push(
          `Confirm the required configuration for ${selectedNodeType}: ${selectionResult.selectedCandidate.semanticSignals.join(
            ", "
          )}.`
        );
      }
    }

    const plan: PipelinePlan = {
      version: 1,
      intent: "goal",
      status: "needs_clarification",
      workspaceID: params.workspaceID,
      platform: null,
      goal: params.goal ?? null,
      sql: null,
      nodes: [
        {
          planNodeID: "node-1",
          name:
            params.targetName ??
            `${buildDefaultNodePrefix(
              selectionResult.selectedCandidate?.family ?? null,
              selectionResult.selectedCandidate?.shortName ?? null
            )}_MULTI_SOURCE`,
          nodeType: selectedNodeType,
          nodeTypeFamily: selectionResult.selectedCandidate?.family ?? null,
          predecessorNodeIDs: uniqueInOrder(sourceRefs.flatMap((ref) =>
            ref.nodeID ? [ref.nodeID] : []
          )),
          predecessorPlanNodeIDs: [],
          predecessorNodeNames: sourceRefs.map((ref) => ref.nodeName),
          description: params.description ?? null,
          sql: null,
          selectItems: [],
          outputColumnNames: [],
          configOverrides: params.configOverrides
            ? deepClone(params.configOverrides)
            : {},
          sourceRefs,
          joinCondition: null,
          location,
          requiresFullSetNode: true,
          ...(selectionResult.selectedCandidate?.templateDefaults
            ? { templateDefaults: selectionResult.selectedCandidate.templateDefaults }
            : {}),
        },
      ],
      assumptions: [
        `Planner ${selectionResult.selection.strategy} selected ${selectedNodeType} from repo/workspace candidates.`,
        "Goal-based planning can scaffold a multisource request, but it does not infer joins automatically.",
      ],
      openQuestions: multiSourceOpenQuestions,
      warnings: multiSourceWarnings,
      supportedNodeTypes:
        selectionResult.selection.supportedNodeTypes.length > 0
          ? selectionResult.selection.supportedNodeTypes
          : [selectedNodeType],
      nodeTypeSelection: selectionResult.selection,
    };
    applyWorkspaceNodeTypeValidation(
      plan,
      workspaceNodeTypeInventory,
      params.targetNodeType
    );

    return plan;
  }

  const openQuestions: string[] = [];
  if (!params.goal || params.goal.trim().length === 0) {
    openQuestions.push("What pipeline should be built, and what should it produce?");
  }
  if (!params.sourceNodeIDs || params.sourceNodeIDs.length === 0) {
    openQuestions.push("Which upstream Coalesce node IDs should this pipeline build from?");
  }
  const selectionResult = selectPipelineNodeType({
    explicitNodeType: params.targetNodeType,
    goal: params.goal,
    targetName: params.targetName,
    sourceCount: 0,
    workspaceNodeTypes: workspaceNodeTypeInventory.nodeTypes,
    workspaceNodeTypeCounts: workspaceNodeTypeInventory.counts,
    repoPath: params.repoPath,
  });

  const plan: PipelinePlan = {
    version: 1,
    intent: "goal",
    status: "needs_clarification",
    workspaceID: params.workspaceID,
    platform: null,
    goal: params.goal ?? null,
    sql: null,
    nodes: [],
    assumptions: [
      selectionResult.selectedCandidate
        ? `Planner ${selectionResult.selection.strategy} would prefer ${selectionResult.selectedCandidate.nodeType} for this goal once sources are confirmed.`
        : "Planner could not rank a preferred node type because no repo-backed or observed workspace candidates were available.",
      "Goal-only planning currently returns clarification questions rather than inferred node graphs.",
    ],
    openQuestions,
    warnings: [...selectionResult.warnings],
    supportedNodeTypes:
      selectionResult.selection.supportedNodeTypes.length > 0
        ? selectionResult.selection.supportedNodeTypes
        : selectionResult.selectedCandidate
          ? [selectionResult.selectedCandidate.nodeType]
          : ["Stage"],
    nodeTypeSelection: selectionResult.selection,
  };
  applyWorkspaceNodeTypeValidation(
    plan,
    workspaceNodeTypeInventory,
    params.targetNodeType
  );

  return plan;
}

export function getNodeColumnArray(node: Record<string, unknown>): Record<string, unknown>[] {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!Array.isArray(metadata?.columns)) {
    return [];
  }
  return metadata.columns.filter(isPlainObject);
}

export function getColumnSourceNodeIDs(column: Record<string, unknown>): string[] {
  if (!Array.isArray(column.sources)) {
    return [];
  }
  const ids = new Set<string>();
  for (const source of column.sources) {
    if (!isPlainObject(source) || !Array.isArray(source.columnReferences)) {
      continue;
    }
    for (const ref of source.columnReferences) {
      if (isPlainObject(ref) && typeof ref.nodeID === "string") {
        ids.add(ref.nodeID);
      }
    }
  }
  return Array.from(ids);
}

export function findMatchingBaseColumn(
  node: Record<string, unknown>,
  selectItem: PlannedSelectItem
): Record<string, unknown> | null {
  const normalizedTargetName = normalizeSqlIdentifier(selectItem.sourceColumnName ?? "");
  for (const column of getNodeColumnArray(node)) {
    if (
      typeof column.name !== "string" ||
      normalizeSqlIdentifier(column.name) !== normalizedTargetName
    ) {
      continue;
    }

    const sourceNodeIDs = getColumnSourceNodeIDs(column);
    if (selectItem.sourceNodeID && sourceNodeIDs.includes(selectItem.sourceNodeID)) {
      return deepClone(column);
    }
    if (!selectItem.sourceNodeID) {
      return deepClone(column);
    }
  }

  return null;
}

export function renameSourceMappingEntries(
  node: Record<string, unknown>,
  newName: string
): Record<string, unknown> {
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  if (!metadata || !Array.isArray(metadata.sourceMapping)) {
    return node;
  }

  const previousName =
    typeof node.name === "string" && node.name.trim().length > 0 ? node.name : null;
  const updateSingleUnnamedMapping = previousName === null && metadata.sourceMapping.length === 1;

  return {
    ...node,
    metadata: {
      ...metadata,
      sourceMapping: metadata.sourceMapping.map((entry) => {
        if (!isPlainObject(entry)) {
          return entry;
        }
        const shouldRename =
          (previousName !== null && entry.name === previousName) ||
          updateSingleUnnamedMapping;
        if (!shouldRename) {
          return entry;
        }
        return {
          ...entry,
          name: newName,
        };
      }),
    },
  };
}

export function buildStageSourceMappingFromPlan(
  currentNode: Record<string, unknown>,
  nodePlan: PlannedPipelineNode
): Record<string, unknown>[] {
  const metadata = isPlainObject(currentNode.metadata) ? currentNode.metadata : undefined;
  const existingEntry =
    metadata && Array.isArray(metadata.sourceMapping)
      ? metadata.sourceMapping.find(isPlainObject)
      : undefined;

  const aliases: Record<string, string> = {};
  for (const ref of nodePlan.sourceRefs) {
    if (!ref.nodeID) {
      continue;
    }
    const alias = ref.alias ?? ref.nodeName;
    if (nodePlan.sourceRefs.length > 1 || ref.alias) {
      aliases[alias] = ref.nodeID;
    }
  }

  return [
    {
      ...(isPlainObject(existingEntry) ? existingEntry : {}),
      aliases,
      customSQL: {
        ...(isPlainObject(existingEntry) && isPlainObject(existingEntry.customSQL)
          ? existingEntry.customSQL
          : {}),
        customSQL: "",
      },
      dependencies: getUniqueSourceDependencies(nodePlan.sourceRefs),
      join: {
        ...(isPlainObject(existingEntry) && isPlainObject(existingEntry.join)
          ? existingEntry.join
          : {}),
        joinCondition: nodePlan.joinCondition ?? "",
      },
      name: nodePlan.name,
      noLinkRefs:
        isPlainObject(existingEntry) && Array.isArray(existingEntry.noLinkRefs)
          ? existingEntry.noLinkRefs
          : [],
    },
  ];
}

function buildStageNodeBodyFromPlan(
  currentNode: Record<string, unknown>,
  nodePlan: PlannedPipelineNode
): Record<string, unknown> {
  const updatedNode = deepClone(currentNode);
  updatedNode.name = nodePlan.name;
  if (nodePlan.description !== null) {
    updatedNode.description = nodePlan.description;
  }

  if (Object.keys(nodePlan.location).length > 0) {
    Object.assign(updatedNode, nodePlan.location);
  }

  updatedNode.config = {
    ...DEFAULT_STAGE_CONFIG,
    ...(isPlainObject(updatedNode.config) ? updatedNode.config : {}),
    ...nodePlan.configOverrides,
  };

  const plannedColumns: Record<string, unknown>[] = [];
  for (const selectItem of nodePlan.selectItems) {
    const baseColumn = findMatchingBaseColumn(updatedNode, selectItem);
    if (!baseColumn) {
      throw new Error(
        `Could not map planned output column ${selectItem.outputName ?? selectItem.expression} onto the created predecessor-based node body.`
      );
    }
    baseColumn.name = selectItem.outputName ?? baseColumn.name;
    if (isPlainObject(baseColumn.columnReference)) {
      baseColumn.columnReference = {
        ...baseColumn.columnReference,
        columnCounter: randomUUID(),
      };
    }
    if (typeof baseColumn.columnID === "string") {
      baseColumn.columnID = randomUUID();
    }
    plannedColumns.push(baseColumn);
  }

  const currentMetadata = isPlainObject(updatedNode.metadata)
    ? updatedNode.metadata
    : {};
  updatedNode.metadata = {
    ...currentMetadata,
    columns: plannedColumns,
    sourceMapping: buildStageSourceMappingFromPlan(updatedNode, nodePlan),
  };

  return renameSourceMappingEntries(updatedNode, nodePlan.name);
}

function getSavedNodeColumnNames(node: Record<string, unknown>): string[] {
  return getColumnNamesFromNode(node);
}

function validateSavedStageNode(
  node: Record<string, unknown>,
  nodePlan: PlannedPipelineNode
) {
  const savedColumnNames = getSavedNodeColumnNames(node);
  const expectedColumnNames = nodePlan.outputColumnNames;
  const normalizedSaved = savedColumnNames.map(normalizeSqlIdentifier);
  const normalizedExpected = expectedColumnNames.map(normalizeSqlIdentifier);
  const referencedPredecessorNodeIDs = new Set<string>();
  const metadata = isPlainObject(node.metadata) ? node.metadata : undefined;
  const sourceMappingEntry =
    metadata && Array.isArray(metadata.sourceMapping)
      ? metadata.sourceMapping.find(isPlainObject)
      : undefined;
  const savedDependencies =
    isPlainObject(sourceMappingEntry) && Array.isArray(sourceMappingEntry.dependencies)
      ? sourceMappingEntry.dependencies
          .filter(isPlainObject)
          .flatMap((dependency) => {
            if (typeof dependency.nodeName !== "string") {
              return [];
            }
            return [{
              locationName:
                typeof dependency.locationName === "string" ? dependency.locationName : null,
              nodeName: dependency.nodeName,
            }];
          })
      : [];
  const expectedDependencies = getUniqueSourceDependencies(nodePlan.sourceRefs);
  const actualDependencyKeys = uniqueInOrder(
    savedDependencies.map((dependency) =>
      buildSourceDependencyKey(dependency.locationName, dependency.nodeName)
    )
  );
  const expectedDependencyKeys = expectedDependencies.map((dependency) =>
    buildSourceDependencyKey(dependency.locationName, dependency.nodeName)
  );
  const expectedPredecessorNodeIDs = uniqueInOrder(nodePlan.predecessorNodeIDs);
  const savedJoinCondition =
    isPlainObject(sourceMappingEntry) &&
    isPlainObject(sourceMappingEntry.join) &&
    typeof sourceMappingEntry.join.joinCondition === "string"
      ? normalizeWhitespace(sourceMappingEntry.join.joinCondition)
      : "";

  for (const column of getNodeColumnArray(node)) {
    for (const nodeID of getColumnSourceNodeIDs(column)) {
      referencedPredecessorNodeIDs.add(nodeID);
    }
  }

  return {
    nodeNameSatisfied: node.name === nodePlan.name,
    expectedColumnCount: expectedColumnNames.length,
    actualColumnCount: savedColumnNames.length,
    outputColumnsSatisfied:
      normalizedExpected.length === normalizedSaved.length &&
      normalizedExpected.every((name, index) => normalizedSaved[index] === name),
    expectedColumnNames,
    actualColumnNames: savedColumnNames,
    sourceMappingDependenciesSatisfied:
      actualDependencyKeys.length === expectedDependencyKeys.length &&
      expectedDependencyKeys.every((key) => actualDependencyKeys.includes(key)),
    expectedDependencyNodeNames: expectedDependencies.map((dependency) => dependency.nodeName),
    actualDependencyNodeNames: uniqueInOrder(
      savedDependencies.map((dependency) => dependency.nodeName)
    ),
    joinConditionSatisfied:
      (nodePlan.joinCondition === null && savedJoinCondition.length === 0) ||
      savedJoinCondition === normalizeWhitespace(nodePlan.joinCondition ?? ""),
    expectedJoinCondition: nodePlan.joinCondition,
    actualJoinCondition:
      savedJoinCondition.length > 0 ? savedJoinCondition : null,
    predecessorCoverageSatisfied: expectedPredecessorNodeIDs.every((nodeID) =>
      referencedPredecessorNodeIDs.has(nodeID)
    ),
    predecessorNodeIDs: expectedPredecessorNodeIDs,
    referencedPredecessorNodeIDs: Array.from(referencedPredecessorNodeIDs),
  };
}

async function deleteWorkspaceNode(
  client: CoalesceClient,
  workspaceID: string,
  nodeID: string
): Promise<void> {
  await client.delete(
    `/api/v1/workspaces/${validatePathSegment(workspaceID, "workspaceID")}/nodes/${validatePathSegment(nodeID, "nodeID")}`
  );
}

async function rollbackCreatedPipelineNodes(
  client: CoalesceClient,
  workspaceID: string,
  nodeIDs: string[]
): Promise<string[]> {
  const rollbackFailures: string[] = [];
  const uniqueNodeIDs = Array.from(new Set(nodeIDs));

  for (const nodeID of uniqueNodeIDs.reverse()) {
    try {
      await deleteWorkspaceNode(client, workspaceID, nodeID);
    } catch {
      rollbackFailures.push(nodeID);
    }
  }

  return rollbackFailures;
}
