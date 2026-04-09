import type {
  PlannedSelectItem,
  ParsedSqlSourceRef,
  SqlParseResult,
} from "./planning-types.js";
import {
  stripIdentifierQuotes,
  splitTopLevel,
} from "./sql-tokenizer.js";
import { normalizeSqlIdentifier, isSupportedIdentifierToken } from "./sql-utils.js";
import { extractSelectClause } from "./clause-extraction.js";

export function splitExpressionAlias(rawItem: string): { expression: string; outputName: string | null } {
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

export function parseSqlSelectItems(sql: string, refs: ParsedSqlSourceRef[]): SqlParseResult {
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
