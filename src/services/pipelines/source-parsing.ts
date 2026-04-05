import type { ParsedSqlSourceRef } from "./planning-types.js";
import {
  stripIdentifierQuotes,
  matchesKeywordAt,
  scanTopLevel,
  splitTopLevel,
  tokenizeTopLevelWhitespace,
  skipSqlTrivia,
} from "./sql-tokenizer.js";
import { isSupportedIdentifierToken } from "./sql-utils.js";
import { extractFromClause } from "./clause-extraction.js";

/** Keywords that terminate a source segment in a FROM clause. */
const SOURCE_SEGMENT_TERMINATORS = [
  "join", "left", "right", "inner", "full", "cross", "natural", "lateral",
  "on", "using",
  "where", "group", "order", "having", "limit", "qualify",
  "union", "intersect", "except", "window", "fetch",
];

function findTerminatorKeyword(value: string, index: number): string | null {
  for (const keyword of SOURCE_SEGMENT_TERMINATORS) {
    if (matchesKeywordAt(value, index, keyword)) {
      return keyword;
    }
  }
  return null;
}

function extractTopLevelSourceSegments(
  fromClause: string
): Array<{ text: string; relationStart: number; relationEnd: number }> {
  const segments: Array<{ text: string; relationStart: number; relationEnd: number }> = [];
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

  scanTopLevel(fromClause, (char, index, parenDepth) => {
    if (parenDepth !== 0) {
      return true;
    }

    if (captureStart === null) {
      if (matchesKeywordAt(fromClause, index, "from")) {
        captureStart = skipSqlTrivia(fromClause, index + 4);
      } else if (matchesKeywordAt(fromClause, index, "join")) {
        captureStart = skipSqlTrivia(fromClause, index + 4);
      } else if (char === ",") {
        captureStart = skipSqlTrivia(fromClause, index + 1);
      }
      return true;
    }

    if (char === ",") {
      pushSegment(index);
      captureStart = skipSqlTrivia(fromClause, index + 1);
      return true;
    }

    const terminator = findTerminatorKeyword(fromClause, index);
    if (terminator) {
      pushSegment(index);
      captureStart =
        terminator === "join"
          ? skipSqlTrivia(fromClause, index + terminator.length)
          : null;
    }

    return true;
  });

  pushSegment(fromClause.length);
  return segments;
}

function parseSqlSourceSegment(
  segment: { text: string; relationStart: number; relationEnd: number }
): ParsedSqlSourceRef | null {
  const relationOffset = skipSqlTrivia(segment.text, 0);
  if (relationOffset >= segment.text.length) {
    return null;
  }

  let relationText: string;
  let relationTokenStart: number;
  let relationTokenEnd: number;
  let aliasTokens: string[];

  if (segment.text.slice(relationOffset).startsWith("{{")) {
    const closingIndex = segment.text.indexOf("}}", relationOffset);
    if (closingIndex < 0) {
      return null;
    }

    relationTokenStart = relationOffset;
    relationTokenEnd = closingIndex + 2;
    relationText = segment.text.slice(relationTokenStart, relationTokenEnd).trim();
    aliasTokens = tokenizeTopLevelWhitespace(segment.text.slice(relationTokenEnd)).map(
      (token) => token.text
    );
  } else {
    const tokens = tokenizeTopLevelWhitespace(segment.text);
    if (tokens.length === 0) {
      return null;
    }

    const relationToken = tokens[0]!;
    relationText = relationToken.text;
    relationTokenStart = relationToken.start;
    relationTokenEnd = relationToken.end;
    aliasTokens = tokens.slice(1).map((token) => token.text);
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
      relationStart: segment.relationStart + relationTokenStart,
      relationEnd: segment.relationStart + relationTokenEnd,
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
    relationStart: segment.relationStart + relationTokenStart,
    relationEnd: segment.relationStart + relationTokenEnd,
  };
}

type SqlSourceParseResult = {
  fromClause: string;
  refs: ParsedSqlSourceRef[];
};

export function parseSqlSourceRefs(sql: string): SqlSourceParseResult {
  const fromClause = extractFromClause(sql);
  if (!fromClause) {
    return { fromClause: "", refs: [] };
  }

  const refs = extractTopLevelSourceSegments(fromClause)
    .map(parseSqlSourceSegment)
    .filter((ref): ref is ParsedSqlSourceRef => ref !== null);

  return { fromClause, refs };
}
