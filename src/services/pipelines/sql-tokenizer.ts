export function isIdentifierChar(char: string | undefined): boolean {
  return !!char && /[A-Za-z0-9_$]/.test(char);
}

export function stripIdentifierQuotes(identifier: string): string {
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

export function findTopLevelKeywordIndex(sql: string, keyword: string, startIndex = 0): number {
  const lowerKeyword = keyword.toLowerCase();
  let parenDepth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let index = startIndex; index < sql.length; index += 1) {
    const char = sql[index];
    const next = sql[index + 1];

    if (inLineComment) {
      if (char === "\n") {
        inLineComment = false;
      }
      continue;
    }
    if (inBlockComment) {
      if (char === "*" && next === "/") {
        inBlockComment = false;
        index += 1;
      }
      continue;
    }
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
    if (char === "-" && next === "-") {
      inLineComment = true;
      index += 1;
      continue;
    }
    if (char === "/" && next === "*") {
      inBlockComment = true;
      index += 1;
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

/**
 * Iterates through a SQL string character-by-character, tracking quoting and
 * parenthesis depth.  For each unquoted, non-comment character (excluding
 * parenthesis delimiters themselves), the callback receives the character,
 * its index, and the current parenthesis depth.  Callers can inspect
 * `parenDepth` to filter for top-level-only processing.
 * The callback returns `true` to continue or `false` to stop early.
 *
 * The scanner handles: single-quoted strings (with '' escapes), double-quoted
 * identifiers, backtick-quoted identifiers, bracket-quoted identifiers, block
 * comments, and line comments.
 */
export function scanTopLevel(
  value: string,
  callback: (char: string, index: number, parenDepth: number) => boolean
): void {
  let parenDepth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let index = 0; index < value.length; index += 1) {
    const char = value[index]!;
    const next = value[index + 1];

    if (inLineComment) {
      if (char === "\n") {
        inLineComment = false;
      }
      continue;
    }
    if (inBlockComment) {
      if (char === "*" && next === "/") {
        inBlockComment = false;
        index += 1;
      }
      continue;
    }
    if (inSingleQuote) {
      if (char === "'" && next === "'") {
        index += 1;
      } else if (char === "'") {
        inSingleQuote = false;
      }
      continue;
    }
    if (inDoubleQuote) {
      if (char === '"') inDoubleQuote = false;
      continue;
    }
    if (inBacktick) {
      if (char === "`") inBacktick = false;
      continue;
    }
    if (inBracket) {
      if (char === "]") inBracket = false;
      continue;
    }

    if (char === "'") { inSingleQuote = true; continue; }
    if (char === '"') { inDoubleQuote = true; continue; }
    if (char === "`") { inBacktick = true; continue; }
    if (char === "[") { inBracket = true; continue; }
    if (char === "-" && next === "-") { inLineComment = true; index += 1; continue; }
    if (char === "/" && next === "*") { inBlockComment = true; index += 1; continue; }
    if (char === "(") { parenDepth += 1; continue; }
    if (char === ")" && parenDepth > 0) { parenDepth -= 1; continue; }

    if (!callback(char, index, parenDepth)) {
      return;
    }
  }
}

export function splitTopLevel(value: string, delimiter: string): string[] {
  const parts: string[] = [];
  let start = 0;

  scanTopLevel(value, (char, index, parenDepth) => {
    if (char === delimiter && parenDepth === 0) {
      parts.push(value.slice(start, index).trim());
      start = index + 1;
    }
    return true;
  });

  const tail = value.slice(start).trim();
  if (tail.length > 0) {
    parts.push(tail);
  }
  return parts;
}

export type TopLevelWhitespaceToken = {
  text: string;
  start: number;
  end: number;
};

export function tokenizeTopLevelWhitespace(value: string): TopLevelWhitespaceToken[] {
  const parts: TopLevelWhitespaceToken[] = [];
  let tokenStart: number | null = null;
  let tokenEnd = 0;
  let tokenText = "";
  let parenDepth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  const appendChar = (char: string, index: number) => {
    if (tokenStart === null) {
      tokenStart = index;
    }
    tokenText += char;
    tokenEnd = index + 1;
  };

  const flushToken = () => {
    if (tokenStart === null || tokenText.length === 0) {
      tokenStart = null;
      tokenEnd = 0;
      tokenText = "";
      return;
    }

    parts.push({
      text: tokenText,
      start: tokenStart,
      end: tokenEnd,
    });
    tokenStart = null;
    tokenEnd = 0;
    tokenText = "";
  };

  for (let index = 0; index < value.length; index += 1) {
    const char = value[index]!;
    const next = value[index + 1];

    if (inLineComment) {
      if (char === "\n") {
        inLineComment = false;
      }
      continue;
    }
    if (inBlockComment) {
      if (char === "*" && next === "/") {
        inBlockComment = false;
        index += 1;
      }
      continue;
    }
    if (inSingleQuote) {
      appendChar(char, index);
      if (char === "'" && next === "'") {
        appendChar(next, index + 1);
        index += 1;
      } else if (char === "'") {
        inSingleQuote = false;
      }
      continue;
    }
    if (inDoubleQuote) {
      appendChar(char, index);
      if (char === '"') {
        inDoubleQuote = false;
      }
      continue;
    }
    if (inBacktick) {
      appendChar(char, index);
      if (char === "`") {
        inBacktick = false;
      }
      continue;
    }
    if (inBracket) {
      appendChar(char, index);
      if (char === "]") {
        inBracket = false;
      }
      continue;
    }

    if (char === "-" && next === "-" && parenDepth === 0) {
      flushToken();
      inLineComment = true;
      index += 1;
      continue;
    }
    if (char === "/" && next === "*" && parenDepth === 0) {
      flushToken();
      inBlockComment = true;
      index += 1;
      continue;
    }
    if (/\s/u.test(char) && parenDepth === 0) {
      flushToken();
      continue;
    }
    if (char === "'") {
      appendChar(char, index);
      inSingleQuote = true;
      continue;
    }
    if (char === '"') {
      appendChar(char, index);
      inDoubleQuote = true;
      continue;
    }
    if (char === "`") {
      appendChar(char, index);
      inBacktick = true;
      continue;
    }
    if (char === "[") {
      appendChar(char, index);
      inBracket = true;
      continue;
    }
    if (char === "(") {
      appendChar(char, index);
      parenDepth += 1;
      continue;
    }
    if (char === ")" && parenDepth > 0) {
      parenDepth -= 1;
      appendChar(char, index);
      continue;
    }

    appendChar(char, index);
  }

  flushToken();
  return parts;
}

export function splitTopLevelWhitespace(value: string): string[] {
  return tokenizeTopLevelWhitespace(value).map((part) => part.text);
}

export function skipSqlTrivia(value: string, index: number): number {
  let nextIndex = index;
  while (nextIndex < value.length) {
    if (/\s/u.test(value[nextIndex] ?? "")) {
      nextIndex += 1;
      continue;
    }
    if (value[nextIndex] === "-" && value[nextIndex + 1] === "-") {
      nextIndex += 2;
      while (nextIndex < value.length && value[nextIndex] !== "\n") {
        nextIndex += 1;
      }
      continue;
    }
    if (value[nextIndex] === "/" && value[nextIndex + 1] === "*") {
      const blockEnd = value.indexOf("*/", nextIndex + 2);
      nextIndex = blockEnd >= 0 ? blockEnd + 2 : value.length;
      continue;
    }
    break;
  }
  return nextIndex;
}

export function matchesKeywordAt(value: string, index: number, keyword: string): boolean {
  return (
    value.slice(index, index + keyword.length).toLowerCase() === keyword &&
    !isIdentifierChar(value[index - 1]) &&
    !isIdentifierChar(value[index + keyword.length])
  );
}

/**
 * Find the index of the closing parenthesis that balances the opening one.
 * `startIndex` should be the position right after the opening '('.
 * Returns the index of the closing ')' or -1 if unbalanced.
 *
 * Handles all SQL quoting contexts: single-quoted strings, double-quoted
 * identifiers, backtick-quoted identifiers, bracket-quoted identifiers,
 * line comments (`--`), and block comments.
 */
export function findClosingParen(sql: string, startIndex: number): number {
  let depth = 1;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let i = startIndex; i < sql.length; i++) {
    const ch = sql[i]!;
    const next = sql[i + 1];

    if (inLineComment) {
      if (ch === "\n") inLineComment = false;
      continue;
    }
    if (inBlockComment) {
      if (ch === "*" && next === "/") { inBlockComment = false; i++; }
      continue;
    }
    if (inSingleQuote) {
      if (ch === "'" && next === "'") { i++; } else if (ch === "'") { inSingleQuote = false; }
      continue;
    }
    if (inDoubleQuote) {
      if (ch === '"') inDoubleQuote = false;
      continue;
    }
    if (inBacktick) {
      if (ch === "`") inBacktick = false;
      continue;
    }
    if (inBracket) {
      if (ch === "]") inBracket = false;
      continue;
    }

    if (ch === "'") { inSingleQuote = true; continue; }
    if (ch === '"') { inDoubleQuote = true; continue; }
    if (ch === "`") { inBacktick = true; continue; }
    if (ch === "[") { inBracket = true; continue; }
    if (ch === "-" && next === "-") { inLineComment = true; i++; continue; }
    if (ch === "/" && next === "*") { inBlockComment = true; i++; continue; }

    if (ch === "(") {
      depth++;
    } else if (ch === ")") {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}

/**
 * Extract the body between balanced parentheses.
 * `startIndex` should be the position right after the opening '('.
 */
export function extractParenBody(sql: string, startIndex: number): string | null {
  const closeIdx = findClosingParen(sql, startIndex);
  if (closeIdx < 0) return null;
  return sql.slice(startIndex, closeIdx).trim();
}

/**
 * Find the LAST `(` outside strings / comments / inner parens. Returns -1
 * when no top-level `(` exists. Used by the DML-envelope peelers to locate
 * the inner SELECT body of an `INSERT INTO target (cols) (SELECT ...)`
 * shape — the LAST top-level paren is the SELECT body; the FIRST is the
 * column list.
 */
export function findLastTopLevelOpenParen(s: string): number {
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;
  let parenDepth = 0;
  let lastIdx = -1;
  for (let i = 0; i < s.length; i++) {
    const c = s[i]!;
    const next = s[i + 1];
    if (inLineComment) { if (c === "\n") { inLineComment = false; } continue; }
    if (inBlockComment) { if (c === "*" && next === "/") { inBlockComment = false; i++; } continue; }
    if (inSingleQuote) {
      if (c === "'" && next === "'") { i++; }
      else if (c === "'") { inSingleQuote = false; }
      continue;
    }
    if (inDoubleQuote) { if (c === '"') { inDoubleQuote = false; } continue; }
    if (inBacktick) { if (c === "`") { inBacktick = false; } continue; }
    if (inBracket) { if (c === "]") { inBracket = false; } continue; }
    if (c === "'") { inSingleQuote = true; continue; }
    if (c === '"') { inDoubleQuote = true; continue; }
    if (c === "`") { inBacktick = true; continue; }
    if (c === "[") { inBracket = true; continue; }
    if (c === "-" && next === "-") { inLineComment = true; i++; continue; }
    if (c === "/" && next === "*") { inBlockComment = true; i++; continue; }
    if (c === "(") {
      if (parenDepth === 0) { lastIdx = i; }
      parenDepth++;
      continue;
    }
    if (c === ")") { if (parenDepth > 0) { parenDepth--; } continue; }
  }
  return lastIdx;
}
