import { describe, expect, it } from "vitest";
import {
  isIdentifierChar,
  stripIdentifierQuotes,
  findTopLevelKeywordIndex,
  scanTopLevel,
  splitTopLevel,
  tokenizeTopLevelWhitespace,
  splitTopLevelWhitespace,
  skipSqlTrivia,
  matchesKeywordAt,
  findClosingParen,
  extractParenBody,
} from "../../src/services/pipelines/sql-tokenizer.js";

// ---------------------------------------------------------------------------
// isIdentifierChar
// ---------------------------------------------------------------------------

describe("isIdentifierChar", () => {
  it("returns true for letters", () => {
    expect(isIdentifierChar("A")).toBe(true);
    expect(isIdentifierChar("z")).toBe(true);
  });

  it("returns true for digits", () => {
    expect(isIdentifierChar("0")).toBe(true);
    expect(isIdentifierChar("9")).toBe(true);
  });

  it("returns true for underscore and dollar sign", () => {
    expect(isIdentifierChar("_")).toBe(true);
    expect(isIdentifierChar("$")).toBe(true);
  });

  it("returns false for non-identifier chars", () => {
    expect(isIdentifierChar(" ")).toBe(false);
    expect(isIdentifierChar(",")).toBe(false);
    expect(isIdentifierChar("(")).toBe(false);
    expect(isIdentifierChar(".")).toBe(false);
  });

  it("returns false for undefined", () => {
    expect(isIdentifierChar(undefined)).toBe(false);
  });

  it("returns false for empty string", () => {
    expect(isIdentifierChar("")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// stripIdentifierQuotes
// ---------------------------------------------------------------------------

describe("stripIdentifierQuotes", () => {
  it("strips double quotes", () => {
    expect(stripIdentifierQuotes('"MY_TABLE"')).toBe("MY_TABLE");
  });

  it("strips backticks", () => {
    expect(stripIdentifierQuotes("`my_column`")).toBe("my_column");
  });

  it("strips square brackets", () => {
    expect(stripIdentifierQuotes("[dbo]")).toBe("dbo");
  });

  it("returns unquoted strings as-is", () => {
    expect(stripIdentifierQuotes("MY_TABLE")).toBe("MY_TABLE");
  });

  it("trims whitespace before checking quotes", () => {
    expect(stripIdentifierQuotes('  "QUOTED"  ')).toBe("QUOTED");
  });

  it("does not strip mismatched quotes", () => {
    expect(stripIdentifierQuotes('"MIXED`')).toBe('"MIXED`');
    expect(stripIdentifierQuotes("[MIXED)")).toBe("[MIXED)");
  });

  it("handles empty string", () => {
    expect(stripIdentifierQuotes("")).toBe("");
  });

  it("handles single-char quoted (just quotes)", () => {
    // "\"\"" → empty string after stripping
    expect(stripIdentifierQuotes('""')).toBe("");
  });
});

// ---------------------------------------------------------------------------
// findTopLevelKeywordIndex
// ---------------------------------------------------------------------------

describe("findTopLevelKeywordIndex", () => {
  it("finds keyword at top level", () => {
    const sql = "SELECT a FROM b WHERE c = 1";
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(9);
    expect(findTopLevelKeywordIndex(sql, "WHERE")).toBe(16);
  });

  it("is case-insensitive", () => {
    expect(findTopLevelKeywordIndex("select a from b", "FROM")).toBe(9);
    expect(findTopLevelKeywordIndex("SELECT a FROM b", "from")).toBe(9);
  });

  it("ignores keywords inside parentheses", () => {
    const sql = "SELECT (SELECT a FROM sub) FROM main";
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(27);
  });

  it("ignores keywords inside single-quoted strings", () => {
    const sql = "SELECT 'FROM inside' FROM real_table";
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(21);
  });

  it("ignores keywords inside double-quoted identifiers", () => {
    const sql = 'SELECT "FROM" FROM real_table';
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(14);
  });

  it("ignores keywords inside backtick-quoted identifiers", () => {
    const sql = "SELECT `FROM` FROM real_table";
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(14);
  });

  it("ignores keywords inside bracket-quoted identifiers", () => {
    const sql = "SELECT [FROM] FROM real_table";
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(14);
  });

  it("ignores keywords inside line comments", () => {
    const sql = "SELECT a -- FROM comment\nFROM real_table";
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(25);
  });

  it("ignores keywords inside block comments", () => {
    const sql = "SELECT a /* FROM block */ FROM real_table";
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(26);
  });

  it("handles escaped single quotes", () => {
    const sql = "SELECT 'it''s FROM here' FROM t";
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(25);
  });

  it("returns -1 when keyword is not found", () => {
    expect(findTopLevelKeywordIndex("SELECT a FROM b", "WHERE")).toBe(-1);
  });

  it("does not match partial words", () => {
    // "FROMAGE" should not match "FROM"
    expect(findTopLevelKeywordIndex("SELECT FROMAGE", "FROM")).toBe(-1);
    // "_FROM_" should not match
    expect(findTopLevelKeywordIndex("SELECT _FROM_", "FROM")).toBe(-1);
  });

  it("matches keyword at start of string", () => {
    expect(findTopLevelKeywordIndex("FROM table1", "FROM")).toBe(0);
  });

  it("matches keyword at end of string", () => {
    expect(findTopLevelKeywordIndex("SELECT * FROM", "FROM")).toBe(9);
  });

  it("respects startIndex parameter", () => {
    const sql = "SELECT a FROM b, c FROM d";
    expect(findTopLevelKeywordIndex(sql, "FROM", 10)).toBe(19);
  });

  it("handles nested parentheses", () => {
    const sql = "SELECT (a + (b * c)) FROM t";
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBe(21);
  });
});

// ---------------------------------------------------------------------------
// scanTopLevel
// ---------------------------------------------------------------------------

describe("scanTopLevel", () => {
  it("yields only non-quoted, non-comment characters", () => {
    const chars: string[] = [];
    scanTopLevel("A 'B' C", (char) => {
      chars.push(char);
      return true;
    });
    // Should see A, space, space, C but not B, quotes, or the content inside quotes
    // Actually: A (idx 0), space (idx 1), space (idx 5), C (idx 6)
    expect(chars).toEqual(["A", " ", " ", "C"]);
  });

  it("tracks parenthesis depth", () => {
    const depths: number[] = [];
    scanTopLevel("A(B(C)D)E", (char, _idx, parenDepth) => {
      depths.push(parenDepth);
      return true;
    });
    // A at depth 0, B at depth 1, C at depth 2, D at depth 1, E at depth 0
    expect(depths).toEqual([0, 1, 2, 1, 0]);
  });

  it("stops early when callback returns false", () => {
    const chars: string[] = [];
    scanTopLevel("ABCDEF", (char) => {
      chars.push(char);
      return char !== "C";
    });
    expect(chars).toEqual(["A", "B", "C"]);
  });

  it("skips block comments entirely", () => {
    const chars: string[] = [];
    scanTopLevel("A/* comment */B", (char) => {
      chars.push(char);
      return true;
    });
    expect(chars).toEqual(["A", "B"]);
  });

  it("skips line comments up to newline", () => {
    const chars: string[] = [];
    scanTopLevel("A-- comment\nB", (char) => {
      chars.push(char);
      return true;
    });
    expect(chars).toEqual(["A", "B"]);
  });
});

// ---------------------------------------------------------------------------
// splitTopLevel
// ---------------------------------------------------------------------------

describe("splitTopLevel", () => {
  it("splits on commas at top level", () => {
    expect(splitTopLevel("a, b, c", ",")).toEqual(["a", "b", "c"]);
  });

  it("does not split on commas inside parentheses", () => {
    expect(splitTopLevel("a, fn(b, c), d", ",")).toEqual(["a", "fn(b, c)", "d"]);
  });

  it("does not split on commas inside strings", () => {
    expect(splitTopLevel("a, 'b, c', d", ",")).toEqual(["a", "'b, c'", "d"]);
  });

  it("handles single element (no delimiter)", () => {
    expect(splitTopLevel("abc", ",")).toEqual(["abc"]);
  });

  it("handles empty string", () => {
    expect(splitTopLevel("", ",")).toEqual([]);
  });

  it("trims whitespace from parts", () => {
    expect(splitTopLevel("  a ,  b  ,  c  ", ",")).toEqual(["a", "b", "c"]);
  });

  it("preserves content in nested parens", () => {
    expect(splitTopLevel("COALESCE(a, b), c", ",")).toEqual(["COALESCE(a, b)", "c"]);
  });
});

// ---------------------------------------------------------------------------
// tokenizeTopLevelWhitespace / splitTopLevelWhitespace
// ---------------------------------------------------------------------------

describe("tokenizeTopLevelWhitespace", () => {
  it("splits on whitespace at top level", () => {
    const tokens = tokenizeTopLevelWhitespace("SELECT a FROM b");
    expect(tokens.map((t) => t.text)).toEqual(["SELECT", "a", "FROM", "b"]);
  });

  it("preserves whitespace inside quoted strings", () => {
    const tokens = tokenizeTopLevelWhitespace("SELECT 'hello world' FROM t");
    expect(tokens.map((t) => t.text)).toEqual(["SELECT", "'hello world'", "FROM", "t"]);
  });

  it("preserves whitespace inside parenthesized expressions", () => {
    const tokens = tokenizeTopLevelWhitespace("SELECT COALESCE(a, b) FROM t");
    expect(tokens.map((t) => t.text)).toEqual(["SELECT", "COALESCE(a, b)", "FROM", "t"]);
  });

  it("tracks start and end positions", () => {
    const tokens = tokenizeTopLevelWhitespace("AB CD");
    expect(tokens).toEqual([
      { text: "AB", start: 0, end: 2 },
      { text: "CD", start: 3, end: 5 },
    ]);
  });

  it("strips line comments at top level", () => {
    const tokens = tokenizeTopLevelWhitespace("A -- comment\nB");
    expect(tokens.map((t) => t.text)).toEqual(["A", "B"]);
  });

  it("strips block comments at top level", () => {
    const tokens = tokenizeTopLevelWhitespace("A /* comment */ B");
    expect(tokens.map((t) => t.text)).toEqual(["A", "B"]);
  });

  it("handles escaped single quotes in strings", () => {
    const tokens = tokenizeTopLevelWhitespace("'it''s' X");
    expect(tokens.map((t) => t.text)).toEqual(["'it''s'", "X"]);
  });

  it("handles double-quoted identifiers with spaces", () => {
    const tokens = tokenizeTopLevelWhitespace('"My Column" X');
    expect(tokens.map((t) => t.text)).toEqual(['"My Column"', "X"]);
  });

  it("handles bracket-quoted identifiers with spaces", () => {
    const tokens = tokenizeTopLevelWhitespace("[My Column] X");
    expect(tokens.map((t) => t.text)).toEqual(["[My Column]", "X"]);
  });

  it("handles empty string", () => {
    expect(tokenizeTopLevelWhitespace("")).toEqual([]);
  });
});

describe("splitTopLevelWhitespace", () => {
  it("returns array of text tokens", () => {
    expect(splitTopLevelWhitespace("A B C")).toEqual(["A", "B", "C"]);
  });

  it("preserves parenthesized groups", () => {
    expect(splitTopLevelWhitespace("fn(a, b) c")).toEqual(["fn(a, b)", "c"]);
  });
});

// ---------------------------------------------------------------------------
// skipSqlTrivia
// ---------------------------------------------------------------------------

describe("skipSqlTrivia", () => {
  it("skips whitespace", () => {
    expect(skipSqlTrivia("   A", 0)).toBe(3);
  });

  it("skips line comments", () => {
    expect(skipSqlTrivia("-- comment\nA", 0)).toBe(11);
  });

  it("skips block comments", () => {
    expect(skipSqlTrivia("/* block */A", 0)).toBe(11);
  });

  it("skips mixed trivia", () => {
    expect(skipSqlTrivia("  -- line\n  /* block */  A", 0)).toBe(25);
  });

  it("returns same index when no trivia", () => {
    expect(skipSqlTrivia("A", 0)).toBe(0);
  });

  it("handles unclosed block comment", () => {
    const sql = "/* never closed";
    expect(skipSqlTrivia(sql, 0)).toBe(sql.length);
  });

  it("respects start index", () => {
    expect(skipSqlTrivia("AB   C", 2)).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// matchesKeywordAt
// ---------------------------------------------------------------------------

describe("matchesKeywordAt", () => {
  it("matches keyword at exact position", () => {
    expect(matchesKeywordAt("SELECT a FROM b", 9, "from")).toBe(true);
  });

  it("is case-insensitive (keyword must be lowercase)", () => {
    expect(matchesKeywordAt("FROM", 0, "from")).toBe(true);
    expect(matchesKeywordAt("from", 0, "from")).toBe(true);
    expect(matchesKeywordAt("From", 0, "from")).toBe(true);
  });

  it("rejects partial matches (followed by identifier char)", () => {
    expect(matchesKeywordAt("FROMAGE", 0, "from")).toBe(false);
  });

  it("rejects partial matches (preceded by identifier char)", () => {
    expect(matchesKeywordAt("_FROM", 1, "from")).toBe(false);
  });

  it("matches at end of string", () => {
    expect(matchesKeywordAt("SELECT", 0, "select")).toBe(true);
  });

  it("matches keyword preceded by non-identifier char", () => {
    expect(matchesKeywordAt("(FROM)", 1, "from")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// findClosingParen
// ---------------------------------------------------------------------------

describe("findClosingParen", () => {
  it("finds simple closing paren", () => {
    // sql = "(abc)", startIndex = 1 (after opening paren)
    expect(findClosingParen("(abc)", 1)).toBe(4);
  });

  it("handles nested parens", () => {
    // sql = "(a(b)c)", startIndex = 1
    expect(findClosingParen("(a(b)c)", 1)).toBe(6);
  });

  it("handles deeply nested parens", () => {
    expect(findClosingParen("(((a)))", 1)).toBe(6);
  });

  it("ignores parens inside single-quoted strings", () => {
    expect(findClosingParen("(')')", 1)).toBe(4);
  });

  it("ignores parens inside double-quoted identifiers", () => {
    expect(findClosingParen('(")")', 1)).toBe(4);
  });

  it("ignores parens inside backtick-quoted identifiers", () => {
    expect(findClosingParen("(`)`)", 1)).toBe(4);
  });

  it("ignores parens inside bracket-quoted identifiers", () => {
    // Bracket quoting starts with [ and ends with ]
    // ([)]) — [ starts bracket context at idx 1, ) inside bracket is literal, ] closes at idx 3, ) closes paren at idx 4
    expect(findClosingParen("([)])", 1)).toBe(4);
  });

  it("ignores parens inside line comments", () => {
    expect(findClosingParen("(a -- )\nb)", 1)).toBe(9);
  });

  it("ignores parens inside block comments", () => {
    expect(findClosingParen("(a /* ) */ b)", 1)).toBe(12);
  });

  it("handles escaped single quotes", () => {
    expect(findClosingParen("('it''s)')", 1)).toBe(9);
  });

  it("returns -1 for unbalanced parens", () => {
    expect(findClosingParen("(abc", 1)).toBe(-1);
  });

  it("returns -1 for empty after open", () => {
    expect(findClosingParen("(", 1)).toBe(-1);
  });
});

// ---------------------------------------------------------------------------
// extractParenBody
// ---------------------------------------------------------------------------

describe("extractParenBody", () => {
  it("extracts body between balanced parens", () => {
    expect(extractParenBody("(hello world)", 1)).toBe("hello world");
  });

  it("trims the result", () => {
    expect(extractParenBody("(  spaced  )", 1)).toBe("spaced");
  });

  it("preserves nested parens in body", () => {
    expect(extractParenBody("(a (b) c)", 1)).toBe("a (b) c");
  });

  it("returns null for unbalanced parens", () => {
    expect(extractParenBody("(unclosed", 1)).toBeNull();
  });

  it("handles empty body", () => {
    expect(extractParenBody("()", 1)).toBe("");
  });

  it("handles complex SQL expression", () => {
    const sql = "(COALESCE(a, 'N/A'), b)";
    expect(extractParenBody(sql, 1)).toBe("COALESCE(a, 'N/A'), b");
  });
});

// ---------------------------------------------------------------------------
// Integration: realistic SQL scenarios
// ---------------------------------------------------------------------------

describe("integration: realistic SQL", () => {
  it("finds all top-level keywords in a complex query", () => {
    const sql = `
      SELECT a, b, COUNT(*)
      FROM schema.table1 t1
      LEFT JOIN (SELECT x FROM sub WHERE y = 1) s ON t1.id = s.id
      WHERE t1.status = 'active'
      GROUP BY a, b
      HAVING COUNT(*) > 10
      ORDER BY a
    `;
    expect(findTopLevelKeywordIndex(sql, "SELECT")).toBeGreaterThan(-1);
    expect(findTopLevelKeywordIndex(sql, "FROM")).toBeGreaterThan(-1);
    expect(findTopLevelKeywordIndex(sql, "WHERE")).toBeGreaterThan(-1);
    expect(findTopLevelKeywordIndex(sql, "GROUP BY")).toBeGreaterThan(-1);
    expect(findTopLevelKeywordIndex(sql, "HAVING")).toBeGreaterThan(-1);
    expect(findTopLevelKeywordIndex(sql, "ORDER BY")).toBeGreaterThan(-1);

    // The first FROM should be the top-level one, not the subquery one
    const firstFrom = findTopLevelKeywordIndex(sql, "FROM");
    const sqlFromFrom = sql.slice(firstFrom);
    expect(sqlFromFrom.trimStart().startsWith("FROM schema.table1")).toBe(true);
  });

  it("splits a Coalesce-style SELECT list with {{ ref() }}", () => {
    const selectList =
      '{{ ref("SRC_ORDERS") }}."ORDER_ID", ' +
      '{{ ref("SRC_ORDERS") }}."CUSTOMER_ID", ' +
      "COALESCE(\"STATUS\", 'unknown') AS STATUS";

    const parts = splitTopLevel(selectList, ",");
    expect(parts).toHaveLength(3);
    expect(parts[0]).toContain("ORDER_ID");
    expect(parts[1]).toContain("CUSTOMER_ID");
    expect(parts[2]).toContain("COALESCE");
  });

  it("handles CTE with WITH clause", () => {
    const sql =
      "WITH cte AS (SELECT a FROM t WHERE x = 1) " +
      "SELECT cte.a FROM cte WHERE cte.a > 0";

    // Top-level FROM should be after the CTE
    const fromIdx = findTopLevelKeywordIndex(sql, "FROM");
    // Skip the WITH keyword
    const withIdx = findTopLevelKeywordIndex(sql, "WITH");
    expect(withIdx).toBe(0);
    // The first top-level FROM should be in the main SELECT, not inside CTE parens
    expect(sql.slice(fromIdx, fromIdx + 8)).toBe("FROM cte");
  });
});
