import { describe, expect, it } from "vitest";
import {
  extractSelectClause,
  extractFromClause,
} from "../../src/services/pipelines/clause-extraction.js";

// ---------------------------------------------------------------------------
// extractSelectClause — pulls the column list between SELECT and FROM
// ---------------------------------------------------------------------------

describe("extractSelectClause", () => {
  it("extracts columns from a simple SELECT ... FROM query", () => {
    expect(extractSelectClause("SELECT a, b, c FROM X")).toBe("a, b, c");
  });

  it("trims whitespace around the column list", () => {
    expect(extractSelectClause("SELECT   a, b   FROM X")).toBe("a, b");
  });

  it("is case-insensitive on keywords", () => {
    expect(extractSelectClause("select a, b from X")).toBe("a, b");
  });

  it("handles a multiline SELECT clause", () => {
    const sql = `SELECT
  a,
  b,
  c
FROM X`;
    expect(extractSelectClause(sql)).toBe("a,\n  b,\n  c");
  });

  it("returns null when SELECT is missing", () => {
    expect(extractSelectClause("UPDATE X SET Y = 1")).toBeNull();
  });

  it("returns null when FROM is missing", () => {
    expect(extractSelectClause("SELECT 1")).toBeNull();
  });

  it("skips SELECTs nested inside parentheses (subqueries)", () => {
    // Top-level is the outer SELECT ... FROM. The extractor should grab
    // everything between the outer SELECT and the outer FROM, including
    // the subquery expression.
    const sql = "SELECT (SELECT MAX(x) FROM INNER_T) AS M FROM OUTER_T";
    expect(extractSelectClause(sql)).toBe(
      "(SELECT MAX(x) FROM INNER_T) AS M"
    );
  });

  it("ignores SELECT inside string literals", () => {
    const sql = "SELECT 'SELECT is a keyword' AS note FROM X";
    expect(extractSelectClause(sql)).toBe("'SELECT is a keyword' AS note");
  });

  it("ignores FROM inside string literals", () => {
    const sql = "SELECT 'from the start' AS note FROM X";
    expect(extractSelectClause(sql)).toBe("'from the start' AS note");
  });

  it("handles columns containing the substring 'from' inside identifier", () => {
    // "FROM_DATE" contains "FROM" as a substring but is not a keyword token.
    expect(extractSelectClause("SELECT FROM_DATE, TO_DATE FROM X")).toBe(
      "FROM_DATE, TO_DATE"
    );
  });
});

// ---------------------------------------------------------------------------
// extractFromClause — pulls everything from FROM onward
// ---------------------------------------------------------------------------

describe("extractFromClause", () => {
  it("extracts a simple FROM clause", () => {
    expect(extractFromClause("SELECT * FROM X")).toBe("FROM X");
  });

  it("extracts FROM with JOINs", () => {
    const sql = "SELECT * FROM A JOIN B ON A.id = B.id";
    expect(extractFromClause(sql)).toBe("FROM A JOIN B ON A.id = B.id");
  });

  it("strips a trailing semicolon", () => {
    expect(extractFromClause("SELECT * FROM X;")).toBe("FROM X");
  });

  it("strips a run of consecutive trailing semicolons", () => {
    expect(extractFromClause("SELECT * FROM X;;;")).toBe("FROM X");
  });

  it("strips trailing whitespace-then-semicolon sequence", () => {
    // Initial .trim() removes the outer whitespace; the regex removes the
    // final ;+\s* run. Whitespace *inside* the clause (before the semicolon)
    // is left alone.
    expect(extractFromClause("  SELECT * FROM X;   ")).toBe("FROM X");
  });

  it("returns null when SELECT is missing", () => {
    expect(extractFromClause("UPDATE X SET Y = 1")).toBeNull();
  });

  it("returns null when FROM is missing", () => {
    expect(extractFromClause("SELECT 1")).toBeNull();
  });

  it("preserves WHERE / GROUP BY / ORDER BY after FROM", () => {
    const sql =
      "SELECT * FROM X WHERE a = 1 GROUP BY b ORDER BY c";
    expect(extractFromClause(sql)).toBe(
      "FROM X WHERE a = 1 GROUP BY b ORDER BY c"
    );
  });

  it("handles a subquery in the FROM clause", () => {
    const sql = "SELECT * FROM (SELECT x FROM Y) sub";
    expect(extractFromClause(sql)).toBe("FROM (SELECT x FROM Y) sub");
  });

  it("ignores FROM inside string literals in the SELECT list", () => {
    const sql = "SELECT 'from literal' AS note FROM X";
    expect(extractFromClause(sql)).toBe("FROM X");
  });
});
