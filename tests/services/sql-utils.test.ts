import { describe, it, expect } from "vitest";
import {
  normalizeSqlIdentifier,
  normalizeWhitespace,
  buildSourceDependencyKey,
  getUniqueSourceDependencies,
  escapeRegExp,
  isSimpleColumnRef,
  extractBareColumnName,
  isSupportedIdentifierToken,
} from "../../src/services/pipelines/sql-utils.js";

describe("normalizeSqlIdentifier", () => {
  it("strips double quotes and uppercases", () => {
    expect(normalizeSqlIdentifier('"customer_id"')).toBe("CUSTOMER_ID");
  });

  it("uppercases an unquoted identifier", () => {
    expect(normalizeSqlIdentifier("customer_id")).toBe("CUSTOMER_ID");
  });

  it("handles empty input", () => {
    expect(normalizeSqlIdentifier("")).toBe("");
  });
});

describe("normalizeWhitespace", () => {
  it("collapses runs of whitespace into a single space", () => {
    expect(normalizeWhitespace("a   b\tc\n\nd")).toBe("a b c d");
  });

  it("trims leading and trailing whitespace", () => {
    expect(normalizeWhitespace("   hello   ")).toBe("hello");
  });

  it("returns an empty string when only whitespace was provided", () => {
    expect(normalizeWhitespace("   \t\n  ")).toBe("");
  });
});

describe("buildSourceDependencyKey", () => {
  it("joins normalized location and node names with `::`", () => {
    expect(buildSourceDependencyKey("raw", "orders")).toBe("RAW::ORDERS");
  });

  it("treats null/undefined location names as empty", () => {
    expect(buildSourceDependencyKey(null, "orders")).toBe("::ORDERS");
    expect(buildSourceDependencyKey(undefined, "orders")).toBe("::ORDERS");
  });

  it("strips quotes from both halves before composing the key", () => {
    expect(buildSourceDependencyKey('"raw"', '"orders"')).toBe("RAW::ORDERS");
  });
});

describe("getUniqueSourceDependencies", () => {
  it("preserves first-seen order while deduplicating by normalized key", () => {
    const result = getUniqueSourceDependencies([
      { locationName: "RAW", nodeName: "ORDERS" },
      { locationName: "raw", nodeName: "orders" },
      { locationName: "RAW", nodeName: "CUSTOMERS" },
      { locationName: '"raw"', nodeName: '"orders"' },
    ]);
    expect(result).toEqual([
      { locationName: "RAW", nodeName: "ORDERS" },
      { locationName: "RAW", nodeName: "CUSTOMERS" },
    ]);
  });

  it("returns the original entries (not normalized copies)", () => {
    // Callers downstream rely on the originals to preserve the exact
    // location/node casing for ref() emission.
    const result = getUniqueSourceDependencies([
      { locationName: "Raw", nodeName: "Orders" },
    ]);
    expect(result).toEqual([{ locationName: "Raw", nodeName: "Orders" }]);
  });

  it("handles an empty input list", () => {
    expect(getUniqueSourceDependencies([])).toEqual([]);
  });
});

describe("escapeRegExp", () => {
  it("escapes regex metacharacters so they are matched literally", () => {
    const escaped = escapeRegExp("CTE$1.foo+bar");
    expect(new RegExp(escaped).test("CTE$1.foo+bar")).toBe(true);
    expect(new RegExp(escaped).test("CTEX1Xfoo+bar")).toBe(false);
  });

  it("leaves alphanumeric values untouched", () => {
    expect(escapeRegExp("plain_name1")).toBe("plain_name1");
  });

  it("escapes every metacharacter the regex grammar treats specially", () => {
    expect(escapeRegExp(".*+?^${}()|[]\\")).toBe(
      "\\.\\*\\+\\?\\^\\$\\{\\}\\(\\)\\|\\[\\]\\\\"
    );
  });

  it("returns an empty string for an empty input", () => {
    expect(escapeRegExp("")).toBe("");
  });
});

describe("isSimpleColumnRef", () => {
  it("accepts a bare unquoted identifier", () => {
    expect(isSimpleColumnRef("col")).toBe(true);
  });

  it("accepts a double-quoted identifier", () => {
    expect(isSimpleColumnRef('"COL"')).toBe(true);
  });

  it("accepts a table.col reference", () => {
    expect(isSimpleColumnRef("table.col")).toBe(true);
  });

  it("accepts a fully-quoted table.col reference", () => {
    expect(isSimpleColumnRef('"table"."col"')).toBe(true);
  });

  it("accepts a mixed-quoted table.col reference", () => {
    expect(isSimpleColumnRef('table."col"')).toBe(true);
    expect(isSimpleColumnRef('"table".col')).toBe(true);
  });

  it("rejects expressions with function calls", () => {
    expect(isSimpleColumnRef("UPPER(col)")).toBe(false);
  });

  it("rejects multi-segment dotted refs (database.schema.table.col)", () => {
    expect(isSimpleColumnRef("db.schema.table.col")).toBe(false);
  });

  it("rejects an empty string", () => {
    expect(isSimpleColumnRef("")).toBe(false);
  });

  it("rejects expressions with operators", () => {
    expect(isSimpleColumnRef("a + b")).toBe(false);
  });

  it("trims surrounding whitespace before checking", () => {
    expect(isSimpleColumnRef("  col  ")).toBe(true);
  });
});

describe("extractBareColumnName", () => {
  it("returns the unquoted column from a bare identifier", () => {
    expect(extractBareColumnName("CUSTOMER_ID")).toBe("CUSTOMER_ID");
  });

  it("strips quotes around the trailing column", () => {
    expect(extractBareColumnName('"customer_id"')).toBe("customer_id");
  });

  it("returns the column from a table.col reference", () => {
    expect(extractBareColumnName("orders.customer_id")).toBe("customer_id");
  });

  it("returns the column from a fully-quoted reference", () => {
    expect(extractBareColumnName('"ORDERS"."CUSTOMER_ID"')).toBe("CUSTOMER_ID");
  });

  it("returns null when the input has no identifier-like segment", () => {
    expect(extractBareColumnName("UPPER(col)")).toBeNull();
  });

  it("trims surrounding whitespace before extracting", () => {
    expect(extractBareColumnName("  col  ")).toBe("col");
  });

  it("documents the trailing-identifier behavior on hyphenated tokens", () => {
    // The regex is end-anchored without a leading boundary, so an input like
    // "a-b" matches the trailing identifier `b` rather than rejecting the
    // whole token. Callers downstream (ref() emission, SQL rewrites) only
    // pass results through isSimpleColumnRef first, so this surprising path
    // is unreachable in production — but locking the behavior here keeps a
    // future caller from quietly relying on a different return.
    expect(extractBareColumnName("a-b")).toBe("b");
  });
});

describe("isSupportedIdentifierToken", () => {
  it("accepts unquoted identifiers", () => {
    expect(isSupportedIdentifierToken("CUSTOMER_ID")).toBe(true);
    expect(isSupportedIdentifierToken("_under")).toBe(true);
  });

  it("accepts double-quoted identifiers", () => {
    expect(isSupportedIdentifierToken('"My Column"')).toBe(true);
  });

  it("accepts backtick-quoted identifiers (MySQL/BigQuery)", () => {
    expect(isSupportedIdentifierToken("`my col`")).toBe(true);
  });

  it("accepts bracket-quoted identifiers (SQL Server)", () => {
    expect(isSupportedIdentifierToken("[my col]")).toBe(true);
  });

  it("rejects empty quoted identifiers", () => {
    expect(isSupportedIdentifierToken('""')).toBe(false);
    expect(isSupportedIdentifierToken("``")).toBe(false);
    expect(isSupportedIdentifierToken("[]")).toBe(false);
  });

  it("rejects identifiers starting with a digit", () => {
    expect(isSupportedIdentifierToken("1col")).toBe(false);
  });

  it("rejects mismatched quote pairs", () => {
    expect(isSupportedIdentifierToken('"col`')).toBe(false);
    expect(isSupportedIdentifierToken("`col]")).toBe(false);
  });

  it("rejects multi-token strings (table.col)", () => {
    expect(isSupportedIdentifierToken("table.col")).toBe(false);
  });
});
