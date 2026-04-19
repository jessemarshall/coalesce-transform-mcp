import { describe, expect, it } from "vitest";
import {
  parseSqlSelectItems,
  splitExpressionAlias,
} from "../../src/services/pipelines/select-parsing.js";
import type { ParsedSqlSourceRef } from "../../src/services/pipelines/planning-types.js";

function makeRef(overrides: Partial<ParsedSqlSourceRef> = {}): ParsedSqlSourceRef {
  return {
    locationName: "STAGING",
    nodeName: "STG_CUSTOMER",
    alias: null,
    nodeID: "node-cust",
    sourceStyle: "table_name",
    locationCandidates: ["STAGING"],
    relationStart: 0,
    relationEnd: 0,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// splitExpressionAlias — parses "expr AS alias" / "expr alias" / "expr"
// ---------------------------------------------------------------------------

describe("splitExpressionAlias", () => {
  it("splits explicit AS alias", () => {
    expect(splitExpressionAlias("c.FIRST_NAME AS FNAME")).toEqual({
      expression: "c.FIRST_NAME",
      outputName: "FNAME",
    });
  });

  it("is case-insensitive on AS keyword", () => {
    expect(splitExpressionAlias("c.FIRST_NAME as FNAME")).toEqual({
      expression: "c.FIRST_NAME",
      outputName: "FNAME",
    });
  });

  it("strips double-quoted alias", () => {
    expect(splitExpressionAlias('c.X AS "My Column"')).toEqual({
      expression: "c.X",
      outputName: "My Column",
    });
  });

  it("strips backtick-quoted alias", () => {
    expect(splitExpressionAlias("c.X AS `colname`")).toEqual({
      expression: "c.X",
      outputName: "colname",
    });
  });

  it("strips square-bracket-quoted alias", () => {
    expect(splitExpressionAlias("c.X AS [colname]")).toEqual({
      expression: "c.X",
      outputName: "colname",
    });
  });

  it("treats bare trailing identifier as alias when expression is dotted", () => {
    expect(splitExpressionAlias("c.FIRST_NAME FNAME")).toEqual({
      expression: "c.FIRST_NAME",
      outputName: "FNAME",
    });
  });

  it("treats bare trailing identifier as alias when expression contains a function call", () => {
    expect(splitExpressionAlias("UPPER(c.NAME) UNAME")).toEqual({
      expression: "UPPER(c.NAME)",
      outputName: "UNAME",
    });
  });

  it("does NOT treat bare identifier as alias for simple column name (avoids false positives)", () => {
    expect(splitExpressionAlias("FIRST_NAME LAST")).toEqual({
      expression: "FIRST_NAME LAST",
      outputName: null,
    });
  });

  it("returns no alias for a bare column reference", () => {
    expect(splitExpressionAlias("c.FIRST_NAME")).toEqual({
      expression: "c.FIRST_NAME",
      outputName: null,
    });
  });

  it("trims internal whitespace around AS and the expression", () => {
    // Callers (parseSqlSelectItems via splitTopLevel) pre-trim the raw item,
    // so trailing whitespace on the input is out of contract — but the
    // function must still trim around the AS keyword and expression body.
    expect(splitExpressionAlias("c.FIRST_NAME   AS   FNAME")).toEqual({
      expression: "c.FIRST_NAME",
      outputName: "FNAME",
    });
  });
});

// ---------------------------------------------------------------------------
// parseSqlSelectItems — the critical path for pipeline output columns
// ---------------------------------------------------------------------------

describe("parseSqlSelectItems", () => {
  // -----------------------------------------------------------------------
  // Graceful failure on missing SELECT/FROM
  // -----------------------------------------------------------------------

  it("returns empty result with warning when there's no SELECT clause", () => {
    const result = parseSqlSelectItems("UPDATE foo SET bar = 1", []);
    expect(result.selectItems).toEqual([]);
    expect(result.warnings[0]).toMatch(/Could not find a top-level SELECT/i);
    expect(result.refs).toEqual([]);
  });

  it("returns empty result with warning when there's no FROM clause", () => {
    const result = parseSqlSelectItems("SELECT 1", []);
    expect(result.selectItems).toEqual([]);
    expect(result.warnings[0]).toMatch(/Could not find a top-level SELECT/i);
  });

  // -----------------------------------------------------------------------
  // Direct column projection — the dominant case
  // -----------------------------------------------------------------------

  it("parses a single qualified column reference", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems("SELECT c.CUSTOMER_ID FROM X", [ref]);

    expect(result.selectItems).toHaveLength(1);
    expect(result.selectItems[0]).toMatchObject({
      expression: "c.CUSTOMER_ID",
      outputName: "CUSTOMER_ID",
      sourceNodeAlias: "c",
      sourceNodeName: "STG_CUSTOMER",
      sourceNodeID: "node-cust",
      sourceColumnName: "CUSTOMER_ID",
      kind: "column",
      supported: true,
    });
  });

  it("parses a qualified column with explicit alias", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems(
      "SELECT c.FIRST_NAME AS FNAME FROM X",
      [ref]
    );

    expect(result.selectItems[0]).toMatchObject({
      expression: "c.FIRST_NAME",
      outputName: "FNAME",
      sourceColumnName: "FIRST_NAME",
      supported: true,
    });
  });

  it("parses multiple comma-separated columns", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems(
      "SELECT c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME FROM X",
      [ref]
    );

    expect(result.selectItems).toHaveLength(3);
    expect(result.selectItems.map((i) => i.outputName)).toEqual([
      "CUSTOMER_ID",
      "FIRST_NAME",
      "LAST_NAME",
    ]);
  });

  it("resolves unqualified columns when exactly one ref is present", () => {
    const ref = makeRef();
    const result = parseSqlSelectItems("SELECT CUSTOMER_ID FROM STG_CUSTOMER", [ref]);

    expect(result.selectItems[0]).toMatchObject({
      sourceColumnName: "CUSTOMER_ID",
      sourceNodeID: "node-cust",
      sourceNodeName: "STG_CUSTOMER",
      supported: true,
    });
  });

  it("rejects unqualified columns when multiple refs exist", () => {
    const refA = makeRef({ alias: "c", nodeName: "STG_CUSTOMER" });
    const refB = makeRef({
      alias: "o",
      nodeName: "STG_ORDERS",
      nodeID: "node-orders",
    });
    const result = parseSqlSelectItems(
      "SELECT CUSTOMER_ID FROM STG_CUSTOMER c JOIN STG_ORDERS o ON c.ID = o.CID",
      [refA, refB]
    );

    expect(result.selectItems[0]).toMatchObject({
      supported: false,
      reason: expect.stringMatching(/Unqualified columns/i),
    });
  });

  // -----------------------------------------------------------------------
  // Alias resolution — matching by alias or by node name
  // -----------------------------------------------------------------------

  it("resolves columns via alias lookup (case-insensitive)", () => {
    const ref = makeRef({ alias: "C" });
    const result = parseSqlSelectItems("SELECT c.X FROM STG_CUSTOMER c", [ref]);

    expect(result.selectItems[0]).toMatchObject({
      sourceNodeID: "node-cust",
      supported: true,
    });
  });

  it("resolves columns by node name when no alias is set", () => {
    const ref = makeRef();
    const result = parseSqlSelectItems(
      "SELECT STG_CUSTOMER.ID FROM STG_CUSTOMER",
      [ref]
    );

    expect(result.selectItems[0]).toMatchObject({
      sourceNodeID: "node-cust",
      supported: true,
    });
  });

  it("marks unresolved alias as unsupported", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems("SELECT x.FOO FROM X x", [ref]);

    expect(result.selectItems[0]).toMatchObject({
      supported: false,
      sourceNodeAlias: "x",
      reason: expect.stringMatching(/did not match a predecessor/i),
    });
  });

  // -----------------------------------------------------------------------
  // Wildcards
  // -----------------------------------------------------------------------

  it("parses a bare * wildcard when exactly one ref is present", () => {
    const ref = makeRef();
    const result = parseSqlSelectItems("SELECT * FROM STG_CUSTOMER", [ref]);

    expect(result.selectItems[0]).toMatchObject({
      expression: "*",
      sourceColumnName: "*",
      sourceNodeID: "node-cust",
      supported: true,
    });
  });

  it("rejects bare * when multiple refs are present", () => {
    const refA = makeRef({ alias: "c" });
    const refB = makeRef({
      alias: "o",
      nodeName: "STG_ORDERS",
      nodeID: "node-orders",
    });
    const result = parseSqlSelectItems(
      "SELECT * FROM STG_CUSTOMER c JOIN STG_ORDERS o ON c.ID = o.CID",
      [refA, refB]
    );

    expect(result.selectItems[0]).toMatchObject({
      supported: false,
      reason: expect.stringMatching(/Unqualified \*/i),
    });
  });

  it("parses a qualified alias.* wildcard", () => {
    const refA = makeRef({ alias: "c" });
    const refB = makeRef({
      alias: "o",
      nodeName: "STG_ORDERS",
      nodeID: "node-orders",
    });
    const result = parseSqlSelectItems(
      "SELECT c.* FROM STG_CUSTOMER c JOIN STG_ORDERS o ON c.ID = o.CID",
      [refA, refB]
    );

    expect(result.selectItems[0]).toMatchObject({
      sourceNodeAlias: "c",
      sourceNodeID: "node-cust",
      sourceColumnName: "*",
      supported: true,
    });
  });

  it("marks unresolved wildcard alias as unsupported", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems("SELECT x.* FROM STG_CUSTOMER c", [ref]);

    expect(result.selectItems[0]).toMatchObject({
      supported: false,
      sourceNodeAlias: "x",
      reason: expect.stringMatching(/Wildcard source alias/i),
    });
  });

  // -----------------------------------------------------------------------
  // Computed expressions — require an alias
  // -----------------------------------------------------------------------

  it("accepts a computed expression when aliased", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems(
      "SELECT UPPER(c.FIRST_NAME) AS FNAME_UPPER FROM X",
      [ref]
    );

    expect(result.selectItems[0]).toMatchObject({
      kind: "expression",
      outputName: "FNAME_UPPER",
      sourceColumnName: null,
      supported: true,
    });
  });

  it("rejects a computed expression without an alias", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems("SELECT UPPER(c.FIRST_NAME) FROM X", [ref]);

    expect(result.selectItems[0]).toMatchObject({
      kind: "expression",
      outputName: null,
      supported: false,
      reason: expect.stringMatching(/Computed expressions require an alias/i),
    });
  });

  it("accepts a CASE expression with alias", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems(
      "SELECT CASE WHEN c.STATUS = 'A' THEN 1 ELSE 0 END AS IS_ACTIVE FROM X",
      [ref]
    );

    expect(result.selectItems[0]).toMatchObject({
      kind: "expression",
      outputName: "IS_ACTIVE",
      supported: true,
    });
  });

  it("splits commas only at the top level (preserves function calls)", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems(
      "SELECT CONCAT(c.FIRST, ', ', c.LAST) AS FULL_NAME, c.ID FROM X",
      [ref]
    );

    expect(result.selectItems).toHaveLength(2);
    expect(result.selectItems[0]!.outputName).toBe("FULL_NAME");
    expect(result.selectItems[1]!.outputName).toBe("ID");
  });

  // -----------------------------------------------------------------------
  // Edge cases & guarantees
  // -----------------------------------------------------------------------

  it("warns when the SELECT clause is empty", () => {
    // Empty-between-keywords SELECT clause trims to "", which parseSqlSelectItems
    // treats the same as a missing clause (empty string is falsy).
    const result = parseSqlSelectItems("SELECT   FROM X", []);
    expect(result.selectItems).toEqual([]);
    expect(result.warnings).toEqual([
      "Could not find a top-level SELECT ... FROM clause in the SQL.",
    ]);
  });

  it("handles double-quoted column identifiers", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems('SELECT c."First Name" FROM X', [ref]);

    expect(result.selectItems[0]).toMatchObject({
      sourceColumnName: "First Name",
      supported: true,
    });
  });

  it("preserves the original expression text", () => {
    const ref = makeRef({ alias: "c" });
    const result = parseSqlSelectItems(
      "SELECT   UPPER(c.FIRST_NAME)   AS   FNAME   FROM X",
      [ref]
    );

    // Expression retains its core form after alias stripping
    expect(result.selectItems[0]!.expression).toBe("UPPER(c.FIRST_NAME)");
  });

  it("returns the same refs array it received", () => {
    const refs = [makeRef({ alias: "c" })];
    const result = parseSqlSelectItems("SELECT c.X FROM X", refs);
    expect(result.refs).toBe(refs);
  });
});
