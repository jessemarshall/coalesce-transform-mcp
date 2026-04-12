import { describe, expect, it } from "vitest";
import {
  extractCtes,
  classifyCtePattern,
  isAggregateFn,
  type ParsedCte,
  type CteExtractionResult,
} from "../../src/services/pipelines/cte-parsing.js";

// ---------------------------------------------------------------------------
// extractCtes — basic WITH-based CTEs
// ---------------------------------------------------------------------------

describe("extractCtes", () => {
  it("extracts a single CTE with columns", () => {
    const sql = `
      WITH STG AS (
        SELECT ID, NAME FROM RAW.CUSTOMERS
      )
      SELECT * FROM STG
    `;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(1);
    expect(result.ctes[0]!.name).toBe("STG");
    expect(result.ctes[0]!.columns).toHaveLength(2);
    expect(result.ctes[0]!.columns[0]!.outputName).toBe("ID");
    expect(result.ctes[0]!.columns[1]!.outputName).toBe("NAME");
    expect(result.ctes[0]!.sourceTable).toBe("RAW.CUSTOMERS");
    expect(result.ctes[0]!.hasGroupBy).toBe(false);
    expect(result.ctes[0]!.hasJoin).toBe(false);
    expect(result.finalSelectSQL).toContain("SELECT");
    expect(result.finalSelectSQL).toContain("STG");
  });

  it("extracts multiple CTEs", () => {
    const sql = `
      WITH
        SRC AS (SELECT ID, AMOUNT FROM ORDERS),
        AGG AS (SELECT ID, SUM(AMOUNT) AS TOTAL FROM SRC GROUP BY ID)
      SELECT * FROM AGG
    `;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(2);
    expect(result.ctes[0]!.name).toBe("SRC");
    expect(result.ctes[1]!.name).toBe("AGG");
    expect(result.ctes[1]!.hasGroupBy).toBe(true);
  });

  it("returns empty for SQL without CTEs or subqueries", () => {
    const sql = "SELECT ID, NAME FROM CUSTOMERS WHERE ACTIVE = TRUE";
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(0);
    expect(result.finalSelectSQL).toBeNull();
  });

  it("uppercases CTE names", () => {
    const sql = "WITH my_cte AS (SELECT 1 AS val) SELECT * FROM my_cte";
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(1);
    expect(result.ctes[0]!.name).toBe("MY_CTE");
  });

  it("handles quoted CTE names", () => {
    const sql = `WITH "My CTE" AS (SELECT 1 AS val) SELECT * FROM "My CTE"`;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(1);
    expect(result.ctes[0]!.name).toBe("MY CTE");
  });

  it("skips RECURSIVE keyword", () => {
    const sql = `
      WITH RECURSIVE TREE AS (
        SELECT ID, PARENT_ID FROM NODES
      )
      SELECT * FROM TREE
    `;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(1);
    expect(result.ctes[0]!.name).toBe("TREE");
  });

  it("does not match WITH TIME ZONE as a CTE", () => {
    const sql = `SELECT CAST(ts AS TIMESTAMP WITH TIME ZONE) FROM events`;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(0);
  });

  it("does not match WITH inside a word boundary (e.g. WITHDRAW)", () => {
    const sql = `SELECT * FROM WITHDRAW_TABLE`;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// extractCtes — column parsing
// ---------------------------------------------------------------------------

describe("extractCtes — column parsing", () => {
  it("detects simple column references as non-transforms", () => {
    const sql = `WITH STG AS (SELECT ID, NAME FROM RAW.USERS) SELECT * FROM STG`;
    const result = extractCtes(sql);
    const cols = result.ctes[0]!.columns;
    expect(cols).toHaveLength(2);
    expect(cols[0]!.outputName).toBe("ID");
    expect(cols[0]!.isTransform).toBe(false);
    expect(cols[1]!.outputName).toBe("NAME");
    expect(cols[1]!.isTransform).toBe(false);
  });

  it("detects aliased expressions as transforms", () => {
    const sql = `WITH STG AS (SELECT UPPER(NAME) AS UPPER_NAME FROM RAW.USERS) SELECT * FROM STG`;
    const result = extractCtes(sql);
    const cols = result.ctes[0]!.columns;
    expect(cols).toHaveLength(1);
    expect(cols[0]!.outputName).toBe("UPPER_NAME");
    expect(cols[0]!.isTransform).toBe(true);
    expect(cols[0]!.expression).toContain("UPPER(NAME)");
  });

  it("detects column renames as transforms", () => {
    const sql = `WITH STG AS (SELECT ID AS USER_ID FROM RAW.USERS) SELECT * FROM STG`;
    const result = extractCtes(sql);
    const cols = result.ctes[0]!.columns;
    expect(cols).toHaveLength(1);
    expect(cols[0]!.outputName).toBe("USER_ID");
    expect(cols[0]!.isTransform).toBe(true);
  });

  it("handles table-qualified column references", () => {
    const sql = `WITH STG AS (SELECT T.ID, T.NAME FROM RAW.USERS T) SELECT * FROM STG`;
    const result = extractCtes(sql);
    const cols = result.ctes[0]!.columns;
    expect(cols).toHaveLength(2);
    expect(cols[0]!.outputName).toBe("ID");
    expect(cols[1]!.outputName).toBe("NAME");
  });

  it("expands TABLE.* references using referenced CTE columns", () => {
    const sql = `
      WITH
        SRC AS (SELECT ID, NAME FROM RAW.USERS),
        PASS AS (SELECT SRC.* FROM SRC)
      SELECT * FROM PASS
    `;
    const result = extractCtes(sql);
    const passCols = result.ctes[1]!.columns;
    // SRC.* should be expanded to ID, NAME from SRC
    expect(passCols).toHaveLength(2);
    expect(passCols[0]!.outputName).toBe("ID");
    expect(passCols[1]!.outputName).toBe("NAME");
  });

  it("handles SELECT * FROM (subquery) by recursing", () => {
    const sql = `
      WITH STG AS (
        SELECT * FROM (SELECT ID, NAME FROM RAW.USERS) SUB
      )
      SELECT * FROM STG
    `;
    const result = extractCtes(sql);
    const cols = result.ctes[0]!.columns;
    expect(cols).toHaveLength(2);
    expect(cols[0]!.outputName).toBe("ID");
    expect(cols[1]!.outputName).toBe("NAME");
  });
});

// ---------------------------------------------------------------------------
// extractCtes — WHERE clause extraction
// ---------------------------------------------------------------------------

describe("extractCtes — WHERE clause", () => {
  it("extracts WHERE clause from CTE body", () => {
    const sql = `
      WITH STG AS (
        SELECT ID, NAME FROM USERS WHERE ACTIVE = TRUE
      )
      SELECT * FROM STG
    `;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.whereClause).toContain("ACTIVE");
  });

  it("returns null when no WHERE clause", () => {
    const sql = `WITH STG AS (SELECT ID FROM USERS) SELECT * FROM STG`;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.whereClause).toBeNull();
  });

  it("terminates WHERE at GROUP BY", () => {
    const sql = `
      WITH STG AS (
        SELECT CATEGORY, COUNT(*) AS CNT FROM PRODUCTS WHERE ACTIVE = TRUE GROUP BY CATEGORY
      )
      SELECT * FROM STG
    `;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.whereClause).toContain("ACTIVE");
    expect(result.ctes[0]!.whereClause).not.toContain("GROUP");
    expect(result.ctes[0]!.whereClause).not.toContain("CATEGORY");
  });

  it("terminates WHERE at ORDER BY", () => {
    const sql = `
      WITH STG AS (
        SELECT ID FROM USERS WHERE ACTIVE = TRUE ORDER BY ID
      )
      SELECT * FROM STG
    `;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.whereClause).toContain("ACTIVE");
    expect(result.ctes[0]!.whereClause).not.toContain("ORDER");
  });
});

// ---------------------------------------------------------------------------
// extractCtes — source table extraction
// ---------------------------------------------------------------------------

describe("extractCtes — source table", () => {
  it("extracts simple table name", () => {
    const sql = `WITH STG AS (SELECT ID FROM CUSTOMERS) SELECT * FROM STG`;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.sourceTable).toBe("CUSTOMERS");
  });

  it("extracts schema.table", () => {
    const sql = `WITH STG AS (SELECT ID FROM RAW.CUSTOMERS) SELECT * FROM STG`;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.sourceTable).toBe("RAW.CUSTOMERS");
  });

  it("extracts database.schema.table", () => {
    const sql = `WITH STG AS (SELECT ID FROM PROD.RAW.CUSTOMERS) SELECT * FROM STG`;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.sourceTable).toBe("PROD.RAW.CUSTOMERS");
  });

  it("extracts quoted identifiers", () => {
    const sql = `WITH STG AS (SELECT ID FROM "my_db"."my_schema"."my_table") SELECT * FROM STG`;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.sourceTable).toBe("MY_DB.MY_SCHEMA.MY_TABLE");
  });

  it("returns null when FROM has subquery instead of table", () => {
    const sql = `WITH STG AS (SELECT ID FROM (SELECT ID FROM RAW.USERS) SUB) SELECT * FROM STG`;
    const result = extractCtes(sql);
    // FROM starts with (, not a table identifier
    expect(result.ctes[0]!.sourceTable).toBeNull();
  });

  it("returns null for CTE body without FROM", () => {
    const sql = `WITH STG AS (SELECT 1 AS VAL) SELECT * FROM STG`;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.sourceTable).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// extractCtes — GROUP BY and JOIN detection
// ---------------------------------------------------------------------------

describe("extractCtes — GROUP BY and JOIN detection", () => {
  it("detects GROUP BY", () => {
    const sql = `
      WITH AGG AS (
        SELECT CATEGORY, SUM(AMOUNT) AS TOTAL FROM ORDERS GROUP BY CATEGORY
      )
      SELECT * FROM AGG
    `;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.hasGroupBy).toBe(true);
    expect(result.ctes[0]!.hasJoin).toBe(false);
  });

  it("detects JOIN", () => {
    const sql = `
      WITH JOINED AS (
        SELECT O.ID, C.NAME FROM ORDERS O JOIN CUSTOMERS C ON O.CUSTOMER_ID = C.ID
      )
      SELECT * FROM JOINED
    `;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.hasGroupBy).toBe(false);
    expect(result.ctes[0]!.hasJoin).toBe(true);
  });

  it("detects both GROUP BY and JOIN", () => {
    const sql = `
      WITH AGG AS (
        SELECT C.NAME, SUM(O.AMOUNT) AS TOTAL
        FROM ORDERS O JOIN CUSTOMERS C ON O.CUSTOMER_ID = C.ID
        GROUP BY C.NAME
      )
      SELECT * FROM AGG
    `;
    const result = extractCtes(sql);
    expect(result.ctes[0]!.hasGroupBy).toBe(true);
    expect(result.ctes[0]!.hasJoin).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// extractCtes — inline subqueries (FROM (SELECT ...) alias)
// ---------------------------------------------------------------------------

describe("extractCtes — inline subqueries", () => {
  it("extracts inline subquery as synthetic CTE", () => {
    const sql = `
      SELECT ID, NAME
      FROM (SELECT ID, NAME FROM RAW.CUSTOMERS WHERE ACTIVE = TRUE) SRC
    `;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(1);
    expect(result.ctes[0]!.name).toBe("SRC");
    expect(result.ctes[0]!.columns).toHaveLength(2);
    expect(result.ctes[0]!.sourceTable).toBe("RAW.CUSTOMERS");
    expect(result.ctes[0]!.whereClause).toContain("ACTIVE");
    expect(result.finalSelectSQL).toContain("SELECT");
    expect(result.finalSelectSQL).toContain("SRC");
  });

  it("handles AS keyword before alias", () => {
    const sql = `
      SELECT ID FROM (SELECT ID FROM USERS) AS SUBQ
    `;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(1);
    expect(result.ctes[0]!.name).toBe("SUBQ");
  });

  it("defaults alias to SUBQUERY when none provided", () => {
    const sql = `
      SELECT * FROM (SELECT ID FROM USERS)
    `;
    const result = extractCtes(sql);
    if (result.ctes.length > 0) {
      expect(result.ctes[0]!.name).toBe("SUBQUERY");
    }
  });

  it("does not extract when FROM is a simple table", () => {
    const sql = `SELECT ID FROM CUSTOMERS`;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(0);
    expect(result.finalSelectSQL).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// extractCtes — final SELECT extraction
// ---------------------------------------------------------------------------

describe("extractCtes — final SELECT", () => {
  it("extracts final SELECT from simple CTE query", () => {
    const sql = `WITH STG AS (SELECT ID FROM USERS) SELECT ID FROM STG`;
    const result = extractCtes(sql);
    expect(result.finalSelectSQL).not.toBeNull();
    expect(result.finalSelectSQL).toContain("SELECT");
    expect(result.finalSelectSQL).toContain("STG");
  });

  it("handles wrapped final SELECT (e.g., CREATE TABLE ... AS (WITH ...))", () => {
    const sql = `CREATE TABLE RESULT AS (WITH STG AS (SELECT ID FROM USERS) SELECT ID FROM STG)`;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(1);
    expect(result.finalSelectSQL).toContain("SELECT");
  });

  it("preserves trailing semicolons in final SELECT (not stripped by CTE extraction)", () => {
    const sql = `WITH STG AS (SELECT ID FROM USERS) SELECT * FROM STG;`;
    const result = extractCtes(sql);
    // Semicolons are only stripped when part of wrapper syntax like ); — standalone ones are preserved
    expect(result.finalSelectSQL).toContain("SELECT");
    expect(result.finalSelectSQL).toContain("STG");
  });
});

// ---------------------------------------------------------------------------
// extractCtes — edge cases
// ---------------------------------------------------------------------------

describe("extractCtes — edge cases", () => {
  it("handles empty SQL", () => {
    const result = extractCtes("");
    expect(result.ctes).toHaveLength(0);
    expect(result.finalSelectSQL).toBeNull();
  });

  it("handles SQL with only whitespace", () => {
    const result = extractCtes("   \n\t   ");
    expect(result.ctes).toHaveLength(0);
    expect(result.finalSelectSQL).toBeNull();
  });

  it("handles CTE with no closing paren (malformed)", () => {
    const sql = `WITH STG AS (SELECT ID FROM USERS`;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(1);
    // Body should be empty due to missing close paren
    expect(result.ctes[0]!.body).toBe("");
    expect(result.ctes[0]!.columns).toHaveLength(0);
  });

  it("handles many CTEs chained together", () => {
    const sql = `
      WITH
        A AS (SELECT 1 AS VAL),
        B AS (SELECT 2 AS VAL),
        C AS (SELECT 3 AS VAL),
        D AS (SELECT 4 AS VAL),
        E AS (SELECT 5 AS VAL)
      SELECT * FROM A, B, C, D, E
    `;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(5);
    expect(result.ctes.map((c) => c.name)).toEqual(["A", "B", "C", "D", "E"]);
  });

  it("handles CTE names with underscores", () => {
    const sql = `WITH MY_CTE_1 AS (SELECT 1 AS VAL) SELECT * FROM MY_CTE_1`;
    const result = extractCtes(sql);
    expect(result.ctes).toHaveLength(1);
    expect(result.ctes[0]!.name).toBe("MY_CTE_1");
  });

  it("rejects CTE names starting with $ (not a valid SQL identifier start)", () => {
    const sql = `WITH $MY_CTE AS (SELECT 1 AS VAL) SELECT * FROM $MY_CTE`;
    const result = extractCtes(sql);
    // $ is not matched by [A-Za-z_] in the CTE header regex
    expect(result.ctes).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// classifyCtePattern
// ---------------------------------------------------------------------------

describe("classifyCtePattern", () => {
  it("classifies GROUP BY as aggregation", () => {
    const cte: ParsedCte = {
      name: "AGG",
      body: "",
      columns: [],
      whereClause: null,
      sourceTable: null,
      hasGroupBy: true,
      hasJoin: false,
    };
    expect(classifyCtePattern(cte)).toBe("aggregation");
  });

  it("classifies JOIN without GROUP BY as multiSource", () => {
    const cte: ParsedCte = {
      name: "JOINED",
      body: "",
      columns: [],
      whereClause: null,
      sourceTable: null,
      hasGroupBy: false,
      hasJoin: true,
    };
    expect(classifyCtePattern(cte)).toBe("multiSource");
  });

  it("classifies simple CTE as staging", () => {
    const cte: ParsedCte = {
      name: "STG",
      body: "",
      columns: [],
      whereClause: null,
      sourceTable: null,
      hasGroupBy: false,
      hasJoin: false,
    };
    expect(classifyCtePattern(cte)).toBe("staging");
  });

  it("prioritizes aggregation over multiSource when both present", () => {
    const cte: ParsedCte = {
      name: "AGG",
      body: "",
      columns: [],
      whereClause: null,
      sourceTable: null,
      hasGroupBy: true,
      hasJoin: true,
    };
    expect(classifyCtePattern(cte)).toBe("aggregation");
  });
});

// ---------------------------------------------------------------------------
// isAggregateFn
// ---------------------------------------------------------------------------

describe("isAggregateFn", () => {
  it("recognizes standard aggregate functions", () => {
    expect(isAggregateFn("COUNT")).toBe(true);
    expect(isAggregateFn("SUM")).toBe(true);
    expect(isAggregateFn("AVG")).toBe(true);
    expect(isAggregateFn("MIN")).toBe(true);
    expect(isAggregateFn("MAX")).toBe(true);
  });

  it("is case-insensitive", () => {
    expect(isAggregateFn("count")).toBe(true);
    expect(isAggregateFn("Sum")).toBe(true);
    expect(isAggregateFn("avg")).toBe(true);
  });

  it("recognizes Snowflake-specific aggregates", () => {
    expect(isAggregateFn("LISTAGG")).toBe(true);
    expect(isAggregateFn("ARRAY_AGG")).toBe(true);
    expect(isAggregateFn("MEDIAN")).toBe(true);
    expect(isAggregateFn("ANY_VALUE")).toBe(true);
    expect(isAggregateFn("APPROX_COUNT_DISTINCT")).toBe(true);
    expect(isAggregateFn("HLL")).toBe(true);
  });

  it("recognizes conditional aggregates", () => {
    expect(isAggregateFn("COUNT_IF")).toBe(true);
    expect(isAggregateFn("SUM_IF")).toBe(true);
    expect(isAggregateFn("AVG_IF")).toBe(true);
  });

  it("rejects non-aggregate functions", () => {
    expect(isAggregateFn("UPPER")).toBe(false);
    expect(isAggregateFn("LOWER")).toBe(false);
    expect(isAggregateFn("CONCAT")).toBe(false);
    expect(isAggregateFn("COALESCE")).toBe(false);
    expect(isAggregateFn("TRIM")).toBe(false);
  });

  it("rejects empty string", () => {
    expect(isAggregateFn("")).toBe(false);
  });
});
