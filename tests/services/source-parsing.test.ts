import { describe, expect, it } from "vitest";
import { parseSqlSourceRefs } from "../../src/services/pipelines/source-parsing.js";

// ---------------------------------------------------------------------------
// parseSqlSourceRefs — extracts source references from SQL FROM/JOIN clauses
// ---------------------------------------------------------------------------

describe("parseSqlSourceRefs", () => {
  // -----------------------------------------------------------------------
  // Happy path — simple table references
  // -----------------------------------------------------------------------

  it("extracts a single table reference", () => {
    const result = parseSqlSourceRefs("SELECT * FROM CUSTOMERS");
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[0]!.sourceStyle).toBe("table_name");
    expect(result.refs[0]!.alias).toBeNull();
    expect(result.refs[0]!.locationCandidates).toEqual([]);
  });

  it("extracts a schema-qualified table reference", () => {
    const result = parseSqlSourceRefs("SELECT * FROM RAW.CUSTOMERS");
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[0]!.locationCandidates).toEqual(["RAW"]);
    expect(result.refs[0]!.sourceStyle).toBe("table_name");
  });

  it("extracts a fully-qualified database.schema.table reference", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM PROD_DB.RAW_SCHEMA.CUSTOMERS"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    // locationCandidates is reversed: schema first, then database
    expect(result.refs[0]!.locationCandidates).toEqual([
      "RAW_SCHEMA",
      "PROD_DB",
    ]);
  });

  it("extracts a table with explicit alias using AS", () => {
    const result = parseSqlSourceRefs("SELECT c.ID FROM CUSTOMERS AS c");
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[0]!.alias).toBe("c");
  });

  it("extracts a table with implicit alias (no AS keyword)", () => {
    const result = parseSqlSourceRefs("SELECT c.ID FROM CUSTOMERS c");
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[0]!.alias).toBe("c");
  });

  // -----------------------------------------------------------------------
  // Multiple sources — comma-separated and JOINs
  // -----------------------------------------------------------------------

  it("extracts multiple comma-separated table references", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS, ORDERS, PRODUCTS"
    );
    expect(result.refs).toHaveLength(3);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[1]!.nodeName).toBe("ORDERS");
    expect(result.refs[2]!.nodeName).toBe("PRODUCTS");
  });

  it("extracts references from INNER JOIN", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS c INNER JOIN ORDERS o ON c.ID = o.CUSTOMER_ID"
    );
    expect(result.refs).toHaveLength(2);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[0]!.alias).toBe("c");
    expect(result.refs[1]!.nodeName).toBe("ORDERS");
    expect(result.refs[1]!.alias).toBe("o");
  });

  it("extracts references from LEFT JOIN", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS LEFT JOIN ORDERS ON CUSTOMERS.ID = ORDERS.CUSTOMER_ID"
    );
    expect(result.refs).toHaveLength(2);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[1]!.nodeName).toBe("ORDERS");
  });

  it("extracts references from multiple JOIN types", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS c " +
        "LEFT JOIN ORDERS o ON c.ID = o.CUSTOMER_ID " +
        "RIGHT JOIN RETURNS r ON o.ORDER_ID = r.ORDER_ID " +
        "FULL JOIN REFUNDS f ON r.RETURN_ID = f.RETURN_ID"
    );
    expect(result.refs).toHaveLength(4);
    expect(result.refs.map((r) => r.nodeName)).toEqual([
      "CUSTOMERS",
      "ORDERS",
      "RETURNS",
      "REFUNDS",
    ]);
  });

  it("extracts references from CROSS JOIN", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM DATES CROSS JOIN PRODUCTS"
    );
    expect(result.refs).toHaveLength(2);
    expect(result.refs[0]!.nodeName).toBe("DATES");
    expect(result.refs[1]!.nodeName).toBe("PRODUCTS");
  });

  // -----------------------------------------------------------------------
  // Coalesce ref() syntax
  // -----------------------------------------------------------------------

  it("extracts a Coalesce ref() source reference", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM {{ ref('STG', 'STG_CUSTOMERS') }}"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("STG_CUSTOMERS");
    expect(result.refs[0]!.locationName).toBe("STG");
    expect(result.refs[0]!.sourceStyle).toBe("coalesce_ref");
    expect(result.refs[0]!.locationCandidates).toEqual(["STG"]);
  });

  it("extracts a ref() with double quotes inside", () => {
    const result = parseSqlSourceRefs(
      'SELECT * FROM {{ ref("WORK", "DIM_CUSTOMER") }}'
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("DIM_CUSTOMER");
    expect(result.refs[0]!.locationName).toBe("WORK");
    expect(result.refs[0]!.sourceStyle).toBe("coalesce_ref");
  });

  it("extracts a ref() with alias", () => {
    const result = parseSqlSourceRefs(
      "SELECT c.ID FROM {{ ref('STG', 'STG_CUSTOMERS') }} AS c"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("STG_CUSTOMERS");
    expect(result.refs[0]!.alias).toBe("c");
  });

  it("extracts mixed ref() and table sources", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM {{ ref('STG', 'STG_ORDERS') }} o " +
        "JOIN RAW.CUSTOMERS c ON o.CUSTOMER_ID = c.ID"
    );
    expect(result.refs).toHaveLength(2);
    expect(result.refs[0]!.sourceStyle).toBe("coalesce_ref");
    expect(result.refs[0]!.nodeName).toBe("STG_ORDERS");
    expect(result.refs[1]!.sourceStyle).toBe("table_name");
    expect(result.refs[1]!.nodeName).toBe("CUSTOMERS");
  });

  // -----------------------------------------------------------------------
  // Quoted identifiers
  // -----------------------------------------------------------------------

  it("handles double-quoted identifiers", () => {
    const result = parseSqlSourceRefs(
      'SELECT * FROM "MY_SCHEMA"."My Table"'
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("My Table");
    expect(result.refs[0]!.locationCandidates).toEqual(["MY_SCHEMA"]);
  });

  it("handles double-quoted alias", () => {
    const result = parseSqlSourceRefs(
      'SELECT * FROM CUSTOMERS AS "cust"'
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.alias).toBe("cust");
  });

  // -----------------------------------------------------------------------
  // Clauses that terminate source capture
  // -----------------------------------------------------------------------

  it("stops at WHERE clause", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS WHERE ACTIVE = TRUE"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
  });

  it("stops at GROUP BY clause", () => {
    const result = parseSqlSourceRefs(
      "SELECT REGION, COUNT(*) FROM CUSTOMERS GROUP BY REGION"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
  });

  it("stops at ORDER BY clause", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS ORDER BY NAME"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
  });

  it("stops at HAVING clause", () => {
    const result = parseSqlSourceRefs(
      "SELECT REGION, COUNT(*) FROM CUSTOMERS GROUP BY REGION HAVING COUNT(*) > 5"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
  });

  it("stops at LIMIT clause", () => {
    const result = parseSqlSourceRefs("SELECT * FROM CUSTOMERS LIMIT 10");
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
  });

  it("extracts sources from both sides of UNION", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS UNION SELECT * FROM ARCHIVED_CUSTOMERS"
    );
    expect(result.refs).toHaveLength(2);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[1]!.nodeName).toBe("ARCHIVED_CUSTOMERS");
  });

  it("stops at QUALIFY clause (Snowflake)", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY TS DESC) = 1"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
  });

  // -----------------------------------------------------------------------
  // Edge cases — empty / no FROM clause
  // -----------------------------------------------------------------------

  it("returns empty refs for SQL without FROM clause", () => {
    const result = parseSqlSourceRefs("SELECT 1 AS ONE");
    expect(result.refs).toHaveLength(0);
    expect(result.fromClause).toBe("");
  });

  it("returns empty refs for empty string", () => {
    const result = parseSqlSourceRefs("");
    expect(result.refs).toHaveLength(0);
    expect(result.fromClause).toBe("");
  });

  // -----------------------------------------------------------------------
  // Subqueries (parenthesized) are skipped
  // -----------------------------------------------------------------------

  it("skips subquery in FROM clause", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM (SELECT ID FROM CUSTOMERS) sub"
    );
    // Subqueries wrapped in parens are skipped (return null from parseSqlSourceSegment)
    expect(result.refs).toHaveLength(0);
  });

  it("extracts table ref alongside subquery", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM (SELECT ID FROM RAW_DATA) sub JOIN ORDERS o ON sub.ID = o.CUSTOMER_ID"
    );
    // Subquery is skipped, but ORDERS is extracted
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("ORDERS");
  });

  // -----------------------------------------------------------------------
  // Relation position tracking (relationStart / relationEnd)
  // -----------------------------------------------------------------------

  it("tracks relation start and end positions", () => {
    const result = parseSqlSourceRefs("SELECT * FROM CUSTOMERS");
    expect(result.refs).toHaveLength(1);
    const ref = result.refs[0]!;
    expect(ref.relationStart).toBeGreaterThanOrEqual(0);
    expect(ref.relationEnd).toBeGreaterThan(ref.relationStart);
    // The positions should mark the relation token within the fromClause
    expect(
      result.fromClause.slice(ref.relationStart, ref.relationEnd)
    ).toContain("CUSTOMERS");
  });

  // -----------------------------------------------------------------------
  // Case insensitivity for SQL keywords
  // -----------------------------------------------------------------------

  it("handles lowercase SQL keywords", () => {
    const result = parseSqlSourceRefs(
      "select * from customers c left join orders o on c.id = o.customer_id"
    );
    expect(result.refs).toHaveLength(2);
    expect(result.refs[0]!.nodeName).toBe("customers");
    expect(result.refs[1]!.nodeName).toBe("orders");
  });

  it("handles mixed-case SQL keywords", () => {
    const result = parseSqlSourceRefs(
      "Select * From CUSTOMERS Left Join ORDERS On CUSTOMERS.ID = ORDERS.CUSTOMER_ID"
    );
    expect(result.refs).toHaveLength(2);
  });

  // -----------------------------------------------------------------------
  // Incomplete or malformed ref() syntax
  // -----------------------------------------------------------------------

  it("skips incomplete ref() with missing closing braces", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM {{ ref('STG', 'NODE')"
    );
    // Missing }}, should not produce a coalesce_ref, may produce nothing
    expect(result.refs).toHaveLength(0);
  });

  // -----------------------------------------------------------------------
  // USING clause (does not terminate JOIN source capture incorrectly)
  // -----------------------------------------------------------------------

  it("handles JOIN with USING clause", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS JOIN ORDERS USING (CUSTOMER_ID)"
    );
    expect(result.refs).toHaveLength(2);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[1]!.nodeName).toBe("ORDERS");
  });

  // -----------------------------------------------------------------------
  // Whitespace and formatting variations
  // -----------------------------------------------------------------------

  it("handles multi-line SQL with varied indentation", () => {
    const sql = `
      SELECT
        c.ID,
        o.AMOUNT
      FROM
        CUSTOMERS c
      INNER JOIN
        ORDERS o
          ON c.ID = o.CUSTOMER_ID
      WHERE
        c.ACTIVE = TRUE
    `;
    const result = parseSqlSourceRefs(sql);
    expect(result.refs).toHaveLength(2);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[0]!.alias).toBe("c");
    expect(result.refs[1]!.nodeName).toBe("ORDERS");
    expect(result.refs[1]!.alias).toBe("o");
  });

  it("handles extra whitespace between tokens", () => {
    const result = parseSqlSourceRefs(
      "SELECT  *  FROM   CUSTOMERS   AS   c"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[0]!.alias).toBe("c");
  });

  // -----------------------------------------------------------------------
  // fromClause extraction
  // -----------------------------------------------------------------------

  it("populates fromClause on the result", () => {
    const result = parseSqlSourceRefs(
      "SELECT ID FROM CUSTOMERS WHERE ACTIVE = TRUE"
    );
    expect(result.fromClause).toBeTruthy();
    expect(result.fromClause).toContain("FROM");
    expect(result.fromClause).toContain("CUSTOMERS");
  });

  // -----------------------------------------------------------------------
  // NATURAL JOIN
  // -----------------------------------------------------------------------

  it("extracts references from NATURAL JOIN", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM CUSTOMERS NATURAL JOIN ORDERS"
    );
    expect(result.refs).toHaveLength(2);
    expect(result.refs[0]!.nodeName).toBe("CUSTOMERS");
    expect(result.refs[1]!.nodeName).toBe("ORDERS");
  });

  // -----------------------------------------------------------------------
  // ref() with extra whitespace
  // -----------------------------------------------------------------------

  it("extracts ref() with extra whitespace inside braces", () => {
    const result = parseSqlSourceRefs(
      "SELECT * FROM {{   ref(  'STG' ,  'STG_CUSTOMERS'  )   }}"
    );
    expect(result.refs).toHaveLength(1);
    expect(result.refs[0]!.nodeName).toBe("STG_CUSTOMERS");
    expect(result.refs[0]!.sourceStyle).toBe("coalesce_ref");
  });
});
