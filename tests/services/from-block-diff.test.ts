import { describe, expect, it } from "vitest";
import {
	diffFromBlock,
	extractFromBlock,
	extractFromBlockSources,
	normalizeForCompare,
	rewriteTableRefsToCoalesceRefs,
} from "../../src/services/templates/from-block-diff.js";

// ── extractFromBlock ────────────────────────────────────────────────────────

describe("extractFromBlock", () => {
	it("extracts a simple FROM clause from a SELECT", () => {
		const sql = `SELECT a, b FROM "DB"."LOC"."T" T`;
		expect(extractFromBlock(sql)).toBe(`FROM "DB"."LOC"."T" T`);
	});

	it("includes the WHERE clause", () => {
		const sql = `SELECT a FROM "DB"."LOC"."T" T WHERE T.x > 5`;
		expect(extractFromBlock(sql)).toBe(`FROM "DB"."LOC"."T" T WHERE T.x > 5`);
	});

	it("stops at GROUP BY", () => {
		const sql = `SELECT a FROM "DB"."LOC"."T" T WHERE x > 5 GROUP BY a`;
		expect(extractFromBlock(sql)).toBe(`FROM "DB"."LOC"."T" T WHERE x > 5`);
	});

	it("stops at ORDER BY", () => {
		const sql = `SELECT a FROM "DB"."LOC"."T" T ORDER BY a`;
		expect(extractFromBlock(sql)).toBe(`FROM "DB"."LOC"."T" T`);
	});

	it("stops at LIMIT (so iteration-4 LIMIT rejection sees it cleanly)", () => {
		const sql = `SELECT a FROM "DB"."LOC"."T" T LIMIT 10`;
		expect(extractFromBlock(sql)).toBe(`FROM "DB"."LOC"."T" T`);
	});

	it("handles INSERT envelope (FROM is inside parens)", () => {
		const sql = `INSERT INTO "DB"."LOC"."N" (cols) (SELECT a FROM "DB"."LOC"."T" T LIMIT 10);`;
		expect(extractFromBlock(sql)).toBe(`FROM "DB"."LOC"."T" T`);
	});

	it("handles JOIN + ON inside the FROM block", () => {
		const sql =
			`SELECT a FROM "DB"."LOC"."A" A LEFT JOIN "DB"."LOC"."B" B ON A.k = B.k WHERE A.x > 0`;
		expect(extractFromBlock(sql)).toBe(
			`FROM "DB"."LOC"."A" A LEFT JOIN "DB"."LOC"."B" B ON A.k = B.k WHERE A.x > 0`,
		);
	});

	it("returns undefined when no FROM is present", () => {
		expect(extractFromBlock("SHOW TABLES")).toBeUndefined();
		expect(extractFromBlock("CREATE TABLE FOO (x NUMBER)")).toBeUndefined();
	});
});

// ── extractFromBlockSources ─────────────────────────────────────────────────

describe("extractFromBlockSources", () => {
	it("extracts a single three-part identifier", () => {
		const block = `FROM "DB"."LOC"."T" T`;
		expect(extractFromBlockSources(block)).toEqual([
			{ locationName: "LOC", nodeName: "T" },
		]);
	});

	it("extracts multiple three-part identifiers (FROM + JOIN)", () => {
		const block = `FROM "DB"."L"."A" A LEFT JOIN "DB"."L"."B" B ON A.k = B.k`;
		expect(extractFromBlockSources(block)).toEqual([
			{ locationName: "L", nodeName: "A" },
			{ locationName: "L", nodeName: "B" },
		]);
	});

	it("extracts {{ ref('LOC', 'NAME') }} placeholders", () => {
		const block = `FROM {{ ref('ANALYTICS', 'STG_ORDERS') }} STG_ORDERS`;
		expect(extractFromBlockSources(block)).toEqual([
			{ locationName: "ANALYTICS", nodeName: "STG_ORDERS" },
		]);
	});

	it("dedupes when the same source appears in both ref-form and three-part form", () => {
		const block =
			`FROM {{ ref('L', 'N') }} N CROSS JOIN "DB"."L"."N" N2`;
		expect(extractFromBlockSources(block)).toEqual([
			{ locationName: "L", nodeName: "N" },
		]);
	});

	it("ignores bare identifiers (Coalesce always quotes predecessor refs)", () => {
		// `FROM CUSTOMER` shouldn't match — only fully-qualified refs do.
		const block = `FROM CUSTOMER C`;
		expect(extractFromBlockSources(block)).toEqual([]);
	});
});

// ── rewriteTableRefsToCoalesceRefs ──────────────────────────────────────────

describe("rewriteTableRefsToCoalesceRefs", () => {
	it("rewrites three-part identifiers to ref() placeholders", () => {
		const block = `FROM "DB"."LOC"."NAME" NAME`;
		expect(rewriteTableRefsToCoalesceRefs(block)).toBe(
			`FROM {{ ref('LOC', 'NAME') }} NAME`,
		);
	});

	it("rewrites multiple three-part refs in a JOIN block", () => {
		const block = `FROM "DB"."L"."A" A LEFT JOIN "DB"."L"."B" B ON A.k = B.k`;
		expect(rewriteTableRefsToCoalesceRefs(block)).toBe(
			`FROM {{ ref('L', 'A') }} A LEFT JOIN {{ ref('L', 'B') }} B ON A.k = B.k`,
		);
	});

	it("leaves existing ref() placeholders alone", () => {
		const block = `FROM {{ ref('L', 'A') }} A`;
		expect(rewriteTableRefsToCoalesceRefs(block)).toBe(block);
	});

	it("doesn't accidentally rewrite the schema portion of a three-part ref via the two-part regex", () => {
		// Edge: the two-part regex `"X"."Y"` could match the inner schema.table
		// portion if not properly anchored. This test locks the precedence.
		const block = `FROM "JESSE_DEV"."ANALYTICS"."STG_ORDERS" STG_ORDERS`;
		expect(rewriteTableRefsToCoalesceRefs(block)).toBe(
			`FROM {{ ref('ANALYTICS', 'STG_ORDERS') }} STG_ORDERS`,
		);
	});
});

// ── normalizeForCompare ─────────────────────────────────────────────────────

describe("normalizeForCompare", () => {
	it("treats whitespace-only differences as equal", () => {
		const a = `FROM "L"."N" N\n  WHERE N.x = 1`;
		const b = `FROM   "L"."N"   N\nWHERE   N.x = 1`;
		expect(normalizeForCompare(a)).toBe(normalizeForCompare(b));
	});

	it("treats keyword case differences as equal", () => {
		const upper = `FROM "L"."N" N WHERE N.x = 1`;
		const lower = `from "L"."N" N where N.x = 1`;
		expect(normalizeForCompare(upper)).toBe(normalizeForCompare(lower));
	});

	it("preserves quoted identifier casing (Snowflake is case-sensitive there)", () => {
		const a = `"L"."MyName"`;
		const b = `"L"."myname"`;
		expect(normalizeForCompare(a)).not.toBe(normalizeForCompare(b));
	});

	it("strips line comments before comparison", () => {
		const a = `FROM T  -- a comment\nWHERE x = 1`;
		const b = `FROM T WHERE x = 1`;
		expect(normalizeForCompare(a)).toBe(normalizeForCompare(b));
	});
});

// ── diffFromBlock ───────────────────────────────────────────────────────────

describe("diffFromBlock", () => {
	const baseJoinCondition = `FROM {{ ref('L', 'STG') }} STG`;

	it("returns identical when the user SQL matches the existing joinCondition", () => {
		const userSql = `SELECT a FROM "DB"."L"."STG" STG`;
		expect(diffFromBlock(userSql, baseJoinCondition)).toEqual({ kind: "identical" });
	});

	it("returns identical for whitespace/case-only edits", () => {
		const userSql = `SELECT a from "DB"."L"."STG"   STG`;
		expect(diffFromBlock(userSql, baseJoinCondition)).toEqual({ kind: "identical" });
	});

	it("returns whereOrJoinEdit when a WHERE is added (no source change)", () => {
		const userSql = `SELECT a FROM "DB"."L"."STG" STG WHERE STG.status = 'A'`;
		const result = diffFromBlock(userSql, baseJoinCondition);
		expect(result.kind).toBe("whereOrJoinEdit");
		if (result.kind === "whereOrJoinEdit") {
			expect(result.newJoinCondition).toContain("WHERE STG.status = 'A'");
			expect(result.newJoinCondition).toContain("{{ ref('L', 'STG') }}");
		}
	});

	it("returns whereOrJoinEdit when an ON condition changes", () => {
		const existingJC =
			`FROM {{ ref('L', 'A') }} A LEFT JOIN {{ ref('L', 'B') }} B ON A.k = B.k`;
		const userSql =
			`SELECT * FROM "DB"."L"."A" A LEFT JOIN "DB"."L"."B" B ON A.id = B.fk`;
		const result = diffFromBlock(userSql, existingJC);
		expect(result.kind).toBe("whereOrJoinEdit");
		if (result.kind === "whereOrJoinEdit") {
			expect(result.newJoinCondition).toContain("ON A.id = B.fk");
		}
	});

	it("returns newSource when a JOIN against a new table appears", () => {
		const userSql =
			`SELECT * FROM "DB"."L"."STG" STG LEFT JOIN "DB"."L"."NEW_TABLE" NEW_TABLE ON STG.k = NEW_TABLE.k`;
		const result = diffFromBlock(userSql, baseJoinCondition);
		expect(result.kind).toBe("newSource");
		if (result.kind === "newSource") {
			expect(result.added).toEqual([{ locationName: "L", nodeName: "NEW_TABLE" }]);
		}
	});

	it("returns removedSource when an existing source disappears", () => {
		const existingJC =
			`FROM {{ ref('L', 'A') }} A LEFT JOIN {{ ref('L', 'B') }} B ON A.k = B.k`;
		const userSql = `SELECT * FROM "DB"."L"."A" A`;
		const result = diffFromBlock(userSql, existingJC);
		expect(result.kind).toBe("removedSource");
		if (result.kind === "removedSource") {
			expect(result.removed).toEqual([{ locationName: "L", nodeName: "B" }]);
		}
	});

	it("returns identical when the user SQL has no FROM (e.g. a CREATE TABLE input)", () => {
		const userSql = `CREATE TABLE FOO (x NUMBER)`;
		expect(diffFromBlock(userSql, baseJoinCondition)).toEqual({ kind: "identical" });
	});

	it("ignores LIMIT additions (handled by iteration 4 rejection separately)", () => {
		const userSql = `SELECT a FROM "DB"."L"."STG" STG LIMIT 10`;
		expect(diffFromBlock(userSql, baseJoinCondition)).toEqual({ kind: "identical" });
	});
});

// ── Pass-1 review fixes: CTE + multi-statement + comment stripping ─────────

describe("diffFromBlock: review-pass-1 fixes", () => {
	it("returns kind: 'unsupported' for CTE-shaped (WITH ...) input", () => {
		const userSql =
			`WITH cte AS (SELECT a FROM "DB"."L"."INNER")\nSELECT * FROM "DB"."L"."OUTER" O JOIN cte ON O.k = cte.k`;
		const result = diffFromBlock(userSql, `FROM {{ ref('L', 'OUTER') }} O`);
		expect(result.kind).toBe("unsupported");
		if (result.kind === "unsupported") {
			expect(result.reason).toMatch(/CTE/i);
		}
	});

	it("doesn't slurp a trailing statement after a top-level semicolon", () => {
		const userSql = `SELECT a FROM "DB"."L"."STG" STG; SELECT b FROM "DB"."L"."STG" STG`;
		const result = diffFromBlock(userSql, `FROM {{ ref('L', 'STG') }} STG`);
		// Should be identical — the trailing statement is dropped.
		expect(result).toEqual({ kind: "identical" });
	});

	it("strips line comments from newJoinCondition before persisting", () => {
		const userSql =
			`SELECT a FROM "DB"."L"."STG" STG -- a stray comment\nWHERE STG.x = 1`;
		const result = diffFromBlock(userSql, `FROM {{ ref('L', 'STG') }} STG`);
		expect(result.kind).toBe("whereOrJoinEdit");
		if (result.kind === "whereOrJoinEdit") {
			expect(result.newJoinCondition).not.toContain("-- a stray comment");
			expect(result.newJoinCondition).toContain("WHERE STG.x = 1");
		}
	});

	it("strips block comments from newJoinCondition before persisting", () => {
		const userSql =
			`SELECT a FROM "DB"."L"."STG" STG /* block */ WHERE STG.x = 1`;
		const result = diffFromBlock(userSql, `FROM {{ ref('L', 'STG') }} STG`);
		expect(result.kind).toBe("whereOrJoinEdit");
		if (result.kind === "whereOrJoinEdit") {
			expect(result.newJoinCondition).not.toContain("block");
		}
	});

	it("preserves `--` and `/*` inside string literals when stripping comments", () => {
		// Pass-2 regression: the original `stripCommentsForPersist` was a
		// raw regex that mis-treated `--` / `/*` inside strings as a
		// comment start, truncating the literal and corrupting joinCondition.
		const userSql =
			`SELECT a FROM "DB"."L"."STG" STG WHERE STG.label = 'pre -- not a comment' AND STG.note = 'has /* fake block */ inside'`;
		const result = diffFromBlock(userSql, `FROM {{ ref('L', 'STG') }} STG`);
		expect(result.kind).toBe("whereOrJoinEdit");
		if (result.kind === "whereOrJoinEdit") {
			expect(result.newJoinCondition).toContain("'pre -- not a comment'");
			expect(result.newJoinCondition).toContain("'has /* fake block */ inside'");
		}
	});
});

describe("extractFromBlock: review-pass-1 fixes", () => {
	it("ignores `(` inside a string literal when looking for the INSERT envelope", () => {
		// No real top-level SELECT, but the SQL has `(` chars inside string
		// literals. Should NOT recurse into them and find a fake SELECT.
		const sql = `EXEC PROCEDURE 'OPEN(' || foo || ')'`;
		expect(extractFromBlock(sql)).toBeUndefined();
	});

	it("stops at top-level `;` (no slurping past statement boundary)", () => {
		const sql = `SELECT a FROM "DB"."L"."T" T; SELECT b FROM other`;
		expect(extractFromBlock(sql)).toBe(`FROM "DB"."L"."T" T`);
	});

	it("ignores `;` inside a string literal (only true top-level terminates)", () => {
		// The `;` inside `'abc;def'` must NOT terminate the from-block,
		// otherwise the WHERE comparison would be truncated to
		// `WHERE x.k = 'abc` and the cloud node would receive a broken
		// joinCondition.
		const sql = `SELECT a FROM "DB"."L"."T" T WHERE T.k = 'abc;def' AND T.flag = 1`;
		const block = extractFromBlock(sql);
		expect(block).toContain("'abc;def'");
		expect(block).toContain("AND T.flag = 1");
	});

	it("ignores `;` inside a parenthesized subquery (paren-depth tracking)", () => {
		// `;` is invalid Snowflake mid-statement, but the parser must be
		// robust against it; if a future SQL dialect or comment-stripped
		// input slips one in, the outer WHERE shouldn't be truncated.
		//
		// Important: the `;` here is OUTSIDE any string literal, so
		// string-handling can't account for it being skipped — only
		// paren-depth tracking can. If `walkSqlTopLevel`'s `trackParens`
		// arm regressed, this test would fail.
		const sql = `SELECT a FROM "DB"."L"."T" T WHERE T.k IN (SELECT b FROM x WHERE x.f = 1 ; bogus) AND T.flag = 1`;
		const block = extractFromBlock(sql);
		expect(block).toContain("AND T.flag = 1");
	});
});
