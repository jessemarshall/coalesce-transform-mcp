import { describe, expect, it } from "vitest";
import {
	appendLimitToJoinCondition,
	diffLimit,
	extractTrailingLimit,
	stripTrailingLimit,
} from "../../src/services/templates/limit-diff.js";

// ── extractTrailingLimit ────────────────────────────────────────────────────

describe("extractTrailingLimit", () => {
	it("extracts a numeric LIMIT at end of statement", () => {
		expect(extractTrailingLimit(`SELECT * FROM T LIMIT 10`)).toBe(10);
	});

	it("extracts LIMIT before a trailing semicolon", () => {
		expect(extractTrailingLimit(`SELECT * FROM T LIMIT 100;`)).toBe(100);
	});

	it("extracts LIMIT inside an INSERT envelope", () => {
		expect(extractTrailingLimit(
			`INSERT INTO X (cols) (SELECT * FROM T LIMIT 5);`,
		)).toBe(5);
	});

	it("returns null when no LIMIT", () => {
		expect(extractTrailingLimit(`SELECT * FROM T`)).toBeNull();
	});

	it("ignores LIMIT inside a string literal", () => {
		expect(extractTrailingLimit(
			`SELECT * FROM T WHERE label = 'LIMIT 100'`,
		)).toBeNull();
	});

	it("ignores LIMIT inside a line comment", () => {
		expect(extractTrailingLimit(
			`SELECT * FROM T -- LIMIT 100\nWHERE x = 1`,
		)).toBeNull();
	});

	it("returns the OUTER LIMIT when a CTE/subquery has its own", () => {
		// Inner LIMIT is inside parens; the outer one wins.
		expect(extractTrailingLimit(
			`SELECT * FROM (SELECT * FROM X LIMIT 100) Y LIMIT 10`,
		)).toBe(10);
	});

	it("returns null for `LIMITED` (longer word) — word-boundary check", () => {
		expect(extractTrailingLimit(`SELECT * FROM T WHERE LIMITED = 1`)).toBeNull();
	});

	// ── Pass-1 review fixes ─────────────────────────────────────────────────

	it("does NOT false-match a CTE's internal LIMIT as the outer LIMIT", () => {
		// Pass-1 regression: paren-recursion fired on every LIMIT-less
		// SQL, even plain SELECTs with a CTE LIMIT.
		expect(extractTrailingLimit(
			`WITH cte AS (SELECT a FROM x LIMIT 5) SELECT * FROM cte`,
		)).toBeNull();
	});

	it("does NOT false-match a subquery LIMIT in a WHERE clause", () => {
		expect(extractTrailingLimit(
			`SELECT * FROM T WHERE k IN (SELECT k FROM x LIMIT 5)`,
		)).toBeNull();
	});

	it("does NOT false-match a scalar subquery LIMIT in a SELECT projection", () => {
		expect(extractTrailingLimit(
			`SELECT (SELECT max(x) FROM y LIMIT 1) FROM t`,
		)).toBeNull();
	});

	it("DOES still recurse one paren-depth into INSERT envelopes", () => {
		// The INSERT-shape gate must still allow the legitimate INSERT
		// envelope case to peel one paren.
		expect(extractTrailingLimit(
			`INSERT INTO X (cols) (SELECT * FROM T LIMIT 5);`,
		)).toBe(5);
	});

	it("DOES recurse for MERGE statements (same envelope shape)", () => {
		expect(extractTrailingLimit(
			`MERGE INTO X USING (SELECT * FROM T LIMIT 5) S ON X.k = S.k`,
		)).toBe(5);
	});

	it("throws on `LIMIT N OFFSET M` rather than silently dropping the offset", () => {
		expect(() =>
			extractTrailingLimit(`SELECT * FROM T LIMIT 5 OFFSET 10`),
		).toThrow(/OFFSET/);
	});

	it("throws on MySQL `LIMIT N, M` rather than silently dropping the count", () => {
		expect(() =>
			extractTrailingLimit(`SELECT * FROM T LIMIT 100, 50`),
		).toThrow(/OFFSET|row-range/);
	});

	// ── Pass-2 review fixes (comment-aware bounding) ────────────────────────

	it("returns the value when an inline block comment follows it", () => {
		// Pass-2 regression: `parseLimitClauseValue` was not comment-aware
		// and would return null for `LIMIT 5/* c */`, silently dropping
		// the user's LIMIT.
		expect(extractTrailingLimit(`SELECT * FROM T LIMIT 5/* c */`)).toBe(5);
	});

	it("still throws on OFFSET when shielded by a block comment between value and OFFSET", () => {
		// Pass-2 regression: comment-shielded OFFSET silently passed
		// through as null instead of triggering the rejection.
		expect(() =>
			extractTrailingLimit(`SELECT * FROM T LIMIT 5 /*c*/ OFFSET 10`),
		).toThrow(/OFFSET/);
	});

	it("still throws on OFFSET when shielded by a line comment between value and OFFSET", () => {
		expect(() =>
			extractTrailingLimit(`SELECT * FROM T LIMIT 5 -- c\nOFFSET 10`),
		).toThrow(/OFFSET/);
	});
});

// ── stripTrailingLimit ──────────────────────────────────────────────────────

describe("stripTrailingLimit", () => {
	it("removes a trailing LIMIT", () => {
		expect(stripTrailingLimit(
			`FROM {{ ref('L', 'T') }} T\nLIMIT 10`,
		)).toBe(`FROM {{ ref('L', 'T') }} T`);
	});

	it("is a no-op when no LIMIT is present", () => {
		const jc = `FROM {{ ref('L', 'T') }} T`;
		expect(stripTrailingLimit(jc)).toBe(jc);
	});

	it("doesn't strip LIMIT inside a parenthesized subquery", () => {
		const jc =
			`FROM {{ ref('L', 'T') }} T WHERE k IN (SELECT k FROM x LIMIT 5)`;
		expect(stripTrailingLimit(jc)).toBe(jc);
	});
});

// ── appendLimitToJoinCondition ──────────────────────────────────────────────

describe("appendLimitToJoinCondition", () => {
	it("appends a LIMIT when none exists", () => {
		expect(appendLimitToJoinCondition(
			`FROM {{ ref('L', 'T') }} T`,
			10,
		)).toBe(`FROM {{ ref('L', 'T') }} T\nLIMIT 10`);
	});

	it("replaces an existing trailing LIMIT", () => {
		expect(appendLimitToJoinCondition(
			`FROM {{ ref('L', 'T') }} T\nLIMIT 100`,
			10,
		)).toBe(`FROM {{ ref('L', 'T') }} T\nLIMIT 10`);
	});

	it("normalizes trailing whitespace before appending", () => {
		expect(appendLimitToJoinCondition(
			`FROM {{ ref('L', 'T') }} T   \n\n`,
			5,
		)).toBe(`FROM {{ ref('L', 'T') }} T\nLIMIT 5`);
	});

	it("doesn't produce a leading newline when the base is empty (pass-1 fix)", () => {
		expect(appendLimitToJoinCondition("", 10)).toBe(`LIMIT 10`);
		expect(appendLimitToJoinCondition("   \n\n", 10)).toBe(`LIMIT 10`);
	});
});

// ── diffLimit ───────────────────────────────────────────────────────────────

describe("diffLimit", () => {
	const baseJC = `FROM {{ ref('L', 'T') }} T`;
	const noTailClauses = { groupByAll: false, orderby: false };

	it("identical when both sides have no LIMIT", () => {
		expect(diffLimit(`SELECT * FROM "DB"."L"."T" T`, baseJC, noTailClauses))
			.toEqual({ kind: "identical" });
	});

	it("identical when both sides have the same LIMIT", () => {
		expect(diffLimit(
			`SELECT * FROM "DB"."L"."T" T LIMIT 10`,
			`${baseJC}\nLIMIT 10`,
			noTailClauses,
		)).toEqual({ kind: "identical" });
	});

	it("added when user introduces a LIMIT", () => {
		expect(diffLimit(
			`SELECT * FROM "DB"."L"."T" T LIMIT 10`,
			baseJC,
			noTailClauses,
		)).toEqual({ kind: "added", newLimit: 10, warnsClobberByTailClause: false });
	});

	it("changed when user updates an existing LIMIT", () => {
		expect(diffLimit(
			`SELECT * FROM "DB"."L"."T" T LIMIT 50`,
			`${baseJC}\nLIMIT 10`,
			noTailClauses,
		)).toEqual({ kind: "changed", newLimit: 50, warnsClobberByTailClause: false });
	});

	it("removed when user drops an existing LIMIT", () => {
		expect(diffLimit(
			`SELECT * FROM "DB"."L"."T" T`,
			`${baseJC}\nLIMIT 10`,
			noTailClauses,
		)).toEqual({ kind: "removed" });
	});

	it("warns about tail-clause clobber when groupByAll is set", () => {
		const result = diffLimit(
			`SELECT * FROM "DB"."L"."T" T LIMIT 10`,
			baseJC,
			{ groupByAll: true, orderby: false },
		);
		expect(result.kind).toBe("added");
		if (result.kind === "added") {
			expect(result.warnsClobberByTailClause).toBe(true);
		}
	});

	it("warns about tail-clause clobber when orderby is set", () => {
		const result = diffLimit(
			`SELECT * FROM "DB"."L"."T" T LIMIT 10`,
			baseJC,
			{ groupByAll: false, orderby: true },
		);
		expect(result.kind).toBe("added");
		if (result.kind === "added") {
			expect(result.warnsClobberByTailClause).toBe(true);
		}
	});

	it("returns kind: 'unsupported' for OFFSET clauses (pass-1 fix)", () => {
		const result = diffLimit(
			`SELECT * FROM "DB"."L"."T" T LIMIT 5 OFFSET 10`,
			baseJC,
			noTailClauses,
		);
		expect(result.kind).toBe("unsupported");
		if (result.kind === "unsupported") {
			expect(result.reason).toMatch(/OFFSET/);
		}
	});

	it("returns kind: 'unsupported' for MySQL `LIMIT N, M` (pass-1 fix)", () => {
		const result = diffLimit(
			`SELECT * FROM "DB"."L"."T" T LIMIT 100, 50`,
			baseJC,
			noTailClauses,
		);
		expect(result.kind).toBe("unsupported");
	});
});
