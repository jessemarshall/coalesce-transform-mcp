import { describe, expect, it } from "vitest";
import {
	diffTailClauses,
	extractGroupByClause,
	extractOrderByClause,
	parseOrderByItems,
} from "../../src/services/templates/tail-clauses-diff.js";

// ── extractGroupByClause ────────────────────────────────────────────────────

describe("extractGroupByClause", () => {
	it("extracts a single-column GROUP BY", () => {
		expect(extractGroupByClause(`SELECT a, COUNT(*) FROM T GROUP BY a`)).toBe("a");
	});

	it("extracts a multi-column GROUP BY", () => {
		expect(extractGroupByClause(`SELECT a, b FROM T GROUP BY a, b`)).toBe("a, b");
	});

	it("stops at HAVING / ORDER BY / LIMIT", () => {
		expect(extractGroupByClause(`SELECT a FROM T GROUP BY a HAVING COUNT(*) > 1`))
			.toBe("a");
		expect(extractGroupByClause(`SELECT a FROM T GROUP BY a ORDER BY a`)).toBe("a");
		expect(extractGroupByClause(`SELECT a FROM T GROUP BY a LIMIT 10`)).toBe("a");
	});

	it("returns undefined when no GROUP BY", () => {
		expect(extractGroupByClause(`SELECT * FROM T`)).toBeUndefined();
	});

	it("doesn't match a `GROUP` token without a following `BY`", () => {
		expect(extractGroupByClause(`SELECT * FROM T WHERE GROUP_ID = 1`)).toBeUndefined();
	});
});

// ── extractOrderByClause ────────────────────────────────────────────────────

describe("extractOrderByClause", () => {
	it("extracts a single column", () => {
		expect(extractOrderByClause(`SELECT * FROM T ORDER BY a`)).toBe("a");
	});

	it("extracts multiple columns with directions", () => {
		expect(extractOrderByClause(`SELECT * FROM T ORDER BY a DESC, b ASC, c`))
			.toBe("a DESC, b ASC, c");
	});

	it("stops at LIMIT and `;`", () => {
		expect(extractOrderByClause(`SELECT * FROM T ORDER BY a LIMIT 10`)).toBe("a");
		expect(extractOrderByClause(`SELECT * FROM T ORDER BY a; SELECT b`)).toBe("a");
	});

	it("returns undefined when no ORDER BY", () => {
		expect(extractOrderByClause(`SELECT * FROM T`)).toBeUndefined();
	});
});

// ── parseOrderByItems ───────────────────────────────────────────────────────

describe("parseOrderByItems", () => {
	it("parses a single column with default direction (asc)", () => {
		expect(parseOrderByItems("a")).toEqual({
			items: [{ sortColName: "A", sortOrder: "asc" }],
			unsupportedExpressions: [],
		});
	});

	it("parses a single column with explicit DESC", () => {
		expect(parseOrderByItems("a DESC")).toEqual({
			items: [{ sortColName: "A", sortOrder: "desc" }],
			unsupportedExpressions: [],
		});
	});

	it("parses multiple columns mixing directions", () => {
		expect(parseOrderByItems("a DESC, b ASC, c")).toEqual({
			items: [
				{ sortColName: "A", sortOrder: "desc" },
				{ sortColName: "B", sortOrder: "asc" },
				{ sortColName: "C", sortOrder: "asc" },
			],
			unsupportedExpressions: [],
		});
	});

	it("strips quotes around identifiers", () => {
		expect(parseOrderByItems('"My Col" DESC')).toEqual({
			items: [{ sortColName: "MY COL", sortOrder: "desc" }],
			unsupportedExpressions: [],
		});
	});

	it("uppercases column names to match Coalesce storage convention", () => {
		expect(parseOrderByItems("MyCol")).toEqual({
			items: [{ sortColName: "MYCOL", sortOrder: "asc" }],
			unsupportedExpressions: [],
		});
	});

	it("uses the trailing identifier when the expression is qualified", () => {
		expect(parseOrderByItems("T.col1, x.col2 DESC")).toEqual({
			items: [
				{ sortColName: "COL1", sortOrder: "asc" },
				{ sortColName: "COL2", sortOrder: "desc" },
			],
			unsupportedExpressions: [],
		});
	});

	// ── Pass-1 review fixes ─────────────────────────────────────────────────

	it("strips NULLS LAST without corrupting the column name (was: returned 'LAST')", () => {
		expect(parseOrderByItems("a NULLS LAST")).toEqual({
			items: [{ sortColName: "A", sortOrder: "asc" }],
			unsupportedExpressions: [],
		});
	});

	it("strips NULLS FIRST after an explicit direction", () => {
		expect(parseOrderByItems("a DESC NULLS FIRST")).toEqual({
			items: [{ sortColName: "A", sortOrder: "desc" }],
			unsupportedExpressions: [],
		});
	});

	it("flags expression-form items (e.g. COALESCE) as unsupported instead of corrupting sortColName", () => {
		const result = parseOrderByItems("COALESCE(a, 0) DESC, b ASC");
		expect(result.items).toEqual([{ sortColName: "B", sortOrder: "asc" }]);
		expect(result.unsupportedExpressions).toEqual(["COALESCE(a, 0) DESC"]);
	});

	it("flags expression items wrapped in functions (LOWER, UPPER) as unsupported", () => {
		const result = parseOrderByItems("LOWER(name) ASC");
		expect(result.items).toEqual([]);
		expect(result.unsupportedExpressions).toEqual(["LOWER(name) ASC"]);
	});

	it("treats a column literally named ASC or DESC as a column (no direction)", () => {
		// `ASC` as a single token is a column, not a direction (no
		// preceding expression). Pin the contract.
		expect(parseOrderByItems("ASC")).toEqual({
			items: [{ sortColName: "ASC", sortOrder: "asc" }],
			unsupportedExpressions: [],
		});
	});
});

// ── diffTailClauses ─────────────────────────────────────────────────────────

describe("diffTailClauses: GROUP BY presence toggle", () => {
	it("identical when both user SQL and config agree on no GROUP BY", () => {
		const result = diffTailClauses(`SELECT * FROM T`, { groupByAll: false });
		expect(result.groupBy).toEqual({ kind: "identical" });
		expect(result.unsupportedOrderByExpressions).toEqual([]);
	});

	it("identical when both have GROUP BY", () => {
		const result = diffTailClauses(`SELECT a FROM T GROUP BY a`, { groupByAll: true });
		expect(result.groupBy).toEqual({ kind: "identical" });
		expect(result.unsupportedOrderByExpressions).toEqual([]);
	});

	it("added when user adds a GROUP BY where config has none", () => {
		const result = diffTailClauses(`SELECT a FROM T GROUP BY a`, { groupByAll: false });
		expect(result.groupBy).toEqual({ kind: "added", groupByAll: true });
	});

	it("removed when user removes a GROUP BY that the node had", () => {
		const result = diffTailClauses(`SELECT a FROM T`, { groupByAll: true });
		expect(result.groupBy).toEqual({ kind: "removed", groupByAll: false });
	});
});

describe("diffTailClauses: ORDER BY", () => {
	it("identical when neither side has ORDER BY", () => {
		const result = diffTailClauses(`SELECT * FROM T`, { orderby: false });
		expect(result.orderBy).toEqual({ kind: "identical" });
	});

	it("identical when both sides have the same items", () => {
		const result = diffTailClauses(`SELECT * FROM T ORDER BY a DESC`, {
			orderby: true,
			orderbycolumn: { items: [{ sortColName: "A", sortOrder: "desc" }] },
		});
		expect(result.orderBy).toEqual({ kind: "identical" });
	});

	it("added when user adds an ORDER BY where config had none", () => {
		const result = diffTailClauses(`SELECT * FROM T ORDER BY a, b DESC`, {
			orderby: false,
		});
		expect(result.orderBy).toEqual({
			kind: "added",
			orderby: true,
			items: [
				{ sortColName: "A", sortOrder: "asc" },
				{ sortColName: "B", sortOrder: "desc" },
			],
		});
	});

	it("removed when user drops an ORDER BY that the node had", () => {
		const result = diffTailClauses(`SELECT * FROM T`, {
			orderby: true,
			orderbycolumn: { items: [{ sortColName: "A", sortOrder: "asc" }] },
		});
		expect(result.orderBy).toEqual({ kind: "removed", orderby: false });
	});

	it("changed when the ordering differs (different columns)", () => {
		const result = diffTailClauses(`SELECT * FROM T ORDER BY b DESC`, {
			orderby: true,
			orderbycolumn: { items: [{ sortColName: "A", sortOrder: "asc" }] },
		});
		expect(result.orderBy).toEqual({
			kind: "changed",
			orderby: true,
			items: [{ sortColName: "B", sortOrder: "desc" }],
		});
	});

	it("changed when only the direction differs", () => {
		const result = diffTailClauses(`SELECT * FROM T ORDER BY a DESC`, {
			orderby: true,
			orderbycolumn: { items: [{ sortColName: "A", sortOrder: "asc" }] },
		});
		expect(result.orderBy.kind).toBe("changed");
	});

	it("treats existing config orderbycolumn = [{}] (Coalesce default placeholder) as no items", () => {
		const result = diffTailClauses(`SELECT * FROM T`, {
			orderby: false,
			orderbycolumn: { items: [{}] },
		});
		expect(result.orderBy).toEqual({ kind: "identical" });
		expect(result.unsupportedOrderByExpressions).toEqual([]);
	});

	it("placeholder [{}] with orderby:true is treated as zero items (real placeholder-filter exercise)", () => {
		// Real exercise of the `extractExistingOrderByItems` placeholder
		// filter — `orderby: true` so the `existingOrderby === false`
		// short-circuit doesn't hide the items comparison. With the
		// existing items effectively empty (placeholder filtered out)
		// and the user adding `a`, the diff fires `changed` (the toggle
		// was already on but items-comparison says they differ). The
		// apply path treats `added` and `changed` the same way; what
		// matters here is that the diff DETECTS the difference rather
		// than spuriously returning `identical`.
		const result = diffTailClauses(`SELECT * FROM T ORDER BY a`, {
			orderby: true,
			orderbycolumn: { items: [{}] },
		});
		expect(result.orderBy).toEqual({
			kind: "changed",
			orderby: true,
			items: [{ sortColName: "A", sortOrder: "asc" }],
		});
	});

	it("propagates ORDER BY expression warnings via unsupportedOrderByExpressions", () => {
		const result = diffTailClauses(`SELECT * FROM T ORDER BY COALESCE(a, 0) DESC`, {
			orderby: false,
		});
		expect(result.unsupportedOrderByExpressions).toEqual(["COALESCE(a, 0) DESC"]);
		// No bare-column items, so orderBy is identical (no addable items).
		expect(result.orderBy).toEqual({ kind: "identical" });
	});

	it("does NOT silently destroy existing ORDER BY when the user replaces all items with expressions", () => {
		// Pass-2 regression: previously, `ORDER BY LOWER(name)` against
		// a node that had `orderby: true, items: [{A, asc}]` would land
		// `kind: "removed"` and wipe the existing config. Now it stays
		// `identical` and the warning surfaces via unsupportedOrderByExpressions.
		const result = diffTailClauses(`SELECT * FROM T ORDER BY LOWER(name)`, {
			orderby: true,
			orderbycolumn: { items: [{ sortColName: "A", sortOrder: "asc" }] },
		});
		expect(result.orderBy).toEqual({ kind: "identical" });
		expect(result.unsupportedOrderByExpressions).toEqual(["LOWER(name)"]);
	});

	it("does NOT silently destroy existing ORDER BY when the user wrote a bare `ORDER BY` with no items", () => {
		// Pass-3 regression: previously, an empty-clause `ORDER BY`
		// (illegal SQL but reachable via copy-paste truncation) fell
		// through to `kind: "removed"` and wiped existing config. Now
		// it preserves — same rule as the all-expressions case.
		const result = diffTailClauses(`SELECT * FROM T ORDER BY`, {
			orderby: true,
			orderbycolumn: { items: [{ sortColName: "A", sortOrder: "asc" }] },
		});
		expect(result.orderBy).toEqual({ kind: "identical" });
	});
});

describe("parseOrderByItems: quote-aware dot splitting (pass-3 fix)", () => {
	it("preserves a literal dot inside a quoted identifier", () => {
		// Pass-3 regression: `expr.split(".")` corrupted `"My.Col"`
		// into `["\"My", "Col\""]`, returning `sortColName: "COL\""`.
		// `lastDotSegment` is now quote-aware.
		expect(parseOrderByItems('"My.Col" DESC')).toEqual({
			items: [{ sortColName: "MY.COL", sortOrder: "desc" }],
			unsupportedExpressions: [],
		});
	});

	it("still splits on a real dot separator after a quoted segment", () => {
		expect(parseOrderByItems('"L"."My.Col" ASC')).toEqual({
			items: [{ sortColName: "MY.COL", sortOrder: "asc" }],
			unsupportedExpressions: [],
		});
	});

	it("extracts GROUP BY when followed by HAVING + ORDER BY in one query", () => {
		const result = diffTailClauses(
			`SELECT a, COUNT(*) FROM T GROUP BY a HAVING COUNT(*) > 1 ORDER BY a DESC`,
			{ groupByAll: false, orderby: false },
		);
		expect(result.groupBy).toEqual({ kind: "added", groupByAll: true });
		expect(result.orderBy).toEqual({
			kind: "added",
			orderby: true,
			items: [{ sortColName: "A", sortOrder: "desc" }],
		});
	});

	it("doesn't false-match GROUPING SETS as a GROUP BY", () => {
		// `findTopLevelKeywordIndex` does require trailing word boundary
		// after `group`, but lock the contract with a real test.
		const result = diffTailClauses(
			`SELECT * FROM T WITH GROUPING SETS ((a), (b))`,
			{ groupByAll: false },
		);
		expect(result.groupBy).toEqual({ kind: "identical" });
	});
});
