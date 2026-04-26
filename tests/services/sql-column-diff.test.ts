import { describe, expect, it } from "vitest";
import {
	applyColumnDiff,
	diffColumns,
	parseCreateTableColumns,
	parseSelectColumnsForApply,
} from "../../src/services/templates/sql-column-diff.js";

// ── parseSelectColumnsForApply (DML path) ───────────────────────────────────

describe("parseSelectColumnsForApply", () => {
	it("extracts name + expression for an aliased aggregate, infers NUMBER", () => {
		const sql = `
			SELECT
				STG_ORDERS_SF1000.CUSTOMER_KEY AS CUSTOMER_KEY,
				COUNT(STG_ORDERS_SF1000.ORDER_KEY) AS TOTAL_ORDERS
			FROM "ANALYTICS"."STG_ORDERS_SF1000" STG_ORDERS_SF1000
		`;
		const result = parseSelectColumnsForApply(sql);
		expect(result).toBeDefined();
		const total = result!.find((c) => c.name === "TOTAL_ORDERS");
		expect(total).toBeDefined();
		expect(total!.expression).toContain("COUNT");
		expect(total!.dataType).toBe("NUMBER");
	});

	it("returns undefined when no SELECT can be found", () => {
		expect(parseSelectColumnsForApply("CREATE TABLE FOO (x NUMBER)")).toBeUndefined();
	});

	it("preserves the SELECT expression verbatim for source-mapping inference", () => {
		const sql = `
			SELECT
				GREATEST(STG.LOAD_TS, DIM.LOAD_TS) AS MERGED_LOAD_TS
			FROM "ANALYTICS"."STG" STG
			LEFT JOIN "ANALYTICS"."DIM" DIM ON STG.K = DIM.K
		`;
		const result = parseSelectColumnsForApply(sql);
		expect(result).toBeDefined();
		const merged = result!.find((c) => c.name === "MERGED_LOAD_TS");
		expect(merged?.expression).toBe("GREATEST(STG.LOAD_TS, DIM.LOAD_TS)");
	});
});

// ── diffColumns: DML inputs with empty dataType ─────────────────────────────

describe("diffColumns with DML-style empty dataType", () => {
	it("treats columns with empty dataType as unchanged (skips type-change)", () => {
		const existing = [
			{ name: "ORDER_KEY", dataType: "NUMBER(38,0)" },
			{ name: "TOTAL_PRICE", dataType: "NUMBER(38,2)" },
		];
		const parsed = [
			// DML where infer couldn't pin a type
			{ name: "ORDER_KEY", dataType: "" },
			{ name: "TOTAL_PRICE", dataType: "" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.unchanged).toEqual(["ORDER_KEY", "TOTAL_PRICE"]);
		expect(diff.typeChanged).toEqual([]);
		expect(diff.removed).toEqual([]);
	});

	it("still flags type-change when parsed dataType IS present (DDL path)", () => {
		const existing = [{ name: "X", dataType: "VARCHAR(10)" }];
		const parsed = [{ name: "X", dataType: "VARCHAR(25)" }];
		const diff = diffColumns(parsed, existing);
		expect(diff.typeChanged).toEqual([
			{ name: "X", from: "VARCHAR(10)", to: "VARCHAR(25)" },
		]);
	});
});

// ── applyColumnDiff: new addedColumnsByName option ──────────────────────────

describe("applyColumnDiff with addedColumnsByName", () => {
	const existing = [
		{ name: "ORDER_KEY", dataType: "NUMBER", id: "col-1" },
		{ name: "TOTAL_PRICE", dataType: "NUMBER(38,2)", id: "col-2" },
	];

	it("applies a pre-built added column from the map", () => {
		const parsed = parseCreateTableColumns(
			"CREATE TABLE X (ORDER_KEY NUMBER, TOTAL_PRICE NUMBER(38,2), TOTAL_ORDERS NUMBER)",
		)!;
		const diff = diffColumns(parsed, existing);
		expect(diff.added).toHaveLength(1);
		const inferred = {
			name: "TOTAL_ORDERS",
			dataType: "NUMBER",
			nullable: true,
			sources: [{ transform: "COUNT(X.ORDER_KEY)", columnReferences: [] }],
		};
		const out = applyColumnDiff(parsed, existing, diff, {
			addedColumnsByName: new Map([["TOTAL_ORDERS", inferred]]),
		});
		expect(out).toHaveLength(3);
		expect(out[2]).toEqual(inferred);
	});

	it("throws when adds exist but no map is provided", () => {
		const parsed = parseCreateTableColumns(
			"CREATE TABLE X (ORDER_KEY NUMBER, TOTAL_PRICE NUMBER(38,2), NEW_COL NUMBER)",
		)!;
		const diff = diffColumns(parsed, existing);
		expect(() => applyColumnDiff(parsed, existing, diff)).toThrow(
			/no inferred column entries were supplied/,
		);
	});

	it("preserves existing dataType when parsed dataType is empty (DML round-trip)", () => {
		const parsed = [
			{ name: "ORDER_KEY", dataType: "" },
			{ name: "TOTAL_PRICE", dataType: "" },
		];
		const diff = diffColumns(parsed, existing);
		const out = applyColumnDiff(parsed, existing, diff);
		expect(out).toHaveLength(2);
		// dataType should be untouched
		expect((out[0] as Record<string, unknown>).dataType).toBe("NUMBER");
		expect((out[1] as Record<string, unknown>).dataType).toBe("NUMBER(38,2)");
	});

	it("drops removed columns", () => {
		const parsed = parseCreateTableColumns(
			"CREATE TABLE X (ORDER_KEY NUMBER)",
		)!;
		const diff = diffColumns(parsed, existing);
		const out = applyColumnDiff(parsed, existing, diff);
		expect(out).toHaveLength(1);
		expect((out[0] as Record<string, unknown>).name).toBe("ORDER_KEY");
	});
});

// ── Iteration 1: AS rename detection ────────────────────────────────────────

describe("diffColumns: rename detection", () => {
	it("pairs a removed and added column with the same expression as a rename", () => {
		const existing = [
			{
				name: "ORDER_KEY",
				dataType: "NUMBER",
				id: "col-1",
				sources: [{ transform: 'X."O_ORDERKEY"', columnReferences: [] }],
			},
		];
		const parsed = [
			{ name: "ORDER_PK", dataType: "", expression: 'X."O_ORDERKEY"' },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.renamed).toEqual([
			{ from: "ORDER_KEY", to: "ORDER_PK", expression: 'X."O_ORDERKEY"' },
		]);
		expect(diff.removed).toEqual([]);
		expect(diff.added).toEqual([]);
	});

	it("does NOT pair when the expression differs (true different column)", () => {
		const existing = [
			{
				name: "TOTAL_ORDERS",
				dataType: "NUMBER",
				sources: [{ transform: "COUNT(X.A)", columnReferences: [] }],
			},
		];
		const parsed = [
			{ name: "TOTAL_AMOUNT", dataType: "", expression: "SUM(X.B)" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.renamed).toEqual([]);
		expect(diff.removed).toEqual(["TOTAL_ORDERS"]);
		expect(diff.added).toHaveLength(1);
		expect(diff.added[0].name).toBe("TOTAL_AMOUNT");
	});

	it("does NOT pair when the same expression appears multiple times (ambiguous)", () => {
		const existing = [
			{ name: "A", sources: [{ transform: "X.foo" }] },
			{ name: "B", sources: [{ transform: "X.foo" }] },
		];
		const parsed = [
			{ name: "C", dataType: "", expression: "X.foo" },
			{ name: "D", dataType: "", expression: "X.foo" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.renamed).toEqual([]);
		expect(diff.removed.sort()).toEqual(["A", "B"]);
		expect(diff.added.map(a => a.name).sort()).toEqual(["C", "D"]);
	});

	it("skips rename detection for DDL (no expressions) — falls back to drop+add", () => {
		const existing = [
			{ name: "OLD", sources: [{ transform: "X.foo" }] },
		];
		const parsed = [
			{ name: "NEW", dataType: "VARCHAR" }, // no expression — DDL path
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.renamed).toEqual([]);
		expect(diff.removed).toEqual(["OLD"]);
		expect(diff.added).toHaveLength(1);
	});
});

describe("applyColumnDiff: rename preserves lineage", () => {
	it("renames in place, preserving id and sources", () => {
		const existing = [
			{
				name: "ORDER_KEY",
				dataType: "NUMBER",
				id: "col-original",
				nullable: false,
				description: "the order primary key",
				sources: [
					{
						transform: 'X."O_ORDERKEY"',
						columnReferences: [{ nodeID: "n1", columnID: "c1" }],
					},
				],
			},
		];
		const parsed = [
			{ name: "ORDER_PK", dataType: "", expression: 'X."O_ORDERKEY"' },
		];
		const diff = diffColumns(parsed, existing);
		const out = applyColumnDiff(parsed, existing, diff);
		expect(out).toHaveLength(1);
		const renamed = out[0] as Record<string, unknown>;
		expect(renamed.name).toBe("ORDER_PK");
		expect(renamed.id).toBe("col-original");
		expect(renamed.nullable).toBe(false);
		expect(renamed.description).toBe("the order primary key");
		expect(renamed.sources).toEqual(existing[0].sources);
	});

	it("respects SQL order when a rename appears mid-list (reorder)", () => {
		const existing = [
			{ name: "A", id: "id-a", sources: [{ transform: "X.a" }] },
			{ name: "B", id: "id-b", sources: [{ transform: "X.b" }] },
			{ name: "C", id: "id-c", sources: [{ transform: "X.c" }] },
		];
		const parsed = [
			{ name: "B", dataType: "", expression: "X.b" },
			{ name: "C", dataType: "", expression: "X.c" },
			{ name: "A_RENAMED", dataType: "", expression: "X.a" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.renamed).toEqual([
			{ from: "A", to: "A_RENAMED", expression: "X.a" },
		]);
		const out = applyColumnDiff(parsed, existing, diff);
		expect(out.map(c => (c as Record<string, unknown>).name)).toEqual(
			["B", "C", "A_RENAMED"],
		);
		expect((out[2] as Record<string, unknown>).id).toBe("id-a");
	});
});

// ── Iteration 1: expression edit ────────────────────────────────────────────

describe("diffColumns: expression-change detection", () => {
	it("flags an edited expression on a same-named column", () => {
		const existing = [
			{
				name: "TOTAL_ORDERS",
				dataType: "NUMBER",
				sources: [{ transform: "COUNT(X.A)", columnReferences: [] }],
			},
		];
		const parsed = [
			{ name: "TOTAL_ORDERS", dataType: "", expression: "SUM(X.A)" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.expressionChanged).toEqual([
			{ name: "TOTAL_ORDERS", from: "COUNT(X.A)", to: "SUM(X.A)" },
		]);
		expect(diff.unchanged).toEqual([]);
	});

	it("does not flag whitespace-only differences in the transform", () => {
		const existing = [
			{ name: "X", sources: [{ transform: "COUNT( X.A )" }] },
		];
		const parsed = [
			{ name: "X", dataType: "", expression: "COUNT(X.A)" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.expressionChanged).toEqual([]);
		expect(diff.unchanged).toEqual(["X"]);
	});

	it("does not flag for DDL inputs (no expression captured)", () => {
		const existing = [
			{ name: "X", sources: [{ transform: "COUNT(A)" }] },
		];
		const parsed = [
			{ name: "X", dataType: "NUMBER" }, // no expression
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.expressionChanged).toEqual([]);
	});
});

describe("diffColumns: dual-bucket (type AND expression changed on same column)", () => {
	it("places a column in BOTH typeChanged and expressionChanged when both differ", () => {
		const existing = [
			{
				name: "TOTAL",
				dataType: "NUMBER",
				sources: [{ transform: "COUNT(X.A)", columnReferences: [] }],
			},
		];
		const parsed = [
			{ name: "TOTAL", dataType: "NUMBER(38,2)", expression: "SUM(X.A)" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.typeChanged).toEqual([
			{ name: "TOTAL", from: "NUMBER", to: "NUMBER(38,2)" },
		]);
		expect(diff.expressionChanged).toEqual([
			{ name: "TOTAL", from: "COUNT(X.A)", to: "SUM(X.A)" },
		]);
		expect(diff.unchanged).toEqual([]);
	});

	it("applyColumnDiff applies BOTH a dataType update and an expression update on the same column", () => {
		const existing = [
			{
				name: "TOTAL",
				dataType: "NUMBER",
				id: "col-1",
				sources: [{ transform: "COUNT(X.A)", columnReferences: [{ nodeID: "n", columnID: "c" }] }],
			},
		];
		const parsed = [
			{ name: "TOTAL", dataType: "NUMBER(38,2)", expression: "SUM(X.A)" },
		];
		const diff = diffColumns(parsed, existing);
		const newSources = [{ transform: "SUM(X.A)", columnReferences: [{ nodeID: "n", columnID: "c" }] }];
		const out = applyColumnDiff(parsed, existing, diff, {
			updatedSourcesByName: new Map([["TOTAL", newSources]]),
		});
		const updated = out[0] as Record<string, unknown>;
		expect(updated.id).toBe("col-1");
		expect(updated.dataType).toBe("NUMBER(38,2)");
		expect(updated.sources).toEqual(newSources);
	});
});

describe("diffColumns: rename + dataType change in same edit (review-pass-2 fix)", () => {
	it("emits BOTH a renamed entry AND a typeChanged entry under the new name", () => {
		const existing = [
			{
				name: "ORDER_KEY",
				dataType: "NUMBER",
				sources: [{ transform: "X.O_ORDERKEY" }],
			},
		];
		const parsed = [
			{ name: "ORDER_PK", dataType: "NUMBER(38,0)", expression: "X.O_ORDERKEY" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.renamed).toEqual([
			{ from: "ORDER_KEY", to: "ORDER_PK", expression: "X.O_ORDERKEY" },
		]);
		expect(diff.typeChanged).toEqual([
			{ name: "ORDER_PK", from: "NUMBER", to: "NUMBER(38,0)" },
		]);
	});

	it("does NOT emit a typeChanged entry when only the rename happens (types match)", () => {
		const existing = [
			{ name: "A", dataType: "NUMBER", sources: [{ transform: "X.Y" }] },
		];
		const parsed = [
			{ name: "B", dataType: "NUMBER", expression: "X.Y" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.renamed).toHaveLength(1);
		expect(diff.typeChanged).toEqual([]);
	});

	it("applyColumnDiff lands the new dataType on the renamed entry", () => {
		const existing = [
			{
				name: "ORIG",
				dataType: "VARCHAR(10)",
				id: "col-1",
				sources: [{ transform: "X.Y" }],
			},
		];
		const parsed = [
			{ name: "RENAMED", dataType: "VARCHAR(50)", expression: "X.Y" },
		];
		const diff = diffColumns(parsed, existing);
		const out = applyColumnDiff(parsed, existing, diff);
		const c = out[0] as Record<string, unknown>;
		expect(c.id).toBe("col-1");
		expect(c.name).toBe("RENAMED");
		expect(c.dataType).toBe("VARCHAR(50)");
	});
});

describe("applyColumnDiff: rename of column with multiple sources entries (T2)", () => {
	it("preserves all sources entries, not just the first", () => {
		const existing = [
			{
				name: "MERGED",
				id: "col-merged",
				sources: [
					{ transform: "X.A", columnReferences: [{ nodeID: "n1", columnID: "c1" }] },
					{ transform: "Y.B", columnReferences: [{ nodeID: "n2", columnID: "c2" }] },
				],
			},
		];
		const parsed = [
			{ name: "MERGED_RENAMED", dataType: "", expression: "X.A" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.renamed).toHaveLength(1);
		const out = applyColumnDiff(parsed, existing, diff);
		const renamed = out[0] as Record<string, unknown>;
		expect(renamed.name).toBe("MERGED_RENAMED");
		expect(renamed.sources).toEqual(existing[0].sources);
		expect((renamed.sources as unknown[]).length).toBe(2);
	});
});

describe("diffColumns: same upstream column projected under multiple aliases (T4)", () => {
	it("treats both as adds (no auto-pairing) when one removed has the same expression", () => {
		const existing = [
			{ name: "ORIGINAL", sources: [{ transform: "X.A" }] },
		];
		const parsed = [
			{ name: "A1", dataType: "", expression: "X.A" },
			{ name: "A2", dataType: "", expression: "X.A" },
		];
		const diff = diffColumns(parsed, existing);
		// One removed, two adds with same expression → ambiguous → no rename pairing.
		expect(diff.renamed).toEqual([]);
		expect(diff.removed).toEqual(["ORIGINAL"]);
		expect(diff.added.map(a => a.name).sort()).toEqual(["A1", "A2"]);
	});
});

describe("diffColumns: rename + expression edit on same logical column (T5 — locks contract)", () => {
	it("falls to drop+add (does NOT pair as rename) when expression also changed", () => {
		const existing = [
			{ name: "OLD", sources: [{ transform: "COUNT(X.A)" }] },
		];
		const parsed = [
			{ name: "NEW", dataType: "", expression: "SUM(X.A)" },
		];
		const diff = diffColumns(parsed, existing);
		// Pairing requires exact expression match; COUNT(X.A) ≠ SUM(X.A)
		// so the column is treated as drop+add. Documented contract.
		expect(diff.renamed).toEqual([]);
		expect(diff.removed).toEqual(["OLD"]);
		expect(diff.added).toHaveLength(1);
	});
});

describe("diffColumns + applyColumnDiff: quoted output names (T6)", () => {
	it("normalizes quoted vs unquoted names so the apply path key-matches correctly", () => {
		const existing = [
			{ name: "MY_COL", id: "col-1", sources: [{ transform: "X.A" }] },
		];
		// Imagine a parser variant emitting the name with surrounding quotes.
		// `normalizeColumnKey` strips them, so the apply path still finds the
		// existing column by key.
		const parsed = [
			{ name: '"MY_COL"', dataType: "", expression: "X.A" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.unchanged).toEqual(["MY_COL"]);
		const out = applyColumnDiff(parsed, existing, diff);
		const c = out[0] as Record<string, unknown>;
		expect(c.id).toBe("col-1");
	});
});

describe("applyColumnDiff: rename invariant", () => {
	it("throws when a renamed column's source is missing from existing (caller bug)", () => {
		const existing = [
			{ name: "ORIG", id: "id-1", sources: [{ transform: "X.Y" }] },
		];
		const parsed = [
			{ name: "RENAMED", dataType: "", expression: "X.Y" },
		];
		const diff = diffColumns(parsed, existing);
		expect(diff.renamed).toHaveLength(1);
		// Simulate the caller-bug: pass an existing array WITHOUT the original column.
		expect(() =>
			applyColumnDiff(parsed, /* existing */ [], diff),
		).toThrow(/rename target/);
	});
});

describe("applyColumnDiff: expression edit", () => {
	const existing = [
		{
			name: "TOTAL_ORDERS",
			dataType: "NUMBER",
			id: "col-1",
			sources: [
				{
					transform: "COUNT(X.A)",
					columnReferences: [{ nodeID: "stg", columnID: "col-A" }],
				},
			],
		},
	];

	it("uses updatedSourcesByName when provided (preserves id)", () => {
		const parsed = [
			{ name: "TOTAL_ORDERS", dataType: "", expression: "SUM(X.A)" },
		];
		const diff = diffColumns(parsed, existing);
		const newSources = [
			{
				transform: "SUM(X.A)",
				columnReferences: [{ nodeID: "stg", columnID: "col-A" }],
			},
		];
		const out = applyColumnDiff(parsed, existing, diff, {
			updatedSourcesByName: new Map([["TOTAL_ORDERS", newSources]]),
		});
		const updated = out[0] as Record<string, unknown>;
		expect(updated.id).toBe("col-1");
		expect(updated.sources).toEqual(newSources);
	});

	it("falls back to patching only the transform when no updatedSources are passed", () => {
		const parsed = [
			{ name: "TOTAL_ORDERS", dataType: "", expression: "SUM(X.A)" },
		];
		const diff = diffColumns(parsed, existing);
		const out = applyColumnDiff(parsed, existing, diff);
		const updated = out[0] as Record<string, unknown>;
		expect(updated.id).toBe("col-1");
		const sources = updated.sources as Array<Record<string, unknown>>;
		expect(sources[0].transform).toBe("SUM(X.A)");
		// columnReferences are preserved (potentially stale — caller is
		// responsible for surfacing a warning).
		expect(sources[0].columnReferences).toEqual([
			{ nodeID: "stg", columnID: "col-A" },
		]);
	});
});

// ── Iteration 1: SQL-order preservation ────────────────────────────────────

describe("applyColumnDiff: output respects SQL order", () => {
	it("emits columns in the order they appear in the parsed SELECT", () => {
		const existing = [
			{ name: "A", id: "1" },
			{ name: "B", id: "2" },
			{ name: "C", id: "3" },
		];
		const parsed = [
			{ name: "C", dataType: "" },
			{ name: "A", dataType: "" },
			{ name: "B", dataType: "" },
		];
		const diff = diffColumns(parsed, existing);
		const out = applyColumnDiff(parsed, existing, diff);
		expect(out.map(c => (c as Record<string, unknown>).name)).toEqual(["C", "A", "B"]);
		// ids preserved across the reorder
		expect(out.map(c => (c as Record<string, unknown>).id)).toEqual(["3", "1", "2"]);
	});
});
