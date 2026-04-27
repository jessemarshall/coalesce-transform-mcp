import { describe, expect, it } from "vitest";
import {
	extractAllColumnRefsFromTransform,
	extractColumnRefFromTransform,
	inferColumnFromAddedItem,
	neutralizeQuotedDots,
	neutralizeStringLiterals,
	resolveColumnSources,
} from "../../src/services/templates/infer-source-mapping.js";

// ---------------------------------------------------------------------------
// extractColumnRefFromTransform — mirrors the Python docstring scenarios so
// the two implementations stay in lockstep on the cases we both care about.
// ---------------------------------------------------------------------------

describe("extractColumnRefFromTransform", () => {
	it("extracts simple ALIAS.COLUMN", () => {
		expect(extractColumnRefFromTransform("SRC.EAAN8")).toEqual({
			alias: "SRC",
			column: "EAAN8",
		});
	});

	it("looks past CAST() wrapper", () => {
		expect(extractColumnRefFromTransform("CAST(SRC.EAAN8 AS INT)")).toEqual({
			alias: "SRC",
			column: "EAAN8",
		});
	});

	it("looks past COALESCE with constant", () => {
		expect(extractColumnRefFromTransform("COALESCE(SRC.COL, 0)")).toEqual({
			alias: "SRC",
			column: "COL",
		});
	});

	it("returns first ref when same alias is referenced multiple times", () => {
		expect(extractColumnRefFromTransform("MD5(CONCAT(SRC.A, SRC.B))")).toEqual({
			alias: "SRC",
			column: "A",
		});
	});

	it("returns null/null for true multi-source (different aliases)", () => {
		expect(extractColumnRefFromTransform("COALESCE(A.X, B.Y)")).toEqual({
			alias: null,
			column: null,
		});
	});

	it("returns null/BARE for unqualified bare column", () => {
		expect(extractColumnRefFromTransform("STATUS")).toEqual({
			alias: null,
			column: "STATUS",
		});
	});

	it("returns null/null for numeric literal", () => {
		expect(extractColumnRefFromTransform("42")).toEqual({ alias: null, column: null });
	});

	it("returns null/null for string literal", () => {
		expect(extractColumnRefFromTransform("'literal'")).toEqual({
			alias: null,
			column: null,
		});
	});

	it("returns null/null for empty transform", () => {
		expect(extractColumnRefFromTransform("")).toEqual({ alias: null, column: null });
	});

	it("ignores function-prefix pseudo-aliases (CAST.X is not a real ref)", () => {
		// The simple-ref fast path would match CAST.X if we didn't filter
		// SQL_FUNCTION_PREFIXES. Verify CAST is excluded.
		expect(extractColumnRefFromTransform("CAST.X")).toEqual({
			alias: null,
			column: null,
		});
	});

	it("ignores column-like patterns inside string literals", () => {
		expect(extractColumnRefFromTransform("CONCAT(SRC.COL, 'ABC.DEF')")).toEqual({
			alias: "SRC",
			column: "COL",
		});
	});
});

// ---------------------------------------------------------------------------
// extractAllColumnRefsFromTransform — covers the multi-source resolution path
// ---------------------------------------------------------------------------

describe("extractAllColumnRefsFromTransform", () => {
	it("extracts both refs for COALESCE(A.X, B.Y)", () => {
		expect(extractAllColumnRefsFromTransform("COALESCE(A.X, B.Y)")).toEqual([
			{ alias: "A", column: "X" },
			{ alias: "B", column: "Y" },
		]);
	});

	it("dedupes repeated refs", () => {
		expect(extractAllColumnRefsFromTransform("MD5(CONCAT(SRC.A, SRC.A))")).toEqual([
			{ alias: "SRC", column: "A" },
		]);
	});

	it("returns empty for literal-only transform", () => {
		expect(extractAllColumnRefsFromTransform("'literal'")).toEqual([]);
	});

	it("skips function-prefix pseudo-refs", () => {
		// CAST.X looks like a qualified ref but CAST is in SQL_FUNCTION_PREFIXES.
		expect(extractAllColumnRefsFromTransform("CAST(X.Y AS INT)")).toEqual([
			{ alias: "X", column: "Y" },
		]);
	});
});

// ---------------------------------------------------------------------------
// neutralization helpers
// ---------------------------------------------------------------------------

describe("neutralization helpers", () => {
	it("neutralizes dots inside quoted identifiers", () => {
		expect(neutralizeQuotedDots('"MY.SCHEMA".COL')).toBe('"MY_SCHEMA".COL');
	});

	it("neutralizes content inside single-quoted strings", () => {
		// Length-preserving so regex offsets in callers stay sensible.
		const out = neutralizeStringLiterals("'ABC.DEF'");
		expect(out.length).toBe("'ABC.DEF'".length);
		expect(out).not.toContain("ABC.DEF");
	});
});

// ---------------------------------------------------------------------------
// resolveColumnSources — the main multi-strategy resolver
// ---------------------------------------------------------------------------

describe("resolveColumnSources", () => {
	const STG_ID = "node-stg-orders";
	const DIM_ID = "node-dim-customer";
	const ORDER_KEY_COL = "col-order-key";
	const TOTAL_PRICE_COL = "col-total-price";
	const STG_LOAD_TS = "col-stg-load-ts";
	const DIM_LOAD_TS = "col-dim-load-ts";
	const NAME_COL = "col-name";

	const smAliases = {
		STG_ORDERS_SF1000: STG_ID,
		DIM_CUSTOMER_SF1000: DIM_ID,
	};
	const predColLookup = {
		[STG_ID]: {
			ORDER_KEY: ORDER_KEY_COL,
			TOTAL_PRICE: TOTAL_PRICE_COL,
			LOAD_TIMESTAMP: STG_LOAD_TS,
		},
		[DIM_ID]: {
			CUSTOMER_KEY: "col-cust-key",
			NAME: NAME_COL,
			LOAD_TIMESTAMP: DIM_LOAD_TS,
		},
	};

	it("Strategy 1: resolves alias-prefix match through CAST wrapper", () => {
		const result = resolveColumnSources({
			colName: "ORDER_KEY",
			transform: 'CAST("STG_ORDERS_SF1000"."ORDER_KEY" AS NUMBER)',
			smAliases,
			predColLookup,
			defaultPredID: STG_ID,
		});
		expect(result).toEqual([
			{
				transform: 'CAST("STG_ORDERS_SF1000"."ORDER_KEY" AS NUMBER)',
				columnReferences: [{ nodeID: STG_ID, columnID: ORDER_KEY_COL }],
			},
		]);
	});

	it("Strategy 1: resolves direct ALIAS.COLUMN reference", () => {
		const result = resolveColumnSources({
			colName: "TOTAL_ORDERS",
			transform: 'COUNT("STG_ORDERS_SF1000"."ORDER_KEY")',
			smAliases,
			predColLookup,
			defaultPredID: STG_ID,
		});
		expect(result).toHaveLength(1);
		expect(result![0].columnReferences).toEqual([
			{ nodeID: STG_ID, columnID: ORDER_KEY_COL },
		]);
	});

	it("Strategy 3: scans all predecessors when alias missing", () => {
		// Bare 'NAME' — no alias, present only in DIM_CUSTOMER_SF1000.
		const result = resolveColumnSources({
			colName: "CUSTOMER_NAME",
			transform: "NAME",
			smAliases,
			predColLookup,
			defaultPredID: STG_ID,
		});
		expect(result).toEqual([
			{
				transform: "NAME",
				columnReferences: [{ nodeID: DIM_ID, columnID: NAME_COL }],
			},
		]);
	});

	it("Strategy 4: multi-source for GREATEST(A.X, B.Y)", () => {
		// Output column name MUST differ from the upstream names in both
		// predecessors — otherwise Strategy 1's default-pred + colName fallback
		// short-circuits with a single ref before Strategy 4 runs (this matches
		// the Python algorithm's contract; see test_multi_source_all_resolved
		// in test_column_lineage_edge_cases.py).
		const result = resolveColumnSources({
			colName: "MERGED_LOAD_TS",
			transform:
				'GREATEST("STG_ORDERS_SF1000"."LOAD_TIMESTAMP","DIM_CUSTOMER_SF1000"."LOAD_TIMESTAMP")',
			smAliases,
			predColLookup,
			defaultPredID: STG_ID,
		});
		expect(result).toHaveLength(1);
		expect(result![0].columnReferences).toEqual([
			{ nodeID: STG_ID, columnID: STG_LOAD_TS },
			{ nodeID: DIM_ID, columnID: DIM_LOAD_TS },
		]);
	});

	it("Strategy 4: partial-multi result when one alias is unresolvable", () => {
		const result = resolveColumnSources({
			colName: "MIXED",
			transform: 'COALESCE("STG_ORDERS_SF1000"."ORDER_KEY", "UNKNOWN_ALIAS"."X")',
			smAliases,
			predColLookup,
			defaultPredID: STG_ID,
		});
		expect(result).toHaveLength(1);
		// Only the resolvable half makes it in.
		expect(result![0].columnReferences).toEqual([
			{ nodeID: STG_ID, columnID: ORDER_KEY_COL },
		]);
	});

	it("returns null when no strategy can resolve", () => {
		const result = resolveColumnSources({
			colName: "RANDOM",
			transform: "CURRENT_TIMESTAMP",
			smAliases,
			predColLookup,
			defaultPredID: STG_ID,
		});
		// CURRENT_TIMESTAMP is a bare ident, scan-all finds nothing.
		expect(result).toBeNull();
	});

	it("returns null when transform is a literal", () => {
		const result = resolveColumnSources({
			colName: "FLAG",
			transform: "'Y'",
			smAliases,
			predColLookup,
			defaultPredID: STG_ID,
		});
		expect(result).toBeNull();
	});
});

// ---------------------------------------------------------------------------
// inferColumnFromAddedItem — the apply-path-facing wrapper
// ---------------------------------------------------------------------------

describe("inferColumnFromAddedItem", () => {
	it("returns a fully-resolved column when lineage resolves", () => {
		const out = inferColumnFromAddedItem(
			{
				name: "TOTAL_ORDERS_JESSE",
				dataType: "NUMBER",
				transform: 'COUNT("STG_ORDERS_SF1000"."ORDER_KEY")',
			},
			{
				smAliases: { STG_ORDERS_SF1000: "stg-id" },
				predColLookup: { "stg-id": { ORDER_KEY: "ord-key-id" } },
				defaultPredID: "stg-id",
			},
		);
		expect(out.resolved).toBe(true);
		expect(out.column.name).toBe("TOTAL_ORDERS_JESSE");
		expect(out.column.dataType).toBe("NUMBER");
		expect(out.column.sources[0].columnReferences).toEqual([
			{ nodeID: "stg-id", columnID: "ord-key-id" },
		]);
	});

	it("falls back to a bare column with empty columnReferences when lineage fails", () => {
		const out = inferColumnFromAddedItem(
			{ name: "WEIRD", dataType: "VARCHAR", transform: "SOME_FN()" },
			{
				smAliases: { STG: "stg-id" },
				predColLookup: { "stg-id": { OTHER: "other-id" } },
				defaultPredID: "stg-id",
			},
		);
		expect(out.resolved).toBe(false);
		expect(out.column.sources).toEqual([
			{ columnReferences: [], transform: "SOME_FN()" },
		]);
	});

	it("emits a columnID UUID — the Coalesce REST PUT requires it on every column", () => {
		// Without this, the apply path's added columns hit an
		// `request/body/metadata/columns/N must have required property
		// 'columnID'` validation error from set_workspace_node.
		const uuidV4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
		const resolved = inferColumnFromAddedItem(
			{
				name: "TOTAL_ORDERS_JESSE",
				dataType: "NUMBER",
				transform: 'COUNT("STG_ORDERS_SF1000"."ORDER_KEY")',
			},
			{
				smAliases: { STG_ORDERS_SF1000: "stg-id" },
				predColLookup: { "stg-id": { ORDER_KEY: "ord-key-id" } },
				defaultPredID: "stg-id",
			},
		);
		expect(resolved.column.columnID).toMatch(uuidV4);

		const bare = inferColumnFromAddedItem(
			{ name: "WEIRD", dataType: "VARCHAR", transform: "SOME_FN()" },
			{ smAliases: {}, predColLookup: {}, defaultPredID: null },
		);
		expect(bare.column.columnID).toMatch(uuidV4);
		expect(bare.column.columnID).not.toBe(resolved.column.columnID);
	});

	it("emits an empty `description` on every column — the Coalesce REST PUT requires it", () => {
		// Mirrors the columnID invariant above. Without this, an apply that
		// adds a brand-new column lands a metadata.columns[] entry with no
		// `description`, and the API rejects with `must have required
		// property 'description'`.
		const resolved = inferColumnFromAddedItem(
			{
				name: "TOTAL_ORDERS_JESSE",
				dataType: "NUMBER",
				transform: 'COUNT("STG_ORDERS_SF1000"."ORDER_KEY")',
			},
			{
				smAliases: { STG_ORDERS_SF1000: "stg-id" },
				predColLookup: { "stg-id": { ORDER_KEY: "ord-key-id" } },
				defaultPredID: "stg-id",
			},
		);
		expect(resolved.column.description).toBe("");

		const bare = inferColumnFromAddedItem(
			{ name: "WEIRD", dataType: "VARCHAR", transform: "SOME_FN()" },
			{ smAliases: {}, predColLookup: {}, defaultPredID: null },
		);
		expect(bare.column.description).toBe("");
	});
});
