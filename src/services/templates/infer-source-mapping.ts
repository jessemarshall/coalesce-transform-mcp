/**
 * Best-effort source-mapping inference for columns added by SQL edits to a
 * cloud node. Given a column's transform expression and the node's
 * predecessor alias map + upstream column inventories, builds the cloud-shape
 * `sources[]` entry (transform + columnReferences) so the column can be
 * applied without requiring the user to fill in lineage by hand.
 *
 * **This is a TypeScript port** of `resolve_column_sources` in
 * coalesce-migration-agents/src/coalesce_migration_agents/utils/column_lineage.py
 * — that's the production-tested algorithm used during pipeline migrations.
 * Strategies are kept identical except where noted:
 *
 *   1. Alias-prefix match — pull `ALIAS.COL` (incl. inside CAST/COALESCE/etc.
 *      function wrappers) from the transform and look up that predecessor.
 *   3. Scan-all-predecessors fallback — when no alias resolved, search every
 *      predecessor for a column matching the source name.
 *   4. Multi-source — for `COALESCE(A.X, B.Y)` / `GREATEST(...)`, emit one
 *      `columnReferences[]` entry per qualified ref. Falls through to a
 *      partial result if not all refs resolve.
 *
 * Skipped from the Python original (with rationale):
 *   - Strategy 2 (fuzzy CTE-suffix alias matching). Migration-specific; CTE
 *     decomposition produces synthetic aliases that we don't generate here.
 *   - Strategy 4b (unqualified-identifier scan via sqlglot AST). Requires a
 *     SQL-parser dependency we don't have on the TS side; the apply path's
 *     inputs are typically rendered DML (qualified refs), so this is a
 *     gap rather than a regression for our use case.
 *   - Strategy 5 (preserve cloud auto_sources). The apply path is
 *     synthesizing fresh columns — there are no existing auto_sources to
 *     preserve.
 *   - Telemetry recording on resolution failure.
 *
 * Function-prefix exclusions (`SQL_FUNCTION_PREFIXES`) are mirrored verbatim
 * from sql_parser.py so behavior matches across the two implementations.
 */
import { stripIdentifierQuotes } from "../pipelines/sql-tokenizer.js";

/**
 * SQL function names that look like `TABLE.COLUMN` in transforms but aren't
 * predecessor aliases. Mirrors `SQL_FUNCTION_PREFIXES` in the Python
 * sql_parser.py — keep these two lists in sync if the Python set is
 * updated.
 */
const SQL_FUNCTION_PREFIXES: ReadonlySet<string> = new Set([
	"CAST", "TRIM", "NVL", "COALESCE", "IFF", "DECODE",
	"TO_CHAR", "TO_DATE", "TO_NUMBER", "TO_DECIMAL", "TO_DOUBLE",
	"REPLACE", "UPPER", "LOWER", "SUBSTR", "SUBSTRING",
	"LEFT", "RIGHT", "CONCAT",
	"MD5", "SHA1", "SHA2", "HASH",
	"ROW_NUMBER", "RANK", "DENSE_RANK",
	"ROUND", "TRUNC", "ABS", "CEIL", "FLOOR",
	"NULLIF", "IFNULL", "TRY_CAST",
]);

const SIMPLE_REF_RE = /^"?([A-Z_]\w*)"?\."?([A-Z_]\w*)"?$/i;
const QUALIFIED_COL_RE = /\b([A-Z_]\w*)\.([A-Z_]\w*)\b/gi;
const QUOTED_IDENT_RE = /"(?:[^"]|"")*"/g;
const SINGLE_QUOTED_RE = /'[^']*'/g;
const NUMERIC_LITERAL_RE = /^\d+(\.\d+)?$/;

/** Replace dots inside double-quoted identifiers with underscores so
 *  `"MY.SCHEMA".COL` doesn't get parsed as alias=MY, col=SCHEMA. */
export function neutralizeQuotedDots(text: string): string {
	return text.replace(QUOTED_IDENT_RE, (m) => m.replace(/\./g, "_"));
}

/** Replace single-quoted string content with underscores so column-like
 *  patterns inside literals (`'ABC.DEF'`) don't match as references. */
export function neutralizeStringLiterals(text: string): string {
	return text.replace(SINGLE_QUOTED_RE, (m) => "'" + "_".repeat(Math.max(0, m.length - 2)) + "'");
}

/**
 * Strip double-quotes around identifiers so the rendered Coalesce form
 * `"ALIAS"."COL"` reduces to `ALIAS.COL`, which is what the Python algorithm
 * was tested against. Run AFTER {@link neutralizeQuotedDots} so a quoted
 * identifier containing a dot has already been collapsed to a safe token.
 *
 * This is a TS-port-only step — the Python original doesn't include it
 * because its inputs are migration-source SQL (unquoted aliases like `SRC.X`),
 * but our inputs are Coalesce-rendered DML which always quotes identifiers.
 */
export function stripIdentifierQuotesForScan(text: string): string {
	return text.replace(/"([A-Za-z_][\w$]*)"/g, "$1");
}

export type AliasColumnRef = { alias: string; column: string };

/**
 * Extract `(alias, column)` from a SQL transform — the "first reference"
 * variant. Returns `null` for both when:
 *   - The transform is empty or a literal
 *   - Multiple *different* aliases are referenced (true multi-source —
 *     the caller should switch to {@link extractAllColumnRefsFromTransform})
 *
 * Returns `{alias: null, column: BARE}` for unqualified bare names.
 *
 * Examples (mirror the Python docstring):
 *   "SRC.EAAN8"               → {SRC, EAAN8}
 *   "CAST(SRC.EAAN8 AS INT)"  → {SRC, EAAN8}
 *   "COALESCE(SRC.COL, 0)"    → {SRC, COL}
 *   "MD5(CONCAT(SRC.A,SRC.B))"→ {SRC, A}
 *   "COALESCE(A.X, B.Y)"      → {null, null}   (multi-alias; caller falls through)
 *   "STATUS"                  → {null, STATUS}
 *   "42"                      → {null, null}
 */
export function extractColumnRefFromTransform(
	transform: string,
): { alias: string | null; column: string | null } {
	const t = transform.trim();
	if (!t) { return { alias: null, column: null }; }

	const safe = stripIdentifierQuotesForScan(
		neutralizeStringLiterals(neutralizeQuotedDots(t)),
	);

	// Fast path: simple TABLE.COLUMN
	const simple = SIMPLE_REF_RE.exec(safe);
	if (simple) {
		const alias = (simple[1] ?? "").toUpperCase();
		const col = (simple[2] ?? "").toUpperCase();
		if (!SQL_FUNCTION_PREFIXES.has(alias)) {
			return { alias, column: col };
		}
	}

	// Scan for ALIAS.COLUMN inside expressions; bail to multi-source if
	// we see two distinct non-function aliases.
	let firstAlias: string | null = null;
	let firstCol: string | null = null;
	const re = new RegExp(QUALIFIED_COL_RE.source, "gi");
	let m: RegExpExecArray | null;
	while ((m = re.exec(safe)) !== null) {
		const alias = m[1].toUpperCase();
		if (SQL_FUNCTION_PREFIXES.has(alias)) { continue; }
		if (firstAlias === null) {
			firstAlias = alias;
			firstCol = m[2].toUpperCase();
		} else if (alias !== firstAlias) {
			return { alias: null, column: null };
		}
	}
	if (firstAlias !== null) {
		return { alias: firstAlias, column: firstCol };
	}

	// Bare column name — no dot, no parens, no string literal, not a number
	if (!safe.includes(".") && !safe.includes("(") && !safe.includes("'")) {
		const bare = stripIdentifierQuotes(t).toUpperCase();
		if (bare && !NUMERIC_LITERAL_RE.test(bare)) {
			return { alias: null, column: bare };
		}
	}

	return { alias: null, column: null };
}

/**
 * Extract every distinct `(alias, column)` pair from a transform — used by
 * the multi-source resolution path (Strategy 4). Skips function-prefix
 * pseudo-refs and dedupes.
 */
export function extractAllColumnRefsFromTransform(transform: string): AliasColumnRef[] {
	const t = transform.trim();
	if (!t) { return []; }
	const safe = stripIdentifierQuotesForScan(
		neutralizeStringLiterals(neutralizeQuotedDots(t)),
	);
	const refs: AliasColumnRef[] = [];
	const seen = new Set<string>();
	const re = new RegExp(QUALIFIED_COL_RE.source, "gi");
	let m: RegExpExecArray | null;
	while ((m = re.exec(safe)) !== null) {
		const alias = m[1].toUpperCase();
		if (SQL_FUNCTION_PREFIXES.has(alias)) { continue; }
		const col = m[2].toUpperCase();
		const key = `${alias}.${col}`;
		if (seen.has(key)) { continue; }
		seen.add(key);
		refs.push({ alias, column: col });
	}
	return refs;
}

// ── Resolution ──────────────────────────────────────────────────────────────

/**
 * Cloud-shape source entry: a transform string plus the column references it
 * resolves to. Mirrors `metadata.columns[].sources[]` in the cloud body.
 */
export interface ResolvedColumnSource {
	columnReferences: Array<{ nodeID: string; columnID: string }>;
	transform: string;
}

export interface ResolveColumnSourcesParams {
	colName: string;
	transform: string;
	/** alias → upstream node ID, sourced from the node's sourceMapping. */
	smAliases: Record<string, string>;
	/** node ID → (column name → column ID), one entry per predecessor. */
	predColLookup: Record<string, Record<string, string>>;
	/** Which predecessor to try first when the transform has no alias prefix. */
	defaultPredID: string | null;
}

/**
 * Build `sources[]` with full `columnReferences` for a single column. Returns
 * null when no strategy succeeds — the caller can fall back to writing the
 * column with empty `columnReferences` (the "bare add" path) and surfacing a
 * warning to the user.
 */
export function resolveColumnSources(
	params: ResolveColumnSourcesParams,
): ResolvedColumnSource[] | null {
	const { colName, transform, smAliases, predColLookup, defaultPredID } = params;
	const colUpper = colName.toUpperCase();
	const aliasMap = upperKeyMap(smAliases);
	const predMap = upperKeyOuterMap(predColLookup);

	const { alias: aliasPrefix, column: sourceCol } = extractColumnRefFromTransform(transform);
	const sourceColUpper = sourceCol ?? colUpper;
	let targetNodeID: string | null = defaultPredID;

	// Strategy 1: alias-prefix match
	if (aliasPrefix) {
		const mapped = aliasMap.get(aliasPrefix);
		if (mapped) { targetNodeID = mapped; }
	}

	// Build the search-name fallback list (matches the Python ordering)
	const searchNames = [sourceColUpper];
	if (colUpper !== sourceColUpper) { searchNames.push(colUpper); }

	if (targetNodeID) {
		const targetCols = predMap.get(targetNodeID) ?? new Map();
		const colID = lookupColumn(targetCols, ...searchNames);
		if (colID) {
			return [buildSourceEntry(transform, [{ nodeID: targetNodeID, columnID: colID }])];
		}
	}

	// Strategy 3: scan all predecessors
	for (const [predID, predCols] of predMap) {
		const colID = lookupColumn(predCols, ...searchNames);
		if (colID) {
			return [buildSourceEntry(transform, [{ nodeID: predID, columnID: colID }])];
		}
	}

	// Strategy 4: multi-source for computed columns
	let partialMulti: Array<{ nodeID: string; columnID: string }> | null = null;
	const allRefs = extractAllColumnRefsFromTransform(transform);
	if (allRefs.length > 1) {
		const columnReferences: Array<{ nodeID: string; columnID: string }> = [];
		for (const ref of allRefs) {
			const refNodeID = aliasMap.get(ref.alias);
			if (!refNodeID) { continue; }
			const refCols = predMap.get(refNodeID) ?? new Map();
			const colID = lookupColumn(refCols, ref.column);
			if (colID) {
				columnReferences.push({ nodeID: refNodeID, columnID: colID });
			}
		}
		if (columnReferences.length === allRefs.length) {
			return [buildSourceEntry(transform, columnReferences)];
		} else if (columnReferences.length > 0) {
			partialMulti = columnReferences;
		}
	}

	// Strategy 6: partial-multi as last resort
	if (partialMulti) {
		return [buildSourceEntry(transform, partialMulti)];
	}

	return null;
}

// ── Higher-level wrapper used by the apply path ─────────────────────────────

export interface InferColumnInput {
	name: string;
	dataType: string;
	transform: string;
}

export interface InferredColumn {
	column: {
		name: string;
		dataType: string;
		nullable: boolean;
		sources: ResolvedColumnSource[];
	};
	/** True when {@link resolveColumnSources} produced a result. False = bare column. */
	resolved: boolean;
}

/**
 * Build a cloud-shape column ready to push as part of `metadata.columns[]`.
 * If lineage resolves, `column.sources[]` carries `columnReferences`. If not,
 * we still emit a single source entry with the transform but empty
 * `columnReferences` — that's the Phase-1 "bare add" fallback, kept here so
 * the apply path can use one code path for both outcomes.
 */
export function inferColumnFromAddedItem(
	input: InferColumnInput,
	resolveParams: Omit<ResolveColumnSourcesParams, "colName" | "transform">,
): InferredColumn {
	const sources = resolveColumnSources({
		colName: input.name,
		transform: input.transform,
		...resolveParams,
	});
	if (sources) {
		return {
			column: { name: input.name, dataType: input.dataType, nullable: true, sources },
			resolved: true,
		};
	}
	return {
		column: {
			name: input.name,
			dataType: input.dataType,
			nullable: true,
			sources: [{ columnReferences: [], transform: input.transform }],
		},
		resolved: false,
	};
}

// ── helpers ─────────────────────────────────────────────────────────────────

function buildSourceEntry(
	transform: string,
	columnReferences: Array<{ nodeID: string; columnID: string }>,
): ResolvedColumnSource {
	return { transform, columnReferences };
}

function lookupColumn(predCols: Map<string, string>, ...names: string[]): string | undefined {
	for (const name of names) {
		const upper = name.toUpperCase();
		const hit = predCols.get(upper);
		if (hit) { return hit; }
	}
	// Case-insensitive scan as a last-resort fallback (mirrors the Python
	// `_lookup_column` behavior — guards against keys that aren't fully
	// uppercased, e.g. quoted identifiers).
	for (const name of names) {
		const lower = name.toLowerCase();
		for (const [k, v] of predCols) {
			if (k.toLowerCase() === lower) { return v; }
		}
	}
	return undefined;
}

function upperKeyMap(input: Record<string, string>): Map<string, string> {
	const out = new Map<string, string>();
	for (const [k, v] of Object.entries(input)) {
		out.set(k.toUpperCase(), v);
	}
	return out;
}

function upperKeyOuterMap(
	input: Record<string, Record<string, string>>,
): Map<string, Map<string, string>> {
	const out = new Map<string, Map<string, string>>();
	for (const [k, v] of Object.entries(input)) {
		out.set(k, upperKeyMap(v));
	}
	return out;
}
