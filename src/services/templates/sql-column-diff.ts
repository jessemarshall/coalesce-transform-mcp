/**
 * Apply column-level edits from a rendered DDL/DML SQL document back to the
 * cloud workspace node it was rendered from. Lets users tweak the rendered
 * SQL — e.g. remove a column they don't want, change a data type — and have
 * those changes flow back to the node's `metadata.columns[]`.
 *
 * Scope is intentionally narrow: this is a *column-level* diff. The
 * declarative parts of a node (sources, sourceMapping, joinCondition,
 * config, …) are owned by the node YAML, not by the rendered SQL — so we
 * don't try to reverse-engineer joins, CTEs, or WHERE clauses out of edited
 * SQL. Three buckets of change:
 *
 *   - **Removed columns** (in YAML but not in SQL) → dropped from metadata.
 *   - **Type changes** (matching name, different `dataType`) → updated.
 *   - **New columns** (in SQL but not in YAML) → REJECTED, since a new
 *     column needs a source mapping (which column on which upstream node it
 *     comes from) that the SQL alone doesn't carry. Surface a clear error
 *     and tell the user to edit the YAML or use the cloud UI for adds.
 *
 * Matching is by column name (case-insensitive, identifier-quotes stripped),
 * which is how Coalesce already does column lookup elsewhere
 * (see {@link findMatchingBaseColumn} in column-helpers.ts).
 */

import {
	stripIdentifierQuotes,
	findClosingParen,
	findLastTopLevelOpenParen,
	findTopLevelKeywordIndex,
} from "../pipelines/sql-tokenizer.js";
import { parseSqlSelectItems } from "../pipelines/select-parsing.js";
import { parseSqlSourceRefs } from "../pipelines/source-parsing.js";
import { inferDatatype } from "../workspace/join-helpers.js";
import { isPlainObject } from "../../utils.js";
import type {
	InferredColumn,
	ResolvedColumnSource,
} from "./infer-source-mapping.js";

export interface ParsedSqlColumn {
	name: string;
	dataType: string;
	/**
	 * SELECT expression that produces this column. Populated for DML inputs
	 * (`parseSelectColumnsForApply`); undefined for DDL inputs
	 * (`parseCreateTableColumns`) — DDL doesn't carry transforms.
	 *
	 * The apply path uses this to feed source-mapping inference for newly
	 * added columns; without it, adds fall back to the bare-column path.
	 */
	expression?: string;
}

export interface ColumnDiff {
	/** Columns kept with their existing metadata, no change. */
	unchanged: string[];
	/** Columns whose dataType was updated. */
	typeChanged: Array<{ name: string; from: string; to: string }>;
	/**
	 * Columns whose output name changed but whose source mapping (transform
	 * expression) is the same. Detected only for DML inputs — DDL inputs
	 * carry no expressions, so renames there look like drop+add. The apply
	 * path preserves the existing column id / lineage and just renames.
	 */
	renamed: Array<{ from: string; to: string; expression: string }>;
	/**
	 * Columns where the name is unchanged but the SQL expression (transform)
	 * has been edited. Detected only for DML inputs. The apply path updates
	 * `sources[0].transform` in place; if the new expression references
	 * different upstream columns, the apply path also re-runs source-mapping
	 * inference to refresh `columnReferences`.
	 */
	expressionChanged: Array<{ name: string; from: string; to: string }>;
	/** Columns dropped from the node (present in YAML, missing in SQL). */
	removed: string[];
	/**
	 * Columns present in the SQL but not in the YAML. Returned for caller
	 * inspection. The apply path is expected to build cloud-shape entries
	 * (via source-mapping inference for DML, or empty `sources` for DDL)
	 * and pass them to `applyColumnDiff` via `options.addedColumnsByName`.
	 */
	added: ParsedSqlColumn[];
}

/**
 * Parse a CREATE [OR REPLACE] TABLE statement and extract its column list.
 * Tolerates Snowflake / generic-SQL syntax: identifier quoting, qualified
 * table names (`db.schema.table`), trailing column modifiers like
 * `identity`, `not null`, `default X`, etc.
 *
 * Returns undefined when the input doesn't look like a CREATE TABLE — the
 * caller should fall back to a SELECT-list parser (e.g., parseSqlSelectItems)
 * for DML inputs.
 */
export function parseCreateTableColumns(sql: string): ParsedSqlColumn[] | undefined {
	// Strip line comments and block comments so they don't confuse the parser.
	const stripped = sql
		.replace(/--[^\n]*/g, '')
		.replace(/\/\*[\s\S]*?\*\//g, '');

	// Match: CREATE [OR REPLACE] [TEMP[ORARY]] TABLE [IF NOT EXISTS] <name> ( <body> )
	// The body is everything between the matching parens. We rely on a regex
	// to find the opening `(` then walk the string to find the matching close,
	// because nested parens inside expressions (e.g. NUMBER(38,0)) would
	// confuse a naive [^()] regex.
	const headerMatch = /\bcreate\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?table\s+(?:if\s+not\s+exists\s+)?[^(]+\(/i.exec(stripped);
	if (!headerMatch) { return undefined; }

	const openIdx = headerMatch.index + headerMatch[0].length - 1;
	const body = readBalancedParens(stripped, openIdx);
	if (!body) { return undefined; }

	const items = splitTopLevelCommas(body);
	const columns: ParsedSqlColumn[] = [];
	for (const item of items) {
		const trimmed = item.trim();
		if (!trimmed) { continue; }
		// Skip table-level constraints like PRIMARY KEY (...) — they aren't columns.
		if (/^(?:constraint\b|primary\s+key\b|foreign\s+key\b|unique\s+\(|check\s*\()/i.test(trimmed)) { continue; }

		// First token = identifier (possibly quoted). Everything after is the
		// data type and modifiers; we treat all of it as the dataType for
		// round-tripping purposes — coa stores the verbatim type string.
		const nameMatch = /^("[^"]+"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_$]*)\s+(.+)$/s.exec(trimmed);
		if (!nameMatch) { continue; }
		const name = stripIdentifierQuotes(nameMatch[1]);
		const dataType = nameMatch[2].trim();
		columns.push({ name, dataType: cleanDataType(dataType) });
	}

	return columns.length > 0 ? columns : undefined;
}

/**
 * Strip leading whitespace and comments and return whether the SQL starts
 * with `MERGE INTO`. Coalesce dimension / Type-2 SCD nodes always render as
 * MERGE, so the apply path needs to recognize them as DML.
 */
export function isMergeShape(sql: string): boolean {
	const stripped = sql.replace(/^(?:\s|--[^\n]*\n|\/\*[\s\S]*?\*\/)+/, "");
	return /^merge\s+into\s+/i.test(stripped);
}

/**
 * Strip leading whitespace and comments and return whether the SQL starts
 * with `INSERT`. Coalesce fact / staging / work nodes typically render as
 * `INSERT INTO target ( cols ) ( SELECT ... )`, where the SELECT is wrapped
 * in parens — those need envelope peeling for the SELECT-list parser.
 */
export function isInsertShape(sql: string): boolean {
	const stripped = sql.replace(/^(?:\s|--[^\n]*\n|\/\*[\s\S]*?\*\/)+/, "");
	return /^insert\s+/i.test(stripped);
}

/**
 * Peel a MERGE envelope down to its inner SELECT. Returns the inner SELECT
 * as a standalone string, or undefined when the input isn't a MERGE or the
 * envelope can't be parsed.
 *
 * Coalesce typically renders dim nodes as
 *   MERGE INTO <target> "TGT" USING ( ( SELECT ... FROM ... ) ) "SRC" ON ...
 * The SELECT can be wrapped in one or more layers of parens. We find the
 * USING ( ... ), capture the balanced body, then peel any single-paren
 * wrappers until we land on a string starting with SELECT or WITH.
 *
 * Everything outside the inner SELECT (the MERGE header, ON condition, WHEN
 * MATCHED / WHEN NOT MATCHED clauses, target alias) is discarded — those are
 * Coalesce-managed and not editable via the apply path.
 */
export function peelMergeEnvelope(sql: string): string | undefined {
	if (!isMergeShape(sql)) { return undefined; }
	const usingIdx = findTopLevelKeywordIndex(sql, "using");
	if (usingIdx < 0) { return undefined; }
	let i = usingIdx + "using".length;
	while (i < sql.length && /\s/.test(sql[i]!)) { i++; }
	if (sql[i] !== "(") { return undefined; }
	const closeIdx = findClosingParen(sql, i + 1);
	if (closeIdx < 0) { return undefined; }
	let inner = sql.slice(i + 1, closeIdx).trim();
	// Peel single-paren wrappers (only when the trailing `)` closes the
	// leading `(` — guarded by re-running findClosingParen on the leading
	// paren so `(a) OR (b)` doesn't get mis-peeled).
	while (inner.startsWith("(")) {
		const innerClose = findClosingParen(inner, 1);
		if (innerClose !== inner.length - 1) { break; }
		inner = inner.slice(1, -1).trim();
	}
	return inner;
}

/**
 * Peel an INSERT envelope down to its inner SELECT. Returns the inner
 * SELECT, or undefined when the input isn't an INSERT or the envelope
 * doesn't wrap the SELECT in parens (i.e. plain `INSERT INTO target SELECT
 * ...` — already top-level, no peel needed).
 *
 * Coalesce typically renders fact / staging / work nodes as
 *   INSERT INTO <target> ( <cols> ) ( SELECT ... FROM ... GROUP BY ... )
 * The SELECT is in the LAST top-level paren; the FIRST is the column list.
 * If the SQL is INSERT but already has a top-level SELECT (no parens around
 * it), we return undefined so callers fall through to using the raw SQL.
 */
export function peelInsertEnvelope(sql: string): string | undefined {
	if (!isInsertShape(sql)) { return undefined; }
	if (findTopLevelKeywordIndex(sql, "select") >= 0) { return undefined; }
	const lastOpen = findLastTopLevelOpenParen(sql);
	if (lastOpen < 0) { return undefined; }
	const closeIdx = findClosingParen(sql, lastOpen + 1);
	if (closeIdx < 0) { return undefined; }
	let inner = sql.slice(lastOpen + 1, closeIdx).trim();
	while (inner.startsWith("(")) {
		const innerClose = findClosingParen(inner, 1);
		if (innerClose !== inner.length - 1) { break; }
		inner = inner.slice(1, -1).trim();
	}
	return inner;
}

/**
 * Peel any DML envelope (MERGE or INSERT-with-paren-wrapped-SELECT) down to
 * its inner SELECT. Returns undefined when no peel is needed (plain SELECT,
 * `INSERT INTO target SELECT ...` without paren wrapping, etc.) — callers
 * should fall back to the raw SQL in that case.
 */
export function peelDmlEnvelope(sql: string): string | undefined {
	return peelMergeEnvelope(sql) ?? peelInsertEnvelope(sql);
}

/**
 * Parse a DML-shaped SQL document (the output of `coa_dry_run_run` — an
 * INSERT INTO … SELECT, a bare SELECT, or a MERGE INTO … USING ( SELECT … ))
 * and extract the SELECT-list as parsed columns. Both MERGE and
 * paren-wrapped INSERT envelopes are peeled to the inner SELECT before
 * parsing, so the user's editable surface is the SELECT only.
 *
 * Each column's `expression` is populated so the apply path can pass it to
 * source-mapping inference; `dataType` is best-effort via {@link inferDatatype}
 * and may be empty when no pattern matches (callers should fall back to the
 * existing column's dataType for those).
 *
 * Returns undefined when the input has no recognizable SELECT-list.
 */
export function parseSelectColumnsForApply(sql: string): ParsedSqlColumn[] | undefined {
	const target = peelDmlEnvelope(sql) ?? sql;
	const sourceParse = parseSqlSourceRefs(target);
	const { selectItems } = parseSqlSelectItems(target, sourceParse.refs);
	if (selectItems.length === 0) { return undefined; }

	const columns: ParsedSqlColumn[] = [];
	for (const item of selectItems) {
		// Wildcards (`*` / `ALIAS.*`) don't map to a single output column —
		// the rendered DML expands them, but we shouldn't fabricate a column
		// for the unexpanded form. Skip silently; type-changes/removes still
		// work for everything else.
		if (item.sourceColumnName === "*") { continue; }
		const name = item.outputName ?? item.sourceColumnName;
		if (!name) { continue; }
		columns.push({
			name,
			dataType: inferDatatype(item.expression) ?? "",
			expression: item.expression,
		});
	}
	return columns.length > 0 ? columns : undefined;
}

/**
 * Diff a parsed SQL column list against an existing cloud node's
 * `metadata.columns[]`. The diff is name-keyed — column names are the
 * stable identity from the user's perspective.
 *
 * When a parsed column has no `dataType` (e.g. DML where {@link inferDatatype}
 * couldn't match), type-change detection is skipped for that column — we'd
 * rather report `unchanged` than spurious type churn.
 */
export function diffColumns(
	parsed: ParsedSqlColumn[],
	existing: unknown[],
): ColumnDiff {
	const parsedByName = new Map<string, ParsedSqlColumn>();
	for (const col of parsed) {
		parsedByName.set(normalizeName(col.name), col);
	}

	const existingByName = new Map<string, Record<string, unknown>>();
	for (const col of existing) {
		if (!isPlainObject(col)) { continue; }
		const name = typeof col.name === 'string' ? col.name : undefined;
		if (!name) { continue; }
		existingByName.set(normalizeName(name), col);
	}

	const unchanged: string[] = [];
	const typeChanged: Array<{ name: string; from: string; to: string }> = [];
	const expressionChanged: Array<{ name: string; from: string; to: string }> = [];
	const tentativelyRemoved: Array<{ name: string; expression: string }> = [];
	const tentativelyAdded: ParsedSqlColumn[] = [];

	// Pass 1 — name-keyed match. Columns matched here can move into
	// unchanged / typeChanged / expressionChanged buckets.
	for (const [normName, existingCol] of existingByName) {
		const displayName = typeof existingCol.name === 'string' ? existingCol.name : normName;
		const parsedCol = parsedByName.get(normName);
		if (!parsedCol) {
			tentativelyRemoved.push({
				name: displayName,
				expression: extractExistingTransform(existingCol),
			});
			continue;
		}
		const existingType = typeof existingCol.dataType === 'string' ? existingCol.dataType : '';
		// dataType comparison: only flag a type change when the parsed
		// column actually carries a type (DML inputs may have empty
		// dataType when `inferDatatype` couldn't match). Same rule as
		// before — preserves existing types on round-trips.
		const typeWasChanged = parsedCol.dataType
			&& normalizeType(existingType) !== normalizeType(parsedCol.dataType);

		// Expression comparison runs only when the parsed column carries
		// an expression (DML path). DDL inputs leave it undefined so we
		// silently skip — type-changes and renames-by-name still work.
		const existingTransform = extractExistingTransform(existingCol);
		const exprWasChanged =
			parsedCol.expression !== undefined
			&& parsedCol.expression !== ""
			&& existingTransform !== ""
			&& normalizeExpression(parsedCol.expression) !== normalizeExpression(existingTransform);

		if (typeWasChanged) {
			typeChanged.push({ name: displayName, from: existingType, to: parsedCol.dataType });
		}
		if (exprWasChanged) {
			expressionChanged.push({
				name: displayName,
				from: existingTransform,
				to: parsedCol.expression!,
			});
		}
		if (!typeWasChanged && !exprWasChanged) {
			unchanged.push(displayName);
		}
	}

	for (const [normName, parsedCol] of parsedByName) {
		if (!existingByName.has(normName)) {
			tentativelyAdded.push(parsedCol);
		}
	}

	// Pass 2 — pair tentative removes with tentative adds when their
	// expressions match exactly. That's a rename: same column, new output
	// name. Lineage is preserved by the apply path.
	//
	// The migration-agents pipeline_builder/column_builder reuses an
	// existing columnID only when the *name* matches (positional reuse is
	// unsafe — see column_builder.py:281). That rule is correct for
	// regenerate-from-SQL flows where the user's intent is to rebuild.
	// Here we extend it with a transform-pair check so interactive renames
	// preserve lineage instead of dropping+re-adding (which loses the
	// downstream column references). The check is conservative: ambiguous
	// matches (multiple removes or adds sharing the same expression) are
	// NOT paired — left as drop+add for the user to disambiguate.
	//
	// Skipped entirely when expressions are missing on either side (DDL
	// inputs, or existing columns with no `sources[0].transform`).
	const renamed: Array<{ from: string; to: string; expression: string }> = [];
	const removedByExpr = bucketByExpression(tentativelyRemoved.map(r => ({
		key: r.name,
		expression: r.expression,
	})));
	const addedByExpr = bucketByExpression(tentativelyAdded
		.filter(a => a.expression !== undefined && a.expression !== "")
		.map(a => ({ key: a.name, expression: a.expression! })));

	// Re-index existing + parsed by name so a paired rename can also pick up
	// a simultaneous dataType change. Without this, a rename whose type is
	// also being modified would silently land the new dataType (the apply
	// path applies it via the rename entry's parsed dataType) but the diff
	// returned to the user would NOT mention the type change — `dryRun: true`
	// would mislead about what the apply will write. So when we pair a
	// rename, also compare the from/to dataTypes and append a typeChanged
	// entry under the *new* name when they differ.
	const existingByDisplayName = new Map<string, Record<string, unknown>>();
	for (const col of existingByName.values()) {
		const display = typeof col.name === 'string' ? col.name : '';
		if (display) { existingByDisplayName.set(display, col); }
	}
	const parsedByDisplayName = new Map<string, ParsedSqlColumn>();
	for (const col of parsed) { parsedByDisplayName.set(col.name, col); }

	const renamedFromKeys = new Set<string>();
	const renamedToKeys = new Set<string>();
	for (const [expr, removedKeys] of removedByExpr) {
		const addedKeys = addedByExpr.get(expr);
		if (!addedKeys) { continue; }
		if (removedKeys.length === 1 && addedKeys.length === 1) {
			const fromName = removedKeys[0];
			const toName = addedKeys[0];
			renamed.push({ from: fromName, to: toName, expression: expr });
			renamedFromKeys.add(fromName);
			renamedToKeys.add(toName);

			const oldCol = existingByDisplayName.get(fromName);
			const newCol = parsedByDisplayName.get(toName);
			const oldType = typeof oldCol?.dataType === 'string' ? oldCol.dataType : '';
			const newType = newCol?.dataType ?? '';
			if (newType && normalizeType(oldType) !== normalizeType(newType)) {
				typeChanged.push({ name: toName, from: oldType, to: newType });
			}
		}
	}

	const removed = tentativelyRemoved
		.filter(r => !renamedFromKeys.has(r.name))
		.map(r => r.name);
	const added = tentativelyAdded.filter(a => !renamedToKeys.has(a.name));

	return { unchanged, typeChanged, renamed, expressionChanged, removed, added };
}

/**
 * Extract a column's first-source transform from a cloud-shape column
 * record. Returns "" when no source / transform is set so callers can do
 * an empty-check rather than worrying about nesting.
 */
function extractExistingTransform(col: Record<string, unknown>): string {
	const sources = Array.isArray(col.sources) ? col.sources : undefined;
	if (!sources || sources.length === 0) { return ""; }
	const first = sources[0];
	if (!isPlainObject(first)) { return ""; }
	return typeof first.transform === 'string' ? first.transform : "";
}

/**
 * Normalize an expression for comparison. Conservative on purpose —
 * Coalesce's renderer is consistent with quoting and casing, so two
 * expressions that should match will only differ in whitespace, not
 * case or quote style.
 *
 *   1. Trim and collapse internal whitespace runs to single spaces
 *      (so `COUNT(X,\n  Y)` matches `COUNT(X, Y)`).
 *   2. Drop whitespace adjacent to non-word punctuation
 *      (so `COUNT( X.A )` matches `COUNT(X.A)` and `X . A` matches `X.A`).
 *
 * Whitespace BETWEEN two identifier characters is preserved so keyword
 * boundaries like `X AND Y` don't collapse to `XANDY`.
 */
function normalizeExpression(expr: string): string {
	return expr
		.trim()
		.replace(/\s+/g, ' ')
		.replace(/\s*([^\w\s])\s*/g, '$1');
}

function bucketByExpression(
	items: Array<{ key: string; expression: string }>,
): Map<string, string[]> {
	const out = new Map<string, string[]>();
	for (const item of items) {
		const norm = normalizeExpression(item.expression);
		if (!norm) { continue; }
		const list = out.get(norm) ?? [];
		list.push(item.key);
		out.set(norm, list);
	}
	return out;
}

/**
 * Build a new `metadata.columns[]` array applying the diff. Output is
 * ordered to match the parsed SQL — a reorder in the SELECT list is
 * reflected in the node's column order. Existing column metadata
 * (`id`/`columnID`, `nullable`, `description`, …) is preserved across
 * unchanged, type-changed, expression-changed, and renamed columns so
 * downstream nodes that reference these columns by id keep working.
 *
 * Per-column behavior:
 *   - **unchanged / type-changed** — copy existing entry; update `dataType`
 *     when the parsed column carries one.
 *   - **renamed** — copy existing entry under the new name (id preserved).
 *   - **expression-changed** — copy existing entry; replace `sources[]`
 *     with `options.updatedSourcesByName.get(name)` when supplied (the
 *     apply path re-runs source-mapping inference for the new transform);
 *     otherwise patch `sources[0].transform` only and warn that lineage
 *     refs may be stale.
 *   - **added** — use `options.addedColumnsByName.get(name)`; the apply
 *     path builds these via source-mapping inference.
 *   - **removed** — silently dropped.
 *
 * Throws when adds exist but no `addedColumnsByName` is provided — it's a
 * caller bug to ignore the diff's adds, and silently dropping them would be
 * worse than erroring.
 *
 * **Aliasing:** the returned entries are shallow-copies of the input
 * `existing` rows (`{ ...existingCol }`). Nested arrays/objects — notably
 * `sources[]` and the entries inside it — are reference-shared with the
 * input. The current single-pass apply-and-write flow doesn't mutate the
 * result before sending to `set_workspace_node`, so this is safe in practice;
 * future callers that mutate the returned columns must `structuredClone`
 * first or accept that they're modifying the original cloud body in place.
 */
export function applyColumnDiff(
	parsed: ParsedSqlColumn[],
	existing: unknown[],
	diff: ColumnDiff,
	options?: {
		/**
		 * Pre-built cloud-shape entries for added columns, keyed by
		 * {@link normalizeColumnKey}. Build via {@link inferColumnFromAddedItem}
		 * in the apply path. Required when `diff.added.length > 0`.
		 */
		addedColumnsByName?: Map<string, InferredColumn["column"]>;
		/**
		 * For expression-changed columns: the re-resolved `sources[]` array
		 * (cloud shape — `[{ transform, columnReferences }]`), keyed by
		 * {@link normalizeColumnKey}. The apply path builds these by running
		 * `resolveColumnSources` against the new transform; without it, we
		 * fall back to patching only the transform string on `sources[0]`,
		 * which can leave stale `columnReferences`.
		 */
		updatedSourcesByName?: Map<string, ResolvedColumnSource[]>;
	},
): unknown[] {
	const addedColumnsByName = options?.addedColumnsByName;
	const updatedSourcesByName = options?.updatedSourcesByName;
	if (diff.added.length > 0 && !addedColumnsByName) {
		throw new Error(
			`Cannot apply edits: ${diff.added.length} new column(s) (${diff.added.map(a => a.name).join(', ')}) `
			+ `appear in the SQL but no inferred column entries were supplied. The apply path is `
			+ `responsible for building these — pass them via options.addedColumnsByName.`,
		);
	}

	const existingByName = new Map<string, Record<string, unknown>>();
	for (const col of existing) {
		if (!isPlainObject(col)) { continue; }
		const name = typeof col.name === 'string' ? col.name : undefined;
		if (!name) { continue; }
		existingByName.set(normalizeName(name), col);
	}

	// rename: new-name → old-name lookup so we can pull the existing
	// entry when a parsed column appears under a fresh alias.
	const renameToFrom = new Map<string, string>();
	for (const r of diff.renamed) {
		renameToFrom.set(normalizeName(r.to), r.from);
	}
	const expressionChangedByName = new Set<string>();
	for (const e of diff.expressionChanged) {
		expressionChangedByName.add(normalizeName(e.name));
	}

	const out: unknown[] = [];
	// Iterate in parsed-SQL order so reorders in the SELECT list flow
	// through to the node's column order.
	for (const parsedCol of parsed) {
		const norm = normalizeName(parsedCol.name);

		// 1. Rename: pull the existing entry by its OLD name, rename it.
		const renamedFromName = renameToFrom.get(norm);
		if (renamedFromName !== undefined) {
			const existingCol = existingByName.get(normalizeName(renamedFromName));
			if (!existingCol) {
				// Invariant: a `renamed` entry was paired in `diffColumns`
				// against a row that exists in `existingByName`. If we get
				// here, the caller passed mismatched `parsed` / `existing` /
				// `diff` arguments — silently dropping the column would lose
				// data, so fail loudly.
				throw new Error(
					`applyColumnDiff invariant: rename target "${parsedCol.name}" was paired `
					+ `with source "${renamedFromName}", but that row is missing from the `
					+ `existing columns. Re-run diffColumns against the same existing array.`,
				);
			}
			out.push({
				...existingCol,
				name: parsedCol.name,
				...(parsedCol.dataType ? { dataType: parsedCol.dataType } : {}),
			});
			continue;
		}

		// 2. Add: use the pre-built cloud-shape entry.
		const built = addedColumnsByName?.get(norm);
		if (built !== undefined) {
			out.push(built);
			continue;
		}

		// 3. Match by name — unchanged / type-changed / expression-changed.
		const existingCol = existingByName.get(norm);
		if (!existingCol) { continue; }

		const updated: Record<string, unknown> = { ...existingCol };
		if (parsedCol.dataType) { updated.dataType = parsedCol.dataType; }

		if (expressionChangedByName.has(norm)) {
			const newSources = updatedSourcesByName?.get(norm);
			if (newSources) {
				updated.sources = newSources;
			} else {
				// Caller didn't pass re-resolved sources — patch the
				// transform on the first source so the node still renders
				// the user's edited expression. `columnReferences` may be
				// stale; a warning at the apply-path level should surface
				// that to the user.
				const sources = Array.isArray(existingCol.sources) ? existingCol.sources : [];
				const first = sources[0];
				if (isPlainObject(first)) {
					updated.sources = [
						{ ...first, transform: parsedCol.expression ?? first.transform },
						...sources.slice(1),
					];
				} else {
					updated.sources = [{ transform: parsedCol.expression ?? "", columnReferences: [] }];
				}
			}
		}

		out.push(updated);
	}

	return out;
}

// ── internals ──────────────────────────────────────────────────────────────

function readBalancedParens(s: string, openIdx: number): string | undefined {
	if (s[openIdx] !== '(') { return undefined; }
	let depth = 0;
	let inStr: string | null = null;
	for (let i = openIdx; i < s.length; i++) {
		const ch = s[i];
		if (inStr) {
			if (ch === inStr) { inStr = null; }
			continue;
		}
		if (ch === '"' || ch === "'" || ch === '`') {
			inStr = ch;
			continue;
		}
		if (ch === '(') { depth++; continue; }
		if (ch === ')') {
			depth--;
			if (depth === 0) {
				return s.slice(openIdx + 1, i);
			}
		}
	}
	return undefined;
}

function splitTopLevelCommas(s: string): string[] {
	const out: string[] = [];
	let depth = 0;
	let inStr: string | null = null;
	let start = 0;
	for (let i = 0; i < s.length; i++) {
		const ch = s[i];
		if (inStr) {
			if (ch === inStr) { inStr = null; }
			continue;
		}
		if (ch === '"' || ch === "'" || ch === '`') {
			inStr = ch;
			continue;
		}
		if (ch === '(') { depth++; continue; }
		if (ch === ')') { depth--; continue; }
		if (ch === ',' && depth === 0) {
			out.push(s.slice(start, i));
			start = i + 1;
		}
	}
	out.push(s.slice(start));
	return out;
}

/**
 * Canonical column-name key used everywhere this module compares names.
 * Strips identifier quotes (`"FOO"` → `FOO`) and uppercases. Exported so
 * external callers (e.g. the apply path's `addedColumnsByName` map) build
 * keys the same way as `applyColumnDiff`'s lookups — otherwise quoted or
 * mixed-case names from a future parser variant would silently miss.
 */
export function normalizeColumnKey(s: string): string {
	return stripIdentifierQuotes(s).toUpperCase();
}

// Internal alias preserved so the rest of this file's call sites stay readable.
const normalizeName = normalizeColumnKey;

function normalizeType(s: string): string {
	return s.replace(/\s+/g, ' ').trim().toUpperCase();
}

function cleanDataType(s: string): string {
	// Snowflake's CREATE TABLE often appends modifiers like `not null`, `default <x>`,
	// `identity`, `comment '...'`, etc. coa stores just the type expression for the
	// `dataType` field, so strip well-known modifiers when round-tripping. If the
	// caller wants the modifiers preserved, that lives in `nullable` / config.
	return s
		.replace(/\s+(?:not\s+null|null)\s*$/i, '')
		.replace(/\s+identity(?:\s*\([^)]*\))?\s*$/i, '')
		.replace(/\s+default\s+.*$/i, '')
		.replace(/\s+comment\s+'[^']*'\s*$/i, '')
		.trim();
}
