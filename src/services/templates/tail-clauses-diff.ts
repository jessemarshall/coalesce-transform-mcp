/**
 * Iteration 3 of the "edit any rendered SQL → declarative Coalesce node update"
 * project: detect and apply edits to the GROUP BY / ORDER BY tail of a
 * rendered DML.
 *
 * Storage in Coalesce's data model (cloud body):
 *   - `config.groupByAll: boolean` — when true, Coalesce auto-generates
 *     `GROUP BY <non-aggregate columns>` at render time. The tail diff
 *     handles this as a presence toggle: if the user added a GROUP BY
 *     where the node had none, set `groupByAll: true`; vice versa for
 *     removal. Explicit GROUP BY column lists are NOT written — Coalesce
 *     derives them from the column-level aggregate flags.
 *   - `config.orderby: boolean` + `config.orderbycolumn.items[]: {
 *       sortColName: string,
 *       sortOrder: "asc" | "desc",
 *     }` — full ORDER BY support. The parser extracts each column +
 *     direction pair from the user's `ORDER BY a DESC, b ASC` and
 *     populates the items array.
 *
 * Things explicitly NOT in scope here:
 *   - HAVING (aggregation post-filter; goes into joinCondition or a
 *     dedicated config field — needs more research before iteration 4).
 *   - QUALIFY (window-function post-filter; same).
 *   - LIMIT — iteration 4 has its own dedicated module.
 *   - Reordering existing groupBy columns by editing a column-level
 *     aggregate flag — out of scope.
 */
import {
	findTopLevelKeywordIndex,
	stripIdentifierQuotes,
} from "../pipelines/sql-tokenizer.js";

export interface OrderByItem {
	sortColName: string;
	sortOrder: "asc" | "desc";
}

export type TailClausesDiff = {
	groupBy: GroupByDiff;
	orderBy: OrderByDiff;
	/**
	 * ORDER BY items that couldn't be reduced to a column name (function
	 * expressions, computed fields). The apply path should surface these
	 * as warnings so the user knows the items were dropped from
	 * `config.orderbycolumn`.
	 */
	unsupportedOrderByExpressions: string[];
};

export type GroupByDiff =
	| { kind: "identical" }
	| { kind: "added"; /** New value to write into config.groupByAll. */ groupByAll: true }
	| { kind: "removed"; groupByAll: false };

export type OrderByDiff =
	| { kind: "identical" }
	| {
		kind: "added" | "changed";
		/** New value to write into config.orderby. */
		orderby: true;
		/** New items array to write into config.orderbycolumn.items. */
		items: OrderByItem[];
	}
	| { kind: "removed"; orderby: false };

/**
 * Extract the GROUP BY clause body from a rendered SQL document — the
 * substring AFTER `GROUP BY` and BEFORE the next terminator (`HAVING`,
 * `QUALIFY`, `ORDER BY`, `LIMIT`, top-level `;`, or closing paren).
 *
 * Returns undefined when the SQL has no top-level GROUP BY.
 */
export function extractGroupByClause(sql: string): string | undefined {
	return extractByClause(sql, "group", ["having", "qualify", "order", "limit"]);
}

/**
 * Extract the ORDER BY clause body. Same structure as
 * {@link extractGroupByClause} but searches for `ORDER BY` and stops at
 * `LIMIT` / `;` / `)`.
 */
export function extractOrderByClause(sql: string): string | undefined {
	return extractByClause(sql, "order", ["limit"]);
}

/** Shared `<keyword> BY ...` extractor. Confirms the keyword is followed
 *  by `\s+by\b` (so `GROUPING SETS` doesn't false-match) before slicing. */
function extractByClause(
	sql: string,
	keyword: "group" | "order",
	terminators: string[],
): string | undefined {
	const start = findTopLevelKeywordIndex(sql, keyword);
	if (start < 0) { return undefined; }
	const after = start + keyword.length;
	const byMatch = /^\s+by\b/i.exec(sql.slice(after));
	if (!byMatch) { return undefined; }
	const bodyStart = after + byMatch[0].length;
	const endIdx = findTailTerminator(sql, bodyStart, terminators);
	return sql.slice(bodyStart, endIdx).trim();
}

/**
 * Parse an ORDER BY clause body into structured items. Handles:
 *   - Bare column refs: `col1`
 *   - Qualified refs: `T.col1` / `"T"."col1"`
 *   - Direction modifier: `col1 DESC`, `col1 ASC`
 *   - NULLS FIRST / NULLS LAST modifier: stripped (Coalesce v1 doesn't
 *     model NULLS positioning) and noted via {@link parseOrderByItems}
 *     return shape — items themselves don't carry it.
 *   - Multiple items separated by commas
 *
 * Default direction (no modifier) is `asc` (Snowflake's render default
 * when neither `ASC` nor `DESC` appears).
 *
 * Output column names are stored unquoted and uppercase. Expression-form
 * ORDER BY items (`COALESCE(a, 0)`, `LOWER(name)`) are SKIPPED — Coalesce's
 * `orderbycolumn.items[].sortColName` is keyed against `metadata.columns[].name`,
 * not arbitrary SQL. The skipped items are returned via the second return
 * value (`unsupportedExpressions`) so the caller can surface a warning.
 */
export interface ParsedOrderByItems {
	items: OrderByItem[];
	/** Expressions we couldn't reduce to a column name; surfaced as warnings. */
	unsupportedExpressions: string[];
}

export function parseOrderByItems(clause: string): ParsedOrderByItems {
	const items: OrderByItem[] = [];
	const unsupportedExpressions: string[] = [];
	for (const raw of splitTopLevelCommas(clause)) {
		const trimmed = raw.trim();
		if (!trimmed) { continue; }

		// Strip NULLS FIRST / NULLS LAST first — it's a positioning
		// modifier, not a column-name-affecting one. Order matters: must
		// run BEFORE the asc/desc strip because `... NULLS LAST` can
		// follow either form (`X ASC NULLS LAST`, `X DESC NULLS FIRST`,
		// or bare `X NULLS LAST`).
		const withoutNulls = trimmed.replace(/\s+nulls\s+(first|last)\s*$/i, "");

		// Now split the trailing direction. Use a greedy capture on the
		// expression and a fully-anchored asc/desc so we only match a
		// trailing keyword, never one buried inside the expression. If
		// there's no direction, the column-only fallback runs below.
		let expr = withoutNulls;
		let dir: "asc" | "desc" = "asc";
		const dirMatch = /^(.+?)\s+(asc|desc)$/i.exec(withoutNulls);
		if (dirMatch) {
			expr = dirMatch[1].trim();
			dir = dirMatch[2].toLowerCase() as "asc" | "desc";
		}

		// Reject expression-form items (anything containing parens,
		// operators, or whitespace after stripping qualifications). What
		// we WILL accept: a single column or a qualified column ref like
		// `T.col` / `"T"."col"` / `"DB"."L"."T".col`.
		if (!isPlainColumnRef(expr)) {
			unsupportedExpressions.push(trimmed);
			continue;
		}

		// `isPlainColumnRef` guarantees a chain of identifiers joined by
		// dots; pull the last segment via a quote-aware splitter — a
		// naive `expr.split(".")` corrupts quoted identifiers that
		// contain a literal dot (`"My.Col"`).
		const lastSegment = lastDotSegment(expr);
		const sortColName = stripIdentifierQuotes(lastSegment).toUpperCase();
		if (!sortColName) {
			unsupportedExpressions.push(trimmed);
			continue;
		}
		items.push({ sortColName, sortOrder: dir });
	}
	return { items, unsupportedExpressions };
}

/**
 * Return the substring AFTER the last top-level `.` separator in a
 * chained column reference, or the whole string when there's no dot.
 * Quote-aware: a dot inside `"..."`, `` `...` ``, or `[...]` is treated
 * as part of the identifier (so `"My.Col"` stays intact instead of
 * being split into `"My` + `Col"`).
 */
function lastDotSegment(s: string): string {
	let inDoubleQuote = false;
	let inBacktick = false;
	let inBracket = false;
	let lastDotIdx = -1;
	for (let i = 0; i < s.length; i++) {
		const c = s[i]!;
		if (inDoubleQuote) { if (c === '"') { inDoubleQuote = false; } continue; }
		if (inBacktick) { if (c === "`") { inBacktick = false; } continue; }
		if (inBracket) { if (c === "]") { inBracket = false; } continue; }
		if (c === '"') { inDoubleQuote = true; continue; }
		if (c === "`") { inBacktick = true; continue; }
		if (c === "[") { inBracket = true; continue; }
		if (c === ".") { lastDotIdx = i; }
	}
	return lastDotIdx < 0 ? s.trim() : s.slice(lastDotIdx + 1).trim();
}

/**
 * Returns true when `s` is a single column reference: a bare identifier,
 * a quoted identifier, or a chain of those joined by dots. Rejects
 * expressions containing parens, operators, function calls, or
 * whitespace between identifier-like tokens.
 */
function isPlainColumnRef(s: string): boolean {
	const trimmed = s.trim();
	if (!trimmed) { return false; }
	// Allowed: `IDENT`, `"QUOTED IDENT"`, `IDENT.IDENT`, `"X"."Y"`,
	// `"DB"."L"."T".COL`, etc. NOT allowed: `func(x)`, `a + b`, `CASE...END`.
	return /^(?:"[^"]+"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*)(?:\s*\.\s*(?:"[^"]+"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*))*$/.test(trimmed);
}

/**
 * Top-level diff entry. Compares the user's rendered SQL against the
 * existing node's `config.groupByAll`, `config.orderby`, and
 * `config.orderbycolumn` and returns the bucketed verdict.
 *
 * The apply path consumes `groupBy` and `orderBy` independently — only
 * non-`identical` outcomes need to be written.
 */
export function diffTailClauses(
	userSql: string,
	existingConfig: {
		groupByAll?: boolean;
		orderby?: boolean;
		orderbycolumn?: { items?: Array<Record<string, unknown>> };
	},
): TailClausesDiff {
	const userGroupBy = extractGroupByClause(userSql);
	const userOrderBy = extractOrderByClause(userSql);

	const userHasGroupBy = userGroupBy !== undefined;
	const existingGroupByAll = existingConfig.groupByAll === true;

	let groupBy: GroupByDiff;
	if (userHasGroupBy === existingGroupByAll) {
		groupBy = { kind: "identical" };
	} else if (userHasGroupBy) {
		groupBy = { kind: "added", groupByAll: true };
	} else {
		groupBy = { kind: "removed", groupByAll: false };
	}

	const userParsed = userOrderBy !== undefined
		? parseOrderByItems(userOrderBy)
		: { items: [], unsupportedExpressions: [] };
	const userItems = userParsed.items;
	const unsupportedOrderByExpressions = userParsed.unsupportedExpressions;
	const existingOrderby = existingConfig.orderby === true;
	const existingItems = extractExistingOrderByItems(existingConfig.orderbycolumn?.items);

	// User wrote an ORDER BY but produced no usable items. Two flavors:
	//   (a) every item was an unsupported expression (`ORDER BY COALESCE(a, 0)`)
	//   (b) the clause was empty / mangled (`ORDER BY` with nothing after)
	// Both could plausibly happen via copy-paste truncation or partially-
	// edited SQL. Treating either as `removed` would silently wipe the
	// node's existing orderby config — strictly worse than doing nothing.
	// Preserve the existing config and let the
	// `unsupportedOrderByExpressions` warnings (when present) tell the
	// user their intent wasn't applied.
	const userIntendedOrderBy = userOrderBy !== undefined;
	const userProducedNoUsableItems = userIntendedOrderBy && userItems.length === 0;

	let orderBy: OrderByDiff;
	if (userProducedNoUsableItems) {
		orderBy = { kind: "identical" };
	} else if (userItems.length === 0 && !existingOrderby) {
		orderBy = { kind: "identical" };
	} else if (userItems.length === 0 && existingOrderby) {
		orderBy = { kind: "removed", orderby: false };
	} else if (userItems.length > 0 && !existingOrderby) {
		orderBy = { kind: "added", orderby: true, items: userItems };
	} else if (orderByItemsEqual(userItems, existingItems)) {
		orderBy = { kind: "identical" };
	} else {
		orderBy = { kind: "changed", orderby: true, items: userItems };
	}

	return { groupBy, orderBy, unsupportedOrderByExpressions };
}

// ── helpers ─────────────────────────────────────────────────────────────────

/**
 * Find the smallest index strictly after `startIdx` at which a top-level
 * terminator keyword (one of `terminators`) or `;`/`)` appears. Returns
 * `sql.length` when no terminator is found.
 *
 * Uses {@link findTopLevelKeywordIndex} which already skips inside
 * parens / strings / comments. The `;` and `)` walk falls through the
 * native string semantics (we don't expect them to terminate inside
 * a string in practice).
 */
function findTailTerminator(sql: string, startIdx: number, terminators: string[]): number {
	let endIdx = sql.length;
	for (const term of terminators) {
		const idx = findTopLevelKeywordIndex(sql, term, startIdx);
		if (idx >= 0 && idx < endIdx) { endIdx = idx; }
	}
	// Top-level semicolon / unmatched close-paren: scan for the earliest
	// of either. Quote/comment-aware via a small inline walker.
	const breakIdx = findFirstTopLevelBreak(sql, startIdx);
	if (breakIdx >= 0 && breakIdx < endIdx) { endIdx = breakIdx; }
	return endIdx;
}

/**
 * Find the index of the first `;` or unmatched `)` at or after
 * `startIdx`, accounting for string literals and comments. Mirrors the
 * walker in `from-block-diff.ts` but doesn't track paren depth past 0
 * (the caller is operating on a slice of an outer SQL block).
 */
function findFirstTopLevelBreak(s: string, startIdx: number): number {
	let inSingleQuote = false;
	let inDoubleQuote = false;
	let inBacktick = false;
	let inBracket = false;
	let inLineComment = false;
	let inBlockComment = false;
	let parenDepth = 0;
	for (let i = startIdx; i < s.length; i++) {
		const c = s[i]!;
		const next = s[i + 1];
		if (inLineComment) { if (c === "\n") { inLineComment = false; } continue; }
		if (inBlockComment) { if (c === "*" && next === "/") { inBlockComment = false; i++; } continue; }
		if (inSingleQuote) {
			if (c === "'" && next === "'") { i++; }
			else if (c === "'") { inSingleQuote = false; }
			continue;
		}
		if (inDoubleQuote) { if (c === '"') { inDoubleQuote = false; } continue; }
		if (inBacktick) { if (c === "`") { inBacktick = false; } continue; }
		if (inBracket) { if (c === "]") { inBracket = false; } continue; }
		if (c === "'") { inSingleQuote = true; continue; }
		if (c === '"') { inDoubleQuote = true; continue; }
		if (c === "`") { inBacktick = true; continue; }
		if (c === "[") { inBracket = true; continue; }
		if (c === "-" && next === "-") { inLineComment = true; i++; continue; }
		if (c === "/" && next === "*") { inBlockComment = true; i++; continue; }
		if (c === "(") { parenDepth++; continue; }
		if (c === ")") {
			if (parenDepth === 0) { return i; }
			parenDepth--;
			continue;
		}
		if (c === ";" && parenDepth === 0) { return i; }
	}
	return -1;
}

/**
 * Split a comma-separated list while respecting paren-nesting and
 * string literals. ORDER BY items can include function calls (`NULLS
 * FIRST` extensions, `COALESCE(a, b) DESC`) so a naive `.split(",")`
 * breaks them.
 */
function splitTopLevelCommas(s: string): string[] {
	const out: string[] = [];
	let parenDepth = 0;
	let inSingleQuote = false;
	let inDoubleQuote = false;
	let last = 0;
	for (let i = 0; i < s.length; i++) {
		const c = s[i]!;
		const next = s[i + 1];
		if (inSingleQuote) {
			if (c === "'" && next === "'") { i++; }
			else if (c === "'") { inSingleQuote = false; }
			continue;
		}
		if (inDoubleQuote) { if (c === '"') { inDoubleQuote = false; } continue; }
		if (c === "'") { inSingleQuote = true; continue; }
		if (c === '"') { inDoubleQuote = true; continue; }
		if (c === "(") { parenDepth++; continue; }
		if (c === ")") { if (parenDepth > 0) { parenDepth--; } continue; }
		if (c === "," && parenDepth === 0) {
			out.push(s.slice(last, i));
			last = i + 1;
		}
	}
	out.push(s.slice(last));
	return out;
}

function extractExistingOrderByItems(
	items: Array<Record<string, unknown>> | undefined,
): OrderByItem[] {
	if (!items) { return []; }
	const out: OrderByItem[] = [];
	for (const item of items) {
		const sortColName = typeof item.sortColName === "string" ? item.sortColName : "";
		if (!sortColName) { continue; }
		const dir = typeof item.sortOrder === "string" && item.sortOrder.toLowerCase() === "desc"
			? "desc"
			: "asc";
		out.push({ sortColName: sortColName.toUpperCase(), sortOrder: dir });
	}
	return out;
}

function orderByItemsEqual(a: OrderByItem[], b: OrderByItem[]): boolean {
	if (a.length !== b.length) { return false; }
	for (let i = 0; i < a.length; i++) {
		if (a[i].sortColName !== b[i].sortColName) { return false; }
		if (a[i].sortOrder !== b[i].sortOrder) { return false; }
	}
	return true;
}
