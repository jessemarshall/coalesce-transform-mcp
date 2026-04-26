/**
 * Iteration 5 of the "edit any rendered SQL → declarative Coalesce node update"
 * project: detect and apply edits to the INSERT INTO header of a rendered DML.
 *
 * The rendered DML looks like:
 *
 *     INSERT INTO "DB"."LOC"."NAME" (
 *         "COL1",
 *         "COL2",
 *         …
 *     ) (
 *         SELECT … FROM …
 *     )
 *
 * Two editable parts of the header:
 *   1. **Target identifier** (`"DB"."LOC"."NAME"`) — corresponds to the
 *      cloud node's `database` / `locationName` / `name` top-level fields.
 *      Editing this renames or re-locates the node.
 *   2. **Target column list** — the parenthesized list right after the
 *      table identifier. Coalesce auto-generates this from
 *      `metadata.columns[].name`, in `metadata.columns[]` order. The
 *      user can't meaningfully edit just the INSERT column list — that
 *      would desync from the SELECT list. The diff detects this case
 *      and rejects, telling the user to edit the SELECT list (which
 *      iteration 1 already handles) instead.
 *
 * Runs alongside iteration 1's column diff: the SELECT list is already
 * the source of truth for column add/remove/rename/reorder. Here we
 * only look at the INSERT header's target identifier and validate that
 * its column list matches what the SELECT projects.
 */

export interface InsertTargetIdentifier {
	database: string;
	locationName: string;
	name: string;
}

export type InsertHeaderDiff =
	| { kind: "identical" }
	| {
		kind: "targetChanged";
		from: InsertTargetIdentifier;
		to: InsertTargetIdentifier;
		/** Which fields actually changed — surfaced in the warning so
		 *  the user knows whether they're renaming, re-locating, or
		 *  changing databases. */
		changedFields: Array<"database" | "locationName" | "name">;
	}
	| {
		kind: "columnListMismatch";
		insertColumns: string[];
		selectColumns: string[];
		/** Human-readable explanation of the mismatch (extra/missing/reordered). */
		reason: string;
	}
	| {
		/** SQL isn't INSERT-shaped (e.g. CREATE TABLE DDL) — no header to diff. */
		kind: "notApplicable";
	}
	| {
		/**
		 * SQL starts with INSERT but the header couldn't be parsed
		 * (e.g. two-part identifier instead of three-part, missing
		 * column list, unbalanced parens). Surfaced as a warning so the
		 * user knows their header edit was ignored, rather than
		 * silently flowing through as `notApplicable`.
		 */
		kind: "malformedHeader";
		reason: string;
	};

/**
 * Result of parsing an INSERT header: either the parsed shape, a
 * malformed-INSERT marker (so callers can warn), or null when the SQL
 * isn't INSERT-shaped at all.
 */
export type ExtractedInsertHeader =
	| { kind: "ok"; target: InsertTargetIdentifier; columns: string[] }
	| { kind: "malformed"; reason: string };

/**
 * Parse the INSERT INTO header out of a rendered DML. Returns:
 *   - `null` when the SQL doesn't start with INSERT (lets non-INSERT
 *     inputs flow through as `notApplicable`).
 *   - `{kind: "malformed", reason}` when SQL starts with INSERT but
 *     the header doesn't parse (two-part identifier, missing column
 *     list, unbalanced parens).
 *   - `{kind: "ok", target, columns}` for a well-formed header.
 */
export function extractInsertHeader(sql: string): ExtractedInsertHeader | null {
	const stripped = sql.replace(/^(?:\s|--[^\n]*\n|\/\*[\s\S]*?\*\/)+/, "").trimStart();
	if (!/^insert\s+into\s+/i.test(stripped)) { return null; }

	// Match the target identifier: `"DB"."LOC"."NAME"` (or two-part /
	// bare-name fallbacks). Capture the three (or two, or one) parts.
	const targetMatch =
		/^insert\s+into\s+(?:"([^"]+)"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))\s*\.\s*(?:"([^"]+)"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))\s*\.\s*(?:"([^"]+)"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))/i
			.exec(stripped);
	if (!targetMatch) {
		return {
			kind: "malformed",
			reason: "INSERT INTO target must be a three-part identifier "
				+ "(`\"DB\".\"LOC\".\"NAME\"`); two-part / bare forms aren't supported.",
		};
	}
	const database = targetMatch[1] ?? targetMatch[2] ?? targetMatch[3] ?? targetMatch[4] ?? "";
	const locationName = targetMatch[5] ?? targetMatch[6] ?? targetMatch[7] ?? targetMatch[8] ?? "";
	const name = targetMatch[9] ?? targetMatch[10] ?? targetMatch[11] ?? targetMatch[12] ?? "";

	// Find the column list — the first `(...)` after the target identifier.
	const afterTarget = stripped.slice(targetMatch.index + targetMatch[0].length);
	const parenStart = afterTarget.indexOf("(");
	if (parenStart < 0) {
		return {
			kind: "malformed",
			reason: "INSERT INTO header is missing the parenthesized column list.",
		};
	}
	const parenEnd = findMatchingClose(afterTarget, parenStart);
	if (parenEnd < 0) {
		return {
			kind: "malformed",
			reason: "INSERT INTO column-list parens don't balance.",
		};
	}
	const colListBody = afterTarget.slice(parenStart + 1, parenEnd);
	const columns = parseInsertColumnList(colListBody);

	return { kind: "ok", target: { database, locationName, name }, columns };
}

/**
 * Compare the user's INSERT header against the existing cloud node and
 * produce the diff. Pass the SELECT list's output column names (already
 * extracted by iteration 1's `parseSelectColumnsForApply`) for the
 * column-list-consistency check.
 */
export function diffInsertHeader(
	userSql: string,
	existingNode: { database?: string; locationName?: string; name?: string },
	selectColumns: string[],
): InsertHeaderDiff {
	const header = extractInsertHeader(userSql);
	if (header === null) { return { kind: "notApplicable" }; }
	if (header.kind === "malformed") {
		return { kind: "malformedHeader", reason: header.reason };
	}

	const insertColsNorm = header.columns.map((c) => c.toUpperCase());
	const selectColsNorm = selectColumns.map((c) => c.toUpperCase());
	const mismatch = insertVsSelectMismatch(insertColsNorm, selectColsNorm);
	if (mismatch) {
		return {
			kind: "columnListMismatch",
			insertColumns: header.columns,
			selectColumns,
			reason: mismatch,
		};
	}

	const existing: InsertTargetIdentifier = {
		database: typeof existingNode.database === "string" ? existingNode.database : "",
		locationName: typeof existingNode.locationName === "string" ? existingNode.locationName : "",
		name: typeof existingNode.name === "string" ? existingNode.name : "",
	};
	// Compare case-insensitively to match Snowflake's identifier-folding
	// semantics — `"DB"` and `db` resolve to the same warehouse object,
	// and Coalesce stores identifiers upper-cased. Storing what the
	// user typed would let `db` vs `"DB"` look like a `targetChanged`
	// even though it's a no-op at the warehouse.
	const changedFields: Array<"database" | "locationName" | "name"> = [];
	if (!identifiersEqual(header.target.database, existing.database)) { changedFields.push("database"); }
	if (!identifiersEqual(header.target.locationName, existing.locationName)) { changedFields.push("locationName"); }
	if (!identifiersEqual(header.target.name, existing.name)) { changedFields.push("name"); }

	if (changedFields.length === 0) { return { kind: "identical" }; }
	return {
		kind: "targetChanged",
		from: existing,
		// Preserve the existing case where the user didn't actually
		// change the field — only override the fields that differ. The
		// user-typed value wins for changed fields (case and all).
		to: {
			database: changedFields.includes("database") ? header.target.database : existing.database,
			locationName: changedFields.includes("locationName") ? header.target.locationName : existing.locationName,
			name: changedFields.includes("name") ? header.target.name : existing.name,
		},
		changedFields,
	};
}

/**
 * Snowflake-style case-folding compare for identifiers. Approximates the
 * warehouse's resolution rules: bare identifiers fold to upper-case;
 * `"FOO"` and `foo` resolve to the same object. The approximation is
 * imperfect — `"foo"` and `"FOO"` are technically DIFFERENT objects in
 * Snowflake (quoted identifiers preserve case), but Coalesce always
 * stores names in upper-case canonical form and the renderer always
 * emits `"UPPER"`, so the edge case is unreachable via the normal
 * round-trip. A user who hand-types `"foo"` against an existing `"FOO"`
 * node would see `identical` here.
 */
function identifiersEqual(a: string, b: string): boolean {
	return a.toUpperCase() === b.toUpperCase();
}

// ── helpers ─────────────────────────────────────────────────────────────────

function parseInsertColumnList(body: string): string[] {
	const out: string[] = [];
	for (const raw of splitTopLevelCommas(body)) {
		const trimmed = raw.trim();
		if (!trimmed) { continue; }
		const m = /^"([^"]+)"|^`([^`]+)`|^\[([^\]]+)\]|^([A-Za-z_][\w$]*)/.exec(trimmed);
		if (!m) { continue; }
		out.push(m[1] ?? m[2] ?? m[3] ?? m[4] ?? "");
	}
	return out;
}

function splitTopLevelCommas(s: string): string[] {
	const out: string[] = [];
	let parenDepth = 0;
	let inDoubleQuote = false;
	let inSingleQuote = false;
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

/**
 * Find the index of the `)` that matches the `(` at `openIdx`. Returns
 * -1 when the parens don't balance. Quote-aware so a `(` inside a
 * string literal isn't counted.
 */
function findMatchingClose(s: string, openIdx: number): number {
	if (s[openIdx] !== "(") { return -1; }
	let depth = 0;
	let inSingleQuote = false;
	let inDoubleQuote = false;
	for (let i = openIdx; i < s.length; i++) {
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
		if (c === "(") { depth++; continue; }
		if (c === ")") {
			depth--;
			if (depth === 0) { return i; }
		}
	}
	return -1;
}

/**
 * Compare the INSERT column list against the SELECT column list and
 * return a human-readable mismatch reason, or null when they match
 * (same columns in the same order, case-insensitive).
 */
function insertVsSelectMismatch(insertCols: string[], selectCols: string[]): string | null {
	if (insertCols.length !== selectCols.length) {
		return `INSERT column list has ${insertCols.length} columns but SELECT projects `
			+ `${selectCols.length}. Coalesce auto-generates the INSERT list from the SELECT — `
			+ `edit the SELECT to add/remove columns.`;
	}
	for (let i = 0; i < insertCols.length; i++) {
		if (insertCols[i] !== selectCols[i]) {
			return `INSERT column at position ${i + 1} is "${insertCols[i]}" but SELECT projects `
				+ `"${selectCols[i]}" at the same position. Coalesce auto-generates the INSERT list `
				+ `from the SELECT — edit the SELECT (and use AS aliases) instead of the INSERT header.`;
		}
	}
	return null;
}
