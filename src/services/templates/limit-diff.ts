/**
 * Iteration 4 of the "edit any rendered SQL → declarative Coalesce node update"
 * project: detect and apply edits to a `LIMIT N` clause on a rendered DML.
 *
 * Coalesce has no native `limit` config field for typical SQL nodes (Stage,
 * Source, Fact, Dim). The only `*limit*` attributes in the node-type corpus
 * are `sizeLimit` (file types) and `sampleRows` (View types) — neither maps
 * to a plain `LIMIT N` on a SELECT.
 *
 * Workaround: store LIMIT as the trailing clause of `metadata.sourceMapping[0].join.joinCondition`.
 * Coalesce's renderer copies joinCondition verbatim into the rendered SQL,
 * so a trailing `LIMIT N` flows through. **Caveat:** the renderer also
 * appends auto-generated GROUP BY (when `config.groupByAll: true`) and
 * ORDER BY (when `config.orderby: true`) AFTER joinCondition. If either
 * is set, an appended LIMIT lands in invalid position (before GROUP BY /
 * ORDER BY). The diff detects this and emits a warning telling the user
 * to either disable those toggles or restructure the pipeline.
 *
 * Coordination with iteration 2's join-block diff:
 *   - The user's full SQL contains the FROM/JOIN/WHERE block AND the LIMIT.
 *   - Iteration 2's `extractFromBlock` STOPS at LIMIT (so the join-diff
 *     doesn't see LIMIT-only changes).
 *   - This module reads ONLY the LIMIT from the user's SQL and from the
 *     existing joinCondition's trailing position. The apply path
 *     coordinates the two by composing the new joinCondition: take
 *     iteration 2's `newJoinCondition` (or the existing one when iteration 2
 *     said identical) with its trailing LIMIT stripped, then append the
 *     user's LIMIT (or omit it for `removed`).
 */
import { findLastTopLevelOpenParen } from "../pipelines/sql-tokenizer.js";
import { safeErrorMessage } from "../../utils.js";

/** Diff outcome for the LIMIT comparison. */
export type LimitDiff =
	| { kind: "identical" }
	| {
		kind: "added" | "changed";
		/** New numeric value. */
		newLimit: number;
		/** True when `groupByAll` or `orderby` is on — appending LIMIT to
		 *  joinCondition produces invalid SQL on next render. The apply
		 *  path should still write the change but surface this warning. */
		warnsClobberByTailClause: boolean;
	}
	| { kind: "removed" }
	| {
		/**
		 * The LIMIT clause has an OFFSET or `LIMIT N, M` (MySQL row-range)
		 * tail that Coalesce's single-LIMIT joinCondition slot can't
		 * round-trip. Apply path should reject with `reason` rather than
		 * silently truncating to the leading number.
		 */
		kind: "unsupported";
		reason: string;
	};

/**
 * Extract a top-level trailing `LIMIT N` from a SQL document — the value
 * after `LIMIT` and before end-of-statement (`;`, end of string, or
 * closing `)` for INSERT envelopes). Returns `null` when no LIMIT is
 * present.
 *
 * For INSERT-shaped DML, the inner SELECT (and its LIMIT) lives one
 * paren-depth in. When no top-level LIMIT is found AND the SQL is
 * shaped like an INSERT envelope, we recurse into the first top-level
 * `(...)` region. The INSERT-shape gate prevents false positives where
 * a top-level SELECT contains a CTE or subquery with its own internal
 * LIMIT (`WITH cte AS (SELECT … LIMIT 5) SELECT … FROM cte` would
 * otherwise return 5 — that LIMIT is for the CTE, not the outer query).
 *
 * Throws when the LIMIT clause has an OFFSET / `LIMIT N, M` (MySQL)
 * tail; both indicate the user wants a row-range that Coalesce's
 * single-`LIMIT` joinCondition slot can't represent. Returning the
 * leading number silently would corrupt the round-trip — better to
 * surface a clear error so the apply path can reject.
 *
 * Quote-/comment-aware so a `LIMIT` inside a string literal or comment
 * doesn't false-match.
 */
export function extractTrailingLimit(sql: string): number | null {
	const limitIdx = findLastTopLevelKeyword(sql, "limit");
	if (limitIdx < 0) {
		// Only peel paren-depth for genuine INSERT/MERGE-shaped DML — a
		// plain SELECT with a CTE/subquery LIMIT must NOT recurse, or
		// the inner LIMIT would be falsely reported as the outer LIMIT.
		if (!startsWithInsert(sql)) { return null; }
		// INSERT envelopes can have TWO top-level parens (the column
		// list + the SELECT body). Find the LAST top-level paren group
		// — that's the SELECT body for INSERT, the USING clause for
		// MERGE — and recurse into it. Recursing into the FIRST paren
		// would land in the column list (no LIMIT possible there).
		const lastOpen = findLastTopLevelOpenParen(sql);
		if (lastOpen < 0) { return null; }
		const inner = sql.slice(lastOpen + 1);
		// Recurse without the INSERT-shape gate — we're now inside the
		// envelope body. Use the no-gate variant to avoid an infinite
		// loop on weird inputs.
		return extractTrailingLimitInner(inner);
	}
	return parseLimitClauseValue(sql.slice(limitIdx + "limit".length));
}

/**
 * Parse the value portion of a LIMIT clause. `tail` is the substring
 * AFTER the `LIMIT` keyword. Bounds the parse at the next `)` (we're
 * inside an INSERT envelope's inner SELECT and the `)` closes it),
 * `;` (statement terminator), or end-of-string. Both the bounding scan
 * and the post-value rest-check are comment- and string-aware so an
 * inline comment between the value and OFFSET (`LIMIT 5 -- c\nOFFSET 10`)
 * doesn't shield the OFFSET from rejection.
 *
 * Returns the integer value when the clause is a plain `LIMIT N`.
 * Throws when the clause has an OFFSET or `LIMIT N, M` tail (Coalesce
 * can't round-trip those). Returns null when the tail isn't a number
 * at all.
 */
function parseLimitClauseValue(tail: string): number | null {
	// Bound the tail at the next `)` / `;` / EOF — quote/comment-aware
	// so a `;` inside a string or after a comment-shielded section
	// doesn't false-terminate.
	const endIdx = findFirstTopLevelChar(tail, [")", ";"]);
	const boundedTail = endIdx < 0 ? tail : tail.slice(0, endIdx);
	// Strip comments + string contents from the bounded tail before
	// regex matching so a comment between the value and OFFSET doesn't
	// hide the OFFSET (which would silently truncate user intent).
	const clauseTail = stripCommentsAndStrings(boundedTail).trim();
	const m = /^(\d+)(\b[\s\S]*)?$/.exec(clauseTail);
	if (!m) { return null; }
	const rest = (m[2] ?? "").trim();
	if (rest !== "") {
		if (/^,\s*\d+\b/.test(rest) || /^offset\b/i.test(rest)) {
			// Collapse runs of whitespace in the displayed excerpt —
			// `stripCommentsAndStrings` replaces comment content with
			// spaces, so a comment-shielded `LIMIT 5 /*c*/ OFFSET 10`
			// would otherwise print as `LIMIT 5       OFFSET 10`.
			const excerpt = m[0].replace(/\s+/g, " ").trim();
			throw new Error(
				`LIMIT clause includes an OFFSET or row-range tail (\`LIMIT ${excerpt}\`); `
				+ `Coalesce's joinCondition only supports a single LIMIT N. Either `
				+ `drop the OFFSET / second value, or restructure the pipeline.`,
			);
		}
		return null;
	}
	return parseInt(m[1], 10);
}

/**
 * Quote/comment-aware scan: find the first index where any character in
 * `targets` appears at top-level (outside strings, comments, parens).
 * Returns -1 when none found.
 */
function findFirstTopLevelChar(s: string, targets: string[]): number {
	const targetSet = new Set(targets);
	let parenDepth = 0;
	let inSingleQuote = false;
	let inDoubleQuote = false;
	let inBacktick = false;
	let inBracket = false;
	let inLineComment = false;
	let inBlockComment = false;
	for (let i = 0; i < s.length; i++) {
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
			if (parenDepth === 0 && targetSet.has(c)) { return i; }
			if (parenDepth > 0) { parenDepth--; }
			continue;
		}
		if (parenDepth === 0 && targetSet.has(c)) { return i; }
	}
	return -1;
}

/**
 * Replace SQL comments and string literals with spaces (length-preserving)
 * so regex matching on the result ignores their content. Used in
 * {@link parseLimitClauseValue} to keep an inline comment between the
 * value and a trailing OFFSET from shielding the OFFSET from rejection.
 */
function stripCommentsAndStrings(s: string): string {
	let out = "";
	let inSingleQuote = false;
	let inDoubleQuote = false;
	let inBacktick = false;
	let inBracket = false;
	let inLineComment = false;
	let inBlockComment = false;
	for (let i = 0; i < s.length; i++) {
		const c = s[i]!;
		const next = s[i + 1];
		if (inLineComment) {
			if (c === "\n") { inLineComment = false; out += c; }
			else { out += " "; }
			continue;
		}
		if (inBlockComment) {
			if (c === "*" && next === "/") {
				inBlockComment = false;
				out += "  ";
				i++;
			} else { out += " "; }
			continue;
		}
		if (inSingleQuote) {
			if (c === "'" && next === "'") { out += "  "; i++; }
			else if (c === "'") { inSingleQuote = false; out += "'"; }
			else { out += " "; }
			continue;
		}
		if (inDoubleQuote) {
			if (c === '"') { inDoubleQuote = false; }
			out += c;
			continue;
		}
		if (inBacktick) {
			if (c === "`") { inBacktick = false; }
			out += c;
			continue;
		}
		if (inBracket) {
			if (c === "]") { inBracket = false; }
			out += c;
			continue;
		}
		if (c === "'") { inSingleQuote = true; out += "'"; continue; }
		if (c === '"') { inDoubleQuote = true; out += c; continue; }
		if (c === "`") { inBacktick = true; out += c; continue; }
		if (c === "[") { inBracket = true; out += c; continue; }
		if (c === "-" && next === "-") { inLineComment = true; out += "  "; i++; continue; }
		if (c === "/" && next === "*") { inBlockComment = true; out += "  "; i++; continue; }
		out += c;
	}
	return out;
}

/**
 * Whether the SQL starts (after stripping leading whitespace + comments)
 * with `INSERT` or `MERGE`. Used to gate the paren-recursion fallback
 * in {@link extractTrailingLimit}.
 */
function startsWithInsert(sql: string): boolean {
	const stripped = sql.replace(/^(?:\s|--[^\n]*\n|\/\*[\s\S]*?\*\/)+/, "").trimStart();
	return /^(?:insert|merge)\s+/i.test(stripped);
}

/**
 * No-gate inner extractor used after the INSERT-shape recursion has
 * already peeled into the envelope. Same logic as
 * {@link extractTrailingLimit} but skips the gate to avoid an infinite
 * loop on weird inputs (and the inner string no longer starts with
 * `INSERT`/`MERGE` so the gate would block legitimate recursions).
 */
function extractTrailingLimitInner(sql: string): number | null {
	const limitIdx = findLastTopLevelKeyword(sql, "limit");
	if (limitIdx < 0) { return null; }
	return parseLimitClauseValue(sql.slice(limitIdx + "limit".length));
}

/**
 * Strip a trailing `LIMIT N` from a joinCondition (used when the user
 * removes LIMIT or changes it — we replace, never append a duplicate).
 * Preserves leading content; only the LIMIT clause and any trailing
 * whitespace are removed.
 */
export function stripTrailingLimit(joinCondition: string): string {
	const limitIdx = findLastTopLevelKeyword(joinCondition, "limit");
	if (limitIdx < 0) { return joinCondition; }
	const tail = joinCondition.slice(limitIdx + "limit".length).trim();
	if (!/^\d+\b/.test(tail)) { return joinCondition; }
	return joinCondition.slice(0, limitIdx).replace(/[\s\n]+$/, "");
}

/**
 * Append a `LIMIT N` clause to a joinCondition, replacing any existing
 * trailing LIMIT first. Emits with a leading newline so the
 * joinCondition stays readable when rendered. Empty-base case skips
 * the leading newline so the result doesn't start with a stray blank
 * line (which would pollute every future render).
 */
export function appendLimitToJoinCondition(joinCondition: string, limit: number): string {
	const stripped = stripTrailingLimit(joinCondition);
	const trimmed = stripped.replace(/[\s\n]+$/, "");
	return trimmed === "" ? `LIMIT ${limit}` : `${trimmed}\nLIMIT ${limit}`;
}

/**
 * Compare the user's LIMIT against the existing joinCondition's
 * trailing LIMIT and produce the diff.
 *
 * `groupByAll` / `orderby` reflect the node's config; when either is
 * `true` AND the diff is `added` / `changed`, we surface a warning
 * because the resulting render order is invalid SQL.
 */
export function diffLimit(
	userSql: string,
	existingJoinCondition: string,
	context: { groupByAll: boolean; orderby: boolean },
): LimitDiff {
	let userLimit: number | null;
	try {
		userLimit = extractTrailingLimit(userSql);
	} catch (err) {
		// `extractTrailingLimit` throws on OFFSET / `LIMIT N, M` —
		// surface as a diff outcome so the apply path can reject
		// without crashing.
		return {
			kind: "unsupported",
			reason: safeErrorMessage(err),
		};
	}
	// Existing joinCondition is internal data; if it has an OFFSET tail
	// for some reason we'd swallow that defensively (no throw) — the
	// strip helper handles the no-OFFSET form which is what we wrote.
	let existingLimit: number | null;
	try {
		existingLimit = extractTrailingLimit(existingJoinCondition);
	} catch {
		existingLimit = null;
	}

	if (userLimit === existingLimit) { return { kind: "identical" }; }

	if (userLimit === null) { return { kind: "removed" }; }

	const warnsClobberByTailClause = context.groupByAll || context.orderby;
	if (existingLimit === null) {
		return { kind: "added", newLimit: userLimit, warnsClobberByTailClause };
	}
	return { kind: "changed", newLimit: userLimit, warnsClobberByTailClause };
}

// ── helpers ─────────────────────────────────────────────────────────────────

/**
 * Find the LAST occurrence of `keyword` at top-level in `s` (i.e.
 * outside strings, comments, and parens). Returns -1 if not found.
 *
 * Trailing LIMIT means we want the last one, not the first — a CTE or
 * subquery may contain its own internal LIMIT, but only the OUTER
 * LIMIT is the statement-level row cap.
 */
function findLastTopLevelKeyword(s: string, keyword: string): number {
	const lower = keyword.toLowerCase();
	let parenDepth = 0;
	let inSingleQuote = false;
	let inDoubleQuote = false;
	let inBacktick = false;
	let inBracket = false;
	let inLineComment = false;
	let inBlockComment = false;
	let lastIdx = -1;
	for (let i = 0; i < s.length; i++) {
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
		if (c === ")") { if (parenDepth > 0) { parenDepth--; } continue; }
		if (parenDepth !== 0) { continue; }
		// Match keyword with word boundaries on both sides.
		if (
			s.length - i >= lower.length
			&& s.slice(i, i + lower.length).toLowerCase() === lower
			&& !isIdentChar(s[i - 1])
			&& !isIdentChar(s[i + lower.length])
		) {
			lastIdx = i;
			i += lower.length - 1;
		}
	}
	return lastIdx;
}

function isIdentChar(c: string | undefined): boolean {
	if (c === undefined) { return false; }
	return /[A-Za-z0-9_$]/.test(c);
}
