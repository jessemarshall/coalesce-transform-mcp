/**
 * Iteration 2 of the "edit any rendered SQL → declarative Coalesce node update"
 * project: detect and apply edits to the FROM/JOIN/ON/WHERE/QUALIFY block.
 *
 * In Coalesce's data model, this entire block is stored as a single string in
 * `metadata.sourceMapping[].join.joinCondition`, with `{{ ref('LOC', 'NAME') }}`
 * placeholders for predecessor refs. The rendered DML the user edits has real
 * three-part identifiers (`"DB"."LOC"."NAME"`) instead, so the diff logic must
 * normalize between the two forms before comparing.
 *
 * Scope for iteration 2:
 *   - **Supported**: edits that change WHERE / ON conditions while keeping the
 *     same set of source tables (no new dependencies, none removed).
 *     `applyJoinConditionDiff` returns the new joinCondition with table refs
 *     re-substituted to `{{ ref() }}` form.
 *   - **Unsupported (rejected with clear message)**: adding or removing a
 *     source — these need new entries in `sourceMapping[].dependencies`
 *     and `aliases`, and we can't safely look up the upstream node ID without
 *     a workspace search. Returned as a `kind: "newSource" | "removedSource"`
 *     diff for the apply path to surface to the user.
 *
 * Things explicitly NOT in scope here:
 *   - LIMIT/GROUP BY/ORDER BY/HAVING — those are config flags, not joinCondition
 *     content. The from-block extractor stops AT those keywords so they never
 *     bleed into a joinCondition diff. (LIMIT is rejected separately in the
 *     iteration-4 task.)
 *   - Reformatting/whitespace-only edits — `normalizeForCompare` collapses
 *     whitespace so cosmetic reformat doesn't produce a spurious change.
 */
import { findTopLevelKeywordIndex } from "../pipelines/sql-tokenizer.js";

/** A predecessor reference identified by its Coalesce location + node name. */
export interface SourceRef {
	locationName: string;
	nodeName: string;
}

/** Diff outcome for the from-block comparison. */
export type FromBlockDiff =
	| { kind: "identical" }
	| {
		kind: "whereOrJoinEdit";
		/** New `joinCondition` value to write to the node, with table refs
		 *  rewritten to `{{ ref() }}` form. */
		newJoinCondition: string;
	}
	| {
		kind: "newSource";
		/** Sources that appear in the user's SQL but not in the existing
		 *  joinCondition. Listed for the user; apply must reject until they
		 *  add the predecessor manually. */
		added: SourceRef[];
	}
	| {
		kind: "removedSource";
		removed: SourceRef[];
	}
	| {
		/**
		 * The diff couldn't be computed — usually because the user's SQL
		 * contains a CTE (`WITH ...`) or other structural shape we don't
		 * yet support inverting. Apply path should treat this as "no
		 * joinCondition change" and surface `reason` as a warning, so the
		 * user knows their JOIN-block edits weren't applied.
		 */
		kind: "unsupported";
		reason: string;
	};

/**
 * Extract the FROM-through-WHERE/QUALIFY block from a rendered SQL document.
 * Stops AT `GROUP BY`, `ORDER BY`, `LIMIT`, `HAVING`, `QUALIFY`, a top-level
 * `;`, or a closing paren — the joinCondition field doesn't include those
 * clauses, and a trailing statement after `;` shouldn't be slurped into the
 * extracted block (would otherwise be persisted into the cloud node verbatim).
 *
 * Returns undefined when no top-level FROM is found (e.g. SQL is a CREATE
 * TABLE, a SHOW, or otherwise non-SELECT-shaped).
 */
export function extractFromBlock(sql: string): string | undefined {
	// For INSERT-shaped DML, the inner SELECT is wrapped in parentheses.
	// `findTopLevelKeywordIndex` skips inside paren depth, so we'd miss
	// the `FROM` inside `INSERT INTO ... ( SELECT ... FROM ... )`. Locate
	// the first `SELECT` (top-level) and search relative to that.
	const selectIdx = findTopLevelKeywordIndex(sql, "select");
	if (selectIdx < 0) {
		// Try inside the first parenthesized region — INSERT envelopes
		// hide the SELECT one paren-depth in. Use a quote-/comment-aware
		// scan to avoid landing on a `(` inside a string literal.
		const parenStart = findFirstTopLevelOpenParen(sql);
		if (parenStart < 0) { return undefined; }
		const inner = sql.slice(parenStart + 1);
		return extractFromBlock(inner);
	}

	const fromIdx = findTopLevelKeywordIndex(sql, "from", selectIdx);
	if (fromIdx < 0) { return undefined; }

	// End of the from-block: earliest top-level terminator after FROM.
	const terminators = ["group", "order", "limit", "having", "qualify"];
	let endIdx = sql.length;
	for (const term of terminators) {
		const idx = findTopLevelKeywordIndex(sql, term, fromIdx + 4);
		if (idx >= 0 && idx < endIdx) { endIdx = idx; }
	}

	// Top-level `;` ends a statement — anything after it is a separate
	// statement we mustn't drag into the joinCondition.
	const semiIdx = findFirstTopLevelSemicolon(sql, fromIdx);
	if (semiIdx >= 0 && semiIdx < endIdx) { endIdx = semiIdx; }

	// If the SELECT is wrapped in `(...)` (INSERT envelope), the closing
	// paren is also a terminator. Walk top-level paren depth from fromIdx
	// to find the first depth-going-negative position.
	const parenEnd = findUnmatchedClose(sql, fromIdx);
	if (parenEnd >= 0 && parenEnd < endIdx) { endIdx = parenEnd; }

	return sql.slice(fromIdx, endIdx).trim();
}

/**
 * Detect whether the user's SQL starts with a top-level `WITH` clause
 * (a CTE). The from-block diff doesn't yet understand CTEs — the inner
 * SELECT(s) reference real sources but we can't reliably invert the
 * structure into a `joinCondition` rewrite. Apply path treats `unsupported`
 * as "no joinCondition change" and surfaces a warning.
 */
function startsWithCte(sql: string): boolean {
	const stripped = sql
		.replace(/^(?:\s|--[^\n]*\n|\/\*[\s\S]*?\*\/)+/, "")
		.trimStart();
	return /^with\s+/i.test(stripped);
}

/**
 * Single SQL-aware walker: scan forward, skipping string literals
 * (single, double, backtick, square-bracket), line comments (`-- ...`),
 * and block comments (`/* ... *\/`). Returns the index of the first
 * character for which `predicate(char, parenDepth)` returns true, or -1
 * if no such character is found before EOF.
 *
 * Hand-rolled because `scanTopLevel` consumes `(` / `)` for paren-depth
 * tracking BEFORE invoking its callback, so paren chars never reach the
 * consumer. Used by `findFirstTopLevelOpenParen`,
 * `findFirstTopLevelSemicolon`, `findUnmatchedClose`, and
 * `stripCommentsForPersist` (the last via a different driver) so all
 * walkers in this module share the same string/comment handling.
 */
function walkSqlTopLevel(
	s: string,
	opts: {
		startIdx?: number;
		trackParens: boolean;
		predicate: (char: string, parenDepth: number) => boolean;
	},
): number {
	const startIdx = opts.startIdx ?? 0;
	let parenDepth = 0;
	let inSingleQuote = false;
	let inDoubleQuote = false;
	let inBacktick = false;
	let inBracket = false;
	let inLineComment = false;
	let inBlockComment = false;
	for (let i = startIdx; i < s.length; i++) {
		const c = s[i]!;
		const next = s[i + 1];
		if (inLineComment) { if (c === "\n") { inLineComment = false; } continue; }
		if (inBlockComment) { if (c === "*" && next === "/") { inBlockComment = false; i++; } continue; }
		if (inSingleQuote) { if (c === "'" && next === "'") { i++; } else if (c === "'") { inSingleQuote = false; } continue; }
		if (inDoubleQuote) { if (c === '"') { inDoubleQuote = false; } continue; }
		if (inBacktick) { if (c === "`") { inBacktick = false; } continue; }
		if (inBracket) { if (c === "]") { inBracket = false; } continue; }
		if (c === "'") { inSingleQuote = true; continue; }
		if (c === '"') { inDoubleQuote = true; continue; }
		if (c === "`") { inBacktick = true; continue; }
		if (c === "[") { inBracket = true; continue; }
		if (c === "-" && next === "-") { inLineComment = true; i++; continue; }
		if (c === "/" && next === "*") { inBlockComment = true; i++; continue; }
		if (opts.trackParens) {
			if (c === "(") { parenDepth++; continue; }
			if (c === ")") { if (parenDepth > 0) { parenDepth--; continue; } }
		}
		if (opts.predicate(c, parenDepth)) { return i; }
	}
	return -1;
}

function findFirstTopLevelOpenParen(s: string): number {
	return walkSqlTopLevel(s, {
		trackParens: false,
		predicate: (c) => c === "(",
	});
}

function findFirstTopLevelSemicolon(s: string, startIdx: number): number {
	return walkSqlTopLevel(s, {
		startIdx,
		trackParens: true,
		predicate: (c, depth) => c === ";" && depth === 0,
	});
}

/**
 * Walk forward from `startIdx` and return the index of the first `)` that
 * isn't matched by a `(` later than `startIdx`. Used to detect the closing
 * paren of an INSERT envelope. Returns -1 if no unmatched `)` is found.
 *
 * Delegates to {@link walkSqlTopLevel} for consistent string/comment
 * handling across all walkers in this module.
 */
function findUnmatchedClose(s: string, startIdx: number): number {
	return walkSqlTopLevel(s, {
		startIdx,
		trackParens: true,
		predicate: (c, depth) => c === ")" && depth === 0,
	});
}

/**
 * Find every qualified table reference (`"DB"."LOC"."NAME"`, two-part forms,
 * or `{{ ref('LOC', 'NAME') }}`) appearing in a from-block. Returns the
 * location + node name pairs; the database part is dropped because the
 * `joinCondition` field doesn't carry it.
 */
export function extractFromBlockSources(fromBlock: string): SourceRef[] {
	const out: SourceRef[] = [];
	const seen = new Set<string>();

	// Coalesce ref form: {{ ref('LOC', 'NAME') }} or {{ ref("LOC", "NAME") }}.
	const coalesceRefRe = /\{\{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}/gi;
	let m: RegExpExecArray | null;
	while ((m = coalesceRefRe.exec(fromBlock)) !== null) {
		const ref: SourceRef = { locationName: m[1], nodeName: m[2] };
		const key = `${ref.locationName}::${ref.nodeName}`;
		if (!seen.has(key)) { seen.add(key); out.push(ref); }
	}

	// Three-part identifier: "DB"."LOC"."NAME". Two-part is also accepted
	// (`"LOC"."NAME"`) — we drop the database part either way. Bare
	// identifiers (FROM CUSTOMER) aren't matched here; Coalesce nodes
	// always reference predecessors via fully-qualified or ref() syntax.
	const threePartRe = /"([A-Za-z_][\w$]*)"\s*\.\s*"([A-Za-z_][\w$]*)"\s*\.\s*"([A-Za-z_][\w$]*)"/g;
	while ((m = threePartRe.exec(fromBlock)) !== null) {
		const ref: SourceRef = { locationName: m[2], nodeName: m[3] };
		const key = `${ref.locationName}::${ref.nodeName}`;
		if (!seen.has(key)) { seen.add(key); out.push(ref); }
	}

	const twoPartRe = /(?<![.\w])"([A-Za-z_][\w$]*)"\s*\.\s*"([A-Za-z_][\w$]*)"(?!\s*\.)/g;
	while ((m = twoPartRe.exec(fromBlock)) !== null) {
		const ref: SourceRef = { locationName: m[1], nodeName: m[2] };
		const key = `${ref.locationName}::${ref.nodeName}`;
		if (!seen.has(key)) { seen.add(key); out.push(ref); }
	}

	return out;
}

/**
 * Replace every fully-qualified `"DB"."LOC"."NAME"` (and two-part
 * `"LOC"."NAME"`) reference in a from-block with the equivalent
 * `{{ ref('LOC', 'NAME') }}` placeholder so the result can be written
 * back to a node's `joinCondition`. Coalesce-ref placeholders that are
 * already present are left untouched.
 */
export function rewriteTableRefsToCoalesceRefs(fromBlock: string): string {
	// Three-part first (longest match wins to avoid the two-part regex
	// chewing the schema/table portion of a three-part ref).
	let out = fromBlock.replace(
		/"([A-Za-z_][\w$]*)"\s*\.\s*"([A-Za-z_][\w$]*)"\s*\.\s*"([A-Za-z_][\w$]*)"/g,
		(_, _db: string, loc: string, name: string) => `{{ ref('${loc}', '${name}') }}`,
	);
	out = out.replace(
		/(?<![.\w])"([A-Za-z_][\w$]*)"\s*\.\s*"([A-Za-z_][\w$]*)"(?!\s*\.)/g,
		(_, loc: string, name: string) => `{{ ref('${loc}', '${name}') }}`,
	);
	return out;
}

/**
 * Normalize a from-block for whitespace-tolerant comparison. Strips
 * comments, collapses whitespace runs, drops whitespace adjacent to
 * non-identifier punctuation, and lower-cases SQL keywords for
 * case-insensitive equality. Identifier casing is preserved for any of
 * the three SQL quoting styles we recognize (`"…"`, `` `…` ``, `[…]`)
 * so case-sensitive identifiers in any dialect aren't collapsed.
 *
 * Doubled-quote escapes inside identifiers (`"a""b"`) aren't recognized;
 * Coalesce node names don't contain `"` in practice. If that ever changes,
 * the segmenter regex needs to be upgraded.
 *
 * Used to decide whether a user edit is purely cosmetic (return
 * `identical`) or a real content change.
 */
export function normalizeForCompare(fromBlock: string): string {
	return fromBlock
		// Strip comments
		.replace(/--[^\n]*/g, "")
		.replace(/\/\*[\s\S]*?\*\//g, "")
		// Collapse whitespace
		.replace(/\s+/g, " ")
		// Drop whitespace next to punctuation (preserves keyword
		// boundaries; same rule as sql-column-diff's normalizeExpression).
		.replace(/\s*([^\w\s])\s*/g, "$1")
		// Lower-case keywords. Segments matching a quoted-identifier form
		// are preserved verbatim; everything else lower-cases.
		.split(/("[^"]*"|`[^`]*`|\[[^\]]*\])/)
		.map((seg, i) => i % 2 === 0 ? seg.toLowerCase() : seg)
		.join("")
		.trim();
}

/**
 * Strip line and block comments before persisting a joinCondition. The
 * raw user-edited block may carry inline comments that helped the user
 * reason about the edit but shouldn't be saved into the cloud node
 * (Coalesce re-renders these into DDL/DML, so a stray `-- TODO …` in the
 * joinCondition becomes part of every future render).
 *
 * Walks the string with the same SQL-awareness as the other walkers so
 * a `--` or `/*` *inside a string literal* (`'foo -- bar'`) is preserved
 * verbatim — the previous regex-only implementation truncated the
 * string and corrupted the cloud node's joinCondition.
 */
function stripCommentsForPersist(s: string): string {
	const out: string[] = [];
	let inSingleQuote = false;
	let inDoubleQuote = false;
	let inBacktick = false;
	let inBracket = false;
	for (let i = 0; i < s.length; i++) {
		const c = s[i]!;
		const next = s[i + 1];
		if (inSingleQuote) {
			out.push(c);
			if (c === "'" && next === "'") { out.push(next!); i++; }
			else if (c === "'") { inSingleQuote = false; }
			continue;
		}
		if (inDoubleQuote) {
			out.push(c);
			if (c === '"') { inDoubleQuote = false; }
			continue;
		}
		if (inBacktick) {
			out.push(c);
			if (c === "`") { inBacktick = false; }
			continue;
		}
		if (inBracket) {
			out.push(c);
			if (c === "]") { inBracket = false; }
			continue;
		}
		if (c === "'") { inSingleQuote = true; out.push(c); continue; }
		if (c === '"') { inDoubleQuote = true; out.push(c); continue; }
		if (c === "`") { inBacktick = true; out.push(c); continue; }
		if (c === "[") { inBracket = true; out.push(c); continue; }
		// Line comment — skip to end of line (consume the newline so the
		// surrounding line structure is preserved as a blank-line break).
		if (c === "-" && next === "-") {
			while (i < s.length && s[i] !== "\n") { i++; }
			continue;
		}
		// Block comment — skip to closing `*/`.
		if (c === "/" && next === "*") {
			i += 2;
			while (i < s.length && !(s[i] === "*" && s[i + 1] === "/")) { i++; }
			i += 1; // step past the `*`; the loop's `i++` skips the `/`.
			continue;
		}
		out.push(c);
	}
	return out
		.join("")
		// Trim trailing whitespace each line that the comment removal
		// may have left behind; preserves line structure otherwise.
		.replace(/[ \t]+$/gm, "")
		.trim();
}

/**
 * Top-level diff entry. Compares the user's rendered SQL against the
 * existing node's joinCondition and returns the appropriate verdict.
 *
 * - Identical: nothing to do.
 * - whereOrJoinEdit: in-place text change to the joinCondition (no source
 *   add/remove). `newJoinCondition` is the value to write to the node,
 *   already rewritten to `{{ ref() }}` form.
 * - newSource / removedSource: dependency change — apply path should
 *   reject with a message asking the user to add/remove the predecessor
 *   manually.
 * - unsupported: the SQL shape isn't safely invertible (CTE today; future:
 *   nested INSERT-as-SELECT etc.). Apply path treats this as "no
 *   joinCondition change" and surfaces `reason` as a warning.
 */
export function diffFromBlock(
	userSql: string,
	existingJoinCondition: string,
): FromBlockDiff {
	// CTEs reference predecessors INSIDE a sub-SELECT we can't see at the
	// outer FROM. Round-tripping a CTE-shaped edit through this diff would
	// drop refs the YAML carries. Bail with a warning and let the user
	// know joinCondition edits aren't applied for CTE inputs.
	if (startsWithCte(userSql)) {
		return {
			kind: "unsupported",
			reason: "CTE-shaped SQL (`WITH ...`) isn't yet supported for joinCondition diffing. "
				+ "FROM/JOIN/WHERE edits in CTE inputs are silently skipped — flatten the CTE "
				+ "or edit the joinCondition via the cloud UI.",
		};
	}

	const userBlock = extractFromBlock(userSql);
	if (!userBlock) {
		// User SQL has no recognizable FROM — nothing to compare. Treat as
		// identical so the apply path doesn't double-write joinCondition
		// when it's only doing column-level edits on a CREATE-TABLE input.
		return { kind: "identical" };
	}

	// Re-rewrite the user's block back to `{{ ref() }}` form, then compare
	// against the existing joinCondition. This is the unit of identity:
	// what would the joinCondition look like if we wrote the user's edit?
	const userBlockAsRefs = rewriteTableRefsToCoalesceRefs(userBlock);

	const userSources = extractFromBlockSources(userBlock);
	const existingSources = extractFromBlockSources(existingJoinCondition);
	const userKeys = new Set(userSources.map(sourceKey));
	const existingKeys = new Set(existingSources.map(sourceKey));

	const added = userSources.filter((s) => !existingKeys.has(sourceKey(s)));
	const removed = existingSources.filter((s) => !userKeys.has(sourceKey(s)));

	if (added.length > 0) { return { kind: "newSource", added }; }
	if (removed.length > 0) { return { kind: "removedSource", removed }; }

	if (normalizeForCompare(userBlockAsRefs) === normalizeForCompare(existingJoinCondition)) {
		return { kind: "identical" };
	}
	return {
		kind: "whereOrJoinEdit",
		newJoinCondition: stripCommentsForPersist(userBlockAsRefs),
	};
}

function sourceKey(s: SourceRef): string {
	return `${s.locationName.toUpperCase()}::${s.nodeName.toUpperCase()}`;
}
