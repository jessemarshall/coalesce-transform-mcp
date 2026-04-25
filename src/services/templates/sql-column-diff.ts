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

import { stripIdentifierQuotes } from "../pipelines/sql-tokenizer.js";
import { isPlainObject } from "../../utils.js";

export interface ParsedSqlColumn {
	name: string;
	dataType: string;
}

export interface ColumnDiff {
	/** Columns kept with their existing metadata, no change. */
	unchanged: string[];
	/** Columns whose dataType was updated. */
	typeChanged: Array<{ name: string; from: string; to: string }>;
	/** Columns dropped from the node (present in YAML, missing in SQL). */
	removed: string[];
	/**
	 * Columns present in the SQL but not in the YAML. Returned for caller
	 * inspection but applying them would require source mappings we can't
	 * infer from the SQL alone — applyColumnDiff will refuse if this array
	 * is non-empty unless the caller passes `allowAddsAsBareColumns: true`.
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
 * Diff a parsed SQL column list against an existing cloud node's
 * `metadata.columns[]`. The diff is name-keyed — column names are the
 * stable identity from the user's perspective.
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
	const removed: string[] = [];

	for (const [normName, existingCol] of existingByName) {
		const parsedCol = parsedByName.get(normName);
		const displayName = typeof existingCol.name === 'string' ? existingCol.name : normName;
		if (!parsedCol) {
			removed.push(displayName);
			continue;
		}
		const existingType = typeof existingCol.dataType === 'string' ? existingCol.dataType : '';
		if (normalizeType(existingType) !== normalizeType(parsedCol.dataType)) {
			typeChanged.push({ name: displayName, from: existingType, to: parsedCol.dataType });
		} else {
			unchanged.push(displayName);
		}
	}

	const added: ParsedSqlColumn[] = [];
	for (const [normName, parsedCol] of parsedByName) {
		if (!existingByName.has(normName)) {
			added.push(parsedCol);
		}
	}

	return { unchanged, typeChanged, removed, added };
}

/**
 * Build a new `metadata.columns[]` array applying the diff. Preserves the
 * original column ordering; removed columns are filtered out, and matching
 * columns get their `dataType` updated in place (so existing
 * `columnReference`, `sources`, `sourceColumnReferences`, and other lineage
 * metadata is preserved).
 *
 * If `diff.added` is non-empty, this function refuses by default. New
 * columns can't be synthesized without a source mapping. Pass
 * `allowAddsAsBareColumns: true` to write them with empty `sources` — only
 * useful if the caller also plans to populate sources separately.
 */
export function applyColumnDiff(
	parsed: ParsedSqlColumn[],
	existing: unknown[],
	diff: ColumnDiff,
	options?: { allowAddsAsBareColumns?: boolean },
): unknown[] {
	if (diff.added.length > 0 && !options?.allowAddsAsBareColumns) {
		throw new Error(
			`Cannot apply edits: ${diff.added.length} new column(s) (${diff.added.map(a => a.name).join(', ')}) `
			+ `appear in the SQL but not on the node. New columns need a source mapping (which upstream column they come from) `
			+ `that the SQL alone doesn't carry. Edit the YAML or add the columns via the cloud UI first, then re-render.`,
		);
	}

	const parsedByName = new Map<string, ParsedSqlColumn>();
	for (const col of parsed) {
		parsedByName.set(normalizeName(col.name), col);
	}

	const out: unknown[] = [];
	for (const col of existing) {
		if (!isPlainObject(col)) { continue; }
		const name = typeof col.name === 'string' ? col.name : undefined;
		if (!name) { continue; }
		const parsedCol = parsedByName.get(normalizeName(name));
		if (!parsedCol) { continue; }	// removed
		// Update dataType in place; preserve everything else.
		out.push({ ...col, dataType: parsedCol.dataType });
	}

	if (options?.allowAddsAsBareColumns) {
		for (const added of diff.added) {
			out.push({
				name: added.name,
				dataType: added.dataType,
				nullable: true,
				sources: [],
			});
		}
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

function normalizeName(s: string): string {
	return stripIdentifierQuotes(s).toUpperCase();
}

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
