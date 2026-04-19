/**
 * Parser for Coalesce job selector DSL.
 *
 * Grammar (v1, exact matches only — no globs):
 *   selector  := term ( OR term )*
 *   term      := "{" ( subgraphClause | locationNameClause ) "}"
 *   subgraphClause     := "subgraph:" value
 *   locationNameClause := "location:" value "name:" value
 *   value     := quotedValue | bareValue
 *
 * An empty string parses to zero terms (used for empty includeSelector /
 * excludeSelector).
 *
 * The `{ A || B }` form (combined `||` inside one brace pair) silently matches
 * zero nodes in Coalesce. The parser emits a warning and drops the term so the
 * caller can surface it; see preflight.ts checkSelector for the same footgun.
 */

export type SelectorTerm =
  | { kind: "subgraph"; name: string }
  | { kind: "location_name"; location: string; name: string };

export type ParsedSelector = {
  terms: SelectorTerm[];
  warnings: string[];
};

const OR_SPLIT = /\s+OR\s+/i;

export function parseJobSelector(input: string | undefined | null): ParsedSelector {
  const warnings: string[] = [];
  const terms: SelectorTerm[] = [];

  if (!input) return { terms, warnings };
  const trimmed = input.trim();
  if (!trimmed) return { terms, warnings };

  const chunks = splitTopLevelOr(trimmed);
  for (const raw of chunks) {
    const chunk = raw.trim();
    if (!chunk) continue;

    if (!chunk.startsWith("{") || !chunk.endsWith("}")) {
      warnings.push(
        `Selector term "${chunk}" is not wrapped in braces — skipped. Expected form: { subgraph: NAME } or { location: LOC name: NAME }.`
      );
      continue;
    }

    const body = chunk.slice(1, -1).trim();

    if (/\|\|/.test(body)) {
      warnings.push(
        `Selector term "${chunk}" uses \`{ A || B }\` form which silently matches zero nodes. Use \`{ A } OR { B }\` (separate braces per operand). Term skipped.`
      );
      continue;
    }

    const parsed = parseTermBody(body);
    if (parsed.term) {
      terms.push(parsed.term);
    } else if (parsed.warning) {
      warnings.push(`Selector term "${chunk}": ${parsed.warning}`);
    }
  }

  return { terms, warnings };
}

/**
 * Split on `OR` only at top level (outside of braces). Real selectors nest
 * braces one level deep so a simple depth counter is sufficient.
 */
function splitTopLevelOr(input: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  let i = 0;
  while (i < input.length) {
    const ch = input[i];
    if (ch === "{") depth++;
    else if (ch === "}") depth = Math.max(0, depth - 1);
    else if (depth === 0) {
      const slice = input.slice(i);
      const m = slice.match(/^(\s+)OR(\s+)/i);
      if (m) {
        parts.push(input.slice(start, i));
        i += m[0].length;
        start = i;
        continue;
      }
    }
    i++;
  }
  parts.push(input.slice(start));
  return parts;
}

function parseTermBody(body: string): { term?: SelectorTerm; warning?: string } {
  const subgraphMatch = body.match(/^subgraph\s*:\s*(.+)$/i);
  if (subgraphMatch) {
    const name = stripQuotes(subgraphMatch[1].trim());
    if (!name) return { warning: "empty subgraph name" };
    return { term: { kind: "subgraph", name } };
  }

  const locationNameMatch = body.match(/^location\s*:\s*(\S+)\s+name\s*:\s*(.+)$/i);
  if (locationNameMatch) {
    const location = stripQuotes(locationNameMatch[1].trim());
    const name = stripQuotes(locationNameMatch[2].trim());
    if (!location || !name) return { warning: "empty location or name" };
    return { term: { kind: "location_name", location, name } };
  }

  return { warning: `unrecognized clause "${body}" — expected subgraph: or location:+name:` };
}

function stripQuotes(value: string): string {
  if (value.length >= 2) {
    const first = value[0];
    const last = value[value.length - 1];
    if ((first === '"' && last === '"') || (first === "'" && last === "'")) {
      return value.slice(1, -1);
    }
  }
  return value;
}
