// Re-export hub — the implementation lives in focused modules.
// All external consumers can continue importing from "./sql-parsing.js".

export {
  normalizeSqlIdentifier,
  deepClone,
  normalizeWhitespace,
  buildSourceDependencyKey,
  getUniqueSourceDependencies,
  escapeRegExp,
} from "./sql-utils.js";

export {
  extractSelectClause,
  extractFromClause,
} from "./clause-extraction.js";

export { parseSqlSourceRefs } from "./source-parsing.js";

export { splitExpressionAlias, parseSqlSelectItems } from "./select-parsing.js";

export type { ParsedCte, CteColumn } from "./cte-parsing.js";
export { extractCtes } from "./cte-parsing.js";

export { buildCtePlan } from "./cte-planning.js";
