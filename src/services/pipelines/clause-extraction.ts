import { findTopLevelKeywordIndex } from "./sql-tokenizer.js";

export function extractSelectClause(sql: string): string | null {
  const selectIndex = findTopLevelKeywordIndex(sql, "select");
  if (selectIndex < 0) {
    return null;
  }
  const fromIndex = findTopLevelKeywordIndex(sql, "from", selectIndex + 6);
  if (fromIndex < 0) {
    return null;
  }
  return sql.slice(selectIndex + 6, fromIndex).trim();
}

export function extractFromClause(sql: string): string | null {
  const selectIndex = findTopLevelKeywordIndex(sql, "select");
  if (selectIndex < 0) {
    return null;
  }
  const fromIndex = findTopLevelKeywordIndex(sql, "from", selectIndex + 6);
  if (fromIndex < 0) {
    return null;
  }
  return sql
    .slice(fromIndex)
    .trim()
    .replace(/;+\s*$/u, "");
}
