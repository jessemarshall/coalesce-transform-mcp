import { isPlainObject } from "../../utils.js";

const SQL_OVERRIDE_CONTROL_TYPE = "overrideSQLToggle";

function cloneValue<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

export type SqlOverridePolicySanitizationResult = {
  nodeDefinition: Record<string, unknown>;
  warnings: string[];
};

export function filterSqlOverrideControls(values: string[]): string[] {
  return values.filter((value) => value !== SQL_OVERRIDE_CONTROL_TYPE);
}

export function sanitizeNodeDefinitionSqlOverridePolicy(
  nodeDefinition: Record<string, unknown>
): SqlOverridePolicySanitizationResult {
  const cloned = cloneValue(nodeDefinition);
  let removedControlCount = 0;
  let rewrittenExpressionCount = 0;

  if (Array.isArray(cloned.config)) {
    cloned.config = cloned.config.flatMap((group) => {
      if (!isPlainObject(group)) {
        return [];
      }

      const items = Array.isArray(group.items)
        ? group.items.filter((item) => {
            if (!isPlainObject(item)) {
              return false;
            }
            if (item.type === SQL_OVERRIDE_CONTROL_TYPE) {
              removedControlCount += 1;
              return false;
            }
            return true;
          })
        : [];

      if (items.length === 0) {
        return [];
      }

      return [{ ...group, items }];
    });
  }

  function sanitizeValue(value: unknown): unknown {
    if (Array.isArray(value)) {
      return value.map((entry) => sanitizeValue(entry));
    }

    if (!isPlainObject(value)) {
      return value;
    }

    const sanitized: Record<string, unknown> = {};
    for (const [key, entryValue] of Object.entries(value)) {
      if (
        (key === "enableIf" || key === "disableIf") &&
        typeof entryValue === "string"
      ) {
        const rewritten = entryValue.replace(
          /node\.override\.[A-Za-z0-9_.]+/gu,
          "false"
        );
        if (rewritten !== entryValue) {
          rewrittenExpressionCount += 1;
        }
        sanitized[key] = rewritten;
        continue;
      }

      sanitized[key] = sanitizeValue(entryValue);
    }

    return sanitized;
  }

  const sanitizedDefinition = sanitizeValue(cloned) as Record<string, unknown>;
  const warnings: string[] = [];

  if (removedControlCount > 0) {
    warnings.push(
      `Removed ${removedControlCount} SQL override control(s) from the returned node definition because SQL override is disallowed for this project.`
    );
  }

  if (rewrittenExpressionCount > 0) {
    warnings.push(
      `Rewrote ${rewrittenExpressionCount} conditional expression(s) that referenced node.override.* so returned definitions behave as if SQL override is disabled.`
    );
  }

  return {
    nodeDefinition: sanitizedDefinition,
    warnings,
  };
}

function formatPathSegment(segment: string): string {
  return /^\[\d+\]$/u.test(segment) ? segment : `.${segment}`;
}

function formatPath(segments: string[]): string {
  if (segments.length === 0) {
    return "<root>";
  }

  return segments.reduce((path, segment, index) => {
    if (index === 0 && !/^\[\d+\]$/u.test(segment)) {
      return segment;
    }
    return `${path}${formatPathSegment(segment)}`;
  }, "");
}

function collectSqlOverridePaths(
  value: unknown,
  pathSegments: string[] = []
): string[] {
  if (Array.isArray(value)) {
    return value.flatMap((entry, index) =>
      collectSqlOverridePaths(entry, [...pathSegments, `[${index}]`])
    );
  }

  if (!isPlainObject(value)) {
    return [];
  }

  const matches: string[] = [];
  for (const [key, entryValue] of Object.entries(value)) {
    const nextPath = [...pathSegments, key];
    if (key === "overrideSQL" || key === "override") {
      matches.push(formatPath(nextPath));
      continue;
    }

    matches.push(...collectSqlOverridePaths(entryValue, nextPath));
  }

  return matches;
}

export function assertNoSqlOverridePayload(
  value: unknown,
  context: string
): void {
  const offendingPaths = Array.from(new Set(collectSqlOverridePaths(value)));
  if (offendingPaths.length === 0) {
    return;
  }

  throw new Error(
    `${context} cannot set SQL override fields. Remove ${offendingPaths.join(
      ", "
    )}. SQL override is intentionally disallowed in this project.`
  );
}
