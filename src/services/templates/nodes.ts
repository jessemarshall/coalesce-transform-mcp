import { sanitizeNodeDefinitionSqlOverridePolicy } from "../policies/sql-override.js";
import { isPlainObject } from "../../utils.js";
import { deepClone } from "../shared/node-helpers.js";

export type NodeDefinitionTemplateOptions = {
  nodeName?: string;
  nodeType?: string;
  locationName?: string;
  database?: string;
  schema?: string;
};

export type NodeDefinitionFieldMapping = {
  groupIndex: number;
  itemIndex: number;
  groupName: string | null;
  itemType: string | null;
  displayName: string | null;
  attributeName: string | null;
  targetPath: string | null;
  defaultValue: unknown;
  enableIf: string | null;
  note: string;
};

type InferredTargetMapping = {
  targetPath: string | null;
  note: string;
};

export type GeneratedNodeDefinitionTemplate = {
  definitionSummary: {
    capitalized: string | null;
    short: string | null;
    plural: string | null;
    tagColor: string | null;
    configGroupCount: number;
    configItemCount: number;
  };
  fieldMappings: NodeDefinitionFieldMapping[];
  inferredTopLevelFields: Record<string, unknown>;
  inferredConfig: Record<string, unknown>;
  setWorkspaceNodeBodyTemplate: Record<string, unknown>;
  usageGuidance: string[];
  warnings: string[];
};

export type TemplateComparisonResult = {
  checkedFieldCount: number;
  matchedFieldCount: number;
  mismatchedFieldCount: number;
  missingFieldCount: number;
  fields: Array<{
    targetPath: string;
    inferredDefault: unknown;
    actualValue: unknown;
    status: "matched" | "mismatched" | "missing";
  }>;
};

function getString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function getFirstOptionValue(value: unknown): unknown {
  if (!Array.isArray(value) || value.length === 0) {
    return undefined;
  }

  const [first] = value;
  if (
    isPlainObject(first) &&
    Object.prototype.hasOwnProperty.call(first, "value")
  ) {
    return first.value;
  }
  return first;
}

function inferDefaultValue(item: Record<string, unknown>): unknown {
  if (Object.prototype.hasOwnProperty.call(item, "default")) {
    return deepClone(item.default);
  }

  const type = getString(item.type);
  switch (type) {
    case "materializationSelector":
      return getFirstOptionValue(item.options) ?? "table";
    case "multisourceToggle":
      return false;
    case "overrideSQLToggle":
      return false;
    case "toggleButton":
      return false;
    case "textBox":
      return "";
    case "dropdownSelector":
      return getFirstOptionValue(item.options);
    default:
      return undefined;
  }
}

function inferTargetMappings(
  item: Record<string, unknown>
): InferredTargetMapping[] {
  const type = getString(item.type);
  const attributeName = getString(item.attributeName);

  switch (type) {
    case "materializationSelector": {
      const mappings: InferredTargetMapping[] = [
        {
          targetPath: "materializationType",
          note: "Built-in selector maps to the top-level node materialization field.",
        },
      ];
      if (attributeName) {
        mappings.push({
          targetPath: `config.${attributeName}`,
          note: "Built-in selector also persists under config.<attributeName> when the definition supplies a custom attributeName.",
        });
      }
      return mappings;
    }
    case "multisourceToggle": {
      const mappings: InferredTargetMapping[] = [
        {
          targetPath: "isMultisource",
          note: "Built-in toggle maps to the top-level multisource flag.",
        },
      ];
      if (attributeName) {
        mappings.push({
          targetPath: `config.${attributeName}`,
          note: "Built-in toggle also persists under config.<attributeName> when the definition supplies a custom attributeName.",
        });
      }
      return mappings;
    }
    case "overrideSQLToggle": {
      const mappings: InferredTargetMapping[] = [
        {
          targetPath: "overrideSQL",
          note: "Built-in toggle maps to the top-level override SQL flag.",
        },
      ];
      if (attributeName) {
        mappings.push({
          targetPath: `config.${attributeName}`,
          note: "Built-in toggle also persists under config.<attributeName> when the definition supplies a custom attributeName.",
        });
      }
      return mappings;
    }
    case "columnSelector":
      if (attributeName) {
        return [
          {
            targetPath: `columns[].${attributeName}`,
            note: `Column-level attribute. Set "${attributeName}: true" on each column object in metadata.columns that should be selected. Look up the attributeName in the node type definition file under nodeTypes/ in the local repo.`,
          },
        ];
      }
      return [
        {
          targetPath: null,
          note: "columnSelector without attributeName — cannot determine column-level target.",
        },
      ];
    default:
      if (attributeName) {
        return [
          {
            targetPath: `config.${attributeName}`,
            note: "Generic node-definition input maps into the hydrated config object.",
          },
        ];
      }
      return [
        {
          targetPath: null,
          note: "No attributeName or built-in target mapping was found for this item.",
        },
      ];
  }
}

function setByPath(
  target: Record<string, unknown>,
  path: string,
  value: unknown
): void {
  const parts = path.split(".");
  let cursor: Record<string, unknown> = target;

  for (let index = 0; index < parts.length - 1; index += 1) {
    const part = parts[index];
    const current = cursor[part];
    if (!isPlainObject(current)) {
      cursor[part] = {};
    }
    cursor = cursor[part] as Record<string, unknown>;
  }

  cursor[parts[parts.length - 1]] = value;
}

function getByPath(source: Record<string, unknown>, path: string): unknown {
  const parts = path.split(".");
  let current: unknown = source;

  for (const part of parts) {
    if (!isPlainObject(current) || !(part in current)) {
      return undefined;
    }
    current = current[part];
  }

  return current;
}

export function buildSetWorkspaceNodeTemplateFromDefinition(
  nodeDefinition: Record<string, unknown>,
  options: NodeDefinitionTemplateOptions = {}
): GeneratedNodeDefinitionTemplate {
  const sanitizedDefinition = sanitizeNodeDefinitionSqlOverridePolicy(nodeDefinition);
  const configGroups = Array.isArray(sanitizedDefinition.nodeDefinition.config)
    ? sanitizedDefinition.nodeDefinition.config.filter(isPlainObject)
    : [];
  const configItemCount = configGroups.reduce((count, group) => {
    const items = Array.isArray(group.items) ? group.items.filter(isPlainObject) : [];
    return count + items.length;
  }, 0);

  const fieldMappings: NodeDefinitionFieldMapping[] = [];
  const inferredTopLevelFields: Record<string, unknown> = {};
  const inferredConfig: Record<string, unknown> = {};
  const warnings: string[] = [...sanitizedDefinition.warnings];

  configGroups.forEach((group, groupIndex) => {
    const groupName = getString(group.groupName);
    const items = Array.isArray(group.items) ? group.items.filter(isPlainObject) : [];

    items.forEach((item, itemIndex) => {
      const defaultValue = inferDefaultValue(item);
      const targetMappings = inferTargetMappings(item);

      targetMappings.forEach(({ targetPath, note }) => {
        const mapping: NodeDefinitionFieldMapping = {
          groupIndex,
          itemIndex,
          groupName,
          itemType: getString(item.type),
          displayName: getString(item.displayName),
          attributeName: getString(item.attributeName),
          targetPath,
          defaultValue,
          enableIf: getString(item.enableIf),
          note,
        };
        fieldMappings.push(mapping);

        if (!targetPath) {
          warnings.push(
            `Config item ${groupIndex}.${itemIndex} (${getString(item.type) ?? "unknown"}) does not map cleanly to set_workspace_node body fields.`
          );
          return;
        }

        if (targetPath.includes("[]")) {
          // Column-level paths (e.g., columns[].isBusinessKey) are informational —
          // documented in fieldMappings and usageGuidance but not settable as
          // top-level fields. Feeding them to setByPath would create garbage keys
          // like {"columns[]": {"isBusinessKey": true}}.
          return;
        }

        if (defaultValue === undefined) {
          warnings.push(
            `Config item ${targetPath} has no inferred default. Fill it before calling set_workspace_node if the node type requires it.`
          );
          return;
        }

        if (targetPath.startsWith("config.")) {
          setByPath(inferredConfig, targetPath.replace(/^config\./u, ""), defaultValue);
          return;
        }

        setByPath(inferredTopLevelFields, targetPath, defaultValue);
      });
    });
  });

  const capitalized = getString(sanitizedDefinition.nodeDefinition.capitalized);
  const short = getString(sanitizedDefinition.nodeDefinition.short);
  const plural = getString(sanitizedDefinition.nodeDefinition.plural);
  const tagColor = getString(sanitizedDefinition.nodeDefinition.tagColor);
  const defaultNodeName =
    options.nodeName ??
    (short ? `${short}_NODE` : capitalized ? `${capitalized.toUpperCase()}_NODE` : "NEW_NODE");
  const nodeType = options.nodeType ?? capitalized ?? "Stage";

  const setWorkspaceNodeBodyTemplate: Record<string, unknown> = {
    name: defaultNodeName,
    description: "",
    nodeType,
    ...(options.database !== undefined ? { database: options.database } : {}),
    ...(options.schema !== undefined ? { schema: options.schema } : {}),
    ...(options.locationName !== undefined
      ? { locationName: options.locationName }
      : {}),
    ...inferredTopLevelFields,
    config: inferredConfig,
    metadata: {
      columns: [],
      sourceMapping: [],
      cteString: "",
      appliedNodeTests: [],
      enabledColumnTestIDs: [],
    },
  };

  return {
    definitionSummary: {
      capitalized,
      short,
      plural,
      tagColor,
      configGroupCount: configGroups.length,
      configItemCount,
    },
    fieldMappings,
    inferredTopLevelFields,
    inferredConfig,
    setWorkspaceNodeBodyTemplate,
    usageGuidance: [
      "Use create_workspace_node_from_predecessor or create_workspace_node_from_scratch first to get a real workspace node ID.",
      "Fill metadata.columns and metadata.sourceMapping before calling set_workspace_node; those arrays are replace-on-write.",
      "Keep materializationType and isMultisource at the top level when the definition uses the built-in selector/toggle items.",
      "Keep generic definition attributes under config.<attributeName>.",
      "If a built-in selector or toggle also defines attributeName, mirror the same value under both the top-level field and config.<attributeName>.",
      "For columnSelector items (e.g., isBusinessKey, isChangeTracking), set the attributeName as a boolean directly on each column in metadata.columns — e.g., { name: 'CUSTOMER_ID', isBusinessKey: true, ... }. Look up attribute names in the node type definition file under nodeTypes/ in the local repo.",
      "Do not add overrideSQL or override.* fields; SQL override is intentionally disallowed in this project.",
    ],
    warnings,
  };
}

export function compareGeneratedTemplateToWorkspaceNode(
  generated: GeneratedNodeDefinitionTemplate,
  workspaceNode: Record<string, unknown>
): TemplateComparisonResult {
  const checks = generated.fieldMappings
    .filter((mapping) => mapping.targetPath)
    .map((mapping) => {
      const targetPath = mapping.targetPath!;
      const actualValue = getByPath(workspaceNode, targetPath);
      const inferredDefault = mapping.defaultValue;

      if (actualValue === undefined) {
        return {
          targetPath,
          inferredDefault,
          actualValue: undefined,
          status: "missing" as const,
        };
      }

      return {
        targetPath,
        inferredDefault,
        actualValue,
        status:
          JSON.stringify(actualValue) === JSON.stringify(inferredDefault)
            ? ("matched" as const)
            : ("mismatched" as const),
      };
    });

  return {
    checkedFieldCount: checks.length,
    matchedFieldCount: checks.filter((check) => check.status === "matched").length,
    mismatchedFieldCount: checks.filter((check) => check.status === "mismatched").length,
    missingFieldCount: checks.filter((check) => check.status === "missing").length,
    fields: checks,
  };
}

function stringifyYamlScalar(value: unknown): string {
  if (typeof value === "string") {
    if (value.length === 0) {
      return '""';
    }

    if (/^[A-Za-z0-9_.-]+$/u.test(value)) {
      return value;
    }

    return JSON.stringify(value);
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  if (value === null) {
    return "null";
  }

  return JSON.stringify(value);
}

function renderYamlValue(value: unknown, indentLevel: number): string[] {
  const indent = "  ".repeat(indentLevel);

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return [`${indent}[]`];
    }

    const lines: string[] = [];
    value.forEach((entry) => {
      if (Array.isArray(entry) || isPlainObject(entry)) {
        lines.push(`${indent}-`);
        lines.push(...renderYamlValue(entry, indentLevel + 1));
      } else {
        lines.push(`${indent}- ${stringifyYamlScalar(entry)}`);
      }
    });
    return lines;
  }

  if (isPlainObject(value)) {
    const entries = Object.entries(value);
    if (entries.length === 0) {
      return [`${indent}{}`];
    }

    const lines: string[] = [];
    for (const [key, entryValue] of entries) {
      if (Array.isArray(entryValue) || isPlainObject(entryValue)) {
        lines.push(`${indent}${key}:`);
        lines.push(...renderYamlValue(entryValue, indentLevel + 1));
      } else {
        lines.push(`${indent}${key}: ${stringifyYamlScalar(entryValue)}`);
      }
    }
    return lines;
  }

  return [`${indent}${stringifyYamlScalar(value)}`];
}

export function renderYaml(value: unknown): string {
  return `${renderYamlValue(value, 0).join("\n")}\n`;
}
