export interface ConfigItem {
  attributeName?: string;
  type: string;
  isRequired?: boolean | string;
  default?: unknown;
  enableIf?: string;
  displayName?: string;
}

export interface ClassifiedFields {
  required: string[];
  conditionalRequired: string[];
  optionalWithDefaults: string[];
  contextual: string[];
  columnSelectors: Array<{
    attributeName: string;
    displayName: string | undefined;
    isRequired: boolean;
  }>;
}

export function classifyConfigFields(
  config: Array<{ groupName: string; items: ConfigItem[] }>
): ClassifiedFields {
  const required: string[] = [];
  const conditionalRequired: string[] = [];
  const optionalWithDefaults: string[] = [];
  const contextual: string[] = [];
  const columnSelectors: ClassifiedFields["columnSelectors"] = [];

  for (const group of config) {
    for (const item of group.items) {
      // Skip items without attributeName
      if (!item.attributeName) {
        continue;
      }

      // columnSelector items are column-level attributes, not node-level config
      if (item.type === "columnSelector") {
        columnSelectors.push({
          attributeName: item.attributeName,
          displayName: item.displayName,
          isRequired: item.isRequired === true,
        });
        continue;
      }

      // Classify required fields (isRequired === true)
      if (item.isRequired === true) {
        required.push(item.attributeName);
      }
      // Conditional required (isRequired is string)
      else if (typeof item.isRequired === "string") {
        conditionalRequired.push(item.attributeName);
      }
      // Optional with defaults (default !== undefined)
      else if (item.default !== undefined) {
        optionalWithDefaults.push(item.attributeName);
      }
      // Contextual (everything else)
      else {
        contextual.push(item.attributeName);
      }
    }
  }

  return {
    required,
    conditionalRequired,
    optionalWithDefaults,
    contextual,
    columnSelectors,
  };
}
