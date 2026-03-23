import { getRepoNodeTypeDefinition } from "../repo/operations.js";
import { loadNodeTypeCorpusSnapshot } from "../corpus/loader.js";
import { searchNodeTypeCorpusVariants } from "../corpus/search.js";
import { resolveOptionalRepoPathInput } from "../repo/path.js";
import { isPlainObject } from "../../utils.js";

export interface NodeTypeSchema {
  config: Array<{
    groupName: string;
    items: Array<{
      attributeName?: string;
      type: string;
      isRequired?: boolean | string;
      default?: unknown;
      enableIf?: string;
      displayName?: string;
    }>;
  }>;
}

export interface SchemaResolution {
  source: "repo" | "corpus";
  schema: NodeTypeSchema;
}

function normalizeNodeTypeFamily(nodeType: string): string {
  return nodeType.trim().toLowerCase().replace(/[^a-z0-9]/g, "");
}

function parseNodeTypeSchema(
  nodeDefinition: unknown,
  sourceLabel: string
): NodeTypeSchema {
  if (!isPlainObject(nodeDefinition) || !Array.isArray(nodeDefinition.config)) {
    throw new Error(`${sourceLabel} node definition does not contain a config array`);
  }

  const config = nodeDefinition.config.flatMap((group) => {
    if (!isPlainObject(group) || !Array.isArray(group.items)) {
      return [];
    }

    const items = group.items.flatMap((item) => {
      if (!isPlainObject(item) || typeof item.type !== "string") {
        return [];
      }

      return [{
        attributeName:
          typeof item.attributeName === "string" ? item.attributeName : undefined,
        type: item.type,
        isRequired:
          typeof item.isRequired === "boolean" || typeof item.isRequired === "string"
            ? item.isRequired
            : undefined,
        default: item.default,
        enableIf: typeof item.enableIf === "string" ? item.enableIf : undefined,
        displayName:
          typeof item.displayName === "string" ? item.displayName : undefined,
      }];
    });

    return [{
      groupName: typeof group.groupName === "string" ? group.groupName : "Config",
      items,
    }];
  });

  return { config };
}

export async function resolveNodeTypeSchema(
  nodeType: string,
  repoPath?: string
): Promise<SchemaResolution> {
  // Resolve repoPath with COALESCE_REPO_PATH env var fallback
  const resolvedRepoPath = resolveOptionalRepoPathInput(repoPath);

  // Try repo first if path provided
  if (resolvedRepoPath) {
    try {
      const def = await getRepoNodeTypeDefinition(resolvedRepoPath, nodeType);
      return {
        source: "repo",
        schema: parseNodeTypeSchema(def.nodeDefinition, "Repo"),
      };
    } catch (error) {
      // Fall through to corpus
    }
  }

  // Try corpus fallback
  try {
    const snapshot = loadNodeTypeCorpusSnapshot();
    const normalizedFamily = normalizeNodeTypeFamily(nodeType);
    const result = searchNodeTypeCorpusVariants(snapshot, {
      normalizedFamily,
      supportStatus: "supported",
      limit: 1,
    });

    if (result.matches.length === 0) {
      throw new Error(
        `No supported corpus variant found for normalized family '${normalizedFamily}'`
      );
    }

    const variant = snapshot.variants.find(
      (v) => v.variantKey === result.matches[0].variantKey
    );

    if (!variant?.nodeDefinition) {
      throw new Error(
        `Corpus variant ${result.matches[0].variantKey} has no parseable definition`
      );
    }

    return {
      source: "corpus",
      schema: parseNodeTypeSchema(variant.nodeDefinition, "Corpus"),
    };
  } catch (error) {
    throw new Error(
      `Cannot resolve node type schema for '${nodeType}'. ` +
      `Repo resolution failed${resolvedRepoPath ? "" : " (no repoPath provided, COALESCE_REPO_PATH not set)"} and corpus lookup failed: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}
