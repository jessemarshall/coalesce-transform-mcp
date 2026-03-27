import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  loadNodeTypeCorpusSnapshot,
  type NodeTypeCorpusSupportStatus,
} from "../services/corpus/loader.js";
import {
  summarizeNodeTypeCorpus,
  searchNodeTypeCorpusVariants,
  getNodeTypeCorpusVariant,
  buildVariantSummary,
} from "../services/corpus/search.js";
import { validateVariantTemplateGeneration } from "../services/corpus/templates.js";
import {
  buildSetWorkspaceNodeTemplateFromDefinition,
  compareGeneratedTemplateToWorkspaceNode,
  renderYaml,
} from "../services/templates/nodes.js";
import {
  filterSqlOverrideControls,
  sanitizeNodeDefinitionSqlOverridePolicy,
} from "../services/policies/sql-override.js";
import { getWorkspaceNode } from "../coalesce/api/nodes.js";
import {
  buildJsonToolResponse,
  getToolOutputSchema,
  handleToolError,
  READ_ONLY_LOCAL_ANNOTATIONS,
} from "../coalesce/types.js";
import { isPlainObject } from "../utils.js";

function sanitizeVariantForResponse(variant: ReturnType<typeof getNodeTypeCorpusVariant>) {
  if (!variant.nodeDefinition) {
    return {
      ...variant,
      primitiveSignature: filterSqlOverrideControls(variant.primitiveSignature),
      controlSignature: filterSqlOverrideControls(variant.controlSignature),
      unsupportedPrimitives: filterSqlOverrideControls(variant.unsupportedPrimitives),
    };
  }

  const sanitized = sanitizeNodeDefinitionSqlOverridePolicy(variant.nodeDefinition);
  return {
    ...variant,
    primitiveSignature: filterSqlOverrideControls(variant.primitiveSignature),
    controlSignature: filterSqlOverrideControls(variant.controlSignature),
    unsupportedPrimitives: filterSqlOverrideControls(variant.unsupportedPrimitives),
    nodeDefinition: sanitized.nodeDefinition,
    nodeMetadataSpec: renderYaml(sanitized.nodeDefinition),
    warnings: sanitized.warnings,
  };
}

export function registerNodeTypeCorpusTools(
  server: McpServer,
  client: CoalesceClient
): void {
  server.registerTool(
    "coalesce_search_node_type_variants",
    {
      title: "Search Node Type Variants",
      description: "Search the generated node-type corpus snapshot by normalized family, package, primitive, or support status. This tool queries the committed snapshot and does not require access to the original external node source repo at runtime.",
      inputSchema: z.object({
        normalizedFamily: z
          .string()
          .optional()
          .describe("Case-insensitive exact match against the normalized family name."),
        packageName: z
          .string()
          .optional()
          .describe("Case-insensitive exact match against one of the package names that carries the variant."),
        primitive: z
          .string()
          .optional()
          .describe("Case-insensitive match against a primitive used in the node definition, such as tabular or materializationSelector."),
        supportStatus: z
          .enum(["supported", "partial"])
          .or(z.literal("parse_error"))
          .optional()
          .describe("Filter by current MCP support classification."),
        limit: z
          .number()
          .int()
          .positive()
          .optional()
          .describe("Maximum number of matches to return. Defaults to 25, max 200."),
      }),
      outputSchema: getToolOutputSchema("coalesce_search_node_type_variants"),
      annotations: READ_ONLY_LOCAL_ANNOTATIONS,
    },
    async (params) => {
      try {
        const snapshot = loadNodeTypeCorpusSnapshot();
        const result = {
          summary: summarizeNodeTypeCorpus(snapshot),
          ...searchNodeTypeCorpusVariants(snapshot, params),
        };
        return buildJsonToolResponse("coalesce_search_node_type_variants", result);
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_get_node_type_variant",
    {
      title: "Get Node Type Variant",
      description: "Get one node-type corpus variant from the committed snapshot by variantKey. Use coalesce_search_node_type_variants first when you need discovery.",
      inputSchema: z.object({
        variantKey: z.string().describe("The exact node-type corpus variant key."),
      }),
      outputSchema: getToolOutputSchema("coalesce_get_node_type_variant"),
      annotations: READ_ONLY_LOCAL_ANNOTATIONS,
    },
    async (params) => {
      try {
        const snapshot = loadNodeTypeCorpusSnapshot();
        const variant = getNodeTypeCorpusVariant(snapshot, params.variantKey);
        return buildJsonToolResponse(
          "coalesce_get_node_type_variant",
          sanitizeVariantForResponse(variant)
        );
      } catch (error) {
        return handleToolError(error);
      }
    }
  );

  server.registerTool(
    "coalesce_generate_set_workspace_node_template_from_variant",
    {
      title: "Generate Set Workspace Node Template from Variant",
      description: "Generate a coalesce_set_workspace_node body template from a node-type corpus variant stored in the committed snapshot. This avoids requiring the original external node source repo at runtime, rejects partial variants unless allowPartial=true, and can optionally compare the inferred template against a live workspace node. SQL override controls are removed from returned templates because they are disallowed in this project.",
      inputSchema: z.object({
        variantKey: z.string().describe("The exact node-type corpus variant key."),
        nodeName: z
          .string()
          .optional()
          .describe("Optional node name to inject into the generated template."),
        nodeType: z
          .string()
          .optional()
          .describe("Optional nodeType override. Defaults to the variant definition capitalized field."),
        locationName: z
          .string()
          .optional()
          .describe("Optional storage location name to include in the template."),
        database: z
          .string()
          .optional()
          .describe("Optional database value to include in the template."),
        schema: z
          .string()
          .optional()
          .describe("Optional schema value to include in the template."),
        allowPartial: z
          .boolean()
          .optional()
          .describe("When true, allow best-effort generation for variants currently marked partial."),
        workspaceID: z
          .string()
          .optional()
          .describe("Optional workspace ID for comparing inferred mappings to a live workspace node."),
        nodeID: z
          .string()
          .optional()
          .describe("Optional node ID for comparing inferred mappings to a live workspace node."),
      }),
      outputSchema: getToolOutputSchema("coalesce_generate_set_workspace_node_template_from_variant"),
      annotations: READ_ONLY_LOCAL_ANNOTATIONS,
    },
    async (params) => {
      try {
        if ((params.workspaceID && !params.nodeID) || (!params.workspaceID && params.nodeID)) {
          throw new Error(
            "workspaceID and nodeID must be provided together when requesting a live node comparison."
          );
        }

        const snapshot = loadNodeTypeCorpusSnapshot();
        const variant = getNodeTypeCorpusVariant(snapshot, params.variantKey);
        validateVariantTemplateGeneration(variant, {
          allowPartial: params.allowPartial,
        });
        const sanitizedVariant = sanitizeVariantForResponse(variant);
        if (!sanitizedVariant.nodeDefinition || !isPlainObject(sanitizedVariant.nodeDefinition)) {
          throw new Error("Sanitized node definition was not an object.");
        }

        const generated = buildSetWorkspaceNodeTemplateFromDefinition(
          variant.nodeDefinition!,
          {
            nodeName: params.nodeName,
            nodeType: params.nodeType,
            locationName: params.locationName,
            database: params.database,
            schema: params.schema,
          }
        );
        const filteredUnsupportedPrimitives = filterSqlOverrideControls(
          variant.unsupportedPrimitives
        );
        const warnings = Array.from(
          new Set(
            variant.supportStatus === "partial"
              ? [
                  `Best-effort template only. This variant uses unsupported primitives: ${
                    filteredUnsupportedPrimitives.length > 0
                      ? filteredUnsupportedPrimitives.join(", ")
                      : "none after SQL override controls were removed"
                  }.`,
                  ...generated.warnings,
                ]
              : generated.warnings
          )
        );

        let comparison:
          | {
              workspaceID: string;
              nodeID: string;
              result: ReturnType<typeof compareGeneratedTemplateToWorkspaceNode>;
            }
          | undefined;

        if (params.workspaceID && params.nodeID) {
          const workspaceNode = await getWorkspaceNode(client, {
            workspaceID: params.workspaceID,
            nodeID: params.nodeID,
          });
          if (!isPlainObject(workspaceNode)) {
            throw new Error("Workspace node comparison target was not an object");
          }
          comparison = {
            workspaceID: params.workspaceID,
            nodeID: params.nodeID,
            result: compareGeneratedTemplateToWorkspaceNode(
              generated,
              workspaceNode
            ),
          };
        }

        const result = {
          variant: buildVariantSummary(variant),
          nodeMetadataSpecYaml: sanitizedVariant.nodeMetadataSpec,
          ...generated,
          warnings,
          setWorkspaceNodeBodyTemplateYaml: renderYaml(
            generated.setWorkspaceNodeBodyTemplate
          ),
          ...(comparison ? { comparison } : {}),
        };
        return buildJsonToolResponse(
          "coalesce_generate_set_workspace_node_template_from_variant",
          result
        );
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
}
