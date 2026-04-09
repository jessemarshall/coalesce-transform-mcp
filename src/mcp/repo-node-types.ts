import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CoalesceClient } from "../client.js";
import {
  buildSetWorkspaceNodeTemplateFromDefinition,
  compareGeneratedTemplateToWorkspaceNode,
  renderYaml,
  type GeneratedNodeDefinitionTemplate,
} from "../services/templates/nodes.js";
import { sanitizeNodeDefinitionSqlOverridePolicy } from "../services/policies/sql-override.js";
import { getWorkspaceNode } from "../coalesce/api/nodes.js";
import {
  READ_ONLY_ANNOTATIONS,
  READ_ONLY_LOCAL_ANNOTATIONS,
  type ToolDefinition,
} from "../coalesce/types.js";
import {
  parseRepo,
  resolveRepoNodeType,
  type ParsedRepo,
  type RepoNodeTypeRecord,
  type RepoNodeTypeResolution,
  type RepoPackageRecord,
} from "../services/repo/parser.js";
import { resolveRepoPathInput } from "../services/repo/path.js";
import { isPlainObject } from "../utils.js";
import { defineSimpleTool, defineLocalTool } from "./tool-helpers.js";

function compareStrings(left: string, right: string): number {
  return left.localeCompare(right, undefined, {
    numeric: true,
    sensitivity: "case",
  });
}

function requireComparisonPair(params: {
  workspaceID?: string;
  nodeID?: string;
}): void {
  if ((params.workspaceID && !params.nodeID) || (!params.workspaceID && params.nodeID)) {
    throw new Error(
      "workspaceID and nodeID must be provided together when requesting a live node comparison."
    );
  }
}

function summarizePackageRecord(record: RepoPackageRecord) {
  return {
    alias: record.alias,
    aliasSource: record.aliasSource,
    packageFilePath: record.packageFilePath,
    packageID: record.packageID,
    releaseID: record.releaseID,
    packageVariables: record.packageVariables,
    enabledNodeTypeIDs: record.enabledNodeTypeIDs,
    resolvedDefinitionIDs: record.resolvedDefinitionIDs,
    missingDefinitionIDs: record.missingDefinitionIDs,
    ambiguousDefinitionIDs: record.ambiguousDefinitionIDs,
    usageByNodeTypeID: record.usageByNodeTypeID,
    usageCount: record.usageCount,
    warnings: record.warnings,
  };
}

function buildRepoSummary(parsedRepo: ParsedRepo) {
  return {
    repoPath: parsedRepo.summary.repoPath,
    resolvedRepoPath: parsedRepo.summary.resolvedRepoPath,
    packageCount: parsedRepo.summary.packageCount,
    uniquePackageAliasCount: parsedRepo.summary.uniquePackageAliasCount,
    nodeTypeDefinitionCount: parsedRepo.summary.nodeTypeDefinitionCount,
    uniqueNodeTypeIDCount: parsedRepo.summary.uniqueNodeTypeIDCount,
    nodeCount: parsedRepo.summary.nodeCount,
    warnings: parsedRepo.summary.warnings,
  };
}

function buildNodeTypeRow(
  resolution: RepoNodeTypeResolution
): {
  nodeType: string;
  outerID: string | null;
  displayName: string | null;
  resolutionKind: "direct" | "package";
  packageAlias?: string;
  definitionPath: string;
  createPath: string | null;
  runPath: string | null;
  usageCount: number;
  parseError: string | null;
  warnings: string[];
} {
  return {
    nodeType: resolution.resolvedNodeType,
    outerID: resolution.nodeTypeRecord.outerDefinition.id,
    displayName: resolution.nodeTypeRecord.outerDefinition.name,
    resolutionKind: resolution.resolutionKind,
    ...(resolution.resolutionKind === "package"
      ? { packageAlias: resolution.packageAlias }
      : {}),
    definitionPath: resolution.nodeTypeRecord.definitionPath,
    createPath: resolution.nodeTypeRecord.createPath,
    runPath: resolution.nodeTypeRecord.runPath,
    usageCount: resolution.usageCount,
    parseError: resolution.nodeTypeRecord.parseError,
    warnings: resolution.nodeTypeRecord.warnings,
  };
}

function buildResolutionMetadata(resolution: RepoNodeTypeResolution) {
  if (resolution.resolutionKind === "package") {
    return {
      resolutionKind: resolution.resolutionKind,
      requestedNodeType: resolution.requestedNodeType,
      resolvedNodeType: resolution.resolvedNodeType,
      packageAlias: resolution.packageAlias,
      packageFilePath: resolution.packageRecord.packageFilePath,
      packageID: resolution.packageRecord.packageID,
      releaseID: resolution.packageRecord.releaseID,
      enabledNodeTypeID: resolution.nodeTypeRecord.outerDefinition.id,
      usageCount: resolution.usageCount,
    };
  }

  return {
    resolutionKind: resolution.resolutionKind,
    requestedNodeType: resolution.requestedNodeType,
    resolvedNodeType: resolution.resolvedNodeType,
    usageCount: resolution.usageCount,
  };
}

function sanitizeRepoNodeDefinition(
  nodeDefinition: RepoNodeTypeRecord["nodeDefinition"],
  nodeMetadataSpec: string | null,
  warnings: string[]
) {
  if (!isPlainObject(nodeDefinition)) {
    return {
      nodeDefinition,
      nodeMetadataSpecYaml: nodeMetadataSpec,
      warnings,
    };
  }

  const sanitized = sanitizeNodeDefinitionSqlOverridePolicy(nodeDefinition);
  return {
    nodeDefinition: sanitized.nodeDefinition,
    nodeMetadataSpecYaml: renderYaml(sanitized.nodeDefinition),
    warnings: Array.from(new Set([...warnings, ...sanitized.warnings])),
  };
}

export function listRepoPackages(params: { repoPath?: string }) {
  const repoPath = resolveRepoPathInput(params.repoPath);
  const parsedRepo = parseRepo(repoPath);
  return {
    summary: buildRepoSummary(parsedRepo),
    packages: parsedRepo.packages
      .slice()
      .sort((left, right) => compareStrings(left.alias, right.alias))
      .map(summarizePackageRecord),
  };
}

export function listRepoNodeTypes(params: {
  repoPath?: string;
  packageAlias?: string;
  inUseOnly?: boolean;
}) {
  const repoPath = resolveRepoPathInput(params.repoPath);
  const parsedRepo = parseRepo(repoPath);
  const rows: ReturnType<typeof buildNodeTypeRow>[] = [];
  const warnings = [...parsedRepo.summary.warnings];

  if (params.packageAlias) {
    const packageMatches = parsedRepo.packagesByAlias.get(params.packageAlias) ?? [];
    if (packageMatches.length === 0) {
      warnings.push(`No package alias ${params.packageAlias} was found under packages/.`);
    } else if (packageMatches.length > 1) {
      warnings.push(
        `Package alias ${params.packageAlias} is ambiguous across ${packageMatches
          .map((match) => match.packageFilePath)
          .join(", ")}.`
      );
    } else {
      for (const definitionID of packageMatches[0].resolvedDefinitionIDs) {
        const resolution = resolveRepoNodeType(
          parsedRepo,
          `${params.packageAlias}:::${definitionID}`
        );
        rows.push(buildNodeTypeRow(resolution));
      }
    }
  } else {
    for (const [id, matches] of parsedRepo.nodeTypesByID.entries()) {
      if (matches.length !== 1) {
        continue;
      }
      rows.push(buildNodeTypeRow(resolveRepoNodeType(parsedRepo, id)));
    }

    for (const packageRecord of parsedRepo.packages) {
      const packageMatches = parsedRepo.packagesByAlias.get(packageRecord.alias) ?? [];
      if (packageMatches.length !== 1) {
        continue;
      }
      for (const definitionID of packageRecord.resolvedDefinitionIDs) {
        rows.push(
          buildNodeTypeRow(
            resolveRepoNodeType(parsedRepo, `${packageRecord.alias}:::${definitionID}`)
          )
        );
      }
    }
  }

  const filteredRows = (params.inUseOnly ? rows.filter((row) => row.usageCount > 0) : rows)
    .sort((left, right) =>
      right.usageCount === left.usageCount
        ? compareStrings(left.nodeType, right.nodeType)
        : right.usageCount - left.usageCount
    );

  return {
    summary: {
      ...buildRepoSummary(parsedRepo),
      warnings: Array.from(new Set(warnings)).sort(compareStrings),
      packageAlias: params.packageAlias ?? null,
      inUseOnly: params.inUseOnly ?? false,
      matchedCount: filteredRows.length,
    },
    nodeTypes: filteredRows,
  };
}

export function getRepoNodeTypeDefinition(params: {
  repoPath?: string;
  nodeType: string;
}) {
  const repoPath = resolveRepoPathInput(params.repoPath);
  const parsedRepo = parseRepo(repoPath);
  const resolution = resolveRepoNodeType(parsedRepo, params.nodeType);
  const sanitized = sanitizeRepoNodeDefinition(
    resolution.nodeTypeRecord.nodeDefinition,
    resolution.nodeTypeRecord.nodeMetadataSpec,
    resolution.nodeTypeRecord.warnings
  );

  return {
    repoPath,
    resolvedRepoPath: parsedRepo.summary.resolvedRepoPath,
    repoWarnings: parsedRepo.summary.warnings,
    requestedNodeType: params.nodeType,
    resolvedNodeType: resolution.resolvedNodeType,
    resolution: buildResolutionMetadata(resolution),
    ...(resolution.resolutionKind === "package"
      ? {
          package: summarizePackageRecord(resolution.packageRecord),
        }
      : {}),
    outerDefinition: resolution.nodeTypeRecord.outerDefinition,
    nodeMetadataSpecYaml: sanitized.nodeMetadataSpecYaml,
    nodeDefinition: sanitized.nodeDefinition,
    parseError: resolution.nodeTypeRecord.parseError,
    filePaths: {
      definitionPath: resolution.nodeTypeRecord.definitionPath,
      createPath: resolution.nodeTypeRecord.createPath,
      runPath: resolution.nodeTypeRecord.runPath,
    },
    usageSummary: {
      exactNodeType: resolution.resolvedNodeType,
      usageCount: resolution.usageCount,
    },
    warnings: sanitized.warnings,
  };
}

async function maybeBuildComparison(
  client: CoalesceClient,
  generated: GeneratedNodeDefinitionTemplate,
  params: { workspaceID?: string; nodeID?: string }
) {
  requireComparisonPair(params);
  if (!params.workspaceID || !params.nodeID) {
    return undefined;
  }

  const workspaceNode = await getWorkspaceNode(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
  });
  if (!isPlainObject(workspaceNode)) {
    throw new Error("Workspace node comparison target was not an object");
  }

  return {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
    result: compareGeneratedTemplateToWorkspaceNode(generated, workspaceNode),
  };
}

export async function generateSetWorkspaceNodeTemplate(
  client: CoalesceClient,
  params: {
    definition?: Record<string, unknown>;
    repoPath?: string;
    nodeType?: string;
    nodeName?: string;
    locationName?: string;
    database?: string;
    schema?: string;
    workspaceID?: string;
    nodeID?: string;
  }
) {
  const usingDefinition = params.definition !== undefined;

  if (usingDefinition && params.repoPath !== undefined) {
    throw new Error(
      "Provide exactly one input mode: either definition for raw mode or repoPath with nodeType for repo mode."
    );
  }

  if (usingDefinition && !isPlainObject(params.definition)) {
    throw new Error("definition must be an object when using raw mode.");
  }

  if (!usingDefinition && !params.nodeType) {
    throw new Error("nodeType is required when using repo mode.");
  }

  let generated: GeneratedNodeDefinitionTemplate;
  let result: Record<string, unknown>;

  if (usingDefinition) {
    const sanitizedDefinition = sanitizeNodeDefinitionSqlOverridePolicy(
      params.definition!
    );
    generated = buildSetWorkspaceNodeTemplateFromDefinition(params.definition!, {
      nodeName: params.nodeName,
      nodeType: params.nodeType,
      locationName: params.locationName,
      database: params.database,
      schema: params.schema,
    });

    result = {
      definitionSource: {
        mode: "raw",
      },
      nodeDefinition: sanitizedDefinition.nodeDefinition,
      ...generated,
      warnings: Array.from(new Set([...generated.warnings, ...sanitizedDefinition.warnings])),
      setWorkspaceNodeBodyTemplateYaml: renderYaml(
        generated.setWorkspaceNodeBodyTemplate
      ),
    };
  } else {
    const repoPath = resolveRepoPathInput(params.repoPath);
    const definition = getRepoNodeTypeDefinition({
      repoPath,
      nodeType: params.nodeType!,
    });

    if (!isPlainObject(definition.nodeDefinition)) {
      const parseError =
        typeof definition.parseError === "string" ? definition.parseError : "unknown";
      throw new Error(
        `Repo-backed definition could not be resolved for template generation because metadata.nodeMetadataSpec could not be parsed. Parse error: ${parseError} Use the corpus tools (search_node_type_variants, get_node_type_variant, or generate_set_workspace_node_template_from_variant) as the next step.`
      );
    }

    generated = buildSetWorkspaceNodeTemplateFromDefinition(definition.nodeDefinition, {
      nodeName: params.nodeName,
      nodeType: definition.resolvedNodeType,
      locationName: params.locationName,
      database: params.database,
      schema: params.schema,
    });

    result = {
      definitionSource: {
        mode: "repo",
      },
      repoPath,
      resolvedRepoPath: definition.resolvedRepoPath,
      requestedNodeType: definition.requestedNodeType,
      resolvedNodeType: definition.resolvedNodeType,
      resolution: definition.resolution,
      ...(definition.package ? { package: definition.package } : {}),
      outerDefinition: definition.outerDefinition,
      nodeMetadataSpecYaml: definition.nodeMetadataSpecYaml,
      nodeDefinition: definition.nodeDefinition,
      filePaths: definition.filePaths,
      usageSummary: definition.usageSummary,
      repoWarnings: definition.repoWarnings,
      ...generated,
      warnings: Array.from(
        new Set([...(definition.warnings as string[]), ...generated.warnings])
      ),
      setWorkspaceNodeBodyTemplateYaml: renderYaml(
        generated.setWorkspaceNodeBodyTemplate
      ),
    };
  }

  const comparison = await maybeBuildComparison(client, generated, params);
  return comparison ? { ...result, comparison } : result;
}

export function defineRepoNodeTypeTools(
  _server: McpServer,
  client: CoalesceClient
): ToolDefinition[] {
  return [
  defineLocalTool("list_repo_packages", {
    title: "List Repo Packages",
    description: "Inspect a committed local Coalesce repo and list package aliases from packages/*.yml. Use this when a local repo is available and you want repo-backed node-type discovery before falling back to the corpus.",
    inputSchema: z.object({
      repoPath: z
        .string()
        .optional()
        .describe("Optional absolute or relative path to the local committed Coalesce repo. Falls back to COALESCE_REPO_PATH when omitted."),
    }),
    annotations: READ_ONLY_LOCAL_ANNOTATIONS,
  }, listRepoPackages),

  defineLocalTool("list_repo_node_types", {
    title: "List Repo Node Types",
    description: "Inspect a committed local Coalesce repo and list exact resolvable node-type identifiers from nodeTypes/, optionally filtered to one package alias. Repo-backed discovery is preferred when the repo contains the committed definition; otherwise use the corpus tools.",
    inputSchema: z.object({
      repoPath: z
        .string()
        .optional()
        .describe("Optional absolute or relative path to the local committed Coalesce repo. Falls back to COALESCE_REPO_PATH when omitted."),
      packageAlias: z
        .string()
        .optional()
        .describe("Optional exact package alias filter sourced from package YAML name."),
      inUseOnly: z
        .boolean()
        .optional()
        .describe("When true, return only identifiers currently referenced by committed nodes/*.yml operation.sqlType values."),
    }),
    annotations: READ_ONLY_LOCAL_ANNOTATIONS,
  }, listRepoNodeTypes),

  defineLocalTool("get_repo_node_type_definition", {
    title: "Get Repo Node Type Definition",
    description: "Resolve one exact node type from a committed local Coalesce repo. Supports direct identifiers like Stage or 65 and package-backed identifiers like alias:::id. If the repo cannot resolve the definition exactly, use the corpus tools as the fallback path.",
    inputSchema: z.object({
      repoPath: z
        .string()
        .optional()
        .describe("Optional absolute or relative path to the local committed Coalesce repo. Falls back to COALESCE_REPO_PATH when omitted."),
      nodeType: z
        .string()
        .describe("Exact direct node type identifier or exact package-backed alias:::id value."),
    }),
    annotations: READ_ONLY_LOCAL_ANNOTATIONS,
  }, getRepoNodeTypeDefinition),

  defineSimpleTool(client, "generate_set_workspace_node_template", {
    title: "Generate Set Workspace Node Template",
    description: "Generate a YAML-friendly set_workspace_node body template either from a raw node definition object or by resolving a committed node type from a local repo. Prefer repo mode when a local committed repo contains the definition; use the corpus tools when repo-backed resolution is unavailable. SQL override controls are removed from returned templates because they are disallowed in this project.",
    inputSchema: z.object({
      definition: z
        .record(z.unknown())
        .optional()
        .describe("Raw parsed node definition object for template generation in raw mode."),
      repoPath: z
        .string()
        .optional()
        .describe("Optional local committed repo path for repo mode. Falls back to COALESCE_REPO_PATH when omitted."),
      nodeType: z
        .string()
        .optional()
        .describe("Exact repo node type identifier in repo mode, or optional nodeType override in raw mode."),
      nodeName: z
        .string()
        .optional()
        .describe("Optional node name to inject into the generated template."),
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
      workspaceID: z
        .string()
        .optional()
        .describe("Optional workspace ID for comparing inferred mappings to a live workspace node."),
      nodeID: z
        .string()
        .optional()
        .describe("Optional node ID for comparing inferred mappings to a live workspace node."),
    }),
    annotations: READ_ONLY_ANNOTATIONS,
  }, generateSetWorkspaceNodeTemplate),
  ];
}
