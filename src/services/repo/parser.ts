import {
  existsSync,
  readFileSync,
  readdirSync,
  realpathSync,
  statSync,
} from "node:fs";
import { basename, join, resolve } from "node:path";
import YAML from "yaml";
import { isPlainObject } from "../../utils.js";

export type RepoOuterDefinition = {
  fileVersion: unknown;
  id: string | null;
  isDisabled: boolean | null;
  name: string | null;
  type: string | null;
  inputMode: string | null;
};

export type RepoNodeTypeRecord = {
  dirName: string;
  dirPath: string;
  definitionPath: string;
  createPath: string | null;
  runPath: string | null;
  outerDefinition: RepoOuterDefinition;
  nodeMetadataSpec: string | null;
  nodeDefinition: Record<string, unknown> | null;
  parseError: string | null;
  warnings: string[];
};

export type RepoPackageRecord = {
  alias: string;
  aliasSource: "name" | "filename";
  packageFilePath: string;
  packageID: string | null;
  releaseID: string | null;
  packageVariables: string | null;
  enabledNodeTypeIDs: string[];
  resolvedDefinitionIDs: string[];
  missingDefinitionIDs: string[];
  ambiguousDefinitionIDs: string[];
  usageByNodeTypeID: Record<string, number>;
  usageCount: number;
  warnings: string[];
};

export type ParsedRepoSummary = {
  repoPath: string;
  resolvedRepoPath: string;
  packageCount: number;
  uniquePackageAliasCount: number;
  nodeTypeDefinitionCount: number;
  uniqueNodeTypeIDCount: number;
  nodeCount: number;
  warnings: string[];
};

export type ParsedRepo = {
  summary: ParsedRepoSummary;
  packages: RepoPackageRecord[];
  nodeTypes: RepoNodeTypeRecord[];
  usageCounts: Record<string, number>;
  packagesByAlias: Map<string, RepoPackageRecord[]>;
  nodeTypesByID: Map<string, RepoNodeTypeRecord[]>;
};

export type RepoNodeTypeResolution =
  | {
      resolutionKind: "direct";
      requestedNodeType: string;
      resolvedNodeType: string;
      usageCount: number;
      nodeTypeRecord: RepoNodeTypeRecord;
    }
  | {
      resolutionKind: "package";
      requestedNodeType: string;
      resolvedNodeType: string;
      packageAlias: string;
      packageRecord: RepoPackageRecord;
      usageCount: number;
      nodeTypeRecord: RepoNodeTypeRecord;
    };

function getScalarString(value: unknown): string | null {
  if (typeof value === "string") {
    return value;
  }
  if (
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return String(value);
  }
  return null;
}

function compareStrings(left: string, right: string): number {
  return left.localeCompare(right, undefined, {
    numeric: true,
    sensitivity: "case",
  });
}

function readYamlFile(filePath: string): unknown {
  return YAML.parse(readFileSync(filePath, "utf8"));
}

function listYamlFiles(dirPath: string): string[] {
  if (!existsSync(dirPath)) {
    return [];
  }

  return readdirSync(dirPath, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith(".yml"))
    .map((entry) => join(dirPath, entry.name))
    .sort(compareStrings);
}

function normalizeRepoPath(repoPath: string): string {
  const absolutePath = resolve(repoPath);
  if (!existsSync(absolutePath)) {
    throw new Error(
      "Repo path does not exist. Check the provided path or COALESCE_REPO_PATH environment variable."
    );
  }

  const stats = statSync(absolutePath);
  if (!stats.isDirectory()) {
    throw new Error(
      "Repo path is not a directory. Expected a Coalesce repo directory containing a nodeTypes/ subdirectory."
    );
  }

  const resolvedRepoPath = realpathSync(absolutePath);
  const nodeTypesDir = join(resolvedRepoPath, "nodeTypes");
  if (!existsSync(nodeTypesDir) || !statSync(nodeTypesDir).isDirectory()) {
    throw new Error(
      "Invalid repo path: missing nodeTypes/ subdirectory. " +
      "Expected a Coalesce repo directory containing nodeTypes/."
    );
  }

  return resolvedRepoPath;
}

function buildOuterDefinition(parsed: Record<string, unknown>): RepoOuterDefinition {
  return {
    fileVersion: Object.prototype.hasOwnProperty.call(parsed, "fileVersion")
      ? parsed.fileVersion
      : null,
    id: getScalarString(parsed.id),
    isDisabled: typeof parsed.isDisabled === "boolean" ? parsed.isDisabled : null,
    name: getScalarString(parsed.name),
    type: getScalarString(parsed.type),
    inputMode: getScalarString(parsed.inputMode),
  };
}

function loadRepoNodeTypes(
  resolvedRepoPath: string,
  warnings: string[]
): RepoNodeTypeRecord[] {
  const nodeTypesDir = join(resolvedRepoPath, "nodeTypes");
  return readdirSync(nodeTypesDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort(compareStrings)
    .flatMap((dirName) => {
      const dirPath = join(nodeTypesDir, dirName);
      const definitionPath = join(dirPath, "definition.yml");
      const createPath = join(dirPath, "create.sql.j2");
      const runPath = join(dirPath, "run.sql.j2");

      if (!existsSync(definitionPath)) {
        warnings.push(
          `Skipping node type directory without definition.yml: ${dirPath}`
        );
        return [];
      }

      try {
        const parsed = readYamlFile(definitionPath);
        if (!isPlainObject(parsed)) {
          warnings.push(
            `Skipping node type definition that did not parse to an object: ${definitionPath}`
          );
          return [];
        }

        const metadata = isPlainObject(parsed.metadata) ? parsed.metadata : undefined;
        const nodeMetadataSpec =
          metadata && typeof metadata.nodeMetadataSpec === "string"
            ? metadata.nodeMetadataSpec
            : null;

        let nodeDefinition: Record<string, unknown> | null = null;
        let parseError: string | null = null;
        const recordWarnings: string[] = [];

        if (!nodeMetadataSpec) {
          recordWarnings.push(
            `Missing metadata.nodeMetadataSpec in ${definitionPath}`
          );
        } else {
          try {
            const parsedNodeDefinition = YAML.parse(nodeMetadataSpec);
            if (!isPlainObject(parsedNodeDefinition)) {
              throw new Error("Parsed nodeMetadataSpec was not an object");
            }
            nodeDefinition = parsedNodeDefinition;
          } catch (error) {
            parseError = error instanceof Error ? error.message : String(error);
            recordWarnings.push(
              `Unable to parse metadata.nodeMetadataSpec in ${definitionPath}: ${parseError}`
            );
          }
        }

        if (!existsSync(createPath)) {
          recordWarnings.push(`Missing create.sql.j2 for ${definitionPath}`);
        }
        if (!existsSync(runPath)) {
          recordWarnings.push(`Missing run.sql.j2 for ${definitionPath}`);
        }

        return [
          {
            dirName,
            dirPath,
            definitionPath,
            createPath: existsSync(createPath) ? createPath : null,
            runPath: existsSync(runPath) ? runPath : null,
            outerDefinition: buildOuterDefinition(parsed),
            nodeMetadataSpec,
            nodeDefinition,
            parseError,
            warnings: recordWarnings,
          } satisfies RepoNodeTypeRecord,
        ];
      } catch (error) {
        warnings.push(
          `Skipping unreadable node type definition ${definitionPath}: ${error instanceof Error ? error.message : String(error)}`
        );
        return [];
      }
    });
}

function buildNodeTypesByID(
  nodeTypes: RepoNodeTypeRecord[]
): Map<string, RepoNodeTypeRecord[]> {
  const nodeTypesByID = new Map<string, RepoNodeTypeRecord[]>();
  for (const nodeType of nodeTypes) {
    const id = nodeType.outerDefinition.id;
    if (!id) {
      continue;
    }
    const existing = nodeTypesByID.get(id) ?? [];
    existing.push(nodeType);
    nodeTypesByID.set(id, existing);
  }
  return nodeTypesByID;
}

function loadUsageCounts(
  resolvedRepoPath: string,
  warnings: string[]
): { nodeCount: number; usageCounts: Record<string, number> } {
  const nodesDir = join(resolvedRepoPath, "nodes");
  if (!existsSync(nodesDir) || !statSync(nodesDir).isDirectory()) {
    warnings.push(
      `Repo ${resolvedRepoPath} is missing nodes/; usage counts are unavailable.`
    );
    return { nodeCount: 0, usageCounts: {} };
  }

  const usageCounts: Record<string, number> = {};
  let nodeCount = 0;

  for (const filePath of listYamlFiles(nodesDir)) {
    try {
      const parsed = readYamlFile(filePath);
      if (!isPlainObject(parsed)) {
        warnings.push(
          `Skipping node file that did not parse to an object: ${filePath}`
        );
        continue;
      }

      nodeCount += 1;
      const operation = isPlainObject(parsed.operation) ? parsed.operation : undefined;
      const sqlType = getScalarString(operation?.sqlType);
      if (!sqlType) {
        continue;
      }

      usageCounts[sqlType] = (usageCounts[sqlType] ?? 0) + 1;
    } catch (error) {
      warnings.push(
        `Skipping unreadable node file ${filePath}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  return { nodeCount, usageCounts };
}

function loadPackages(
  resolvedRepoPath: string,
  nodeTypesByID: Map<string, RepoNodeTypeRecord[]>,
  usageCounts: Record<string, number>,
  warnings: string[]
): RepoPackageRecord[] {
  const packagesDir = join(resolvedRepoPath, "packages");
  if (!existsSync(packagesDir) || !statSync(packagesDir).isDirectory()) {
    warnings.push(
      `Repo ${resolvedRepoPath} is missing packages/; package-backed discovery is unavailable.`
    );
    return [];
  }

  return listYamlFiles(packagesDir).flatMap((packageFilePath) => {
    try {
      const parsed = readYamlFile(packageFilePath);
      if (!isPlainObject(parsed)) {
        warnings.push(
          `Skipping package file that did not parse to an object: ${packageFilePath}`
        );
        return [];
      }

      const parsedAlias = getScalarString(parsed.name);
      const aliasSource = parsedAlias ? "name" : "filename";
      const alias = parsedAlias ?? basename(packageFilePath, ".yml");
      const config = isPlainObject(parsed.config) ? parsed.config : undefined;
      const entities = isPlainObject(config?.entities) ? config.entities : undefined;
      const nodeTypes = isPlainObject(entities?.nodeTypes) ? entities.nodeTypes : {};

      const enabledNodeTypeIDs = Object.entries(nodeTypes)
        .filter(([, value]) => !(isPlainObject(value) && value.isDisabled === true))
        .map(([id]) => id)
        .sort(compareStrings);

      const resolvedDefinitionIDs: string[] = [];
      const missingDefinitionIDs: string[] = [];
      const ambiguousDefinitionIDs: string[] = [];
      const usageByNodeTypeID: Record<string, number> = {};

      for (const id of enabledNodeTypeIDs) {
        const matches = nodeTypesByID.get(id) ?? [];
        if (matches.length === 1) {
          resolvedDefinitionIDs.push(id);
        } else if (matches.length === 0) {
          missingDefinitionIDs.push(id);
        } else {
          ambiguousDefinitionIDs.push(id);
        }
        usageByNodeTypeID[id] = usageCounts[`${alias}:::${id}`] ?? 0;
      }

      const recordWarnings: string[] = [];
      if (!parsedAlias) {
        recordWarnings.push(
          `Package ${packageFilePath} is missing name; falling back to filename alias ${alias}.`
        );
      }
      if (missingDefinitionIDs.length > 0) {
        recordWarnings.push(
          `Package alias ${alias} enables node type IDs without committed definitions: ${missingDefinitionIDs.join(", ")}.`
        );
      }
      if (ambiguousDefinitionIDs.length > 0) {
        recordWarnings.push(
          `Package alias ${alias} enables node type IDs with ambiguous committed definitions: ${ambiguousDefinitionIDs.join(", ")}.`
        );
      }

      return [
        {
          alias,
          aliasSource,
          packageFilePath,
          packageID: getScalarString(parsed.packageID),
          releaseID: getScalarString(parsed.releaseID),
          packageVariables: getScalarString(config?.packageVariables),
          enabledNodeTypeIDs,
          resolvedDefinitionIDs,
          missingDefinitionIDs,
          ambiguousDefinitionIDs,
          usageByNodeTypeID,
          usageCount: Object.values(usageByNodeTypeID).reduce(
            (sum, value) => sum + value,
            0
          ),
          warnings: recordWarnings,
        } satisfies RepoPackageRecord,
      ];
    } catch (error) {
      warnings.push(
        `Skipping unreadable package file ${packageFilePath}: ${error instanceof Error ? error.message : String(error)}`
      );
      return [];
    }
  });
}

function buildPackagesByAlias(
  packages: RepoPackageRecord[]
): Map<string, RepoPackageRecord[]> {
  const packagesByAlias = new Map<string, RepoPackageRecord[]>();
  for (const record of packages) {
    const existing = packagesByAlias.get(record.alias) ?? [];
    existing.push(record);
    packagesByAlias.set(record.alias, existing);
  }
  return packagesByAlias;
}

function collectAmbiguityWarnings(
  packagesByAlias: Map<string, RepoPackageRecord[]>,
  nodeTypesByID: Map<string, RepoNodeTypeRecord[]>
): string[] {
  const warnings: string[] = [];

  for (const [alias, matches] of packagesByAlias.entries()) {
    if (matches.length < 2) {
      continue;
    }
    warnings.push(
      `Multiple package manifests share alias ${alias}: ${matches
        .map((match) => match.packageFilePath)
        .join(", ")}.`
    );
  }

  for (const [id, matches] of nodeTypesByID.entries()) {
    if (matches.length < 2) {
      continue;
    }
    warnings.push(
      `Multiple committed nodeTypes share id ${id}: ${matches
        .map((match) => match.definitionPath)
        .join(", ")}.`
    );
  }

  return warnings.sort(compareStrings);
}

export function parseRepo(repoPath: string): ParsedRepo {
  const resolvedRepoPath = normalizeRepoPath(repoPath);
  const warnings: string[] = [];
  const nodeTypes = loadRepoNodeTypes(resolvedRepoPath, warnings);
  const nodeTypesByID = buildNodeTypesByID(nodeTypes);
  const { nodeCount, usageCounts } = loadUsageCounts(resolvedRepoPath, warnings);
  const packages = loadPackages(
    resolvedRepoPath,
    nodeTypesByID,
    usageCounts,
    warnings
  );
  const packagesByAlias = buildPackagesByAlias(packages);
  warnings.push(...collectAmbiguityWarnings(packagesByAlias, nodeTypesByID));

  return {
    summary: {
      repoPath,
      resolvedRepoPath,
      packageCount: packages.length,
      uniquePackageAliasCount: packagesByAlias.size,
      nodeTypeDefinitionCount: nodeTypes.length,
      uniqueNodeTypeIDCount: nodeTypesByID.size,
      nodeCount,
      warnings: Array.from(new Set(warnings)),
    },
    packages,
    nodeTypes,
    usageCounts,
    packagesByAlias,
    nodeTypesByID,
  };
}

function buildRepoResolutionError(
  parsedRepo: ParsedRepo,
  requestedNodeType: string,
  detail: string
): Error {
  return new Error(
    `Repo-backed definition could not be resolved for ${requestedNodeType} in ${parsedRepo.summary.resolvedRepoPath}. ${detail} Use the corpus tools (search_node_type_variants, get_node_type_variant, or generate_set_workspace_node_template_from_variant) as the next step.`
  );
}

export function resolveRepoNodeType(
  parsedRepo: ParsedRepo,
  requestedNodeType: string
): RepoNodeTypeResolution {
  const delimiterIndex = requestedNodeType.indexOf(":::");

  if (delimiterIndex !== -1) {
    const packageAlias = requestedNodeType.slice(0, delimiterIndex);
    const definitionID = requestedNodeType.slice(delimiterIndex + 3);
    if (!packageAlias || !definitionID) {
      throw buildRepoResolutionError(
        parsedRepo,
        requestedNodeType,
        "Package-backed identifiers must use the exact alias:::id format."
      );
    }

    const packageMatches = parsedRepo.packagesByAlias.get(packageAlias) ?? [];
    if (packageMatches.length === 0) {
      throw buildRepoResolutionError(
        parsedRepo,
        requestedNodeType,
        `No committed package alias ${packageAlias} was found under packages/.`
      );
    }
    if (packageMatches.length > 1) {
      throw buildRepoResolutionError(
        parsedRepo,
        requestedNodeType,
        `Package alias ${packageAlias} is ambiguous across ${packageMatches
          .map((match) => match.packageFilePath)
          .join(", ")}.`
      );
    }

    const packageRecord = packageMatches[0];
    if (!packageRecord.enabledNodeTypeIDs.includes(definitionID)) {
      throw buildRepoResolutionError(
        parsedRepo,
        requestedNodeType,
        `Package alias ${packageAlias} does not enable node type ID ${definitionID}.`
      );
    }

    const nodeTypeMatches = parsedRepo.nodeTypesByID.get(definitionID) ?? [];
    if (nodeTypeMatches.length === 0) {
      throw buildRepoResolutionError(
        parsedRepo,
        requestedNodeType,
        `Package alias ${packageAlias} enables ID ${definitionID}, but no committed nodeTypes definition with outer id ${definitionID} was found.`
      );
    }
    if (nodeTypeMatches.length > 1) {
      throw buildRepoResolutionError(
        parsedRepo,
        requestedNodeType,
        `Package alias ${packageAlias} maps to multiple committed definitions for outer id ${definitionID}: ${nodeTypeMatches
          .map((match) => match.definitionPath)
          .join(", ")}.`
      );
    }

    return {
      resolutionKind: "package",
      requestedNodeType,
      resolvedNodeType: `${packageAlias}:::${definitionID}`,
      packageAlias,
      packageRecord,
      usageCount: parsedRepo.usageCounts[`${packageAlias}:::${definitionID}`] ?? 0,
      nodeTypeRecord: nodeTypeMatches[0],
    };
  }

  const nodeTypeMatches = parsedRepo.nodeTypesByID.get(requestedNodeType) ?? [];
  if (nodeTypeMatches.length === 0) {
    throw buildRepoResolutionError(
      parsedRepo,
      requestedNodeType,
      `No committed nodeTypes definition with outer id ${requestedNodeType} was found.`
    );
  }
  if (nodeTypeMatches.length > 1) {
    throw buildRepoResolutionError(
      parsedRepo,
      requestedNodeType,
      `Multiple committed nodeTypes definitions share outer id ${requestedNodeType}: ${nodeTypeMatches
        .map((match) => match.definitionPath)
        .join(", ")}.`
    );
  }

  return {
    resolutionKind: "direct",
    requestedNodeType,
    resolvedNodeType: requestedNodeType,
    usageCount: parsedRepo.usageCounts[requestedNodeType] ?? 0,
    nodeTypeRecord: nodeTypeMatches[0],
  };
}
