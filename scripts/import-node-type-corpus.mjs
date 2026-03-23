import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { join, relative } from "node:path";
import YAML from "yaml";

const DEFAULT_SOURCE_ROOT =
  "/Users/jmarshall/Documents/GitHub/client_services_toolkit/node_investigations/node_source_code";
const OUTPUT_PATH = join(
  process.cwd(),
  "generated",
  "node-type-corpus.json"
);

const SUPPORTED_PRIMITIVES = new Set([
  "dropdownSelector",
  "label",
  "Label",
  "materializationSelector",
  "multisourceToggle",
  "overrideSQLToggle",
  "textBox",
  "toggleButton",
]);

function isPlainObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readUtf8(filePath) {
  return readFileSync(filePath, "utf8");
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function normalizeFamily(nodeTypeDirName) {
  return nodeTypeDirName.replace(/-\d+$/u, "");
}

function getString(value) {
  return typeof value === "string" ? value : null;
}

function buildControlSignature(item) {
  const itemType = getString(item.type) ?? "unknown";
  if (itemType === "tabular" && Array.isArray(item.columns)) {
    const nested = item.columns
      .filter(isPlainObject)
      .map((column) => getString(column.type) ?? "unknown");
    return `${itemType}[${nested.join(",")}]`;
  }
  return itemType;
}

function collectConfigMetadata(nodeDefinition) {
  const configGroups = Array.isArray(nodeDefinition.config)
    ? nodeDefinition.config.filter(isPlainObject)
    : [];
  const primitiveSet = new Set();
  const controlSignature = [];
  let configItemCount = 0;

  for (const group of configGroups) {
    const items = Array.isArray(group.items)
      ? group.items.filter(isPlainObject)
      : [];
    for (const item of items) {
      configItemCount += 1;
      const itemType = getString(item.type);
      if (itemType) {
        primitiveSet.add(itemType);
      }
      controlSignature.push(buildControlSignature(item));

      if (!Array.isArray(item.columns)) {
        continue;
      }

      for (const column of item.columns.filter(isPlainObject)) {
        const columnType = getString(column.type);
        if (columnType) {
          primitiveSet.add(columnType);
        }
      }
    }
  }

  return {
    configGroupCount: configGroups.length,
    configItemCount,
    primitiveSignature: Array.from(primitiveSet).sort(),
    controlSignature,
  };
}

function listPackageDirs(sourceRoot) {
  return readdirSync(sourceRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => join(sourceRoot, entry.name))
    .filter((dirPath) => existsSync(join(dirPath, "nodeTypes")))
    .sort();
}

function buildVariantRecord(sourceRoot, packageDir, nodeTypeDirName) {
  const nodeTypeDir = join(packageDir, "nodeTypes", nodeTypeDirName);
  const definitionPath = join(nodeTypeDir, "definition.yml");
  const createPath = join(nodeTypeDir, "create.sql.j2");
  const runPath = join(nodeTypeDir, "run.sql.j2");

  if (
    !existsSync(definitionPath) ||
    !existsSync(createPath) ||
    !existsSync(runPath)
  ) {
    return null;
  }

  const definitionText = readUtf8(definitionPath);
  const createText = readUtf8(createPath);
  const runText = readUtf8(runPath);
  const definitionOuter = YAML.parse(definitionText);
  const nodeMetadataSpec =
    definitionOuter &&
    isPlainObject(definitionOuter.metadata) &&
    typeof definitionOuter.metadata.nodeMetadataSpec === "string"
      ? definitionOuter.metadata.nodeMetadataSpec
      : null;

  if (!nodeMetadataSpec) {
    throw new Error(`Missing metadata.nodeMetadataSpec in ${definitionPath}`);
  }

  const normalizedFamily = normalizeFamily(nodeTypeDirName);
  const definitionHash = sha256(definitionText);
  const createHash = sha256(createText);
  const runHash = sha256(runText);
  const variantKey = [
    normalizedFamily,
    definitionHash,
    createHash,
    runHash,
  ].join("|");
  let nodeDefinition = null;
  let parseError = null;
  let configMetadata = {
    configGroupCount: 0,
    configItemCount: 0,
    primitiveSignature: [],
    controlSignature: [],
  };

  try {
    const parsed = YAML.parse(nodeMetadataSpec);
    if (!isPlainObject(parsed)) {
      throw new Error("Parsed nodeMetadataSpec was not an object");
    }
    nodeDefinition = parsed;
    configMetadata = collectConfigMetadata(nodeDefinition);
  } catch (error) {
    parseError = error instanceof Error ? error.message : String(error);
  }

  const unsupportedPrimitives =
    nodeDefinition === null
      ? []
      : configMetadata.primitiveSignature.filter(
          (primitive) => !SUPPORTED_PRIMITIVES.has(primitive)
        );
  const supportStatus =
    nodeDefinition === null
      ? "parse_error"
      : unsupportedPrimitives.length === 0
        ? "supported"
        : "partial";

  return {
    variantKey,
    normalizedFamily,
    definitionHash,
    createHash,
    runHash,
    packageName: relative(sourceRoot, packageDir),
    nodeTypeDirName,
    nodeTypeDirPath: relative(sourceRoot, nodeTypeDir),
    definitionPath: relative(sourceRoot, definitionPath),
    createPath: relative(sourceRoot, createPath),
    runPath: relative(sourceRoot, runPath),
    nodeMetadataSpec,
    nodeDefinition,
    parseError,
    outerDefinition: {
      fileVersion:
        definitionOuter &&
        Object.prototype.hasOwnProperty.call(definitionOuter, "fileVersion")
          ? definitionOuter.fileVersion
          : null,
      id: getString(definitionOuter?.id),
      isDisabled:
        definitionOuter && typeof definitionOuter.isDisabled === "boolean"
          ? definitionOuter.isDisabled
          : null,
      name: getString(definitionOuter?.name),
      type: getString(definitionOuter?.type),
    },
    definitionSummary: {
      capitalized: getString(nodeDefinition?.capitalized),
      short: getString(nodeDefinition?.short),
      plural: getString(nodeDefinition?.plural),
      tagColor: getString(nodeDefinition?.tagColor),
      deployStrategy: getString(nodeDefinition?.deployStrategy),
      configGroupCount: configMetadata.configGroupCount,
      configItemCount: configMetadata.configItemCount,
    },
    primitiveSignature: configMetadata.primitiveSignature,
    controlSignature: configMetadata.controlSignature,
    unsupportedPrimitives,
    supportStatus,
    parseError,
  };
}

function buildSnapshot(sourceRoot) {
  const packageDirs = listPackageDirs(sourceRoot);
  const variantMap = new Map();
  let definitionCount = 0;

  for (const packageDir of packageDirs) {
    const nodeTypesDir = join(packageDir, "nodeTypes");
    const nodeTypeDirNames = readdirSync(nodeTypesDir, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)
      .sort();

    for (const nodeTypeDirName of nodeTypeDirNames) {
      const record = buildVariantRecord(sourceRoot, packageDir, nodeTypeDirName);
      if (!record) {
        continue;
      }
      definitionCount += 1;

      const existing = variantMap.get(record.variantKey);
      if (existing) {
        existing.packageNames = Array.from(
          new Set([...existing.packageNames, record.packageName])
        ).sort();
        existing.occurrenceCount += 1;
        existing.occurrences.push({
          packageName: record.packageName,
          nodeTypeDirName: record.nodeTypeDirName,
          nodeTypeDirPath: record.nodeTypeDirPath,
          definitionPath: record.definitionPath,
          createPath: record.createPath,
          runPath: record.runPath,
        });
        if (existing.parseError === null && record.parseError !== null) {
          existing.parseError = record.parseError;
        }
        continue;
      }

      variantMap.set(record.variantKey, {
        variantKey: record.variantKey,
        normalizedFamily: record.normalizedFamily,
        packageNames: [record.packageName],
        occurrenceCount: 1,
        occurrences: [
          {
            packageName: record.packageName,
            nodeTypeDirName: record.nodeTypeDirName,
            nodeTypeDirPath: record.nodeTypeDirPath,
            definitionPath: record.definitionPath,
            createPath: record.createPath,
            runPath: record.runPath,
          },
        ],
        definitionHash: record.definitionHash,
        createHash: record.createHash,
        runHash: record.runHash,
        primitiveSignature: record.primitiveSignature,
        controlSignature: record.controlSignature,
        unsupportedPrimitives: record.unsupportedPrimitives,
        supportStatus: record.supportStatus,
        definitionSummary: record.definitionSummary,
        outerDefinition: record.outerDefinition,
        nodeMetadataSpec: record.nodeMetadataSpec,
        nodeDefinition: record.nodeDefinition,
        parseError: record.parseError,
      });
    }
  }

  const variants = Array.from(variantMap.values()).sort((left, right) =>
    left.variantKey.localeCompare(right.variantKey)
  );

  return {
    generatedAt: new Date().toISOString(),
    sourceRoot,
    packageCount: packageDirs.length,
    definitionCount,
    uniqueVariantCount: variants.length,
    uniqueNormalizedFamilyCount: new Set(
      variants.map((variant) => variant.normalizedFamily)
    ).size,
    supportedVariantCount: variants.filter(
      (variant) => variant.supportStatus === "supported"
    ).length,
    partialVariantCount: variants.filter(
      (variant) => variant.supportStatus === "partial"
    ).length,
    parseErrorVariantCount: variants.filter(
      (variant) => variant.supportStatus === "parse_error"
    ).length,
    variants,
  };
}

function main() {
  const sourceRoot =
    process.argv[2] ??
    process.env.NODE_TYPE_CORPUS_SOURCE_ROOT ??
    DEFAULT_SOURCE_ROOT;

  if (!existsSync(sourceRoot)) {
    throw new Error(
      `Node type corpus source root not found: ${sourceRoot}\nPass a path explicitly or set NODE_TYPE_CORPUS_SOURCE_ROOT.`
    );
  }

  const snapshot = buildSnapshot(sourceRoot);
  mkdirSync(join(process.cwd(), "generated"), { recursive: true });
  writeFileSync(OUTPUT_PATH, `${JSON.stringify(snapshot, null, 2)}\n`, "utf8");

  console.log(
    JSON.stringify(
      {
        outputPath: OUTPUT_PATH,
        packageCount: snapshot.packageCount,
        definitionCount: snapshot.definitionCount,
        uniqueVariantCount: snapshot.uniqueVariantCount,
        uniqueNormalizedFamilyCount: snapshot.uniqueNormalizedFamilyCount,
        supportedVariantCount: snapshot.supportedVariantCount,
        partialVariantCount: snapshot.partialVariantCount,
        parseErrorVariantCount: snapshot.parseErrorVariantCount,
      },
      null,
      2
    )
  );
}

main();
