/**
 * Workspace analysis functions for detecting patterns in Coalesce workspace nodes.
 * All functions are pure (no I/O) and operate on node data arrays.
 */

export interface NodeSummary {
  nodeType: string;
  name: string;
  predecessors?: string[];
}

export interface PackageDetectionResult {
  packages: string[];
  packageAdoption: Record<string, boolean>;
  builtInTypes: string[];
}

/**
 * Detect package usage patterns in a workspace by scanning observed node type prefixes.
 * Presence of any node with a package prefix is strong evidence the package is installed and in use,
 * but absence is not a reliable negative signal because this is not an installed-type registry.
 */
export function detectPackages(nodes: NodeSummary[]): PackageDetectionResult {
  const packageSet = new Set<string>();
  const builtInSet = new Set<string>();

  for (const node of nodes) {
    const separatorIndex = node.nodeType.indexOf(":::");
    if (separatorIndex > 0) {
      packageSet.add(node.nodeType.substring(0, separatorIndex));
    } else {
      builtInSet.add(node.nodeType);
    }
  }

  const packages = Array.from(packageSet).sort();
  const packageAdoption: Record<string, boolean> = {};
  for (const pkg of packages) {
    packageAdoption[pkg] = true;
  }

  return {
    packages,
    packageAdoption,
    builtInTypes: Array.from(builtInSet).sort(),
  };
}

export type NodeLayer = "bronze" | "staging" | "intermediate" | "mart" | "unknown";

export interface LayerSummary {
  nodeTypes: string[];
  count: number;
}

export interface LayerAnalysis {
  bronze: LayerSummary;
  staging: LayerSummary;
  intermediate: LayerSummary;
  mart: LayerSummary;
  unknown: LayerSummary;
}

const LAYER_NAME_PATTERNS: [RegExp, NodeLayer][] = [
  [/^(RAW_|SRC_|LANDING_|L0_)/i, "bronze"],
  [/^(STG_|STAGE_|CLEAN_|L1_)/i, "staging"],
  [/^(INT_|TMP_|WORK_|TRANSFORM_)/i, "intermediate"],
  [/^(DIM_|DIMENSION_|FACT_|FCT_|MART_|RPT_)/i, "mart"],
];

const MART_NODE_TYPES = new Set(["Dimension", "Fact"]);

export function inferNodeLayer(node: NodeSummary): NodeLayer {
  const upperName = node.name.toUpperCase();

  for (const [pattern, layer] of LAYER_NAME_PATTERNS) {
    if (pattern.test(upperName)) {
      return layer;
    }
  }

  const baseType = node.nodeType.includes(":::")
    ? node.nodeType.split(":::")[1]
    : node.nodeType;
  if (MART_NODE_TYPES.has(baseType)) {
    return "mart";
  }

  return "unknown";
}

export function inferLayers(nodes: NodeSummary[]): LayerAnalysis {
  const layers: Record<NodeLayer, { types: Set<string>; count: number }> = {
    bronze: { types: new Set(), count: 0 },
    staging: { types: new Set(), count: 0 },
    intermediate: { types: new Set(), count: 0 },
    mart: { types: new Set(), count: 0 },
    unknown: { types: new Set(), count: 0 },
  };

  for (const node of nodes) {
    const layer = inferNodeLayer(node);
    layers[layer].types.add(node.nodeType);
    layers[layer].count += 1;
  }

  const toSummary = (entry: { types: Set<string>; count: number }): LayerSummary => ({
    nodeTypes: Array.from(entry.types).sort(),
    count: entry.count,
  });

  return {
    bronze: toSummary(layers.bronze),
    staging: toSummary(layers.staging),
    intermediate: toSummary(layers.intermediate),
    mart: toSummary(layers.mart),
    unknown: toSummary(layers.unknown),
  };
}

export type Methodology = "kimball" | "data-vault" | "dbt-style" | "mixed";

export function detectMethodology(nodes: NodeSummary[]): Methodology {
  if (nodes.length === 0) {
    return "mixed";
  }

  const upperNames = nodes.map((n) => n.name.toUpperCase());

  // Data Vault signals: HUB_, SAT_, LINK_ naming
  const hubCount = upperNames.filter((n) => /^HUB_|_HUB$/.test(n)).length;
  const satCount = upperNames.filter((n) => /^SAT_|_SAT$/.test(n)).length;
  const linkCount = upperNames.filter((n) => /^LINK_|_LINK$/.test(n)).length;
  if (hubCount >= 1 && satCount >= 1) {
    return "data-vault";
  }

  // Kimball signals: DIM_/FACT_ naming or Dimension/Fact node types
  const dimCount = nodes.filter(
    (n) =>
      /^DIM_|^DIMENSION_/i.test(n.name) ||
      n.nodeType === "Dimension" ||
      n.nodeType.endsWith(":::Dimension")
  ).length;
  const factCount = nodes.filter(
    (n) =>
      /^FACT_|^FCT_/i.test(n.name) ||
      n.nodeType === "Fact" ||
      n.nodeType.endsWith(":::Fact")
  ).length;
  if (dimCount >= 1 && factCount >= 1) {
    return "kimball";
  }

  // dbt-style signals: stg_/int_/fct_ lowercase naming with view intermediates
  const stgCount = nodes.filter((n) => /^stg_/i.test(n.name)).length;
  const intCount = nodes.filter((n) => /^int_/i.test(n.name)).length;
  if (stgCount >= 1 && intCount >= 1) {
    return "dbt-style";
  }

  return "mixed";
}

export interface WorkspaceProfile {
  workspaceID: string;
  analyzedAt: string;
  nodeCount: number;
  packageAdoption: PackageDetectionResult;
  layerPatterns: LayerAnalysis;
  methodology: Methodology;
  recommendations: {
    defaultPackage: string | null;
    stagingType: string;
    transformType: string;
    dimensionType: string;
    factType: string;
  };
}

/**
 * Build a complete workspace profile from a list of nodes.
 * This is the main entry point for workspace analysis.
 */
export function buildWorkspaceProfile(
  workspaceID: string,
  nodes: NodeSummary[]
): WorkspaceProfile {
  const packageAdoption = detectPackages(nodes);
  const layerPatterns = inferLayers(nodes);
  const methodology = detectMethodology(nodes);

  const preferredPackage = packageAdoption.packages.includes("base-nodes")
    ? "base-nodes"
    : packageAdoption.packages[0] ?? null;

  const prefix = preferredPackage ? `${preferredPackage}:::` : "";

  const findDominantType = (layer: LayerSummary, fallback: string): string => {
    if (layer.nodeTypes.length === 0) {
      return prefix ? `${prefix}${fallback}` : fallback;
    }
    // Prefer the packaged version matching fallback, then any packaged, then matching built-in, then first
    const packagedMatch = layer.nodeTypes.find(
      (t) => t.includes(":::") && t.endsWith(`:::${fallback}`)
    );
    if (packagedMatch) return packagedMatch;
    const builtInMatch = layer.nodeTypes.find((t) => t === fallback);
    if (builtInMatch) return builtInMatch;
    const packaged = layer.nodeTypes.find((t) => t.includes(":::"));
    return packaged ?? layer.nodeTypes[0];
  };

  return {
    workspaceID,
    analyzedAt: new Date().toISOString(),
    nodeCount: nodes.length,
    packageAdoption,
    layerPatterns,
    methodology,
    recommendations: {
      defaultPackage: preferredPackage,
      stagingType: findDominantType(layerPatterns.staging, "Stage"),
      transformType: findDominantType(layerPatterns.intermediate, "View"),
      dimensionType: findDominantType(layerPatterns.mart, "Dimension"),
      factType: findDominantType(layerPatterns.mart, "Fact"),
    },
  };
}
