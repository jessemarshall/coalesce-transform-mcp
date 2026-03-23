# Data Engineering Principles Resource Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a comprehensive AI resource file for data engineering principles and a new `analyze-workspace-patterns` tool that generates cached workspace profiles.

**Architecture:** Two deliverables — (1) a markdown resource file registered as an MCP resource at `coalesce://context/data-engineering-principles`, and (2) a new tool in `src/tools/nodes.ts` that analyzes workspace node data, infers package adoption / layer patterns / methodology, and writes a JSON profile to the `data/` folder. The resource guides AI decision-making; the tool generates the data the AI references.

**Tech Stack:** TypeScript, Zod, MCP SDK (`@modelcontextprotocol/sdk`), Vitest, Node `fs` (for file I/O in the analysis tool)

---

## File Structure

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `src/resources/context/data-engineering-principles.md` | AI resource: DE principles, layer patterns, methodology recognition, materialization strategies, dependency management, package recommendations |
| Delete | `src/resources/context/node-selection.md` | Superseded by data-engineering-principles.md — contains outdated 80% threshold guidance |
| Modify | `src/resources/index.ts` | Remove node-selection, add data-engineering-principles (net resource count stays at 5) |
| Modify | `tests/resources.test.ts` | Replace node-selection with data-engineering-principles in expected list (count stays 5) |
| Create | `src/tools/workspace-analysis.ts` | Pure analysis functions: `detectPackages`, `inferLayers`, `detectMethodology`, `buildWorkspaceProfile` |
| Modify | `src/tools/nodes.ts` | Import analysis module, register `analyze-workspace-patterns` tool, replace node-selection resource refs with data-engineering-principles |
| Modify | `tests/registration.test.ts` | Update tool count from 40→41, add `analyze-workspace-patterns` to expected names |
| Create | `tests/tools/workspace-analysis.test.ts` | Unit tests for all analysis functions |
| Modify | `tests/tools/nodes.test.ts` | Add registration test for the new tool |
| Modify | `README.md` | Replace node-selection with data-engineering-principles in resources table, add analyze-workspace-patterns tool, update node count to 10 |

---

### Task 1: Create the Data Engineering Principles Resource File

**Files:**
- Create: `src/resources/context/data-engineering-principles.md`

This is the core deliverable — the markdown content that teaches the AI how to make data engineering decisions. All content comes from the approved design spec at `docs/superpowers/specs/2026-03-20-data-engineering-principles-design.md`.

- [ ] **Step 1: Create the resource file**

Write the complete markdown file at `src/resources/context/data-engineering-principles.md`. Translate each "Section" from the design spec into the corresponding markdown section. The file structure is:

```markdown
# Data Engineering Principles for Coalesce

## How to Use This Guide
- When to consult (before creating workspace nodes, when user mentions DE concepts)
- Application pattern (ALWAYS check workspace profile first, cross-reference best practices)
- Recommendation philosophy (informative not prescriptive, show current AND better)
- Confidence levels (high/medium/low)
- User-specified types (trust user, skip analysis)

## Workspace Pattern Analysis

### Data Caching Strategy
- All API responses save to data/ folder
- Workflow: list-workspace-nodes → save → analyze-workspace-patterns → profile
- AI checks data/ folder first, only calls API if cache missing/stale

### Level 1: Package Detection
- Scan node types for package prefixes (base-nodes:::*, etc.)
- Presence indicates availability, don't use percentages
- Package categories: base-nodes, specialized methodology, platform-specific, data quality, built-in

### Level 2: DAG Topology Analysis
- Bronze/Landing: 0 predecessors, RAW_*/SRC_*/LANDING_*
- Silver/Staging: 1-2 predecessors, STG_*/STAGE_*
- Intermediate/Transform: Mid-pipeline, INT_*/WORK_*
- Gold/Mart: Multiple predecessors, few downstream, DIM_*/FACT_*

### Level 3: Lineage-Based Methodology Detection
- Kimball: DIM/FACT separation, star/snowflake topology
- Data Vault: Hub/Satellite/Link structural patterns
- dbt-Style: stg_ → int_ → fct_/dim_ naming, heavy view usage

### Decision Framework
- Check cached profile → detect packages → analyze topology → check methodology → recommend

## Layered Architecture Patterns
- Bronze/Landing: Stage nodes, table materialization
- Silver/Staging: Stage nodes, table or incremental
- Intermediate/Transform: View nodes, view materialization
- Gold/Mart: Dimension/Fact nodes, tables for dims, incremental for facts
- Layer Flow Validation: warn on cross-layer skips

## Methodology-Specific Guidance
- Kimball Dimensional Modeling (node type mapping, SCD Type 2 detection)
- Data Vault 2.0 (hub/satellite/link structural patterns, package pointers)
- dbt-Style Staging (naming convention, view-heavy intermediates)
- Mixed/Unclear: ask user, default to simple staging → mart

## Materialization Strategies
- Table (Full Refresh): when/tradeoffs/config indicators
- View: when/tradeoffs
- Incremental (Merge/Append): when/tradeoffs/config indicators
- Layer-Specific defaults

## Dependency Management
- Healthy: fan-out, fan-in (2-4), linear chains (3-5 steps)
- Problematic: excessive fan-in (>5), deep chains (>6), circular, cross-layer skips
- DAG best practices: reuse staging, focused joins, layer appropriately, parallelizable DAGs

## Package Recommendations
- Base Nodes: detection, when to recommend, soft recommendation approach
- Incremental-Nodes: detection signals, node types provided
- Semantic Node Types, Platform-Specific, Methodology packages
- Package install path: Build Settings → Packages in Coalesce UI
- Exploration guidance: github.com/coalesceio
```

Copy the full content from each design spec section into the resource file. Remove "Section N:" prefixes — use clean markdown headers. Remove design-document meta-commentary (e.g., "Purpose: Teach the AI…") — the resource IS the teaching material, so write it directly as guidance.

- [ ] **Step 2: Verify the file reads correctly**

Run: `node -e "const fs = require('fs'); const content = fs.readFileSync('src/resources/context/data-engineering-principles.md', 'utf-8'); console.log('Lines:', content.split('\\n').length); console.log('First line:', content.split('\\n')[0])"`

Expected: File reads without error, first line is `# Data Engineering Principles for Coalesce`

- [ ] **Step 3: Commit**

```bash
git add src/resources/context/data-engineering-principles.md
git commit -m "feat: add data engineering principles resource content"
```

---

### Task 2: Replace node-selection Resource with data-engineering-principles

**Files:**

- Delete: `src/resources/context/node-selection.md`
- Modify: `src/resources/index.ts`
- Modify: `tests/resources.test.ts`

- [ ] **Step 1: Write the failing test**

In `tests/resources.test.ts`, replace `node-selection` with `data-engineering-principles` in the expected list. Resource count stays at 5.

Change the first test:

```typescript
it("registers all fixed Coalesce context resources", () => {
    registerResources(server);

    expect(resourceSpy).toHaveBeenCalledTimes(5);
    const calls = resourceSpy.mock.calls.map((call) => ({
      name: call[0],
      uri: call[1],
    }));
    expect(calls).toEqual([
      {
        name: "Coalesce Overview",
        uri: "coalesce://context/overview",
      },
      {
        name: "Platform-Specific SQL Rules",
        uri: "coalesce://context/sql-platforms",
      },
      {
        name: "Storage Locations and References",
        uri: "coalesce://context/storage-mappings",
      },
      {
        name: "Tool Usage Patterns",
        uri: "coalesce://context/tool-usage",
      },
      {
        name: "Data Engineering Principles",
        uri: "coalesce://context/data-engineering-principles",
      },
    ]);
  });
```

Also update the second test that reads a resource — if it references `overview` it can stay, but verify the callback index matches the new order.

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/resources.test.ts`

Expected: FAIL — `node-selection` is still registered but no longer expected.

- [ ] **Step 3: Replace the resource in index.ts**

In `src/resources/index.ts`, remove `NODE_SELECTION` and add `DATA_ENGINEERING_PRINCIPLES`:

```typescript
const RESOURCES = {
  OVERVIEW: "coalesce://context/overview",
  SQL_PLATFORMS: "coalesce://context/sql-platforms",
  STORAGE_MAPPINGS: "coalesce://context/storage-mappings",
  TOOL_USAGE: "coalesce://context/tool-usage",
  DATA_ENGINEERING_PRINCIPLES: "coalesce://context/data-engineering-principles",
} as const;
```

Update `RESOURCE_FILES` — remove node-selection entry, add:

```typescript
[RESOURCES.DATA_ENGINEERING_PRINCIPLES]: "context/data-engineering-principles.md",
```

Update `RESOURCE_METADATA` — remove node-selection entry, add:

```typescript
[RESOURCES.DATA_ENGINEERING_PRINCIPLES]: {
    name: "Data Engineering Principles",
    description:
      "Data engineering best practices for node type selection, layered architecture, methodology detection, materialization strategies, and dependency management",
    mimeType: "text/markdown",
  },
```

- [ ] **Step 4: Delete the old node-selection.md file**

```bash
git rm src/resources/context/node-selection.md
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `npx vitest run tests/resources.test.ts`

Expected: PASS — 5 resources registered, node-selection removed, data-engineering-principles added.

- [ ] **Step 6: Commit**

```bash
git add src/resources/index.ts tests/resources.test.ts
git commit -m "feat: replace node-selection resource with data-engineering-principles"
```

---

### Task 3: Create Workspace Analysis Functions

**Files:**
- Create: `src/tools/workspace-analysis.ts`
- Create: `tests/tools/workspace-analysis.test.ts`

This module contains pure analysis functions (no I/O) that take node data arrays and return analysis results.

- [ ] **Step 1: Write the failing tests for `detectPackages`**

Create `tests/tools/workspace-analysis.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import {
  detectPackages,
  inferNodeLayer,
  inferLayers,
  detectMethodology,
  buildWorkspaceProfile,
} from "../../src/tools/workspace-analysis.js";

describe("detectPackages", () => {
  it("detects base-nodes package from node types", () => {
    const nodes = [
      { nodeType: "base-nodes:::Stage", name: "STG_ORDERS" },
      { nodeType: "base-nodes:::Dimension", name: "DIM_CUSTOMER" },
      { nodeType: "Stage", name: "STG_RAW" },
    ];
    const result = detectPackages(nodes);
    expect(result.packages).toContain("base-nodes");
    expect(result.packageAdoption["base-nodes"]).toBe(true);
  });

  it("returns empty packages for built-in only", () => {
    const nodes = [
      { nodeType: "Stage", name: "STG_ORDERS" },
      { nodeType: "View", name: "VW_CUSTOMERS" },
    ];
    const result = detectPackages(nodes);
    expect(result.packages).toEqual([]);
  });

  it("detects multiple packages", () => {
    const nodes = [
      { nodeType: "base-nodes:::Stage", name: "STG_ORDERS" },
      { nodeType: "incremental-nodes:::IncrementalLoad", name: "FCT_EVENTS" },
    ];
    const result = detectPackages(nodes);
    expect(result.packages).toContain("base-nodes");
    expect(result.packages).toContain("incremental-nodes");
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/tools/workspace-analysis.test.ts`

Expected: FAIL — module not found.

- [ ] **Step 3: Implement `detectPackages`**

Create `src/tools/workspace-analysis.ts`:

```typescript
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
 * Detect which packages are available in a workspace by scanning node type prefixes.
 * Presence of any node with a package prefix indicates that package is installed.
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/tools/workspace-analysis.test.ts`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tools/workspace-analysis.ts tests/tools/workspace-analysis.test.ts
git commit -m "feat: add detectPackages workspace analysis function"
```

---

### Task 4: Add Layer Inference Functions

**Files:**
- Modify: `src/tools/workspace-analysis.ts`
- Modify: `tests/tools/workspace-analysis.test.ts`

- [ ] **Step 1: Write failing tests for `inferNodeLayer` and `inferLayers`**

Add to `tests/tools/workspace-analysis.test.ts`:

```typescript
describe("inferNodeLayer", () => {
  it("infers bronze layer from naming", () => {
    expect(inferNodeLayer({ nodeType: "Stage", name: "RAW_ORDERS" })).toBe("bronze");
    expect(inferNodeLayer({ nodeType: "Stage", name: "SRC_CUSTOMERS" })).toBe("bronze");
    expect(inferNodeLayer({ nodeType: "Stage", name: "LANDING_PRODUCTS" })).toBe("bronze");
  });

  it("infers staging layer from naming", () => {
    expect(inferNodeLayer({ nodeType: "Stage", name: "STG_ORDERS" })).toBe("staging");
    expect(inferNodeLayer({ nodeType: "Stage", name: "STAGE_CUSTOMERS" })).toBe("staging");
  });

  it("infers intermediate layer from naming", () => {
    expect(inferNodeLayer({ nodeType: "View", name: "INT_ORDER_METRICS" })).toBe("intermediate");
    expect(inferNodeLayer({ nodeType: "View", name: "WORK_CUSTOMER_PREP" })).toBe("intermediate");
  });

  it("infers mart layer from naming and node type", () => {
    expect(inferNodeLayer({ nodeType: "Dimension", name: "DIM_CUSTOMER" })).toBe("mart");
    expect(inferNodeLayer({ nodeType: "Fact", name: "FACT_SALES" })).toBe("mart");
    expect(inferNodeLayer({ nodeType: "Stage", name: "FCT_ORDERS" })).toBe("mart");
    expect(inferNodeLayer({ nodeType: "Stage", name: "MART_REVENUE" })).toBe("mart");
  });

  it("returns unknown for ambiguous nodes", () => {
    expect(inferNodeLayer({ nodeType: "Stage", name: "CUSTOMERS" })).toBe("unknown");
  });
});

describe("inferLayers", () => {
  it("groups nodes by inferred layer", () => {
    const nodes = [
      { nodeType: "Stage", name: "RAW_ORDERS" },
      { nodeType: "Stage", name: "STG_ORDERS" },
      { nodeType: "View", name: "INT_CLEAN" },
      { nodeType: "Dimension", name: "DIM_CUSTOMER" },
      { nodeType: "Fact", name: "FACT_SALES" },
    ];
    const result = inferLayers(nodes);
    expect(result.bronze.count).toBe(1);
    expect(result.staging.count).toBe(1);
    expect(result.intermediate.count).toBe(1);
    expect(result.mart.count).toBe(2);
  });

  it("collects node types per layer", () => {
    const nodes = [
      { nodeType: "Stage", name: "STG_A" },
      { nodeType: "base-nodes:::Stage", name: "STG_B" },
    ];
    const result = inferLayers(nodes);
    expect(result.staging.nodeTypes).toContain("Stage");
    expect(result.staging.nodeTypes).toContain("base-nodes:::Stage");
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/tools/workspace-analysis.test.ts`

Expected: FAIL — `inferNodeLayer` and `inferLayers` not exported.

- [ ] **Step 3: Implement the layer inference functions**

Add to `src/tools/workspace-analysis.ts`:

```typescript
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

/**
 * Infer which pipeline layer a node belongs to based on its name and node type.
 */
export function inferNodeLayer(node: NodeSummary): NodeLayer {
  const upperName = node.name.toUpperCase();

  for (const [pattern, layer] of LAYER_NAME_PATTERNS) {
    if (pattern.test(upperName)) {
      return layer;
    }
  }

  // Check node type for mart indicators
  const baseType = node.nodeType.includes(":::")
    ? node.nodeType.split(":::")[1]
    : node.nodeType;
  if (MART_NODE_TYPES.has(baseType)) {
    return "mart";
  }

  return "unknown";
}

/**
 * Analyze all nodes and group them by inferred layer.
 */
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/tools/workspace-analysis.test.ts`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tools/workspace-analysis.ts tests/tools/workspace-analysis.test.ts
git commit -m "feat: add layer inference functions for workspace analysis"
```

---

### Task 5: Add Methodology Detection Function

**Files:**
- Modify: `src/tools/workspace-analysis.ts`
- Modify: `tests/tools/workspace-analysis.test.ts`

- [ ] **Step 1: Write failing tests for `detectMethodology`**

Add to `tests/tools/workspace-analysis.test.ts`:

```typescript
describe("detectMethodology", () => {
  it("detects kimball methodology from DIM/FACT patterns", () => {
    const nodes = [
      { nodeType: "Stage", name: "STG_ORDERS" },
      { nodeType: "Dimension", name: "DIM_CUSTOMER" },
      { nodeType: "Dimension", name: "DIM_PRODUCT" },
      { nodeType: "Fact", name: "FACT_SALES" },
      { nodeType: "Fact", name: "FACT_ORDERS" },
    ];
    expect(detectMethodology(nodes)).toBe("kimball");
  });

  it("detects data-vault methodology from hub/satellite naming", () => {
    const nodes = [
      { nodeType: "Stage", name: "STG_ORDERS" },
      { nodeType: "Stage", name: "HUB_CUSTOMER" },
      { nodeType: "Stage", name: "SAT_CUSTOMER_DETAILS" },
      { nodeType: "Stage", name: "LINK_ORDER_CUSTOMER" },
    ];
    expect(detectMethodology(nodes)).toBe("data-vault");
  });

  it("detects dbt-style methodology from stg/int/fct naming", () => {
    const nodes = [
      { nodeType: "Stage", name: "stg_orders" },
      { nodeType: "View", name: "int_orders_cleaned" },
      { nodeType: "View", name: "int_orders_enriched" },
      { nodeType: "Stage", name: "fct_orders" },
    ];
    expect(detectMethodology(nodes)).toBe("dbt-style");
  });

  it("returns mixed for ambiguous workspaces", () => {
    const nodes = [
      { nodeType: "Stage", name: "ORDERS" },
      { nodeType: "View", name: "CUSTOMERS_VIEW" },
    ];
    expect(detectMethodology(nodes)).toBe("mixed");
  });

  it("returns mixed for empty workspace", () => {
    expect(detectMethodology([])).toBe("mixed");
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/tools/workspace-analysis.test.ts`

Expected: FAIL — `detectMethodology` not exported.

- [ ] **Step 3: Implement `detectMethodology`**

Add to `src/tools/workspace-analysis.ts`:

```typescript
export type Methodology = "kimball" | "data-vault" | "dbt-style" | "mixed";

/**
 * Detect the data modeling methodology used in a workspace based on node naming patterns.
 */
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/tools/workspace-analysis.test.ts`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tools/workspace-analysis.ts tests/tools/workspace-analysis.test.ts
git commit -m "feat: add methodology detection for workspace analysis"
```

---

### Task 6: Add `buildWorkspaceProfile` Function

**Files:**
- Modify: `src/tools/workspace-analysis.ts`
- Modify: `tests/tools/workspace-analysis.test.ts`

- [ ] **Step 1: Write failing test for `buildWorkspaceProfile`**

Add to `tests/tools/workspace-analysis.test.ts`:

```typescript
describe("buildWorkspaceProfile", () => {
  it("builds a complete profile from workspace nodes", () => {
    const nodes = [
      { nodeType: "base-nodes:::Stage", name: "STG_ORDERS" },
      { nodeType: "base-nodes:::Stage", name: "STG_CUSTOMERS" },
      { nodeType: "base-nodes:::View", name: "INT_CLEAN" },
      { nodeType: "base-nodes:::Dimension", name: "DIM_CUSTOMER" },
      { nodeType: "base-nodes:::Fact", name: "FACT_SALES" },
    ];

    const profile = buildWorkspaceProfile("ws-123", nodes);

    expect(profile.workspaceID).toBe("ws-123");
    expect(profile.nodeCount).toBe(5);
    expect(profile.packageAdoption.packages).toContain("base-nodes");
    expect(profile.layerPatterns.staging.count).toBe(2);
    expect(profile.layerPatterns.intermediate.count).toBe(1);
    expect(profile.layerPatterns.mart.count).toBe(2);
    expect(profile.methodology).toBe("kimball");
    expect(profile.recommendations.defaultPackage).toBe("base-nodes");
    expect(profile.recommendations.stagingType).toBe("base-nodes:::Stage");
    expect(typeof profile.analyzedAt).toBe("string");
  });

  it("recommends built-in types when no packages detected", () => {
    const nodes = [
      { nodeType: "Stage", name: "STG_ORDERS" },
      { nodeType: "Dimension", name: "DIM_CUSTOMER" },
      { nodeType: "Fact", name: "FACT_SALES" },
    ];

    const profile = buildWorkspaceProfile("ws-456", nodes);

    expect(profile.recommendations.defaultPackage).toBeNull();
    expect(profile.recommendations.stagingType).toBe("Stage");
    expect(profile.recommendations.dimensionType).toBe("Dimension");
    expect(profile.recommendations.factType).toBe("Fact");
  });

  it("handles empty workspace", () => {
    const profile = buildWorkspaceProfile("ws-empty", []);

    expect(profile.nodeCount).toBe(0);
    expect(profile.methodology).toBe("mixed");
    expect(profile.packageAdoption.packages).toEqual([]);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/tools/workspace-analysis.test.ts`

Expected: FAIL — `buildWorkspaceProfile` not exported.

- [ ] **Step 3: Implement `buildWorkspaceProfile`**

Add to `src/tools/workspace-analysis.ts`:

```typescript
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
    // Prefer the packaged version if present, otherwise first type found
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/tools/workspace-analysis.test.ts`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tools/workspace-analysis.ts tests/tools/workspace-analysis.test.ts
git commit -m "feat: add buildWorkspaceProfile for workspace analysis"
```

---

### Task 7: Register the `analyze-workspace-patterns` Tool

**Files:**
- Modify: `src/tools/nodes.ts`
- Modify: `tests/tools/nodes.test.ts`
- Modify: `tests/registration.test.ts`

- [ ] **Step 1: Write the failing test in nodes.test.ts**

Add to `tests/tools/nodes.test.ts`:

```typescript
import { buildWorkspaceProfile } from "../../src/tools/workspace-analysis.js";

it("registers all 10 node tools without throwing", () => {
    const server = new McpServer({ name: "test", version: "0.0.1" });
    const client = createMockClient();
    registerNodeTools(server, client as any);
    // Count will be verified in registration.test.ts
    expect(true).toBe(true);
  });
```

Update the existing test description from "9 node tools" to "10 node tools" if it exists.

In `tests/registration.test.ts`, update:

```typescript
expect(toolSpy).toHaveBeenCalledTimes(41);  // was 40
```

And add to the expected tool names:

```typescript
expect(toolNames).toContain("analyze-workspace-patterns");
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/registration.test.ts`

Expected: FAIL — tool count is 40 not 41, `analyze-workspace-patterns` not registered.

- [ ] **Step 3: Register the tool in nodes.ts**

At the top of `src/tools/nodes.ts`, add the import:

```typescript
import { buildWorkspaceProfile } from "./workspace-analysis.js";
import type { NodeSummary } from "./workspace-analysis.js";
```

At the end of `registerNodeTools`, before the closing `}`, add:

```typescript
  server.tool(
    "analyze-workspace-patterns",
    "Analyze workspace node patterns to detect package adoption, pipeline layers, data modeling methodology, and generate recommendations. Results are returned as a workspace profile summary. For large workspaces, save the profile to a data/ folder file for reuse.\n\nThis tool examines existing workspace nodes to understand conventions before creating new nodes.",
    {
      workspaceID: z.string().describe("The workspace ID to analyze"),
    },
    READ_ONLY_ANNOTATIONS,
    async (params) => {
      try {
        const nodesResponse = await listWorkspaceNodes(client, {
          workspaceID: params.workspaceID,
        });

        const nodes: NodeSummary[] = [];
        if (
          nodesResponse &&
          typeof nodesResponse === "object" &&
          "data" in nodesResponse &&
          Array.isArray((nodesResponse as Record<string, unknown>).data)
        ) {
          for (const node of (nodesResponse as Record<string, unknown>)
            .data as Record<string, unknown>[]) {
            if (
              typeof node.nodeType === "string" &&
              typeof node.name === "string"
            ) {
              nodes.push({
                nodeType: node.nodeType,
                name: node.name,
              });
            }
          }
        }

        const profile = buildWorkspaceProfile(params.workspaceID, nodes);

        return {
          content: [
            { type: "text", text: JSON.stringify(profile, null, 2) },
          ],
        };
      } catch (error) {
        return handleToolError(error);
      }
    }
  );
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/registration.test.ts tests/tools/nodes.test.ts`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/tools/nodes.ts tests/tools/nodes.test.ts tests/registration.test.ts
git commit -m "feat: register analyze-workspace-patterns tool"
```

---

### Task 8: Replace node-selection References in Tool Descriptions

**Files:**

- Modify: `src/tools/nodes.ts`

Several existing node creation tools reference `coalesce://context/node-selection`. Since that resource is deleted, replace all references with `coalesce://context/data-engineering-principles`.

- [ ] **Step 1: Update tool descriptions**

In `src/tools/nodes.ts`, find-and-replace in description strings:

Replace `coalesce://context/node-selection` with `coalesce://context/data-engineering-principles` in:

- `create-workspace-node` description
- `create-workspace-node-from-scratch` description
- `create-workspace-node-from-predecessor` description

Each description has a line like:
`"For guidance on node types and SQL patterns, see resources: coalesce://context/node-selection, coalesce://context/sql-platforms, ..."`

Change to:
`"For guidance on node types and SQL patterns, see resources: coalesce://context/data-engineering-principles, coalesce://context/sql-platforms, ..."`

- [ ] **Step 2: Run tests to verify nothing broke**

Run: `npx vitest run`

Expected: PASS — all tests pass (description changes don't affect test logic).

- [ ] **Step 3: Commit**

```bash
git add src/tools/nodes.ts
git commit -m "feat: replace node-selection with data-engineering-principles in tool descriptions"
```

---

### Task 9: Update README

**Files:**

- Modify: `README.md`

- [ ] **Step 1: Update the Nodes tool count and list**

In `README.md`, update:

```markdown
### Nodes (10)
```

Add the new tool to the list:

```markdown
- `analyze-workspace-patterns` — Analyze workspace nodes to detect package adoption, pipeline layers, methodology, and generate node type recommendations
```

- [ ] **Step 2: Update the Resources table**

Replace `node-selection` with `data-engineering-principles` in the AI Assistant Guidance Resources table:

Remove:

```markdown
| `coalesce://context/node-selection` | Node type categories, selection priorities, and workspace pattern matching |
```

Add:

```markdown
| `coalesce://context/data-engineering-principles` | Data engineering best practices for node type selection, layered architecture, and methodology detection |
```

- [ ] **Step 3: Run tests to verify nothing broke**

Run: `npx vitest run`

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: update README with new tool and replace node-selection resource"
```

---

### Task 10: Run Full Test Suite and Verify Build

**Files:** None (verification only)

- [ ] **Step 1: Run all tests**

Run: `npx vitest run`

Expected: All tests pass. No regressions.

- [ ] **Step 2: Run TypeScript build**

Run: `npx tsc --noEmit`

Expected: No type errors.

- [ ] **Step 3: Verify build output**

Run: `npm run build`

Expected: Clean build, no errors. `dist/` output includes new files.

- [ ] **Step 4: Final commit if any fixups needed**

Only if previous steps revealed issues. Otherwise, skip.
