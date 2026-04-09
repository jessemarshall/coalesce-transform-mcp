import { describe, it, expect } from "vitest";
import {
  detectPackages,
  inferNodeLayer,
  inferLayers,
  detectMethodology,
  buildWorkspaceProfile,
} from "../../src/services/workspace/analysis.js";

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
