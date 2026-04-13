import { describe, it, expect } from "vitest";
import {
  buildPredecessorSummary,
  getReferencedPredecessorNodeIDs,
  extractPredecessorNodeIDs,
  extractPredecessorRefInfo,
  buildJoinSuggestions,
  generateJoinSQL,
  generateRefJoinSQL,
  inferDatatype,
  analyzeColumnsForGroupBy,
  ensureFromClauseInSourceMapping,
  appendWhereToJoinCondition,
  type PredecessorSummary,
  type PredecessorRefInfo,
  type JoinSuggestion,
  type ColumnTransform,
} from "../../src/services/workspace/join-helpers.js";

// ── Test fixtures ──────────────────────────────────────────────────────────

function makeNode(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    name: "STG_ORDERS",
    locationName: "STG",
    metadata: {
      columns: [
        { name: "ORDER_ID", columnID: "col-1", sources: [] },
        { name: "CUSTOMER_ID", columnID: "col-2", sources: [] },
        { name: "AMOUNT", columnID: "col-3", sources: [] },
      ],
    },
    ...overrides,
  };
}

function makePredSummary(overrides: Partial<PredecessorSummary> = {}): PredecessorSummary {
  return {
    nodeID: "node-1",
    nodeName: "STG_ORDERS",
    columnCount: 3,
    columnNames: ["ORDER_ID", "CUSTOMER_ID", "AMOUNT"],
    ...overrides,
  };
}

function makePredRefInfo(overrides: Partial<PredecessorRefInfo> = {}): PredecessorRefInfo {
  return {
    nodeID: "node-1",
    nodeName: "STG_ORDERS",
    locationName: "STG",
    columnNames: ["ORDER_ID", "CUSTOMER_ID", "AMOUNT"],
    ...overrides,
  };
}

/** Read the joinCondition from a body's first sourceMapping entry. */
function getJoinCondition(body: Record<string, unknown>): string {
  const metadata = body.metadata as Record<string, unknown>;
  const sourceMapping = metadata.sourceMapping as Record<string, unknown>[];
  const first = sourceMapping[0] as Record<string, unknown>;
  const join = first.join as Record<string, unknown>;
  return join.joinCondition as string;
}

// ── buildPredecessorSummary ────────────────────────────────────────────────

describe("buildPredecessorSummary", () => {
  it("extracts name, column count, and column names", () => {
    const node = makeNode();
    const result = buildPredecessorSummary("node-1", node);

    expect(result.nodeID).toBe("node-1");
    expect(result.nodeName).toBe("STG_ORDERS");
    expect(result.columnCount).toBe(3);
    expect(result.columnNames).toEqual(["ORDER_ID", "CUSTOMER_ID", "AMOUNT"]);
  });

  it("returns null nodeName when name is missing", () => {
    const node = makeNode({ name: undefined });
    const result = buildPredecessorSummary("node-1", node);
    expect(result.nodeName).toBeNull();
  });

  it("returns null nodeName when name is not a string", () => {
    const node = makeNode({ name: 42 });
    const result = buildPredecessorSummary("node-1", node);
    expect(result.nodeName).toBeNull();
  });

  it("returns 0 columns when metadata is missing", () => {
    const result = buildPredecessorSummary("node-1", {});
    expect(result.columnCount).toBe(0);
    expect(result.columnNames).toEqual([]);
  });

  it("returns 0 columns when columns is not an array", () => {
    const result = buildPredecessorSummary("node-1", { metadata: { columns: "bad" } });
    expect(result.columnCount).toBe(0);
    expect(result.columnNames).toEqual([]);
  });
});

// ── getReferencedPredecessorNodeIDs ────────────────────────────────────────

describe("getReferencedPredecessorNodeIDs", () => {
  it("returns predecessor IDs that appear in column source references", () => {
    const node = makeNode({
      metadata: {
        columns: [
          {
            name: "ORDER_ID",
            sources: [
              { columnReferences: [{ nodeID: "pred-a", columnID: "c1" }] },
            ],
          },
          {
            name: "CUSTOMER_ID",
            sources: [
              { columnReferences: [{ nodeID: "pred-b", columnID: "c2" }] },
            ],
          },
        ],
      },
    });

    const result = getReferencedPredecessorNodeIDs(node, ["pred-a", "pred-b", "pred-c"]);
    expect(result).toEqual(["pred-a", "pred-b"]);
  });

  it("returns empty array when no columns match", () => {
    const node = makeNode({
      metadata: {
        columns: [
          {
            name: "X",
            sources: [
              { columnReferences: [{ nodeID: "other-node", columnID: "c1" }] },
            ],
          },
        ],
      },
    });
    const result = getReferencedPredecessorNodeIDs(node, ["pred-a"]);
    expect(result).toEqual([]);
  });

  it("returns empty array when metadata has no columns", () => {
    const result = getReferencedPredecessorNodeIDs({}, ["pred-a"]);
    expect(result).toEqual([]);
  });

  it("deduplicates predecessor IDs in input", () => {
    const node = makeNode({
      metadata: {
        columns: [
          {
            name: "X",
            sources: [
              { columnReferences: [{ nodeID: "pred-a", columnID: "c1" }] },
            ],
          },
        ],
      },
    });
    const result = getReferencedPredecessorNodeIDs(node, ["pred-a", "pred-a", "pred-a"]);
    expect(result).toEqual(["pred-a"]);
  });

  it("preserves input order of predecessor IDs", () => {
    const node = makeNode({
      metadata: {
        columns: [
          {
            name: "X",
            sources: [
              { columnReferences: [
                { nodeID: "pred-b", columnID: "c1" },
                { nodeID: "pred-a", columnID: "c2" },
              ] },
            ],
          },
        ],
      },
    });
    const result = getReferencedPredecessorNodeIDs(node, ["pred-a", "pred-b"]);
    expect(result).toEqual(["pred-a", "pred-b"]);
  });

  it("skips malformed column entries gracefully", () => {
    const node = makeNode({
      metadata: {
        columns: [
          null,
          "not-an-object",
          { name: "X" }, // no sources
          { name: "Y", sources: "bad" }, // sources not array
          { name: "Z", sources: [null, "bad", { columnReferences: "bad" }] },
          {
            name: "OK",
            sources: [
              { columnReferences: [{ nodeID: "pred-a", columnID: "c1" }] },
            ],
          },
        ],
      },
    });
    const result = getReferencedPredecessorNodeIDs(node, ["pred-a"]);
    expect(result).toEqual(["pred-a"]);
  });
});

// ── extractPredecessorNodeIDs ──────────────────────────────────────────────

describe("extractPredecessorNodeIDs", () => {
  it("extracts IDs from sourceMapping aliases", () => {
    const metadata = {
      sourceMapping: [
        { aliases: { SRC: "node-a", REF: "node-b" } },
      ],
    };
    const result = extractPredecessorNodeIDs(metadata);
    expect(result).toContain("node-a");
    expect(result).toContain("node-b");
    expect(result).toHaveLength(2);
  });

  it("falls back to column references when aliases are empty", () => {
    const metadata = {
      sourceMapping: [{ aliases: {} }],
      columns: [
        {
          name: "X",
          sources: [
            { columnReferences: [{ nodeID: "col-node", columnID: "c1" }] },
          ],
        },
      ],
    };
    const result = extractPredecessorNodeIDs(metadata);
    expect(result).toEqual(["col-node"]);
  });

  it("does NOT use column fallback when aliases have entries", () => {
    const metadata = {
      sourceMapping: [{ aliases: { SRC: "alias-node" } }],
      columns: [
        {
          name: "X",
          sources: [
            { columnReferences: [{ nodeID: "col-node", columnID: "c1" }] },
          ],
        },
      ],
    };
    const result = extractPredecessorNodeIDs(metadata);
    expect(result).toEqual(["alias-node"]);
    expect(result).not.toContain("col-node");
  });

  it("returns empty array when sourceMapping is missing", () => {
    expect(extractPredecessorNodeIDs({})).toEqual([]);
  });

  it("returns empty array when sourceMapping is not an array", () => {
    expect(extractPredecessorNodeIDs({ sourceMapping: "bad" })).toEqual([]);
  });

  it("skips empty-string alias values", () => {
    const metadata = {
      sourceMapping: [{ aliases: { SRC: "", REF: "node-a" } }],
    };
    const result = extractPredecessorNodeIDs(metadata);
    expect(result).toEqual(["node-a"]);
  });

  it("deduplicates IDs across multiple sourceMapping entries", () => {
    const metadata = {
      sourceMapping: [
        { aliases: { SRC: "node-a" } },
        { aliases: { REF: "node-a" } },
      ],
    };
    const result = extractPredecessorNodeIDs(metadata);
    expect(result).toEqual(["node-a"]);
  });
});

// ── extractPredecessorRefInfo ──────────────────────────────────────────────

describe("extractPredecessorRefInfo", () => {
  it("extracts full ref info from a valid node", () => {
    const node = makeNode();
    const result = extractPredecessorRefInfo("node-1", node);
    expect(result).toEqual({
      nodeID: "node-1",
      nodeName: "STG_ORDERS",
      locationName: "STG",
      columnNames: ["ORDER_ID", "CUSTOMER_ID", "AMOUNT"],
    });
  });

  it("returns null when name is missing", () => {
    const node = makeNode({ name: undefined });
    expect(extractPredecessorRefInfo("node-1", node)).toBeNull();
  });

  it("returns null when locationName is missing", () => {
    const node = makeNode({ locationName: undefined });
    expect(extractPredecessorRefInfo("node-1", node)).toBeNull();
  });

  it("returns null when name is not a string", () => {
    const node = makeNode({ name: 42 });
    expect(extractPredecessorRefInfo("node-1", node)).toBeNull();
  });
});

// ── buildJoinSuggestions ───────────────────────────────────────────────────

describe("buildJoinSuggestions", () => {
  it("finds common columns between two predecessors (case-insensitive)", () => {
    const left = makePredSummary({
      nodeID: "n1",
      nodeName: "ORDERS",
      columnNames: ["ORDER_ID", "customer_id", "AMOUNT"],
    });
    const right = makePredSummary({
      nodeID: "n2",
      nodeName: "CUSTOMERS",
      columnNames: ["CUSTOMER_ID", "NAME", "EMAIL"],
    });

    const result = buildJoinSuggestions([left, right]);
    expect(result).toHaveLength(1);
    expect(result[0].leftPredecessorNodeID).toBe("n1");
    expect(result[0].rightPredecessorNodeID).toBe("n2");
    expect(result[0].commonColumns).toHaveLength(1);
    expect(result[0].commonColumns[0].normalizedName).toBe("CUSTOMER_ID");
    expect(result[0].commonColumns[0].leftColumnName).toBe("customer_id");
    expect(result[0].commonColumns[0].rightColumnName).toBe("CUSTOMER_ID");
  });

  it("returns empty commonColumns when there is no overlap", () => {
    const left = makePredSummary({ nodeID: "n1", columnNames: ["A", "B"] });
    const right = makePredSummary({ nodeID: "n2", columnNames: ["C", "D"] });
    const result = buildJoinSuggestions([left, right]);
    expect(result).toHaveLength(1);
    expect(result[0].commonColumns).toEqual([]);
  });

  it("generates all pairwise combinations for 3 predecessors", () => {
    const preds = [
      makePredSummary({ nodeID: "a", columnNames: ["X"] }),
      makePredSummary({ nodeID: "b", columnNames: ["X"] }),
      makePredSummary({ nodeID: "c", columnNames: ["X"] }),
    ];
    const result = buildJoinSuggestions(preds);
    // 3 choose 2 = 3 pairs: (a,b), (a,c), (b,c)
    expect(result).toHaveLength(3);
  });

  it("returns empty array for single predecessor", () => {
    const result = buildJoinSuggestions([makePredSummary()]);
    expect(result).toEqual([]);
  });

  it("returns empty array for no predecessors", () => {
    const result = buildJoinSuggestions([]);
    expect(result).toEqual([]);
  });

  it("sorts common columns alphabetically by normalized name", () => {
    const left = makePredSummary({ nodeID: "n1", columnNames: ["Z_COL", "A_COL", "M_COL"] });
    const right = makePredSummary({ nodeID: "n2", columnNames: ["M_COL", "Z_COL", "A_COL"] });
    const result = buildJoinSuggestions([left, right]);
    const names = result[0].commonColumns.map((c) => c.normalizedName);
    expect(names).toEqual(["A_COL", "M_COL", "Z_COL"]);
  });

  it("uses the first occurrence for duplicate normalized names", () => {
    // If a predecessor has "  ID  " and "ID", normalizeColumnName trims+uppercases both to "ID"
    // Only the first occurrence should be used
    const left = makePredSummary({ nodeID: "n1", columnNames: ["  ID  ", "ID"] });
    const right = makePredSummary({ nodeID: "n2", columnNames: ["id"] });
    const result = buildJoinSuggestions([left, right]);
    expect(result[0].commonColumns).toHaveLength(1);
    expect(result[0].commonColumns[0].leftColumnName).toBe("  ID  ");
  });
});

// ── generateJoinSQL ────────────────────────────────────────────────────────

describe("generateJoinSQL", () => {
  it("generates INNER JOIN SQL from a suggestion with common columns", () => {
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "ORDERS",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "CUSTOMERS",
        commonColumns: [
          { normalizedName: "CUSTOMER_ID", leftColumnName: "CUSTOMER_ID", rightColumnName: "CUSTOMER_ID" },
        ],
      },
    ];

    const result = generateJoinSQL(suggestions);
    expect(result.fromClause).toBe('FROM "ORDERS"');
    expect(result.joinClauses).toHaveLength(1);
    expect(result.joinClauses[0].type).toBe("INNER JOIN");
    expect(result.joinClauses[0].rightTable).toBe("CUSTOMERS");
    expect(result.fullSQL).toContain('FROM "ORDERS"');
    expect(result.fullSQL).toContain('INNER JOIN "CUSTOMERS"');
    expect(result.fullSQL).toContain('"ORDERS"."CUSTOMER_ID" = "CUSTOMERS"."CUSTOMER_ID"');
  });

  it("generates multi-condition ON clause for multiple common columns", () => {
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "T1",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "T2",
        commonColumns: [
          { normalizedName: "A", leftColumnName: "A", rightColumnName: "A" },
          { normalizedName: "B", leftColumnName: "B", rightColumnName: "B" },
        ],
      },
    ];

    const result = generateJoinSQL(suggestions, "LEFT JOIN");
    expect(result.fullSQL).toContain("LEFT JOIN");
    expect(result.fullSQL).toContain("AND");
    expect(result.joinClauses[0].onConditions).toHaveLength(2);
  });

  it("supports all join types", () => {
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "T1",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "T2",
        commonColumns: [
          { normalizedName: "ID", leftColumnName: "ID", rightColumnName: "ID" },
        ],
      },
    ];

    for (const joinType of ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"] as const) {
      const result = generateJoinSQL(suggestions, joinType);
      expect(result.fullSQL).toContain(joinType);
    }
  });

  it("returns empty result for empty suggestions", () => {
    const result = generateJoinSQL([]);
    expect(result.fromClause).toBe("");
    expect(result.joinClauses).toEqual([]);
    expect(result.fullSQL).toBe("");
  });

  it("falls back to LEFT_TABLE / RIGHT_TABLE when names are null", () => {
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: null,
        rightPredecessorNodeID: "n2",
        rightPredecessorName: null,
        commonColumns: [
          { normalizedName: "ID", leftColumnName: "ID", rightColumnName: "ID" },
        ],
      },
    ];

    const result = generateJoinSQL(suggestions);
    expect(result.fromClause).toContain("LEFT_TABLE");
    expect(result.fullSQL).toContain("RIGHT_TABLE");
  });

  it("uses each suggestion's own leftPredecessorName in ON conditions for 3+ predecessors", () => {
    // buildJoinSuggestions produces all pairwise combos: (A,B), (A,C), (B,C)
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "ORDERS",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "CUSTOMERS",
        commonColumns: [
          { normalizedName: "CUSTOMER_ID", leftColumnName: "CUSTOMER_ID", rightColumnName: "CUSTOMER_ID" },
        ],
      },
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "ORDERS",
        rightPredecessorNodeID: "n3",
        rightPredecessorName: "PRODUCTS",
        commonColumns: [
          { normalizedName: "PRODUCT_ID", leftColumnName: "PRODUCT_ID", rightColumnName: "PRODUCT_ID" },
        ],
      },
      {
        leftPredecessorNodeID: "n2",
        leftPredecessorName: "CUSTOMERS",
        rightPredecessorNodeID: "n3",
        rightPredecessorName: "PRODUCTS",
        commonColumns: [
          { normalizedName: "REGION_ID", leftColumnName: "REGION_ID", rightColumnName: "REGION_ID" },
        ],
      },
    ];

    const result = generateJoinSQL(suggestions);

    // FROM clause uses the first suggestion's left table
    expect(result.fromClause).toBe('FROM "ORDERS"');
    expect(result.joinClauses).toHaveLength(3);

    // First join: ORDERS → CUSTOMERS — ON should reference ORDERS
    expect(result.joinClauses[0].onConditions[0]).toBe(
      '"ORDERS"."CUSTOMER_ID" = "CUSTOMERS"."CUSTOMER_ID"'
    );

    // Second join: ORDERS → PRODUCTS — ON should reference ORDERS
    expect(result.joinClauses[1].onConditions[0]).toBe(
      '"ORDERS"."PRODUCT_ID" = "PRODUCTS"."PRODUCT_ID"'
    );

    // Third join: CUSTOMERS → PRODUCTS — ON must reference CUSTOMERS, not ORDERS
    expect(result.joinClauses[2].onConditions[0]).toBe(
      '"CUSTOMERS"."REGION_ID" = "PRODUCTS"."REGION_ID"'
    );
  });
});

// ── generateRefJoinSQL ─────────────────────────────────────────────────────

describe("generateRefJoinSQL", () => {
  it("generates ref-style FROM and JOIN with {{ ref() }} syntax", () => {
    const preds: PredecessorRefInfo[] = [
      makePredRefInfo({ nodeID: "n1", nodeName: "ORDERS", locationName: "STG" }),
      makePredRefInfo({ nodeID: "n2", nodeName: "CUSTOMERS", locationName: "STG" }),
    ];
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "ORDERS",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "CUSTOMERS",
        commonColumns: [
          { normalizedName: "CUSTOMER_ID", leftColumnName: "CUSTOMER_ID", rightColumnName: "CUSTOMER_ID" },
        ],
      },
    ];

    const result = generateRefJoinSQL(preds, suggestions, "LEFT JOIN");
    expect(result.fromClause).toContain("{{ ref('STG', 'ORDERS') }}");
    expect(result.joinClauses).toHaveLength(1);
    expect(result.joinClauses[0]).toContain("{{ ref('STG', 'CUSTOMERS') }}");
    expect(result.joinClauses[0]).toContain("LEFT JOIN");
    expect(result.fullSQL).toContain('"ORDERS"."CUSTOMER_ID" = "CUSTOMERS"."CUSTOMER_ID"');
    expect(result.warnings).toEqual([]);
  });

  it("returns empty result for no predecessors", () => {
    const result = generateRefJoinSQL([], [], "INNER JOIN");
    expect(result.fromClause).toBe("");
    expect(result.joinClauses).toEqual([]);
    expect(result.fullSQL).toBe("");
    expect(result.warnings).toEqual([]);
  });

  it("warns when no common columns and no overrides", () => {
    const preds: PredecessorRefInfo[] = [
      makePredRefInfo({ nodeID: "n1", nodeName: "T1", locationName: "L1" }),
      makePredRefInfo({ nodeID: "n2", nodeName: "T2", locationName: "L2" }),
    ];
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "T1",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "T2",
        commonColumns: [],
      },
    ];

    const result = generateRefJoinSQL(preds, suggestions, "INNER JOIN");
    expect(result.warnings).toHaveLength(1);
    expect(result.warnings[0]).toContain("No common columns");
    // T2 also warned as not included
    expect(result.warnings.some((w) => w.includes("T2"))).toBe(true);
  });

  it("uses joinColumnOverrides instead of common columns when provided", () => {
    const preds: PredecessorRefInfo[] = [
      makePredRefInfo({ nodeID: "n1", nodeName: "T1", locationName: "L1" }),
      makePredRefInfo({ nodeID: "n2", nodeName: "T2", locationName: "L2" }),
    ];
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "T1",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "T2",
        commonColumns: [
          { normalizedName: "AUTO_COL", leftColumnName: "AUTO_COL", rightColumnName: "AUTO_COL" },
        ],
      },
    ];

    const result = generateRefJoinSQL(preds, suggestions, "INNER JOIN", [
      { leftPredecessor: "T1", rightPredecessor: "T2", leftColumn: "MY_KEY", rightColumn: "THEIR_KEY" },
    ]);

    expect(result.fullSQL).toContain('"MY_KEY"');
    expect(result.fullSQL).toContain('"THEIR_KEY"');
    expect(result.fullSQL).not.toContain("AUTO_COL");
    expect(result.warnings).toEqual([]);
  });

  it("joins extra predecessors via overrides when they have no suggestion", () => {
    const preds: PredecessorRefInfo[] = [
      makePredRefInfo({ nodeID: "n1", nodeName: "T1", locationName: "L1" }),
      makePredRefInfo({ nodeID: "n2", nodeName: "T2", locationName: "L2" }),
      makePredRefInfo({ nodeID: "n3", nodeName: "T3", locationName: "L3" }),
    ];
    // Only a suggestion between T1 and T2
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "T1",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "T2",
        commonColumns: [
          { normalizedName: "ID", leftColumnName: "ID", rightColumnName: "ID" },
        ],
      },
    ];

    // T3 has an override but no suggestion
    const result = generateRefJoinSQL(preds, suggestions, "LEFT JOIN", [
      { leftPredecessor: "T1", rightPredecessor: "T3", leftColumn: "KEY", rightColumn: "FK" },
    ]);

    expect(result.joinClauses).toHaveLength(2); // T2 + T3
    expect(result.fullSQL).toContain("T3");
    expect(result.warnings).toEqual([]);
  });

  it("skips duplicate right-side predecessor joins", () => {
    const preds: PredecessorRefInfo[] = [
      makePredRefInfo({ nodeID: "n1", nodeName: "T1", locationName: "L1" }),
      makePredRefInfo({ nodeID: "n2", nodeName: "T2", locationName: "L2" }),
    ];
    // Two suggestions both joining T2 — second should be skipped
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "T1",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "T2",
        commonColumns: [
          { normalizedName: "ID", leftColumnName: "ID", rightColumnName: "ID" },
        ],
      },
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "T1",
        rightPredecessorNodeID: "n2",
        rightPredecessorName: "T2",
        commonColumns: [
          { normalizedName: "CODE", leftColumnName: "CODE", rightColumnName: "CODE" },
        ],
      },
    ];

    const result = generateRefJoinSQL(preds, suggestions, "INNER JOIN");
    expect(result.joinClauses).toHaveLength(1);
  });

  it("resolves right predecessor by name when ID doesn't match", () => {
    const preds: PredecessorRefInfo[] = [
      makePredRefInfo({ nodeID: "n1", nodeName: "T1", locationName: "L1" }),
      makePredRefInfo({ nodeID: "n2-actual", nodeName: "T2", locationName: "L2" }),
    ];
    // Suggestion references a different ID but matching name
    const suggestions: JoinSuggestion[] = [
      {
        leftPredecessorNodeID: "n1",
        leftPredecessorName: "T1",
        rightPredecessorNodeID: "n2-stale",
        rightPredecessorName: "T2",
        commonColumns: [
          { normalizedName: "ID", leftColumnName: "ID", rightColumnName: "ID" },
        ],
      },
    ];

    const result = generateRefJoinSQL(preds, suggestions, "INNER JOIN");
    expect(result.joinClauses).toHaveLength(1);
    expect(result.fullSQL).toContain("T2");
  });
});

// ── inferDatatype ──────────────────────────────────────────────────────────

describe("inferDatatype", () => {
  it("returns NUMBER for COUNT", () => {
    expect(inferDatatype("COUNT(*)")).toBe("NUMBER");
    expect(inferDatatype("count(id)")).toBe("NUMBER");
  });

  it("returns NUMBER for COUNT(DISTINCT ...)", () => {
    expect(inferDatatype("COUNT(DISTINCT customer_id)")).toBe("NUMBER");
  });

  it("returns NUMBER(38,4) for SUM, AVG, STDDEV, VARIANCE", () => {
    expect(inferDatatype("SUM(amount)")).toBe("NUMBER(38,4)");
    expect(inferDatatype("avg(price)")).toBe("NUMBER(38,4)");
    expect(inferDatatype("STDDEV(val)")).toBe("NUMBER(38,4)");
    expect(inferDatatype("VARIANCE(metric)")).toBe("NUMBER(38,4)");
  });

  it("returns DATE for date functions", () => {
    expect(inferDatatype("DATEADD(day, 1, col)")).toBe("DATE");
    expect(inferDatatype("CURRENT_DATE")).toBe("DATE");
  });

  it("returns NUMBER for DATEDIFF", () => {
    expect(inferDatatype("DATEDIFF(day, a, b)")).toBe("NUMBER");
  });

  it("returns TIMESTAMP_NTZ(9) for CURRENT_TIMESTAMP", () => {
    expect(inferDatatype("CURRENT_TIMESTAMP")).toBe("TIMESTAMP_NTZ(9)");
  });

  it("infers TIMESTAMP_NTZ(9) for MIN/MAX with _TS suffix", () => {
    expect(inferDatatype("MIN(CREATED_TS)")).toBe("TIMESTAMP_NTZ(9)");
    expect(inferDatatype("MAX(UPDATED_TS)")).toBe("TIMESTAMP_NTZ(9)");
  });

  it("infers DATE for MIN/MAX with _DATE suffix", () => {
    expect(inferDatatype("MIN(ORDER_DATE)")).toBe("DATE");
    expect(inferDatatype("MAX(SHIP_DATE)")).toBe("DATE");
  });

  it("returns VARCHAR for string functions", () => {
    expect(inferDatatype("CONCAT(a, b)")).toBe("VARCHAR");
    expect(inferDatatype("UPPER(name)")).toBe("VARCHAR");
    expect(inferDatatype("LOWER(name)")).toBe("VARCHAR");
    expect(inferDatatype("TRIM(name)")).toBe("VARCHAR");
    expect(inferDatatype("SUBSTR(name, 1, 3)")).toBe("VARCHAR");
    expect(inferDatatype("LEFT(name, 5)")).toBe("VARCHAR");
    expect(inferDatatype("RIGHT(name, 5)")).toBe("VARCHAR");
  });

  it("returns VARCHAR for CASE expressions", () => {
    expect(inferDatatype("CASE WHEN x THEN y ELSE z END")).toBe("VARCHAR");
  });

  it("returns NUMBER for window functions", () => {
    expect(inferDatatype("ROW_NUMBER()")).toBe("NUMBER");
    expect(inferDatatype("RANK()")).toBe("NUMBER");
    expect(inferDatatype("DENSE_RANK()")).toBe("NUMBER");
  });

  it("returns undefined for unknown transforms", () => {
    expect(inferDatatype("MY_COLUMN")).toBeUndefined();
    expect(inferDatatype("")).toBeUndefined();
    expect(inferDatatype("SOME_FUNCTION(x)")).toBeUndefined();
  });

  it("is case-insensitive", () => {
    expect(inferDatatype("sum(amount)")).toBe("NUMBER(38,4)");
    expect(inferDatatype("Count(*)")).toBe("NUMBER");
    expect(inferDatatype("current_date")).toBe("DATE");
  });

  it("prioritizes date functions over MIN/MAX", () => {
    // DATEDIFF contains "DATE" and could also match other patterns
    expect(inferDatatype("DATEDIFF(day, a, b)")).toBe("NUMBER");
  });
});

// ── analyzeColumnsForGroupBy ───────────────────────────────────────────────

describe("analyzeColumnsForGroupBy", () => {
  it("identifies GROUP BY columns and aggregate columns", () => {
    const columns: ColumnTransform[] = [
      { name: "REGION", transform: "REGION" },
      { name: "TOTAL_SALES", transform: "SUM(AMOUNT)" },
      { name: "ORDER_COUNT", transform: "COUNT(*)" },
    ];

    const result = analyzeColumnsForGroupBy(columns);
    expect(result.groupByColumns).toEqual(["REGION"]);
    expect(result.aggregateColumns).toHaveLength(2);
    expect(result.hasAggregates).toBe(true);
    expect(result.groupByClause).toBe("GROUP BY REGION");
    expect(result.validation.valid).toBe(true);
    expect(result.validation.errors).toEqual([]);
  });

  it("generates multi-column GROUP BY clause", () => {
    const columns: ColumnTransform[] = [
      { name: "REGION", transform: "REGION" },
      { name: "CATEGORY", transform: "CATEGORY" },
      { name: "TOTAL", transform: "SUM(AMOUNT)" },
    ];

    const result = analyzeColumnsForGroupBy(columns);
    expect(result.groupByColumns).toEqual(["REGION", "CATEGORY"]);
    expect(result.groupByClause).toBe("GROUP BY REGION, CATEGORY");
  });

  it("returns empty groupByClause when no aggregates exist", () => {
    const columns: ColumnTransform[] = [
      { name: "A", transform: "COL_A" },
      { name: "B", transform: "COL_B" },
    ];

    const result = analyzeColumnsForGroupBy(columns);
    expect(result.groupByColumns).toEqual(["COL_A", "COL_B"]);
    expect(result.aggregateColumns).toEqual([]);
    expect(result.hasAggregates).toBe(false);
    expect(result.groupByClause).toBe("");
  });

  it("treats window functions as aggregates (not in GROUP BY)", () => {
    const columns: ColumnTransform[] = [
      { name: "ID", transform: "ID" },
      { name: "RN", transform: "ROW_NUMBER() OVER (PARTITION BY ID ORDER BY DATE)" },
    ];

    const result = analyzeColumnsForGroupBy(columns);
    expect(result.aggregateColumns).toHaveLength(1);
    expect(result.aggregateColumns[0].name).toBe("RN");
    expect(result.groupByColumns).toEqual(["ID"]);
  });

  it("treats pure-aggregate queries as valid without GROUP BY", () => {
    const columns: ColumnTransform[] = [
      { name: "TOTAL", transform: "SUM(AMOUNT)" },
      { name: "AVG_PRICE", transform: "AVG(PRICE)" },
    ];

    const result = analyzeColumnsForGroupBy(columns);
    // Pure-aggregate queries are valid SQL — the entire result set is one group
    expect(result.validation.valid).toBe(true);
    expect(result.validation.errors).toEqual([]);
    expect(result.groupByClause).toBe("");
  });

  it("considers single aggregate column valid without GROUP BY", () => {
    const columns: ColumnTransform[] = [
      { name: "TOTAL", transform: "SUM(AMOUNT)" },
    ];

    const result = analyzeColumnsForGroupBy(columns);
    expect(result.validation.valid).toBe(true);
    expect(result.groupByClause).toBe("");
  });

  it("detects all aggregate function types", () => {
    const fns = ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "STDDEV(", "VARIANCE(", "LISTAGG(", "ARRAY_AGG("];
    for (const fn of fns) {
      const columns: ColumnTransform[] = [
        { name: "X", transform: `${fn}col)` },
      ];
      const result = analyzeColumnsForGroupBy(columns);
      expect(result.hasAggregates).toBe(true);
    }
  });

  it("handles empty column list", () => {
    const result = analyzeColumnsForGroupBy([]);
    expect(result.groupByColumns).toEqual([]);
    expect(result.aggregateColumns).toEqual([]);
    expect(result.hasAggregates).toBe(false);
    expect(result.groupByClause).toBe("");
    expect(result.validation.valid).toBe(true);
  });
});

// ── ensureFromClauseInSourceMapping ────────────────────────────────────────

describe("ensureFromClauseInSourceMapping", () => {
  it("generates FROM clause when joinCondition is empty", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: "" },
            dependencies: [
              { nodeName: "UPSTREAM", locationName: "STG" },
            ],
          },
        ],
      },
    };

    ensureFromClauseInSourceMapping(body);

    expect(getJoinCondition(body)).toBe(
      `FROM {{ ref('STG', 'UPSTREAM') }} "UPSTREAM"`
    );
  });

  it("does not overwrite existing joinCondition", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: "FROM existing" },
            dependencies: [
              { nodeName: "UPSTREAM", locationName: "STG" },
            ],
          },
        ],
      },
    };

    ensureFromClauseInSourceMapping(body);

    expect(getJoinCondition(body)).toBe("FROM existing");
  });

  it("does nothing when metadata is missing", () => {
    const body: Record<string, unknown> = {};
    ensureFromClauseInSourceMapping(body);
    // No error, no changes
    expect(body.metadata).toBeUndefined();
  });

  it("does nothing when sourceMapping is empty", () => {
    const body: Record<string, unknown> = { metadata: { sourceMapping: [] } };
    ensureFromClauseInSourceMapping(body);
    const metadata = body.metadata as Record<string, unknown>;
    expect(metadata.sourceMapping).toEqual([]);
  });

  it("generates ref without locationName when it is missing", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: {},
            dependencies: [{ nodeName: "UPSTREAM" }],
          },
        ],
      },
    };

    ensureFromClauseInSourceMapping(body);

    expect(getJoinCondition(body)).toBe(`FROM {{ ref('UPSTREAM') }} "UPSTREAM"`);
  });

  it("does nothing when dependencies are empty", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: "" },
            dependencies: [],
          },
        ],
      },
    };

    ensureFromClauseInSourceMapping(body);

    expect(getJoinCondition(body)).toBe("");
  });

  it("treats whitespace-only joinCondition as empty", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: "   " },
            dependencies: [
              { nodeName: "UPSTREAM", locationName: "STG" },
            ],
          },
        ],
      },
    };

    ensureFromClauseInSourceMapping(body);

    // "   ".trim() === "" is falsy, so the function generates the FROM clause
    expect(getJoinCondition(body)).toContain("FROM");
  });
});

// ── appendWhereToJoinCondition ─────────────────────────────────────────────

describe("appendWhereToJoinCondition", () => {
  it("appends WHERE to existing FROM clause", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: `FROM {{ ref('STG', 'ORDERS') }} "ORDERS"` },
            dependencies: [],
          },
        ],
      },
    };

    appendWhereToJoinCondition(body, "STATUS = 'ACTIVE'");

    expect(getJoinCondition(body)).toContain("WHERE STATUS = 'ACTIVE'");
    expect(getJoinCondition(body)).toContain("FROM");
  });

  it("appends with AND when WHERE already exists", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: {
              joinCondition: `FROM "T"\nWHERE STATUS = 'ACTIVE'`,
            },
            dependencies: [],
          },
        ],
      },
    };

    appendWhereToJoinCondition(body, "REGION = 'US'");

    expect(getJoinCondition(body)).toContain("AND REGION = 'US'");
  });

  it("strips leading WHERE keyword from the condition", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: `FROM "T"` },
            dependencies: [],
          },
        ],
      },
    };

    appendWhereToJoinCondition(body, "WHERE STATUS = 'ACTIVE'");

    const condition = getJoinCondition(body);
    // Should not have "WHERE WHERE"
    expect(condition).not.toContain("WHERE WHERE");
    expect(condition).toContain("WHERE STATUS = 'ACTIVE'");
  });

  it("strips backslash-escaped quotes", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: `FROM "T"` },
            dependencies: [],
          },
        ],
      },
    };

    appendWhereToJoinCondition(body, 'STATUS = \\"ACTIVE\\"');

    const condition = getJoinCondition(body);
    expect(condition).toContain('STATUS = "ACTIVE"');
    expect(condition).not.toContain("\\");
  });

  it("generates FROM clause from dependencies when joinCondition is empty", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: {},
            dependencies: [{ nodeName: "UPSTREAM", locationName: "STG" }],
          },
        ],
      },
    };

    appendWhereToJoinCondition(body, "X = 1");

    const condition = getJoinCondition(body);
    expect(condition).toContain("FROM {{ ref('STG', 'UPSTREAM') }}");
    expect(condition).toContain("WHERE X = 1");
  });

  it("creates standalone WHERE when no existing clause and no dependencies", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: {},
            dependencies: [],
          },
        ],
      },
    };

    appendWhereToJoinCondition(body, "X = 1");

    expect(getJoinCondition(body)).toBe("WHERE X = 1");
  });

  it("does nothing for empty where condition", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: `FROM "T"` },
            dependencies: [],
          },
        ],
      },
    };

    appendWhereToJoinCondition(body, "   ");

    expect(getJoinCondition(body)).toBe(`FROM "T"`);
  });

  it("does nothing for WHERE-only input", () => {
    const body: Record<string, unknown> = {
      metadata: {
        sourceMapping: [
          {
            join: { joinCondition: `FROM "T"` },
            dependencies: [],
          },
        ],
      },
    };

    appendWhereToJoinCondition(body, "WHERE   ");

    expect(getJoinCondition(body)).toBe(`FROM "T"`);
  });

  it("does nothing when metadata is missing", () => {
    const body: Record<string, unknown> = {};
    appendWhereToJoinCondition(body, "X = 1");
    expect(body.metadata).toBeUndefined();
  });

  it("does nothing when sourceMapping is empty", () => {
    const body: Record<string, unknown> = { metadata: { sourceMapping: [] } };
    appendWhereToJoinCondition(body, "X = 1");
    const metadata = body.metadata as Record<string, unknown>;
    expect(metadata.sourceMapping).toEqual([]);
  });
});
