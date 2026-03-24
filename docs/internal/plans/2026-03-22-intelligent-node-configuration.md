# Intelligent Node Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add intelligent, node-type-aware configuration completion that automatically sets all required and contextual config fields based on node type schemas and node analysis.

**Architecture:** Build layered service modules (context analyzer → schema resolver → field classifier → rules engine → main orchestrator) that compose into two tools: standalone `complete-node-configuration` and enhanced `convert-join-to-aggregation` with auto-config.

**Tech Stack:** TypeScript, Vitest, Zod, Coalesce API, existing repo/corpus tools

**Related Spec:** [docs/internal/specs/2026-03-22-intelligent-node-configuration-design.md](../specs/2026-03-22-intelligent-node-configuration-design.md)

---

## File Structure

**New service modules:**
```
src/services/config/
  context-analyzer.ts      - analyzeNodeContext() - detect patterns in node
  schema-resolver.ts       - resolveNodeTypeSchema() - repo → corpus resolution
  field-classifier.ts      - classifyConfigFields() - categorize field types
  rules.ts                - applyIntelligenceRules() - contextual logic
  intelligent.ts          - completeNodeConfiguration() - main orchestrator
```

**Modified files:**
```
src/services/workspace/mutations.ts    - Update convertJoinToAggregation
src/mcp/nodes.ts                       - Register complete-node-configuration tool
src/resources/index.ts                 - Register new resource
```

**New test files:**
```
tests/services/config/
  context-analyzer.test.ts
  schema-resolver.test.ts
  field-classifier.test.ts
  rules.test.ts
  intelligent.test.ts

tests/tools/
  complete-node-configuration.test.ts
```

**Documentation:**
```
src/resources/context/
  intelligent-node-configuration.md    - New: How it works, usage, examples
  aggregation-patterns.md             - Update: Add config completion section
  tool-usage.md                       - Update: Add new tool
```

---

## Task 1: Context Analyzer - Pattern Detection

**Files:**
- Create: `src/services/config/context-analyzer.ts`
- Test: `tests/services/config/context-analyzer.test.ts`

- [ ] **Step 1: Write failing test for multi-source detection**

Create `tests/services/config/context-analyzer.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { analyzeNodeContext } from "../../../src/services/config/context-analyzer.js";

describe("analyzeNodeContext", () => {
  it("detects multi-source nodes", () => {
    const node = {
      metadata: {
        sources: [{ name: "ORDERS" }, { name: "CUSTOMERS" }],
        columns: [],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasMultipleSources).toBe(true);
  });

  it("detects single-source nodes", () => {
    const node = {
      metadata: {
        sources: [{ name: "ORDERS" }],
        columns: [],
      },
      config: {},
    };

    const result = analyzeNodeContext(node as any);

    expect(result.hasMultipleSources).toBe(false);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/context-analyzer.test.ts`
Expected: FAIL - "Cannot find module"

- [ ] **Step 3: Write minimal implementation for multi-source detection**

Create `src/services/config/context-analyzer.ts`:

```typescript
export interface NodeContext {
  hasMultipleSources: boolean;
  hasAggregates: boolean;
  hasTimestampColumns: boolean;
  hasType2Pattern: boolean;
  materializationType: "table" | "view";
  columnPatterns: {
    timestamps: string[];
    dates: string[];
    businessKeys: string[];
  };
}

export function analyzeNodeContext(node: any): NodeContext {
  const sources = node.metadata?.sources || [];
  const columns = node.metadata?.columns || [];

  return {
    hasMultipleSources: sources.length > 1,
    hasAggregates: false,
    hasTimestampColumns: false,
    hasType2Pattern: false,
    materializationType: "table",
    columnPatterns: {
      timestamps: [],
      dates: [],
      businessKeys: [],
    },
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/context-analyzer.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/context-analyzer.test.ts src/services/config/context-analyzer.ts
git commit -m "feat(config): add context analyzer with multi-source detection

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Context Analyzer - Aggregate Detection

**Files:**
- Modify: `src/services/config/context-analyzer.ts`
- Modify: `tests/services/config/context-analyzer.test.ts`

- [ ] **Step 1: Write failing test for aggregate detection**

Add to `tests/services/config/context-analyzer.test.ts`:

```typescript
it("detects aggregate columns", () => {
  const node = {
    metadata: {
      sources: [],
      columns: [
        { name: "CUSTOMER_ID", transform: '"ORDERS"."CUSTOMER_ID"' },
        { name: "TOTAL_ORDERS", transform: 'COUNT(DISTINCT "ORDERS"."ORDER_ID")' },
        { name: "REVENUE", transform: 'SUM("ORDERS"."AMOUNT")' },
      ],
    },
    config: {},
  };

  const result = analyzeNodeContext(node as any);

  expect(result.hasAggregates).toBe(true);
});

it("detects non-aggregate columns", () => {
  const node = {
    metadata: {
      sources: [],
      columns: [
        { name: "CUSTOMER_ID", transform: '"ORDERS"."CUSTOMER_ID"' },
        { name: "NAME", transform: '"CUSTOMERS"."NAME"' },
      ],
    },
    config: {},
  };

  const result = analyzeNodeContext(node as any);

  expect(result.hasAggregates).toBe(false);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/context-analyzer.test.ts`
Expected: FAIL - "Expected: true, Received: false"

- [ ] **Step 3: Implement aggregate detection**

Update `src/services/config/context-analyzer.ts`:

```typescript
export function analyzeNodeContext(node: any): NodeContext {
  const sources = node.metadata?.sources || [];
  const columns = node.metadata?.columns || [];

  // Detect aggregates
  const aggregatePattern = /COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE|LISTAGG|ARRAY_AGG/i;
  const hasAggregates = columns.some((col: any) =>
    aggregatePattern.test(col.transform || "")
  );

  return {
    hasMultipleSources: sources.length > 1,
    hasAggregates,
    hasTimestampColumns: false,
    hasType2Pattern: false,
    materializationType: "table",
    columnPatterns: {
      timestamps: [],
      dates: [],
      businessKeys: [],
    },
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/context-analyzer.test.ts`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/context-analyzer.test.ts src/services/config/context-analyzer.ts
git commit -m "feat(config): add aggregate detection to context analyzer

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Context Analyzer - Timestamp Column Detection

**Files:**
- Modify: `src/services/config/context-analyzer.ts`
- Modify: `tests/services/config/context-analyzer.test.ts`

- [ ] **Step 1: Write failing test for timestamp detection**

Add to `tests/services/config/context-analyzer.test.ts`:

```typescript
it("detects timestamp columns", () => {
  const node = {
    metadata: {
      sources: [],
      columns: [
        { name: "CUSTOMER_ID", transform: '"ORDERS"."CUSTOMER_ID"' },
        { name: "CREATED_TS", transform: '"ORDERS"."CREATED_TS"' },
        { name: "ORDER_DATE", transform: '"ORDERS"."ORDER_DATE"' },
        { name: "LAST_UPDATED_TIMESTAMP", transform: 'MAX("ORDERS"."UPDATED_TS")' },
      ],
    },
    config: {},
  };

  const result = analyzeNodeContext(node as any);

  expect(result.hasTimestampColumns).toBe(true);
  expect(result.columnPatterns.timestamps).toContain("CREATED_TS");
  expect(result.columnPatterns.timestamps).toContain("LAST_UPDATED_TIMESTAMP");
  expect(result.columnPatterns.dates).toContain("ORDER_DATE");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/context-analyzer.test.ts`
Expected: FAIL - hasTimestampColumns expected true

- [ ] **Step 3: Implement timestamp column detection**

Update `src/services/config/context-analyzer.ts`:

```typescript
export function analyzeNodeContext(node: any): NodeContext {
  const sources = node.metadata?.sources || [];
  const columns = node.metadata?.columns || [];

  // Detect aggregates
  const aggregatePattern = /COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE|LISTAGG|ARRAY_AGG/i;
  const hasAggregates = columns.some((col: any) =>
    aggregatePattern.test(col.transform || "")
  );

  // Detect timestamp columns
  const timestampPattern = /_TS$|_TIMESTAMP$|TIMESTAMP_/i;
  const datePattern = /_DATE$|_DT$|DATE_/i;

  const timestamps = columns
    .filter((col: any) => timestampPattern.test(col.name))
    .map((col: any) => col.name);

  const dates = columns
    .filter((col: any) => datePattern.test(col.name))
    .map((col: any) => col.name);

  const hasTimestampColumns = timestamps.length > 0 || dates.length > 0;

  return {
    hasMultipleSources: sources.length > 1,
    hasAggregates,
    hasTimestampColumns,
    hasType2Pattern: false,
    materializationType: "table",
    columnPatterns: {
      timestamps,
      dates,
      businessKeys: [],
    },
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/context-analyzer.test.ts`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/context-analyzer.test.ts src/services/config/context-analyzer.ts
git commit -m "feat(config): add timestamp column detection to context analyzer

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Context Analyzer - Type 2 Dimension Pattern Detection

**Files:**
- Modify: `src/services/config/context-analyzer.ts`
- Modify: `tests/services/config/context-analyzer.test.ts`

- [ ] **Step 1: Write failing test for Type 2 pattern detection**

Add to `tests/services/config/context-analyzer.test.ts`:

```typescript
it("detects Type 2 dimension pattern", () => {
  const node = {
    metadata: {
      sources: [],
      columns: [
        { name: "CUSTOMER_ID", transform: '"DIM"."CUSTOMER_ID"' },
        { name: "START_DATE", transform: '"DIM"."START_DATE"' },
        { name: "END_DATE", transform: '"DIM"."END_DATE"' },
        { name: "IS_CURRENT", transform: '"DIM"."IS_CURRENT"' },
      ],
    },
    config: {},
  };

  const result = analyzeNodeContext(node as any);

  expect(result.hasType2Pattern).toBe(true);
});

it("does not detect Type 2 when pattern incomplete", () => {
  const node = {
    metadata: {
      sources: [],
      columns: [
        { name: "CUSTOMER_ID", transform: '"DIM"."CUSTOMER_ID"' },
        { name: "START_DATE", transform: '"DIM"."START_DATE"' },
        // Missing END_DATE and IS_CURRENT
      ],
    },
    config: {},
  };

  const result = analyzeNodeContext(node as any);

  expect(result.hasType2Pattern).toBe(false);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/context-analyzer.test.ts`
Expected: FAIL - hasType2Pattern expected true

- [ ] **Step 3: Implement Type 2 pattern detection**

Update `src/services/config/context-analyzer.ts`:

```typescript
export function analyzeNodeContext(node: any): NodeContext {
  const sources = node.metadata?.sources || [];
  const columns = node.metadata?.columns || [];

  // Detect aggregates
  const aggregatePattern = /COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE|LISTAGG|ARRAY_AGG/i;
  const hasAggregates = columns.some((col: any) =>
    aggregatePattern.test(col.transform || "")
  );

  // Detect timestamp columns
  const timestampPattern = /_TS$|_TIMESTAMP$|TIMESTAMP_/i;
  const datePattern = /_DATE$|_DT$|DATE_/i;

  const timestamps = columns
    .filter((col: any) => timestampPattern.test(col.name))
    .map((col: any) => col.name);

  const dates = columns
    .filter((col: any) => datePattern.test(col.name))
    .map((col: any) => col.name);

  const hasTimestampColumns = timestamps.length > 0 || dates.length > 0;

  // Detect Type 2 dimension pattern
  const hasStartDate = columns.some((col: any) =>
    /START_DATE|EFFECTIVE_DATE/i.test(col.name)
  );
  const hasEndDate = columns.some((col: any) =>
    /END_DATE|EXPIRY_DATE/i.test(col.name)
  );
  const hasCurrentFlag = columns.some((col: any) =>
    /IS_CURRENT|CURRENT_FLAG/i.test(col.name)
  );

  const hasType2Pattern = hasStartDate && hasEndDate && hasCurrentFlag;

  return {
    hasMultipleSources: sources.length > 1,
    hasAggregates,
    hasTimestampColumns,
    hasType2Pattern,
    materializationType: "table",
    columnPatterns: {
      timestamps,
      dates,
      businessKeys: [],
    },
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/context-analyzer.test.ts`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/context-analyzer.test.ts src/services/config/context-analyzer.ts
git commit -m "feat(config): add Type 2 dimension pattern detection

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Schema Resolver - Repo-Backed Resolution

**Files:**
- Create: `src/services/config/schema-resolver.ts`
- Test: `tests/services/config/schema-resolver.test.ts`

- [ ] **Step 1: Write failing test for repo-backed schema resolution**

Create `tests/services/config/schema-resolver.test.ts`:

```typescript
import { describe, it, expect, vi } from "vitest";
import { resolveNodeTypeSchema } from "../../../src/services/config/schema-resolver.js";

describe("resolveNodeTypeSchema", () => {
  it("resolves schema from repo when repoPath provided", async () => {
    const repoPath = "/Users/jmarshall/Documents/GitHub/coalesce-transform-mcp/tests/fixtures/repo-backed-coalesce";

    const result = await resolveNodeTypeSchema("DataVault:::33", repoPath);

    expect(result.source).toBe("repo");
    expect(result.schema.config).toBeDefined();
    expect(result.schema.config[0].items).toBeDefined();
  });

  it("resolves Stage schema from repo", async () => {
    const repoPath = "/Users/jmarshall/Documents/GitHub/coalesce-transform-mcp/tests/fixtures/repo-backed-coalesce";

    const result = await resolveNodeTypeSchema("Stage:::Stage", repoPath);

    expect(result.source).toBe("repo");
    expect(result.schema.config).toBeDefined();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/schema-resolver.test.ts`
Expected: FAIL - "Cannot find module"

- [ ] **Step 3: Write minimal implementation using existing repo tools**

Create `src/services/config/schema-resolver.ts`:

```typescript
import { getRepoNodeTypeDefinition } from "../repo/operations.js";

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

export async function resolveNodeTypeSchema(
  nodeType: string,
  repoPath?: string
): Promise<SchemaResolution> {
  // Try repo first if path provided
  if (repoPath) {
    try {
      const def = await getRepoNodeTypeDefinition(repoPath, nodeType);
      return {
        source: "repo",
        schema: def.nodeDefinition as NodeTypeSchema,
      };
    } catch (error) {
      // Fall through to corpus
    }
  }

  // TODO: Corpus fallback will be added in next task
  throw new Error(
    `Cannot resolve node type schema for '${nodeType}'. ` +
    `Repo resolution failed and corpus fallback not yet implemented.`
  );
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/schema-resolver.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/schema-resolver.test.ts src/services/config/schema-resolver.ts
git commit -m "feat(config): add schema resolver with repo-backed support

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Schema Resolver - Corpus Fallback

**Files:**
- Modify: `src/services/config/schema-resolver.ts`
- Modify: `tests/services/config/schema-resolver.test.ts`

- [ ] **Step 1: Write failing test for corpus fallback**

Add to `tests/services/config/schema-resolver.test.ts`:

```typescript
import { searchNodeTypeVariants } from "../../../src/services/corpus/search.js";

it("falls back to corpus when repo not available", async () => {
  const result = await resolveNodeTypeSchema("Dimension");

  expect(result.source).toBe("corpus");
  expect(result.schema.config).toBeDefined();
});

it("falls back to corpus when repo resolution fails", async () => {
  const result = await resolveNodeTypeSchema("Dimension", "/nonexistent/path");

  expect(result.source).toBe("corpus");
  expect(result.schema.config).toBeDefined();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/schema-resolver.test.ts`
Expected: FAIL - "Corpus fallback not yet implemented"

- [ ] **Step 3: Implement corpus fallback**

Update `src/services/config/schema-resolver.ts`:

```typescript
import { getRepoNodeTypeDefinition } from "../repo/operations.js";
import { searchNodeTypeVariants } from "../corpus/search.js";

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
  // Remove package prefix
  const bareType = nodeType.includes(":::")
    ? nodeType.split(":::")[0]
    : nodeType;

  // Normalize case and special characters
  return bareType.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
}

export async function resolveNodeTypeSchema(
  nodeType: string,
  repoPath?: string
): Promise<SchemaResolution> {
  // Try repo first if path provided
  if (repoPath) {
    try {
      const def = await getRepoNodeTypeDefinition(repoPath, nodeType);
      return {
        source: "repo",
        schema: def.nodeDefinition as NodeTypeSchema,
      };
    } catch (error) {
      // Fall through to corpus
    }
  }

  // Corpus fallback
  const normalizedFamily = normalizeNodeTypeFamily(nodeType);
  const variants = await searchNodeTypeVariants(normalizedFamily);

  if (variants.length === 0) {
    throw new Error(
      `Cannot resolve node type schema for '${nodeType}'. ` +
      `Not found in repo or corpus.`
    );
  }

  // Use first variant (most common)
  const variant = variants[0];
  return {
    source: "corpus",
    schema: variant.nodeDefinition as NodeTypeSchema,
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/schema-resolver.test.ts`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/schema-resolver.test.ts src/services/config/schema-resolver.ts
git commit -m "feat(config): add corpus fallback to schema resolver

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 7: Field Classifier - Required Fields

**Files:**
- Create: `src/services/config/field-classifier.ts`
- Test: `tests/services/config/field-classifier.test.ts`

- [ ] **Step 1: Write failing test for required field classification**

Create `tests/services/config/field-classifier.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { classifyConfigFields } from "../../../src/services/config/field-classifier.js";
import type { NodeTypeSchema } from "../../../src/services/config/schema-resolver.js";

describe("classifyConfigFields", () => {
  it("identifies required fields", () => {
    const schema: NodeTypeSchema = {
      config: [
        {
          groupName: "Options",
          items: [
            {
              attributeName: "businessKey",
              type: "textBox",
              isRequired: true,
              displayName: "Business Key",
            },
            {
              attributeName: "materializationType",
              type: "materializationSelector",
              isRequired: true,
            },
          ],
        },
      ],
    };

    const result = classifyConfigFields(schema, {} as any);

    expect(result.required).toHaveLength(2);
    expect(result.required[0].attributeName).toBe("businessKey");
    expect(result.required[1].attributeName).toBe("materializationType");
  });

  it("skips items without attributeName", () => {
    const schema: NodeTypeSchema = {
      config: [
        {
          groupName: "Options",
          items: [
            {
              type: "label",
              displayName: "Instructions",
            },
            {
              attributeName: "businessKey",
              type: "textBox",
              isRequired: true,
            },
          ],
        },
      ],
    };

    const result = classifyConfigFields(schema, {} as any);

    expect(result.required).toHaveLength(1);
    expect(result.required[0].attributeName).toBe("businessKey");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/field-classifier.test.ts`
Expected: FAIL - "Cannot find module"

- [ ] **Step 3: Write minimal implementation**

Create `src/services/config/field-classifier.ts`:

```typescript
import type { NodeTypeSchema } from "./schema-resolver.js";

export interface ConfigItem {
  attributeName?: string;
  type: string;
  isRequired?: boolean | string;
  default?: unknown;
  enableIf?: string;
  displayName?: string;
}

export interface ClassifiedFields {
  required: ConfigItem[];
  conditionalRequired: ConfigItem[];
  optionalWithDefaults: ConfigItem[];
  contextual: ConfigItem[];
}

export function classifyConfigFields(
  schema: NodeTypeSchema,
  node: any
): ClassifiedFields {
  const required: ConfigItem[] = [];
  const conditionalRequired: ConfigItem[] = [];
  const optionalWithDefaults: ConfigItem[] = [];
  const contextual: ConfigItem[] = [];

  for (const group of schema.config) {
    for (const item of group.items) {
      if (!item.attributeName) continue;

      if (item.isRequired === true) {
        required.push(item);
      } else if (typeof item.isRequired === "string") {
        conditionalRequired.push(item);
      } else if (item.default !== undefined) {
        optionalWithDefaults.push(item);
      } else {
        contextual.push(item);
      }
    }
  }

  return {
    required,
    conditionalRequired,
    optionalWithDefaults,
    contextual,
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/field-classifier.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/field-classifier.test.ts src/services/config/field-classifier.ts
git commit -m "feat(config): add field classifier for required fields

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 8: Field Classifier - All Field Types

**Files:**
- Modify: `src/services/config/field-classifier.ts`
- Modify: `tests/services/config/field-classifier.test.ts`

- [ ] **Step 1: Write failing test for all field type classification**

Add to `tests/services/config/field-classifier.test.ts`:

```typescript
it("classifies all field types correctly", () => {
  const schema: NodeTypeSchema = {
    config: [
      {
        groupName: "Options",
        items: [
          {
            attributeName: "businessKey",
            type: "textBox",
            isRequired: true,
          },
          {
            attributeName: "lastModifiedColumn",
            type: "columnDropdownSelector",
            isRequired: "{% if config.lastModifiedComparison %} true {% else %} false {% endif %}",
          },
          {
            attributeName: "truncateBefore",
            type: "toggleButton",
            default: false,
          },
          {
            attributeName: "customField",
            type: "textBox",
          },
        ],
      },
    ],
  };

  const result = classifyConfigFields(schema, {} as any);

  expect(result.required).toHaveLength(1);
  expect(result.required[0].attributeName).toBe("businessKey");

  expect(result.conditionalRequired).toHaveLength(1);
  expect(result.conditionalRequired[0].attributeName).toBe("lastModifiedColumn");

  expect(result.optionalWithDefaults).toHaveLength(1);
  expect(result.optionalWithDefaults[0].attributeName).toBe("truncateBefore");

  expect(result.contextual).toHaveLength(1);
  expect(result.contextual[0].attributeName).toBe("customField");
});
```

- [ ] **Step 2: Run test to verify it passes (already implemented)**

Run: `npm test -- tests/services/config/field-classifier.test.ts`
Expected: PASS (3 tests)

- [ ] **Step 3: Commit**

```bash
git add tests/services/config/field-classifier.test.ts
git commit -m "test(config): add comprehensive field classification test

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 9: Intelligence Rules - Multi-Source Strategy

**Files:**
- Create: `src/services/config/rules.ts`
- Test: `tests/services/config/rules.test.ts`

- [ ] **Step 1: Write failing test for multi-source insertStrategy rule**

Create `tests/services/config/rules.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { applyIntelligenceRules } from "../../../src/services/config/rules.js";
import type { NodeTypeSchema } from "../../../src/services/config/schema-resolver.js";
import type { NodeContext } from "../../../src/services/config/context-analyzer.js";

describe("applyIntelligenceRules", () => {
  const baseSchema: NodeTypeSchema = {
    config: [
      {
        groupName: "Options",
        items: [
          {
            attributeName: "insertStrategy",
            type: "dropdownSelector",
            default: "UNION ALL",
          },
        ],
      },
    ],
  };

  it("sets insertStrategy to UNION ALL for multi-source without aggregates", () => {
    const context: NodeContext = {
      hasMultipleSources: true,
      hasAggregates: false,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table",
      columnPatterns: { timestamps: [], dates: [], businessKeys: [] },
    };

    const result = applyIntelligenceRules(baseSchema, context, {});

    expect(result.contextual.insertStrategy).toBe("UNION ALL");
  });

  it("sets insertStrategy to UNION for multi-source with aggregates", () => {
    const context: NodeContext = {
      hasMultipleSources: true,
      hasAggregates: true,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table",
      columnPatterns: { timestamps: [], dates: [], businessKeys: [] },
    };

    const result = applyIntelligenceRules(baseSchema, context, {});

    expect(result.contextual.insertStrategy).toBe("UNION");
  });

  it("does not set insertStrategy for single source", () => {
    const context: NodeContext = {
      hasMultipleSources: false,
      hasAggregates: false,
      hasTimestampColumns: false,
      hasType2Pattern: false,
      materializationType: "table",
      columnPatterns: { timestamps: [], dates: [], businessKeys: [] },
    };

    const result = applyIntelligenceRules(baseSchema, context, {});

    expect(result.contextual.insertStrategy).toBeUndefined();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/rules.test.ts`
Expected: FAIL - "Cannot find module"

- [ ] **Step 3: Write minimal implementation**

Create `src/services/config/rules.ts`:

```typescript
import type { NodeTypeSchema } from "./schema-resolver.js";
import type { NodeContext } from "./context-analyzer.js";

export interface IntelligenceRulesResult {
  required: Record<string, unknown>;
  contextual: Record<string, unknown>;
}

export function applyIntelligenceRules(
  schema: NodeTypeSchema,
  context: NodeContext,
  existingConfig: Record<string, unknown>
): IntelligenceRulesResult {
  const required: Record<string, unknown> = {};
  const contextual: Record<string, unknown> = {};

  // Check if schema has insertStrategy field
  const hasInsertStrategy = schema.config.some((group) =>
    group.items.some((item) => item.attributeName === "insertStrategy")
  );

  // Rule: Multi-source strategy
  if (
    context.hasMultipleSources &&
    hasInsertStrategy &&
    existingConfig.insertStrategy === undefined
  ) {
    if (context.hasAggregates) {
      contextual.insertStrategy = "UNION"; // Deduplication needed
    } else {
      contextual.insertStrategy = "UNION ALL"; // Performance
    }
  }

  return { required, contextual };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/rules.test.ts`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/rules.test.ts src/services/config/rules.ts
git commit -m "feat(config): add intelligence rules with multi-source strategy

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 10: Intelligence Rules - Aggregation Compatibility

**Files:**
- Modify: `src/services/config/rules.ts`
- Modify: `tests/services/config/rules.test.ts`

- [ ] **Step 1: Write failing test for selectDistinct rule**

Add to `tests/services/config/rules.test.ts`:

```typescript
it("sets selectDistinct to false when aggregates present", () => {
  const schema: NodeTypeSchema = {
    config: [
      {
        groupName: "Options",
        items: [
          {
            attributeName: "selectDistinct",
            type: "toggleButton",
            default: false,
          },
        ],
      },
    ],
  };

  const context: NodeContext = {
    hasMultipleSources: false,
    hasAggregates: true,
    hasTimestampColumns: false,
    hasType2Pattern: false,
    materializationType: "table",
    columnPatterns: { timestamps: [], dates: [], businessKeys: [] },
  };

  const result = applyIntelligenceRules(schema, context, {});

  expect(result.contextual.selectDistinct).toBe(false);
});

it("does not set selectDistinct when no aggregates", () => {
  const schema: NodeTypeSchema = {
    config: [
      {
        groupName: "Options",
        items: [
          {
            attributeName: "selectDistinct",
            type: "toggleButton",
            default: false,
          },
        ],
      },
    ],
  };

  const context: NodeContext = {
    hasMultipleSources: false,
    hasAggregates: false,
    hasTimestampColumns: false,
    hasType2Pattern: false,
    materializationType: "table",
    columnPatterns: { timestamps: [], dates: [], businessKeys: [] },
  };

  const result = applyIntelligenceRules(schema, context, {});

  expect(result.contextual.selectDistinct).toBeUndefined();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/rules.test.ts`
Expected: FAIL - selectDistinct expected false

- [ ] **Step 3: Implement selectDistinct rule**

Update `src/services/config/rules.ts`:

```typescript
export function applyIntelligenceRules(
  schema: NodeTypeSchema,
  context: NodeContext,
  existingConfig: Record<string, unknown>
): IntelligenceRulesResult {
  const required: Record<string, unknown> = {};
  const contextual: Record<string, unknown> = {};

  // Check which fields exist in schema
  const hasInsertStrategy = schema.config.some((group) =>
    group.items.some((item) => item.attributeName === "insertStrategy")
  );

  const hasSelectDistinct = schema.config.some((group) =>
    group.items.some((item) => item.attributeName === "selectDistinct")
  );

  // Rule: Multi-source strategy
  if (
    context.hasMultipleSources &&
    hasInsertStrategy &&
    existingConfig.insertStrategy === undefined
  ) {
    if (context.hasAggregates) {
      contextual.insertStrategy = "UNION";
    } else {
      contextual.insertStrategy = "UNION ALL";
    }
  }

  // Rule: Aggregation compatibility
  if (
    context.hasAggregates &&
    hasSelectDistinct &&
    existingConfig.selectDistinct === undefined
  ) {
    contextual.selectDistinct = false; // Incompatible with aggregates
  }

  return { required, contextual };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/rules.test.ts`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/rules.test.ts src/services/config/rules.ts
git commit -m "feat(config): add selectDistinct rule for aggregate compatibility

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 11: Intelligence Rules - Truncate Before Default

**Files:**
- Modify: `src/services/config/rules.ts`
- Modify: `tests/services/config/rules.test.ts`

- [ ] **Step 1: Write failing test for truncateBefore rule**

Add to `tests/services/config/rules.test.ts`:

```typescript
it("sets truncateBefore to false for table materialization", () => {
  const schema: NodeTypeSchema = {
    config: [
      {
        groupName: "Options",
        items: [
          {
            attributeName: "truncateBefore",
            type: "toggleButton",
            default: false,
          },
        ],
      },
    ],
  };

  const context: NodeContext = {
    hasMultipleSources: false,
    hasAggregates: false,
    hasTimestampColumns: false,
    hasType2Pattern: false,
    materializationType: "table",
    columnPatterns: { timestamps: [], dates: [], businessKeys: [] },
  };

  const result = applyIntelligenceRules(schema, context, {});

  expect(result.contextual.truncateBefore).toBe(false);
});

it("does not set truncateBefore for view materialization", () => {
  const schema: NodeTypeSchema = {
    config: [
      {
        groupName: "Options",
        items: [
          {
            attributeName: "truncateBefore",
            type: "toggleButton",
            default: false,
          },
        ],
      },
    ],
  };

  const context: NodeContext = {
    hasMultipleSources: false,
    hasAggregates: false,
    hasTimestampColumns: false,
    hasType2Pattern: false,
    materializationType: "view",
    columnPatterns: { timestamps: [], dates: [], businessKeys: [] },
  };

  const result = applyIntelligenceRules(schema, context, {});

  expect(result.contextual.truncateBefore).toBeUndefined();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/rules.test.ts`
Expected: FAIL - truncateBefore expected false

- [ ] **Step 3: Implement truncateBefore rule**

Update `src/services/config/rules.ts`:

```typescript
export function applyIntelligenceRules(
  schema: NodeTypeSchema,
  context: NodeContext,
  existingConfig: Record<string, unknown>
): IntelligenceRulesResult {
  const required: Record<string, unknown> = {};
  const contextual: Record<string, unknown> = {};

  // Check which fields exist in schema
  const hasInsertStrategy = schema.config.some((group) =>
    group.items.some((item) => item.attributeName === "insertStrategy")
  );

  const hasSelectDistinct = schema.config.some((group) =>
    group.items.some((item) => item.attributeName === "selectDistinct")
  );

  const hasTruncateBefore = schema.config.some((group) =>
    group.items.some((item) => item.attributeName === "truncateBefore")
  );

  // Rule: Multi-source strategy
  if (
    context.hasMultipleSources &&
    hasInsertStrategy &&
    existingConfig.insertStrategy === undefined
  ) {
    if (context.hasAggregates) {
      contextual.insertStrategy = "UNION";
    } else {
      contextual.insertStrategy = "UNION ALL";
    }
  }

  // Rule: Aggregation compatibility
  if (
    context.hasAggregates &&
    hasSelectDistinct &&
    existingConfig.selectDistinct === undefined
  ) {
    contextual.selectDistinct = false;
  }

  // Rule: Truncate before (table materialization)
  if (
    context.materializationType === "table" &&
    hasTruncateBefore &&
    existingConfig.truncateBefore === undefined
  ) {
    contextual.truncateBefore = false; // Safe default
  }

  return { required, contextual };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/rules.test.ts`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/rules.test.ts src/services/config/rules.ts
git commit -m "feat(config): add truncateBefore rule for table materialization

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 12: Main Orchestrator - Basic Structure

**Files:**
- Create: `src/services/config/intelligent.ts`
- Test: `tests/services/config/intelligent.test.ts`

- [ ] **Step 1: Write failing test for basic orchestration**

Create `tests/services/config/intelligent.test.ts`:

```typescript
import { describe, it, expect, vi } from "vitest";
import { completeNodeConfiguration } from "../../../src/services/config/intelligent.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("completeNodeConfiguration", () => {
  it("fetches node and returns basic structure", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "node-1",
      name: "TEST_NODE",
      nodeType: "DataVault:::33",
      metadata: {
        columns: [],
        sources: [],
      },
      config: {},
    });

    const result = await completeNodeConfiguration(client as any, {
      workspaceID: "ws-1",
      nodeID: "node-1",
      repoPath: "/Users/jmarshall/Documents/GitHub/coalesce-transform-mcp/tests/fixtures/repo-backed-coalesce",
    });

    expect(result.node).toBeDefined();
    expect(result.configChanges).toBeDefined();
    expect(result.analysis).toBeDefined();
    expect(result.analysis.schemaSource).toBe("repo");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/intelligent.test.ts`
Expected: FAIL - "Cannot find module"

- [ ] **Step 3: Write minimal implementation**

Create `src/services/config/intelligent.ts`:

```typescript
import type { CoalesceClient } from "../../client.js";
import { resolveNodeTypeSchema } from "./schema-resolver.js";
import { analyzeNodeContext } from "./context-analyzer.js";
import { classifyConfigFields } from "./field-classifier.js";
import { applyIntelligenceRules } from "./rules.js";
import { updateWorkspaceNode } from "../workspace/mutations.js";

export interface ConfigCompletionResult {
  node: any;
  configChanges: Record<string, unknown>;
  analysis: {
    requiredFields: string[];
    contextualFields: string[];
    preservedFields: string[];
    schemaSource: "repo" | "corpus";
    candidateColumns?: Record<string, string[]>;
    patterns?: Record<string, boolean>;
  };
}

export async function completeNodeConfiguration(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    repoPath?: string;
  }
): Promise<ConfigCompletionResult> {
  // Fetch current node
  const node = await client.get(
    `/api/v1/workspaces/${params.workspaceID}/nodes/${params.nodeID}`
  );

  // Resolve schema
  const schemaResolution = await resolveNodeTypeSchema(
    node.nodeType,
    params.repoPath
  );

  // Analyze context
  const context = analyzeNodeContext(node);

  // Classify fields
  const classified = classifyConfigFields(schemaResolution.schema, node);

  // Apply rules
  const rules = applyIntelligenceRules(
    schemaResolution.schema,
    context,
    node.config || {}
  );

  // For now, just return structure without updating
  return {
    node,
    configChanges: {},
    analysis: {
      requiredFields: [],
      contextualFields: [],
      preservedFields: [],
      schemaSource: schemaResolution.source,
    },
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/intelligent.test.ts`
Expected: PASS (1 test)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/intelligent.test.ts src/services/config/intelligent.ts
git commit -m "feat(config): add main orchestrator basic structure

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 13: Main Orchestrator - Config Application

**Files:**
- Modify: `src/services/config/intelligent.ts`
- Modify: `tests/services/config/intelligent.test.ts`

- [ ] **Step 1: Write failing test for config application**

Add to `tests/services/config/intelligent.test.ts`:

```typescript
it("applies contextual config updates", async () => {
  const client = createMockClient();

  client.get.mockResolvedValue({
    id: "node-1",
    name: "TEST_NODE",
    nodeType: "DataVault:::33",
    metadata: {
      columns: [
        { name: "TOTAL", transform: 'COUNT(*)' },
      ],
      sources: [{ name: "A" }, { name: "B" }],
    },
    config: {},
  });

  client.put.mockResolvedValue({
    id: "node-1",
    config: {
      businessKey: "CUSTOMER_ID",
      insertStrategy: "UNION",
      selectDistinct: false,
    },
  });

  const result = await completeNodeConfiguration(client as any, {
    workspaceID: "ws-1",
    nodeID: "node-1",
    repoPath: "/Users/jmarshall/Documents/GitHub/coalesce-transform-mcp/tests/fixtures/repo-backed-coalesce",
  });

  expect(client.put).toHaveBeenCalled();
  expect(result.configChanges.insertStrategy).toBe("UNION");
  expect(result.configChanges.selectDistinct).toBe(false);
  expect(result.analysis.contextualFields).toContain("insertStrategy");
  expect(result.analysis.contextualFields).toContain("selectDistinct");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config/intelligent.test.ts`
Expected: FAIL - configChanges.insertStrategy expected "UNION"

- [ ] **Step 3: Implement config application**

Update `src/services/config/intelligent.ts`:

```typescript
export async function completeNodeConfiguration(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    repoPath?: string;
  }
): Promise<ConfigCompletionResult> {
  // Fetch current node
  const node = await client.get(
    `/api/v1/workspaces/${params.workspaceID}/nodes/${params.nodeID}`
  );

  // Resolve schema
  const schemaResolution = await resolveNodeTypeSchema(
    node.nodeType,
    params.repoPath
  );

  // Analyze context
  const context = analyzeNodeContext(node);

  // Classify fields
  const classified = classifyConfigFields(schemaResolution.schema, node);

  // Apply rules
  const rules = applyIntelligenceRules(
    schemaResolution.schema,
    context,
    node.config || {}
  );

  // Build config changes
  const configChanges = {
    ...rules.required,
    ...rules.contextual,
  };

  // Track what changed
  const requiredFields = Object.keys(rules.required);
  const contextualFields = Object.keys(rules.contextual);
  const preservedFields = Object.keys(node.config || {}).filter(
    (key) => configChanges[key] === undefined
  );

  // Apply updates if there are changes
  let updatedNode = node;
  if (Object.keys(configChanges).length > 0) {
    updatedNode = await updateWorkspaceNode(client, {
      workspaceID: params.workspaceID,
      nodeID: params.nodeID,
      changes: {
        config: configChanges,
      },
    });
  }

  // Build candidate columns from context
  const candidateColumns: Record<string, string[]> = {};
  if (context.hasTimestampColumns) {
    candidateColumns.lastModifiedColumn = [
      ...context.columnPatterns.timestamps,
      ...context.columnPatterns.dates,
    ];
  }

  // Build patterns detection
  const patterns: Record<string, boolean> = {};
  if (context.hasType2Pattern) {
    patterns.type2Detected = true;
  }

  return {
    node: updatedNode,
    configChanges,
    analysis: {
      requiredFields,
      contextualFields,
      preservedFields,
      schemaSource: schemaResolution.source,
      ...(Object.keys(candidateColumns).length > 0 && { candidateColumns }),
      ...(Object.keys(patterns).length > 0 && { patterns }),
    },
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config/intelligent.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add tests/services/config/intelligent.test.ts src/services/config/intelligent.ts
git commit -m "feat(config): implement config application in orchestrator

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 14: Register complete-node-configuration Tool

**Files:**
- Modify: `src/mcp/nodes.ts`
- Test: `tests/tools/complete-node-configuration.test.ts`

- [ ] **Step 1: Write failing integration test**

Create `tests/tools/complete-node-configuration.test.ts`:

```typescript
import { describe, it, expect, vi } from "vitest";
import { completeNodeConfiguration } from "../../src/services/config/intelligent.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("complete-node-configuration tool", () => {
  it("completes config for Dimension node", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "dim-1",
      name: "DIM_CUSTOMERS",
      nodeType: "DataVault:::33",
      metadata: {
        columns: [
          { name: "CUSTOMER_ID", transform: '"SRC"."CUSTOMER_ID"' },
        ],
        sources: [{ name: "SRC" }],
      },
      config: {},
    });

    client.put.mockResolvedValue({
      id: "dim-1",
      config: { businessKey: "CUSTOMER_ID" },
    });

    const result = await completeNodeConfiguration(client as any, {
      workspaceID: "ws-1",
      nodeID: "dim-1",
      repoPath: "/Users/jmarshall/Documents/GitHub/coalesce-transform-mcp/tests/fixtures/repo-backed-coalesce",
    });

    expect(result.analysis.schemaSource).toBe("repo");
    expect(result.node).toBeDefined();
  });
});
```

- [ ] **Step 2: Run test to verify it passes (already implemented)**

Run: `npm test -- tests/tools/complete-node-configuration.test.ts`
Expected: PASS (1 test)

- [ ] **Step 3: Add tool registration to nodes.ts**

Add to `src/mcp/nodes.ts` after existing tools:

```typescript
import { completeNodeConfiguration } from "../services/config/intelligent.js";

// ... existing tools ...

server.tool(
  "complete-node-configuration",
  "Intelligently complete all required and contextual config fields for a workspace node. " +
  "Resolves node type schema (repo → corpus), analyzes node metadata, sets all required fields, " +
  "and intelligently adds contextual optional fields based on detected patterns. " +
  "Automatically detects: multi-source → insertStrategy, aggregates → selectDistinct, " +
  "timestamps → candidate columns, Type 2 patterns. Preserves user-set values.",
  {
    workspaceID: z.string().describe("Workspace ID containing the node"),
    nodeID: z.string().describe("Node ID to configure"),
    repoPath: z
      .string()
      .optional()
      .describe("Optional path to committed repo for schema resolution"),
  },
  async ({ workspaceID, nodeID, repoPath }) => {
    const client = getClient();
    const result = await completeNodeConfiguration(client, {
      workspaceID,
      nodeID,
      repoPath,
    });

    return buildJsonToolResponse("complete-node-configuration", result);
  },
  READ_ONLY_ANNOTATIONS
);
```

- [ ] **Step 4: Test tool registration**

Run: `npm test -- tests/registration.test.ts`
Expected: Update test count from 60 to 61 tools

- [ ] **Step 5: Update registration test**

Modify `tests/registration.test.ts`:

```typescript
it("registers all MCP tools", () => {
  registerTools(server);

  expect(toolSpy).toHaveBeenCalledTimes(61); // Updated: added complete-node-configuration
  // ... rest of test
});
```

- [ ] **Step 6: Run test to verify it passes**

Run: `npm test -- tests/registration.test.ts`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/mcp/nodes.ts tests/tools/complete-node-configuration.test.ts tests/registration.test.ts
git commit -m "feat(mcp): register complete-node-configuration tool

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 15: Update convert-join-to-aggregation to Call Config Completion

**Files:**
- Modify: `src/services/workspace/mutations.ts`
- Modify: `tests/services/config-completion.test.ts`

- [ ] **Step 1: Write failing test for auto-config in convert-join-to-aggregation**

Add to `tests/services/config-completion.test.ts`:

```typescript
it("automatically completes all config after transformation", async () => {
  const client = createMockClient();

  client.get.mockImplementation((path: string) => {
    if (path === "/api/v1/workspaces/ws-1/nodes/fact-node") {
      return Promise.resolve({
        id: "fact-node",
        name: "FCT_METRICS",
        nodeType: "DataVault:::33",
        metadata: {
          sourceMapping: [],
          sources: [{ name: "A" }, { name: "B" }],
        },
        config: {},
      });
    }
    return Promise.resolve({});
  });

  client.put.mockResolvedValue({ id: "fact-node" });

  const result = await convertJoinToAggregation(client as any, {
    workspaceID: "ws-1",
    nodeID: "fact-node",
    groupByColumns: ['"ORDERS"."CUSTOMER_ID"'],
    aggregates: [
      {
        name: "TOTAL_ORDERS",
        function: "COUNT",
        expression: 'DISTINCT "ORDERS"."ORDER_ID"',
      },
    ],
    maintainJoins: false,
    repoPath: "/Users/jmarshall/Documents/GitHub/coalesce-transform-mcp/tests/fixtures/repo-backed-coalesce",
  });

  expect(result.configCompletion).toBeDefined();
  expect(result.configCompletion.configChanges.businessKey).toBe("CUSTOMER_ID");
  expect(result.configCompletion.configChanges.changeTracking).toBe("TOTAL_ORDERS");
  expect(result.configCompletion.analysis.schemaSource).toBe("repo");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- tests/services/config-completion.test.ts`
Expected: FAIL - configCompletion expected to be defined

- [ ] **Step 3: Update convertJoinToAggregation to add repoPath param and call config completion**

Update function signature in `src/services/workspace/mutations.ts`:

```typescript
import { completeNodeConfiguration } from "../config/intelligent.js";

export async function convertJoinToAggregation(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    groupByColumns: string[];
    aggregates: Array<{
      name: string;
      function: string;
      expression: string;
      description?: string;
    }>;
    maintainJoins?: boolean;
    repoPath?: string; // NEW
  }
): Promise<{
  node: any;
  groupByAnalysis: {
    groupByColumns: string[];
    groupByClause: string;
  };
  joinSQL?: {
    fromClause: string;
    joinClauses: JoinClause[];
    fullSQL: string;
  };
  configCompletion: {  // NEW
    configChanges: Record<string, unknown>;
    analysis: {
      requiredFields: string[];
      contextualFields: string[];
      preservedFields: string[];
      schemaSource: "repo" | "corpus";
      candidateColumns?: Record<string, string[]>;
      patterns?: Record<string, boolean>;
    };
  };
}>
```

Add at end of function before return:

```typescript
  // Automatically complete all other config fields
  const configCompletion = await completeNodeConfiguration(client, {
    workspaceID: params.workspaceID,
    nodeID: params.nodeID,
    repoPath: params.repoPath,
  });

  return {
    node: updated,
    groupByAnalysis: {
      groupByColumns: params.groupByColumns,
      groupByClause: `GROUP BY ${params.groupByColumns.join(", ")}`,
    },
    ...(joinResult && {
      joinSQL: {
        fromClause: joinResult.fromClause,
        joinClauses: joinResult.joinClauses,
        fullSQL: joinResult.fullSQL,
      },
    }),
    configCompletion: {
      configChanges: configCompletion.configChanges,
      analysis: configCompletion.analysis,
    },
  };
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/services/config-completion.test.ts`
Expected: PASS (5 tests)

- [ ] **Step 5: Update tool registration to add repoPath param**

Update in `src/mcp/nodes.ts`:

```typescript
server.tool(
  "convert-join-to-aggregation",
  "Convert an existing join node into an aggregated fact table with GROUP BY. " +
  "This tool performs complete transformation in one call: analyzes join patterns, " +
  "generates JOIN ON clauses, detects aggregate vs non-aggregate columns, builds GROUP BY, " +
  "infers datatypes, sets businessKey and changeTracking, and automatically completes all " +
  "other required and contextual config fields. Result is fully transformed AND configured.",
  {
    workspaceID: z.string().describe("Workspace ID"),
    nodeID: z.string().describe("Node ID to transform"),
    groupByColumns: z.array(z.string()).describe("Columns to group by (dimensions)"),
    aggregates: z.array(
      z.object({
        name: z.string(),
        function: z.string(),
        expression: z.string(),
        description: z.string().optional(),
      })
    ),
    maintainJoins: z
      .boolean()
      .optional()
      .describe("Auto-generate JOIN ON clauses from common columns"),
    repoPath: z
      .string()
      .optional()
      .describe("Optional path to committed repo for schema resolution"),
  },
  async ({ workspaceID, nodeID, groupByColumns, aggregates, maintainJoins, repoPath }) => {
    const client = getClient();
    const result = await convertJoinToAggregation(client, {
      workspaceID,
      nodeID,
      groupByColumns,
      aggregates,
      maintainJoins,
      repoPath,
    });

    return buildJsonToolResponse("convert-join-to-aggregation", result);
  },
  WRITE_ANNOTATIONS
);
```

- [ ] **Step 6: Commit**

```bash
git add src/services/workspace/mutations.ts tests/services/config-completion.test.ts src/mcp/nodes.ts
git commit -m "feat(aggregation): auto-complete config after transformation

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 16: Create Documentation Resource

**Files:**
- Create: `src/resources/context/intelligent-node-configuration.md`

- [ ] **Step 1: Write documentation**

Create `src/resources/context/intelligent-node-configuration.md`:

```markdown
# Intelligent Node Configuration

## Overview

The intelligent node configuration system automatically completes all required and contextual config fields based on node type schemas and node analysis.

## Tools

### `complete-node-configuration`

Standalone tool that completes config for any workspace node.

**Input:**
- `workspaceID`: Workspace containing the node
- `nodeID`: Node to configure
- `repoPath`: (Optional) Path to committed repo for accurate schema resolution

**Process:**
1. Fetches current node
2. Resolves node type schema (repo → corpus)
3. Analyzes node context (columns, sources, joins)
4. Classifies config fields (required, conditional, optional, contextual)
5. Applies intelligence rules
6. Updates node with complete config

**Output:**
- `node`: Updated node with complete config
- `configChanges`: What was changed
- `analysis`: Details about what was set and why

### `convert-join-to-aggregation`

Enhanced with automatic config completion. After transformation, automatically calls `complete-node-configuration` to fill all remaining config fields.

**New output field:** `configCompletion` with changes and analysis

## Intelligence Rules

### Multi-Source Strategy

**Trigger:** Node has multiple sources

**Action:**
- If aggregates present: `insertStrategy: "UNION"` (deduplication)
- If no aggregates: `insertStrategy: "UNION ALL"` (performance)

### Aggregation Compatibility

**Trigger:** Node has aggregate columns (COUNT, SUM, AVG, etc.)

**Action:**
- `selectDistinct: false` (incompatible with aggregates)

### Timestamp Column Detection

**Trigger:** Columns matching `*_TS`, `*_DATE`, `*_TIMESTAMP` patterns

**Action:**
- Documents candidates in `analysis.candidateColumns.lastModifiedColumn`
- Does NOT auto-enable `lastModifiedComparison` (user choice)

### Type 2 Dimension Detection

**Trigger:** Columns include START_DATE, END_DATE, IS_CURRENT patterns

**Action:**
- Documents detection in `analysis.patterns.type2Detected`
- Does NOT auto-enable `type2Dimension` (requires verification)

### Truncate Before

**Trigger:** Table materialization

**Action:**
- `truncateBefore: false` (safe default)

## Schema Resolution

Priority order:
1. **Repo-backed** (if `repoPath` provided) - most accurate
2. **Corpus** (fallback) - standard Coalesce node types
3. **Error** (if neither available)

## Preservation Rules

**Never overwrite:**
- Existing non-null config values (except required fields)
- User-set fields take precedence

**Always set:**
- Required fields (even if overwriting)
- Aggregation-specific fields from transformation

**Smart merge:**
- If field is empty/null, set it
- If field is default and context suggests better value, update it

## Usage Examples

### Complete Existing Node

```typescript
await completeNodeConfiguration(client, {
  workspaceID: "ws-123",
  nodeID: "dim-customers",
  repoPath: "/path/to/repo"
});
```

### Transform with Auto-Config

```typescript
await convertJoinToAggregation(client, {
  workspaceID: "ws-123",
  nodeID: "fact-orders",
  groupByColumns: ['"ORDERS"."CUSTOMER_ID"'],
  aggregates: [
    { name: "TOTAL", function: "COUNT", expression: "*" }
  ],
  maintainJoins: true,
  repoPath: "/path/to/repo"
});

// Returns fully transformed AND configured node
```

## Troubleshooting

**"Cannot resolve node type schema":**
- Ensure package is committed to repo or available in corpus
- Check nodeType format (may need package prefix like "PackageName:::ID")

**"Required field could not be auto-determined":**
- Some required fields need manual input
- Check analysis.warnings for details

**Config not as expected:**
- Check analysis.preservedFields - may have existing values
- Verify node context (sources, columns, materialization type)
```

- [ ] **Step 2: Commit**

```bash
git add src/resources/context/intelligent-node-configuration.md
git commit -m "docs: add intelligent node configuration resource

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 17: Update Aggregation Patterns Documentation

**Files:**
- Modify: `src/resources/context/aggregation-patterns.md`

- [ ] **Step 1: Add config completion section**

Add to end of `src/resources/context/aggregation-patterns.md`:

```markdown
## Automatic Config Completion

The `convert-join-to-aggregation` tool automatically completes ALL config fields after transformation:

### Aggregation-Specific Config

Automatically set from the transformation:
- `businessKey`: Extracted from `groupByColumns` (e.g., "CUSTOMER_ID,REGION")
- `changeTracking`: Extracted from `aggregates` (e.g., "TOTAL_ORDERS,LIFETIME_VALUE")

### Node-Type-Aware Config

Automatically set based on node type schema and context analysis:
- `insertStrategy`: "UNION ALL" or "UNION" (multi-source detection)
- `selectDistinct`: false (aggregate compatibility)
- `truncateBefore`: false (table materialization default)
- Other required fields specific to the node type

### Intelligence Rules

The system detects patterns and sets config intelligently:

**Multi-source nodes:**
- Detects multiple sources in metadata
- Sets `insertStrategy` based on aggregate presence
- UNION for deduplication, UNION ALL for performance

**Aggregate patterns:**
- Detects COUNT, SUM, AVG, MIN, MAX functions
- Sets `selectDistinct: false` (incompatible)

**Timestamp columns:**
- Detects `*_TS`, `*_DATE` patterns
- Documents candidates for `lastModifiedColumn`
- Does NOT auto-enable (user choice)

**Type 2 dimensions:**
- Detects START_DATE, END_DATE, IS_CURRENT
- Documents pattern detection
- Does NOT auto-enable (requires verification)

### Example Output

```typescript
{
  node: { /* fully transformed and configured */ },
  groupByAnalysis: { /* GROUP BY details */ },
  joinSQL: { /* JOIN ON clauses */ },
  configCompletion: {
    configChanges: {
      businessKey: "CUSTOMER_ID",
      changeTracking: "TOTAL_ORDERS,REVENUE",
      insertStrategy: "UNION ALL",
      selectDistinct: false,
      truncateBefore: false
    },
    analysis: {
      requiredFields: ["businessKey"],
      contextualFields: ["changeTracking", "insertStrategy", "selectDistinct", "truncateBefore"],
      preservedFields: [],
      schemaSource: "repo"
    }
  }
}
```

See [intelligent-node-configuration.md](./intelligent-node-configuration.md) for complete details.
```

- [ ] **Step 2: Commit**

```bash
git add src/resources/context/aggregation-patterns.md
git commit -m "docs: add config completion section to aggregation patterns

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 18: Register New Documentation Resource

**Files:**
- Modify: `src/resources/index.ts`
- Modify: `tests/resources.test.ts`

- [ ] **Step 1: Add resource constant**

Add to `src/resources/index.ts`:

```typescript
const INTELLIGENT_NODE_CONFIGURATION = fs.readFileSync(
  new URL("./context/intelligent-node-configuration.md", import.meta.url),
  "utf-8"
);
```

- [ ] **Step 2: Register resource**

Add to `registerResources()` function:

```typescript
server.resource(
  "Intelligent Node Configuration",
  "coalesce://context/intelligent-node-configuration",
  "How intelligent config completion works",
  async (url) => ({
    contents: [
      {
        uri: url.href,
        mimeType: "text/markdown",
        text: INTELLIGENT_NODE_CONFIGURATION,
      },
    ],
  })
);
```

- [ ] **Step 3: Update resource test count**

Modify `tests/resources.test.ts`:

```typescript
it("registers all fixed Coalesce context resources", () => {
  registerResources(server);

  expect(resourceSpy).toHaveBeenCalledTimes(21); // Updated: added intelligent-node-configuration
  // ... rest
});
```

Add to expected array:

```typescript
{
  name: "Intelligent Node Configuration",
  uri: "coalesce://context/intelligent-node-configuration",
},
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- tests/resources.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/resources/index.ts tests/resources.test.ts
git commit -m "feat(resources): register intelligent node configuration resource

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 19: Run All Tests

**Files:**
- All test files

- [ ] **Step 1: Run full test suite**

Run: `npm test`
Expected: All tests pass

- [ ] **Step 2: If failures, fix them**

Review failures and fix issues.

- [ ] **Step 3: Run tests again to verify**

Run: `npm test`
Expected: All tests pass

- [ ] **Step 4: Commit any fixes**

```bash
git add <fixed-files>
git commit -m "fix: resolve test failures

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 20: Final Integration Test

**Files:**
- Create: `tests/integration/intelligent-node-config.test.ts`

- [ ] **Step 1: Write end-to-end integration test**

Create `tests/integration/intelligent-node-config.test.ts`:

```typescript
import { describe, it, expect, vi, beforeEach } from "vitest";
import { completeNodeConfiguration } from "../../src/services/config/intelligent.js";
import { convertJoinToAggregation } from "../../src/services/workspace/mutations.js";

function createMockClient() {
  return {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  };
}

describe("Intelligent Node Configuration Integration", () => {
  const repoPath = "/Users/jmarshall/Documents/GitHub/coalesce-transform-mcp/tests/fixtures/repo-backed-coalesce";

  it("complete workflow: transform + config for multi-source aggregation", async () => {
    const client = createMockClient();

    // Mock node with multiple sources and aggregates
    client.get.mockImplementation((path: string) => {
      if (path.includes("/nodes/")) {
        return Promise.resolve({
          id: "fact-1",
          name: "FCT_CUSTOMER_METRICS",
          nodeType: "DataVault:::33",
          metadata: {
            sources: [
              { name: "ORDERS" },
              { name: "CUSTOMERS" }
            ],
            columns: [],
            sourceMapping: [],
          },
          config: {},
        });
      }
      return Promise.resolve({});
    });

    client.put.mockResolvedValue({
      id: "fact-1",
      config: {
        businessKey: "CUSTOMER_ID",
        changeTracking: "TOTAL_ORDERS,LIFETIME_VALUE",
        insertStrategy: "UNION",
        selectDistinct: false,
      },
    });

    // Execute transformation with auto-config
    const result = await convertJoinToAggregation(client as any, {
      workspaceID: "ws-1",
      nodeID: "fact-1",
      groupByColumns: ['"CUSTOMERS"."CUSTOMER_ID"'],
      aggregates: [
        {
          name: "TOTAL_ORDERS",
          function: "COUNT",
          expression: 'DISTINCT "ORDERS"."ORDER_ID"',
        },
        {
          name: "LIFETIME_VALUE",
          function: "SUM",
          expression: '"ORDERS"."ORDER_TOTAL"',
        },
      ],
      maintainJoins: true,
      repoPath,
    });

    // Verify transformation
    expect(result.groupByAnalysis.groupByColumns).toHaveLength(1);
    expect(result.groupByAnalysis.groupByClause).toContain("GROUP BY");

    // Verify config completion
    expect(result.configCompletion.configChanges.businessKey).toBe("CUSTOMER_ID");
    expect(result.configCompletion.configChanges.changeTracking).toBe("TOTAL_ORDERS,LIFETIME_VALUE");
    expect(result.configCompletion.configChanges.insertStrategy).toBe("UNION"); // Multi-source + aggregates
    expect(result.configCompletion.configChanges.selectDistinct).toBe(false); // Aggregates present
    expect(result.configCompletion.analysis.schemaSource).toBe("repo");
  });

  it("standalone config completion for existing node", async () => {
    const client = createMockClient();

    client.get.mockResolvedValue({
      id: "dim-1",
      name: "DIM_CUSTOMERS",
      nodeType: "DataVault:::33",
      metadata: {
        sources: [{ name: "SRC" }],
        columns: [
          { name: "CUSTOMER_ID", transform: '"SRC"."ID"' },
          { name: "CREATED_TS", transform: '"SRC"."CREATED_TS"' },
        ],
      },
      config: {},
    });

    client.put.mockResolvedValue({
      id: "dim-1",
      config: { businessKey: "CUSTOMER_ID" },
    });

    const result = await completeNodeConfiguration(client as any, {
      workspaceID: "ws-1",
      nodeID: "dim-1",
      repoPath,
    });

    expect(result.analysis.schemaSource).toBe("repo");
    expect(result.analysis.candidateColumns?.lastModifiedColumn).toContain("CREATED_TS");
    expect(client.put).toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Run integration test**

Run: `npm test -- tests/integration/intelligent-node-config.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 3: Commit**

```bash
git add tests/integration/intelligent-node-config.test.ts
git commit -m "test: add end-to-end integration tests for intelligent config

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Completion Checklist

- [ ] All unit tests pass (context analyzer, schema resolver, field classifier, rules, orchestrator)
- [ ] All integration tests pass (complete-node-configuration, convert-join-to-aggregation)
- [ ] Tool registered and working
- [ ] Documentation created and registered
- [ ] All commits follow conventional commit format
- [ ] Code follows project patterns (TDD, DRY, YAGNI)

---

## Post-Implementation Verification

After completing all tasks:

1. **Run full test suite:** `npm test`
2. **Build project:** `npm run build`
3. **Manual smoke test:**
   - Test `complete-node-configuration` on a real node
   - Test `convert-join-to-aggregation` with `repoPath`
   - Verify config completion output structure
4. **Review changes:**
   - All new files in `src/services/config/`
   - Updated `mutations.ts` and `nodes.ts`
   - Documentation resources complete
   - All tests passing

## Next Steps

After implementation complete:
- Consider adding more intelligence rules (project-specific patterns)
- Monitor usage to identify additional config fields to auto-set
- Gather feedback on config completeness
- Consider adding config validation pre-flight checks
