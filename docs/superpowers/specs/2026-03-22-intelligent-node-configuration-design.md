# Intelligent Node Configuration Design

**Date:** 2026-03-22
**Status:** Approved
**Author:** Claude Code

## Overview

This design adds intelligent, node-type-aware configuration completion to the Coalesce Transform MCP. It introduces automatic detection and population of required and contextual config fields based on node type schemas and node context analysis.

## Problem Statement

Currently, `convert-join-to-aggregation` only sets `businessKey` and `changeTracking` config fields. Different node types have different config schemas with:
- Required fields that must be set
- Conditional required fields (based on other config)
- Optional fields with defaults
- Contextual optional fields (relevant based on node content)

Users must manually set these fields, leading to:
- Incomplete node configurations
- Validation errors
- Trial-and-error to determine required fields
- Inconsistent config across similar nodes

## Goals

1. **Intelligent Config Completion:** Automatically set all required config fields based on node type schema
2. **Contextual Intelligence:** Detect and set relevant optional fields based on node analysis
3. **Reusable Tool:** Provide standalone config completion tool for any node
4. **One-Stop Transformation:** Enhanced `convert-join-to-aggregation` with automatic config completion
5. **Schema Resolution:** Support both repo-backed and corpus-backed node type definitions

## Non-Goals

- Setting every possible config field (only required + contextual)
- Overwriting user-set config values
- Supporting custom/unknown node types without schema
- SQL override field configuration (project policy: never set override fields)

## Architecture

### Two Tools, One Flow

#### Tool 1: `complete-node-configuration` (New)

**Purpose:** Standalone intelligent config completion for any node

**Input:**
```typescript
{
  workspaceID: string;        // Required: workspace containing the node
  nodeID: string;             // Required: node to configure
  repoPath?: string;          // Optional: path to committed repo for schema resolution
}
```

**Process:**
1. Read current node from workspace API
2. Resolve node type schema (repo → corpus → error)
3. Analyze node context (columns, sources, joins, existing config)
4. Classify config fields (required, conditional, optional, contextual)
5. Determine field values based on context and rules
6. Build config updates (preserving existing values)
7. Apply updates via `updateWorkspaceNode`

**Output:**
```typescript
{
  node: WorkspaceNode;              // Updated node with complete config
  configChanges: {                  // What was changed
    [fieldName: string]: unknown;
  };
  analysis: {
    requiredFields: string[];       // Required fields that were set
    contextualFields: string[];     // Optional fields intelligently added
    preservedFields: string[];      // Existing fields preserved
    schemaSource: 'repo' | 'corpus'; // Where schema came from
  };
}
```

#### Tool 2: `convert-join-to-aggregation` (Enhanced)

**Current behavior:**
- Transforms joins to aggregations
- Generates JOIN ON clauses
- Detects GROUP BY requirements
- Infers datatypes
- Sets `businessKey` and `changeTracking`

**New behavior:**
- All current behavior PLUS
- **Automatically calls** `complete-node-configuration` at end
- Returns combined result with config completion analysis

**Updated Output:**
```typescript
{
  node: WorkspaceNode;               // Fully transformed AND configured
  groupByAnalysis: {
    groupByColumns: string[];
    groupByClause: string;
  };
  joinSQL?: {
    fromClause: string;
    joinClauses: JoinClause[];
    fullSQL: string;
  };
  configCompletion: {                // NEW: from auto-config step
    configChanges: Record<string, unknown>;
    analysis: {
      requiredFields: string[];
      contextualFields: string[];
      preservedFields: string[];
      schemaSource: 'repo' | 'corpus';
    };
  };
}
```

### Data Flow

```
User calls convert-join-to-aggregation
  ↓
Transform logic (existing: JOIN ON, columns, datatypes)
  ↓
Set aggregation config (existing: businessKey, changeTracking)
  ↓
Auto-call complete-node-configuration (NEW)
  ↓
Config intelligence (NEW: required + contextual fields)
  ↓
Return complete node
```

## Node Type Schema Resolution

### Priority Order

1. **Repo-backed definition** (if `repoPath` provided)
   - Use existing `get-repo-node-type-definition` tool
   - Most accurate, reflects committed node type in workspace
   - Handles package-prefixed types like `"IncrementalLoading:::230"`

2. **Node type corpus** (fallback)
   - Use existing `get-node-type-variant` tool
   - Match by normalized family name
   - Reliable for standard Coalesce built-in node types

3. **Error** (if neither available)
   - Return clear error message
   - Cannot proceed without schema

### Schema Structure

From node type definitions, we extract the `config` array:

```yaml
config:
  - groupName: Options
    items:
      - type: textBox
        displayName: Business Key
        attributeName: businessKey
        isRequired: true

      - type: toggleButton
        displayName: Truncate Before
        attributeName: truncateBefore
        default: false
        enableIf: "{% if node.materializationType == 'view' %} false {% else %} true {% endif %}"
```

Each config item has:
- `attributeName`: Field name in `node.config`
- `type`: UI control type (textBox, toggleButton, dropdownSelector, etc.)
- `isRequired`: Boolean or Jinja expression
- `default`: Default value
- `enableIf`: Jinja condition for when field is enabled

## Config Field Classification

### Required Fields

**Criteria:** `isRequired: true`

**Examples:**
- `businessKey` (Dimension/Fact)
- Materialization selector (most node types)

**Handling:**
- MUST be set, even if no contextual value detected
- If cannot determine value, return error or warning

### Conditional Required Fields

**Criteria:** `isRequired` contains Jinja expression

**Example:**
```yaml
- attributeName: lastModifiedColumn
  isRequired: "{% if config.lastModifiedComparison %} true {% else %} false {% endif %}"
```

**Handling:**
- Evaluate condition against current node state
- If true, treat as required
- If false, skip

### Optional with Defaults

**Criteria:** `default` value exists, `isRequired` is false/missing

**Examples:**
- `truncateBefore: false`
- `selectDistinct: false`
- `insertStrategy: "UNION ALL"`

**Handling:**
- Set if context makes sense
- Use default value unless context suggests otherwise
- Examples:
  - `selectDistinct: false` if node has aggregates
  - `insertStrategy: "UNION ALL"` if multi-source detected

### Contextual Optional Fields

**Criteria:** No default, not required, but relevant based on context

**Examples:**
- `lastModifiedColumn` when timestamp columns exist
- Type 2 dimension fields when pattern detected

**Handling:**
- Intelligently determine based on node analysis
- Do NOT auto-enable features, only suggest/prepare fields
- Document detection in analysis output

## Node Context Analysis

### Context Analyzer

Function: `analyzeNodeContext(node: WorkspaceNode)`

**Returns:**
```typescript
{
  hasMultipleSources: boolean;      // > 1 source in metadata
  hasAggregates: boolean;           // Columns with COUNT, SUM, etc.
  hasTimestampColumns: boolean;     // Columns matching *_TS, *_DATE patterns
  hasType2Pattern: boolean;         // START_DATE, END_DATE, IS_CURRENT detected
  materializationType: 'table' | 'view';
  columnPatterns: {
    timestamps: string[];           // Column names matching timestamp patterns
    dates: string[];               // Column names matching date patterns
    businessKeys: string[];        // Columns that look like keys
  };
}
```

**Detection Logic:**

**Multi-source:**
```typescript
hasMultipleSources = node.metadata.sources?.length > 1
```

**Aggregates:**
```typescript
hasAggregates = node.metadata.columns?.some(col =>
  /COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE|LISTAGG|ARRAY_AGG/i.test(col.transform)
)
```

**Timestamp columns:**
```typescript
timestamps = columns.filter(col =>
  /_TS$|_TIMESTAMP$|TIMESTAMP_/i.test(col.name)
)
dates = columns.filter(col =>
  /_DATE$|_DT$|DATE_/i.test(col.name)
)
hasTimestampColumns = timestamps.length > 0 || dates.length > 0
```

**Type 2 pattern:**
```typescript
hasType2Pattern =
  columns.some(c => /START_DATE|EFFECTIVE_DATE/i.test(c.name)) &&
  columns.some(c => /END_DATE|EXPIRY_DATE/i.test(c.name)) &&
  columns.some(c => /IS_CURRENT|CURRENT_FLAG/i.test(c.name))
```

## Contextual Intelligence Rules

### Rule Engine

Function: `determineConfigFields(schema, context, existingConfig)`

**Input:**
- `schema`: Parsed node type config schema
- `context`: Output from `analyzeNodeContext()`
- `existingConfig`: Current node.config values

**Output:**
```typescript
{
  required: Record<string, unknown>;      // Required fields to set
  contextual: Record<string, unknown>;    // Contextual optional fields
}
```

### Intelligence Rules

#### Multi-Source Strategy

**Trigger:** `context.hasMultipleSources === true`

**Action:**
```typescript
if (schema has 'insertStrategy' field && !existingConfig.insertStrategy) {
  if (context.hasAggregates || existingConfig.selectDistinct === true) {
    configUpdates.insertStrategy = "UNION";  // Deduplication needed
  } else {
    configUpdates.insertStrategy = "UNION ALL";  // Performance
  }
}
```

#### Aggregation Compatibility

**Trigger:** `context.hasAggregates === true`

**Action:**
```typescript
if (schema has 'selectDistinct' field && !existingConfig.selectDistinct) {
  configUpdates.selectDistinct = false;  // Incompatible with aggregates
}
```

#### Timestamp Column Detection

**Trigger:** `context.hasTimestampColumns === true`

**Action:**
```typescript
// DO NOT auto-enable lastModifiedComparison (user choice)
// But document available columns in analysis
analysis.candidateColumns = {
  lastModifiedColumn: context.columnPatterns.timestamps
};
```

#### Type 2 Dimension Detection

**Trigger:** `context.hasType2Pattern === true`

**Action:**
```typescript
// DO NOT auto-enable type2Dimension (requires verification)
// But document detection in analysis
analysis.patterns.type2Detected = true;
```

#### Truncate Before (Table Materialization)

**Trigger:** `context.materializationType === 'table'`

**Action:**
```typescript
if (schema has 'truncateBefore' field && existingConfig.truncateBefore === undefined) {
  configUpdates.truncateBefore = false;  // Safe default, user can enable
}
```

### Preservation Rules

**Never overwrite:**
- Existing non-null config values (except for required fields)
- User-set fields take precedence
- Exception: aggregation-specific fields from `convert-join-to-aggregation`

**Always set:**
- Required fields (even if overwriting)
- Aggregation-specific fields (`businessKey`, `changeTracking`) from transformation

**Smart merge:**
- If field exists but is empty string or null, set it
- If field is default value and context suggests better value, update it
- Document all changes in `configChanges` output

## Implementation Details

### File Organization

**New service layer:**
```
src/services/config/
  intelligent.ts       - completeNodeConfiguration() main function
  schema-resolver.ts   - resolveNodeTypeSchema() - repo → corpus resolution
  field-classifier.ts  - classifyConfigFields() - required/optional/contextual
  context-analyzer.ts  - analyzeNodeContext() - multi-source, aggregates, patterns
  rules.ts            - applyIntelligenceRules() - contextual logic
```

**Updates to existing files:**
```
src/services/workspace/mutations.ts
  - Update convertJoinToAggregation to call completeNodeConfiguration
  - Keep existing transformation logic
  - Add config completion to return value

src/mcp/nodes.ts
  - Register complete-node-configuration tool
  - Update convert-join-to-aggregation tool description
```

### Core Function Signatures

#### `completeNodeConfiguration()`

```typescript
export async function completeNodeConfiguration(
  client: CoalesceClient,
  params: {
    workspaceID: string;
    nodeID: string;
    repoPath?: string;
  }
): Promise<{
  node: WorkspaceNode;
  configChanges: Record<string, unknown>;
  analysis: {
    requiredFields: string[];
    contextualFields: string[];
    preservedFields: string[];
    schemaSource: 'repo' | 'corpus';
    candidateColumns?: Record<string, string[]>;
    patterns?: Record<string, boolean>;
  };
}>
```

**Algorithm:**
1. Fetch node: `GET /api/v1/workspaces/{workspaceID}/nodes/{nodeID}`
2. Resolve schema: `await resolveNodeTypeSchema(node.nodeType, repoPath)`
3. Analyze context: `const context = analyzeNodeContext(node)`
4. Classify fields: `const fields = classifyConfigFields(schema, node)`
5. Apply rules: `const updates = applyIntelligenceRules(schema, context, node.config)`
6. Update node: `await updateWorkspaceNode(client, { workspaceID, nodeID, changes: { config: updates } })`
7. Return result with analysis

#### `resolveNodeTypeSchema()`

```typescript
export async function resolveNodeTypeSchema(
  nodeType: string,
  repoPath?: string
): Promise<{
  source: 'repo' | 'corpus';
  schema: {
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
  };
}>
```

**Logic:**
```typescript
if (repoPath) {
  try {
    const def = await getRepoNodeTypeDefinition(repoPath, nodeType);
    return { source: 'repo', schema: def.nodeDefinition };
  } catch (e) {
    // Fall through to corpus
  }
}

// Corpus fallback
const normalizedFamily = normalizeNodeTypeFamily(nodeType);
const variant = await getNodeTypeVariant(normalizedFamily);
return { source: 'corpus', schema: variant.nodeDefinition };
```

#### `analyzeNodeContext()`

```typescript
export function analyzeNodeContext(node: WorkspaceNode): {
  hasMultipleSources: boolean;
  hasAggregates: boolean;
  hasTimestampColumns: boolean;
  hasType2Pattern: boolean;
  materializationType: 'table' | 'view';
  columnPatterns: {
    timestamps: string[];
    dates: string[];
    businessKeys: string[];
  };
}
```

**Implementation:** Uses regex patterns and metadata inspection as documented in "Node Context Analysis" section.

#### `classifyConfigFields()`

```typescript
export function classifyConfigFields(
  schema: NodeTypeSchema,
  node: WorkspaceNode
): {
  required: ConfigItem[];
  conditionalRequired: ConfigItem[];
  optionalWithDefaults: ConfigItem[];
  contextual: ConfigItem[];
}
```

**Logic:**
```typescript
for (const group of schema.config) {
  for (const item of group.items) {
    if (!item.attributeName) continue;

    if (item.isRequired === true) {
      required.push(item);
    } else if (typeof item.isRequired === 'string') {
      // Jinja expression - evaluate against node
      conditionalRequired.push(item);
    } else if (item.default !== undefined) {
      optionalWithDefaults.push(item);
    } else {
      contextual.push(item);
    }
  }
}
```

#### `applyIntelligenceRules()`

```typescript
export function applyIntelligenceRules(
  schema: NodeTypeSchema,
  context: NodeContext,
  existingConfig: Record<string, unknown>
): {
  required: Record<string, unknown>;
  contextual: Record<string, unknown>;
}
```

**Implementation:** Applies all rules documented in "Contextual Intelligence Rules" section.

### Error Handling

#### Schema Resolution Fails

```typescript
throw new Error(
  `Cannot resolve node type schema for '${nodeType}'. ` +
  `Ensure package is committed to repo or available in corpus.`
);
```

#### Required Field Cannot Be Determined

```typescript
// Return partial result with warning
return {
  node: updatedNode,
  configChanges,
  analysis: {
    ...analysis,
    warnings: [
      `Required field '${fieldName}' could not be auto-determined. Please set manually.`
    ]
  }
};
```

#### API Call Fails

```typescript
// Propagate with context
throw new Error(
  `Failed to update node configuration: ${apiError.message}`
);
```

### Edge Cases

**Node type not in repo or corpus:**
- Return clear error message
- Suggest installing package or updating corpus

**Partial schema (missing config section):**
- Return empty config updates
- Document in analysis that no schema available

**Conflicting rules (multiple patterns match):**
- Use priority order (required > contextual > defaults)
- Document conflicts in analysis warnings

**Empty/minimal nodes:**
- Still apply required fields
- Skip contextual rules that need column data
- Document limited analysis in output

## Testing Strategy

### Unit Tests

**Test each intelligence rule independently:**
```typescript
describe('Intelligence Rules', () => {
  it('sets insertStrategy for multi-source nodes', () => {
    const context = { hasMultipleSources: true, hasAggregates: false };
    const result = applyIntelligenceRules(schema, context, {});
    expect(result.contextual.insertStrategy).toBe('UNION ALL');
  });

  it('sets insertStrategy to UNION when aggregates present', () => {
    const context = { hasMultipleSources: true, hasAggregates: true };
    const result = applyIntelligenceRules(schema, context, {});
    expect(result.contextual.insertStrategy).toBe('UNION');
  });
});
```

**Test schema resolution (repo → corpus → error):**
```typescript
describe('Schema Resolution', () => {
  it('resolves from repo when repoPath provided', async () => {
    const result = await resolveNodeTypeSchema('Dimension', '/path/to/repo');
    expect(result.source).toBe('repo');
  });

  it('falls back to corpus when repo fails', async () => {
    const result = await resolveNodeTypeSchema('Dimension');
    expect(result.source).toBe('corpus');
  });

  it('throws error when neither available', async () => {
    await expect(resolveNodeTypeSchema('UnknownType')).rejects.toThrow();
  });
});
```

**Test field classification logic:**
```typescript
describe('Field Classification', () => {
  it('identifies required fields', () => {
    const classified = classifyConfigFields(schema, node);
    expect(classified.required).toContainEqual(
      expect.objectContaining({ attributeName: 'businessKey' })
    );
  });

  it('identifies optional with defaults', () => {
    const classified = classifyConfigFields(schema, node);
    expect(classified.optionalWithDefaults).toContainEqual(
      expect.objectContaining({
        attributeName: 'truncateBefore',
        default: false
      })
    );
  });
});
```

**Test context analyzer with various node shapes:**
```typescript
describe('Context Analyzer', () => {
  it('detects multi-source nodes', () => {
    const node = { metadata: { sources: [{ name: 'A' }, { name: 'B' }] } };
    const context = analyzeNodeContext(node);
    expect(context.hasMultipleSources).toBe(true);
  });

  it('detects aggregate columns', () => {
    const node = {
      metadata: {
        columns: [
          { name: 'TOTAL', transform: 'COUNT(*)' }
        ]
      }
    };
    const context = analyzeNodeContext(node);
    expect(context.hasAggregates).toBe(true);
  });

  it('detects timestamp columns', () => {
    const node = {
      metadata: {
        columns: [
          { name: 'CREATED_TS' },
          { name: 'ORDER_DATE' }
        ]
      }
    };
    const context = analyzeNodeContext(node);
    expect(context.hasTimestampColumns).toBe(true);
    expect(context.columnPatterns.timestamps).toContain('CREATED_TS');
    expect(context.columnPatterns.dates).toContain('ORDER_DATE');
  });
});
```

### Integration Tests

**Test `complete-node-configuration` with Dimension, Fact, Stage nodes:**
```typescript
describe('complete-node-configuration Integration', () => {
  it('completes Dimension node config', async () => {
    // Create minimal Dimension node
    const node = await createWorkspaceNode(client, {
      workspaceID: 'ws-1',
      name: 'DIM_TEST',
      nodeType: 'Dimension',
      // ... minimal fields
    });

    // Complete config
    const result = await completeNodeConfiguration(client, {
      workspaceID: 'ws-1',
      nodeID: node.id
    });

    expect(result.node.config.businessKey).toBeDefined();
    expect(result.analysis.requiredFields).toContain('businessKey');
  });
});
```

**Test `convert-join-to-aggregation` end-to-end with config completion:**
```typescript
describe('convert-join-to-aggregation with auto-config', () => {
  it('transforms and completes config in one call', async () => {
    const result = await convertJoinToAggregation(client, {
      workspaceID: 'ws-1',
      nodeID: 'fact-1',
      groupByColumns: ['"ORDERS"."CUSTOMER_ID"'],
      aggregates: [
        { name: 'TOTAL', function: 'COUNT', expression: '*' }
      ]
    });

    // Transformation
    expect(result.node.metadata.columns).toHaveLength(2);
    expect(result.groupByAnalysis.groupByColumns).toHaveLength(1);

    // Config completion
    expect(result.configCompletion.configChanges.businessKey).toBe('CUSTOMER_ID');
    expect(result.configCompletion.configChanges.changeTracking).toBe('TOTAL');
    expect(result.configCompletion.analysis.requiredFields.length).toBeGreaterThan(0);
  });
});
```

**Test repo-backed vs corpus-backed resolution:**
```typescript
describe('Schema Resolution Integration', () => {
  it('uses repo when available', async () => {
    const result = await completeNodeConfiguration(client, {
      workspaceID: 'ws-1',
      nodeID: 'node-1',
      repoPath: '/path/to/repo'
    });

    expect(result.analysis.schemaSource).toBe('repo');
  });

  it('falls back to corpus', async () => {
    const result = await completeNodeConfiguration(client, {
      workspaceID: 'ws-1',
      nodeID: 'node-1'
      // No repoPath
    });

    expect(result.analysis.schemaSource).toBe('corpus');
  });
});
```

**Test preservation of existing config values:**
```typescript
describe('Config Preservation', () => {
  it('preserves user-set config values', async () => {
    // Create node with explicit config
    const node = await createWorkspaceNode(client, {
      workspaceID: 'ws-1',
      name: 'TEST',
      nodeType: 'Dimension',
      config: {
        truncateBefore: true,  // User explicitly set this
        selectDistinct: true
      }
    });

    const result = await completeNodeConfiguration(client, {
      workspaceID: 'ws-1',
      nodeID: node.id
    });

    // Should preserve user values
    expect(result.node.config.truncateBefore).toBe(true);
    expect(result.node.config.selectDistinct).toBe(true);
    expect(result.analysis.preservedFields).toContain('truncateBefore');
    expect(result.analysis.preservedFields).toContain('selectDistinct');
  });
});
```

### Test Fixtures

**Create test fixtures for common node types:**
```typescript
// tests/fixtures/node-configs/
dimension-minimal.json
dimension-with-aggregates.json
fact-multi-source.json
stage-simple.json
```

**Create test fixtures for node type schemas:**
```typescript
// tests/fixtures/schemas/
dimension-schema.json
fact-schema.json
stage-schema.json
```

## Documentation Updates

### Resource Files

**Create new resource:**
```
src/resources/context/intelligent-node-configuration.md
```

**Content:**
- How `complete-node-configuration` works
- When to use it vs manual config
- Intelligence rules explained
- Examples for each node type
- Troubleshooting common issues

**Update existing resources:**

`aggregation-patterns.md`:
- Add section on automatic config completion
- Document new fields in output
- Examples with config completion

`tool-usage.md`:
- Add `complete-node-configuration` to tool catalog
- Update `convert-join-to-aggregation` description
- When to use config completion

### Tool Descriptions

**`complete-node-configuration`:**
```
Intelligently complete all required and contextual config fields for a workspace node.

This tool:
- Resolves node type schema from repo or corpus
- Analyzes node metadata (columns, sources, joins)
- Sets all required config fields
- Intelligently adds contextual optional fields based on detected patterns
- Preserves existing user-set values

Use this when:
- Node was created via UI and needs config completion
- Manual edits left config incomplete
- You want to ensure all required fields are set

The tool automatically detects:
- Multi-source scenarios → sets insertStrategy
- Aggregation patterns → sets selectDistinct: false
- Timestamp columns → documents candidates for lastModifiedColumn
- Type 2 dimension patterns → documents detection

Input: workspaceID, nodeID, optional repoPath
Output: Updated node + analysis of what was changed
```

**`convert-join-to-aggregation` (updated):**
```
Convert a join-based node into an aggregated fact table with GROUP BY.

This tool performs complete transformation in one call:
- Analyzes join patterns and generates JOIN ON clauses
- Detects aggregate vs non-aggregate columns
- Builds GROUP BY clause and validates correctness
- Infers datatypes from SQL expressions
- Sets businessKey (from GROUP BY) and changeTracking (from aggregates)
- **Automatically completes all other required and contextual config fields**

The result is a fully transformed AND configured node ready to use.

Use maintainJoins: true to automatically generate JOIN ON clauses from common columns.
Use repoPath to ensure accurate schema resolution for config completion.
```

## Migration Path

### Existing Code

**No breaking changes:**
- Current `convert-join-to-aggregation` calls continue to work
- New `configCompletion` field added to response (non-breaking)
- Existing tests continue to pass with enhanced behavior

### New Code

**Adoption:**
- Users get config completion automatically in `convert-join-to-aggregation`
- Can use `complete-node-configuration` standalone for existing nodes
- Documentation encourages using config tools for all node operations

## Success Metrics

**Quantitative:**
- All required config fields set after transformation (100% target)
- Contextual fields detected and set (measure coverage)
- Time saved vs manual config (benchmark)
- Validation errors reduced (before/after comparison)

**Qualitative:**
- User feedback on config completeness
- Reduction in "how do I set X?" support questions
- Improved first-run success rate for transformations

## Future Enhancements

**Out of scope for v1, but future possibilities:**

1. **Custom Rules Engine:**
   - User-defined config rules
   - Project-specific intelligence patterns
   - Rule versioning and management

2. **Config Validation:**
   - Pre-flight validation before API call
   - Suggest fixes for invalid combinations
   - Explain why field is required

3. **Config Templates:**
   - Save/load config templates by node type
   - Organization-wide defaults
   - Template inheritance

4. **Interactive Config:**
   - Prompt for required fields that can't be determined
   - Suggest values with confidence scores
   - Allow user override in tool call

5. **Config Diff:**
   - Show before/after config changes
   - Explain why each field was set
   - Allow selective application

6. **Extended Pattern Detection:**
   - SCD Type 1/2/3/6 patterns
   - Fact table grain detection
   - Bridge table patterns
   - Slowly changing dimension detection

## Appendix

### Example Scenarios

#### Scenario 1: Complete Existing Dimension

```typescript
// User created node via UI, needs config
await completeNodeConfiguration(client, {
  workspaceID: 'ws-123',
  nodeID: 'dim-customers'
});

// Returns:
{
  node: { /* fully configured */ },
  configChanges: {
    businessKey: 'CUSTOMER_ID',
    selectDistinct: false,
    truncateBefore: false
  },
  analysis: {
    requiredFields: ['businessKey'],
    contextualFields: ['selectDistinct', 'truncateBefore'],
    preservedFields: ['materializationType'],
    schemaSource: 'corpus'
  }
}
```

#### Scenario 2: Transform with Auto-Config

```typescript
// One-stop transformation
await convertJoinToAggregation(client, {
  workspaceID: 'ws-123',
  nodeID: 'fact-orders',
  groupByColumns: ['"ORDERS"."CUSTOMER_ID"', '"ORDERS"."ORDER_DATE"'],
  aggregates: [
    { name: 'TOTAL_ORDERS', function: 'COUNT', expression: 'DISTINCT "ORDERS"."ORDER_ID"' },
    { name: 'REVENUE', function: 'SUM', expression: '"ORDERS"."AMOUNT"' }
  ],
  maintainJoins: true,
  repoPath: '/path/to/repo'
});

// Returns:
{
  node: { /* transformed and configured */ },
  groupByAnalysis: {
    groupByColumns: ['"ORDERS"."CUSTOMER_ID"', '"ORDERS"."ORDER_DATE"'],
    groupByClause: 'GROUP BY "ORDERS"."CUSTOMER_ID", "ORDERS"."ORDER_DATE"'
  },
  joinSQL: { /* generated joins */ },
  configCompletion: {
    configChanges: {
      businessKey: 'CUSTOMER_ID,ORDER_DATE',
      changeTracking: 'TOTAL_ORDERS,REVENUE',
      selectDistinct: false,
      insertStrategy: 'UNION ALL'  // Multi-source detected
    },
    analysis: {
      requiredFields: ['businessKey'],
      contextualFields: ['changeTracking', 'selectDistinct', 'insertStrategy'],
      preservedFields: [],
      schemaSource: 'repo'
    }
  }
}
```

#### Scenario 3: Multi-Source with Type Detection

```typescript
// Node with multiple sources and timestamp columns
const node = {
  nodeType: 'Fact',
  metadata: {
    sources: [
      { name: 'ORDERS' },
      { name: 'CUSTOMERS' }
    ],
    columns: [
      { name: 'CUSTOMER_ID', transform: '"CUSTOMERS"."CUSTOMER_ID"' },
      { name: 'TOTAL_ORDERS', transform: 'COUNT(DISTINCT "ORDERS"."ORDER_ID")' },
      { name: 'LAST_ORDER_TS', transform: 'MAX("ORDERS"."ORDER_TS")' }
    ]
  }
};

await completeNodeConfiguration(client, { workspaceID, nodeID });

// Returns:
{
  configChanges: {
    businessKey: 'CUSTOMER_ID',
    insertStrategy: 'UNION',  // Multi-source + aggregates = UNION
    selectDistinct: false      // Has aggregates
  },
  analysis: {
    contextualFields: ['insertStrategy', 'selectDistinct'],
    candidateColumns: {
      lastModifiedColumn: ['LAST_ORDER_TS']  // Detected but not set
    }
  }
}
```

### Config Item Type Reference

Common `type` values in node type schemas:

- `textBox` - Free text input
- `toggleButton` - Boolean on/off
- `dropdownSelector` - Choose from options list
- `materializationSelector` - table/view choice
- `businessKeyColumns` - Special: column selector for business key
- `changeTrackingColumns` - Special: column selector for change tracking
- `columnDropdownSelector` - Single column selection
- `tabular` - Table/grid input
- `multisourceToggle` - Enable multi-source mode
- `overrideSQLToggle` - **IGNORED per project policy**

### Jinja Expression Handling

Node type schemas use Jinja for conditional logic. For v1:

**Simple evaluation:**
- Parse basic conditionals: `{% if config.field %} true {% else %} false {% endif %}`
- Support common operators: `==`, `!=`, `and`, `or`, `not`

**Limited scope:**
- Only evaluate against `node` and `config` objects
- Do NOT execute arbitrary Jinja code
- Fail safely: if can't parse, assume false

**Future enhancement:**
- Full Jinja interpreter
- Custom filter support
- Expression debugging

### Node Type Normalization

Converting node type identifiers to normalized families:

```typescript
function normalizeNodeTypeFamily(nodeType: string): string {
  // Remove package prefix
  const bareType = nodeType.includes(':::')
    ? nodeType.split(':::')[0]
    : nodeType;

  // Normalize case and special characters
  return bareType
    .replace(/[^a-zA-Z0-9]/g, '')
    .toLowerCase();
}

// Examples:
// "Dimension" → "dimension"
// "IncrementalLoading:::230" → "incrementalloading"
// "Copy of Dimension" → "copyofdimension"
```

## Conclusion

This design provides intelligent, node-type-aware config completion that:
- Eliminates manual config work for common scenarios
- Reduces validation errors and trial-and-error
- Provides reusable, standalone config tool
- Enhances existing transformation with automatic config
- Supports both repo-backed and corpus-backed schemas
- Preserves user intent while completing missing fields

The implementation is focused, testable, and follows project patterns. It sets the foundation for future config intelligence enhancements while delivering immediate value.
