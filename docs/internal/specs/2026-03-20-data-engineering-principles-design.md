# Data Engineering Principles Resource Design

## Overview

**Goal:** Create a comprehensive AI resource that enables intelligent node type selection and data pipeline design recommendations based on data engineering best practices.

**Problem Statement:**
- Choosing the correct node type is critical but difficult to reverse (requires creating a new node)
- Coalesce API doesn't provide a way to list available node types
- Current `node-selection.md` focuses on pattern matching but doesn't evaluate whether patterns are optimal
- AI needs to understand data engineering principles to recommend improvements, not just replicate existing patterns

**Solution:**
Create `src/resources/context/data-engineering-principles.md` that:
1. Teaches the AI to analyze workspace DAG topology and lineage
2. Recognizes data pipeline layers and methodologies from structure
3. Recommends appropriate node types based on purpose and best practices
4. Suggests improvements when existing patterns could be enhanced
5. Maps purposes to node types with confidence levels

## File Location

`src/resources/context/data-engineering-principles.md`

## Target Audience

AI assistant (not user-facing documentation). The resource is loaded and consulted when making node creation decisions.

## Document Structure

```markdown
# Data Engineering Principles for Coalesce

## How to Use This Guide
[AI instructions on when/how to apply this]

## Workspace Pattern Analysis
[Multi-level analysis: package adoption → DAG topology → lineage patterns]

## Layered Architecture Patterns
[Bronze/Silver/Gold recognition from DAG structure]

## Methodology-Specific Guidance
[Kimball, Data Vault, dbt-style pattern recognition]

## Materialization Strategies
[Views vs Tables vs Incremental by layer and purpose]

## Dependency Management
[Healthy vs problematic DAG patterns]

## Package Recommendations
[github.com/coalesceio package guidance]
```

## Section 1: How to Use This Guide

**Purpose:** Tell the AI when and how to apply this resource.

**Key Instructions:**

1. **When to consult this resource:**
   - Before creating any workspace node
   - When user mentions data modeling concepts
   - When evaluating existing workspace structure

2. **Application pattern:**
   - **ALWAYS** call `list-workspace-nodes` first (unless user specifies exact node type)
   - Analyze package adoption, DAG topology, and lineage patterns
   - Cross-reference against this guide's best practices
   - If workspace differs from recommendations → inform user with rationale
   - If workspace aligns → proceed with existing pattern

3. **Recommendation philosophy:**
   - Be informative, not prescriptive
   - Show current pattern AND potential improvements
   - Let user decide on changes to established workspaces
   - Example: "Your workspace uses Stage nodes. The base-nodes:::Stage type offers [benefits]. Want to use that for this new node?"

4. **Confidence levels:**
   - **High confidence:** General DE principles, standard layer patterns, DAG topology analysis
   - **Medium confidence:** Package-specific features (refer to github.com/coalesceio)
   - **Low confidence:** Custom node type template behavior (direct user to verify in UI)

5. **User-specified types:**
   - If user specifies exact node type → use it, skip analysis
   - Trust user expertise

## Section 2: Workspace Pattern Analysis

**Purpose:** Teach multi-level pattern recognition from packages → DAG → lineage.

### Data Caching Strategy

Following the established pattern in [usage-guide.md](../usage-guide.md#data-management):

**All API responses MUST be saved to the `data/` folder:**

```
data/
  ├── nodes/
  │   ├── workspace-{workspaceID}-nodes.json       # Full node list
  │   └── workspace-{workspaceID}-profile.json     # Analyzed patterns
  ├── environments/
  │   └── env-{environmentID}-nodes.json
  └── ...
```

**Workflow:**

1. **list-workspace-nodes** → Saves to `data/nodes/workspace-{workspaceID}-nodes.json`
2. **analyze-workspace-patterns** (new tool) → Reads cached data, generates profile
3. **AI analysis** → Checks `data/` folder first, only calls API if cache missing/stale

**Benefits:**
- Handles workspaces with thousands of nodes (avoids context overflow)
- Fast pattern analysis (reads local files, no API latency)
- Historical tracking (see how patterns evolve)
- Reduces API calls (reuse cached data for multiple node creations)

**AI Decision Logic:**

```
Before analyzing workspace patterns:

1. Check if data/nodes/workspace-{workspaceID}-nodes.json exists
   - If YES and recent (< 24 hours old) → read from file
   - If NO or stale → call list-workspace-nodes, save response

2. Check if data/workspace-{workspaceID}-profile.json exists
   - If YES and recent → use cached profile
   - If NO or stale → analyze nodes data, generate profile

3. Use profile data for node type recommendations
```

### Level 1: Package Detection

**Process:**
```
Read from data/nodes/workspace-{workspaceID}-nodes.json (or call API and save):

Scan node types for package prefixes:
- base-nodes:::* → base-nodes package IS available
- custom-package:::* → custom-package IS available
- No prefix → Built-in type

Key insight: PRESENCE indicates availability. Don't use percentages.
If package types exist anywhere, the workspace has access to that package.

Save detected packages to workspace profile.
```

**Package Categories:**

- **base-nodes:** Enhanced versions of standard types (Stage, View, Dimension, Fact, Work)
- **Specialized methodology:** Data Vault, Kimball extensions, semantic layers
- **Platform-specific:** Databricks DLT, BigQuery advanced, Dynamic Tables, Streams/Tasks
- **Data quality:** Incremental loading, test filtering, data validation
- **Built-in:** Stage, View, Dimension, Fact, persistentStage (no prefix)

### Level 2: DAG Topology Analysis

**Layer Recognition from Predecessor Count:**

**Bronze/Landing Layer:**
- **DAG signature:** 0 predecessors OR predecessors are source nodes (not workspace nodes)
- **Purpose:** Raw data ingestion
- **Typical names:** RAW_*, SRC_*, LANDING_*, L0_*

**Silver/Staging Layer:**
- **DAG signature:** 1-2 predecessors from bronze/landing
- **Purpose:** Cleansing, standardization, light transformation
- **Typical names:** STG_*, STAGE_*, CLEAN_*, L1_*

**Intermediate/Transform Layer:**
- **DAG signature:** Mid-pipeline (has both predecessors AND downstream consumers)
- **Purpose:** Business logic, joins, derived fields
- **Typical names:** INT_*, TMP_*, WORK_*, TRANSFORM_*

**Gold/Mart Layer:**
- **DAG signature:** Multiple predecessors, few/no downstream consumers (end of DAG)
- **Purpose:** Business-ready analytics
- **Typical names:** DIM_*, FACT_*, FCT_*, MART_*, RPT_*

### Level 3: Lineage-Based Methodology Detection

**Kimball Dimensional Modeling:**

**Dimension detection:**
- Node is referenced by multiple fact nodes downstream
- Typically 1-2 predecessors
- Named DIM_*, DIMENSION_*
- May have SCD tracking columns (EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT)

**Fact detection:**
- Node has 3+ dimension predecessors
- High column count (includes dimension foreign keys + measures)
- Named FACT_*, FCT_*
- Typically end-of-DAG (few downstream consumers)

**Topology:** Star or snowflake pattern visible

**Data Vault 2.0:**

**Hub detection:**
- Few columns (business key + metadata columns)
- Many downstream nodes (satellites reference it)
- Represents core business entities
- Named *_HUB, HUB_*

**Satellite detection:**
- Many columns (descriptive attributes)
- Single hub predecessor
- History tracking (LOAD_DATE, HASH_DIFF)
- Named *_SAT, SAT_*

**Link detection:**
- Multiple hub predecessors (2+)
- Represents relationships between entities
- Named *_LINK, LINK_*

**Topology:** Hub-and-spoke pattern visible

**dbt-Style Staging:**

**Pattern detection:**
- Clear staging → intermediate → mart flow
- Heavy use of View node types in intermediate layer
- Selective table materialization (only where needed)
- Consistent naming: stg_ → int_ → fct_/dim_

### Decision Framework

```
When creating a new node:

1. Call list-workspace-nodes to get all nodes
2. Detect package availability (presence check)
3. Analyze DAG topology:
   - Where will this node sit? (predecessor count, naming)
   - What layer does it belong to?
4. Analyze lineage patterns:
   - What methodology is in use? (Kimball, Data Vault, dbt-style)
   - What similar nodes exist in this layer?
5. Cross-reference with best practices (later sections)
6. Make recommendation:
   - IF workspace pattern exists AND is sound → follow it
   - IF workspace pattern exists BUT could be improved → suggest improvement
   - IF no clear pattern → recommend best practice default
```

## Section 3: Layered Architecture Patterns

**Purpose:** Map DAG layers to appropriate node types and materialization.

### Bronze/Landing Layer

**Recognition:**
- Nodes with 0 predecessors or source node predecessors
- Named RAW_*, SRC_*, LANDING_*, L0_*
- Purpose: Raw data ingestion, minimal transformation

**Recommended node types:**
- `Stage` or `base-nodes:::Stage` (if package available)
- Occasionally `View` for pass-through scenarios

**Materialization:**
- **Tables** (need persistence for raw data)
- Full refresh or incremental depending on source volume

**Rationale:**
- Preserve raw data for auditing and reprocessing
- Establish consistent starting point for downstream transformations

### Silver/Staging Layer

**Recognition:**
- 1-2 predecessors from bronze/landing
- Named STG_*, STAGE_*, CLEAN_*, L1_*
- Purpose: Cleansing, type casting, standardization

**Recommended node types:**
- `Stage` or `base-nodes:::Stage`
- `Incremental Load` (from Incremental-Nodes package) for large datasets
- `Test Passed Records` / `Test Failed Records` for data quality workflows

**Materialization:**
- **Tables** for small-medium datasets
- **Incremental tables** for large, time-series data

**Rationale:**
- Standardized staging enables reuse across multiple downstream consumers
- Quality checks at this layer prevent bad data propagation

### Intermediate/Transform Layer

**Recognition:**
- Mid-pipeline (has predecessors AND downstream consumers)
- Named INT_*, TMP_*, WORK_*, TRANSFORM_*
- Purpose: Business logic, joins, calculated fields

**Recommended node types:**
- `View` or `base-nodes:::View`
- `Work` or `base-nodes:::Work`

**Materialization:**
- **Views** (minimize storage, always fresh)
- **Tables** only if computation is expensive AND node is heavily queried

**Rationale:**
- Views avoid unnecessary storage costs
- Intermediate steps should be lightweight and recomputable

### Gold/Mart Layer

**Recognition:**
- Multiple predecessors, few/no downstream consumers
- Named DIM_*, FACT_*, FCT_*, MART_*, RPT_*
- Purpose: Business-ready analytics, dimensions, facts, metrics

**Recommended node types:**
- Dimensions: `Dimension` or `base-nodes:::Dimension`
- Facts: `Fact` or `base-nodes:::Fact`
- Metrics/aggregations: `View` or `base-nodes:::View`

**Materialization:**
- **Dimensions:** Tables (small, need persistence, may have SCD logic)
- **Facts:** Incremental tables (large, time-series, append-heavy)
- **Aggregations:** Views if simple, tables if complex/heavily queried

**Rationale:**
- End-users query these directly → performance matters
- Dimensions typically small enough for full refresh
- Facts typically large → incremental loading essential

### Layer Flow Validation

When creating nodes, validate proper layer flow:

```
✅ GOOD: Bronze → Silver → Gold
✅ GOOD: Bronze → Silver → Intermediate → Gold
✅ GOOD: Silver → Intermediate → Intermediate → Gold (multi-step transforms)

❌ BAD: Bronze → Gold (skips silver standardization)
❌ BAD: Gold → Silver (backwards flow)
❌ BAD: Intermediate → Bronze (backwards flow)
```

If user creates cross-layer dependency, warn:
> "This node reads directly from Bronze, skipping the Silver layer. Consider routing through staging for consistent standardization."

## Section 4: Methodology-Specific Guidance

**Purpose:** Recognize and support specific data modeling methodologies.

### Kimball Dimensional Modeling

**Recognition Signals:**

From workspace analysis:
- Clear separation between DIM_* and FACT_* nodes
- Facts have multiple dimension predecessors (3-7 typically)
- Dimensions have 1-2 predecessors from staging
- Star or snowflake topology visible in DAG

**Node Type Mapping:**

| Purpose | Recommended Type | Materialization |
|---------|-----------------|-----------------|
| Dimension | `Dimension` or `base-nodes:::Dimension` | Table (full refresh or SCD Type 2) |
| Fact | `Fact` or `base-nodes:::Fact` | Incremental table |
| Staging | `Stage` or `base-nodes:::Stage` | Table or incremental |

**SCD Type 2 Detection:**

Look for dimension nodes with:
- `EFFECTIVE_FROM`, `EFFECTIVE_TO` columns
- `IS_CURRENT`, `CURRENT_FLAG` columns
- Multiple records per business key
- Potential config: `scdType: 2`

**AI Decision Logic:**
```
IF workspace shows Kimball patterns:
  - For dimension creation → use Dimension node type
  - For fact creation → use Fact node type
  - Suggest SCD Type 2 config if dimension tracks history
  - Validate fact has appropriate dimension predecessors
```

### Data Vault 2.0

**Recognition Signals:**

From workspace analysis:
- Hub nodes: Few columns, many downstream satellites
- Satellite nodes: Many columns, single hub predecessor, history tracking
- Link nodes: Multiple hub predecessors

**Structural Patterns:**

**Hub Structure:**
- Business key column(s)
- HASH_KEY (unique identifier)
- LOAD_DATE, RECORD_SOURCE (metadata)
- Few/no descriptive attributes
- Many downstream satellites

**Satellite Structure:**
- Hub foreign key reference
- HASH_DIFF (change detection)
- LOAD_DATE, LOAD_END_DATE (validity)
- Many descriptive columns
- Single hub predecessor

**Link Structure:**
- Multiple hub foreign keys (2+)
- HASH_KEY for link
- LOAD_DATE, RECORD_SOURCE
- Represents many-to-many relationships

**Node Type Mapping:**

Data Vault patterns may use:
- Specialized Data Vault packages from github.com/coalesceio
- Standard types with Data Vault-specific config
- Custom node types matching the pattern

**AI Decision Logic:**
```
IF workspace shows Data Vault patterns:
  - Recognize the methodology
  - Check if specialized DV package is in use (package prefix detection)
  - IF package exists → use those node types
  - IF no package → suggest exploring github.com/coalesceio for DV support
  - Validate structural patterns (hubs reference satellites, links connect hubs)
```

### dbt-Style Staging

**Recognition Signals:**

From workspace analysis:
- Clear naming convention: stg_ → int_ → fct_/dim_
- Heavy use of View node types in intermediate layer
- Selective materialization (tables only where needed)
- Modular, single-purpose transformations

**Node Type Mapping:**

| Layer | Recommended Type | Materialization |
|-------|-----------------|-----------------|
| Staging (stg_) | `Stage` or `base-nodes:::Stage` | Table or incremental |
| Intermediate (int_) | `View` or `base-nodes:::View` | View (ephemeral) |
| Marts (fct_, dim_) | `Fact`, `Dimension`, `View` | Table or view based on use |

**Philosophy:**
- Minimize storage (views where possible)
- Break complex logic into readable steps
- Each transformation has single, clear purpose

**AI Decision Logic:**
```
IF workspace shows dbt-style patterns:
  - Match the naming convention (stg_, int_, fct_)
  - Use Views for intermediate transformations
  - Only materialize as tables when necessary (heavy queries, expensive compute)
  - Suggest breaking complex logic into multiple int_ nodes
```

### Mixed or Unclear Methodology

**When workspace doesn't show clear methodology:**

1. Ask user about their preferred approach (if creating first few nodes)
2. Default to simple staging → mart pattern
3. Offer to implement specific methodology if requested
4. Don't force methodology on established mixed workspaces

## Section 5: Materialization Strategies

**Purpose:** Recommend appropriate materialization based on layer, volume, and usage.

### Materialization Types

#### Table (Full Refresh)

**When to use:**
- Small to medium datasets (<1M rows)
- Data changes significantly each run (>20% of rows)
- Staging/bronze layer (persistent raw data needed)
- Dimensions with low update frequency
- Complete rebuild is acceptable

**Trade-offs:**
- ✅ Simple, predictable
- ✅ Complete refresh ensures consistency
- ✅ No drift risk
- ❌ Higher compute cost for large datasets
- ❌ Longer run times as data grows

**Config indicators:**
- `insertStrategy: "INSERT"`
- `truncateBefore: true`
- `materializationType: "table"`

**Recommended for:**
- Bronze/landing layer
- Small staging tables
- Dimensions (<1M rows)
- Lookup tables

#### View

**When to use:**
- Intermediate transformations (silver → gold)
- Low to medium query frequency
- Always need fresh data (no staleness acceptable)
- Simple transformations without heavy compute
- Storage cost is a concern

**Trade-offs:**
- ✅ No storage cost
- ✅ Always current (no refresh lag)
- ✅ Fast to "build" (no data movement)
- ✅ Easy to modify logic
- ❌ Query time cost (compute on read)
- ❌ Not suitable for complex aggregations queried frequently
- ❌ Can cascade performance issues

**Recommended for:**
- Intermediate/transform layer
- Simple joins and filters
- Aggregations with small result sets
- Metrics computed on-demand

#### Incremental (Merge/Append)

**When to use:**
- Large datasets (>1M rows)
- Only recent data changes (time-series, event data)
- Fact tables with high volume
- Staging tables with clear update patterns (high-water mark)
- Append-heavy workloads

**Trade-offs:**
- ✅ Efficient for large datasets
- ✅ Faster run times (process only deltas)
- ✅ Lower compute cost
- ✅ Scales with data growth
- ❌ More complex logic (merge keys, high-water marks)
- ❌ Risk of drift if logic is incorrect
- ❌ Harder to debug

**Config indicators:**
- `Incremental Load` node type (from Incremental-Nodes package)
- High-water mark columns
- Merge keys defined
- `insertStrategy: "MERGE"`

**Recommended for:**
- Large fact tables
- Event/log data
- Time-series data with consistent append patterns
- Staging tables for large source systems

### Layer-Specific Recommendations

```
Bronze/Landing Layer:
  → Tables (preserve raw data)
  → Incremental if source is very large and has clear time-based updates

Silver/Staging Layer:
  → Tables for small datasets (<1M rows)
  → Incremental for large datasets with time-based updates
  → Consider Incremental-Nodes package for complex patterns

Intermediate/Transform Layer:
  → Views (minimize storage, always fresh)
  → Tables only if:
    - Heavily queried
    - Expensive to compute
    - Downstream dependencies need stability

Gold/Mart Layer:
  Dimensions:
    → Tables (usually small, need persistence)
    → SCD Type 2 dimensions need table materialization

  Facts:
    → Incremental tables (usually large, time-series)
    → Full refresh only if small or complete rebuild needed

  Metrics/Aggregations:
    → Views if simple transformations
    → Tables if complex or heavily queried
```

### AI Decision Logic

```
When recommending materialization:

1. Identify layer (from DAG position)
2. Infer data volume:
   - Check predecessor node metadata if available
   - Look for time-series patterns in column names (DATE, TIMESTAMP)
   - Consider naming (FACT suggests large, DIM suggests small)
3. Check existing patterns in same layer:
   - If all staging uses tables → match that
   - If all intermediate uses views → match that
4. Default conservatively:
   - Bronze → Table
   - Staging → Table (or incremental if volume indicators present)
   - Transform → View
   - Dimension → Table
   - Fact → Ask user about volume/update frequency, suggest incremental
```

**Example recommendations:**

- "Creating staging node STG_ORDERS. I'll use a table for persistence. If this is a large, frequently-updated source, consider the Incremental Load node type from github.com/coalesceio/Incremental-Nodes."

- "Creating intermediate transform INT_CUSTOMER_METRICS. I'll use a view to minimize storage. If this is heavily queried, we can materialize as a table instead."

- "Creating fact table FCT_SALES with 3 dimension predecessors. This will likely be large. Should I use an incremental load pattern or full refresh?"

## Section 6: Dependency Management

**Purpose:** Recognize healthy vs problematic DAG patterns and recommend improvements.

### Healthy DAG Patterns

#### Fan-out (1 → many)

**Pattern:**
One staging node feeds multiple downstream transformations.

**Example:**
```
STG_CUSTOMERS → DIM_CUSTOMER
              → DIM_CUSTOMER_SEGMENT
              → FCT_ORDERS (join with other dims)
```

**Assessment:** ✅ Healthy
- Promotes reusability
- Single source of truth for customer data
- Efficient (standardize once, use many times)

#### Fan-in (many → 1)

**Pattern:**
Multiple sources join into one node.

**Sweet spot:** 2-4 predecessors for fact tables, 1-2 for dimensions

**Example:**
```
DIM_CUSTOMER ─┐
DIM_PRODUCT ──┼→ FCT_SALES
DIM_DATE ─────┘
```

**Assessment:** ✅ Healthy for fact tables
- Natural pattern for dimensional modeling
- Manageable complexity

#### Linear chains (1 → 1 → 1)

**Pattern:**
Sequential transformations.

**Acceptable:** 3-5 steps for complex logic broken into readable chunks

**Example:**
```
RAW_ORDERS → STG_ORDERS → INT_ORDERS_CLEAN → INT_ORDERS_METRICS → FCT_ORDERS
```

**Assessment:** ✅ Healthy if each step adds clear value
- Readable, maintainable
- Each transformation focused on single purpose

### Problematic DAG Patterns

#### Excessive fan-in (>5 predecessors)

**Warning sign:**
Node joins 6+ tables in single transformation.

**Issues:**
- Hard to debug (which join caused the issue?)
- Performance bottlenecks
- Fragile (one upstream failure blocks everything)
- Difficult to understand business logic

**Recommendation:**
Break into intermediate nodes.

**Example fix:**
```
❌ BEFORE:
DIM_A ─┐
DIM_B ─┤
DIM_C ─┤
DIM_D ─┼→ FCT_COMPLEX (joins 8 tables at once)
DIM_E ─┤
DIM_F ─┤
DIM_G ─┤
DIM_H ─┘

✅ AFTER:
DIM_A ─┐
DIM_B ─┼→ INT_CORE_DIMS ─┐
DIM_C ─┘                  │
                          ├→ FCT_COMPLEX
DIM_D ─┐                  │
DIM_E ─┼→ INT_EXTENDED ───┘
DIM_F ─┘
```

**AI warning:**
> "This node will join 7 tables. Consider creating intermediate nodes for subsets of joins (e.g., INT_CORE_DIMS for primary dimensions, INT_EXTENDED for secondary)."

#### Deep linear chains (>6 sequential steps)

**Warning sign:**
7+ nodes in a row (1 → 1 → 1 → 1 → 1 → 1 → 1).

**Issues:**
- Hard to understand data lineage
- Unnecessary intermediate materializations (if all tables)
- Long critical path (sequential execution)
- Difficult to troubleshoot (where did the issue start?)

**Recommendation:**
Consolidate transformations or use views for intermediate steps.

**Example fix:**
```
❌ BEFORE (all tables):
RAW → STAGE1 → STAGE2 → CLEAN1 → CLEAN2 → TRANSFORM1 → TRANSFORM2 → FINAL

✅ AFTER (consolidate + use views):
RAW → STG_COMBINED (table) → INT_TRANSFORMS (view) → FINAL (table)
```

**AI warning:**
> "This creates a 7-step linear chain. Consider consolidating some transformations or using views for intermediate steps to reduce materialization overhead."

#### Circular dependencies

**Warning sign:**
Node A depends on B, B depends on A (directly or indirectly).

**Issues:**
- Cannot execute (DAG is not acyclic)
- Indicates logical design flaw

**Recommendation:**
Break the cycle, typically by extracting shared logic to new upstream node.

**Example fix:**
```
❌ BEFORE:
NODE_A → NODE_B → NODE_C → NODE_A (circular!)

✅ AFTER:
SHARED_LOGIC → NODE_A → NODE_B → NODE_C
```

**AI error:**
This should be caught by Coalesce platform, but if detected:
> "This creates a circular dependency. Extract the shared transformation logic into a separate upstream node."

#### Cross-layer dependencies (anti-pattern)

**Warning sign:**
Gold layer node directly reads from Bronze, skipping Silver.

**Issues:**
- Violates layer separation principle
- Makes lineage unclear
- Difficult to maintain consistency
- Quality checks bypassed

**Recommendation:**
Ensure transformations flow through appropriate layers.

**Example fix:**
```
❌ BEFORE:
RAW_CUSTOMERS → (skip staging) → DIM_CUSTOMER

✅ AFTER:
RAW_CUSTOMERS → STG_CUSTOMERS → DIM_CUSTOMER
```

**AI warning:**
> "This dimension reads directly from Bronze (RAW_CUSTOMERS), skipping the Silver staging layer. Consider routing through STG_CUSTOMERS for consistent standardization and quality checks."

### DAG Best Practices

**Reuse staging nodes:**
- Create once, use in multiple downstream transformations
- Single source of truth for each entity

**Keep joins focused:**
- 2-4 tables per join is manageable
- >5 tables → break into intermediate steps

**Layer appropriately:**
- Don't skip layers (Bronze → Silver → Gold)
- Each layer has purpose

**Use intermediate nodes strategically:**
- Break complex logic into understandable chunks
- Each node should have clear, single purpose
- Balance between too granular (overhead) and too complex (unreadable)

**Aim for parallelizable DAGs:**
- Wide DAGs (fan-out) can execute in parallel
- Deep linear chains are sequential bottlenecks
- Prefer: Bronze → [multiple staging nodes in parallel] → [multiple marts in parallel]

### AI Decision Logic

```
When creating a node:

IF predecessorNodeIDs.length > 5:
  → Warn: "This joins 6+ tables. Consider intermediate nodes for subsets."

IF creating 7th node in linear chain:
  → Warn: "Deep chain detected. Could some steps be consolidated or use views?"

IF new node creates cross-layer skip:
  → Warn: "This skips the [layer] layer. Consider routing through proper layers."

When analyzing existing workspace:
  → Identify bottleneck nodes (high fan-in + high fan-out)
  → Suggest breaking into focused, single-purpose nodes
```

## Section 7: Package Recommendations

**Purpose:** Guide AI on when and how to recommend packages from github.com/coalesceio.

### Base Nodes Package

**Detection:**
Look for `base-nodes:::*` in any node type from workspace analysis.

**When to recommend:**
- Workspace uses built-in types (Stage, View, Dimension, Fact)
- User is creating new workspace or early in development
- Enhanced features would benefit the use case

**Key advantages of base-nodes:**
- Additional configuration options
- Better defaults and validation
- Active maintenance
- Consistent patterns across node types

**Recommendation approach:**

```
IF workspace has NO base-nodes types:
  Soft recommendation when creating first node:
  → "The base-nodes package (github.com/coalesceio/base-nodes) offers enhanced
     versions of Stage, View, Dimension, and Fact nodes with additional config
     options. To install: Build Settings → Packages in Coalesce UI.
     Would you like to use base-nodes:::Stage for this node?"

  Don't push hard - respect existing patterns.

IF workspace HAS base-nodes types:
  → Use base-nodes versions by default (match workspace pattern)
  → "Your workspace uses base-nodes. I'll use base-nodes:::Stage for this staging node."
```

### Specialized Packages

#### Incremental-Nodes

**Package:** github.com/coalesceio/Incremental-Nodes

**Detection signals:**
- Large fact tables (>1M rows inferred)
- User mentions: "incremental", "delta", "high-water mark", "large dataset"
- Time-series data patterns (DATE/TIMESTAMP columns, event logs)

**Node types provided:**
- Incremental Load (efficient delta processing with merge)
- Test Passed Records (DQ filtering)
- Test Failed Records (quarantine bad data)
- Looped Load (grouped incremental processing)
- Run View (orchestration metadata)
- Grouped Incremental Load (aggregation + incremental)

**When to recommend:**
```
IF creating large fact table OR user mentions incremental:
  → "For large-scale incremental loading, check out github.com/coalesceio/Incremental-Nodes.
     It provides specialized node types for efficient delta processing with built-in
     high-water mark logic and data quality filtering.
     To install: Build Settings → Packages in Coalesce UI."

IF workspace already uses Incremental-Nodes package:
  → Use those node types for appropriate scenarios
```

#### Semantic Node Types

**Package:** github.com/coalesceio/semantic-node-types

**Detection signals:**
- User mentions: "metrics", "semantic layer", "business definitions", "KPIs"
- Workspace has metric/aggregation patterns

**When to recommend:**
```
IF user mentions semantic layer or metrics:
  → "For building a semantic layer, explore github.com/coalesceio/semantic-node-types
     which provides node types designed for metric definitions and business logic.
     To install: Build Settings → Packages in Coalesce UI."
```

#### Platform-Specific Packages

**Databricks DLT:**
- Package: github.com/coalesceio/databricks-DLT
- When: User is on Databricks platform, mentions Delta Live Tables

**BigQuery Advanced:**
- Package: github.com/coalesceio/big-query-base-node-types-advanced-deploy
- When: User is on BigQuery platform, needs advanced features

**Dynamic Tables:**
- Package: github.com/coalesceio/Dynamic-Table-Nodes
- When: User mentions dynamic tables, continuous refresh patterns

**Materialized Views:**
- Package: github.com/coalesceio/Materialized-View-Node
- When: User wants materialized views (hybrid view/table)

**Streams and Tasks:**
- Package: github.com/coalesceio/Streams-and-Task-Nodes
- When: User mentions event-driven processing, CDC patterns

#### Methodology Packages

**Data Vault:**
- When workspace shows hub/satellite/link patterns
- Point to github.com/coalesceio to search for Data Vault packages
- To install: Build Settings → Packages in Coalesce UI
- Don't assume package names - let user discover

**Cortex Integration:**
- Package: github.com/coalesceio/Cortex-Node-types
- When: User mentions Snowflake Cortex, ML/AI features

### Package Recommendation Decision Logic

```
When recommending packages:

1. Check if workspace already uses packages:
   IF yes: Match existing patterns (don't mix unless user requests)

2. If workspace uses only built-in types:
   IF creating first few nodes:
     → Soft recommend base-nodes
     → "The base-nodes package offers enhanced versions. See github.com/coalesceio/base-nodes"
   ELSE:
     → Don't push packages on established workspaces
     → Only mention if directly relevant to user's question

3. If user's need matches specialized package:
   → Point to specific package on github.com/coalesceio
   → Explain benefits briefly
   → Remind: "To install: Build Settings → Packages in Coalesce UI"
   → Let user decide

4. If user asks about methodology/pattern:
   → "Explore github.com/coalesceio for packages that might support [Data Vault/semantic/etc]"
   → "To install packages: Build Settings → Packages in Coalesce UI"

5. Never assume package is installed:
   → "If you have the Incremental-Nodes package installed, you could use..."
   → NOT: "Use the Incremental Load node type" (might not be available)
```

### Exploration Guidance

**When user is starting fresh or exploring options:**

Provide pointers without overwhelming:
- "The Coalesce organization on GitHub (github.com/coalesceio) offers packages that extend node type capabilities."
- "Packages cover specialized methodologies (Data Vault), platform optimizations (Databricks DLT), and workflow patterns (incremental loading)."
- "To install packages: In Coalesce UI, go to Build Settings → Packages to browse and install available packages."
- "Once you've installed a package, I can help configure nodes using those types."

**Don't:**
- List every package unprompted
- Assume what the user needs
- Push packages when simpler built-in types work fine
- Assume a package is installed without checking workspace

**Do:**
- Respond to user's expressed needs
- Point to relevant packages when applicable
- Direct users to Build Settings → Packages for installation
- Let user explore and choose

## Section 8: Workspace Analysis Tool

**Purpose:** New tool to analyze workspace patterns and generate lightweight profiles.

### Tool: `analyze-workspace-patterns`

**Description:**
Analyzes a workspace's node types, DAG topology, and lineage patterns to generate a cached profile for AI recommendations.

**Parameters:**

```typescript
{
  workspaceID: string;        // Required: workspace to analyze
  forceRefresh?: boolean;     // Optional: ignore cached data, re-analyze
}
```

**Behavior:**

1. **Load or fetch node data:**
   - Check `data/nodes/workspace-{workspaceID}-nodes.json`
   - If exists and recent (< 24 hours) → use cached
   - If missing or stale OR forceRefresh=true → call `list-workspace-nodes`, save to file

2. **Analyze patterns:**
   - Package adoption (scan for `base-nodes:::*`, other packages)
   - Layer patterns (group by predecessor count, naming conventions)
   - Methodology detection (Kimball, Data Vault, dbt-style)
   - Node type usage per layer

3. **Generate profile:**
   - Save to `data/workspace-{workspaceID}-profile.json`:

   ```json
   {
     "workspaceID": "abc-123",
     "analyzedAt": "2026-03-20T10:30:00Z",
     "nodeCount": 1247,
     "sampleSize": 1247,
     "packageAdoption": {
       "base-nodes": true,
       "incremental-nodes": false,
       "packages": ["base-nodes"]
     },
     "layerPatterns": {
       "bronze": {
         "nodeTypes": ["Stage"],
         "count": 45
       },
       "staging": {
         "nodeTypes": ["Stage", "base-nodes:::Stage"],
         "count": 234,
         "dominant": "base-nodes:::Stage"
       },
       "intermediate": {
         "nodeTypes": ["View", "base-nodes:::View"],
         "count": 512,
         "dominant": "base-nodes:::View"
       },
       "marts": {
         "dimensions": {
           "nodeTypes": ["Dimension", "base-nodes:::Dimension"],
           "count": 89,
           "dominant": "base-nodes:::Dimension"
         },
         "facts": {
           "nodeTypes": ["Fact", "base-nodes:::Fact"],
           "count": 367,
           "dominant": "base-nodes:::Fact"
         }
       }
     },
     "methodology": "kimball",
     "recommendations": {
       "defaultPackage": "base-nodes",
       "stagingType": "base-nodes:::Stage",
       "transformType": "base-nodes:::View",
       "dimensionType": "base-nodes:::Dimension",
       "factType": "base-nodes:::Fact"
     }
   }
   ```

4. **Return summary:**
   - Human-readable summary of findings
   - Detected patterns and recommendations
   - File paths where data was saved

**When to use:**

- User is new to a workspace
- Before bulk node creation
- When patterns change (new packages installed)
- User explicitly asks: "What patterns does my workspace use?"

**AI Integration:**

Before creating nodes, AI should:

1. Check if profile exists and is recent
2. If missing → suggest: "I can analyze your workspace patterns first with `analyze-workspace-patterns`. This will make node creation recommendations more accurate."
3. Use profile data for all subsequent recommendations
4. If profile is stale (> 7 days old) → suggest refresh

## Implementation Notes

### Integration with Existing Resources

This resource complements existing resources:

- **sql-platforms.md:** Platform-specific SQL rules (Snowflake, Databricks, BigQuery)
- **node-selection.md:** Original node selection guidance (may be deprecated/replaced)
- **storage-mappings.md:** Storage location concepts and {{ ref() }} syntax
- **tool-usage.md:** Tool batching, parallelization patterns

### When AI Should Read This Resource

Load this resource when:
- User invokes `create-workspace-node` tools
- User mentions: dimension, fact, staging, hub, satellite, incremental, etc.
- User asks about data modeling approaches
- User requests workspace evaluation/review

### Confidence and Honesty

**High confidence areas (teach authoritatively):**
- General data engineering principles
- Layer-based architecture patterns
- DAG topology analysis
- Best practices for dependencies

**Medium confidence areas (point to resources):**
- Package-specific features → github.com/coalesceio
- Platform-specific optimizations → sql-platforms.md

**Low confidence areas (acknowledge limitations):**
- Custom node type template behavior → "Review node definition in Coalesce UI"
- Unpublished packages or internal tools → "Check with your team or Coalesce support"

**Always:**
- Be honest about limitations
- Point to verification sources
- Acknowledge when speculating vs. certain

## Success Criteria

This resource is successful if the AI:

1. **Analyzes before recommending:** Calls `list-workspace-nodes` and evaluates patterns
2. **Recognizes methodologies:** Identifies Kimball, Data Vault, dbt-style from DAG structure
3. **Recommends improvements:** Suggests better node types/patterns when appropriate
4. **Respects existing patterns:** Doesn't force changes on established workspaces
5. **Provides rationale:** Explains why a recommendation is made
6. **Knows when to ask:** Seeks user input for high-stakes decisions (methodology choice, package adoption)
7. **Avoids hallucination:** Points to github.com/coalesceio rather than inventing packages

## Future Enhancements

Potential additions as product evolves:

- **Testing patterns:** Node-level tests, data quality frameworks
- **Performance tuning:** Clustering, partitioning, optimization techniques
- **CI/CD patterns:** Deployment strategies, environment promotion
- **Governance:** Naming conventions, documentation standards, lineage management
- **Cost optimization:** Storage vs compute tradeoffs, query optimization

These can be added as new sections or separate resources as needed.
