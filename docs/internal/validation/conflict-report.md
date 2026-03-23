# Topic Conflict Analysis and Ownership Assignment

This document analyzes all 31 topics appearing in 2+ resources, identifies conflicts, assigns ownership, and provides resolution guidance.

## Analysis Summary

**Total topics analyzed:** 31

**Conflict types found:**
- Contradiction: 0
- Inconsistent detail: 5
- Redundancy: 17
- No conflict - complementary coverage: 9

---

## SQL Topics

### 1. Coalesce ref() Syntax

**Conflict Type:** Redundancy

**Current State:**
- sql-platform-selection.md: High-level rule to prefer `{{ref()}}` for node references
- storage-mappings.md: Comprehensive reference syntax documentation with examples, argument structure, workflow patterns
- sql-databricks.md: Notes that SQL casing rules don't apply to ref() arguments
- sql-bigquery.md: Notes that SQL casing rules don't apply to ref() arguments

**Owner Assignment:** storage-mappings.md

**Rationale:** storage-mappings.md is the dedicated comprehensive resource for ref() syntax with examples, workflow patterns, and the full two-argument structure. The SQL dialect resources only provide brief clarifications about casing exceptions. sql-platform-selection.md provides only a high-level directive.

**Resolution:**
- storage-mappings.md retains comprehensive ref() documentation
- sql-platform-selection.md keeps high-level directive to prefer ref(), removes implementation details, delegates to storage-mappings.md
- sql-databricks.md and sql-bigquery.md retain their specific casing clarification notes (these are dialect-specific exceptions)

---

### 2. Identifier Conventions and Casing

**Conflict Type:** No conflict - complementary coverage

**Current State:**
- sql-snowflake.md: UPPERCASE unquoted identifiers, double quotes when needed
- sql-databricks.md: lowercase identifiers, backticks only when needed
- sql-bigquery.md: lowercase identifiers, backticks only when needed
- storage-mappings.md: Notes that storage/table names are case-sensitive

**Owner Assignment:** Distributed (each SQL dialect resource owns its platform)

**Rationale:** These are platform-specific conventions with no conflict. Each dialect resource correctly describes casing for its platform. storage-mappings.md provides the important cross-cutting rule that ref() arguments must match actual Coalesce object names exactly (not be normalized to platform casing).

**Resolution:**
- No changes needed
- Each SQL dialect resource retains its casing guidance
- storage-mappings.md retains the case-sensitivity warning for ref() arguments

---

### 3. Editing Guidance

**Conflict Type:** Redundancy

**Current State:**
- sql-snowflake.md: Preserve existing workspace style, don't convert working identifiers, don't change existing casing
- sql-databricks.md: Don't lowercase mechanically, preserve existing alias style, avoid rewriting ref() arguments
- sql-bigquery.md: Preserve existing quoting and casing, don't force qualified names, don't rewrite ref() arguments
- hydrated-metadata.md: Prefer update-workspace-node for partial changes, preserve unknown keys

**Owner Assignment:** sql-platform-selection.md (high-level), individual SQL dialect resources (platform-specific)

**Rationale:** sql-platform-selection.md establishes the overarching principle (preserve existing style, avoid broad rewrites). Each dialect resource applies this principle to platform-specific contexts. hydrated-metadata.md's guidance is about node payload editing, not SQL style.

**Resolution:**
- sql-platform-selection.md expands editing principles section to be the canonical source for "preserve workspace conventions"
- SQL dialect resources keep platform-specific applications of this principle
- hydrated-metadata.md keeps its payload-editing guidance (different domain)

---

### 4. Common SQL Patterns

**Conflict Type:** No conflict - complementary coverage

**Current State:**
- sql-snowflake.md: Snowflake-specific patterns (UPPERCASE, ref() examples, function guidance)
- sql-databricks.md: Databricks-specific patterns (lowercase, ref() examples, function guidance)
- sql-bigquery.md: BigQuery-specific patterns (lowercase, ref() examples, function guidance)

**Owner Assignment:** Distributed (each SQL dialect resource owns its platform)

**Rationale:** Each resource provides platform-specific examples that complement each other. No conflict.

**Resolution:**
- No changes needed
- Each resource retains its platform-specific patterns

---

### 5. SQL Platform Detection and Dialect

**Conflict Type:** Redundancy

**Current State:**
- overview.md: Brief mention to "determine platform first, then follow dialect-specific resource"
- sql-platform-selection.md: Comprehensive 4-step detection methodology (project metadata → existing SQL → neighboring nodes → ask user)

**Owner Assignment:** sql-platform-selection.md

**Rationale:** This is the dedicated comprehensive resource for platform detection with detailed methodology.

**Resolution:**
- sql-platform-selection.md retains comprehensive detection methodology
- overview.md keeps brief workflow mention, delegates to sql-platform-selection.md for details

---

### 6. Snowflake Authentication

**Conflict Type:** No conflict - complementary coverage

**Current State:**
- sql-platform-selection.md: Notes that run-tool auth is Snowflake Key Pair-based, but doesn't mean every project uses Snowflake SQL
- run-operations.md: Notes that run-triggering tools require Snowflake Key Pair auth with environment variables

**Owner Assignment:** run-operations.md

**Rationale:** run-operations.md owns the operational authentication requirement. sql-platform-selection.md provides an important clarification that auth method ≠ SQL dialect.

**Resolution:**
- run-operations.md retains authentication requirements
- sql-platform-selection.md retains its clarification note (prevents misconception)

---

## Node Topics

### 7. Node Creation Workflow

**Conflict Type:** Redundancy

**Current State:**
- overview.md: High-level workflow sequence (inspect → brainstorm → plan → resolve IDs → determine platform → choose workflow → create → verify)
- tool-usage.md: Tool selection criteria (when to use which helper)
- node-creation-decision-tree.md: Complete decision tree for tool selection with specific conditions

**Owner Assignment:** node-creation-decision-tree.md

**Rationale:** This resource is entirely dedicated to the node creation decision flow with specific conditions and tool routing logic. overview.md provides context integration. tool-usage.md provides tool usage patterns.

**Resolution:**
- node-creation-decision-tree.md retains comprehensive decision tree
- overview.md keeps high-level workflow sequence, delegates to decision tree for tool selection
- tool-usage.md keeps tool usage patterns and helper characteristics, delegates to decision tree for "which tool" decisions

---

### 8. Node Type Selection

**Conflict Type:** Inconsistent detail

**Current State:**
- overview.md: Brief mention to "consult data-engineering principles when node type is unclear"
- data-engineering-principles.md: Comprehensive methodology with workspace pattern analysis, package detection, DAG topology, lineage-based detection, layer-specific recommendations

**Owner Assignment:** data-engineering-principles.md

**Rationale:** This resource is entirely dedicated to node type selection with sophisticated analysis framework. overview.md provides only a workflow pointer.

**Resolution:**
- data-engineering-principles.md retains comprehensive node type selection methodology
- overview.md keeps workflow pointer, strengthens delegation to data-engineering-principles.md

**Potential Issue:** overview.md's "Creating Nodes" workflow mentions "consult data-engineering principles when node type is unclear" but doesn't emphasize that analyze-workspace-patterns should often be the first step. Consider clarifying that pattern analysis comes before creation.

---

### 9. Scratch and Placeholder Node Creation

**Conflict Type:** Inconsistent detail

**Current State:**
- tool-usage.md: Describes scratch node creation use cases, default configured target, need for name/columns/config, lowering completion level for placeholders
- node-creation-decision-tree.md: Decision conditions for scratch vs placeholder, completion level choices
- node-payloads.md: Notes scratch creation expectations

**Owner Assignment:** node-creation-decision-tree.md (routing), tool-usage.md (detailed behavior)

**Rationale:** Decision tree owns the "when to use scratch creation" logic. tool-usage.md provides comprehensive detail on how scratch creation works and what it expects.

**Resolution:**
- node-creation-decision-tree.md retains decision routing for scratch vs other creation paths
- tool-usage.md retains detailed scratch creation behavior and expectations
- node-payloads.md note about scratch expectations should delegate to tool-usage.md

**Potential Issue:** Three resources discuss scratch creation at different levels. Ensure node-payloads.md doesn't duplicate what tool-usage.md already covers comprehensively.

---

### 10. Predecessor-Based Node Creation

**Conflict Type:** Redundancy

**Current State:**
- tool-usage.md: Comprehensive guidance on predecessor-based creation (use cases, important validation fields, multi-predecessor warnings)
- node-creation-decision-tree.md: Decision conditions for when to use predecessor-based creation

**Owner Assignment:** tool-usage.md (implementation), node-creation-decision-tree.md (routing)

**Rationale:** Decision tree routes to the tool. tool-usage.md provides comprehensive implementation guidance.

**Resolution:**
- tool-usage.md retains comprehensive predecessor-based creation guidance
- node-creation-decision-tree.md retains routing logic, delegates to tool-usage.md for implementation details

---

### 11. Node Update Operations

**Conflict Type:** Redundancy

**Current State:**
- tool-usage.md: Comprehensive update guidance (when to use update-workspace-node, use cases, when to use set-workspace-node instead)
- node-creation-decision-tree.md: Decision routing for updates vs creation
- node-payloads.md: Brief note to prefer update-workspace-node

**Owner Assignment:** tool-usage.md

**Rationale:** tool-usage.md provides comprehensive update operation guidance with use cases and tool selection.

**Resolution:**
- tool-usage.md retains comprehensive update guidance
- node-creation-decision-tree.md retains routing logic for update vs create decisions
- node-payloads.md delegates to tool-usage.md instead of restating the preference

---

### 12. Validation Inspection After Creation/Writes

**Conflict Type:** Redundancy

**Current State:**
- node-creation-decision-tree.md: Lists validation fields to inspect after multi-predecessor joins
- node-payloads.md: Lists validation and warning fields to inspect after helper-based writes

**Owner Assignment:** node-payloads.md

**Rationale:** This is about node payload validation, which is node-payloads.md's domain. The resource provides general validation inspection rules.

**Resolution:**
- node-payloads.md retains validation inspection guidance
- node-creation-decision-tree.md can reference validation inspection but should delegate details to node-payloads.md

---

### 13. Rename Safety

**Conflict Type:** Redundancy

**Current State:**
- tool-usage.md: Verify saved node name, downstream nodes not automatically updated, update dependents explicitly
- node-payloads.md: Verify saved node body, verify related mapping/source fields, don't assume downstream auto-update

**Owner Assignment:** node-payloads.md

**Rationale:** Rename safety is fundamentally about node payload integrity and field consistency, which is node-payloads.md's domain.

**Resolution:**
- node-payloads.md retains comprehensive rename safety guidance
- tool-usage.md can delegate to node-payloads.md for rename safety details

---

### 14. Storage Location References

**Conflict Type:** Redundancy

**Current State:**
- storage-mappings.md: Comprehensive coverage of storage locations, finding them, ref() syntax, workflow patterns, common errors
- node-payloads.md: Notes that storageLocations and top-level location fields exist, need verification
- hydrated-metadata.md: Notes storageLocations array structure
- node-creation-decision-tree.md: Brief mention to verify storage assumptions after creation
- overview.md: Brief workflow mention

**Owner Assignment:** storage-mappings.md

**Rationale:** This is the dedicated comprehensive resource for storage location concepts, finding them, and using them in ref() syntax.

**Resolution:**
- storage-mappings.md retains comprehensive coverage
- node-payloads.md and hydrated-metadata.md delegate to storage-mappings.md for storage location details, keep only minimal structure notes
- node-creation-decision-tree.md and overview.md keep brief workflow pointers to storage-mappings.md

---

### 15. Higher-Level Helper Preference

**Conflict Type:** Redundancy

**Current State:**
- tool-usage.md: Comprehensive guidance on preferring high-level helpers (create-from-predecessor, create-from-scratch, update-workspace-node) over low-level primitives
- hydrated-metadata.md: Notes when to use raw hydrated input (advanced cases, custom config)
- node-payloads.md: Brief note to prefer update-workspace-node

**Owner Assignment:** tool-usage.md

**Rationale:** tool-usage.md provides the comprehensive tool selection hierarchy and helper preference logic.

**Resolution:**
- tool-usage.md retains comprehensive helper preference guidance
- hydrated-metadata.md retains its specific guidance on when raw hydrated input is appropriate (complementary)
- node-payloads.md delegates to tool-usage.md for helper preference

---

## ID and Discovery Topics

### 16. Workspace Discovery and IDs

**Conflict Type:** Redundancy

**Current State:**
- tool-usage.md: Notes workspace IDs are nested, need includeWorkspaces flag, broader discovery patterns
- id-discovery.md: Specific workspace ID resolution steps with includeWorkspaces examples

**Owner Assignment:** id-discovery.md

**Rationale:** This is the dedicated ID resolution resource with specific lookup patterns.

**Resolution:**
- id-discovery.md retains workspace ID discovery steps
- tool-usage.md keeps discovery patterns section, delegates workspace ID specifics to id-discovery.md

---

### 17. Project Metadata Inspection

**Conflict Type:** No conflict - complementary coverage

**Current State:**
- overview.md: Workflow step "inspect project and workspace context first"
- sql-platform-selection.md: Step 1 detection method "check project metadata first" for platform detection

**Owner Assignment:** Distributed (overview.md for workflow, sql-platform-selection.md for platform detection)

**Rationale:** overview.md establishes the general workflow principle. sql-platform-selection.md applies it specifically to platform detection. No conflict, complementary coverage.

**Resolution:**
- No changes needed
- overview.md retains workflow principle
- sql-platform-selection.md retains specific application to platform detection

---

### 18. Job ID Resolution

**Conflict Type:** Redundancy

**Current State:**
- id-discovery.md: Job ID lookup steps with includeJobs flag
- run-operations.md: Notes start-run needs jobID not job name, resolve it first

**Owner Assignment:** id-discovery.md

**Rationale:** id-discovery.md is the dedicated ID resolution resource.

**Resolution:**
- id-discovery.md retains job ID resolution steps
- run-operations.md keeps operational note about jobID requirement, delegates resolution steps to id-discovery.md

---

### 19. Run Counter vs URL UUID

**Conflict Type:** Redundancy

**Current State:**
- id-discovery.md: Explains run counters vs UUIDs, notes numeric run ID usage
- run-operations.md: Explains which tools use runCounter vs UUID

**Owner Assignment:** run-operations.md

**Rationale:** This is fundamentally about run operations and which tools expect which ID format.

**Resolution:**
- run-operations.md retains comprehensive run ID format guidance
- id-discovery.md can provide basic explanation but should delegate to run-operations.md for operational details

---

### 20. Org ID Resolution

**Conflict Type:** Redundancy

**Current State:**
- id-discovery.md: Notes cancel-run needs orgID via explicit param or COALESCE_ORG_ID env var
- run-operations.md: Notes cancel-run needs orgID with same fallback pattern

**Owner Assignment:** run-operations.md

**Rationale:** Org ID requirement is tied to the cancel-run operation, which is run-operations.md's domain.

**Resolution:**
- run-operations.md retains org ID requirement for cancel-run
- id-discovery.md can delegate org ID context to run-operations.md for the cancel operation

---

## Run Topics

### 21. Run Operations and Workflows

**Conflict Type:** Inconsistent detail

**Current State:**
- tool-usage.md: Brief run workflow guidance (when to use run-and-wait vs start-run/run-status, run result handling, cache tools)
- run-operations.md: Comprehensive run operations resource (tool selection, end-to-end helpers, lifecycle control, diagnostics, result handling)
- systematic-debugging.md: Run issue diagnosis as part of debugging methodology

**Owner Assignment:** run-operations.md

**Rationale:** This is the dedicated comprehensive run operations resource. tool-usage.md provides general tool usage context. systematic-debugging.md focuses on debugging failures.

**Resolution:**
- run-operations.md retains comprehensive run operations guidance
- tool-usage.md keeps brief run workflow section, delegates detailed run operations to run-operations.md
- systematic-debugging.md retains debugging-specific run diagnosis guidance

**Potential Issue:** tool-usage.md's "Preferred Run Workflows" section duplicates run-operations.md's "Tool Selection" and "End-to-End Helpers" sections. Consider consolidating by having tool-usage.md delegate to run-operations.md.

---

### 22. Canceling Runs

**Conflict Type:** Redundancy

**Current State:**
- tool-usage.md: Lists cancel-run requirements (runID, environmentID, orgID)
- run-operations.md: Lists cancel-run requirements with same parameters

**Owner Assignment:** run-operations.md

**Rationale:** This is part of run operations, which is run-operations.md's domain.

**Resolution:**
- run-operations.md retains cancel-run guidance
- tool-usage.md delegates to run-operations.md for run cancellation details

---

## Data and Caching Topics

### 23. Data Caching Strategy

**Conflict Type:** Redundancy

**Current State:**
- data-engineering-principles.md: Comprehensive data caching strategy for workspace analysis (cache file paths, tool behavior, refresh logic, benefits)
- tool-usage.md: Brief mention of cache-related tools (analyze-workspace-patterns, cache-environment-nodes, cache-workspace-nodes, cache-runs)

**Owner Assignment:** data-engineering-principles.md (strategy), tool-usage.md (tool catalog)

**Rationale:** data-engineering-principles.md establishes the caching strategy in context of workspace pattern analysis. tool-usage.md catalogs available cache tools.

**Resolution:**
- data-engineering-principles.md retains comprehensive workspace analysis caching strategy
- tool-usage.md retains tool catalog entries for cache tools, delegates caching strategy to data-engineering-principles.md

---

### 24. Array Replacement Semantics

**Conflict Type:** Redundancy

**Current State:**
- node-payloads.md: Arrays are replace-on-write unless tool documents merge semantics, especially metadata.columns
- hydrated-metadata.md: Brief mention that arrays are full-replacement fields

**Owner Assignment:** node-payloads.md

**Rationale:** This is fundamentally about node payload editing safety, which is node-payloads.md's domain.

**Resolution:**
- node-payloads.md retains comprehensive array safety guidance
- hydrated-metadata.md can reference node-payloads.md for array safety details

---

## Workflow and Process Topics

### 25. Tool Selection Patterns

**Conflict Type:** Inconsistent detail

**Current State:**
- overview.md: Implicit tool selection throughout workflow patterns
- tool-usage.md: Comprehensive tool selection rules (Core Rules section, prefer smallest sufficient tool, list vs get, helper preference)
- node-creation-decision-tree.md: Tool selection logic specifically for node creation/update paths
- run-operations.md: Tool selection for run operations
- writing-plans.md: Brief mention of verifying tools exist before including in plans

**Owner Assignment:** Distributed by domain

**Rationale:** This is a cross-cutting pattern where each resource owns tool selection within its domain. tool-usage.md provides general principles.

**Resolution:**
- tool-usage.md retains general tool selection principles (smallest sufficient tool, helper preference, list vs get)
- Domain-specific resources (node-creation-decision-tree.md, run-operations.md) retain tool selection within their domains
- overview.md keeps workflow context
- writing-plans.md keeps its specific planning check

**Potential Issue:** There's potential overlap between tool-usage.md's general principles and domain-specific tool selection. Ensure consistent messaging by having tool-usage.md establish principles that domain resources apply.

---

### 26. Brainstorming Before Mutation

**Conflict Type:** No conflict - complementary coverage

**Current State:**
- overview.md: Workflow step "brainstorm first when request is ambiguous"
- brainstorming.md: Comprehensive brainstorming methodology (when to use, core behavior, question patterns, when to stop)
- writing-plans.md: Notes to ask focused questions before planning if critical unknowns block execution

**Owner Assignment:** brainstorming.md

**Rationale:** brainstorming.md is the dedicated resource for the brainstorming methodology. overview.md integrates it into workflow. writing-plans.md references it appropriately.

**Resolution:**
- brainstorming.md retains comprehensive methodology
- overview.md keeps workflow integration
- writing-plans.md keeps appropriate reference to clarification before planning

---

### 27. Good Default Sequences

**Conflict Type:** No conflict - complementary coverage

**Current State:**
- tool-usage.md: Default sequences for node creation (manual configured, from predecessors, modify existing)
- id-discovery.md: Default sequence for ID resolution (discover by name → resolve ID → use tool)
- run-operations.md: Default sequences for run operations (start and monitor, retry failed run)

**Owner Assignment:** Distributed by domain

**Rationale:** Each resource owns default sequences within its domain. No conflict - these are domain-specific patterns that complement each other.

**Resolution:**
- No changes needed
- Each resource retains its domain-specific default sequences

---

### 28. Plan Structure and Execution

**Conflict Type:** No conflict - complementary coverage

**Current State:**
- writing-plans.md: Comprehensive plan writing methodology (structure, quality rules, Coalesce-specific checks)
- node-creation-decision-tree.md: Decision tree implies planning by providing structured decision logic
- brainstorming.md: Notes saving brainstorm results, transition to execution

**Owner Assignment:** writing-plans.md

**Rationale:** writing-plans.md is the dedicated comprehensive resource for plan structure and execution methodology.

**Resolution:**
- writing-plans.md retains comprehensive plan writing methodology
- node-creation-decision-tree.md provides decision structure (complementary)
- brainstorming.md handles brainstorm-to-plan transition appropriately

---

### 29. Verification and Validation Workflows

**Conflict Type:** Inconsistent detail

**Current State:**
- verification-before-completion.md: Comprehensive verification methodology (core rules, Coalesce patterns for nodes/runs/cache/code, verification checklist)
- writing-plans.md: Notes to include verification in plan structure
- tool-usage.md: Notes to inspect validation fields after helpers before continuing
- run-operations.md: Result handling (check status, results, resultsError, incomplete)

**Owner Assignment:** verification-before-completion.md

**Rationale:** verification-before-completion.md is the dedicated comprehensive verification resource. Other resources appropriately integrate verification into their workflows.

**Resolution:**
- verification-before-completion.md retains comprehensive verification methodology
- writing-plans.md, tool-usage.md, and run-operations.md keep domain-specific verification integration, can strengthen delegation to verification-before-completion.md

**Potential Issue:** Multiple resources mention validation/verification at different levels. Ensure consistent messaging that verification-before-completion.md is the comprehensive source and other resources appropriately delegate.

---

### 30. Debugging and Problem Diagnosis

**Conflict Type:** No conflict - complementary coverage

**Current State:**
- systematic-debugging.md: Comprehensive debugging methodology (core behavior, flow, Coalesce-specific rules, what to record)
- tool-usage.md: Brief note to use systematic-debugging resource for tool/API behavior issues
- run-operations.md: Run diagnostics as part of operations (get-run-details usage)
- verification-before-completion.md: References debugging as related resource

**Owner Assignment:** systematic-debugging.md

**Rationale:** systematic-debugging.md is the dedicated comprehensive debugging resource. Other resources appropriately reference it.

**Resolution:**
- systematic-debugging.md retains comprehensive debugging methodology
- tool-usage.md and verification-before-completion.md keep appropriate references
- run-operations.md retains operational diagnostic tool guidance (complementary)

---

### 31. Code Review and Quality Assurance

**Conflict Type:** No conflict - complementary coverage

**Current State:**
- requesting-code-review.md: Comprehensive code review methodology (review standard, what to look for, review flow)
- verification-before-completion.md: Verification checklist includes "run smallest relevant automated verification"
- systematic-debugging.md: Notes what to record in debugging results

**Owner Assignment:** requesting-code-review.md

**Rationale:** requesting-code-review.md is the dedicated code review resource. verification-before-completion.md focuses on verifying work completion. systematic-debugging.md focuses on debugging process.

**Resolution:**
- requesting-code-review.md retains comprehensive code review methodology
- verification-before-completion.md retains completion verification (complementary focus)
- systematic-debugging.md retains debugging documentation (complementary focus)

---

## Additional Topics Found During Analysis

During the analysis, the following cross-cutting patterns were observed but are not in the 31-topic matrix:

1. **Resource trigger patterns**: overview.md, tool-usage.md, and individual process resources (brainstorming.md, writing-plans.md, verification-before-completion.md, systematic-debugging.md, requesting-code-review.md) all discuss when to use which resource. This is appropriate cross-cutting guidance with no conflicts.

2. **Config and metadata structures**: Multiple resources touch on node config/metadata but from different angles (node-payloads.md for safety, hydrated-metadata.md for structure, data-engineering-principles.md for node types). No conflicts observed.

3. **Documentation references**: Multiple resources reference Coalesce official docs. This is appropriate external referencing with no conflicts.

---

## Summary of Required Changes

### High Priority (Redundancy Elimination)

1. **storage-mappings.md owns ref() syntax**: sql-platform-selection.md should delegate details
2. **tool-usage.md owns helper preference**: node-payloads.md should delegate instead of restating
3. **node-payloads.md owns rename safety**: tool-usage.md should delegate
4. **node-payloads.md owns validation inspection**: node-creation-decision-tree.md should delegate details
5. **run-operations.md owns run workflows**: tool-usage.md should delegate detailed run operations
6. **run-operations.md owns cancel-run**: tool-usage.md should delegate
7. **node-payloads.md owns array safety**: hydrated-metadata.md should delegate

### Medium Priority (Clarification)

8. **Node type selection emphasis**: overview.md should clarify that analyze-workspace-patterns comes before creation
9. **Scratch creation consolidation**: Ensure node-payloads.md doesn't duplicate tool-usage.md's comprehensive coverage
10. **Verification delegation**: Ensure resources appropriately delegate to verification-before-completion.md
11. **ID resolution boundaries**: Clarify which ID topics belong to id-discovery.md vs run-operations.md

### Low Priority (Strengthening)

12. **SQL editing principles**: sql-platform-selection.md should be clearer as the canonical "preserve workspace style" source
13. **Tool selection consistency**: Ensure tool-usage.md's general principles are consistently applied in domain-specific resources
14. **Cache strategy cross-reference**: Ensure tool-usage.md appropriately references data-engineering-principles.md's caching strategy

---

## Validation Notes

All 31 topics were analyzed. No orphaned references to non-existent resources were found. All resource references in the topic matrix point to existing files in src/resources/context/.

The analysis focused on identifying the conflict pattern and assigning ownership without providing exact line-by-line edits, as requested. Task 4 will handle the actual editing.
