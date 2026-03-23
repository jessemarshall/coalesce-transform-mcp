# MCP Publication Validation Design

## Goal

Validate the coalesce-transform-mcp project for publication to all Coalesce clients by establishing clear topic ownership across all documentation and verifying end-to-end implementation consistency.

## Problem Statement

The project has undergone significant refactoring (splitting SQL platforms, adding data-engineering-principles, adding 5 new operational resources). Before publishing to all Coalesce clients, we need to ensure:

1. No conflicting guidance across the 13 resource files (1,436 lines)
2. All 41 tool descriptions reference correct resources
3. User documentation (README, usage-guide) aligns with resources and tools
4. Implementation matches all documentation promises

**Risk**: Conflicting or overlapping advice across resources will confuse LLMs and lead to incorrect behavior when users adopt this MCP server.

## Approach

**Layer-by-Layer Validation** — systematic bottom-up review establishing stable foundation before validating derivatives.

```
┌─────────────────────────────────────────┐
│ Layer 1: Resource Files (13 files)     │  ← Source of truth
│  - Establish topic ownership           │
│  - Eliminate conflicts                  │
│  - Create cross-reference structure     │
└──────────────┬──────────────────────────┘
               │ validates foundation for ↓
┌──────────────▼──────────────────────────┐
│ Layer 2: Tool Descriptions (41 tools)  │  ← First derivative
│  - Verify resource references           │
│  - Align guidance with resources        │
│  - Check parameter consistency          │
└──────────────┬──────────────────────────┘
               │ validates foundation for ↓
┌──────────────▼──────────────────────────┐
│ Layer 3: User Documentation             │  ← Second derivative
│  - README, usage-guide.md, examples     │
│  - Align with resources and tools       │
│  - Verify setup/workflow instructions   │
└──────────────┬──────────────────────────┘
               │ validates promises via ↓
┌──────────────▼──────────────────────────┐
│ Layer 4: Implementation Verification    │  ← Ground truth
│  - Code behavior matches docs           │
│  - Promised features exist              │
│  - Tests cover documented behavior      │
└─────────────────────────────────────────┘
```

**Why Layer-by-Layer?**

- Ensures systematic completeness (nothing missed)
- Creates stable foundation before validating derivatives
- Natural order matches LLM consumption (read resources → call tools → verify behavior)
- Easier to review each layer independently

## Scope

**Resource Files (13 files, 1,436 lines):**

- `overview.md` — general guidance, workflow patterns
- `data-engineering-principles.md` — node types, layers, methodology
- `sql-platform-selection.md` — platform detection
- `sql-snowflake.md` — Snowflake SQL rules
- `sql-databricks.md` — Databricks SQL rules
- `sql-bigquery.md` — BigQuery SQL rules
- `storage-mappings.md` — {{ref()}} syntax, storage locations
- `tool-usage.md` — batching, parallelization patterns
- `id-discovery.md` — resolving project/workspace/environment/job/node/run IDs
- `node-creation-decision-tree.md` — choosing create/update/set tools
- `node-payloads.md` — working with node body structures
- `hydrated-metadata.md` — advanced metadata structures
- `run-operations.md` — starting, polling, retrying, canceling runs

**Tools (41 tools across 7 modules):**

- `src/tools/nodes.ts` — 10 tools (node CRUD, analyze-workspace-patterns)
- `src/tools/environments.ts` — 5 tools
- `src/tools/runs.ts` — 13 tools
- `src/tools/projects.ts` — 8 tools
- `src/tools/git-accounts.ts` — 2 tools
- `src/tools/users.ts` — 3 tools
- `src/workflows/*.ts` — 4 workflow tools

**User Documentation:**

- `README.md`
- `docs/usage-guide.md`
- `docs/examples.md` (if exists)
- `package.json` (description, keywords)

**Implementation:**

- `src/tools/*.ts`
- `src/workflows/*.ts`
- `src/client.ts`
- `src/resources/index.ts`
- `tests/**/*.test.ts` (189 tests)

## Architecture

### Layer 1: Resource Files Validation

**Objective**: Establish single source of truth for each topic, eliminate conflicts, create cross-reference structure.

**Process:**

1. **Extract Topics** — Read each resource and catalog all distinct topics:
   - Node creation workflow
   - SQL platform detection
   - Column transforms
   - Storage location references
   - Data caching strategy
   - Materialization strategies
   - Package detection
   - etc.

2. **Build Topic Matrix** — For each topic, identify which resources mention it:
   ```
   Topic: Node Creation Workflow
   - overview.md: Section "Creating Nodes" (5-step workflow)
   - data-engineering-principles.md: Section "How to Use This Guide"
   - tool-usage.md: May have guidance on batching node creation
   ```

3. **Identify Conflicts** — For each topic with multiple mentions:
   - **Contradictions** — different advice on same question
   - **Redundancy** — same advice repeated verbatim
   - **Inconsistent detail** — one resource brief, another comprehensive
   - **Orphaned cross-references** — references to deleted resources (e.g., node-selection)

4. **Assign Ownership** — For each topic, decide the single owner:
   - **Owner** — contains full authoritative guidance
   - **Delegates** — reference owner with "See resource: coalesce://context/owner"
   - **Consolidate** — if topic too fragmented, move to one owner

5. **Update Cross-References** — Rewrite delegate sections:
   ```markdown
   For node type selection, see: coalesce://context/data-engineering-principles
   ```
   Instead of repeating partial guidance.

**Deliverables:**

- Topic Ownership Matrix (markdown table)
- Conflict Report (all conflicts + resolutions)
- Updated Resource Files (conflicts resolved)

### Layer 2: Tool Descriptions Validation

**Objective**: Ensure all 41 tool descriptions reference correct resources and align with resource guidance.

**Process:**

1. **Extract Tool Descriptions** — For each tool, capture:
   - Tool name
   - Description text
   - Resource URIs mentioned
   - Embedded guidance/advice

2. **Validate Resource References** — Check every `coalesce://context/*`:
   - Does resource exist?
   - Is it the correct owner for that topic?
   - Missing references? (tool gives guidance that should defer to resource)

3. **Check Description vs Resource Alignment**:
   ```typescript
   // Example: Tool says "create nodes sequentially"
   // but storage-mappings.md says "verify after creation"
   // → Update tool to reference storage-mappings.md instead of repeating
   ```

4. **Identify Embedded Guidance** — Find tools that:
   - Duplicate resource guidance (move to resource, add reference)
   - Contradict resource guidance (fix contradiction)
   - Give advice without referencing authoritative resource (add reference)

5. **Parameter Consistency** — Verify parameter names align with resources:
   ```typescript
   // If data-engineering-principles says "use workspaceID"
   // all tools must use workspaceID (not workspace_id or wsID)
   ```

**Deliverables:**

- Tool Reference Audit (41 tools × resource references)
- Alignment Issues (conflicts between tools and resources)
- Updated Tool Descriptions (correct references, reduced duplication)

### Layer 3: User Documentation Validation

**Objective**: Align user-facing documentation with validated resources and tools.

**Process:**

1. **Setup Instructions** — Verify against implementation:
   - Environment variables match code expectations
   - MCP configuration uses correct command/args
   - Region URLs current and accurate
   - Snowflake requirements match workflow implementations

2. **Tool/Resource Listings** — Validate catalogs:
   - README tool count = registered tools (currently 41)
   - Resource table lists all 13 current resources
   - Descriptions align with actual tool/resource code
   - Categorization accurate (Nodes (10), Environments (5), etc.)

3. **Workflow Examples** — Cross-check with resources:
   - usage-guide.md workflows match overview.md patterns
   - Code examples use correct resource URIs
   - Parameter examples match tool schemas
   - Best practices align with resource guidance

4. **Troubleshooting Alignment** — Ensure troubleshooting sections:
   - Reference correct resources
   - Advice consistent with resources
   - Cover actual error conditions

5. **Terminology Consistency** — Canonical terms across all docs:
   - "workspace" vs "workspace ID" vs "workspaceID"
   - "node type" vs "nodeType"
   - "storage location" vs "storage mapping" vs "locationName"

**Deliverables:**

- Documentation Alignment Report (README vs resources vs tools)
- Terminology Glossary (canonical names)
- Updated Documentation (aligned with resources/tools)

### Layer 4: Implementation Verification

**Objective**: Verify code behavior matches all documentation promises.

**Process:**

1. **Tool Behavior Verification** — For each documented capability:
   - Example: `analyze-workspace-patterns` says "generates profile" → verify code calls `buildWorkspaceProfile` and returns JSON
   - Parameter validation matches Zod schema matches docs
   - Error handling matches usage-guide.md

2. **Resource Instructions Are Possible** — For each workflow:
   ```markdown
   # Example from data-engineering-principles.md:
   "1. Use analyze-workspace-patterns"
   "2. Save to data/ folder"

   → Verify: tool exists ✓
   → Verify: tool returns saveable data ✓
   → Verify: saving is LLM responsibility (not automatic) ✓
   ```

3. **Promised Features Exist** — Check documented features implemented:
   - overview.md: "Save API responses to data/" → tools return saveable data
   - storage-mappings.md: "read node to verify location" → getWorkspaceNode returns storage fields
   - data-engineering-principles.md: package detection → node type detection works with `::: `separator

4. **Test Coverage Alignment** — Verify documented behaviors have tests:
   - Each tool's documented behavior → at least one test
   - Each resource workflow → integration test possible
   - Each error condition in troubleshooting → error handling test

5. **Resource Registration** — Verify src/resources/index.ts:
   - Matches 13 resources in overview.md
   - URIs consistent everywhere
   - Metadata descriptions match file content
   - Files exist at specified paths

**Deliverables:**

- Behavior Verification Matrix (doc claim → code evidence)
- Missing Features Report (documented but not implemented)
- Test Gap Analysis (documented behaviors without tests)
- Implementation Fixes (code changes to match docs)

## Success Criteria

**Layer 1 Complete:**

- ✅ Topic ownership matrix complete (all topics assigned to one owner)
- ✅ Zero conflicts between resources (contradictions eliminated)
- ✅ All cross-references updated (no orphaned references)
- ✅ All 13 resources committed and tested

**Layer 2 Complete:**

- ✅ All 41 tools reference correct resources
- ✅ Zero contradictions between tool descriptions and resources
- ✅ Parameter names consistent with resources
- ✅ All tool descriptions updated and committed

**Layer 3 Complete:**

- ✅ README accurate (tool count, resource list, setup instructions)
- ✅ usage-guide.md aligned with resources
- ✅ Terminology consistent across all docs
- ✅ All documentation updated and committed

**Layer 4 Complete:**

- ✅ All documented tool behaviors verified in code
- ✅ All resource workflows verified possible
- ✅ No missing promised features
- ✅ Test coverage adequate for documented behaviors
- ✅ All implementation issues fixed and tested

**Final Validation:**

- ✅ Full test suite passes (189+ tests)
- ✅ TypeScript build clean (no errors)
- ✅ All commits pushed to main
- ✅ Project ready for npm publication

## Risk Mitigation

**Risk**: Layer 4 reveals fundamental mismatches requiring Layer 1 redesign

**Mitigation**: Each layer validates before proceeding. If Layer 4 finds issues requiring resource changes, revisit Layer 1, then re-validate Layers 2-3 before continuing.

**Risk**: Topic ownership decisions are subjective/contentious

**Mitigation**: Document ownership rationale in the matrix. Favor existing structure unless clear improvement. When in doubt, assign to overview.md as coordinator.

**Risk**: Validation scope creep (finding issues unrelated to conflicts)

**Mitigation**: Track separately. This validation focuses on consistency/conflicts. Other improvements (code quality, performance, etc.) go to backlog.

## Out of Scope

- Code refactoring for quality/performance (unless required for doc alignment)
- New features or capabilities
- API design changes
- Test coverage expansion beyond documented behaviors
- Unrelated bug fixes

## Assumptions

- Current 189 tests are well-designed (validate correct behaviors)
- Resource files are generally well-written (need alignment, not rewrite)
- Tool implementations are functionally correct (verify docs match reality)
- No breaking changes required for publication
