# MCP Publication Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate coalesce-transform-mcp for publication by establishing clear topic ownership across 18 resource files, aligning 50 tool descriptions, validating user documentation, and verifying implementation consistency.

**Architecture:** Four-layer validation workflow: (1) establish resource file topic ownership and eliminate conflicts, (2) align tool descriptions with validated resources, (3) align user documentation with resources/tools, (4) verify implementation matches all documentation promises. Each layer produces audit reports and targeted fixes.

**Tech Stack:** Markdown (resources, reports), TypeScript (tool descriptions, implementation), Vitest (test verification)

---

## File Structure

| Purpose | Files | Responsibility |
|---------|-------|----------------|
| **Validation outputs** | `docs/validation/*.md` | Topic matrix, conflict reports, audit results |
| **Resources to audit** | `src/resources/context/*.md` (18 files) | Source of truth for LLM guidance |
| **Tools to audit** | `src/tools/*.ts` (7 modules, 50 tools) | Tool descriptions and resource references |
| **Docs to audit** | `README.md`, `docs/usage-guide.md`, `docs/examples.md` | User-facing documentation |
| **Implementation to verify** | `src/**/*.ts`, `tests/**/*.test.ts` | Code behavior vs documentation promises |

---

## Task 1: Layer 1 - Extract Topics from All Resources

**Files:**
- Read: `src/resources/context/*.md` (all 18 resources)
- Create: `docs/validation/topic-extraction.md`

- [ ] **Step 1: Create validation output directory**

```bash
mkdir -p docs/validation
```

- [ ] **Step 2: Read all 18 resources and extract topics**

For each resource file, identify all distinct topics covered:
- Node creation workflow
- SQL platform detection
- Column transforms
- Storage location references
- Data caching strategy
- Materialization strategies
- Package detection
- ID resolution (project/workspace/environment/job/node/run)
- Tool selection (create/update/set)
- Node body structure
- Metadata editing
- Run lifecycle (start/retry/poll/cancel)
- Error handling
- etc.

Create a comprehensive topic list with section references.

- [ ] **Step 3: Write topic extraction to file**

```bash
# Format: One topic per resource file with section references
# Example structure:
# ## overview.md
# - Node creation workflow (section: "Creating Nodes")
# - SQL platform detection (section: "Writing SQL for Nodes")
#
# ## data-engineering-principles.md
# - Node type selection (section: "Using This Guide")
# - Package detection (section: "Package Detection")
```

Write to: `docs/validation/topic-extraction.md`

- [ ] **Step 4: Commit**

```bash
git add docs/validation/topic-extraction.md
git commit -m "docs: extract topics from all 18 resource files"
```

---

## Task 2: Layer 1 - Build Topic Ownership Matrix

**Files:**
- Read: `docs/validation/topic-extraction.md`
- Create: `docs/validation/topic-matrix.md`

- [ ] **Step 1: Identify topics mentioned in multiple resources**

Review topic-extraction.md and find all topics that appear in 2+ resource files.

- [ ] **Step 2: Build matrix table**

Create a markdown table showing which resources mention each topic:

```markdown
| Topic | Resources Mentioning It |
|-------|-------------------------|
| Node creation workflow | overview.md (Creating Nodes), node-creation-decision-tree.md (entire file), tool-usage.md (batching) |
| SQL platform detection | overview.md (Writing SQL), sql-platform-selection.md (entire file) |
| Storage location references | overview.md (Writing SQL), storage-mappings.md (entire file), node-payloads.md (locationName field) |
```

- [ ] **Step 3: Write matrix to file**

Write to: `docs/validation/topic-matrix.md`

- [ ] **Step 4: Commit**

```bash
git add docs/validation/topic-matrix.md
git commit -m "docs: build topic ownership matrix"
```

---

## Task 3: Layer 1 - Identify Conflicts and Assign Ownership

**Files:**
- Read: `docs/validation/topic-matrix.md`, `src/resources/context/*.md`
- Create: `docs/validation/conflict-report.md`

- [ ] **Step 1: For each multi-resource topic, identify conflict types**

Read the actual resource content for each topic and categorize:
- **Contradiction**: Different advice on same question
- **Redundancy**: Same advice repeated verbatim
- **Inconsistent detail**: One brief, another comprehensive
- **Orphaned reference**: References to deleted resources

- [ ] **Step 2: Assign ownership for each topic**

Decision rules:
- If one resource is comprehensive and others are brief → comprehensive resource owns it
- If one resource's purpose is that topic → that resource owns it
- If topic spans multiple concerns → overview.md coordinates, others delegate
- Document rationale for each ownership decision

- [ ] **Step 3: Write conflict report**

Format:
```markdown
## Topic: Node Creation Workflow

**Conflict Type**: Inconsistent detail + Redundancy

**Current State**:
- overview.md: 5-step workflow with full detail
- node-creation-decision-tree.md: Tool selection logic (which tool to use)
- tool-usage.md: Brief mention of batching node creation

**Owner Assignment**: node-creation-decision-tree.md

**Rationale**: Decision tree is the authoritative guide for "which tool to use". overview.md should reference it. tool-usage.md focuses on batching patterns, not individual creation.

**Resolution**:
- node-creation-decision-tree.md: Keep as-is (owner)
- overview.md: Replace detailed workflow with reference to node-creation-decision-tree.md
- tool-usage.md: No change (batching is separate concern)
```

Write to: `docs/validation/conflict-report.md`

- [ ] **Step 4: Commit**

```bash
git add docs/validation/conflict-report.md
git commit -m "docs: identify conflicts and assign topic ownership"
```

---

## Task 4: Layer 1 - Update Resource Files with Cross-References

**Files:**
- Read: `docs/validation/conflict-report.md`
- Modify: `src/resources/context/*.md` (as needed per conflict report)

- [ ] **Step 1: For each topic conflict, update delegate resources**

Following the conflict report resolutions, update resource files to:
- Remove redundant content
- Add cross-references to owner resources
- Maintain only topic-specific content in delegates

Example edit:
```markdown
# Before (in overview.md):
## Creating Nodes
1. Analyze workspace first...
2. Consult data-engineering-principles.md...
3. Select appropriate node type...
4. Create node: Use create-workspace-node-from-scratch or...
5. Verify: Read the created node...

# After (in overview.md):
## Creating Nodes

For guidance on choosing the right node creation tool, see: coalesce://context/node-creation-decision-tree

For data engineering principles and node type selection, see: coalesce://context/data-engineering-principles
```

- [ ] **Step 2: Verify no orphaned references remain**

Search all resource files for references to deleted resources (like `node-selection`).

Command:
```bash
grep -r "node-selection" src/resources/context/
```

Expected: No matches

- [ ] **Step 3: Commit each resource file update separately**

```bash
git add src/resources/context/overview.md
git commit -m "docs: update overview.md to delegate node creation to decision-tree"

git add src/resources/context/storage-mappings.md
git commit -m "docs: remove redundant SQL platform content, reference sql-platform-selection"

# Repeat for each updated file
```

---

## Task 5: Layer 1 - Verify Resource Consistency

**Files:**
- Read: `src/resources/context/*.md` (all 18 resources)
- Read: `src/resources/index.ts`
- Modify: `docs/validation/conflict-report.md` (append verification section)

- [ ] **Step 1: Verify all 18 resources registered**

Read `src/resources/index.ts` and confirm:
- `RESOURCES` const has 13 entries
- `RESOURCE_FILES` maps all 13 URIs to file paths
- `RESOURCE_METADATA` has descriptions for all 13
- All file paths exist

- [ ] **Step 2: Verify overview.md lists all resources**

Read `src/resources/context/overview.md` and check "Available Resources" section lists all 13 with correct URIs.

- [ ] **Step 3: Cross-check resource descriptions**

For each resource, verify the description in `src/resources/index.ts` RESOURCE_METADATA matches the actual content/purpose of the resource file.

- [ ] **Step 4: Document verification results**

Append to `docs/validation/conflict-report.md`:
```markdown
## Layer 1 Verification Results

✅ All 18 resources registered in src/resources/index.ts
✅ All 18 resources listed in overview.md "Available Resources"
✅ All resource file paths exist
✅ All resource descriptions accurate
✅ No orphaned cross-references
✅ Topic ownership clearly assigned
```

- [ ] **Step 5: Commit**

```bash
git add docs/validation/conflict-report.md
git commit -m "docs: verify Layer 1 resource consistency"
```

---

## Task 6: Layer 2 - Extract Tool Descriptions and Resource References

**Files:**
- Read: `src/tools/*.ts` (7 modules)
- Create: `docs/validation/tool-audit.md`

- [ ] **Step 1: For each of the 50 tools, extract metadata**

Read all tool registration calls in:
- `src/tools/nodes.ts` (19 tools - includes cache-workspace-nodes, search-cached-workspace-nodes, get-cached-workspace-node)
- `src/tools/environments.ts` (8 tools - includes cache-environment-nodes, search-cached-environment-nodes, get-cached-environment-node)
- `src/tools/runs.ts` (15 tools - includes cache-runs, cache-run-results)
- `src/tools/projects.ts` (8 tools)
- `src/tools/git-accounts.ts` (2 tools)
- `src/tools/users.ts` (4 tools - includes cache-org-users)
- `src/workflows/*.ts` (4 tools)

For each `server.tool()` call, capture:
- Tool name (first argument)
- Description text (second argument)
- Any `coalesce://context/*` URIs in description
- Any workflow/guidance embedded in description

- [ ] **Step 2: Build tool audit table**

Format:
```markdown
| Tool | File | Resource References | Embedded Guidance |
|------|------|---------------------|-------------------|
| list-workspace-nodes | nodes.ts | None | "To find workspace IDs, use list-projects..." |
| create-workspace-node | nodes.ts | node-selection, sql-platforms, storage-mappings | "For guidance on node types..." |
| analyze-workspace-patterns | nodes.ts | None | "Generates workspace profile summary" |
```

- [ ] **Step 3: Write tool audit to file**

Write to: `docs/validation/tool-audit.md`

- [ ] **Step 4: Commit**

```bash
git add docs/validation/tool-audit.md
git commit -m "docs: extract tool descriptions and resource references"
```

---

## Task 7: Layer 2 - Validate Tool Resource References

**Files:**
- Read: `docs/validation/tool-audit.md`, `docs/validation/conflict-report.md`
- Create: `docs/validation/tool-alignment-issues.md`

- [ ] **Step 1: Check each resource reference exists and is correct owner**

For each tool that references a resource:
- Does the resource URI exist? (check against src/resources/index.ts)
- Is it the correct owner for that topic? (check against conflict-report.md topic ownership)
- Are there missing references? (tool gives guidance that should defer to a resource)

- [ ] **Step 2: Identify embedded guidance that should be in resources**

Find tool descriptions that:
- Repeat content from resources (duplication)
- Give workflow advice not in any resource (should move to resource)
- Contradict resource guidance (needs fixing)

- [ ] **Step 3: Document alignment issues**

Format:
```markdown
## create-workspace-node

**Issue**: References deleted resource `node-selection`

**Current**: "For guidance on node types, see: coalesce://context/node-selection"

**Fix**: Replace with "For guidance on node types, see: coalesce://context/node-creation-decision-tree"

---

## update-workspace-node

**Issue**: Embeds guidance that belongs in node-payloads.md

**Current**: "Object fields are deep-merged; arrays replace..."

**Fix**: Move array replacement warning to node-payloads.md, reference it from tool
```

Write to: `docs/validation/tool-alignment-issues.md`

- [ ] **Step 4: Commit**

```bash
git add docs/validation/tool-alignment-issues.md
git commit -m "docs: identify tool-resource alignment issues"
```

---

## Task 8: Layer 2 - Fix Tool Descriptions

**Files:**
- Read: `docs/validation/tool-alignment-issues.md`
- Modify: `src/tools/*.ts` (as needed)

- [ ] **Step 1: Update tool descriptions to reference correct resources**

For each issue in tool-alignment-issues.md:
- Replace deleted resource references with current owners
- Add missing resource references where tools give guidance
- Remove duplicated content, add resource references instead

Example fix:
```typescript
// Before:
server.tool(
  "create-workspace-node",
  "Create a new node. For guidance on node types, see: coalesce://context/node-selection",
  { ... }
);

// After:
server.tool(
  "create-workspace-node",
  "Create a new node. For guidance on choosing the right creation tool, see: coalesce://context/node-creation-decision-tree. For node types and data engineering principles, see: coalesce://context/data-engineering-principles",
  { ... }
);
```

- [ ] **Step 2: Verify parameter naming consistency**

Check that all parameter names match resource guidance:
- `workspaceID` (not `workspace_id` or `wsID`)
- `environmentID` (not `env_id`)
- `nodeID` (not `node_id`)
- `runCounter` for numeric run IDs
- `runID` for string run IDs

- [ ] **Step 3: Commit each tool file update**

```bash
git add src/tools/nodes.ts
git commit -m "fix: update node tool descriptions to reference correct resources"

git add src/tools/runs.ts
git commit -m "fix: update run tool descriptions to reference run-operations.md"

# Repeat for each updated file
```

---

## Task 9: Layer 2 - Verify Tool Consistency

**Files:**
- Read: `src/tools/*.ts`, `tests/registration.test.ts`
- Modify: `docs/validation/tool-alignment-issues.md` (append verification)

- [ ] **Step 1: Verify tool count matches documentation**

Run:
```bash
grep -r "server.tool(" src/tools/ src/workflows/ | wc -l
```

Expected: 50 (matching spec)

- [ ] **Step 2: Verify test registration count**

Read `tests/registration.test.ts` and check:
```typescript
expect(toolSpy).toHaveBeenCalledTimes(50);
```

- [ ] **Step 3: Run tests to verify no breakage**

```bash
npx vitest run tests/registration.test.ts tests/tools/
```

Expected: All tests pass

- [ ] **Step 4: Document verification results**

Append to `docs/validation/tool-alignment-issues.md`:
```markdown
## Layer 2 Verification Results

✅ All 50 tools have correct resource references
✅ No references to deleted resources
✅ Parameter names consistent with resource guidance
✅ Embedded guidance moved to resources or removed
✅ All tests passing
```

- [ ] **Step 5: Commit**

```bash
git add docs/validation/tool-alignment-issues.md
git commit -m "docs: verify Layer 2 tool consistency"
```

---

## Task 10: Layer 3 - Audit User Documentation

**Files:**
- Read: `README.md`, `docs/usage-guide.md`, `docs/examples.md`, `package.json`
- Create: `docs/validation/documentation-audit.md`

- [ ] **Step 1: Verify README accuracy**

Check:
- Tool count: Must say exactly 50 tools
- Resource count: Should say 18 resources
- Resource table: Lists all 13 with current descriptions
- Tool categorization: "Nodes (19), Environments (8), Runs (15), Projects (8), Git (2), Users (4), Workflows (4)" matches reality
- Setup instructions: Environment variables match src/client.ts, src/workflows/*.ts
- MCP configuration: Command/args correct for npm package

- [ ] **Step 2: Verify usage-guide.md alignment**

Check:
- Workflows match overview.md patterns
- Code examples use correct resource URIs (no node-selection)
- Parameter examples match tool Zod schemas
- Best practices align with resource guidance
- Troubleshooting references correct resources

- [ ] **Step 3: Verify docs/examples.md (if exists)**

Check:
- Examples use current tool names
- Resource references are correct
- Code is executable/accurate

- [ ] **Step 4: Extract terminology variations**

Find all variations of key terms across README, usage-guide, examples:
- workspace / workspace ID / workspaceID
- node type / nodeType
- storage location / storage mapping / locationName
- run counter / run ID / runCounter / runID

- [ ] **Step 5: Document audit findings**

Format:
```markdown
## README.md Issues

- Tool count shows 41, should be 50 (missing 9 new cache tools)
- Resource table missing: id-discovery, node-creation-decision-tree, node-payloads, hydrated-metadata, run-operations
- Tool categorization needs updating for cache tools

## usage-guide.md Issues

- References node-selection at line 52
- Workflow example uses incorrect parameter name "workspace_id" (should be workspaceID)

## Terminology Inconsistencies

- "workspaceID" vs "workspace ID" (README uses "workspace ID", code uses workspaceID)
- "runCounter" vs "run ID" (usage-guide inconsistent)
```

Write to: `docs/validation/documentation-audit.md`

- [ ] **Step 6: Commit**

```bash
git add docs/validation/documentation-audit.md
git commit -m "docs: audit user documentation for consistency"
```

---

## Task 11: Layer 3 - Create Terminology Glossary

**Files:**
- Read: `docs/validation/documentation-audit.md`
- Create: `docs/validation/terminology-glossary.md`

- [ ] **Step 1: Define canonical terms**

Establish official terminology for all key concepts:

```markdown
| Concept | Canonical Term | Usage Context | Avoid |
|---------|---------------|---------------|-------|
| Workspace identifier | `workspaceID` | Code, parameters, tool names | workspace_id, wsID, workspace ID |
| Workspace (general reference) | workspace | Prose, user documentation | N/A |
| Node type identifier | `nodeType` | Code, parameters | node_type, type |
| Storage location | storage location | General prose | storage mapping |
| Run numeric ID | `runCounter` | When returned by start-run, used by run-status | run ID (ambiguous) |
| Run ID string | `runID` | When used by get-run-details, get-run-results | N/A |
```

- [ ] **Step 2: Write glossary to file**

Write to: `docs/validation/terminology-glossary.md`

- [ ] **Step 3: Commit**

```bash
git add docs/validation/terminology-glossary.md
git commit -m "docs: create canonical terminology glossary"
```

---

## Task 12: Layer 3 - Fix User Documentation

**Files:**
- Read: `docs/validation/documentation-audit.md`, `docs/validation/terminology-glossary.md`
- Modify: `README.md`, `docs/usage-guide.md`, `docs/examples.md`, `package.json`

- [ ] **Step 1: Update README.md**

Fixes:
- Update tool count to 50
- Add 9 new cache tools to tool listings
- Add 5 missing resources to resource table
- Update resource descriptions to match src/resources/index.ts
- Update tool categorization (Nodes: 19, Environments: 8, Runs: 15, Users: 4)
- Use canonical terminology from glossary
- Verify setup instructions match implementation

- [ ] **Step 2: Update usage-guide.md**

Fixes:
- Replace node-selection references with correct resources
- Fix parameter name inconsistencies (use canonical terms)
- Align workflow examples with overview.md
- Update best practices to match resource guidance
- Fix troubleshooting references

- [ ] **Step 3: Update docs/examples.md (if exists)**

Fixes:
- Update tool names if any changed
- Fix resource references
- Use canonical terminology

- [ ] **Step 4: Update package.json**

Verify:
- description accurately describes the MCP server
- keywords relevant for npm search

- [ ] **Step 5: Commit each file separately**

```bash
git add README.md
git commit -m "docs: update README with 18 resources, 50 tools, canonical terminology"

git add docs/usage-guide.md
git commit -m "docs: align usage-guide with resources and canonical terminology"

git add docs/examples.md
git commit -m "docs: update examples with correct resource references"

git add package.json
git commit -m "docs: verify package.json description and keywords"
```

---

## Task 13: Layer 4 - Verify Tool Behavior Matches Documentation

**Files:**
- Read: `src/tools/*.ts`, tool descriptions from Layer 2 audit
- Create: `docs/validation/behavior-verification.md`

- [ ] **Step 1: For each tool, verify documented behavior exists in code**

Example verification:
```markdown
## analyze-workspace-patterns

**Documented**: "Generates workspace profile summary"

**Code Check**:
- ✅ Calls buildWorkspaceProfile() - src/tools/nodes.ts:904
- ✅ Returns JSON - src/tools/nodes.ts:907
- ✅ Returns WorkspaceProfile type

**Verdict**: ✅ Matches

---

## create-workspace-node-from-scratch

**Documented**: "Defaults to completionLevel: configured"

**Code Check**:
- ✅ src/tools/nodes.ts:886 - completionLevel parameter is optional
- ❌ No default value set in Zod schema - relies on implementation
- ⚠️ Implementation in createWorkspaceNodeFromScratch function - need to verify

**Verdict**: ⚠️ Needs verification in implementation
```

- [ ] **Step 2: Verify parameter validation matches Zod schemas**

For tools with complex parameters, check:
- Zod schema in tool registration matches documented parameters
- Required vs optional matches documentation
- Parameter types match documentation

- [ ] **Step 3: Verify error handling matches usage-guide.md**

Check troubleshooting section claims:
- "Bad request errors" → verify handleToolError handles 400s
- "Snowflake authentication errors" → verify workflow tools check env vars
- Error messages match documented patterns

- [ ] **Step 4: Write behavior verification matrix**

Write to: `docs/validation/behavior-verification.md`

- [ ] **Step 5: Commit**

```bash
git add docs/validation/behavior-verification.md
git commit -m "docs: verify tool behavior matches documentation"
```

---

## Task 14: Layer 4 - Verify Resource Workflows Are Possible

**Files:**
- Read: `src/resources/context/*.md`, `src/tools/*.ts`
- Modify: `docs/validation/behavior-verification.md` (append)

- [ ] **Step 1: Extract all workflow instructions from resources**

For each resource, find all "how to" instructions:
- overview.md: "Creating Nodes" workflow (5 steps)
- data-engineering-principles.md: "Use analyze-workspace-patterns" → "Save to data/ folder"
- storage-mappings.md: "Read node to verify storage location"
- run-operations.md: Start/retry/poll/cancel workflows
- etc.

- [ ] **Step 2: Verify each workflow step is possible with available tools**

Example:
```markdown
## Workflow: Creating Nodes (from overview.md)

1. "Inspect project and workspace context first"
   - ✅ Possible: get-project, list-workspace-nodes tools exist

2. "Consult data-engineering principles when node type is unclear"
   - ✅ Possible: resource exists, LLM can read it

3. "Determine SQL platform"
   - ✅ Possible: sql-platform-selection.md resource exists

4. "Create node"
   - ✅ Possible: create-workspace-node-from-scratch, create-workspace-node-from-predecessor exist

5. "Verify helper validation"
   - ✅ Possible: Tools return validation field in response
```

- [ ] **Step 3: Identify any workflows that reference non-existent tools**

Search for instructions like "Use [tool-name]" and verify the tool exists.

- [ ] **Step 4: Append workflow verification to behavior-verification.md**

- [ ] **Step 5: Commit**

```bash
git add docs/validation/behavior-verification.md
git commit -m "docs: verify resource workflows are possible with available tools"
```

---

## Task 15: Layer 4 - Check for Missing Promised Features

**Files:**
- Read: All documentation, `src/tools/*.ts`, `src/workflows/*.ts`
- Create: `docs/validation/missing-features.md`

- [ ] **Step 1: Extract all feature promises from documentation**

Scan README, usage-guide, resources for claims about what the server does:
- "Save all API responses to data/ folder" (overview.md)
- "analyze-workspace-patterns generates workspace profile"
- "getWorkspaceNode returns storage location fields"
- "Package detection works with ::: separator"
- etc.

- [ ] **Step 2: Verify each promise is implemented**

Check:
- ✅ "Save to data/" - tools return data that CAN be saved (LLM responsibility)
- ✅ "analyze-workspace-patterns" - tool exists and returns profile
- ✅ "storage location fields" - getWorkspaceNode returns those fields
- ✅ "package detection" - buildWorkspaceProfile checks for :::

- [ ] **Step 3: Document any missing features**

If a feature is documented but not implemented, record it:
```markdown
## Missing Feature: Auto-save to data/ folder

**Documented**: overview.md says "Save all API responses to data/ folder"

**Reality**: Tools return data, but saving is LLM's responsibility (not automatic)

**Assessment**: ✅ NOT A BUG - documentation correctly describes LLM workflow, not automatic behavior
```

Write to: `docs/validation/missing-features.md`

- [ ] **Step 4: Commit**

```bash
git add docs/validation/missing-features.md
git commit -m "docs: check for missing promised features"
```

---

## Task 16: Layer 4 - Verify Test Coverage for Documented Behaviors

**Files:**
- Read: `tests/**/*.test.ts`, documentation
- Create: `docs/validation/test-gap-analysis.md`

- [ ] **Step 1: List all documented tool behaviors**

From tool descriptions and resource workflows, extract testable claims:
- analyze-workspace-patterns returns workspace profile
- create-workspace-node-from-scratch defaults to completionLevel: configured
- Parameter validation rejects invalid IDs
- Error handling returns proper error format
- etc.

- [ ] **Step 2: Check if each behavior has a test**

Search tests for coverage:
```bash
# Example: Check if analyze-workspace-patterns is tested
grep -r "analyze-workspace-patterns" tests/
```

- [ ] **Step 3: Document test gaps**

Format:
```markdown
## Behavior: analyze-workspace-patterns returns workspace profile

**Test Coverage**: ✅ tests/tools/nodes.test.ts registers the tool
⚠️ No test verifies it returns WorkspaceProfile structure

**Recommendation**: Add integration test (low priority - implementation verified in Layer 4 Task 13)

---

## Behavior: Error handling for invalid workspace ID

**Test Coverage**: ❌ No test found

**Recommendation**: Add error handling test (medium priority)
```

Write to: `docs/validation/test-gap-analysis.md`

- [ ] **Step 4: Commit**

```bash
git add docs/validation/test-gap-analysis.md
git commit -m "docs: analyze test coverage for documented behaviors"
```

---

## Task 17: Final Validation - Run Full Test Suite

**Files:**
- Run: All tests

- [ ] **Step 1: Run full test suite**

```bash
npx vitest run
```

Expected: All tests pass (at least 189, may be more if validation added tests)

- [ ] **Step 2: Run TypeScript build**

```bash
npx tsc --noEmit
```

Expected: No type errors

- [ ] **Step 3: Build distribution**

```bash
npm run build
```

Expected: Clean build, dist/ output created

- [ ] **Step 4: Verify test results**

If any failures:
- Document in docs/validation/final-validation.md
- Fix issues
- Re-run tests
- Commit fixes

---

## Task 18: Final Validation - Generate Publication Readiness Report

**Files:**
- Read: All validation reports from docs/validation/
- Create: `docs/validation/publication-readiness-report.md`

- [ ] **Step 1: Compile validation results**

Aggregate results from all layers:

```markdown
# MCP Publication Readiness Report

Generated: [DATE]

## Executive Summary

✅ All 4 validation layers complete
✅ 18 resources internally consistent
✅ 50 tools aligned with resources
✅ User documentation accurate
✅ Implementation verified

## Layer 1: Resource Files

✅ Topic ownership matrix complete
✅ Zero conflicts between resources
✅ All cross-references updated
✅ All 18 resources committed and tested

## Layer 2: Tool Descriptions

✅ All 50 tools reference correct resources
✅ Zero contradictions between tools and resources
✅ Parameter names consistent
✅ All tool descriptions updated

## Layer 3: User Documentation

✅ README accurate (50 tools, 18 resources)
✅ usage-guide.md aligned with resources
✅ Terminology consistent
✅ All documentation committed

## Layer 4: Implementation Verification

✅ All tool behaviors verified
✅ All resource workflows verified possible
✅ No missing promised features
⚠️ Test coverage gaps identified (low priority)

## Test Results

✅ 189 tests passing
✅ TypeScript build clean
✅ Distribution build successful

## Known Issues

[List any remaining low-priority issues from test-gap-analysis.md]

## Recommendation

✅ **APPROVED FOR PUBLICATION**

The coalesce-transform-mcp project is ready for npm publication. All critical validation criteria met.
```

Write to: `docs/validation/publication-readiness-report.md`

- [ ] **Step 2: Commit report**

```bash
git add docs/validation/publication-readiness-report.md
git commit -m "docs: generate publication readiness report"
```

---

## Task 19: Cleanup and Finalize

**Files:**
- Optional: Move validation reports to archive

- [ ] **Step 1: Review all validation commits**

```bash
git log --oneline --since="1 day ago"
```

Verify all validation work is committed.

- [ ] **Step 2: Optional: Archive validation reports**

If desired, move validation artifacts to archive:
```bash
mkdir -p docs/archive/2026-03-20-validation
mv docs/validation/*.md docs/archive/2026-03-20-validation/
git add docs/archive/
git commit -m "docs: archive validation reports"
```

Or keep in docs/validation/ for reference.

- [ ] **Step 3: Verify project state**

```bash
git status
npm test
npm run build
```

Expected: Clean working tree, all tests pass, clean build

- [ ] **Step 4: Final commit if needed**

If any cleanup changes:
```bash
git add .
git commit -m "chore: finalize MCP publication validation"
```

---

## Success Criteria

**All tasks complete when:**

- ✅ docs/validation/ contains all audit reports and matrices
- ✅ All 18 resources have clear topic ownership (no conflicts)
- ✅ All 50 tools reference correct resources
- ✅ README and usage-guide accurate and aligned
- ✅ All tests passing (189+)
- ✅ TypeScript build clean
- ✅ Publication readiness report shows APPROVED
