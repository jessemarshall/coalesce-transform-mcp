# Topic Ownership Matrix

This document identifies all topics that appear in 2 or more resource files, showing which resources cover each topic.

## Topics Appearing in Multiple Resources

| Topic | Resources Mentioning It |
|-------|-------------------------|
| Node creation workflow | overview.md (Creating Nodes), tool-usage.md (Node Workflows), node-creation-decision-tree.md (entire file) |
| Data caching strategy | data-engineering-principles.md (Data Caching Strategy), tool-usage.md (Data Caching Strategy) |
| Project metadata inspection | overview.md (Creating Nodes), sql-platform-selection.md (1. Check Project Metadata First) |
| Node type selection | overview.md (Creating Nodes, referenced to data-engineering-principles), data-engineering-principles.md (Node type recommendations by layer, Decision Framework) |
| Storage location references | storage-mappings.md (entire file focus), node-payloads.md (storageLocations and top-level location fields), hydrated-metadata.md (storageLocations array structure), node-creation-decision-tree.md (Storage and SQL Follow-Up), overview.md (Creating Nodes) |
| Coalesce ref() syntax | sql-platform-selection.md (Coalesce-Specific Rule), storage-mappings.md (Reference Syntax), sql-databricks.md (Databricks ref() argument handling), sql-bigquery.md (BigQuery ref() argument handling) |
| Identifier conventions and casing | sql-snowflake.md (Core Rules), sql-databricks.md (Core Rules), sql-bigquery.md (Core Rules), storage-mappings.md (Case sensitivity in storage/table names) |
| Editing guidance | sql-snowflake.md (Editing Guidance), sql-databricks.md (Editing Guidance), sql-bigquery.md (Editing Guidance), hydrated-metadata.md (Editing Rules) |
| Workspace discovery and IDs | tool-usage.md (Find Projects and Workspaces), id-discovery.md (Workspace IDs, Workspace inclusion in API calls) |
| Rename safety | tool-usage.md (Rename Safety), node-payloads.md (Rename Safety) |
| Run operations and workflows | tool-usage.md (Runs and Troubleshooting, Preferred Run Workflows, Run Result Handling), run-operations.md (Tool Selection, End-to-End Helpers, Result Handling), systematic-debugging.md (Run issue diagnosis) |
| Scratch and placeholder node creation | tool-usage.md (Scratch Node Creation), node-creation-decision-tree.md (Scratch node creation decision, Placeholder node creation), node-payloads.md (Scratch creation expectations) |
| SQL platform detection and dialect | overview.md (Writing SQL for Nodes), sql-platform-selection.md (Goal, 1-4 detection methods) |
| Common SQL patterns | sql-snowflake.md (Common Patterns), sql-databricks.md (Common Patterns), sql-bigquery.md (Common Patterns) |
| Node update operations | tool-usage.md (Updating Existing Nodes), node-creation-decision-tree.md (Node update decision), node-payloads.md (Prefer update-workspace-node) |
| Job ID resolution | id-discovery.md (Job IDs), run-operations.md (Job IDs) |
| Run counter vs URL UUID | id-discovery.md (Run IDs and Run Counters), run-operations.md (Run Counter vs URL UUID) |
| Canceling runs | tool-usage.md (Canceling Runs), run-operations.md (Canceling Runs) |
| Validation inspection after creation/writes | node-creation-decision-tree.md (Multi-Predecessor and Join Requests), node-payloads.md (Validation Rules) |
| Tool selection patterns | overview.md (implicit, mentioned throughout), tool-usage.md (Core Rules, Tool selection criteria), node-creation-decision-tree.md (Tool selection logic), run-operations.md (Tool Selection), writing-plans.md (Coalesce-Specific Planning Checks) |
| Snowflake authentication | sql-platform-selection.md (Special Note), run-operations.md (Authentication Rule) |
| Higher-level helper preference | tool-usage.md (Prefer the High-Level Helpers), hydrated-metadata.md (When To Use Raw Hydrated Input), node-payloads.md (Prefer update-workspace-node) |
| Predecessor-based node creation | tool-usage.md (Predecessor-Based Node Creation), node-creation-decision-tree.md (Predecessor-based node creation decision) |
| Array replacement semantics | node-payloads.md (Array Safety), hydrated-metadata.md (Array replacement semantics) |
| Brainstorming before mutation | overview.md (Creating Nodes), brainstorming.md (Mutation prevention during uncertainty, Core Behavior), writing-plans.md (Core Behavior, Plan Quality Rules) |
| Good default sequences | tool-usage.md (Good Default Sequences), id-discovery.md (Good Defaults), run-operations.md (Good Default Sequences) |
| Org ID resolution | id-discovery.md (Org IDs), run-operations.md (Org ID requirement for cancellation) |
| Plan structure and execution | writing-plans.md (entire file focus), node-creation-decision-tree.md (Decision tree implies planning), brainstorming.md (Saving the Result) |
| Verification and validation workflows | verification-before-completion.md (entire file focus), writing-plans.md (Good Plan Structure, Coalesce-Specific Planning Checks), tool-usage.md (Validation inspection), run-operations.md (Result Handling) |
| Debugging and problem diagnosis | systematic-debugging.md (entire file focus), tool-usage.md (Anti-patterns), run-operations.md (Run Diagnostics), verification-before-completion.md (Core Rules) |
| Code review and quality assurance | requesting-code-review.md (entire file focus), verification-before-completion.md (Verification Checklist), systematic-debugging.md (What To Record) |

## Summary

**Total topics appearing in 2+ resources: 31**

- **Previous count (14 resources):** 27 topics
- **New topics added (4 resources):** 4 topics
- **Topics updated to include new resources:** 3 topics

These topics are candidates for conflicts (where different resources give different advice on the same topic) and will be reviewed in Task 3 for conflict identification and ownership assignment.
