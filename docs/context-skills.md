# Context skills

24 curated markdown resources under `coalesce://context/*` guide how agents interact with the server - SQL conventions per warehouse, node-type selection, pipeline workflows, lineage/impact guidance.

## Customizing skills

Set `COALESCE_MCP_SKILLS_DIR` to make them editable on disk:

```bash
export COALESCE_MCP_SKILLS_DIR="/path/to/my-skills"
```

On first run the server seeds the directory with two files per skill:

- `coalesce_skills.<name>.md` - the default skill content (editable)
- `user_skills.<name>.md` - your customization file (starts as an inactive stub with instructions)

## Resolution order

Each resource resolves using this priority:

1. **Override** - `user_skills.<name>.md` starts with `<!-- OVERRIDE -->` → only the user file is served
2. **Augment** - `user_skills.<name>.md` has custom content (remove the `<!-- STUB -->` line first) → default + user content are concatenated
3. **Default** - `user_skills.<name>.md` is missing, empty, or still has the seeded stub → default skill content is served
4. **Disabled** - both files deleted → empty content is served

Seeding is idempotent - it never overwrites files you've already modified.

## All skills

| Skill | File | Description |
| ----- | ---- | ----------- |
| Coalesce Overview | `overview` | General Coalesce concepts, response guidelines, and operational constraints |
| SQL Platform Selection | `sql-platform-selection` | Determining the active SQL platform from project metadata |
| SQL Rules: Snowflake | `sql-snowflake` | Snowflake-specific SQL conventions for node SQL |
| SQL Rules: Databricks | `sql-databricks` | Databricks-specific SQL conventions for node SQL |
| SQL Rules: BigQuery | `sql-bigquery` | BigQuery-specific SQL conventions for node SQL |
| Data Engineering Principles | `data-engineering-principles` | Node type selection, layered architecture, methodology detection, and materialization strategies |
| Storage Locations and References | `storage-mappings` | Storage location concepts, `{{ ref() }}` syntax, and reference patterns |
| Tool Usage Patterns | `tool-usage` | Best practices for tool batching, parallelization, and SQL conversion |
| ID Discovery | `id-discovery` | Resolving project, workspace, environment, job, run, node, and org IDs |
| Node Creation Decision Tree | `node-creation-decision-tree` | Choosing between predecessor-based creation, updates, and full replacements |
| Node Payloads | `node-payloads` | Working with workspace node bodies, metadata, config, and array-replacement risks |
| Hydrated Metadata | `hydrated-metadata` | Coalesce hydrated metadata structures for advanced node payload editing |
| Run Operations | `run-operations` | Starting, retrying, polling, diagnosing, and canceling Coalesce runs |
| Node Type Corpus | `node-type-corpus` | Node type discovery, corpus search, and metadata patterns |
| Aggregation Patterns | `aggregation-patterns` | JOIN ON generation, GROUP BY detection, and join-to-aggregation conversion |
| Intelligent Node Configuration | `intelligent-node-configuration` | How intelligent config completion works, schema resolution, and automatic field detection |
| Pipeline Workflows | `pipeline-workflows` | Building pipelines end-to-end: node type selection, multi-node sequences, and execution |
| Node Operations | `node-operations` | Editing existing nodes: joins, columns, config fields, and SQL-to-graph conversion |
| Node Type Selection Guide | `node-type-selection-guide` | When to use each Coalesce node type (Stage/Work vs Dimension/Fact vs specialized) |
| Intent Pipeline Guide | `intent-pipeline-guide` | Using `build_pipeline_from_intent` to create pipelines from natural language |
| Run Diagnostics Guide | `run-diagnostics-guide` | Using `diagnose_run_failure` to analyze failed runs and determine fixes |
| Pipeline Review Guide | `pipeline-review-guide` | Using `review_pipeline` for pipeline analysis and optimization |
| Pipeline Workshop Guide | `pipeline-workshop-guide` | Using pipeline workshop tools for iterative, conversational pipeline building |
| Ecosystem Boundaries | `ecosystem-boundaries` | Scope of this MCP vs adjacent data engineering MCPs (Snowflake, Fivetran, dbt, Catalog) |

## COA describe topics

10 resources under `coalesce://coa/describe/*` surface the bundled COA CLI's self-describing documentation. Content is fetched from `coa describe <topic>` on first access and cached to disk, keyed by the pinned COA version - agents always see docs that match the CLI they're driving.

Topics: `overview`, `commands`, `selectors`, `schemas`, `workflow`, `structure`, `concepts`, `sql-format`, `node-types`, `config`.

For parameterized topics (`command <name>`, `schema <type>`), use the `coa_describe` tool with a `subtopic` argument.
