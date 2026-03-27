# Intent-Based Pipeline Building

## Overview

The `build_pipeline_from_intent` tool lets users describe pipelines in natural language. The tool parses the description, resolves references to workspace nodes, selects appropriate node types, and assembles a standard pipeline plan.

## When to Use

Use `build_pipeline_from_intent` when the user:
- Describes a pipeline in plain English without providing SQL
- Mentions table or node names and desired transformations conversationally
- Asks to "combine", "join", "aggregate", "stage", or "filter" data without specifying exact SQL

Use `create_pipeline_from_sql` instead when the user provides actual SQL.
Use `plan_pipeline` with `sourceNodeIDs` when you already know the exact upstream node IDs and want goal-based planning.

## How It Works

### 1. Intent Parsing
The tool extracts structured operations from natural language:

| User says | Detected operation |
|---|---|
| "combine X and Y", "join X with Y" | `join` |
| "aggregate", "sum", "total", "group by" | `aggregate` |
| "stage", "load", "ingest" | `stage` |
| "filter", "where", "only active" | `filter` (added to nearest step) |
| "union", "stack", "append" | `union` |

### 2. Entity Resolution
Table/node names mentioned in the intent are fuzzy-matched against existing workspace nodes:

- **Exact match**: `CUSTOMERS` matches `CUSTOMERS` (score 100)
- **Prefix strip**: `CUSTOMERS` matches `STG_CUSTOMERS` (score 90)
- **Pluralization**: `CUSTOMER` matches `CUSTOMERS` or `STG_CUSTOMER` (score 82)
- **Substring**: `CUST` matches `STG_CUSTOMERS` (score 70)

When multiple nodes match at the same score, the tool asks for clarification.

### 3. Node Type Selection
Each pipeline step gets an appropriate node type via the standard `selectPipelineNodeType` ranker:
- **Stage/load** steps → Stage or Work types
- **Join** steps → Stage or Work types with multi-source context
- **Aggregate** steps → Stage or Work types with GROUP BY context

### 4. Plan Assembly
The tool produces a standard `PipelinePlan` object (same format as `plan_pipeline`) that can be executed via `create_pipeline_from_plan`.

## Examples

### Simple staging
> "stage the raw payments table"

Resolves "payments" to a workspace node, creates a single Stage node downstream.

### Join with aggregation
> "combine customers and orders by customer_id, aggregate total revenue by region"

Creates two pipeline steps:
1. **Join node**: INNER JOIN `CUSTOMERS` and `ORDERS` on `CUSTOMER_ID`
2. **Aggregate node**: SUM(REVENUE) GROUP BY REGION

### Multi-source join
> "join products with inventory on product_id"

Creates one join node with FROM/JOIN/ON condition auto-generated.

## Clarification Flow

If the tool cannot resolve entities or the intent is ambiguous, it returns:
- `status: "needs_entity_resolution"` — entity names didn't match workspace nodes
- `status: "needs_clarification"` — missing join keys, group by columns, or ambiguous operations

The `openQuestions` array contains specific questions to ask the user.

## Confirmation Flow

When the plan is ready, the tool follows the same STOP_AND_CONFIRM pattern as other pipeline tools. Present the plan to the user in a table showing each node name and type before executing.
