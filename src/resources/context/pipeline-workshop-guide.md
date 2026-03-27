# Pipeline Workshop Guide

The pipeline workshop provides an iterative, conversational interface for building pipelines step by step. Unlike `build-pipeline-from-intent` (which parses a single description and creates nodes immediately), the workshop lets users refine a plan across multiple turns before creating anything.

## When to Use

Use the workshop when:
- The user wants to build a pipeline incrementally ("let me start with a join, then we'll add aggregation")
- The user is exploring options and may change their mind ("actually, use customer_id not id")
- The pipeline is complex enough to benefit from step-by-step refinement
- The user wants to see and approve the full plan before any nodes are created

Do NOT use when:
- The user has a clear, complete intent — use `build-pipeline-from-intent` instead
- The user just needs to modify an existing node — use `update-workspace-node`
- The user wants a one-shot pipeline creation — use `plan-pipeline` + `create-pipeline-from-plan`

## Tool Lifecycle

```
1. pipeline-workshop-open    → Start session, get sessionID
2. pipeline-workshop-instruct → Send instructions to refine the plan (repeat)
3. pipeline-workshop-status   → Check current plan state
4. pipeline-workshop-close    → Clean up when done
```

To actually create the nodes, pass the workshop plan to `plan-pipeline` or `build-pipeline-from-intent`.

## Supported Instructions

### Adding Nodes
- "join CUSTOMERS and ORDERS on CUSTOMER_ID"
- "stage the PAYMENTS table"
- "aggregate total REVENUE by REGION"
- "combine PRODUCTS with INVENTORY"

### Modifying the Plan
- "change the join key to ORDER_ID"
- "add filter for STATUS = 'active'"
- "add column FULL_NAME as FIRST_NAME || ' ' || LAST_NAME"
- "remove column MIDDLE_NAME"
- "rename STG_ORDERS to STG_SALES"
- "remove the ORPHAN node"

### Each instruction returns:
- `action`: what was done (added_nodes, updated_join, added_filter, etc.)
- `changes`: human-readable list of what changed
- `currentPlan`: the full current node plan
- `openQuestions`: anything that needs clarification

## Session State

The workshop session persists in the cache directory (`workshops/` subdirectory). It stores:
- **nodes**: Planned nodes with predecessors, columns, join conditions, filters
- **resolvedEntities**: Cached workspace node name → ID mappings
- **history**: All instructions and their outcomes

## Entity Resolution

The workshop pre-loads workspace nodes when opening a session. When an instruction references an entity name:
1. First checks the session cache (fast, no API call)
2. Falls back to `resolveIntentEntities()` for fuzzy API matching
3. Unresolved entities generate clarification questions

## Typical Conversation Flow

```
User: "I want to build a pipeline combining customers and orders"
Agent: → pipeline-workshop-open with intent

User: "Actually, join on ORDER_ID not CUSTOMER_ID"
Agent: → pipeline-workshop-instruct "change join key to ORDER_ID"

User: "Add a filter for active customers only"
Agent: → pipeline-workshop-instruct "add filter for IS_ACTIVE = true"

User: "Now aggregate total revenue by region"
Agent: → pipeline-workshop-instruct "aggregate total REVENUE by REGION"

User: "Looks good, create it"
Agent: → Use plan-pipeline with the workshop nodes, then create-pipeline-from-plan
Agent: → pipeline-workshop-close
```

## Important Notes

- The workshop does NOT create nodes — it only builds a plan
- To create nodes from the plan, use `plan-pipeline` or `build-pipeline-from-intent`
- Sessions persist across tool calls but are cleaned up on close
- Each session is scoped to a single workspace
