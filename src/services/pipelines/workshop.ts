import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { z } from "zod";
import type { CoalesceClient } from "../../client.js";
import { isPlainObject } from "../../utils.js";
import { getCacheDir } from "../../cache-dir.js";
import { listWorkspaceNodes } from "../../coalesce/api/nodes.js";
import { CoalesceApiError } from "../../client.js";
import { extractNodeArray } from "../shared/node-helpers.js";
import { parseIntent, resolveIntentEntities } from "./intent.js";

// ── Types ────────────────────────────────────────────────────────────────────

interface WorkshopNodeBase {
  /** Temporary plan ID (before creation) or real node ID (after creation) */
  id: string;
  name: string;
  nodeType: string | null;
  predecessorIDs: string[];
  columns: string[];
  joinCondition: string | null;
  filters: string[];
  groupByColumns: string[];
  aggregates: Array<{ column: string; fn: string }>;
}

export type WorkshopNode =
  | (WorkshopNodeBase & { created: false; createdNodeID: null })
  | (WorkshopNodeBase & { created: true; createdNodeID: string });

export interface WorkshopSession {
  sessionID: string;
  workspaceID: string;
  createdAt: string;
  updatedAt: string;
  nodes: WorkshopNode[];
  history: Array<{ instruction: string; timestamp: string; result: string }>;
  resolvedEntities: Array<{ name: string; nodeID: string; locationName: string | null }>;
}

export interface WorkshopInstructionResult {
  sessionID: string;
  action:
    | "updated"
    | "removed"
    | "renamed"
    | "updated_join"
    | "added_filter"
    | "added_column"
    | "removed_column"
    | "added_nodes"
    | "no_changes"
    | "clarification_needed";
  changes: string[];
  currentPlan: WorkshopNode[];
  openQuestions: string[];
  warnings: string[];
}

const WorkshopNodeSchema = z.object({
  id: z.string(),
  name: z.string(),
  nodeType: z.string().nullable(),
  predecessorIDs: z.array(z.string()),
  columns: z.array(z.string()),
  joinCondition: z.string().nullable(),
  filters: z.array(z.string()),
  groupByColumns: z.array(z.string()),
  aggregates: z.array(z.object({ column: z.string(), fn: z.string() })),
  created: z.boolean(),
  createdNodeID: z.string().nullable(),
}).passthrough();

const WorkshopSessionSchema = z.object({
  sessionID: z.string(),
  workspaceID: z.string(),
  createdAt: z.string(),
  updatedAt: z.string(),
  nodes: z.array(WorkshopNodeSchema),
  history: z.array(z.object({ instruction: z.string(), timestamp: z.string(), result: z.string() })),
  resolvedEntities: z.array(z.object({ name: z.string(), nodeID: z.string(), locationName: z.string().nullable() })),
}).passthrough();

// ── Session persistence ──────────────────────────────────────────────────────

const WORKSHOP_DIR = "workshops";

function getWorkshopDir(): string {
  return join(getCacheDir(), WORKSHOP_DIR);
}

function getSessionPath(sessionID: string): string {
  // Sanitize sessionID to prevent path traversal
  const safe = sessionID.replace(/[^a-zA-Z0-9_-]/g, "");
  if (safe.length === 0) {
    throw new Error("Invalid sessionID: must contain at least one alphanumeric character.");
  }
  return join(getWorkshopDir(), `${safe}.json`);
}

export function loadSession(sessionID: string): WorkshopSession | null {
  const path = getSessionPath(sessionID);
  if (!existsSync(path)) return null;
  let content: string;
  try {
    content = readFileSync(path, "utf8");
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(`Workshop session file exists but could not be read: ${reason}`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(content);
  } catch {
    throw new Error(
      `Workshop session file contains invalid JSON — it may be corrupted. Delete it and start a new session.`
    );
  }
  const result = WorkshopSessionSchema.safeParse(parsed);
  if (result.success) {
    return result.data as WorkshopSession;
  }
  const issues = result.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ");
  throw new Error(
    `Workshop session file has invalid structure (${issues}). Delete it and start a new session.`
  );
}

function saveSession(session: WorkshopSession): void {
  const dir = getWorkshopDir();
  try {
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
    session.updatedAt = new Date().toISOString();
    writeFileSync(getSessionPath(session.sessionID), JSON.stringify(session, null, 2), "utf8");
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Failed to save workshop session ${session.sessionID}: ${reason}. ` +
        `Check disk space and file permissions at ${dir}.`
    );
  }
}

export function deleteSession(sessionID: string): boolean {
  const path = getSessionPath(sessionID);
  if (!existsSync(path)) return false;
  try {
    unlinkSync(path);
    return true;
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Could not delete workshop session file: ${reason}. You may need to delete it manually at ${path}.`
    );
  }
}

// ── Session lifecycle ────────────────────────────────────────────────────────

export async function openWorkshop(
  client: CoalesceClient,
  params: { workspaceID: string; intent?: string }
): Promise<WorkshopSession & { openQuestions: string[]; warnings: string[] }> {
  const sessionID = randomUUID();
  const session: WorkshopSession = {
    sessionID,
    workspaceID: params.workspaceID,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    nodes: [],
    history: [],
    resolvedEntities: [],
  };
  const sessionWarnings: string[] = [];

  // Pre-resolve workspace entities for faster subsequent instructions
  try {
    const rawNodes = await listWorkspaceNodes(client, { workspaceID: params.workspaceID });
    const nodeList = extractNodeArray(rawNodes);
    for (const n of nodeList) {
      if (typeof n.id === "string" && typeof n.name === "string") {
        session.resolvedEntities.push({
          name: n.name,
          nodeID: n.id,
          locationName: typeof n.locationName === "string" ? n.locationName : null,
        });
      }
    }
  } catch (error) {
    if (error instanceof CoalesceApiError && [401, 403, 503].includes(error.status)) {
      throw error;
    }
    const reason = error instanceof Error ? error.message : String(error);
    sessionWarnings.push(
      `Could not pre-load workspace nodes (${reason}). Entity resolution will require API calls per instruction.`
    );
  }

  let openQuestions: string[] = [];

  // If an initial intent is provided, process it
  if (params.intent) {
    const result = await processInstruction(client, session, params.intent);
    openQuestions = result.openQuestions;
    sessionWarnings.push(...result.warnings);
  }

  saveSession(session);
  return { ...session, openQuestions, warnings: sessionWarnings };
}

export function getWorkshopStatus(sessionID: string): WorkshopSession | null {
  return loadSession(sessionID);
}

export async function workshopInstruct(
  client: CoalesceClient,
  params: { sessionID: string; instruction: string }
): Promise<WorkshopInstructionResult> {
  const session = loadSession(params.sessionID);
  if (!session) {
    throw new Error(
      `Workshop session "${params.sessionID}" not found. Use pipeline-workshop-open to start a new session.`
    );
  }

  const result = await processInstruction(client, session, params.instruction);
  saveSession(session);
  return result;
}

export function workshopClose(sessionID: string): { closed: boolean; message: string } {
  const session = loadSession(sessionID);
  if (!session) {
    return { closed: false, message: `Session "${sessionID}" not found.` };
  }

  const uncreated = session.nodes.filter((n) => !n.created).length;
  deleteSession(sessionID);

  return {
    closed: true,
    message: uncreated > 0
      ? `Session closed. ${uncreated} planned node(s) were not created — use build_pipeline_from_intent or plan_pipeline + create_pipeline_from_plan to create them.`
      : "Session closed.",
  };
}

// ── Instruction processing ───────────────────────────────────────────────────

async function processInstruction(
  client: CoalesceClient,
  session: WorkshopSession,
  instruction: string
): Promise<WorkshopInstructionResult> {
  const changes: string[] = [];
  const openQuestions: string[] = [];
  const warnings: string[] = [];
  let action: WorkshopInstructionResult["action"] = "updated";

  const lower = instruction.toLowerCase().trim();

  // Handle remove/delete instructions
  const removeMatch = lower.match(/(?:remove|delete|drop)\s+(?:the\s+)?(?:node\s+)?["']?(\w+)["']?/);
  const isColumnOp = lower.match(/(?:remove|delete|drop)\s+(?:the\s+)?column\s/);

  if (removeMatch && !isColumnOp) {
    const targetName = removeMatch[1]!.toUpperCase();
    const idx = session.nodes.findIndex(
      (n) => n.name.toUpperCase() === targetName
    );
    if (idx >= 0) {
      const removed = session.nodes.splice(idx, 1)[0]!;
      // Remove references from other nodes
      for (const node of session.nodes) {
        node.predecessorIDs = node.predecessorIDs.filter((id) => id !== removed.id);
      }
      changes.push(`Removed node "${removed.name}" from the plan.`);
      action = "removed";
    } else {
      openQuestions.push(
        `No node named "${targetName}" found in the current plan. Available nodes: ${session.nodes.map((n) => n.name).join(", ") || "(none)"}.`
      );
    }
    addHistory(session, instruction, action);
    return buildResult(session, action, changes, openQuestions, warnings);
  }

  // Handle rename instructions
  const renameMatch = lower.match(/rename\s+["']?(\w+)["']?\s+(?:to|as)\s+["']?(\w+)["']?/);
  if (renameMatch) {
    const oldName = renameMatch[1]!.toUpperCase();
    const newName = renameMatch[2]!.toUpperCase();
    const node = session.nodes.find((n) => n.name.toUpperCase() === oldName);
    if (node) {
      node.name = newName;
      changes.push(`Renamed "${oldName}" to "${newName}".`);
      action = "renamed";
    } else {
      openQuestions.push(`No node named "${oldName}" found in the current plan.`);
    }
    addHistory(session, instruction, action);
    return buildResult(session, action, changes, openQuestions, warnings);
  }

  // Handle "change join key" / "join on X" instructions
  const joinKeyMatch = lower.match(/(?:change|update|set)\s+(?:the\s+)?join\s+(?:key|column|condition)\s+(?:to|on)\s+["']?(\w+)["']?/);
  const joinOnMatch = !joinKeyMatch ? lower.match(/join\s+on\s+["']?(\w+)["']?/) : null;
  const keyMatch = joinKeyMatch || joinOnMatch;
  if (keyMatch) {
    const newKey = keyMatch[1]!.toUpperCase();
    const joinNode = session.nodes.find(
      (n) => n.predecessorIDs.length >= 2
    );
    if (joinNode) {
      joinNode.joinCondition = rebuildJoinCondition(joinNode, session, newKey);
      changes.push(`Updated join key to "${newKey}" on node "${joinNode.name}".`);
      action = "updated_join";
    } else {
      openQuestions.push("No join node found in the current plan to update.");
    }
    addHistory(session, instruction, action);
    return buildResult(session, action, changes, openQuestions, warnings);
  }

  // Handle "add filter" instructions
  const filterMatch = lower.match(/(?:add|set)\s+(?:a\s+)?filter\s+(?:for|on|where)?\s*(.+)/);
  if (filterMatch) {
    const filterExpr = filterMatch[1]!.trim();
    // Apply to the last node or a named node
    const targetNode = session.nodes.length > 0
      ? session.nodes[session.nodes.length - 1]!
      : null;
    if (targetNode) {
      targetNode.filters.push(filterExpr);
      changes.push(`Added filter "${filterExpr}" to node "${targetNode.name}".`);
      action = "added_filter";
    } else {
      openQuestions.push("No nodes in the plan to add a filter to.");
    }
    addHistory(session, instruction, action);
    return buildResult(session, action, changes, openQuestions, warnings);
  }

  // Handle "add column" instructions
  const addColMatch = lower.match(/add\s+(?:a\s+)?column\s+["']?(\w+)["']?(?:\s+(?:as|with|=)\s+(.+))?/);
  if (addColMatch) {
    const colName = addColMatch[1]!.toUpperCase();
    const transform = addColMatch[2] ?? null;
    const targetNode = session.nodes.length > 0
      ? session.nodes[session.nodes.length - 1]!
      : null;
    if (targetNode) {
      targetNode.columns.push(colName);
      changes.push(
        transform
          ? `Added column "${colName}" with transform "${transform}" to node "${targetNode.name}".`
          : `Added column "${colName}" to node "${targetNode.name}".`
      );
      action = "added_column";
    } else {
      openQuestions.push("No nodes in the plan to add a column to.");
    }
    addHistory(session, instruction, action);
    return buildResult(session, action, changes, openQuestions, warnings);
  }

  // Handle "remove column" instructions
  const removeColMatch = lower.match(/remove\s+(?:the\s+)?column\s+["']?(\w+)["']?/);
  if (removeColMatch) {
    const colName = removeColMatch[1]!.toUpperCase();
    const targetNode = session.nodes.length > 0
      ? session.nodes[session.nodes.length - 1]!
      : null;
    if (targetNode) {
      const idx = targetNode.columns.findIndex(
        (c) => c.toUpperCase() === colName
      );
      if (idx >= 0) {
        targetNode.columns.splice(idx, 1);
        changes.push(`Removed column "${colName}" from node "${targetNode.name}".`);
      } else {
        openQuestions.push(`Column "${colName}" not found on node "${targetNode.name}".`);
      }
      action = "removed_column";
    }
    addHistory(session, instruction, action);
    return buildResult(session, action, changes, openQuestions, warnings);
  }

  // Default: try to parse as an intent (add new nodes)
  const parsed = parseIntent(instruction);

  if (parsed.openQuestions.length > 0 && parsed.steps.length === 0) {
    openQuestions.push(...parsed.openQuestions);
    addHistory(session, instruction, "clarification_needed");
    return buildResult(session, "clarification_needed", changes, openQuestions, warnings);
  }

  // Resolve entities from the instruction
  const allEntityNames = parsed.steps.flatMap((s) => s.entityNames);
  const uniqueEntityNames = [...new Set(allEntityNames)];

  const resolvedEntities = resolveEntitiesFromSession(session, uniqueEntityNames);
  const unresolvedNames: string[] = [];

  for (const name of uniqueEntityNames) {
    if (!resolvedEntities.has(name.toUpperCase())) {
      unresolvedNames.push(name);
    }
  }

  // Try API resolution for unresolved entities
  if (unresolvedNames.length > 0) {
    try {
      const apiResolved = await resolveIntentEntities(
        client,
        session.workspaceID,
        unresolvedNames
      );
      for (const entity of apiResolved) {
        if (entity.confidence !== "unresolved") {
          resolvedEntities.set(entity.rawName.toUpperCase(), {
            nodeID: entity.resolvedNodeID,
            name: entity.resolvedNodeName,
            locationName: entity.resolvedLocationName,
          });
          // Add to session cache
          session.resolvedEntities.push({
            name: entity.resolvedNodeName,
            nodeID: entity.resolvedNodeID,
            locationName: entity.resolvedLocationName,
          });
        }
      }
    } catch (error) {
      if (error instanceof CoalesceApiError && [401, 403, 503].includes(error.status)) {
        throw error;
      }
      const reason = error instanceof Error ? error.message : String(error);
      warnings.push(
        `Entity resolution API call failed (${reason}). Names could not be verified against the workspace.`
      );
    }

    // Check what's still unresolved
    for (const name of unresolvedNames) {
      if (!resolvedEntities.has(name.toUpperCase())) {
        openQuestions.push(
          `Could not find workspace node matching "${name}". Use list_workspace_nodes to check available nodes.`
        );
      }
    }
  }

  // Build workshop nodes from parsed steps
  for (const step of parsed.steps) {
    const predecessorIDs: string[] = [];
    const columns: string[] = [];

    for (const entityName of step.entityNames) {
      const resolved = resolvedEntities.get(entityName.toUpperCase());
      if (resolved) {
        // Check if this entity already exists as a workshop node
        const existingWorkshopNode = session.nodes.find(
          (n) => n.createdNodeID === resolved.nodeID || n.id === resolved.nodeID
        );
        predecessorIDs.push(existingWorkshopNode?.id ?? resolved.nodeID);
      }
    }

    // Also check if any previous workshop nodes should feed into this one
    if (step.operation === "aggregate" && session.nodes.length > 0) {
      const lastNode = session.nodes[session.nodes.length - 1]!;
      if (!predecessorIDs.includes(lastNode.id)) {
        predecessorIDs.push(lastNode.id);
      }
    }

    // Build columns from step
    for (const col of step.columns) {
      if (col.aggregateFunction) {
        columns.push(`${col.aggregateFunction}(${col.name})`);
      } else {
        columns.push(col.name);
      }
    }

    const workshopNode: WorkshopNode = {
      id: `workshop-${randomUUID()}`,
      name: buildNodeName(step, predecessorIDs, resolvedEntities),
      nodeType: null, // Selected at apply time
      predecessorIDs,
      columns,
      joinCondition: step.joinKey
        ? buildInitialJoinCondition(step, resolvedEntities, session)
        : null,
      filters: step.filters,
      groupByColumns: step.groupByColumns,
      aggregates: step.columns
        .filter((c) => c.aggregateFunction)
        .map((c) => ({ column: c.name, fn: c.aggregateFunction! })),
      created: false,
      createdNodeID: null,
    };

    session.nodes.push(workshopNode);
    changes.push(`Added ${step.operation} node "${workshopNode.name}" to the plan.`);
  }

  if (parsed.openQuestions.length > 0) {
    openQuestions.push(...parsed.openQuestions);
  }

  action = changes.length > 0 ? "added_nodes" : "no_changes";
  addHistory(session, instruction, action);
  return buildResult(session, action, changes, openQuestions, warnings);
}

// ── Helpers ──────────────────────────────────────────────────────────────────


function resolveEntitiesFromSession(
  session: WorkshopSession,
  names: string[]
): Map<string, { nodeID: string; name: string; locationName: string | null }> {
  const resolved = new Map<string, { nodeID: string; name: string; locationName: string | null }>();

  for (const requestedName of names) {
    const upper = requestedName.toUpperCase();

    // Check session's resolved entities cache
    for (const entity of session.resolvedEntities) {
      if (entity.name.toUpperCase() === upper) {
        resolved.set(upper, entity);
        break;
      }
      // Try prefix-stripped match
      const stripped = entity.name.replace(/^(STG_|SRC_|RAW_|INT_|DIM_|FACT_|FCT_)/i, "");
      if (stripped.toUpperCase() === upper) {
        resolved.set(upper, entity);
        break;
      }
    }

    // Check existing workshop nodes
    if (!resolved.has(upper)) {
      for (const node of session.nodes) {
        if (node.name.toUpperCase() === upper) {
          resolved.set(upper, { nodeID: node.id, name: node.name, locationName: null });
          break;
        }
      }
    }
  }

  return resolved;
}

function buildNodeName(
  step: ReturnType<typeof parseIntent>["steps"][0],
  predecessorIDs: string[],
  resolvedEntities: Map<string, { name: string }>
): string {
  const entityNames = step.entityNames.map((n) => {
    const resolved = resolvedEntities.get(n.toUpperCase());
    return resolved ? resolved.name : n.toUpperCase();
  });

  switch (step.operation) {
    case "stage":
      return entityNames.length > 0 ? `STG_${entityNames[0]}` : "STG_NEW_NODE";
    case "join":
      return entityNames.length >= 2
        ? `${entityNames[0]}_${entityNames[1]}`
        : `JOIN_${entityNames[0] ?? "NODE"}`;
    case "aggregate":
      return step.groupByColumns.length > 0
        ? `AGG_BY_${step.groupByColumns[0]}`
        : "AGG_SUMMARY";
    default:
      return `WORKSHOP_${entityNames[0] ?? "NODE"}`;
  }
}

function buildInitialJoinCondition(
  step: ReturnType<typeof parseIntent>["steps"][0],
  resolvedEntities: Map<string, { nodeID: string; name: string; locationName: string | null }>,
  session: WorkshopSession
): string | null {
  if (!step.joinKey || step.entityNames.length < 2) return null;

  const entities = step.entityNames.map((n) => resolvedEntities.get(n.toUpperCase())).filter(Boolean);
  if (entities.length < 2) return null;

  const e1 = entities[0]!;
  const e2 = entities[1]!;
  const loc1 = e1.locationName;
  const loc2 = e2.locationName;
  if (!loc1 || !loc2) {
    // Cannot generate a valid {{ ref() }} without location names
    return null;
  }
  const joinType = step.joinType ?? "JOIN";

  return (
    `FROM {{ ref('${loc1}', '${e1.name}') }} "${e1.name}"\n` +
    `${joinType} {{ ref('${loc2}', '${e2.name}') }} "${e2.name}"\n` +
    `  ON "${e1.name}"."${step.joinKey}" = "${e2.name}"."${step.joinKey}"`
  );
}

function rebuildJoinCondition(
  node: WorkshopNode,
  session: WorkshopSession,
  newKey: string
): string | null {
  // Try to reconstruct from existing join condition
  if (node.joinCondition) {
    // Replace the ON clause key
    return node.joinCondition.replace(
      /ON\s+"([^"]+)"\."([^"]+)"\s*=\s*"([^"]+)"\."([^"]+)"/i,
      `ON "$1"."${newKey}" = "$3"."${newKey}"`
    );
  }
  return null;
}

function addHistory(session: WorkshopSession, instruction: string, result: string): void {
  session.history.push({
    instruction,
    timestamp: new Date().toISOString(),
    result,
  });
}

function buildResult(
  session: WorkshopSession,
  action: WorkshopInstructionResult["action"],
  changes: string[],
  openQuestions: string[],
  warnings: string[]
): WorkshopInstructionResult {
  return {
    sessionID: session.sessionID,
    action,
    changes,
    currentPlan: session.nodes,
    openQuestions,
    warnings,
  };
}
