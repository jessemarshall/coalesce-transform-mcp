# Coalesce Transform MCP Examples

Real-world usage patterns and workflows with the Coalesce Transform MCP.

## Table of Contents

- [Environment Inspection](#environment-inspection)
- [Job Execution](#job-execution)
- [Node Management](#node-management)
- [Complete Workflows](#complete-workflows)

---

## Environment Inspection

### Get environment overview with all nodes

```typescript
// Single call to get environment + all nodes
const overview = await getEnvironmentOverview({ environmentID: "9" });

console.log(`Environment: ${overview.environment.name}`);
console.log(`Status: ${overview.environment.status}`);
console.log(`Nodes: ${overview.nodes.length}`);
```

### Find environments for a project

```typescript
// List all environments
const environments = await listEnvironments();

// Filter by project ID
const projectEnvs = environments.data.filter(
  env => env.project === "f7228082-6bce-4514-b32e-ddebc8b71bcb"
);

console.log(`Project has ${projectEnvs.length} environments`);
```

### Inspect node details in an environment

```typescript
// Get node with full metadata
const node = await getEnvironmentNode({
  environmentID: "9",
  nodeID: "7b962179-39d0-46fe-a22f-219c571ff618"
});

console.log(`Node: ${node.name}`);
console.log(`Type: ${node.nodeType}`);
console.log(`Table: ${node.database}.${node.schema}.${node.table}`);
console.log(`Columns: ${node.metadata.columns.length}`);

// Print column names and types
node.metadata.columns.forEach(col => {
  console.log(`  ${col.name}: ${col.dataType}`);
});
```

---

## Job Execution

All run-triggering examples below (`startRun`, `retryAndWait`, and `runAndWait`) assume Snowflake Key Pair auth is configured.

### Run a specific job and wait for completion

```typescript
// Start the job
const run = await startRun({
  runDetails: {
    environmentID: "9",
    jobID: "14"
  }
});

console.log(`Started run ${run.runCounter}`);

// Poll until complete
let status;
do {
  await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds
  status = await runStatus({ runCounter: run.runCounter });
  console.log(`Status: ${status.runStatus}`);
} while (!["completed", "failed", "canceled"].includes(status.runStatus));

// Get detailed results
if (status.runStatus === "completed") {
  const results = await getRunDetails({ runID: String(run.runCounter) });
  const duration = new Date(results.run.runEndTime) - new Date(results.run.runStartTime);
  console.log(`Duration: ${duration}ms`);
  console.log(`Nodes: ${results.run.runDetails.nodesInRun}`);

  // Print node results
  results.results.data.forEach(node => {
    console.log(`  ${node.name}: ${node.runState}`);
  });
}
```

### Run with the simplified workflow

```typescript
// Automatic polling with timeout
const result = await runAndWait({
  runDetails: {
    environmentID: "9",
    jobID: "14"
  },
  pollInterval: 10,
  timeout: 1800
});

console.log(`Run completed: ${result.status.runStatus}`);
console.log(`Result rows: ${result.results?.data?.length ?? 0}`);
```

### Run specific nodes ad-hoc

```typescript
// Run just two specific nodes
const run = await startRun({
  runDetails: {
    environmentID: "9",
    includeNodesSelector: "node:STG_CUSTOMER,node:DIM_CUSTOMER",
    parallelism: 2  // Run both in parallel
  }
});

console.log(`Started run ${run.runCounter}`);
```

### Run with parameters

```typescript
// Pass runtime parameters to the run
const run = await startRun({
  runDetails: {
    environmentID: "9",
    jobID: "14"
  },
  parameters: {
    start_date: "2024-01-01",
    end_date: "2024-12-31",
    incremental: "true"
  }
});
```

### Retry a failed run

```typescript
// Get the failed run
const run = await getRunDetails({ runID: "399" });

if (run.run.runStatus === "failed") {
  // Retry and wait
  const retry = await retryAndWait({
    runDetails: {
      runID: "399"
    },
    pollInterval: 10,
    timeout: 1800
  });

  console.log(`Retry ${retry.status.runStatus}`);
}
```

---

## Node Management

### Check Available Node Types Before Creation

```typescript
// 1. Check what node types exist in the workspace
const { nodeTypes, counts, total } = await listWorkspaceNodeTypes({
  workspaceID: "1"
});

console.log(`Workspace has ${total} nodes across ${nodeTypes.length} types`);
console.log("\nAvailable node types (by frequency):");
nodeTypes.forEach(type => {
  console.log(`  ${type}: ${counts[type]} nodes`);
});

// 2. Verify desired type exists before creating
const desiredType = "Dimension";
if (!nodeTypes.includes(desiredType)) {
  console.warn(`❌ ${desiredType} nodes are not available in this workspace.`);
  console.warn(`   Install the required package via the Coalesce UI first.`);
} else {
  console.log(`✅ ${desiredType} nodes are available (${counts[desiredType]} existing)`);
  // Safe to proceed with node creation
}
```

### List nodes by type in workspace

```typescript
// Get all nodes
const allNodes = await listWorkspaceNodes({ workspaceID: "1" });

// Group by type
const byType = allNodes.data.reduce((acc, node) => {
  acc[node.nodeType] = acc[node.nodeType] || [];
  acc[node.nodeType].push(node.name);
  return acc;
}, {});

console.log("Nodes by type:");
Object.entries(byType).forEach(([type, nodes]) => {
  console.log(`  ${type}: ${nodes.length}`);
});
```

### Create a staging node from a source

```typescript
const stage = await createWorkspaceNodeFromPredecessor({
  workspaceID: "1",
  nodeType: "Stage",
  predecessorNodeIDs: ["source-node-id-here"],
  changes: {
    name: "STG_ORDERS",
    description: "Auto-populated from ORDERS"
  }
});

console.log(`Created ${stage.node.name} with ${stage.validation.columnCount} columns`);
```

### Update all column data types in a node

```typescript
const node = await getWorkspaceNode({
  workspaceID: "1",
  nodeID: "f1a3e7c9-1ae3-4f08-b665-233bd97f8ea7"
});

const updatedColumns = node.metadata.columns.map((col) => ({
  ...col,
  dataType: "NUMBER(110,100)"
}));

await updateWorkspaceNode({
  workspaceID: "1",
  nodeID: node.id,
  changes: {
    metadata: {
      columns: updatedColumns
    }
  }
});

console.log(`Updated ${updatedColumns.length} columns`);
```

### Clone a node structure

```typescript
const source = await getWorkspaceNode({
  workspaceID: "1",
  nodeID: "original-node-id"
});

const clone = await createWorkspaceNodeFromPredecessor({
  workspaceID: "1",
  nodeType: source.nodeType ?? "Stage",
  predecessorNodeIDs: source.predecessorNodeIDs ?? [],
  changes: {
    name: source.name + "_COPY",
    description: source.description,
    config: source.config,
    database: source.database,
    schema: source.schema,
    locationName: source.locationName
  }
});

console.log(`Cloned ${source.name} to ${clone.node.name}`);
```

---

## Complete Workflows

### Investigate a failed run

```typescript
// Get run details
const run = await getRunDetails({ runID: "398" });

console.log(`Run Status: ${run.run.runStatus}`);
console.log(`Environment: ${run.run.runDetails.environmentID}`);
console.log(`Job: ${run.run.runDetails.jobID}`);
console.log(`Started: ${run.run.runStartTime}`);
console.log(`Ended: ${run.run.runEndTime}`);

// Find failed nodes
const failedNodes = run.results.data.filter(
  node => node.runState === "failed"
);

console.log(`\nFailed nodes: ${failedNodes.length}`);
failedNodes.forEach(node => {
  console.log(`\n${node.name}:`);

  // Print query results with errors
  node.queryResults.forEach(query => {
    if (!query.success) {
      console.log(`  Query: ${query.name}`);
      console.log(`  Error: ${query.status}`);
      console.log(`  SQL: ${query.sql.substring(0, 200)}...`);
    }
  });
});
```

### Deploy and test workflow

```typescript
// 1. Get environment to deploy to
const env = await getEnvironment({ environmentID: "9" });
console.log(`Deploying to: ${env.name}`);

// 2. Run all nodes in the environment
const run = await runAndWait({
  runDetails: {
    environmentID: "9"
  },
  confirmRunAllNodes: true,
  pollInterval: 15,
  timeout: 3600  // 1 hour
});

// 3. Check results
if (run.status.runStatus === "completed" && run.results?.data) {
  const summary = {
    total: run.results.data.length,
    succeeded: run.results.data.filter(n => n.runState === "complete").length,
    failed: run.results.data.filter(n => n.runState === "failed").length
  };

  console.log(`\nDeployment Summary:`);
  console.log(`  Total nodes: ${summary.total}`);
  console.log(`  Succeeded: ${summary.succeeded}`);
  console.log(`  Failed: ${summary.failed}`);

  if (summary.failed === 0) {
    console.log(`\n✓ Deployment successful!`);
  } else {
    console.log(`\n✗ Deployment had failures`);
  }
} else {
  console.log(`\n✗ Deployment ${run.status.runStatus}`);
}
```

### Compare environments

```typescript
// Get two environments
const [prod, dev] = await Promise.all([
  getEnvironmentOverview({ environmentID: "9" }),
  getEnvironmentOverview({ environmentID: "13" })
]);

// Compare node counts
console.log(`Production nodes: ${prod.nodes.length}`);
console.log(`Development nodes: ${dev.nodes.length}`);

// Find nodes in dev but not in prod
const prodNodeNames = new Set(prod.nodes.map(n => n.name));
const newNodes = dev.nodes.filter(n => !prodNodeNames.has(n.name));

if (newNodes.length > 0) {
  console.log(`\nNew nodes in dev:`);
  newNodes.forEach(n => console.log(`  - ${n.name} (${n.nodeType})`));
}

// Compare configurations
console.log(`\nProd mappings: ${Object.keys(prod.environment.currentMappings).length}`);
console.log(`Dev mappings: ${Object.keys(dev.environment.currentMappings).length}`);
```

### Bulk node updates

```typescript
// Get all stage nodes in workspace
const allNodes = await listWorkspaceNodes({ workspaceID: "1" });
const stageNodes = allNodes.data.filter(n => n.nodeType === "Stage");

console.log(`Found ${stageNodes.length} stage nodes`);

// Update truncateBefore setting for all
for (const node of stageNodes) {
  const details = await getWorkspaceNode({
    workspaceID: "1",
    nodeID: node.id
  });

  await updateWorkspaceNode({
    workspaceID: "1",
    nodeID: node.id,
    changes: {
      config: {
        truncateBefore: false
      }
    }
  });

  console.log(`Updated ${details.name}`);
}

console.log("Bulk update complete");
```

### Monitor long-running job

```typescript
// Start the job
const run = await startRun({
  runDetails: {
    environmentID: "9",
    jobID: "22"
  }
});

console.log(`Started run ${run.runCounter}`);

// Monitor with progress updates
const startTime = Date.now();
let status;

do {
  await new Promise(resolve => setTimeout(resolve, 30000)); // 30 seconds

  status = await runStatus({ runCounter: run.runCounter });
  const elapsed = Math.floor((Date.now() - startTime) / 1000);

  if (status.runLink) {
    console.log(`Monitor: ${status.runLink}`);
  }
  console.log(`[${elapsed}s] Status: ${status.runStatus}`);

} while (!["completed", "failed", "canceled"].includes(status.runStatus));

// Final results
const results = await getRunDetails({ runID: String(run.runCounter) });
const duration = new Date(results.run.runEndTime) - new Date(results.run.runStartTime);

console.log(`\nRun ${status.runStatus} in ${duration}ms`);
console.log(`Nodes processed: ${results.run.runDetails.nodesInRun}`);
```

---

## Tips and Tricks

### Get run counter from run link

```typescript
// Parse run counter from Coalesce run link
const runLink = "https://app.coalescesoftware.io/runs/401/f7228082.../9";
const runCounter = parseInt(runLink.split("/runs/")[1].split("/")[0]);

console.log(`Run counter: ${runCounter}`); // 401
```

### Find workspace ID for a project

```typescript
const projects = await listProjects({ includeWorkspaces: true });
const project = projects.data.find(p => p.name === "My Project");

if (project) {
  project.workspaces.forEach(ws => {
    console.log(`Workspace ${ws.id}: ${ws.name}`);
  });
}
```

### Calculate run duration

```typescript
const results = await getRunDetails({ runID: "401" });

const start = new Date(results.run.runStartTime);
const end = new Date(results.run.runEndTime);
const durationMs = end - start;
const durationSec = durationMs / 1000;

console.log(`Run duration: ${durationSec.toFixed(1)}s`);
```

### Check if environment is running

```typescript
const env = await getEnvironment({ environmentID: "9" });

if (env.currentlyRunningJobs.length > 0) {
  console.log(`Environment busy with ${env.currentlyRunningJobs.length} running jobs`);
} else {
  console.log("Environment ready for new runs");
}
```
