# ID Discovery

Use this resource when the user knows names but not Coalesce IDs.

## Core Rule

Prefer list/get discovery tools over guessing IDs from URLs or names.

## Common ID Lookups

### Project IDs

- Use `coalesce_list_projects` to browse projects.
- Use `coalesce_get_project` when you already know the `projectID`.

### Workspace IDs

- Use `coalesce_list_workspaces()` to get all workspaces across projects.
- Use `coalesce_list_workspaces({ projectID })` to get workspaces for a specific project.

### Job IDs

- Jobs are listed by environment but created/updated/deleted by workspace.
- Use `coalesce_list_environment_jobs({ environmentID })` to discover job IDs.

If the user gives a job name, resolve it to a job ID before calling `coalesce_start_run`.

### Environment IDs

- Use `coalesce_list_environments` to discover environments by name.
- Use `coalesce_get_environment` only after you already know the `environmentID`.

### Node IDs

- Use `coalesce_list_workspace_nodes` when working in a workspace.
- Use `coalesce_list_environment_nodes` when working against an environment.
- Use `coalesce_get_workspace_node` or `coalesce_get_environment_node` only after you know the node ID.

### Run IDs and Run Counters

- Use `coalesce_list_runs` to discover recent runs when needed.
- For run ID format details (runCounter vs UUID) and operational usage, see `coalesce://context/run-operations`.

### Org IDs

- For org ID requirements in run operations (coalesce_cancel_run), see `coalesce://context/run-operations`.

## Good Defaults

1. Discover by name with a list tool.
2. Resolve the exact ID.
3. Use the matching get/mutate tool with that ID.

## Related Resources

- `coalesce://context/tool-usage`
- `coalesce://context/run-operations`
- `coalesce://context/sql-platform-selection`
