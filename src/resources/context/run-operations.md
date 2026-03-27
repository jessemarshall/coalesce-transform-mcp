# Run Operations

This guidance matches the Coalesce run source model and the actual run workflows registered in this MCP.

## Current Tool Surface

Use these run tools and workflows:

- `list_runs`
- `get_run`
- `get_run_results`
- `get_run_details`
- `start_run`
- `run_status`
- `run_and_wait`
- `retry_run`
- `retry_and_wait`
- `cancel_run`

## Identifier And Status Model

- Coalesce scheduler start and rerun flows return a numeric `runCounter`.
- `run_status` polls by `runCounter`.
- The same numeric identifier is used as `runID` for `get_run`, `get_run_results`, and `get_run_details`.
- Non-terminal statuses are `waitingToRun` and `running`.
- Terminal statuses are `completed`, `failed`, and `canceled`.

## Source-Derived Lifecycle

- Coalesce app helpers call `/scheduler/startRun` or `/scheduler/rerun`, then poll `/scheduler/runStatus` until the run reaches a terminal status.
- The source only treats terminal completion as the point where success or failure can actually be asserted.
- The MCP `run_and_wait` and `retry_and_wait` workflows follow that same model and then fetch `/api/v1/runs/{runCounter}/results`.

## Routing Rules

- Use `run_and_wait` when the user wants the final run outcome in one call.
- Use `retry_and_wait` when the prior run has already failed and should be retried immediately.
- Use `start_run` or `retry_run` when you want explicit control over the polling sequence.
- Use `run_status` for live scheduler state by `runCounter`.
- Use `get_run_details` when you want run metadata and results together.
- Use `get_run` or `get_run_results` when you only need one side of that data.
- Use `cancel_run` only with `runID`, `environmentID`, and org context.

## Practical Checks

- Do not treat "request accepted" as the same thing as "run completed".
- Inspect terminal status and result payloads before reporting success.
- If the user only knows a job name, resolve its numeric ID first.
- `run_and_wait` and `retry_and_wait` can still return `resultsError`, `incomplete`, or `timedOut`; inspect those fields before calling the workflow successful.

## Avoid

- Do not use browser URL UUID fragments as run IDs.
- Do not poll `run_status` with a job ID or environment ID.
- Do not retry runs that are still `waitingToRun` or `running`.
