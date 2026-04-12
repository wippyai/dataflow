# Workflow Compaction Design

## Status

This is still design-only.

- Do not implement from this document until the in-flight Tier 1 work lands:
  - vacuum bootloader
  - agent checkpoint/compaction changes
- This rewrite keeps the same top 3 opportunities:
  - cycle iteration history pruning
  - parallel checkpoint/result compaction
  - yield history compaction
- The goal here is implementation readiness:
  - retention contract is explicit
  - integration points name current files and line ranges
  - each top-3 item has hooks, config shape, and a test matrix

## Hard Invariants

- Never delete a row unless an equal-or-better recovery checkpoint is already durable.
- Prefer "create new checkpoint row + delete old rows" in the same durable commit.
- Tier 1 vacuum owns age-based deletion. Tier 2 and Tier 3 own live-workflow compaction.
- Tier 1 only acts on terminal workflows.
- `DELETE_WORKFLOW` is the full-workflow cleanup primitive:
  - `src/persist/ops.lua:785-814`
  - FK cascades are already in place:
    - `src/migrations/01_create_dataflows_table.lua:16`
    - `src/migrations/02_create_nodes_table.lua:16-18`
    - `src/migrations/03_create_data_table.lua:17-18`
    - `src/migrations/05_create_dataflow_commits_table.lua:13`
- Delete operations must stay idempotent. A second delete on a missing row must remain a no-op, not a correctness failure.

## Tier Model

| Tier | Scope | Trigger | Deletes what |
| --- | --- | --- | --- |
| Tier 1 | Age-based vacuum | completed workflow exceeds retention | whole workflow via `DELETE_WORKFLOW` |
| Tier 2 | Node-local compaction | iteration end, yield satisfy, node terminal, parallel terminal | historical rows for one node |
| Tier 3 | Rolling workflow checkpoint | periodic frontier checkpoint on active workflows | workflow-wide historical rows outside the live set |

## Tier 1 Retention Contract

### Global knobs

- `data_retention`
  - age threshold for deleting completed workflows
  - when it expires, the workflow is removed from the DB entirely
- `commit_retention`
  - default is now:
    - `data_retention` when `data_retention` is set
    - `7d` when `data_retention` is unset

### Deletion semantics

When `data_retention` expires for a completed workflow, Tier 1 deletes the workflow row and relies on FK cascade to delete:

- the `dataflows` row
- all `dataflow_nodes`
- all `dataflow_data`
  - including intermediate data
  - including outputs/results
  - including `node.yield`, `node.yield.result`, `node.signal`, `parallel.progress`, `cycle.state`, `iteration.result`, `iteration.error`
- all `dataflow_commits`

This is deliberate. After retention expiry, the workflow is gone from the hot DB. Long-tail forensics are an external archival concern.

### Retention anchor

Current code has no `completed_at` column. The practical retention clock is the terminal `dataflows.updated_at` written by:

- workflow completion: `src/runner/orchestrator.lua:358-369`
- workflow cancellation: `src/runner/orchestrator.lua:555-563`
- `UPDATE_WORKFLOW` timestamp update: `src/persist/ops.lua:763-781`

That is good enough for the first pass, but see open questions at the end.

## Recovery Live Set

This is the durable state restart recovery actually needs before Tier 1 deletes the workflow.

| Scope | Recovery-critical durable state | Historical-only once checkpointed |
| --- | --- | --- |
| Workflow runner | `dataflows` row, live `dataflow_nodes`, active `node.input`, active `node.yield`, live `node.signal`, durable terminal outputs/results | satisfied yields, stale signals, inputs for terminal nodes |
| `cycle` | newest retained `cycle.state`; child output already folded into `last_result` | older `cycle.state`, older `cycle.function_result` |
| `parallel` | newest cursor, one completion row per completed iteration, raw iteration rows only for active iterations without completion row | older cursor rows, raw iteration rows after completion row exists |
| yielding nodes | active unsatisfied `node.yield`; `node.yield.result` until parent is terminal | satisfied `node.yield`, terminal-parent `node.yield.result` |

## Top 3 Opportunities

### 1. Cycle Iteration History Pruning

### Why it stays in the top 3

- `cycle.state` grows one row per committed iteration.
- `cycle.function_result` grows one row per function iteration.
- Recovery only needs the newest `cycle.state` row and its embedded `last_result`.

### Concrete integration plan

| Item | Plan |
| --- | --- |
| Files to change | `src/node/cycle/cycle.lua:70-83`, `107-116`, `118-156`, `719-725`; `src/flow/flow.lua:168-179`; `src/flow/flow.spec.md:666-704` |
| Hook | `on_iteration_end`, immediately after `persist_state()` and before `n:submit()` at `src/node/cycle/cycle.lua:719-725` |
| Recovery reader | keep `load_persisted_state()` as the source of truth at `src/node/cycle/cycle.lua:118-156` |
| Config flag | `cycle_history_depth = nil | positive_integer` |
| Schema surface | cycle node config table assembled in `src/flow/flow.lua:168-179` |
| Default | `nil` = disabled, keep all history |
| Disable | omit the field or set `nil` |

### Implementation shape

1. Add a helper in `src/node/cycle/cycle.lua` near the existing persistence helpers:
   - load all `cycle.state` rows for `n.node_id`
   - order by `metadata.iteration`, then by v7 `data_id` for tie-break
   - keep the newest `cycle_history_depth` rows
   - queue `DELETE_DATA` for older `cycle.state` rows
2. Do the same for `cycle.function_result`:
   - keep only rows whose `metadata.iteration` is within the retained state window
3. Call that helper after the new `cycle.state` row is queued and before `n:submit()`:
   - `persist_state(n, current_state, iteration_number, last_result)`
   - `queue_cycle_history_compaction(n, iteration_number, depth)`
   - `n:submit()`

### Safety rule

The delete must be in the same durable submit as the new `cycle.state` row. If the process dies before `n:submit()`, no old rows are lost. If it dies after `n:submit()`, the new state row and old-row deletes are already committed together.

### Test matrix

| Test | File path | Setup | Trigger | Assertion | Durability proof |
| --- | --- | --- | --- | --- | --- |
| Retain newest state rows only | `src/node/cycle/cycle_compaction_test.lua` | Create a cycle workflow with `cycle_history_depth = 1`, run 5 iterations | normal completion | `cycle.state` row count goes from 5 to 1; `cycle.function_result` row count goes from 5 to 1 or 0 depending on retained-window rule | kill after iteration 3 before `n:submit()`; restart; final output matches no-compaction run and retained row count is still correct |
| Retain newest `k` rows | `src/node/cycle/cycle_compaction_test.lua` | same workflow with `cycle_history_depth = 3`, run 7 iterations | normal completion | `cycle.state` count = 3; surviving rows have iterations 5, 6, 7 | kill after commit of iteration 5; restart; final output unchanged and iterations 5-7 remain after completion |
| Recovery uses latest checkpoint only | `src/node/cycle/cycle_compaction_test.lua` | cycle workflow that persists nontrivial `last_result` and state deltas each iteration | terminate orchestrator after a compacting iteration commit, then restart | restart resumes from newest retained `cycle.state` and does not reapply already folded child output | verify no duplicate terminal output and no duplicated state transition |

### 2. Parallel Checkpoint / Result Compaction

### Why it stays in the top 3

- `parallel.progress` accumulates cursor rows and per-iteration completion rows.
- `iteration.result` / `iteration.error` accumulate raw child output rows.
- Recovery already prefers completion rows once they exist.

### Concrete integration plan

| Item | Plan |
| --- | --- |
| Files to change | `src/node/parallel/parallel.lua:681-746`, `752-806`, `1008-1061`, `1063-1207`, `1422-1435`; `src/node/parallel/iterator.lua:65-108`; `src/flow/flow.lua:214-226`; `src/flow/flow.spec.md:704-780` |
| Hooks | `on_batch_submit` for cursor compaction, `on_iteration_complete` for raw-row compaction, `on_node_complete` for optional terminal collapse |
| Recovery reader | keep `load_parallel_progress()` as the single reader at `src/node/parallel/parallel.lua:681-746` |
| Config flags | `parallel_compact_cursor_history = false`; `parallel_compact_iterations = false`; `parallel_compact_on_complete = false` |
| Schema surface | parallel node config table assembled in `src/flow/flow.lua:214-226` |
| Default | all off for first ship |
| Disable | leave flags unset or explicitly `false` |

### Implementation shape

#### Cursor history compaction

Hook: `queue_parallel_cursor()` / `persist_parallel_cursor()` at `src/node/parallel/parallel.lua:752-783`

1. Before submit, query existing `parallel.progress` rows with key `cursor`.
2. Queue one new cursor row.
3. Queue `DELETE_DATA` for older cursor rows.
4. Submit once.

#### Iteration raw-row compaction

Hook: `persist_iteration_completion()` at `src/node/parallel/parallel.lua:785-806`

1. Completion row is the durable checkpoint.
2. After queueing that completion row, query raw `iteration.result` / `iteration.error` rows for the same iteration.
3. Queue deletes for those raw rows.
4. Submit once.

`src/node/parallel/iterator.lua:65-108` already stamps iteration metadata needed to identify those raw rows safely.

#### Terminal compaction

Hook: right before `return n:complete(...)` at `src/node/parallel/parallel.lua:1426-1435`

1. Final output has already been rebuilt in memory.
2. If `parallel_compact_on_complete = true`, queue deletes for:
   - all `parallel.progress`
   - all remaining `iteration.result`
   - all remaining `iteration.error`
3. Let `n:complete()` submit the final output and those deletes in the same final commit.

### Safety rule

- Never delete raw iteration rows before the matching completion row is in the same submit.
- Never delete all progress rows before the final workflow/node output is queued in the same final submit.

### Test matrix

| Test | File path | Setup | Trigger | Assertion | Durability proof |
| --- | --- | --- | --- | --- | --- |
| Cursor rows collapse to latest only | `src/node/parallel/parallel_compaction_test.lua` | Parallel workflow with `parallel_compact_cursor_history = true`, `batch_size = 2`, 6 items | normal completion | `parallel.progress` cursor rows go from multiple to 1 latest cursor row; final output unchanged | kill after first batch cursor commit; restart; completed items are not rerun and cursor row count stays bounded |
| Raw iteration rows are deleted after completion row exists | `src/node/parallel/parallel_compaction_test.lua` | Parallel workflow with `parallel_compact_iterations = true`, 4 items, mix of success and failure | finish first batch | before compaction: raw `iteration.result/error` rows exist; after compaction: per completed iteration there is 1 completion row and 0 raw rows | kill after raw rows exist but before completion submit; restart; raw rows remain and are recovered correctly; kill again after completion+delete submit; restart sees completion row and does not rerun |
| Terminal collapse removes all progress rows | `src/node/parallel/parallel_compaction_test.lua` | Parallel workflow with all three flags enabled | normal completion | after workflow completion: output rows remain, `parallel.progress` = 0, `iteration.result/error` = 0 | kill immediately after final `n:complete()` submit; restart observes completed workflow with correct output and no missing result |

### 3. Yield History Compaction

### Why it stays in the top 3

- Every `yield()` appends a `node.yield`.
- Every satisfaction appends a `node.yield.result`.
- Recovery reconstructs live waits by scanning both sets in `src/runner/workflow_state.lua:544-653`.

### Concrete integration plan

| Item | Plan |
| --- | --- |
| Files to change | `src/node.lua:393-425`; `src/runner/workflow_state.lua:544-653`, `939-1024`, `1057-1075`; `src/runner/orchestrator.lua:301-335`, `342-386`; `src/client.lua:66-83`; `src/flow/flow.lua:68-74`, `361-367`, `415-420`; `src/flow/flow.spec.md:310-321` |
| Hooks | `on_yield_satisfied`, `on_node_terminal`, fallback `on_workflow_complete` sweep |
| Recovery reader | keep `_reconstruct_active_yields()` at `src/runner/workflow_state.lua:544-653` |
| Config flags | workflow metadata `yield_compact_on_satisfy = false`; `yield_result_compact_on_terminal = false` |
| Schema surface | workflow metadata, not a node-local schema; the user-facing entry points are `client.create_workflow()` and `FlowBuilder:with_metadata()` |
| Default | both off for first ship |
| Disable | leave flags unset or explicitly `false` |

### Implementation shape

#### Satisfied yield compaction

Hook: `workflow_state:satisfy_yield()` at `src/runner/workflow_state.lua:1057-1075`

1. Queue `CREATE_DATA` for `node.yield.result`.
2. If `yield_compact_on_satisfy = true`, query the original `node.yield` row(s) for the same `yield_id`.
3. Queue `DELETE_DATA` for those `node.yield` rows.
4. Persist once.

This aligns with `orchestrator.handle_satisfy_yield()` at `src/runner/orchestrator.lua:301-335`, which already persists before replying to the waiting node process.

#### Terminal yield-result cleanup

Primary hook: `workflow_state:handle_process_exit()` at `src/runner/workflow_state.lua:939-1024`

1. When a node becomes terminal, and `yield_result_compact_on_terminal = true`, query that node's:
   - `node.yield.result`
   - stray `node.yield`
2. Queue deletes before the normal `persist()` call in `src/runner/orchestrator.lua:519-530`.

Fallback hook: `orchestrator.handle_complete_workflow()` at `src/runner/orchestrator.lua:342-386`

1. Final sweep for terminal workflow completion.
2. Only needed as a backstop; the main path should be per-node terminal cleanup.

### Safety rule

- Do not delete the original `node.yield` until the matching `node.yield.result` is part of the same durable persist.
- Do not delete `node.yield.result` until the parent node is terminal.

### Test matrix

| Test | File path | Setup | Trigger | Assertion | Durability proof |
| --- | --- | --- | --- | --- | --- |
| Original yield row disappears once satisfied | `src/runner/yield_compaction_test.lua` | Create workflow state with one active yield and metadata `yield_compact_on_satisfy = true` | call `workflow_state:satisfy_yield()` then persist | `node.yield` count goes from 1 to 0; `node.yield.result` count goes from 0 to 1 | kill after persist returns in `handle_satisfy_yield()` but before reply is sent; restart; parent either resumes or reruns correctly and satisfied yield is not lost |
| Terminal parent removes yield-result history | `src/runner/yield_compaction_test.lua` | Workflow with satisfied yield, then parent node exits terminal with metadata `yield_result_compact_on_terminal = true` | process node exit and persist terminal state | `node.yield.result` count for parent goes from 1 to 0; terminal result/output still exists | kill after terminal persist; restart; workflow stays terminal and no stale yield is reconstructed |
| Newest unsatisfied yield still reconstructs | `src/runner/yield_compaction_test.lua` | Parent has older satisfied yield and newer unsatisfied yield; compaction flags enabled | load workflow state | active yield reconstructs only the newest unsatisfied yield | kill after compaction of satisfied yield, restart, verify unsatisfied yield remains active and satisfiable |

## Tier 1 Vacuum Integration Plan

This is the companion design for the in-flight bootloader work. It is here so Tier 2 and Tier 3 are designed against the same retention semantics.

### Planned behavior

1. Select candidate workflows where:
   - `dataflows.status` is terminal
   - `dataflows.updated_at <= now - data_retention`
2. Delete each candidate with one `DELETE_WORKFLOW` command:
   - `src/persist/ops.lua:785-814`
3. Rely on FK cascade for nodes, data, and commits.
4. Commit the delete in one transaction.

### Commit retention behavior

- Effective `commit_retention` default:
  - `data_retention` when set
  - otherwise `7d`
- Once `data_retention` expires the whole workflow delete also removes commits.

### Tier 1 test matrix

| Test | File path | Setup | Trigger | Assertion | Durability proof |
| --- | --- | --- | --- | --- | --- |
| Full workflow cascade delete | `src/runner/vacuum_retention_test.lua` | Create a completed workflow with rows in `dataflows`, `dataflow_nodes`, `dataflow_data`, `dataflow_commits`; set retention cutoff in the past | run vacuum once | row counts for that `dataflow_id` go from `1 / N / M / K` to `0 / 0 / 0 / 0` | kill vacuum worker after candidate selection but before delete commit; restart; rows are either all still present or all gone, never partial |
| Commit retention default inheritance | `src/runner/vacuum_retention_test.lua` | configure `data_retention = 30d`, leave `commit_retention` unset | compute effective vacuum config and run candidate selection | effective `commit_retention = 30d` | restart vacuum process; effective config stays stable across boot |
| No explicit `data_retention` fallback | `src/runner/vacuum_retention_test.lua` | leave `data_retention` unset and `commit_retention` unset | boot vacuum | effective `commit_retention = 7d` | restart vacuum and verify computed default is deterministic |

## Tier Interaction And Precedence

### No live race: Tier 2 vs Tier 1

- Tier 2 compacts active or just-terminal workflows.
- Tier 1 only vacuums terminal workflows whose age exceeds retention.
- That means there is no normal live race while a workflow is still running.

If an operator configures an effectively-zero retention window, Tier 1 is still safe because it deletes with one `DELETE_WORKFLOW` transaction. At that point Tier 1 supersedes any pending Tier 2 cleanup because the whole workflow is being removed anyway.

### Tier 2 vs Tier 3 on active workflows

Tier 3 depends on the durable checkpoints produced by Tier 2. The order is:

1. Tier 2 writes node-local checkpoints.
2. Tier 2 may delete node-local historical rows in that same commit.
3. Tier 3, if enabled, computes the workflow frontier from the now-current durable state.
4. Tier 3 deletes workflow-wide history outside that frontier.

Tier 3 must never assume rows that only exist in memory. It always reads the DB after the Tier 2 checkpoint commit.

### Terminal workflows

- Once a workflow is terminal:
  - Tier 3 rolling checkpointing stops
  - Tier 2 terminal cleanup may still run in the final node/workflow commit
  - Tier 1 later owns age-based deletion

### Precedence order

| Situation | Precedence |
| --- | --- |
| Active workflow, iteration/batch/yield boundary | Tier 2 only |
| Active workflow, periodic rolling checkpoint | Tier 2 first, then Tier 3 |
| Just-completed workflow, before retention age | optional Tier 2 terminal cleanup only |
| Completed workflow past retention | Tier 1 wins; delete whole workflow |

## Remaining Opportunities

These stay in the backlog behind the top 3 and Tier 1 retention work.

| Opportunity | Current status |
| --- | --- |
| Signal mailbox compaction | still useful, but audit expectations are unresolved |
| Ephemeral child subgraph pruning | probably the next-biggest absolute byte win after row-only compaction |
| Terminal input-edge pruning | attractive, but interacts with any future rerun semantics |
| Delta-encoded cycle state | not worth doing before bounded history |
| Rolling workflow checkpoint | still Tier 3, still high-risk |
| Tiered archive storage | still deferred; current retention model deletes, not archives |
| Agent observation/memory compaction | must wait for in-flight agent checkpoint changes |

## Shipping Order

By integration readiness:

1. Yield history compaction
   - smallest surface
   - runner-only
   - cheapest crash-safety story
2. Cycle iteration history pruning
   - single node type
   - straightforward same-submit delete
3. Parallel checkpoint/result compaction
   - largest surface
   - most interactions between cursor rows, raw iteration rows, and recovery

By storage impact:

1. Cycle iteration history pruning
2. Parallel checkpoint/result compaction
3. Yield history compaction

## Open Questions

1. Is `dataflows.updated_at` good enough as the retention clock, or do we want an explicit `completed_at` column for Tier 1?
2. When `data_retention` is unset, the new default still gives `commit_retention = 7d`. That is the requested default, but it means commit history can age out while workflow rows remain indefinitely. Confirm that this is intentional.
