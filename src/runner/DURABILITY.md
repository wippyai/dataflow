# Runner Durability Notes

## Current Orchestrator Guard

`src/runner/orchestrator.lua` registers `dataflow.<dataflow_id>` in `process.registry` before loading workflow state.

This prevents two orchestrators for the same workflow inside the same runtime/host process registry.

## Known Limitation

There is no database-backed lease or heartbeat on the `dataflows` row today.

That means a true split-brain case is still possible if:

- two separate runtimes can both reach the database
- they do not share the same `process.registry`
- both decide they should run the same `dataflow_id`

In that situation both orchestrators can execute until durable constraints or recovery logic stop them from writing duplicate terminal data. The hard unique slots for `dataflow.output` and successful `node.result` rows reduce duplicate durable outputs, but they do not make external side effects safe by themselves.

## Compaction Contract

Compaction must preserve exactly the same crash-safety contract as normal execution.

See `src/runner/COMPACTION.md` for the full design. The important durability rules are:

- never delete a row before an equal-or-better recovery checkpoint is durable
- prefer "new checkpoint row + old-row delete" in the same commit
- Tier 1 vacuum only deletes terminal workflows
- Tier 1 whole-workflow cleanup is one `DELETE_WORKFLOW` transaction, relying on FK cascade
- Tier 2 and Tier 3 compaction paths must stay idempotent because duplicate or delayed delete attempts are possible in restart and split-brain edge cases

This matters for the current compaction candidates:

- yield compaction is safe only because `handle_satisfy_yield()` persists before replying
- cycle compaction is safe only if old `cycle.state` rows are deleted in the same submit as the new state row
- parallel compaction is safe only if raw iteration rows are deleted in the same submit as the completion row that replaces them

## Why No Lightweight Lease Yet

A correct lease needs all of the following:

- atomic claim/update on the workflow row
- ownership identity
- heartbeat/expiry semantics
- recovery rules for stale holders
- scheduler/orchestrator integration for renew and release

The current runner does not have that lease lifecycle yet, so this remains a documented limitation rather than a partially-correct lock.

## Recommended Follow-Up

Add a DB lease on `dataflows` with:

- `lease_owner`
- `lease_expires_at`
- compare-and-swap claim/renew queries
- explicit release on clean shutdown
- stale-lease takeover rules on restart
