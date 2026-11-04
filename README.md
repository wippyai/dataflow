<p align="center">
    <a href="https://wippy.ai" target="_blank">
        <picture>
            <source media="(prefers-color-scheme: dark)" srcset="https://github.com/wippyai/.github/blob/main/logo/wippy-text-dark.svg?raw=true">
            <img width="30%" align="center" src="https://github.com/wippyai/.github/blob/main/logo/wippy-text-light.svg?raw=true" alt="Wippy logo">
        </picture>
    </a>
</p>
<h1 align="center">Dataflow</h1>
<div align="center">

[![Latest Release](https://img.shields.io/github/v/release/wippyai/dataflow?style=flat-square)][releases-page]
[![License](https://img.shields.io/github/license/wippyai/dataflow?style=flat-square)](LICENSE)
[![Documentation](https://img.shields.io/badge/Wippy-Documentation-brightgreen.svg?style=flat-square)][wippy-documentation]

</div>

## Agent route pinning

Agent nodes retain a successful fallback route across iterations and recovery.
This requires Agent 0.5.5 or later. A committed agent or model change clears the
previous route; recovery restores it only for the same configured agent and model.

## Typed storage inspection

`userspace.dataflow:inspection` is a read-only library for trusted owners that
inspect durable flows and external signal waits. Declare it as a library import.
`inspection.flow(dataflow_id)` returns the flow's ID, actor ID, opaque execution
context, status and decoded metadata. `inspection.pending_signal(dataflow_id,
signal_id, node_type)` returns the pending node and yield IDs for a node type that
implements the signal-wait protocol. A declaration without a committed yield is
absent; a committed result closes the current episode even while the node is
still active. Only the latest UUID-v7 yield episode for each node is inspected,
matching recovery's ordering. Older results cannot close a newer episode.

These operations retain the caller's database authority. They do not authorize
the caller, reconstruct an identity, reserve a wait, or execute a workflow.
The owning application must verify the returned flow's actor and terminal status
before performing an external action. Use the arm's idempotency key for delivery.
The signal query filters by flow, node type, active node status and yield record
type rather than loading the entire flow's event history.

Errors are native: confirmed absence has kind `NOT_FOUND` with code
`DATAFLOW_FLOW_NOT_FOUND` or `DATAFLOW_SIGNAL_NOT_FOUND`. Invalid stored metadata
or wait state has kind `INTERNAL`, is not retryable, and uses
`DATAFLOW_FLOW_METADATA_INVALID` or `DATAFLOW_SIGNAL_STATE_INVALID`.
Database failures preserve their native kind, retryability and detail fields;
a database-resource `NOT_FOUND` is not a missing-flow proof. Legacy empty
metadata encodings remain empty metadata. Existing repository/client methods
retain their behavior.

## Durable external waits

Nodes that start external work and then wait for a signal use the declarative park contract:

```lua
local result, err = n:park({
    wait_for_signal = true,
    signal_id = stable_signal_id,
    arm = {
        ref = "component.namespace:arm_function",
        args = { correlation_id = stable_signal_id },
    },
})
```

`park` persists the node yield before the orchestrator invokes `arm.ref`. The arm runs
in an isolated function process under the workflow's recovered actor and scope; arguments
are snapshotted as bounded plain data (scalars and tables with string or positive-integer
keys) before the durable commit. Cycles, runtime values, excessive nesting, and oversized
payloads fail with `PARK_ARM_INVALID`; metatables and later caller mutations cannot cross
the boundary. Arguments cannot select authority. Arm functions must be idempotent: after a crash
between external success and acknowledgement, restart recovery replays the persisted
declaration when the node reattaches. Persist stable correlation in the arguments rather
than returning an opaque handle. A failed arm returns the structured `PARK_ARM_FAILED`
error and abandons the tracked wait so a later signal cannot revive it.

Existing `n:yield` behavior is unchanged.

## Agent checkpoints

Checkpoint options resolve as trait defaults, explicit `agent_options.checkpoint`,
then node `config.checkpoint` overrides. Maps merge recursively; lists replace,
including an explicit empty list. `enabled = false` disables checkpointing.
No checkpoint configuration means no implicit checkpointing; Dataflow retains
its existing threshold rules (zero and negative thresholds disable scheduling).

Checkpoints run before the next model turn, after the preceding tool results
have settled. Bindings run before the configured function fallback, and both
receive the effective `options`; a strict binding failure never falls back.
Without a function fallback, a non-strict binding failure is recorded as a
skipped checkpoint; an explicit host `strict = true` still fails the workflow, unless
the node is completing: no request follows, so the failure is recorded as a skip and an
accepted finish or final text stands.
The host still owns marker persistence and the summary length cap. Checkpointing
is conversation compaction, not durable long-term recall or a hard spending limit.

Opt-in behavior `_control` proposals use the shared declarative subset:
agent/model/trait/tool targets, session/public metadata, and `memory.compact`.
Proposals are persisted with their iteration. Outcomes and delegations settle
before policy application; effects and completion metadata are committed via
the existing node transaction machinery. Recovery applies stored proposals
without rerunning resolved tools. Replay is at-least-once, so external provider
writes and lifecycle hooks must be idempotent using stable host refs.
Proposals apply in order against the updated selection. Explicit model, trait
and tool targets survive an agent switch, including empty tool lists. Recovery
reloads the selected agent before another checkpoint, lifecycle hook or model
step; a failed reload fails the node instead of running the previous agent.
CI also kills and restarts a runtime with a pending behavior proposal, then
checks the replacement agent's first step, its settled tool result and compaction.

`memory.compact = true` persists a request that bypasses the token threshold,
not an explicit disable or a missing provider. False is not cancellation.
Requested compaction also runs when the final response ends the node, rather
than depending on another model step. If a proposal also changes agent, model
or overlays, compaction resolves the committed target's identity, provider and
options, consistently with recovery. The provider still owns long-term memory
storage, retention, retrieval and authorization. No behavior attachment means
no new automatic policy/evaluator calls or loop thresholds.


[wippy-documentation]: https://docs.wippy.ai
[releases-page]: https://github.com/wippyai/dataflow/releases
[packcli]: https://github.com/wippyai/wippy-releases/releases
[modules-registry]: https://modules.wippy.ai
# Optional diagnostic retention

The host can enable native cleanup through its `wippy/dataflow` dependency:

```yaml
parameters:
  - name: userspace.dataflow:retention_enabled
    value: true
  - name: userspace.dataflow:diagnostic_retention_days
    value: "30"
  - name: userspace.dataflow:retention_interval_seconds
    value: "300"
  - name: userspace.dataflow:retention_batch_size
    value: "1000"
```

`retention_enabled` defaults to `false`: the optional process service does not
start until the host enables it. The other defaults are 30 days, 3600 seconds,
and 500 rows per table per batch. Setting days to `0` also disables deletion.
Days accept integers 0–36500, intervals 60–86400 seconds, and batches 1–1000.
The service uses the host's existing `target_db` and `process_host` bindings.

Cleanup starts 30 seconds after service startup and then runs on the configured
interval. Each transaction removes at most one batch from each diagnostic table.
It retries on the next interval after a database or configuration error; logs from
`dataflow.retention` report the policy, candidate counts, and deleted counts.

Only entire parent/child families whose flows are all terminal and older than the
retention window qualify. Active, waiting, paused, recent, unknown-state, and
unrooted families are preserved. PostgreSQL serializes eligibility with flow
status changes and child insertion for each bounded transaction, using a 2-second
lock timeout and a 30-second statement timeout; SQLite reserves its writer before
reading eligibility. A busy database fails the batch safely and retries later.

The sweeper prunes applied commit history (except each flow's last commit) and
intermediate `cycle.state`, `cycle.function_result`, `node.input`, `node.yield`,
`node.yield.result`, and `parallel.progress` records. It preserves pending commits,
flow and node summaries, flow inputs/outputs, node results/outputs, observations,
actions, and evidence. Full diagnostic replay of pruned old flows is no longer
available. This is **diagnostic retention**, not deletion of durable application
records or all workflow history. PostgreSQL autovacuum makes deleted space
reusable; the service does not run disruptive `VACUUM FULL` operations.

For an administrative preview, call the library
`userspace.dataflow.retention:sweeper.run(db, {days=30, batch_size=1000, dry_run=true})`
with an authorized database handle. The library is not a public HTTP endpoint.

## Stable request prefix

While nothing below edits it, each request an agent node sends extends the
previous one: everything sent before the model's output is stored where it was
sent and replayed unchanged, so the provider cache and earlier thinking blocks
stay valid. This requires Agent 0.5.10 and LLM 0.5.13 or later.

- **User message.** The fold of the node's conversation inputs is stored once as
  an `agent.input` row with the node's first commit and replayed on every start,
  so an input that arrives later does not change it.

- **Memory recall.** An agent with a memory contract recalls on each eligible
  step and returns the note in `context_notes`. The notes are stored, in order,
  as `agent.memory` rows whose `metadata.llm_meta` carries `preserve_position`
  and `memory_ids`, after the step's lifecycle notes and before its action row,
  including a truncated action. A malformed note fails the node.
- **Turn context.** When the agent declares `agent_options.turn_context` (the
  `time_aware` trait), the node renders the note once before its first request
  and commits it as a positional system row with key `turn_context`. All of a
  node's conversation inputs fold into one user message, so a node has one note,
  right after that message, however many inputs it has and however many iterations
  it runs. A restart replays the stored note. A checkpoint keeps it, because every
  request sends the input it follows, and leaves it out of the summarized history.
  A note that cannot render (an unknown timezone) fails the node before any request.
- **Thinking blocks.** The action row stores `prefix_fingerprint` in
  `metadata.llm_meta` next to `thinking_blocks`, and every step passes
  `drop_stale_thinking = true`. A changed system prompt or tool set (an agent
  switch) strips the earlier blocks before the next request; the current tool
  round keeps its blocks. A checkpoint's cut is the newest row, and no checkpoint runs
  while a round is open, so every action row that survives a cut was produced after
  the marker: the default fingerprint comparison is sufficient and the node passes
  no `thinking_is_current`.
- **Checkpoints.** No checkpoint runs while the newest action has a call with no
  result on a node that keeps running; a requested compaction waits for the round to
  close. A compaction requested in the round that ends through the finish tool commits
  with the node's completion, so a restart cannot run the finish against that cut. The summary replaces every row up to the cut, calls and results
  together, and a request carries the input, the turn note, the marker and the rows
  after the cut. A later checkpoint summarizes what requests carry: the earlier
  summary and the rows after its cut, so the earlier summary rolls forward. Nothing
  else bounds the payload: the rows the requests already carry, plus the default
  summarizer's `max_row_chars` per row, which does not apply to the earlier summary.
  The host sets no limit of its own. `max_memory_chars` (default 8192) caps the stored
  summary in characters, never splitting one. The default summarizer states the
  budget it has on every summary. Unless the options give `max_tokens`, it uses the
  smallest of twice `max_memory_chars`, an eighth of the model's context window and
  16000, and any `max_tokens` is capped by the model's output limit. The stated budget is
  the smaller of `max_memory_chars` and three quarters of that value, because thinking
  and dense text share the cap. On a class route only the first model's limits count. A
  summary with no visible characters is a failed checkpoint too. A longer summary is cut at the cap and recorded in the marker's
  `checkpoint_memory_truncated`. A summary the model cut off at its `max_tokens` is a
  failed checkpoint with no cut: a skipped checkpoint, or a node failure when
  `strict`. A failed checkpoint is tried again when new rows arrive, not for the same
  rows, within one process. Only the newest action can hold a checkpoint back; an
  older unanswered call is summarized with its round. The
  `since_checkpoint` run context selector reads the same cut, in request order, and
  also returns the input and the turn note, which a summary does not replace. In every
  mode `max_chars` is a hard bound that counts call arguments: rows accumulate from the
  oldest, so `since_id` and `range` page forward, and the list is empty if the first row
  exceeds it. The input and the note take what room is left, and `truncated` is set if
  anything is dropped. `get_prompt` with `window`, `since_id`, `range` or `max_chars` is a
  view and may hold a call without its result; do not send it to a provider.
  `get_history` pairs a result with its preceding call, so reusing a call id in a
  later iteration cannot replace the earlier result's arguments, even in a bounded
  slice. A skipped checkpoint leaves an audit observation that stays out of the
  prompt and of the summary.
- **Empty input.** A node with no conversation input pins its `arena.prompt` as the
  first user message, on every driver, because Claude and Bedrock reject an empty text
  block. With no `arena.prompt` it sends no user message and a note is the only content;
  with no note either it fails naming that.
- **Turns that do not finish.** A text turn below `min_iterations`, or an empty one
  under `tool_calling = "none"`, is answered with a feedback row, so the next request
  does not end on the assistant. Recovery repeats that completion decision after
  an action commit: a length-cut text answer accepted live still completes after a
  restart. Recovery treats a length-cut turn as truncated only when its
  `_truncation_warning` is stored.
- **Lifecycle notes.** Messages from `activate` and `before_step` are sent as
  positional notes after the history and stored, with the step's output, where they
  were sent: `agent.memory` rows with `metadata.role` set to `developer` or `system`,
  `metadata.lifecycle_phase`, and `preserve_position` in `metadata.llm_meta`. They
  are appended, never rebuilt, so `before_step` adds one set of notes per step and
  an activation note stays in the history after the step that activated the agent.
  `activate` fires again when a restarted node resumes, so its note is written again
  at the new position; the earlier note and the prefix are untouched, and a handler
  that wants one note per run keeps that state itself, as for any at-least-once hook.
  A system note is positional and no longer joins the top-level system prompt. A
  lifecycle message in any other role fails the node with an error naming the phase,
  the agent and the role: a host note must not speak as either party.

These edit the prefix by design. The prompt cache restarts at the edit, and the
requests after it extend each other again:

- A checkpoint replaces the rows up to its cut with the summary.
- An agent switch or a trait, tool or overlay change replaces the top-level system
  text or the tool list; the fingerprint strips the earlier thinking blocks.
- `_control.memory.clear` and `_control.memory.delete` remove rows the memory
  directive added. A delete of a row that is gone is a no-op, so a crash replay
  carries on. A delete of any other existing row fails the node, so neither can take
  away a call, a result or a note.
- A step that a crash interrupts before its commit is lost with its notes; the
  restarted node sends that step again from the last committed row.

A round whose results all committed before a kill, but whose closing commit did not,
is settled on recovery: `round_settled` is missing, so the iteration warning is written
if it is not stored and `after_execute` wrappers run for the whole round. A run stored
before `round_settled` gets this once on resume. After a kill with calls still unresolved,
`after_execute` sees only the calls recovery runs: a call whose result committed before
the kill is not shown to it again, so wrappers are not at-least-once per call. A round
settled on recovery shows the wrapper its calls in the live shape, and results from the
stored rows, which cannot recover everything: `true` reads back as 1 on SQLite, a scalar such as `false` reads back as its text, and a
`{success = false}` result is stored as its error text rather than its table.

A `_control` directive that creates children through `commands` or `yield.commands`
commits their ids in `agent_pending` before any wait. Live execution and recovery
collect their output into the next model request, including rounds with delegations.
Command output follows every call's result; its observation and the clearing of
`agent_pending` commit together, so a kill during either wait loses no output and
creates no duplicate observation. The `round_command_delegate` and
`round_yield_delegate` restart proofs kill the runtime during these waits.

`_control.artifacts` take their ids from the call and its position, so a call that
recovery runs again stores each artifact once and the first stored content stands.
An id-generation failure fails the node with an error naming the artifact.
A call can return one `_control.delegate`: each delegate's result answers the call,
so a call returning several fails the node with an error naming it, and `input_data`
is required. An empty delegate list stores the tool's ordinary result.

A selection is durable: `agent_id` and `model` inputs choose only until the node's
first commit, which stores the choice in the config, a `_control.config` switch commits with the results that carry it,
and a restart keeps the stored selection. `_control.memory.add` rows follow every
result of their round, in the action's call order. They commit with the round's
results before the node waits on children; with a delegation they wait in the node
metadata by call id and are written with the delegate's result, so a crash in between
loses none, and a call that recovery runs again replaces its own.

After upgrading, a run stored by an earlier version restarts its cache once when it
resumes: its earlier steps sent lifecycle notes that were never stored, and its
action rows carry no `prefix_fingerprint`, so their thinking blocks are stripped
from the next request (except those of a tool round in progress). A resumed run
past its first iteration gets no turn context note, because its input was already
sent, and its fold is stored at that resume. It keeps the model its state recorded,
over both the template and the `model` input, unless the config holds a newer choice:
a different agent than the state, or a model with no `model` input. Results kept past an earlier checkpoint
leave the prompt on that resume too.

A run stored by the previous release with text between a call and its result fails
on resume with a provider error, and there is no repair at render time. That shape
comes from an iteration warning written before a delegate's result, or a
`_control.memory.add` row written before its call's result. Claude already rejected
it; Bedrock did not, because developer messages then went to `system` blocks. Now they
render as user text there too.

## Live execution authority

Hosts with mutable permissions bind their native identity resolver through `execution_authority_imports` and set `refresh_perms: true` (default false). Dataflow resolves current authority at activation and resume boundaries and preserves explicit narrowing ceilings. The [host binding contract](src/README.md#live-execution-authority) describes capture, resolution and existing frame behavior.

## Iteration input routes

Parallel iterator and Cycle template configurations accept optional
`iteration_input_routes` in raw node commands:

```lua
iteration_input_routes = {
    { node_id = root_template_id, discriminator = "default" },
    { node_id = join_template_id, discriminator = "item", transform = "output" },
}
```

Each `node_id` names a direct template child of the iterator. Routes seed the
Parallel item or the Cycle context (`input`, `state`, `last_result`, `iteration`)
once per iteration, including nodes with incoming body edges. Discriminators
participate in the target's normal `inputs.required` gate. Transforms use the
same expression environment and error handling as data targets, with `output`
set to the item or context.

When routes are present, they replace implicit root item seeding. When absent,
the existing root seeding remains unchanged. Parallel passthrough inputs still
reach roots. Graph construction rejects targets outside the body, duplicate
targets, body dependency cycles, and body edges returning to the iterator.
Routes do not add executable nodes or outgoing body edges. Cycle retains its
existing iteration limit and fails if continuation exceeds it.
