# Structured child failure propagation

`n:fail()` failure envelopes are preserved unchanged when present, retaining their nested `{ code, message }` error; bare process failures are propagated as their original string or structured value.

The process-event regression test now covers both the original plain-string failure and an `n:fail()`-shaped structured failure, asserting the node-result handoff, persisted result, and run-level message.

Verification: `make test` — 902 tests passed, 0 failed.

The requested external scratchpad path was not writable from this workspace, so this report is stored here instead.

## Per-item structured failure propagation

Template-item failures now retain their original `n:fail()` `{ code, message }`
envelope through live collection, durable recovery, and parent template handling.
The message is preserved verbatim; only scalar infrastructure errors receive a
human-readable prefix.

Regression coverage added before the implementation:

- `src/node/parallel/iterator_test.lua` — child `result.error` collection.
- `src/node/parallel/parallel_test.lua` — live per-item aggregation and durable
  `ITERATION_ERROR` rebuild.
- `src/node/cycle/cycle_test.lua` — parent cycle result is compared directly to
  the failed template child's structured error.

Table-capable error sites found and fixed (13):

| Site | Disposition |
| --- | --- |
| `src/child_output.lua:74,120` | Returns a failed child's original `error` envelope instead of recreating it from a string. |
| `src/node/parallel/iterator.lua:418` | Returns one structured template failure unchanged; multi-failure results keep structured `errors`. |
| `src/node/parallel/parallel.lua:763,1013` | Durable iteration rows retain table errors while rebuilding per-item results. |
| `src/node/parallel/parallel.lua:1101,1118` | Live and recovered collection store the raw failure in each item result. |
| `src/node/parallel/parallel.lua:1064,1302,1345` | Item-pipeline, creation, and yield failures preserve tables rather than formatting them. |
| `src/node/cycle/cycle.lua:492,858` | Template-yield failures pass their original envelope to the cycle parent `n:fail()`. |
| `src/node/func/node.lua:171,183` | Function-child collection retains a structured failure when completing through children. |
| `src/node/agent/node.lua:1171,2221` | Agent control-child collection and finalization no longer stringify structured child errors. |
| `src/node/parallel/parallel.lua:185,1502` | Parallel input exceptions retain their structured reason. |
| `src/node/cycle/cycle.lua:659,719` | Cycle input exceptions retain their structured reason. |
| `src/node/func/node.lua:127,254` | Function-node input exceptions retain their structured reason. |
| `src/node/state/state.lua:34,65` | State-node input exceptions retain their structured reason. |
| `src/node/signal/node.lua:32` | Signal-yield structured errors are passed directly to `n:fail()`. |

Remaining `tostring()` audit: `src/node.lua` expression/data-reader errors,
`src/client.lua` contract/repository errors, API HTTP rendering, runner logging,
database helpers, and agent checkpoint/tool adapters are string-only at their
declared boundaries or are non-propagating diagnostics/identifiers. No remaining
table-capable error conversion feeds a node, child, template, per-item, or
workflow failure result.
