# Child Output Collection Design Review

## Contract

Embedded-child output collection now follows the durable yield result path:

1. A parent submits child nodes and calls `n:yield({ run_nodes = child_node_ids })`.
2. The orchestrator tracks each yielded child and, when it exits, records the exact `node.result` data row ID in `yield_info.results[child_node_id]`.
3. Before satisfying the parent yield on child exit, the orchestrator flushes pending commits so the result rows and routed output rows are visible to the parent.
4. Parent node implementations pass the returned yield result map into `child_output.collect_outputs(...)`.
5. `child_output` resolves those exact `node.result` data IDs, reads their `data_ids` envelope, and then fetches the routed `node.output` rows by data ID.

The invariants are:

- Live yield collection must not guess output by child `node_id`.
- A structured successful result with an empty or missing `data_ids` array means "no child output"; it is not a license to scan child outputs.
- A missing returned result row is an error.
- Legacy non-envelope result rows may fall back to output lookup, but only by the `node_id` on the exact returned `node.result` row.
- Routed output/error rows preserve any target-provided `data_id`, so the `data_ids` envelope and the data table agree.
- Recovery paths may collect already durable child outputs before yielding again, but must not poll-and-complete-empty while children are pending.

## Changes By File

- `src/child_output.lua`: Added strict yield-result collection, exact result-row resolution, routed output lookup by `data_id`, structured child failure propagation, and recovery resume plumbing that reuses returned yield result IDs.
- `src/_index.yaml`: Declares the `json` module for `child_output`; without this, runtime binding fails before node execution.
- `src/node.lua`: Preserves target-provided output and error `data_id` values during routing.
- `src/node/func/node.lua`: Uses `child_output.collect_outputs(...)` after live yields and via recovery resume; the plain no-children path remains a direct completion path with no yield or child-output query.
- `src/node/func/func_child_output_test.lua`: Pins yield-result-based collection, no guessing when a result envelope has no output IDs, scoped legacy fallback, recovery collection before re-yield, and unchanged no-children behavior.
- `src/node/cycle/cycle.lua`: Uses strict collection for template and control-command children; wraps structured child collection failures with cycle/template context.
- `src/node/agent/node.lua` and `src/node/agent/_index.yaml`: Agent control-command children use shared strict output collection and bind the helper.
- `src/node/agent/delegation_handler.lua`: Treats invalid delegated JSON as an error instead of returning raw text as a successful delegated result.
- `src/runner/orchestrator.lua`: Flushes startup and pending commits before satisfying a yield after child exit.
- `src/runner/workflow_state.lua` and `src/persist/data_reader.lua`: Replace bad `pcall(json.decode)` patterns with the local JSON API's explicit error return.
- `src/node_test.lua`: Pins preservation of target-provided output and error data IDs.

## Risks

- Legacy fallback is intentionally narrow; older non-envelope child result rows still work only when the exact returned `node.result` row provides a `node_id`.
- Strict result-row lookup turns missing or malformed result envelopes into visible failures. This is expected and prevents silent `{}` completion.
- Callers that create embedded children but bypass `n:yield` still use the older `node_id` query path only when no yield results exist.

## Design Gaps

No strict-contract design gaps were found during this pass. The failures uncovered in verification were implementation issues: the missing `json` module binding for `child_output`, and cycle's attempt to treat a structured child failure as a string instead of wrapping it with cycle/template context.

## Verification

- Initial full suite from the owner WIP: `727 passed, 121 failed`.
- After binding `json`: `899 passed, 1 failed`.
- Final full suite after cycle error wrapping: `900 tests` passed in `208.0s`.
