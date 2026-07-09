# Structured child failure propagation

`n:fail()` failure envelopes are preserved unchanged when present, retaining their nested `{ code, message }` error; bare process failures are propagated as their original string or structured value.

The process-event regression test now covers both the original plain-string failure and an `n:fail()`-shaped structured failure, asserting the node-result handoff, persisted result, and run-level message.

Verification: `make test` — 902 tests passed, 0 failed.

The requested external scratchpad path was not writable from this workspace, so this report is stored here instead.
