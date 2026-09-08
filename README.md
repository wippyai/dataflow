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
