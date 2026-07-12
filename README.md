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
