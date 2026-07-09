# Changelog

## [0.4.0](https://github.com/wippyai/dataflow/compare/v0.3.10...v0.4.0) (2026-07-09)


### Features

* add durable signal revival sweeper ([529a4d8](https://github.com/wippyai/dataflow/commit/529a4d884b75e5f2d3c7c57672948911dab33474))
* **dataflow:** route agent checkpoints through canonical bindings ([2185a94](https://github.com/wippyai/dataflow/commit/2185a94eaccb211016b5d5c057df7943f0e9fd55))
* Define new param `web_host_origin_env` ([#1](https://github.com/wippyai/dataflow/issues/1)) ([0f663d9](https://github.com/wippyai/dataflow/commit/0f663d93030e2f890fe39d7ae26770192d48546f))
* Move code from userspace ([c5d4a45](https://github.com/wippyai/dataflow/commit/c5d4a452cdf23a8b1fe90ed2fe51ead60cf062ba))


### Bug Fixes

* **ci:** repair release-please auth and version bumping ([#22](https://github.com/wippyai/dataflow/issues/22)) ([b0dc2be](https://github.com/wippyai/dataflow/commit/b0dc2bebff5e46c3f237d46985f9802f9d1e350d))
* dataflow_nodes table name ([#6](https://github.com/wippyai/dataflow/issues/6)) ([7a4b4a9](https://github.com/wippyai/dataflow/commit/7a4b4a9f4ee0cfa5904841e23b10d830c03c016f))
* declare modules/imports for require() under per-chunk import scoping ([8156953](https://github.com/wippyai/dataflow/commit/81569536e0c3f242a14b903348c11c4bc71c96d2))
* improve error handling, validation, and diagnostics in flow compiler ([#7](https://github.com/wippyai/dataflow/issues/7)) ([5b6d891](https://github.com/wippyai/dataflow/commit/5b6d891c8859fdc3c409b57ce40d2fdd05f8fe6b))
* preserve agent node context ([cf30632](https://github.com/wippyai/dataflow/commit/cf30632eb067350b1de86ca15bdf9861d210d026))
* recover pending commits on orchestrator restart ([ffdc855](https://github.com/wippyai/dataflow/commit/ffdc855fa3a43ea1f0aa1ef3c4259e14f7eb3462))
* restore backward-compatible API across node, client, and func modules ([8939a59](https://github.com/wippyai/dataflow/commit/8939a599bc568bb5f059651f348ee336eedceed1))
* restore parallel node BC with on_error/filter/unwrap config API ([3e3e876](https://github.com/wippyai/dataflow/commit/3e3e87693b713506cd93b04978b47971332db1f5))
