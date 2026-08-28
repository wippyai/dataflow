# Changelog

## Unreleased

## [0.8.0](https://github.com/wippyai/dataflow/compare/v0.7.14...v0.8.0) (2026-08-28)


### Features

* add durable signal revival sweeper ([529a4d8](https://github.com/wippyai/dataflow/commit/529a4d884b75e5f2d3c7c57672948911dab33474))
* **agent:** max_iterations joins the reserved agent-node inputs ([35cf94d](https://github.com/wippyai/dataflow/commit/35cf94d150aaf4496698001c8410ddab327ca868))
* **agent:** max_iterations joins the reserved agent-node inputs ([aa27014](https://github.com/wippyai/dataflow/commit/aa2701481828e850fc1903da1d30eb562832da11))
* **dataflow:** add durable execution frames ([7fc7639](https://github.com/wippyai/dataflow/commit/7fc7639a88c6919da0ac4059e38900137a496084))
* **dataflow:** add pure overseer lifecycle state ([0c68d50](https://github.com/wippyai/dataflow/commit/0c68d507e88d6ca222c5e0ff3d3d86e86b73eac6))
* **dataflow:** make waits and recovery event driven ([6d6908c](https://github.com/wippyai/dataflow/commit/6d6908c39bdc8d5688e7b43fcf610840993428d8))
* **dataflow:** make waits and recovery event driven ([127b8c0](https://github.com/wippyai/dataflow/commit/127b8c013d24198a9006bb5e07667b95ef7676dd))
* **dataflow:** make workflow ownership restart-safe ([630c4ae](https://github.com/wippyai/dataflow/commit/630c4ae22f68750d2ce32393a27547f5ce01be56))
* **dataflow:** make workflow ownership restart-safe ([102f1e9](https://github.com/wippyai/dataflow/commit/102f1e983dfbec9c068561d2c38cfecc90a11590))
* **dataflow:** route agent checkpoints through canonical bindings ([2185a94](https://github.com/wippyai/dataflow/commit/2185a94eaccb211016b5d5c057df7943f0e9fd55))
* **dataflow:** route client activation through overseer ([51e28e8](https://github.com/wippyai/dataflow/commit/51e28e8e29776b92ff052644609a33be445359ac))
* **dataflow:** supervise durable activations ([c9186d9](https://github.com/wippyai/dataflow/commit/c9186d91f443c650c243029e67a6f2b0de7cbc77))
* Define new param `web_host_origin_env` ([#1](https://github.com/wippyai/dataflow/issues/1)) ([0f663d9](https://github.com/wippyai/dataflow/commit/0f663d93030e2f890fe39d7ae26770192d48546f))
* **migrations:** assert UTF8 database encoding at install ([cc2a51f](https://github.com/wippyai/dataflow/commit/cc2a51fb63f03ba2fd38010ac3c92293deabb6a6))
* **migrations:** the database encoding is a day-one contract ([91dca33](https://github.com/wippyai/dataflow/commit/91dca336fa7ab4f328ab9b9aacfafe24e1a87ff9))
* Move code from userspace ([c5d4a45](https://github.com/wippyai/dataflow/commit/c5d4a452cdf23a8b1fe90ed2fe51ead60cf062ba))
* **parallel:** add durable rolling scheduling ([#50](https://github.com/wippyai/dataflow/issues/50)) ([0eb810a](https://github.com/wippyai/dataflow/commit/0eb810a5bbf24ceb3a9391ee3c8353c0d6262d01))
* **persist:** add generation-fenced activations ([3d1d965](https://github.com/wippyai/dataflow/commit/3d1d9655ae7f9ea3de5cb18ee14efbbdbbad2e6a))
* **persist:** fence workflow lifecycle projections ([04a7417](https://github.com/wippyai/dataflow/commit/04a7417dad5126847ef5fb72ad55943f12677599))


### Bug Fixes

* **agent:** answer sibling tool calls when the exit validator rejects finish ([66ede44](https://github.com/wippyai/dataflow/commit/66ede449207d2dc237815ffa5e043e222d834eed))
* **agent:** keep aggregate failure evidence derived from unhandled outcomes ([a3aacd4](https://github.com/wippyai/dataflow/commit/a3aacd42cc9d7122c6d3cc7e5a6e29a5fe9acc2f))
* **agent:** resume control child DAGs after recovery ([35b96d0](https://github.com/wippyai/dataflow/commit/35b96d0e918234baf7ad95631e00b49dedb8500e))
* **agent:** resume control child DAGs after recovery ([578e1ce](https://github.com/wippyai/dataflow/commit/578e1cec7bd480ae8ea6b100758b99b607d770ae))
* **agent:** treat a nil-content named input as absent ([ea9ed60](https://github.com/wippyai/dataflow/commit/ea9ed60f030af863f660d9b3c2b3ba41afeeb536))
* **agent:** validate structured finish arguments ([eaefd93](https://github.com/wippyai/dataflow/commit/eaefd932c6bb68d24af423b458e9edc44043d15f))
* answer sibling tool calls when exit validator rejects finish ([5723b80](https://github.com/wippyai/dataflow/commit/5723b80a9b56123ef0de7db820802dc6ec4a57d7))
* **api:** declare exact dataflow dependencies ([4121cbf](https://github.com/wippyai/dataflow/commit/4121cbf0185328a101d56116a9375db9bc6b5ef1))
* **api:** narrow dataflow dependency ownership ([e46318f](https://github.com/wippyai/dataflow/commit/e46318f84a02499e8f409bd152d43d2be70fe6a3))
* bind dataflow sweeper security scope ([22e8bab](https://github.com/wippyai/dataflow/commit/22e8babec59a58ed0a682bf5c44484805e55d5ed))
* build wake_repo queries so they run on postgres, and run the wake tests ([3de173c](https://github.com/wippyai/dataflow/commit/3de173c47e85f28a58a5bb11180c05f56164cf40))
* build wake_repo queries so they run on postgres, and run the wake tests ([e07cce2](https://github.com/wippyai/dataflow/commit/e07cce2e16bb2c6f00c5510b8ab2d5227fe0fe6d))
* centralize durable signal wake recovery ([3425ee9](https://github.com/wippyai/dataflow/commit/3425ee9d1ba10cb059df7bffc0d43f1792001458))
* **ci:** repair release-please auth and version bumping ([#22](https://github.com/wippyai/dataflow/issues/22)) ([b0dc2be](https://github.com/wippyai/dataflow/commit/b0dc2bebff5e46c3f237d46985f9802f9d1e350d))
* claim orchestrator before state load ([27ac93d](https://github.com/wippyai/dataflow/commit/27ac93d97dd85268117023b15a118ebe6763947e))
* **client:** authorize durable activation ([700f8b8](https://github.com/wippyai/dataflow/commit/700f8b8bf8708cbcf3df93007e0b5886a03e42d7))
* dataflow_nodes table name ([#6](https://github.com/wippyai/dataflow/issues/6)) ([7a4b4a9](https://github.com/wippyai/dataflow/commit/7a4b4a9f4ee0cfa5904841e23b10d830c03c016f))
* **dataflow:** bound overseer service authority ([d6e70b8](https://github.com/wippyai/dataflow/commit/d6e70b82aa9a669ef41b5ce2dc0216f7375539b3))
* **dataflow:** converge stale wake state ([586241f](https://github.com/wippyai/dataflow/commit/586241f40d641785b4fe754ff6f5bd7f3b32d850))
* **dataflow:** fence orchestrator lifecycle ([9f82b0a](https://github.com/wippyai/dataflow/commit/9f82b0a717d88e5fac0716efd07d95420a24bf80))
* **dataflow:** harden durable wake delivery and packaging ([093cdf9](https://github.com/wippyai/dataflow/commit/093cdf976f44a1960d97e1d1235180e33f0d496c))
* **dataflow:** harden release startup and metadata ([57f8c5e](https://github.com/wippyai/dataflow/commit/57f8c5e535b0519c3c27ff0166027fd9708fc41e))
* **dataflow:** make durable wake delivery exact ([4c1ec29](https://github.com/wippyai/dataflow/commit/4c1ec29b05a7ce0d45755a60a3b9c6c020772efe))
* **dataflow:** make durable wake recovery event-driven ([4987eab](https://github.com/wippyai/dataflow/commit/4987eab1df48116fc4a976dd732fe1ab2e7d0ede))
* **dataflow:** make durable wake recovery event-driven ([1b52970](https://github.com/wippyai/dataflow/commit/1b52970b6a1bf4cfb3f1ac7ee56d2977094ce780))
* **dataflow:** satisfy strict overseer boundaries ([2e45cd7](https://github.com/wippyai/dataflow/commit/2e45cd70ac2534525a5a41991918aff1b6ee186c))
* **dataflow:** satisfy strict overseer boundaries ([d4aab76](https://github.com/wippyai/dataflow/commit/d4aab7654b89c80a53a834ad431710632747c59b))
* declare modules/imports for require() under per-chunk import scoping ([8156953](https://github.com/wippyai/dataflow/commit/81569536e0c3f242a14b903348c11c4bc71c96d2))
* fence rolling yield handoffs ([85d6cf7](https://github.com/wippyai/dataflow/commit/85d6cf75af44188258591918db090c4ae5c3f412))
* fence rolling yield handoffs ([1fa7c94](https://github.com/wippyai/dataflow/commit/1fa7c945c6a80b19da4bf13b796cd40255f84483))
* **flow:** preserve agent capability overlays ([c53f6b2](https://github.com/wippyai/dataflow/commit/c53f6b2274abc45b25fdb3bb5398b60a72791145))
* **flow:** preserve agent capability overlays ([7335beb](https://github.com/wippyai/dataflow/commit/7335beb82e43161e17493a1941ba7c85c7bf71c7))
* improve error handling, validation, and diagnostics in flow compiler ([#7](https://github.com/wippyai/dataflow/issues/7)) ([5b6d891](https://github.com/wippyai/dataflow/commit/5b6d891c8859fdc3c409b57ce40d2fdd05f8fe6b))
* make dataflow PostgreSQL-safe ([c2435d3](https://github.com/wippyai/dataflow/commit/c2435d3fe31bb36b65b4e4af1212c9185d90ca7a))
* make dataflow PostgreSQL-safe ([2cb54fe](https://github.com/wippyai/dataflow/commit/2cb54fee2788146866b608a7b1c0e70d10aa0513))
* nil-resolved transform fields deliver no input; agent reserved carriers read absence ([05fe4ec](https://github.com/wippyai/dataflow/commit/05fe4ec4bb1d64fb851ee0a02147dc82a74901b3))
* **node:** a transform field resolving to nil delivers no input ([acc696d](https://github.com/wippyai/dataflow/commit/acc696dcdeb11072ca9542610d45b13db5d220d2))
* **persist:** complete the storage boundary; respect binary content ([9f85f18](https://github.com/wippyai/dataflow/commit/9f85f187f80d6e66d8f77282901167bfbfb09ec4))
* **persist:** deduplicate concurrent yield creation ([#55](https://github.com/wippyai/dataflow/issues/55)) ([9550f89](https://github.com/wippyai/dataflow/commit/9550f89d29e9a5096c757fb3f142076511862dde))
* **persist:** make mutable slot creation race-safe ([#53](https://github.com/wippyai/dataflow/issues/53)) ([7df3afe](https://github.com/wippyai/dataflow/commit/7df3afe41ee6aa67107963f96dc724cf34a056cc))
* **persist:** order terminal lifecycle locks ([5b62efa](https://github.com/wippyai/dataflow/commit/5b62efa1eeeab68a1dbb2219ec41968abeda196c))
* **persist:** the storage boundary covers every write and respects binary ([d8ac99f](https://github.com/wippyai/dataflow/commit/d8ac99fd7b74281eb2ecc32353ce5a13bcb02bde))
* **persist:** the storage boundary guarantees valid UTF-8 ([3a371c0](https://github.com/wippyai/dataflow/commit/3a371c09506a5f09a75e97d50bba6c1e7dd09c9e))
* **persist:** the storage boundary guarantees valid UTF-8 ([1b6699b](https://github.com/wippyai/dataflow/commit/1b6699be138c6da69c848c8719de0a087cf411eb))
* preserve agent node context ([cf30632](https://github.com/wippyai/dataflow/commit/cf30632eb067350b1de86ca15bdf9861d210d026))
* preserve exact dataflow execution identity ([f7da5a9](https://github.com/wippyai/dataflow/commit/f7da5a9aaea6cfb9bd47503d9854bf440a9e4b5d))
* preserve exact dataflow execution identity ([d5e83ff](https://github.com/wippyai/dataflow/commit/d5e83ff3a980653853cdfd57b0696b179513233b))
* preserve exact wake delivery acknowledgement ([f5c5d07](https://github.com/wippyai/dataflow/commit/f5c5d07a242da2bfea47ebf919affad2f520153b))
* prevent terminal yield recovery loops ([38d8c6d](https://github.com/wippyai/dataflow/commit/38d8c6d3641c03f01ebd55f6162f15a065050cf8))
* reconcile idempotent recovery results ([43b0eaa](https://github.com/wippyai/dataflow/commit/43b0eaad7d227d88a455acf286a6ae7da92d8278))
* recover explicit replay conflicts portably ([6163214](https://github.com/wippyai/dataflow/commit/6163214ed7711b380da4174ad376c32d367df0a2))
* recover explicit replay conflicts portably ([2d34bd0](https://github.com/wippyai/dataflow/commit/2d34bd0eb75c318795a6580ca2300eed1c03a881))
* recover pending commits on orchestrator restart ([ffdc855](https://github.com/wippyai/dataflow/commit/ffdc855fa3a43ea1f0aa1ef3c4259e14f7eb3462))
* relax dataflow revival sweep interval ([bda632e](https://github.com/wippyai/dataflow/commit/bda632e7486c408c0d507dc8fcc814070dc2855e))
* replay consumed signals to replacement yields ([bbf15e0](https://github.com/wippyai/dataflow/commit/bbf15e05306c5292a1a3b9cdb2a12d40847ff239))
* restore backward-compatible API across node, client, and func modules ([8939a59](https://github.com/wippyai/dataflow/commit/8939a599bc568bb5f059651f348ee336eedceed1))
* restore parallel node BC with on_error/filter/unwrap config API ([3e3e876](https://github.com/wippyai/dataflow/commit/3e3e87693b713506cd93b04978b47971332db1f5))
* retry durable wakes until consumption ([0652173](https://github.com/wippyai/dataflow/commit/06521737739698c81666f904300fdb383cefc10d))
* **runner:** a terminal process result always persists as valid content ([bc50465](https://github.com/wippyai/dataflow/commit/bc504655045d1180f23f844a13da9d4b2288f5a8))
* **runner:** a terminal process result always persists as valid content ([1b2176d](https://github.com/wippyai/dataflow/commit/1b2176dd32b3093774779ef185ae3129a33ae67e))
* **runner:** completion persists as its own batch; consumed child errors are not failure evidence ([64d2026](https://github.com/wippyai/dataflow/commit/64d20266059fb3b51606bad483bfb61e92353f03))
* **runner:** persist completion as a generation-fenced batch head ([7a9b985](https://github.com/wippyai/dataflow/commit/7a9b9853d2da6d737496239b2969897349701283))
* stabilize dataflow revival sweeper ([ce39cdc](https://github.com/wippyai/dataflow/commit/ce39cdcade94945d3d44f582430e4a1634194471))

## [0.7.6](https://github.com/wippyai/dataflow/compare/v0.7.5...v0.7.6) (2026-08-10)


### Features

* **agent:** max_iterations joins the reserved agent-node inputs ([35cf94d](https://github.com/wippyai/dataflow/commit/35cf94d150aaf4496698001c8410ddab327ca868))
* **agent:** max_iterations joins the reserved agent-node inputs ([aa27014](https://github.com/wippyai/dataflow/commit/aa2701481828e850fc1903da1d30eb562832da11))
* **migrations:** assert UTF8 database encoding at install ([cc2a51f](https://github.com/wippyai/dataflow/commit/cc2a51fb63f03ba2fd38010ac3c92293deabb6a6))
* **migrations:** the database encoding is a day-one contract ([91dca33](https://github.com/wippyai/dataflow/commit/91dca336fa7ab4f328ab9b9aacfafe24e1a87ff9))


### Bug Fixes

* **agent:** answer sibling tool calls when the exit validator rejects finish ([66ede44](https://github.com/wippyai/dataflow/commit/66ede449207d2dc237815ffa5e043e222d834eed))
* **agent:** keep aggregate failure evidence derived from unhandled outcomes ([a3aacd4](https://github.com/wippyai/dataflow/commit/a3aacd42cc9d7122c6d3cc7e5a6e29a5fe9acc2f))
* **agent:** treat a nil-content named input as absent ([ea9ed60](https://github.com/wippyai/dataflow/commit/ea9ed60f030af863f660d9b3c2b3ba41afeeb536))
* answer sibling tool calls when exit validator rejects finish ([5723b80](https://github.com/wippyai/dataflow/commit/5723b80a9b56123ef0de7db820802dc6ec4a57d7))
* nil-resolved transform fields deliver no input; agent reserved carriers read absence ([05fe4ec](https://github.com/wippyai/dataflow/commit/05fe4ec4bb1d64fb851ee0a02147dc82a74901b3))
* **node:** a transform field resolving to nil delivers no input ([acc696d](https://github.com/wippyai/dataflow/commit/acc696dcdeb11072ca9542610d45b13db5d220d2))
* **persist:** complete the storage boundary; respect binary content ([9f85f18](https://github.com/wippyai/dataflow/commit/9f85f187f80d6e66d8f77282901167bfbfb09ec4))
* **persist:** the storage boundary covers every write and respects binary ([d8ac99f](https://github.com/wippyai/dataflow/commit/d8ac99fd7b74281eb2ecc32353ce5a13bcb02bde))
* **persist:** the storage boundary guarantees valid UTF-8 ([3a371c0](https://github.com/wippyai/dataflow/commit/3a371c09506a5f09a75e97d50bba6c1e7dd09c9e))
* **persist:** the storage boundary guarantees valid UTF-8 ([1b6699b](https://github.com/wippyai/dataflow/commit/1b6699be138c6da69c848c8719de0a087cf411eb))
* **runner:** a terminal process result always persists as valid content ([bc50465](https://github.com/wippyai/dataflow/commit/bc504655045d1180f23f844a13da9d4b2288f5a8))
* **runner:** a terminal process result always persists as valid content ([1b2176d](https://github.com/wippyai/dataflow/commit/1b2176dd32b3093774779ef185ae3129a33ae67e))
* **runner:** completion persists as its own batch; consumed child errors are not failure evidence ([64d2026](https://github.com/wippyai/dataflow/commit/64d20266059fb3b51606bad483bfb61e92353f03))
* **runner:** persist completion as a generation-fenced batch head ([7a9b985](https://github.com/wippyai/dataflow/commit/7a9b9853d2da6d737496239b2969897349701283))

## [0.7.5](https://github.com/wippyai/dataflow/compare/v0.7.4...v0.7.5) (2026-08-06)


### Bug Fixes

* **dataflow:** satisfy strict overseer boundaries ([2e45cd7](https://github.com/wippyai/dataflow/commit/2e45cd70ac2534525a5a41991918aff1b6ee186c))
* **dataflow:** satisfy strict overseer boundaries ([d4aab76](https://github.com/wippyai/dataflow/commit/d4aab7654b89c80a53a834ad431710632747c59b))

## [0.7.4](https://github.com/wippyai/dataflow/compare/v0.7.3...v0.7.4) (2026-08-01)


### Bug Fixes

* fence rolling yield handoffs ([85d6cf7](https://github.com/wippyai/dataflow/commit/85d6cf75af44188258591918db090c4ae5c3f412))
* fence rolling yield handoffs ([1fa7c94](https://github.com/wippyai/dataflow/commit/1fa7c945c6a80b19da4bf13b796cd40255f84483))

## [0.7.3](https://github.com/wippyai/dataflow/compare/v0.7.2...v0.7.3) (2026-08-01)


### Bug Fixes

* recover explicit replay conflicts portably ([6163214](https://github.com/wippyai/dataflow/commit/6163214ed7711b380da4174ad376c32d367df0a2))
* recover explicit replay conflicts portably ([2d34bd0](https://github.com/wippyai/dataflow/commit/2d34bd0eb75c318795a6580ca2300eed1c03a881))

## [0.7.2](https://github.com/wippyai/dataflow/compare/v0.7.1...v0.7.2) (2026-08-01)


### Bug Fixes

* **persist:** deduplicate concurrent yield creation ([#55](https://github.com/wippyai/dataflow/issues/55)) ([9550f89](https://github.com/wippyai/dataflow/commit/9550f89d29e9a5096c757fb3f142076511862dde))

## [0.7.1](https://github.com/wippyai/dataflow/compare/v0.7.0...v0.7.1) (2026-08-01)


### Bug Fixes

* **persist:** make mutable slot creation race-safe ([#53](https://github.com/wippyai/dataflow/issues/53)) ([7df3afe](https://github.com/wippyai/dataflow/commit/7df3afe41ee6aa67107963f96dc724cf34a056cc))

## [0.7.0](https://github.com/wippyai/dataflow/compare/v0.6.1...v0.7.0) (2026-07-31)


### Features

* **parallel:** add durable rolling scheduling ([#50](https://github.com/wippyai/dataflow/issues/50)) ([0eb810a](https://github.com/wippyai/dataflow/commit/0eb810a5bbf24ceb3a9391ee3c8353c0d6262d01))

## [0.6.1](https://github.com/wippyai/dataflow/compare/v0.6.0...v0.6.1) (2026-07-31)


### Bug Fixes

* **flow:** preserve agent capability overlays ([c53f6b2](https://github.com/wippyai/dataflow/commit/c53f6b2274abc45b25fdb3bb5398b60a72791145))

## [0.6.0](https://github.com/wippyai/dataflow/compare/v0.5.5...v0.6.0) (2026-07-25)


### Features

* **dataflow:** add durable execution frames ([7fc7639](https://github.com/wippyai/dataflow/commit/7fc7639a88c6919da0ac4059e38900137a496084))
* **dataflow:** add pure overseer lifecycle state ([0c68d50](https://github.com/wippyai/dataflow/commit/0c68d507e88d6ca222c5e0ff3d3d86e86b73eac6))
* **dataflow:** make workflow ownership restart-safe ([630c4ae](https://github.com/wippyai/dataflow/commit/630c4ae22f68750d2ce32393a27547f5ce01be56))
* **dataflow:** make workflow ownership restart-safe ([102f1e9](https://github.com/wippyai/dataflow/commit/102f1e983dfbec9c068561d2c38cfecc90a11590))
* **dataflow:** route client activation through overseer ([51e28e8](https://github.com/wippyai/dataflow/commit/51e28e8e29776b92ff052644609a33be445359ac))
* **dataflow:** supervise durable activations ([c9186d9](https://github.com/wippyai/dataflow/commit/c9186d91f443c650c243029e67a6f2b0de7cbc77))
* **persist:** add generation-fenced activations ([3d1d965](https://github.com/wippyai/dataflow/commit/3d1d9655ae7f9ea3de5cb18ee14efbbdbbad2e6a))
* **persist:** fence workflow lifecycle projections ([04a7417](https://github.com/wippyai/dataflow/commit/04a7417dad5126847ef5fb72ad55943f12677599))


### Bug Fixes

* **client:** authorize durable activation ([700f8b8](https://github.com/wippyai/dataflow/commit/700f8b8bf8708cbcf3df93007e0b5886a03e42d7))
* **dataflow:** bound overseer service authority ([d6e70b8](https://github.com/wippyai/dataflow/commit/d6e70b82aa9a669ef41b5ce2dc0216f7375539b3))
* **dataflow:** converge stale wake state ([586241f](https://github.com/wippyai/dataflow/commit/586241f40d641785b4fe754ff6f5bd7f3b32d850))
* **dataflow:** fence orchestrator lifecycle ([9f82b0a](https://github.com/wippyai/dataflow/commit/9f82b0a717d88e5fac0716efd07d95420a24bf80))


### Behavior Changes

* Active workflow ownership is recovered only across a full application restart. Losing the canonical orchestrator inside a running application now durably fails the exact activation generation instead of resurrecting it.
* Dataflow owns the lifecycle service actor and security group. Applications no longer inject a sweeper security scope.

## [0.5.5](https://github.com/wippyai/dataflow/compare/v0.5.4...v0.5.5) (2026-07-19)


### Bug Fixes

* claim orchestrator before state load ([27ac93d](https://github.com/wippyai/dataflow/commit/27ac93d97dd85268117023b15a118ebe6763947e))
* centralize durable signal wake recovery ([3425ee9](https://github.com/wippyai/dataflow/commit/3425ee9d1ba10cb059df7bffc0d43f1792001458))
* make dataflow PostgreSQL-safe ([c2435d3](https://github.com/wippyai/dataflow/commit/c2435d3fe31bb36b65b4e4af1212c9185d90ca7a))
* make dataflow PostgreSQL-safe ([2cb54fe](https://github.com/wippyai/dataflow/commit/2cb54fee2788146866b608a7b1c0e70d10aa0513))
* prevent terminal yield recovery loops ([38d8c6d](https://github.com/wippyai/dataflow/commit/38d8c6d3641c03f01ebd55f6162f15a065050cf8))
* preserve exact wake delivery acknowledgement ([f5c5d07](https://github.com/wippyai/dataflow/commit/f5c5d07a242da2bfea47ebf919affad2f520153b))
* reconcile idempotent recovery results ([43b0eaa](https://github.com/wippyai/dataflow/commit/43b0eaad7d227d88a455acf286a6ae7da92d8278))
* replay consumed signals to replacement yields ([bbf15e0](https://github.com/wippyai/dataflow/commit/bbf15e05306c5292a1a3b9cdb2a12d40847ff239))
* retry durable wakes until consumption ([0652173](https://github.com/wippyai/dataflow/commit/06521737739698c81666f904300fdb383cefc10d))

## [0.5.4](https://github.com/wippyai/dataflow/compare/v0.5.3...v0.5.4) (2026-07-17)


### Bug Fixes

* build wake_repo queries so they run on postgres, and run the wake tests ([e07cce2](https://github.com/wippyai/dataflow/commit/e07cce2e16bb2c6f00c5510b8ab2d5227fe0fe6d))
* preserve exact dataflow execution identity ([d5e83ff](https://github.com/wippyai/dataflow/commit/d5e83ff3a980653853cdfd57b0696b179513233b))

## [0.5.3](https://github.com/wippyai/dataflow/compare/v0.5.2...v0.5.3) (2026-07-17)


### Bug Fixes

* **agent:** resume control child DAGs after recovery ([35b96d0](https://github.com/wippyai/dataflow/commit/35b96d0e918234baf7ad95631e00b49dedb8500e))
* **agent:** resume control child DAGs after recovery ([578e1ce](https://github.com/wippyai/dataflow/commit/578e1cec7bd480ae8ea6b100758b99b607d770ae))

## [0.5.2](https://github.com/wippyai/dataflow/compare/v0.5.1...v0.5.2) (2026-07-13)


### Bug Fixes

* **dataflow:** harden durable wake delivery and packaging ([093cdf9](https://github.com/wippyai/dataflow/commit/093cdf976f44a1960d97e1d1235180e33f0d496c))
* **dataflow:** harden release startup and metadata ([57f8c5e](https://github.com/wippyai/dataflow/commit/57f8c5e535b0519c3c27ff0166027fd9708fc41e))
* **dataflow:** make durable wake delivery exact ([4c1ec29](https://github.com/wippyai/dataflow/commit/4c1ec29b05a7ce0d45755a60a3b9c6c020772efe))

## [0.5.1](https://github.com/wippyai/dataflow/compare/v0.5.0...v0.5.1) (2026-07-13)


### Bug Fixes

* **dataflow:** make durable wake recovery event-driven ([#30](https://github.com/wippyai/dataflow/pull/30))

## [0.5.0](https://github.com/wippyai/dataflow/compare/v0.4.31...v0.5.0) (2026-07-13)

### Breaking Changes

* Removed the polling `client:wait` API. Synchronous callers use
  `client:execute`; asynchronous callers use `client:start` and retain the
  returned `dataflow_id`; in-flow composition uses the graph's native child
  commands. `interval_ms` status polling is no longer supported.


### Features

* add durable signal revival sweeper ([529a4d8](https://github.com/wippyai/dataflow/commit/529a4d884b75e5f2d3c7c57672948911dab33474))
* **dataflow:** make waits and recovery event driven ([6d6908c](https://github.com/wippyai/dataflow/commit/6d6908c39bdc8d5688e7b43fcf610840993428d8))
* **dataflow:** make waits and recovery event driven ([127b8c0](https://github.com/wippyai/dataflow/commit/127b8c013d24198a9006bb5e07667b95ef7676dd))
* **dataflow:** route agent checkpoints through canonical bindings ([2185a94](https://github.com/wippyai/dataflow/commit/2185a94eaccb211016b5d5c057df7943f0e9fd55))
* Define new param `web_host_origin_env` ([#1](https://github.com/wippyai/dataflow/issues/1)) ([0f663d9](https://github.com/wippyai/dataflow/commit/0f663d93030e2f890fe39d7ae26770192d48546f))
* Move code from userspace ([c5d4a45](https://github.com/wippyai/dataflow/commit/c5d4a452cdf23a8b1fe90ed2fe51ead60cf062ba))


### Bug Fixes

* bind dataflow sweeper security scope ([22e8bab](https://github.com/wippyai/dataflow/commit/22e8babec59a58ed0a682bf5c44484805e55d5ed))
* **ci:** repair release-please auth and version bumping ([#22](https://github.com/wippyai/dataflow/issues/22)) ([b0dc2be](https://github.com/wippyai/dataflow/commit/b0dc2bebff5e46c3f237d46985f9802f9d1e350d))
* dataflow_nodes table name ([#6](https://github.com/wippyai/dataflow/issues/6)) ([7a4b4a9](https://github.com/wippyai/dataflow/commit/7a4b4a9f4ee0cfa5904841e23b10d830c03c016f))
* declare modules/imports for require() under per-chunk import scoping ([8156953](https://github.com/wippyai/dataflow/commit/81569536e0c3f242a14b903348c11c4bc71c96d2))
* improve error handling, validation, and diagnostics in flow compiler ([#7](https://github.com/wippyai/dataflow/issues/7)) ([5b6d891](https://github.com/wippyai/dataflow/commit/5b6d891c8859fdc3c409b57ce40d2fdd05f8fe6b))
* preserve agent node context ([cf30632](https://github.com/wippyai/dataflow/commit/cf30632eb067350b1de86ca15bdf9861d210d026))
* recover pending commits on orchestrator restart ([ffdc855](https://github.com/wippyai/dataflow/commit/ffdc855fa3a43ea1f0aa1ef3c4259e14f7eb3462))
* relax dataflow revival sweep interval ([bda632e](https://github.com/wippyai/dataflow/commit/bda632e7486c408c0d507dc8fcc814070dc2855e))
* restore backward-compatible API across node, client, and func modules ([8939a59](https://github.com/wippyai/dataflow/commit/8939a599bc568bb5f059651f348ee336eedceed1))
* restore parallel node BC with on_error/filter/unwrap config API ([3e3e876](https://github.com/wippyai/dataflow/commit/3e3e87693b713506cd93b04978b47971332db1f5))
* stabilize dataflow revival sweeper ([ce39cdc](https://github.com/wippyai/dataflow/commit/ce39cdcade94945d3d44f582430e4a1634194471))

## [0.4.31](https://github.com/wippyai/dataflow/compare/v0.4.30...v0.4.31) (2026-07-12)


### Bug Fixes

* accept numeric millisecond timeouts in durable signal waits

## [0.4.1](https://github.com/wippyai/dataflow/compare/v0.4.0...v0.4.1) (2026-07-12)


### Bug Fixes

* stabilize dataflow revival sweeper ([ce39cdc](https://github.com/wippyai/dataflow/commit/ce39cdcade94945d3d44f582430e4a1634194471))

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
