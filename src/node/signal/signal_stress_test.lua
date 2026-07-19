local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function define_tests()
    describe("Signal Stress Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function make_signal_wf(signal_id)
            local nid = uuid.v7()
            local did = uuid.v7()
            return c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = nid,
                    node_type = "userspace.dataflow.node.signal:node",
                    status = consts.STATUS.PENDING,
                    config = { signal_id = signal_id, data_targets = {
                        { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                    }},
                    metadata = { title = "Signal: " .. signal_id }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = did, data_type = consts.DATA_TYPE.NODE_INPUT, node_id = nid,
                    content = { task = "test" }, content_type = consts.CONTENT_TYPE.JSON, key = "default"
                }}
            })
        end

        local function make_func_node(node_id, config)
            config = config or {}
            return {
                type = consts.COMMAND_TYPES.CREATE_NODE,
                payload = {
                    node_id = node_id,
                    node_type = "userspace.dataflow.node.func:node",
                    status = consts.STATUS.PENDING,
                    config = {
                        func_id = "userspace.dataflow.node.func:test_func",
                        inputs = config.inputs,
                        data_targets = config.data_targets or {},
                    },
                    metadata = { title = config.title or "func" },
                },
            }
        end

        local function make_signal_node(node_id, signal_id, config)
            config = config or {}
            return {
                type = consts.COMMAND_TYPES.CREATE_NODE,
                payload = {
                    node_id = node_id,
                    node_type = "userspace.dataflow.node.signal:node",
                    status = consts.STATUS.PENDING,
                    config = {
                        signal_id = signal_id,
                        inputs = config.inputs,
                        data_targets = config.data_targets or {},
                    },
                    metadata = { title = config.title or ("signal:" .. signal_id) },
                },
            }
        end

        local function make_state_node(node_id, config)
            config = config or {}
            return {
                type = consts.COMMAND_TYPES.CREATE_NODE,
                payload = {
                    node_id = node_id,
                    node_type = "userspace.dataflow.node.state:state",
                    status = consts.STATUS.PENDING,
                    config = {
                        output_mode = config.output_mode or "object",
                        inputs = config.inputs,
                        data_targets = config.data_targets or {},
                    },
                    metadata = { title = config.title or "state" },
                },
            }
        end

        local function make_input(data_id, node_id, content, key)
            return {
                type = consts.COMMAND_TYPES.CREATE_DATA,
                payload = {
                    data_id = data_id,
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = node_id,
                    content = content,
                    content_type = consts.CONTENT_TYPE.JSON,
                    key = key or "default",
                },
            }
        end

        local function target_node(node_id, key)
            return { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_id, discriminator = key or "default" }
        end

        local function target_output(key)
            return { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = key or "result", content_type = consts.CONTENT_TYPE.JSON }
        end

        local function kill_orchestrator(df_id)
            local pid = process.registry.lookup("dataflow." .. df_id)
            if pid then
                process.terminate(pid)
                time.sleep("200ms")
            end
        end

        local function wait_complete(df_id, timeout_ms)
            timeout_ms = timeout_ms or 5000
            local iters = math.ceil(timeout_ms / 200)
            for _ = 1, iters do
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_SUCCESS then return true end
                if status == consts.STATUS.COMPLETED_FAILURE then return false end
                time.sleep("200ms")
            end
            return false
        end

        local function wait_running(df_id, timeout_ms)
            timeout_ms = timeout_ms or 3000
            local iters = math.ceil(timeout_ms / 100)
            for _ = 1, iters do
                local status = c:get_status(df_id)
                if status == consts.STATUS.WAITING then return true end
                if status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE then
                    return false
                end
                time.sleep("100ms")
            end
            return false
        end

        -- ==========================================
        -- RAPID SIGNAL BURST
        -- ==========================================

        describe("rapid signal burst", function()
            it("10 signals in rapid succession to same workflow", function()
                local sid = "burst10-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")

                for i = 1, 10 do
                    c:signal(df_id, sid, { burst = i })
                end
                test.is_true(wait_complete(df_id), "completes under burst")
            end)

            it("10 wrong signals then 1 correct", function()
                local sid = "correct-after-wrong-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")

                for i = 1, 10 do
                    c:signal(df_id, "wrong-" .. i, { nope = i })
                end
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "still waiting")

                c:signal(df_id, sid, { correct = true })
                test.is_true(wait_complete(df_id), "correct signal works after noise")
            end)
        end)

        -- ==========================================
        -- TRIPLE SIGNAL CHAIN WITH RECOVERY
        -- ==========================================

        describe("triple signal chain with recovery", function()
            it("func -> sig1 -> sig2 -> sig3 -> func, kill at each signal", function()
                local f1 = uuid.v7()
                local s1 = uuid.v7()
                local s2 = uuid.v7()
                local s3 = uuid.v7()
                local f2 = uuid.v7()
                local sid1 = "chain3-1-" .. uuid.v7()
                local sid2 = "chain3-2-" .. uuid.v7()
                local sid3 = "chain3-3-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, { data_targets = { target_node(s1) } }),
                    make_signal_node(s1, sid1, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(s2) },
                    }),
                    make_signal_node(s2, sid2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(s3) },
                    }),
                    make_signal_node(s3, sid3, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "chain3", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)
                test.is_true(wait_running(df_id), "at sig1")

                -- kill at sig1, send signal
                kill_orchestrator(df_id)
                c:signal(df_id, sid1, { gate = 1 })
                time.sleep("2s")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "at sig2")

                -- kill at sig2, send signal
                kill_orchestrator(df_id)
                c:signal(df_id, sid2, { gate = 2 })
                time.sleep("2s")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "at sig3")

                -- kill at sig3, send signal
                kill_orchestrator(df_id)
                c:signal(df_id, sid3, { message = "final", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id, 10000), "chain3 completed after 3 kills")
            end)
        end)

        -- ==========================================
        -- WIDE FAN-OUT WITH SIGNALS
        -- ==========================================

        describe("wide fan-out with signals", function()
            it("3 parallel signal branches merge into state", function()
                local sig_a = uuid.v7()
                local sig_b = uuid.v7()
                local sig_c = uuid.v7()
                local merge = uuid.v7()
                local sid_a = "fan3-a-" .. uuid.v7()
                local sid_b = "fan3-b-" .. uuid.v7()
                local sid_c = "fan3-c-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_signal_node(sig_a, sid_a, {
                        data_targets = { target_node(merge, "from_a") },
                    }),
                    make_signal_node(sig_b, sid_b, {
                        data_targets = { target_node(merge, "from_b") },
                    }),
                    make_signal_node(sig_c, sid_c, {
                        data_targets = { target_node(merge, "from_c") },
                    }),
                    make_state_node(merge, {
                        inputs = { required = { "from_a", "from_b", "from_c" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), sig_a, { task = "a" }),
                    make_input(uuid.v7(), sig_b, { task = "b" }),
                    make_input(uuid.v7(), sig_c, { task = "c" }),
                })
                c:start(df_id)
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "all 3 waiting")

                c:signal(df_id, sid_a, { branch = "a" })
                time.sleep("300ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "2 waiting")

                c:signal(df_id, sid_b, { branch = "b" })
                time.sleep("300ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "1 waiting")

                c:signal(df_id, sid_c, { branch = "c" })
                test.is_true(wait_complete(df_id), "all 3 signals merge")
            end)

            it("2 parallel signals, kill, signal both", function()
                local sig_a = uuid.v7()
                local sig_b = uuid.v7()
                local merge = uuid.v7()
                local sid_a = "fan2k-a-" .. uuid.v7()
                local sid_b = "fan2k-b-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_signal_node(sig_a, sid_a, {
                        data_targets = { target_node(merge, "from_a") },
                    }),
                    make_signal_node(sig_b, sid_b, {
                        data_targets = { target_node(merge, "from_b") },
                    }),
                    make_state_node(merge, {
                        inputs = { required = { "from_a", "from_b" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), sig_a, { task = "a" }),
                    make_input(uuid.v7(), sig_b, { task = "b" }),
                })
                c:start(df_id)
                time.sleep("500ms")

                kill_orchestrator(df_id)

                c:signal(df_id, sid_a, { branch = "a" })
                time.sleep("200ms")
                c:signal(df_id, sid_b, { branch = "b" })

                test.is_true(wait_complete(df_id, 8000), "both recovered via detached yields")
            end)
        end)

        -- ==========================================
        -- DEEP PIPELINE: 7 NODES
        -- ==========================================

        describe("deep pipeline", function()
            it("func->sig->func->sig->func->sig->func completes", function()
                local n = {}
                for i = 1, 7 do n[i] = uuid.v7() end
                local sid1 = "deep-1-" .. uuid.v7()
                local sid2 = "deep-2-" .. uuid.v7()
                local sid3 = "deep-3-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(n[1], { data_targets = { target_node(n[2]) } }),
                    make_signal_node(n[2], sid1, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(n[3]) },
                    }),
                    make_func_node(n[3], {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(n[4]) },
                    }),
                    make_signal_node(n[4], sid2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(n[5]) },
                    }),
                    make_func_node(n[5], {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(n[6]) },
                    }),
                    make_signal_node(n[6], sid3, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(n[7]) },
                    }),
                    make_func_node(n[7], {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), n[1], { message = "deep", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)
                test.is_true(wait_running(df_id), "at sig1")

                c:signal(df_id, sid1, { message = "g1", delay_ms = 10, should_fail = false })
                time.sleep("500ms")

                c:signal(df_id, sid2, { message = "g2", delay_ms = 10, should_fail = false })
                time.sleep("500ms")

                c:signal(df_id, sid3, { message = "g3", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id, 5000), "deep pipeline done")
            end)
        end)

        -- ==========================================
        -- INTERLEAVED KILL AND SIGNAL
        -- ==========================================

        describe("interleaved kill and signal", function()
            it("alternating kill-signal-kill-signal pattern", function()
                local sid = "interleave-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                test.is_true(wait_running(df_id), "running")

                -- kill 1
                kill_orchestrator(df_id)
                -- restart without signal
                c:start(df_id)
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "still waiting")

                -- kill 2
                kill_orchestrator(df_id)
                -- wrong signal
                c:signal(df_id, "wrong-" .. uuid.v7(), { nope = true })
                time.sleep("2s")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "wrong signal ignored")

                -- kill 3
                kill_orchestrator(df_id)
                -- correct signal
                c:signal(df_id, sid, { finally = true })
                test.is_true(wait_complete(df_id, 8000), "completed after interleaved pattern")
            end)
        end)

        -- ==========================================
        -- CONCURRENT START + SIGNAL RACE
        -- ==========================================

        describe("concurrent start and signal race", function()
            it("signal sent in same tick as start", function()
                local sid = "same-tick-" .. uuid.v7()
                local df_id = make_signal_wf(sid)

                c:start(df_id)
                c:signal(df_id, sid, { instant = true })

                test.is_true(wait_complete(df_id, 8000), "handles same-tick")
            end)

            it("signal before start, then kill, then restart", function()
                local sid = "pre-start-kill-" .. uuid.v7()
                local df_id = make_signal_wf(sid)

                c:signal(df_id, sid, { early = true })
                c:start(df_id)
                time.sleep("200ms")
                kill_orchestrator(df_id)
                c:start(df_id)

                test.is_true(wait_complete(df_id, 8000), "pre-start signal survives kill")
            end)
        end)

        -- ==========================================
        -- MULTIPLE PIPELINES WITH SHARED SIGNAL NAMES
        -- ==========================================

        describe("shared signal names across workflows", function()
            it("same signal_id in different workflows are independent", function()
                local sid = "shared-name-" .. uuid.v7()
                local df1 = make_signal_wf(sid)
                local df2 = make_signal_wf(sid)
                local df3 = make_signal_wf(sid)

                c:start(df1)
                c:start(df2)
                c:start(df3)
                time.sleep("500ms")

                -- signal only wf2
                c:signal(df2, sid, { wf = 2 })
                time.sleep("500ms")
                test.eq(c:get_status(df1), consts.STATUS.WAITING, "wf1 still waiting")
                test.eq(c:get_status(df2), consts.STATUS.COMPLETED_SUCCESS, "wf2 done")
                test.eq(c:get_status(df3), consts.STATUS.WAITING, "wf3 still waiting")

                -- signal remaining
                c:signal(df1, sid, { wf = 1 })
                c:signal(df3, sid, { wf = 3 })
                test.is_true(wait_complete(df1), "wf1 done")
                test.is_true(wait_complete(df3), "wf3 done")
            end)
        end)

        -- ==========================================
        -- FUNC FAILURE AFTER SIGNAL IN PIPELINE
        -- ==========================================

        describe("func failure after signal", function()
            it("signal completes but downstream func fails", function()
                local sig = uuid.v7()
                local f2 = uuid.v7()
                local sid = "fail-after-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_signal_node(sig, sid, {
                        data_targets = { target_node(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), sig, { task = "test" }),
                })
                c:start(df_id)
                test.is_true(wait_running(df_id), "at signal")

                -- signal with data that causes func to fail
                c:signal(df_id, sid, { message = "fail", delay_ms = 10, should_fail = true })
                time.sleep("2s")

                local status = c:get_status(df_id)
                test.neq(status, consts.STATUS.COMPLETED_SUCCESS, "workflow not successful when func fails after signal")
            end)
        end)

        -- ==========================================
        -- DIAMOND WITH SIGNAL + KILL + RECOVERY
        -- ==========================================

        describe("diamond kill recovery", function()
            it("diamond: func fans out, signal branch killed, func branch done, signal recovers", function()
                local root = uuid.v7()
                local sig = uuid.v7()
                local fb = uuid.v7()
                local merge = uuid.v7()
                local sid = "dkill-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(root, {
                        data_targets = {
                            target_node(sig, "default"),
                            target_node(fb, "default"),
                        },
                    }),
                    make_signal_node(sig, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(merge, "from_sig") },
                    }),
                    make_func_node(fb, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node(merge, "from_func") },
                    }),
                    make_state_node(merge, {
                        inputs = { required = { "from_sig", "from_func" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), root, { message = "diamond", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)

                -- func branch should be done, signal still waiting
                test.is_true(wait_running(df_id, 5000), "waiting for signal")

                -- kill and recover with signal
                kill_orchestrator(df_id)
                c:signal(df_id, sid, { approved = true })
                test.is_true(wait_complete(df_id, 10000), "diamond recovered")
            end)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
