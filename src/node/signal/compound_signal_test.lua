local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local WAIT = "3s"

local function define_tests()
    describe("Compound Signal Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        -- helpers

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

        local function target_node_input(node_id, key)
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
            return pid
        end

        local function wait_running(df_id, timeout_ms)
            timeout_ms = timeout_ms or 2000
            local iterations = math.ceil(timeout_ms / 100)
            for _ = 1, iterations do
                local status = c:get_status(df_id)
                if status == consts.STATUS.RUNNING then return true end
                if status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE then
                    return false
                end
                time.sleep("100ms")
            end
            return false
        end

        local function wait_complete(df_id, timeout_ms)
            timeout_ms = timeout_ms or 5000
            local iterations = math.ceil(timeout_ms / 200)
            for _ = 1, iterations do
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_SUCCESS then return true end
                if status == consts.STATUS.COMPLETED_FAILURE then return false end
                time.sleep("200ms")
            end
            return false
        end

        local function count_outputs(df_id)
            return #data_reader.with_dataflow(df_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :all()
        end

        -- ==========================================
        -- FUNC -> SIGNAL -> FUNC PIPELINE
        -- ==========================================

        describe("func -> signal -> func", function()
            it("completes pipeline when signal received", function()
                local f1 = uuid.v7()
                local sig = uuid.v7()
                local f2 = uuid.v7()
                local sid = "pipe-" .. uuid.v7()

                local df_id, err = c:create_workflow({
                    make_func_node(f1, {
                        title = "step1",
                        data_targets = { target_node_input(sig) },
                    }),
                    make_signal_node(sig, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f2) },
                    }),
                    make_func_node(f2, {
                        title = "step3",
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "pipeline", delay_ms = 10, should_fail = false }),
                })
                test.is_nil(err, "create")
                c:start(df_id)

                test.is_true(wait_running(df_id), "running at signal")
                c:signal(df_id, sid, { approved = true })
                test.is_true(wait_complete(df_id), "completed")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "success")
            end)

            it("blocks until signal arrives", function()
                local f1 = uuid.v7()
                local sig = uuid.v7()
                local f2 = uuid.v7()
                local sid = "block-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig) },
                    }),
                    make_signal_node(sig, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "test", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)

                test.is_true(wait_running(df_id), "running")
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.RUNNING, "still running without signal")

                c:signal(df_id, sid, { go = true })
                test.is_true(wait_complete(df_id), "completes after signal")
            end)

            it("passes signal data through to downstream func", function()
                local f1 = uuid.v7()
                local sig = uuid.v7()
                local f2 = uuid.v7()
                local sid = "data-pass-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig) },
                    }),
                    make_signal_node(sig, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "initial", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)
                test.is_true(wait_running(df_id), "running")

                c:signal(df_id, sid, { message = "from-signal", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id), "completed")

                local out = c:output(df_id)
                test.not_nil(out, "has output")
            end)
        end)

        -- ==========================================
        -- SIGNAL AS FIRST NODE
        -- ==========================================

        describe("signal as first node", function()
            it("waits for signal before any func runs", function()
                local sig = uuid.v7()
                local f1 = uuid.v7()
                local sid = "first-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_signal_node(sig, sid, {
                        data_targets = { target_node_input(f1) },
                    }),
                    make_func_node(f1, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), sig, { task = "start" }),
                })
                c:start(df_id)

                test.is_true(wait_running(df_id), "running at signal")
                c:signal(df_id, sid, { message = "go", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id), "completed after signal")
            end)
        end)

        -- ==========================================
        -- SIGNAL AS LAST NODE
        -- ==========================================

        describe("signal as last node", function()
            it("func runs then signal waits for final approval", function()
                local f1 = uuid.v7()
                local sig = uuid.v7()
                local sid = "last-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig) },
                    }),
                    make_signal_node(sig, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "compute", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)

                test.is_true(wait_running(df_id), "running at signal")
                c:signal(df_id, sid, { final_approval = true })
                test.is_true(wait_complete(df_id), "completed after final signal")
            end)
        end)

        -- ==========================================
        -- DOUBLE SIGNAL (TWO SIGNALS IN SEQUENCE)
        -- ==========================================

        describe("double signal in pipeline", function()
            it("func -> signal1 -> signal2 -> func requires both signals", function()
                local f1 = uuid.v7()
                local sig1 = uuid.v7()
                local sig2 = uuid.v7()
                local f2 = uuid.v7()
                local sid1 = "first-gate-" .. uuid.v7()
                local sid2 = "second-gate-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig1) },
                    }),
                    make_signal_node(sig1, sid1, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(sig2) },
                    }),
                    make_signal_node(sig2, sid2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "double", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)

                test.is_true(wait_running(df_id), "running at sig1")
                c:signal(df_id, sid1, { gate = 1 })
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.RUNNING, "still running at sig2")

                c:signal(df_id, sid2, { gate = 2, message = "final", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id), "completed after both signals")
            end)

            it("sending signal2 before signal1 does not unblock", function()
                local f1 = uuid.v7()
                local sig1 = uuid.v7()
                local sig2 = uuid.v7()
                local f2 = uuid.v7()
                local sid1 = "gate-a-" .. uuid.v7()
                local sid2 = "gate-b-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig1) },
                    }),
                    make_signal_node(sig1, sid1, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(sig2) },
                    }),
                    make_signal_node(sig2, sid2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "test", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)

                test.is_true(wait_running(df_id), "running at sig1")
                -- send signal2 while sig1 is still waiting
                c:signal(df_id, sid2, { early = true })
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.RUNNING, "still blocked at sig1")

                c:signal(df_id, sid1, { gate1 = true })
                -- sig2 should be satisfied from the pre-queued signal
                test.is_true(wait_complete(df_id, 6000), "completed after sig1 unblocks")
            end)
        end)

        -- ==========================================
        -- PARALLEL BRANCHES WITH SIGNALS
        -- ==========================================

        describe("parallel branches with signals", function()
            it("two independent func->signal branches converge", function()
                local f_a = uuid.v7()
                local sig_a = uuid.v7()
                local f_b = uuid.v7()
                local sig_b = uuid.v7()
                local merge = uuid.v7()
                local sid_a = "branch-a-" .. uuid.v7()
                local sid_b = "branch-b-" .. uuid.v7()

                local df_id = c:create_workflow({
                    -- branch A
                    make_func_node(f_a, {
                        title = "branch-a-func",
                        data_targets = { target_node_input(sig_a) },
                    }),
                    make_signal_node(sig_a, sid_a, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(merge, "from_a") },
                    }),
                    -- branch B
                    make_func_node(f_b, {
                        title = "branch-b-func",
                        data_targets = { target_node_input(sig_b) },
                    }),
                    make_signal_node(sig_b, sid_b, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(merge, "from_b") },
                    }),
                    -- merge
                    make_state_node(merge, {
                        inputs = { required = { "from_a", "from_b" } },
                        data_targets = { target_output() },
                    }),
                    -- inputs for both branches
                    make_input(uuid.v7(), f_a, { message = "a", delay_ms = 10, should_fail = false }),
                    make_input(uuid.v7(), f_b, { message = "b", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)

                test.is_true(wait_running(df_id), "running")
                time.sleep("500ms")

                -- satisfy branch A
                c:signal(df_id, sid_a, { branch = "A" })
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.RUNNING, "still waiting for branch B")

                -- satisfy branch B
                c:signal(df_id, sid_b, { branch = "B" })
                test.is_true(wait_complete(df_id), "completed after both branches signaled")
            end)

            it("one branch completes while other waits for signal", function()
                local f_fast = uuid.v7()
                local sig_slow = uuid.v7()
                local merge = uuid.v7()
                local sid = "slow-" .. uuid.v7()

                local df_id = c:create_workflow({
                    -- fast branch: func straight to merge
                    make_func_node(f_fast, {
                        title = "fast-branch",
                        data_targets = { target_node_input(merge, "fast") },
                    }),
                    -- slow branch: signal gate
                    make_signal_node(sig_slow, sid, {
                        data_targets = { target_node_input(merge, "slow") },
                    }),
                    -- merge
                    make_state_node(merge, {
                        inputs = { required = { "fast", "slow" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f_fast, { message = "fast", delay_ms = 10, should_fail = false }),
                    make_input(uuid.v7(), sig_slow, { task = "wait" }),
                })
                c:start(df_id)

                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.RUNNING, "func done, waiting for signal")

                c:signal(df_id, sid, { slow_data = true })
                test.is_true(wait_complete(df_id), "completed after signal")
            end)
        end)

        -- ==========================================
        -- LONG CHAIN: FUNC -> SIGNAL -> FUNC -> SIGNAL -> FUNC
        -- ==========================================

        describe("long chain with multiple signals", function()
            it("five-node pipeline with two signals", function()
                local f1 = uuid.v7()
                local sig1 = uuid.v7()
                local f2 = uuid.v7()
                local sig2 = uuid.v7()
                local f3 = uuid.v7()
                local sid1 = "chain-1-" .. uuid.v7()
                local sid2 = "chain-2-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig1) },
                    }),
                    make_signal_node(sig1, sid1, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(sig2) },
                    }),
                    make_signal_node(sig2, sid2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f3) },
                    }),
                    make_func_node(f3, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "chain", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)

                test.is_true(wait_running(df_id), "running at sig1")
                c:signal(df_id, sid1, { message = "step2", delay_ms = 10, should_fail = false })
                time.sleep("800ms")
                test.eq(c:get_status(df_id), consts.STATUS.RUNNING, "waiting at sig2")

                c:signal(df_id, sid2, { message = "step4", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id), "chain completed")
            end)
        end)

        -- ==========================================
        -- FUNC FAILURE BEFORE SIGNAL
        -- ==========================================

        describe("func failure before signal", function()
            it("workflow fails when func before signal fails", function()
                local f1 = uuid.v7()
                local sig = uuid.v7()
                local sid = "after-fail-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig) },
                    }),
                    make_signal_node(sig, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "fail", delay_ms = 10, should_fail = true }),
                })
                c:start(df_id)

                time.sleep("1s")
                local status = c:get_status(df_id)
                test.neq(status, consts.STATUS.COMPLETED_SUCCESS, "not success when func fails")
            end)
        end)

        -- ==========================================
        -- SIGNAL ONLY WORKFLOW
        -- ==========================================

        describe("signal-only workflow", function()
            it("single signal node completes with signal", function()
                local sig = uuid.v7()
                local sid = "solo-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_signal_node(sig, sid, {
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), sig, { task = "just wait" }),
                })
                c:start(df_id)

                test.is_true(wait_running(df_id), "waiting")
                c:signal(df_id, sid, { result = "done" })
                test.is_true(wait_complete(df_id), "completed")
            end)
        end)

        -- ==========================================
        -- DIAMOND: FUNC -> (SIGNAL + FUNC) -> STATE -> FUNC
        -- ==========================================

        describe("diamond topology with signal", function()
            it("func fans out to signal and func, state merges, func finishes", function()
                local root = uuid.v7()
                local sig_branch = uuid.v7()
                local func_branch = uuid.v7()
                local merger = uuid.v7()
                local final = uuid.v7()
                local sid = "diamond-" .. uuid.v7()

                local df_id = c:create_workflow({
                    -- root func fans out
                    make_func_node(root, {
                        data_targets = {
                            target_node_input(sig_branch, "default"),
                            target_node_input(func_branch, "default"),
                        },
                    }),
                    -- signal branch
                    make_signal_node(sig_branch, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(merger, "signal_result") },
                    }),
                    -- func branch
                    make_func_node(func_branch, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(merger, "func_result") },
                    }),
                    -- state merge
                    make_state_node(merger, {
                        inputs = { required = { "signal_result", "func_result" } },
                        data_targets = { target_node_input(final) },
                    }),
                    -- final func
                    make_func_node(final, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), root, { message = "diamond", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)

                test.is_true(wait_running(df_id), "running")
                -- func branch runs immediately, signal branch waits
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.RUNNING, "waiting for signal branch")

                c:signal(df_id, sid, { message = "approved", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id, 8000), "diamond completed")
            end)
        end)

        -- ==========================================
        -- RECOVERY: COMPOUND PIPELINE KILL SCENARIOS
        -- ==========================================

        describe("compound recovery", function()
            it("kills at signal in 3-node pipeline, recovers with signal", function()
                local f1 = uuid.v7()
                local sig = uuid.v7()
                local f2 = uuid.v7()
                local sid = "kill-pipe-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig) },
                    }),
                    make_signal_node(sig, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "kill-test", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)
                test.is_true(wait_running(df_id), "running at signal")

                kill_orchestrator(df_id)
                c:signal(df_id, sid, { message = "after-kill", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id, 8000), "recovered and completed")
            end)

            it("kills at signal in diamond, recovers", function()
                local root = uuid.v7()
                local sig = uuid.v7()
                local func_b = uuid.v7()
                local merger = uuid.v7()
                local sid = "diamond-kill-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(root, {
                        data_targets = {
                            target_node_input(sig, "default"),
                            target_node_input(func_b, "default"),
                        },
                    }),
                    make_signal_node(sig, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(merger, "from_sig") },
                    }),
                    make_func_node(func_b, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(merger, "from_func") },
                    }),
                    make_state_node(merger, {
                        inputs = { required = { "from_sig", "from_func" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), root, { message = "diamond", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)
                test.is_true(wait_running(df_id), "running")
                time.sleep("500ms")

                kill_orchestrator(df_id)
                c:signal(df_id, sid, { approved = true })
                test.is_true(wait_complete(df_id, 8000), "diamond recovered after kill")
                test.eq(count_outputs(df_id), 1, "diamond recovery produced one output")
            end)

            it("kills during double-signal pipeline, recovers step by step", function()
                local f1 = uuid.v7()
                local sig1 = uuid.v7()
                local sig2 = uuid.v7()
                local f2 = uuid.v7()
                local sid1 = "dsig1-" .. uuid.v7()
                local sid2 = "dsig2-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig1) },
                    }),
                    make_signal_node(sig1, sid1, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(sig2) },
                    }),
                    make_signal_node(sig2, sid2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "test", delay_ms = 10, should_fail = false }),
                })
                c:start(df_id)
                test.is_true(wait_running(df_id), "at sig1")

                -- kill at sig1
                kill_orchestrator(df_id)
                c:signal(df_id, sid1, { gate1 = true })
                time.sleep(WAIT)
                test.eq(c:get_status(df_id), consts.STATUS.RUNNING, "now at sig2")

                -- kill at sig2
                kill_orchestrator(df_id)
                c:signal(df_id, sid2, { message = "go", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id, 8000), "completed after double kill at each signal")
            end)

            it("pre-queues signal before starting pipeline", function()
                local f1 = uuid.v7()
                local sig = uuid.v7()
                local f2 = uuid.v7()
                local sid = "prequeue-pipe-" .. uuid.v7()

                local df_id = c:create_workflow({
                    make_func_node(f1, {
                        data_targets = { target_node_input(sig) },
                    }),
                    make_signal_node(sig, sid, {
                        inputs = { required = { "default" } },
                        data_targets = { target_node_input(f2) },
                    }),
                    make_func_node(f2, {
                        inputs = { required = { "default" } },
                        data_targets = { target_output() },
                    }),
                    make_input(uuid.v7(), f1, { message = "prequeue", delay_ms = 10, should_fail = false }),
                })

                -- signal BEFORE start
                c:signal(df_id, sid, { message = "early", delay_ms = 10, should_fail = false })
                time.sleep("100ms")
                c:start(df_id)

                test.is_true(wait_complete(df_id, 8000), "pre-queued signal processed in pipeline")
            end)
        end)

        -- ==========================================
        -- MULTIPLE WORKFLOWS WITH DIFFERENT TOPOLOGIES
        -- ==========================================

        describe("multiple concurrent workflows", function()
            it("runs three workflows with signals independently", function()
                local sids = {}
                local df_ids = {}

                for i = 1, 3 do
                    local sig = uuid.v7()
                    local sid = "multi-" .. i .. "-" .. uuid.v7()
                    sids[i] = sid

                    local df_id = c:create_workflow({
                        make_signal_node(sig, sid, {
                            data_targets = { target_output() },
                        }),
                        make_input(uuid.v7(), sig, { index = i }),
                    })
                    df_ids[i] = df_id
                    c:start(df_id)
                end

                time.sleep("500ms")
                for i = 1, 3 do
                    test.eq(c:get_status(df_ids[i]), consts.STATUS.RUNNING, "wf" .. i .. " running")
                end

                -- signal them in reverse order
                c:signal(df_ids[3], sids[3], { order = 3 })
                time.sleep("500ms")
                test.eq(c:get_status(df_ids[3]), consts.STATUS.COMPLETED_SUCCESS, "wf3 done")
                test.eq(c:get_status(df_ids[1]), consts.STATUS.RUNNING, "wf1 still waiting")

                c:signal(df_ids[1], sids[1], { order = 1 })
                time.sleep("500ms")
                test.eq(c:get_status(df_ids[1]), consts.STATUS.COMPLETED_SUCCESS, "wf1 done")
                test.eq(c:get_status(df_ids[2]), consts.STATUS.RUNNING, "wf2 still waiting")

                c:signal(df_ids[2], sids[2], { order = 2 })
                test.is_true(wait_complete(df_ids[2]), "wf2 done")
            end)
        end)

    end)
end

return { run_tests = test.run_cases(define_tests) }
