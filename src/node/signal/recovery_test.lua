local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local WAIT = "2s"

local function define_tests()
    describe("Signal Recovery Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function create_signal_wf(signal_id)
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

        local function create_func_wf(delay_ms)
            local nid = uuid.v7()
            local did = uuid.v7()
            return c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = nid,
                    node_type = "userspace.dataflow.node.func:node",
                    status = consts.STATUS.PENDING,
                    config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                        { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                    }},
                    metadata = { title = "Func node" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = did, data_type = consts.DATA_TYPE.NODE_INPUT, node_id = nid,
                    content = { message = "test", delay_ms = delay_ms or 10, should_fail = false },
                    content_type = consts.CONTENT_TYPE.JSON, key = "default"
                }}
            })
        end

        local function create_pipeline_wf(signal_id)
            local f1 = uuid.v7()
            local sig = uuid.v7()
            local f2 = uuid.v7()
            local did = uuid.v7()
            return c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = f1,
                    node_type = "userspace.dataflow.node.func:node",
                    status = consts.STATUS.PENDING,
                    config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                        { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = sig, discriminator = "default" }
                    }},
                    metadata = { title = "Step 1" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = sig,
                    node_type = "userspace.dataflow.node.signal:node",
                    status = consts.STATUS.PENDING,
                    config = { signal_id = signal_id, inputs = { required = { "default" } }, data_targets = {
                        { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f2, discriminator = "default" }
                    }},
                    metadata = { title = "Step 2: Signal" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = f2,
                    node_type = "userspace.dataflow.node.func:node",
                    status = consts.STATUS.PENDING,
                    config = { func_id = "userspace.dataflow.node.func:test_func", inputs = { required = { "default" } }, data_targets = {
                        { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                    }},
                    metadata = { title = "Step 3" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = did, data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f1,
                    content = { message = "pipeline", delay_ms = 10, should_fail = false },
                    content_type = consts.CONTENT_TYPE.JSON, key = "default"
                }}
            })
        end

        local function kill_orchestrator(df_id)
            local pid = process.registry.lookup("dataflow." .. df_id)
            if pid then
                process.terminate(pid)
                time.sleep("200ms")
            end
            return pid
        end

        -- ==========================================
        -- BASIC SIGNAL FLOW
        -- ==========================================

        describe("basic signal flow", function()
            it("starts, waits, receives signal, completes", function()
                local sid = "basic-" .. uuid.v7()
                local df_id, err = create_signal_wf(sid)
                test.is_nil(err, "create")
                c:start(df_id)
                time.sleep("300ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "waiting for signal")
                c:signal(df_id, sid, { approved = true })
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "completed after signal")
            end)

            it("signal with data passes through to output", function()
                local sid = "data-" .. uuid.v7()
                local df_id = create_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { key = "value", num = 42 })
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "completed")
            end)

            it("wrong signal_id does not satisfy", function()
                local sid = "correct-" .. uuid.v7()
                local df_id = create_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, "wrong-" .. uuid.v7(), { data = "nope" })
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "still waiting")
                -- now send correct signal
                c:signal(df_id, sid, { data = "yes" })
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "completed with correct signal")
            end)

            it("empty signal data works", function()
                local sid = "empty-" .. uuid.v7()
                local df_id = create_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, {})
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "completed with empty data")
            end)
        end)

        -- ==========================================
        -- KILL AND RECOVER
        -- ==========================================

        describe("kill and recover", function()
            it("supervisor revives an active workflow without a status scan", function()
                local df_id = create_func_wf(1000)
                c:start(df_id)
                time.sleep("5ms")
                local pid = process.registry.lookup("dataflow." .. df_id)
                test.not_nil(pid, "active orchestrator registered")
                process.terminate(pid)

                local completed = false
                for _ = 1, 50 do
                    if c:get_status(df_id) == consts.STATUS.COMPLETED_SUCCESS then
                        completed = true
                        break
                    end
                    time.sleep("100ms")
                end
                test.is_true(completed, "monitor EXIT event revived active workflow")
            end)

            it("recovers signal node after kill", function()
                local sid = "kill-sig-" .. uuid.v7()
                local df_id = create_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "running")
                kill_orchestrator(df_id)
                c:signal(df_id, sid, { recovered = true })
                time.sleep(WAIT)
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "recovered")
            end)

            it("recovers func node after kill", function()
                local df_id = create_func_wf()
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)
                -- respawn by starting again
                c:start(df_id)
                time.sleep(WAIT)
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "func recovered")
            end)

            it("recovers pipeline func->signal->func after kill at signal", function()
                local sid = "pipe-kill-" .. uuid.v7()
                local df_id = create_pipeline_wf(sid)
                c:start(df_id)
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "pipeline running at signal")
                kill_orchestrator(df_id)
                c:signal(df_id, sid, { approved = true })
                time.sleep(WAIT)
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "pipeline recovered")
            end)

            it("double kill and recover", function()
                local sid = "double-kill-" .. uuid.v7()
                local df_id = create_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                kill_orchestrator(df_id)
                time.sleep("100ms")
                -- respawn without signal
                c:start(df_id)
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "still waiting after respawn without signal")
                -- kill again
                kill_orchestrator(df_id)
                -- now send signal (auto-respawns)
                c:signal(df_id, sid, { ok = true })
                time.sleep(WAIT)
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "completed after double kill")
            end)
        end)

        -- ==========================================
        -- COMMIT BACKLOG
        -- ==========================================

        describe("commit backlog", function()
            it("processes signal from backlog after respawn", function()
                local sid = "backlog-" .. uuid.v7()
                local df_id = create_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                kill_orchestrator(df_id)
                c:signal(df_id, sid, { from = "backlog" })
                time.sleep(WAIT)
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "backlog processed")
            end)

            it("signal before start (pre-queued)", function()
                local sid = "prequeue-" .. uuid.v7()
                local df_id = create_signal_wf(sid)
                -- send signal BEFORE starting workflow
                c:signal(df_id, sid, { early = true })
                time.sleep("100ms")
                c:start(df_id)
                time.sleep(WAIT)
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "pre-queued signal works")
            end)
        end)

        -- ==========================================
        -- IDEMPOTENCY AND RACES
        -- ==========================================

        describe("idempotency", function()
            it("handles duplicate signals", function()
                local sid = "dup-" .. uuid.v7()
                local df_id = create_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                c:signal(df_id, sid, { first = true })
                c:signal(df_id, sid, { second = true })
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "handles duplicates")
            end)

            it("handles concurrent signal + respawn", function()
                local sid = "race-" .. uuid.v7()
                local df_id = create_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")
                kill_orchestrator(df_id)
                c:signal(df_id, sid, { a = 1 })
                c:signal(df_id, sid, { b = 2 })
                time.sleep(WAIT)
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "concurrent race OK")
            end)
        end)

        -- ==========================================
        -- MULTIPLE WORKFLOWS
        -- ==========================================

        describe("multiple workflows", function()
            it("two independent signal workflows", function()
                local sid1 = "multi1-" .. uuid.v7()
                local sid2 = "multi2-" .. uuid.v7()
                local df1 = create_signal_wf(sid1)
                local df2 = create_signal_wf(sid2)
                c:start(df1)
                c:start(df2)
                time.sleep("300ms")
                test.eq(c:get_status(df1), consts.STATUS.WAITING, "wf1 running")
                test.eq(c:get_status(df2), consts.STATUS.WAITING, "wf2 running")
                c:signal(df1, sid1, { wf = 1 })
                time.sleep("500ms")
                test.eq(c:get_status(df1), consts.STATUS.COMPLETED_SUCCESS, "wf1 completed")
                test.eq(c:get_status(df2), consts.STATUS.WAITING, "wf2 still running")
                c:signal(df2, sid2, { wf = 2 })
                time.sleep("500ms")
                test.eq(c:get_status(df2), consts.STATUS.COMPLETED_SUCCESS, "wf2 completed")
            end)

            it("signal to wrong workflow does nothing", function()
                local sid1 = "cross1-" .. uuid.v7()
                local sid2 = "cross2-" .. uuid.v7()
                local df1 = create_signal_wf(sid1)
                local df2 = create_signal_wf(sid2)
                c:start(df1)
                c:start(df2)
                time.sleep("300ms")
                -- send wf2's signal to wf1 (should not satisfy)
                c:signal(df1, sid2, { wrong = true })
                time.sleep("500ms")
                test.eq(c:get_status(df1), consts.STATUS.WAITING, "wf1 not satisfied by wrong signal")
                -- correct signals
                c:signal(df1, sid1, { ok = true })
                c:signal(df2, sid2, { ok = true })
                time.sleep("500ms")
                test.eq(c:get_status(df1), consts.STATUS.COMPLETED_SUCCESS, "wf1 completed")
                test.eq(c:get_status(df2), consts.STATUS.COMPLETED_SUCCESS, "wf2 completed")
            end)
        end)

        -- ==========================================
        -- PIPELINE RECOVERY
        -- ==========================================

        describe("pipeline recovery", function()
            it("pipeline completes normally with signal in middle", function()
                local sid = "pipe-normal-" .. uuid.v7()
                local df_id = create_pipeline_wf(sid)
                c:start(df_id)
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "waiting at signal")
                c:signal(df_id, sid, { go = true })
                time.sleep("1s")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "pipeline done")
                local out = c:output(df_id)
                test.not_nil(out, "has output")
            end)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
