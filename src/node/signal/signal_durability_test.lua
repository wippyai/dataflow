local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local sql = require("sql")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local WAIT = "3s"

local function define_tests()
    describe("Signal Durability Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        -- helpers

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

        local function make_func_signal_func_wf(signal_id)
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
            local registry_name = "dataflow." .. df_id
            local pid = process.registry.lookup(registry_name)
            if not pid then return false end

            local events = process.events()
            local monitored, monitor_err = process.monitor(pid)
            if not monitored then
                error("failed to monitor orchestrator before recovery kill: " .. tostring(monitor_err))
            end

            local terminated, terminate_err = process.terminate(pid)
            if not terminated then
                error("failed to terminate orchestrator for recovery: " .. tostring(terminate_err))
            end

            local timeout = time.after("3s")
            local result = channel.select({ events:case_receive(), timeout:case_receive() })
            if result.channel ~= events or result.value.kind ~= process.event.EXIT or
               tostring(result.value.from) ~= tostring(pid) then
                error("orchestrator did not fully exit before recovery restart: " .. df_id)
            end
            return true
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

        local function wait_running(df_id, timeout_ms)
            timeout_ms = timeout_ms or 10000
            local iterations = math.ceil(timeout_ms / 100)
            for _ = 1, iterations do
                local status = c:get_status(df_id)
                if status == consts.STATUS.WAITING then return true end
                if status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE then
                    return false
                end
                time.sleep("100ms")
            end
            return false
        end

        local function failure_diagnostics(df_id)
            local diagnostics = {
                dataflow_id = df_id,
                status = select(1, c:get_status(df_id)),
                owner = tostring(process.registry.lookup("dataflow." .. df_id)),
            }
            local db, db_err = sql.get(consts.APP_DB)
            if not db then
                diagnostics.database_error = tostring(db_err)
            else
                local function rows(fields, table_name, order_by)
                    local query = sql.builder.select(table.unpack(fields))
                        :from(table_name)
                        :where("dataflow_id = ?", df_id)
                    if order_by then query = query:order_by(order_by) end
                    local result, query_err = query:run_with(db):query()
                    if query_err then return { error = tostring(query_err) } end
                    return result or {}
                end
                diagnostics.nodes = rows(
                    { "node_id", "type", "status", "updated_at" },
                    "dataflow_nodes",
                    "created_at ASC"
                )
                diagnostics.wakes = rows(
                    { "wake_key", "wake_at" },
                    "dataflow_wakes",
                    "wake_at ASC"
                )
                diagnostics.commits = rows(
                    { "commit_id", "op_id", "created_at" },
                    "dataflow_commits",
                    "created_at ASC"
                )
                diagnostics.data = rows(
                    { "data_id", "node_id", "type", "key", "discriminator", "created_at" },
                    "dataflow_data",
                    "created_at ASC"
                )
                db:release()
            end
            local encoded, encode_err = json.encode(diagnostics)
            return encoded or ("diagnostics encoding failed: " .. tostring(encode_err))
        end

        -- ==========================================
        -- TRIPLE KILL AND RECOVERY
        -- ==========================================

        describe("triple kill and recovery", function()
            it("recovers after three consecutive kills", function()
                local sid = "triple-kill-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                test.is_true(wait_running(df_id), "running")

                -- kill 1
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_running(df_id), "recovered after kill 1")

                -- kill 2
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_running(df_id), "recovered after kill 2")

                -- kill 3
                kill_orchestrator(df_id)
                c:signal(df_id, sid, { survived = 3 })
                test.is_true(wait_complete(df_id, 8000), "completed after triple kill")
            end)

            it("triple kill on pipeline with signal in middle", function()
                local sid = "triple-pipe-" .. uuid.v7()
                local df_id = make_func_signal_func_wf(sid)
                c:start(df_id)
                test.is_true(wait_running(df_id), "running")

                -- kill 1
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("500ms")

                -- kill 2
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("500ms")

                -- kill 3
                kill_orchestrator(df_id)
                c:signal(df_id, sid, { message = "go", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id, 10000), "pipeline recovered after triple kill")
            end)
        end)

        -- ==========================================
        -- RAPID START/KILL CYCLES
        -- ==========================================

        describe("rapid start/kill cycles", function()
            it("rapid kill-restart 5 times then signal", function()
                local sid = "rapid-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("300ms")

                for i = 1, 5 do
                    kill_orchestrator(df_id)
                    c:start(df_id)
                    time.sleep("200ms")
                end

                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "still running after 5 cycles")
                c:signal(df_id, sid, { cycles = 5 })
                test.is_true(wait_complete(df_id, 8000), "completed after rapid cycles")
            end)

            it("immediate kill after start does not corrupt state", function()
                local sid = "imm-kill-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)

                c:signal(df_id, sid, { recovered = true })
                test.is_true(wait_complete(df_id, 8000), "recovers from immediate kill")
            end)
        end)

        -- ==========================================
        -- SIGNAL BACKLOG SCENARIOS
        -- ==========================================

        describe("signal backlog", function()
            it("signal while orchestrator dead is processed on restart", function()
                local sid = "backlog-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                test.is_true(wait_running(df_id), "running")

                kill_orchestrator(df_id)
                c:signal(df_id, sid, { from = "backlog" })
                test.is_true(wait_complete(df_id, 8000), "backlog signal processed")
            end)

            it("multiple signals while dead - first matches", function()
                local sid = "multi-backlog-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                test.is_true(wait_running(df_id), "running")

                kill_orchestrator(df_id)
                c:signal(df_id, sid, { version = 1 })
                c:signal(df_id, sid, { version = 2 })
                c:signal(df_id, sid, { version = 3 })

                test.is_true(wait_complete(df_id, 8000), "completed from backlog")
            end)

            it("wrong signal in backlog, then correct signal", function()
                local sid = "backlog-wrong-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                test.is_true(wait_running(df_id), "running")

                kill_orchestrator(df_id)
                -- wrong signal in backlog
                c:signal(df_id, "wrong-" .. uuid.v7(), { nope = true })
                -- correct signal
                c:signal(df_id, sid, { correct = true })

                test.is_true(wait_complete(df_id, 8000), "correct backlog signal satisfies")
            end)

            it("signal auto-starts a pending workflow", function()
                local sid = "pre-create-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                -- signal() durably queues the signal and auto-starts the workflow
                c:signal(df_id, sid, { ultra_early = true })
                test.is_true(wait_complete(df_id, 8000), "pre-start signal works")
            end)
        end)

        -- ==========================================
        -- KILL AT TRANSITION POINTS
        -- ==========================================

        describe("kill at transition points", function()
            it("kill right after func completes but before signal starts in pipeline", function()
                local sid = "transition-" .. uuid.v7()
                local df_id = make_func_signal_func_wf(sid)
                c:start(df_id)
                -- wait for func to complete (signal node should be starting)
                time.sleep("200ms")
                kill_orchestrator(df_id)

                c:signal(df_id, sid, { message = "recovery", delay_ms = 10, should_fail = false })
                test.is_true(wait_complete(df_id, 10000), "recovered at transition")
            end)

            it("kill and restart without signal, then kill again and signal", function()
                local sid = "restart-no-sig-" .. uuid.v7()
                local df_id = make_signal_wf(sid)
                c:start(df_id)
                test.is_true(wait_running(df_id), "running")

                -- kill and restart without signal
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_running(df_id), "running again")

                -- still no signal
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "still waiting")

                -- kill again, this time with signal
                kill_orchestrator(df_id)
                c:signal(df_id, sid, { finally = true })
                test.is_true(wait_complete(df_id, 8000), "completed")
            end)
        end)

        -- ==========================================
        -- MULTI-WORKFLOW RECOVERY
        -- ==========================================

        describe("multi-workflow recovery", function()
            it("two workflows killed simultaneously, both recover", function()
                local sid1 = "mwf1-" .. uuid.v7()
                local sid2 = "mwf2-" .. uuid.v7()
                local df1 = make_signal_wf(sid1)
                local df2 = make_signal_wf(sid2)

                c:start(df1)
                c:start(df2)
                time.sleep("300ms")
                test.eq(c:get_status(df1), consts.STATUS.WAITING, "wf1 running")
                test.eq(c:get_status(df2), consts.STATUS.WAITING, "wf2 running")

                -- kill both
                kill_orchestrator(df1)
                kill_orchestrator(df2)

                -- signal both (triggers respawn)
                c:signal(df1, sid1, { recovered = 1 })
                c:signal(df2, sid2, { recovered = 2 })

                test.is_true(wait_complete(df1, 8000), "wf1 recovered")
                test.is_true(wait_complete(df2, 8000), "wf2 recovered")
            end)

            it("three pipelines killed at different stages", function()
                local sid1 = "stage1-" .. uuid.v7()
                local sid2 = "stage2-" .. uuid.v7()
                local sid3 = "stage3-" .. uuid.v7()

                local df1 = make_signal_wf(sid1)
                local df2 = make_func_signal_func_wf(sid2)
                local df3 = make_signal_wf(sid3)

                c:start(df1)
                c:start(df2)
                c:start(df3)
                time.sleep("500ms")

                -- kill all
                kill_orchestrator(df1)
                kill_orchestrator(df2)
                kill_orchestrator(df3)

                -- signal all
                c:signal(df1, sid1, { ok = true })
                c:signal(df2, sid2, { message = "go", delay_ms = 10, should_fail = false })
                c:signal(df3, sid3, { ok = true })

                test.is_true(wait_complete(df1, 8000), "simple wf1 recovered")
                test.is_true(wait_complete(df2, 10000), "pipeline wf2 recovered")
                test.is_true(wait_complete(df3, 8000), "simple wf3 recovered")
            end)
        end)

        -- ==========================================
        -- FUNC NODE RECOVERY (ISOLATION)
        -- ==========================================

        describe("func node recovery in isolation", function()
            local function make_func_wf(delay_ms)
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

            it("func node recovers after kill during execution", function()
                local df_id = make_func_wf(500) -- longer delay to catch it mid-execution
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "func recovered")
            end)

            it("func node recovers after immediate kill", function()
                local df_id = make_func_wf(10)
                c:start(df_id)
                time.sleep("20ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "func recovered from immediate kill")
            end)

            it("func node recovers after triple kill", function()
                local df_id = make_func_wf(200)
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "func recovered after triple kill")
            end)
        end)

        -- ==========================================
        -- STATE NODE RECOVERY (ISOLATION)
        -- ==========================================

        describe("state node recovery in isolation", function()
            local function make_func_state_wf()
                local f1 = uuid.v7()
                local f2 = uuid.v7()
                local st = uuid.v7()
                local d1 = uuid.v7()
                local d2 = uuid.v7()
                return c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f1,
                        node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = st, discriminator = "from_a" }
                        }},
                        metadata = { title = "Func A" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f2,
                        node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = st, discriminator = "from_b" }
                        }},
                        metadata = { title = "Func B" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = st,
                        node_type = "userspace.dataflow.node.state:state",
                        status = consts.STATUS.PENDING,
                        config = { output_mode = "object", inputs = { required = { "from_a", "from_b" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "State merge" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = d1, data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f1,
                        content = { message = "a", delay_ms = 10, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = d2, data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f2,
                        content = { message = "b", delay_ms = 10, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }},
                })
            end

            it("func -> state pipeline recovers after kill", function()
                local df_id = make_func_state_wf()
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "state pipeline recovered")
            end)

            it("func -> state pipeline recovers after double kill", function()
                local df_id = make_func_state_wf()
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "state pipeline recovered after double kill")
            end)
        end)

        -- ==========================================
        -- FUNC PIPELINE RECOVERY
        -- ==========================================

        describe("func pipeline recovery", function()
            local function make_func_chain(count)
                local commands = {}
                local node_ids = {}
                for i = 1, count do
                    node_ids[i] = uuid.v7()
                end

                for i = 1, count do
                    local targets = {}
                    if i < count then
                        table.insert(targets, { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_ids[i + 1], discriminator = "default" })
                    else
                        table.insert(targets, { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON })
                    end

                    local config = {
                        func_id = "userspace.dataflow.node.func:test_func",
                        data_targets = targets,
                    }
                    if i > 1 then
                        config.inputs = { required = { "default" } }
                    end

                    table.insert(commands, { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = node_ids[i],
                        node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = config,
                        metadata = { title = "Func " .. i }
                    }})
                end

                table.insert(commands, { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_ids[1],
                    content = { message = "chain", delay_ms = 50, should_fail = false },
                    content_type = consts.CONTENT_TYPE.JSON, key = "default"
                }})

                return c:create_workflow(commands)
            end

            it("3-func chain recovers after kill", function()
                local df_id = make_func_chain(3)
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                test.is_true(wait_complete(df_id, 10000), "3-func chain recovered")
            end)

            it("5-func chain recovers after kill at various points", function()
                local df_id = make_func_chain(5)
                c:start(df_id)
                time.sleep("150ms")
                kill_orchestrator(df_id)

                c:start(df_id)
                test.is_true(wait_complete(df_id, 15000), "5-func chain recovered")
            end)
        end)

        -- ==========================================
        -- SIGNAL + FUNC COMPOUND RECOVERY
        -- ==========================================

        describe("compound signal+func recovery", function()
            it("central wakes recover repeated shutdown-to-signal handoffs", function()
                for iteration = 1, 10 do
                    local sid = "cf1-" .. uuid.v7()
                    local df_id = make_func_signal_func_wf(sid)
                    c:start(df_id)
                    -- kill early while func might still be running
                    time.sleep("50ms")
                    kill_orchestrator(df_id)

                    c:start(df_id)
                    test.is_true(wait_running(df_id), "recovered, waiting at signal: " .. iteration)

                    local signal_result, signal_err = c:signal(
                        df_id,
                        sid,
                        { message = "go", delay_ms = 10, should_fail = false }
                    )
                    test.is_nil(signal_err, "central wake signal accepted: " .. iteration)
                    test.not_nil(signal_result, "signal commit persisted: " .. iteration)
                    local completed = wait_complete(df_id, 5000)
                    test.is_true(
                        completed,
                        "central wake completed: " .. iteration .. "; " ..
                        (completed and "ok" or failure_diagnostics(df_id))
                    )
                end
            end)

            it("pre-queue signal, kill, restart pipeline", function()
                local sid = "cf2-" .. uuid.v7()
                local df_id = make_func_signal_func_wf(sid)

                -- pre-queue signal before start
                c:signal(df_id, sid, { message = "early", delay_ms = 10, should_fail = false })
                c:start(df_id)
                time.sleep("200ms")

                kill_orchestrator(df_id)
                c:start(df_id)

                test.is_true(wait_complete(df_id, 10000), "pre-queued signal + kill recovery works")
            end)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
