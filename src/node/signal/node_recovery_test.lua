local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function define_tests()
    describe("Node Recovery Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

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
        -- FUNC NODE RECOVERY
        -- ==========================================

        describe("func node recovery", function()
            local function make_func_wf(delay_ms, should_fail)
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
                        metadata = { title = "Func" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = did, data_type = consts.DATA_TYPE.NODE_INPUT, node_id = nid,
                        content = { message = "test", delay_ms = delay_ms or 10, should_fail = should_fail or false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })
            end

            it("recovers after kill during execution", function()
                local df_id = make_func_wf(300)
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "func recovered")
            end)

            it("recovers after immediate kill", function()
                local df_id = make_func_wf(10)
                c:start(df_id)
                time.sleep("20ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "recovered from immediate kill")
            end)

            it("preserves failure semantics after recovery", function()
                local df_id = make_func_wf(10, true)
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("2s")
                local status = c:get_status(df_id)
                test.neq(status, consts.STATUS.COMPLETED_SUCCESS, "failure preserved after recovery")
            end)

            it("double kill during func", function()
                local df_id = make_func_wf(200)
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "double kill func recovered")
            end)
        end)

        -- ==========================================
        -- FUNC PIPELINE RECOVERY
        -- ==========================================

        describe("func pipeline recovery", function()
            local function make_func_chain(count, delay_ms)
                local commands = {}
                local node_ids = {}
                for i = 1, count do node_ids[i] = uuid.v7() end

                for i = 1, count do
                    local targets = {}
                    if i < count then
                        table.insert(targets, { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_ids[i + 1], discriminator = "default" })
                    else
                        table.insert(targets, { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON })
                    end
                    local config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = targets }
                    if i > 1 then config.inputs = { required = { "default" } } end

                    table.insert(commands, { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = node_ids[i], node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING, config = config,
                        metadata = { title = "Func " .. i }
                    }})
                end

                table.insert(commands, { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = node_ids[1],
                    content = { message = "chain", delay_ms = delay_ms or 50, should_fail = false },
                    content_type = consts.CONTENT_TYPE.JSON, key = "default"
                }})

                return c:create_workflow(commands)
            end

            it("3-func chain recovers mid-pipeline", function()
                local df_id = make_func_chain(3, 100)
                c:start(df_id)
                time.sleep("150ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "3-chain recovered")
            end)

            it("5-func chain recovers after kill", function()
                local df_id = make_func_chain(5, 50)
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 10000), "5-chain recovered")
            end)

            it("3-func chain recovers after triple kill", function()
                local df_id = make_func_chain(3, 80)
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
                test.is_true(wait_complete(df_id, 10000), "triple kill 3-chain recovered")
            end)
        end)

        -- ==========================================
        -- STATE NODE RECOVERY
        -- ==========================================

        describe("state node recovery", function()
            local function make_fan_in_wf(branch_count)
                local commands = {}
                local merge = uuid.v7()
                local func_ids = {}

                for i = 1, branch_count do
                    local fid = uuid.v7()
                    func_ids[i] = fid
                    local key = "from_" .. i
                    table.insert(commands, { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = fid, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = merge, discriminator = key }
                        }},
                        metadata = { title = "Func " .. i }
                    }})
                end

                local required = {}
                for i = 1, branch_count do table.insert(required, "from_" .. i) end

                table.insert(commands, { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = merge, node_type = "userspace.dataflow.node.state:state",
                    status = consts.STATUS.PENDING,
                    config = { output_mode = "object", inputs = { required = required }, data_targets = {
                        { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                    }},
                    metadata = { title = "Merge" }
                }})

                for i = 1, branch_count do
                    table.insert(commands, { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = func_ids[i],
                        content = { message = "branch" .. i, delay_ms = 10, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }})
                end

                return c:create_workflow(commands)
            end

            it("2-branch fan-in recovers after kill", function()
                local df_id = make_fan_in_wf(2)
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "2-branch recovered")
            end)

            it("3-branch fan-in recovers after kill", function()
                local df_id = make_fan_in_wf(3)
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "3-branch recovered")
            end)

            it("3-branch fan-in double kill", function()
                local df_id = make_fan_in_wf(3)
                c:start(df_id)
                time.sleep("30ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("30ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "3-branch double kill recovered")
            end)
        end)

        -- ==========================================
        -- MIXED NODE TYPE RECOVERY
        -- ==========================================

        describe("mixed node type recovery", function()
            it("func -> state -> func pipeline recovers", function()
                local f1a = uuid.v7()
                local f1b = uuid.v7()
                local merge = uuid.v7()
                local f2 = uuid.v7()

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f1a, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = merge, discriminator = "a" }
                        }},
                        metadata = { title = "Func A" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f1b, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = merge, discriminator = "b" }
                        }},
                        metadata = { title = "Func B" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = merge, node_type = "userspace.dataflow.node.state:state",
                        status = consts.STATUS.PENDING,
                        config = { output_mode = "object", inputs = { required = { "a", "b" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f2, discriminator = "default" }
                        }},
                        metadata = { title = "Merge" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f2, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func",
                            inputs = { required = { "default" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "Final" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f1a,
                        content = { message = "a", delay_ms = 10, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f1b,
                        content = { message = "b", delay_ms = 10, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }},
                })
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 8000), "func->state->func recovered")
            end)

            it("signal in mixed pipeline: func -> signal -> state -> func", function()
                local f1 = uuid.v7()
                local sig = uuid.v7()
                local f2 = uuid.v7()
                local merge = uuid.v7()
                local f3 = uuid.v7()
                local sid = "mixed-" .. uuid.v7()

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f1, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = sig, discriminator = "default" },
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f2, discriminator = "default" },
                        }},
                        metadata = { title = "Func 1" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = sig, node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = { signal_id = sid, inputs = { required = { "default" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = merge, discriminator = "from_sig" }
                        }},
                        metadata = { title = "Signal" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f2, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func",
                            inputs = { required = { "default" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = merge, discriminator = "from_func" }
                        }},
                        metadata = { title = "Func 2" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = merge, node_type = "userspace.dataflow.node.state:state",
                        status = consts.STATUS.PENDING,
                        config = { output_mode = "object", inputs = { required = { "from_sig", "from_func" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f3, discriminator = "default" }
                        }},
                        metadata = { title = "Merge" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f3, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func",
                            inputs = { required = { "default" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "Final" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f1,
                        content = { message = "start", delay_ms = 10, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }},
                })
                c:start(df_id)
                test.is_true(wait_running(df_id), "waiting for signal")

                -- kill at signal, recover with signal
                kill_orchestrator(df_id)
                c:signal(df_id, sid, { approved = true })
                test.is_true(wait_complete(df_id, 10000), "mixed pipeline recovered")
            end)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
