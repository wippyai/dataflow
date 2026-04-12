local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function define_tests()
    describe("Chaos Tests", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function kill_orchestrator(df_id)
            local pid = process.registry.lookup("dataflow." .. df_id)
            if pid then
                process.terminate(pid)
                time.sleep("100ms")
            end
        end

        local function is_terminal(df_id)
            local status = c:get_status(df_id)
            return status == consts.STATUS.COMPLETED_SUCCESS
                or status == consts.STATUS.COMPLETED_FAILURE
                or status == consts.STATUS.TERMINATED
                or status == consts.STATUS.CANCELLED
        end

        local function get_output_count(df_id)
            local outputs = data_reader.with_dataflow(df_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :all()
            return #outputs
        end

        -- ==========================================
        -- CHAOS: FUNC NODE
        -- ==========================================

        describe("chaos func", function()
            it("func completes after 5 random kills", function()
                local nid = uuid.v7()
                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = nid,
                        node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "Chaos func" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = nid,
                        content = { message = "chaos", delay_ms = 100, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })

                c:start(df_id)
                for i = 1, 5 do
                    time.sleep(tostring(30 + (i * 20)) .. "ms")
                    if is_terminal(df_id) then break end
                    kill_orchestrator(df_id)
                    c:start(df_id)
                end

                -- give it time to finish
                time.sleep("2s")
                if not is_terminal(df_id) then
                    -- one final restart
                    kill_orchestrator(df_id)
                    c:start(df_id)
                    time.sleep("2s")
                end

                test.is_true(is_terminal(df_id), "func reached terminal state after chaos")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "func succeeded")
                test.eq(get_output_count(df_id), 1, "exactly one output")
            end)

            it("3-func chain completes after random kills", function()
                local n1 = uuid.v7()
                local n2 = uuid.v7()
                local n3 = uuid.v7()
                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = n1, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = n2, discriminator = "default" }
                        }},
                        metadata = { title = "F1" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = n2, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func",
                            inputs = { required = { "default" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = n3, discriminator = "default" }
                        }},
                        metadata = { title = "F2" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = n3, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func",
                            inputs = { required = { "default" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "F3" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = n1,
                        content = { message = "chain-chaos", delay_ms = 80, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })

                c:start(df_id)
                for i = 1, 7 do
                    time.sleep(tostring(40 + (i * 15)) .. "ms")
                    if is_terminal(df_id) then break end
                    kill_orchestrator(df_id)
                    c:start(df_id)
                end

                time.sleep("3s")
                if not is_terminal(df_id) then
                    c:start(df_id)
                    time.sleep("3s")
                end

                test.is_true(is_terminal(df_id), "chain reached terminal state")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "chain succeeded")
                test.eq(get_output_count(df_id), 1, "exactly one output")
            end)
        end)

        -- ==========================================
        -- CHAOS: SIGNAL NODE
        -- ==========================================

        describe("chaos signal", function()
            it("signal workflow completes after random kills then signal", function()
                local sig = uuid.v7()
                local sid = "chaos-sig-" .. uuid.v7()
                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = sig,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = { signal_id = sid, data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "Signal" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = sig,
                        content = { task = "chaos" }, content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })

                c:start(df_id)
                -- kill 5 times before sending signal
                for i = 1, 5 do
                    time.sleep("100ms")
                    if is_terminal(df_id) then break end
                    kill_orchestrator(df_id)
                    c:start(df_id)
                end

                -- now send signal (auto-respawns if dead)
                c:signal(df_id, sid, { survived_chaos = true })
                time.sleep("3s")

                test.is_true(is_terminal(df_id), "signal reached terminal state")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "signal succeeded")
                test.eq(get_output_count(df_id), 1, "exactly one output")
            end)

            it("signal sent mid-chaos completes", function()
                local sig = uuid.v7()
                local sid = "mid-chaos-" .. uuid.v7()
                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = sig,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = { signal_id = sid, data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "Signal" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = sig,
                        content = { task = "test" }, content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })

                c:start(df_id)
                time.sleep("200ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("100ms")

                -- signal while orchestrator might be mid-restart
                c:signal(df_id, sid, { ok = true })
                kill_orchestrator(df_id)

                -- signal already in outbox, restart should pick it up
                c:start(df_id)
                time.sleep("3s")

                test.is_true(is_terminal(df_id), "reached terminal")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "succeeded")
                test.eq(get_output_count(df_id), 1, "one output")
            end)
        end)

        -- ==========================================
        -- CHAOS: FUNC -> SIGNAL -> FUNC PIPELINE
        -- ==========================================

        describe("chaos pipeline", function()
            it("func->signal->func survives 8 random kills", function()
                local f1 = uuid.v7()
                local sig = uuid.v7()
                local f2 = uuid.v7()
                local sid = "chaos-pipe-" .. uuid.v7()

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f1, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = sig, discriminator = "default" }
                        }},
                        metadata = { title = "F1" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = sig, node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = { signal_id = sid, inputs = { required = { "default" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f2, discriminator = "default" }
                        }},
                        metadata = { title = "Signal" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f2, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func",
                            inputs = { required = { "default" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "F2" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f1,
                        content = { message = "chaos-pipeline", delay_ms = 50, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })

                c:start(df_id)

                -- chaos: kill 4 times before signal
                for i = 1, 4 do
                    time.sleep(tostring(50 + (i * 30)) .. "ms")
                    if is_terminal(df_id) then break end
                    kill_orchestrator(df_id)
                    c:start(df_id)
                end

                -- send signal
                c:signal(df_id, sid, { message = "go", delay_ms = 10, should_fail = false })

                -- chaos: kill 4 more times after signal
                for i = 1, 4 do
                    time.sleep(tostring(50 + (i * 30)) .. "ms")
                    if is_terminal(df_id) then break end
                    kill_orchestrator(df_id)
                    c:start(df_id)
                end

                time.sleep("3s")
                if not is_terminal(df_id) then
                    c:start(df_id)
                    time.sleep("3s")
                end

                test.is_true(is_terminal(df_id), "pipeline reached terminal")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "pipeline succeeded")
                test.eq(get_output_count(df_id), 1, "one output")
            end)
        end)

        -- ==========================================
        -- CHAOS: CYCLE NODE
        -- ==========================================

        describe("chaos cycle", function()
            it("cycle survives 5 random kills during iteration", function()
                local nid = uuid.v7()
                local input_id = uuid.v7()
                local node_input_id = uuid.v7()

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = nid,
                        node_type = "userspace.dataflow.node.cycle:cycle",
                        status = consts.STATUS.PENDING,
                        config = {
                            func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                            max_iterations = 10,
                            initial_state = { refinement_count = 0, quality_score = 0.3 },
                            data_targets = {
                                { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                            },
                        },
                        metadata = { title = "Chaos cycle" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = { target_quality = 0.8, initial_text = "Chaos refine" },
                        content_type = consts.CONTENT_TYPE.JSON,
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = node_input_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = nid, key = input_id,
                        content = "", content_type = "dataflow/reference",
                    }},
                })

                c:start(df_id)
                for i = 1, 5 do
                    time.sleep(tostring(100 + (i * 40)) .. "ms")
                    if is_terminal(df_id) then break end
                    kill_orchestrator(df_id)
                    c:start(df_id)
                end

                time.sleep("3s")
                if not is_terminal(df_id) then
                    c:start(df_id)
                    time.sleep("3s")
                end

                test.is_true(is_terminal(df_id), "cycle reached terminal")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "cycle succeeded")
                test.eq(get_output_count(df_id), 1, "one output")
            end)
        end)

        -- ==========================================
        -- CHAOS: PARALLEL NODE
        -- ==========================================

        describe("chaos parallel", function()
            it("parallel survives 5 random kills during batch processing", function()
                local par = uuid.v7()
                local tmpl = uuid.v7()
                local input_id = uuid.v7()
                local node_input_id = uuid.v7()

                local items = {}
                for i = 1, 5 do
                    table.insert(items, { message = "chaos" .. i, delay_ms = 80, should_fail = false })
                end

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = par,
                        node_type = "userspace.dataflow.node.parallel:parallel",
                        status = consts.STATUS.PENDING,
                        config = {
                            source_array_key = "items", batch_size = 1,
                            failure_strategy = "collect_errors",
                            data_targets = {
                                { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                            },
                        },
                        metadata = { title = "Chaos parallel" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = tmpl,
                        node_type = "userspace.dataflow.node.func:node",
                        parent_node_id = par,
                        status = consts.STATUS.TEMPLATE,
                        config = {
                            func_id = "userspace.dataflow.node.func:test_func",
                            data_targets = {
                                { data_type = consts.DATA_TYPE.NODE_OUTPUT, key = "processed" }
                            },
                        },
                        metadata = { title = "Template" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = { items = items },
                        content_type = consts.CONTENT_TYPE.JSON,
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = node_input_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = par, key = input_id,
                        content = "", content_type = "dataflow/reference",
                    }},
                })

                c:start(df_id)
                for i = 1, 5 do
                    time.sleep(tostring(80 + (i * 30)) .. "ms")
                    if is_terminal(df_id) then break end
                    kill_orchestrator(df_id)
                    c:start(df_id)
                end

                time.sleep("5s")
                if not is_terminal(df_id) then
                    c:start(df_id)
                    time.sleep("5s")
                end
                if not is_terminal(df_id) then
                    c:start(df_id)
                    time.sleep("5s")
                end

                test.is_true(is_terminal(df_id), "parallel reached terminal")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "parallel succeeded")
                test.eq(get_output_count(df_id), 1, "one output")
            end)
        end)

        -- ==========================================
        -- CHAOS: DIAMOND TOPOLOGY
        -- ==========================================

        describe("chaos diamond", function()
            it("func -> (signal + func) -> state survives kills", function()
                local root = uuid.v7()
                local sig = uuid.v7()
                local fb = uuid.v7()
                local merge = uuid.v7()
                local sid = "chaos-diamond-" .. uuid.v7()

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = root, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = sig, discriminator = "default" },
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = fb, discriminator = "default" },
                        }},
                        metadata = { title = "Root" }
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
                        node_id = fb, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func",
                            inputs = { required = { "default" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = merge, discriminator = "from_func" }
                        }},
                        metadata = { title = "Func branch" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = merge, node_type = "userspace.dataflow.node.state:state",
                        status = consts.STATUS.PENDING,
                        config = { output_mode = "object", inputs = { required = { "from_sig", "from_func" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "Merge" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = root,
                        content = { message = "chaos-diamond", delay_ms = 10, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })

                c:start(df_id)
                for i = 1, 3 do
                    time.sleep(tostring(80 + (i * 40)) .. "ms")
                    if is_terminal(df_id) then break end
                    kill_orchestrator(df_id)
                    c:start(df_id)
                end

                c:signal(df_id, sid, { approved = true })

                for i = 1, 3 do
                    time.sleep(tostring(50 + (i * 30)) .. "ms")
                    if is_terminal(df_id) then break end
                    kill_orchestrator(df_id)
                    c:start(df_id)
                end

                time.sleep("3s")
                if not is_terminal(df_id) then
                    c:start(df_id)
                    time.sleep("3s")
                end

                test.is_true(is_terminal(df_id), "diamond reached terminal")
                test.eq(c:get_status(df_id), consts.STATUS.COMPLETED_SUCCESS, "diamond succeeded")
                test.eq(get_output_count(df_id), 1, "one output")
            end)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
