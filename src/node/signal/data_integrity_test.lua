local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function define_tests()
    describe("Data Integrity Tests", function()
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
            timeout_ms = timeout_ms or 8000
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

        local function get_workflow_outputs(df_id)
            return data_reader.with_dataflow(df_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :fetch_options({ replace_references = true })
                :all()
        end

        local function get_node_signals(df_id)
            return data_reader.with_dataflow(df_id)
                :with_data_types(consts.DATA_TYPE.NODE_SIGNAL)
                :all()
        end

        -- ==========================================
        -- VERIFY OUTPUT DATA AFTER RECOVERY
        -- ==========================================

        describe("output data after func recovery", function()
            it("func output is correct after kill and recovery", function()
                local nid = uuid.v7()
                local did = uuid.v7()
                local df_id = c:create_workflow({
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
                        content = { message = "integrity-check", delay_ms = 200, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id), "completed")

                local outputs = get_workflow_outputs(df_id)
                test.not_nil(outputs, "outputs exist")
                test.eq(#outputs, 1, "exactly one workflow output (no duplicates)")

                local content = outputs[1].content
                if type(content) == "string" then
                    content = json.decode(content :: string)
                end
                test.not_nil(content, "output has content")
                test.eq(content.message, "integrity-check", "input message preserved")
                test.eq(content.success, true, "func reports success")
            end)

            it("func pipeline output has exactly one result after kill", function()
                local f1 = uuid.v7()
                local f2 = uuid.v7()
                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f1,
                        node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func", data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f2, discriminator = "default" }
                        }},
                        metadata = { title = "Func 1" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f2,
                        node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func",
                            inputs = { required = { "default" } },
                            data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "Func 2" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f1,
                        content = { message = "pipeline-integrity", delay_ms = 100, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })
                c:start(df_id)
                time.sleep("80ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id), "completed")

                local outputs = get_workflow_outputs(df_id)
                test.eq(#outputs, 1, "exactly one output after pipeline recovery")
            end)
        end)

        -- ==========================================
        -- SIGNAL DATA INTEGRITY AFTER RECOVERY
        -- ==========================================

        describe("signal data integrity after recovery", function()
            it("signal data passes through correctly after kill", function()
                local sig = uuid.v7()
                local f1 = uuid.v7()
                local sid = "integrity-" .. uuid.v7()
                local signal_payload = { approved = true, reviewer = "alice", score = 95 }

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = sig,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = { signal_id = sid, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = f1, discriminator = "default" }
                        }},
                        metadata = { title = "Signal" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f1,
                        node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = { func_id = "userspace.dataflow.node.func:test_func",
                            inputs = { required = { "default" } },
                            data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "Process" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = sig,
                        content = { task = "wait" }, content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })
                c:start(df_id)
                test.is_true(wait_running(df_id), "at signal")

                kill_orchestrator(df_id)
                c:signal(df_id, sid, signal_payload)
                test.is_true(wait_complete(df_id, 10000), "completed")

                local outputs = get_workflow_outputs(df_id)
                test.eq(#outputs, 1, "exactly one output")

                -- verify the signal data flowed through to the func
                local content = outputs[1].content
                if type(content) == "string" then
                    content = json.decode(content :: string)
                end
                test.not_nil(content, "output content exists")
                -- func echoes input as input_echo
                test.not_nil(content.input_echo, "func received signal data as input")
            end)

            it("no duplicate NODE_SIGNAL records after single signal call", function()
                local sig = uuid.v7()
                local sid = "dedup-" .. uuid.v7()

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
                test.is_true(wait_running(df_id), "at signal")

                c:signal(df_id, sid, { ok = true })
                test.is_true(wait_complete(df_id), "completed")

                local signals = get_node_signals(df_id)
                test.eq(#signals, 1, "exactly one NODE_SIGNAL record")
            end)

            it("concurrent duplicate signals produce correct result", function()
                local sig = uuid.v7()
                local sid = "concurrent-" .. uuid.v7()

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
                test.is_true(wait_running(df_id), "at signal")

                -- fire 3 signals simultaneously
                c:signal(df_id, sid, { version = 1 })
                c:signal(df_id, sid, { version = 2 })
                c:signal(df_id, sid, { version = 3 })
                test.is_true(wait_complete(df_id), "completed")

                -- should have 3 NODE_SIGNAL records but only 1 workflow output
                local signals = get_node_signals(df_id)
                test.eq(#signals, 3, "3 signal records from 3 calls")

                local outputs = get_workflow_outputs(df_id)
                test.eq(#outputs, 1, "exactly one workflow output despite 3 signals")
            end)
        end)

        -- ==========================================
        -- OUTPUT DUPLICATION AFTER RECOVERY
        -- ==========================================

        describe("no output duplication after recovery", function()
            it("signal workflow output is not duplicated after kill+recovery", function()
                local sig = uuid.v7()
                local sid = "nodup-" .. uuid.v7()

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
                test.is_true(wait_running(df_id), "at signal")

                kill_orchestrator(df_id)
                c:signal(df_id, sid, { result = "unique" })
                test.is_true(wait_complete(df_id, 10000), "completed after recovery")

                local outputs = get_workflow_outputs(df_id)
                test.eq(#outputs, 1, "no duplicate outputs after recovery")
            end)

            it("func output not duplicated after double kill", function()
                local nid = uuid.v7()
                local df_id = c:create_workflow({
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
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = nid,
                        content = { message = "double-kill", delay_ms = 200, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("50ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id), "completed")

                local outputs = get_workflow_outputs(df_id)
                test.eq(#outputs, 1, "exactly one output after double kill")
            end)
        end)

        -- ==========================================
        -- CONCURRENT CLIENT OPERATIONS
        -- ==========================================

        describe("concurrent client operations", function()
            it("two signal() calls for different IDs both create records", function()
                local sig_a = uuid.v7()
                local sig_b = uuid.v7()
                local merge = uuid.v7()
                local sid_a = "conc-a-" .. uuid.v7()
                local sid_b = "conc-b-" .. uuid.v7()

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = sig_a,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = { signal_id = sid_a, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = merge, discriminator = "from_a" }
                        }},
                        metadata = { title = "Signal A" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = sig_b,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = { signal_id = sid_b, data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = merge, discriminator = "from_b" }
                        }},
                        metadata = { title = "Signal B" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = merge,
                        node_type = "userspace.dataflow.node.state:state",
                        status = consts.STATUS.PENDING,
                        config = { output_mode = "object", inputs = { required = { "from_a", "from_b" } }, data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }},
                        metadata = { title = "Merge" }
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = sig_a,
                        content = { task = "a" }, content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = sig_b,
                        content = { task = "b" }, content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }},
                })
                c:start(df_id)
                time.sleep("500ms")

                -- fire both signals simultaneously
                c:signal(df_id, sid_a, { branch = "A" })
                c:signal(df_id, sid_b, { branch = "B" })
                test.is_true(wait_complete(df_id), "completed")

                local outputs = get_workflow_outputs(df_id)
                test.eq(#outputs, 1, "exactly one merged output")

                local signals = get_node_signals(df_id)
                test.eq(#signals, 2, "exactly 2 signal records (one per signal_id)")
            end)

            it("double start does not corrupt state", function()
                local nid = uuid.v7()
                local df_id = c:create_workflow({
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
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT, node_id = nid,
                        content = { message = "double-start", delay_ms = 10, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON, key = "default"
                    }}
                })

                -- start twice in rapid succession
                c:start(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id), "completed despite double start")

                local outputs = get_workflow_outputs(df_id)
                test.eq(#outputs, 1, "exactly one output despite double start")
            end)
        end)

        -- ==========================================
        -- CYCLE DATA INTEGRITY
        -- ==========================================

        describe("cycle output integrity", function()
            it("cycle output has correct refinement data after recovery", function()
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
                        metadata = { title = "Cycle" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = { target_quality = 0.8, initial_text = "Refine this text" },
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
                time.sleep("800ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 15000), "cycle completed")

                local outputs = get_workflow_outputs(df_id)
                test.eq(#outputs, 1, "exactly one cycle output")

                local content = outputs[1].content
                if type(content) == "string" then
                    content = json.decode(content :: string)
                end
                test.not_nil(content, "output has content")
                test.not_nil(content.refined_text, "has refined text")
                test.is_true(content.refinement_complete, "refinement marked complete")
            end)
        end)

        -- ==========================================
        -- PARALLEL DATA INTEGRITY
        -- ==========================================

        describe("parallel output integrity", function()
            it("parallel processes all items and output has correct counts after kill", function()
                local par = uuid.v7()
                local tmpl = uuid.v7()
                local input_id = uuid.v7()
                local node_input_id = uuid.v7()

                local items = {}
                for i = 1, 4 do
                    table.insert(items, { message = "item" .. i, delay_ms = 80, should_fail = false })
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
                        metadata = { title = "Parallel" },
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
                time.sleep("150ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 12000), "parallel completed")

                local outputs = get_workflow_outputs(df_id)
                test.eq(#outputs, 1, "exactly one parallel output")

                local content = outputs[1].content
                if type(content) == "string" then
                    content = json.decode(content :: string)
                end
                test.not_nil(content, "output exists")
                test.eq(content.total_iterations, 4, "all 4 items processed")
                test.eq(content.success_count, 4, "all 4 succeeded")
                test.eq(content.failure_count, 0, "no failures")
            end)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
