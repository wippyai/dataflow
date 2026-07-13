local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function define_tests()
    describe("All Nodes Recovery Tests", function()
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

        -- ==========================================
        -- CYCLE NODE RECOVERY
        -- ==========================================

        describe("cycle node recovery", function()
            local function make_cycle_wf(max_iterations, initial_quality)
                local nid = uuid.v7()
                local input_id = uuid.v7()
                local node_input_id = uuid.v7()

                return c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = nid,
                        node_type = "userspace.dataflow.node.cycle:cycle",
                        status = consts.STATUS.PENDING,
                        config = {
                            func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                            max_iterations = max_iterations or 10,
                            initial_state = {
                                refinement_count = 0,
                                quality_score = initial_quality or 0.3,
                            },
                            data_targets = {
                                { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                            },
                        },
                        metadata = { title = "Cycle" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = { target_quality = 0.8, initial_text = "Test input for refinement" },
                        content_type = consts.CONTENT_TYPE.JSON,
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = node_input_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = nid,
                        key = input_id,
                        content = "",
                        content_type = "dataflow/reference",
                    }},
                })
            end

            it("cycle completes normally without kill", function()
                local df_id = make_cycle_wf(10, 0.3)
                local result, err = c:execute(df_id)
                test.is_nil(err, "no error")
                test.not_nil(result, "has result")
                test.is_true(result.success, "success")
            end)

            it("cycle recovers after kill during iteration", function()
                local df_id = make_cycle_wf(10, 0.3)
                c:start(df_id)
                time.sleep("800ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 15000), "cycle recovered")
            end)

            it("cycle recovers after double kill", function()
                local df_id = make_cycle_wf(10, 0.3)
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 10000), "cycle double kill recovered")
            end)

            it("cycle with triple kill recovers", function()
                local df_id = make_cycle_wf(10, 0.3)
                c:start(df_id)
                time.sleep("800ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("800ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("800ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 15000), "cycle triple kill recovered")
            end)

            it("cycle with high initial quality completes fast after kill", function()
                local df_id = make_cycle_wf(10, 0.75)
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 15000), "high-quality cycle recovered")
            end)
        end)

        -- ==========================================
        -- PARALLEL NODE RECOVERY
        -- ==========================================

        describe("parallel node recovery", function()
            local function make_parallel_wf(items, batch_size)
                local nid = uuid.v7()
                local template_id = uuid.v7()
                local input_id = uuid.v7()
                local node_input_id = uuid.v7()

                return c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = nid,
                        node_type = "userspace.dataflow.node.parallel:parallel",
                        status = consts.STATUS.PENDING,
                        config = {
                            source_array_key = "items",
                            iteration_input_key = "default",
                            batch_size = batch_size or 1,
                            failure_strategy = "collect_errors",
                            data_targets = {
                                { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                            },
                        },
                        metadata = { title = "Parallel" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = template_id,
                        node_type = "userspace.dataflow.node.func:node",
                        parent_node_id = nid,
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
                        node_id = nid,
                        key = input_id,
                        content = "",
                        content_type = "dataflow/reference",
                    }},
                })
            end

            local function make_items(count, delay_ms)
                local items = {}
                for i = 1, count do
                    table.insert(items, { message = "item" .. i, delay_ms = delay_ms or 10, should_fail = false })
                end
                return items
            end

            it("parallel completes normally without kill", function()
                local df_id = make_parallel_wf(make_items(3), 1)
                local result, err = c:execute(df_id)
                test.is_nil(err, "no error")
                test.not_nil(result, "has result")
                test.is_true(result.success, "success")
            end)

            it("parallel 3 items recovers after kill", function()
                local df_id = make_parallel_wf(make_items(3, 100), 1)
                c:start(df_id)
                time.sleep("150ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 10000), "parallel 3 recovered")
            end)

            it("parallel 5 items recovers after kill", function()
                local df_id = make_parallel_wf(make_items(5, 50), 1)
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 10000), "parallel 5 recovered")
            end)

            it("parallel batch_size=3 recovers after kill", function()
                local df_id = make_parallel_wf(make_items(6, 50), 3)
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 10000), "batched parallel recovered")
            end)

            it("parallel double kill during processing", function()
                local df_id = make_parallel_wf(make_items(4, 80), 1)
                c:start(df_id)
                time.sleep("80ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                time.sleep("80ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 10000), "parallel double kill recovered")
            end)

            it("parallel with failure_strategy collect_errors survives kill", function()
                local items = {
                    { message = "ok1", delay_ms = 50, should_fail = false },
                    { message = "fail", delay_ms = 50, should_fail = true },
                    { message = "ok2", delay_ms = 50, should_fail = false },
                }
                local df_id = make_parallel_wf(items, 1)
                c:start(df_id)
                time.sleep("100ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 12000), "parallel with errors recovered")
            end)
        end)

        -- ==========================================
        -- FUNC -> CYCLE PIPELINE RECOVERY
        -- ==========================================

        describe("func -> cycle pipeline recovery", function()
            it("func feeds cycle, kill during cycle iteration", function()
                local f1 = uuid.v7()
                local cyc = uuid.v7()
                local input_id = uuid.v7()

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = f1,
                        node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            func_id = "userspace.dataflow.node.func:test_func",
                            data_targets = {
                                { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = cyc, discriminator = "default" }
                            },
                        },
                        metadata = { title = "Func 1" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = cyc,
                        node_type = "userspace.dataflow.node.cycle:cycle",
                        status = consts.STATUS.PENDING,
                        config = {
                            func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                            max_iterations = 5,
                            inputs = { required = { "default" } },
                            initial_state = { refinement_count = 0, quality_score = 0.5 },
                            data_targets = {
                                { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                            },
                        },
                        metadata = { title = "Cycle" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = f1,
                        content = { message = "start", delay_ms = 10, should_fail = false },
                        content_type = consts.CONTENT_TYPE.JSON,
                        key = "default",
                    }},
                })
                c:start(df_id)
                time.sleep("800ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 15000), "func->cycle pipeline recovered")
            end)
        end)

        -- ==========================================
        -- FUNC -> PARALLEL PIPELINE RECOVERY
        -- ==========================================

        describe("func -> parallel pipeline recovery", function()
            it("parallel with delayed items, kill during processing", function()
                local par = uuid.v7()
                local tmpl = uuid.v7()
                local input_id = uuid.v7()
                local node_input_id = uuid.v7()

                local items = {}
                for i = 1, 4 do
                    table.insert(items, { message = "item" .. i, delay_ms = 100, should_fail = false })
                end

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = par,
                        node_type = "userspace.dataflow.node.parallel:parallel",
                        status = consts.STATUS.PENDING,
                        config = {
                            source_array_key = "items",
                            batch_size = 1,
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
                        node_id = par,
                        key = input_id,
                        content = "",
                        content_type = "dataflow/reference",
                    }},
                })
                c:start(df_id)
                time.sleep("200ms")
                kill_orchestrator(df_id)
                c:start(df_id)
                test.is_true(wait_complete(df_id, 12000), "parallel with delayed items recovered")
            end)
        end)

        -- ==========================================
        -- SIGNAL -> PARALLEL COMPOUND RECOVERY
        -- ==========================================

        describe("signal -> parallel compound recovery", function()
            it("signal gates parallel execution, kill at signal, signal and recover", function()
                local sig = uuid.v7()
                local par = uuid.v7()
                local tmpl = uuid.v7()
                local sid = "sig-par-" .. uuid.v7()

                local items = {}
                for i = 1, 3 do
                    table.insert(items, { message = "item" .. i, delay_ms = 10, should_fail = false })
                end

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = sig,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            signal_id = sid,
                            data_targets = {
                                { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = par, discriminator = "default" }
                            },
                        },
                        metadata = { title = "Signal gate" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = par,
                        node_type = "userspace.dataflow.node.parallel:parallel",
                        status = consts.STATUS.PENDING,
                        config = {
                            source_array_key = "items",
                            batch_size = 1,
                            inputs = { required = { "default" } },
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
                        data_id = uuid.v7(),
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = sig,
                        content = { task = "wait for approval" },
                        content_type = consts.CONTENT_TYPE.JSON,
                        key = "default",
                    }},
                })
                c:start(df_id)
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "waiting at signal")

                kill_orchestrator(df_id)
                c:signal(df_id, sid, { items = items, approved = true })
                test.is_true(wait_complete(df_id, 12000), "signal->parallel recovered")
            end)
        end)

        -- ==========================================
        -- SIGNAL -> CYCLE COMPOUND RECOVERY
        -- ==========================================

        describe("signal -> cycle compound recovery", function()
            it("signal gates cycle execution, kill at signal, signal and recover", function()
                local sig = uuid.v7()
                local cyc = uuid.v7()
                local sid = "sig-cyc-" .. uuid.v7()

                local df_id = c:create_workflow({
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = sig,
                        node_type = "userspace.dataflow.node.signal:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            signal_id = sid,
                            data_targets = {
                                { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = cyc, discriminator = "default" }
                            },
                        },
                        metadata = { title = "Signal gate" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = cyc,
                        node_type = "userspace.dataflow.node.cycle:cycle",
                        status = consts.STATUS.PENDING,
                        config = {
                            func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                            max_iterations = 5,
                            inputs = { required = { "default" } },
                            initial_state = { refinement_count = 0, quality_score = 0.6 },
                            data_targets = {
                                { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                            },
                        },
                        metadata = { title = "Cycle" },
                    }},
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(),
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = sig,
                        content = { task = "wait" },
                        content_type = consts.CONTENT_TYPE.JSON,
                        key = "default",
                    }},
                })
                c:start(df_id)
                time.sleep("500ms")
                test.eq(c:get_status(df_id), consts.STATUS.WAITING, "waiting at signal")

                kill_orchestrator(df_id)
                c:signal(df_id, sid, { target_quality = 0.8, initial_text = "Approved text" })
                test.is_true(wait_complete(df_id, 15000), "signal->cycle recovered")
            end)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
