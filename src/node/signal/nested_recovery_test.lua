local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")

local function define_tests()
    describe("Nested Recovery Tests", function()
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

        local function wait_until(predicate, timeout_ms, interval_ms)
            local timeout = timeout_ms or 20000
            local interval = interval_ms or 100
            local attempts = math.ceil(timeout / interval)

            for _ = 1, attempts do
                local ok, value = pcall(predicate)
                if ok and value then
                    return value
                end
                time.sleep(tostring(interval) .. "ms")
            end

            return nil
        end

        local function wait_complete(df_id, timeout_ms)
            return wait_until(function()
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_SUCCESS then
                    return true
                end
                if status == consts.STATUS.COMPLETED_FAILURE then
                    return false
                end
                return nil
            end, timeout_ms or 25000, 100)
        end

        local function make_items(count, delay_ms)
            local items = {}
            for index = 1, count do
                table.insert(items, {
                    message = "item-" .. tostring(index),
                    delay_ms = delay_ms,
                    should_fail = false,
                    target_quality = 0.8,
                    initial_text = "Nested item " .. tostring(index),
                    resume_delay_ms = 100
                })
            end
            return items
        end

        local function workflow_output(df_id)
            local output = data_reader.with_dataflow(df_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :one()
            if output and type(output.content) == "string" then
                local decoded = json.decode(output.content)
                if decoded then
                    output.content = decoded
                end
            end
            return output
        end

        it("recovers a parallel of cycles after a crash", function()
            local parallel_node_id = uuid.v7()
            local cycle_template_id = uuid.v7()
            local input_id = uuid.v7()

            local df_id = c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = parallel_node_id,
                    node_type = "userspace.dataflow.node.parallel:parallel",
                    status = consts.STATUS.PENDING,
                    config = {
                        source_array_key = "items",
                        iteration_input_key = "default",
                        batch_size = 2,
                        failure_strategy = "collect_errors",
                        data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }
                    },
                    metadata = { title = "Parallel Of Cycles" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = cycle_template_id,
                    node_type = "userspace.dataflow.node.cycle:cycle",
                    parent_node_id = parallel_node_id,
                    status = consts.STATUS.TEMPLATE,
                    config = {
                        func_id = "userspace.dataflow.node.cycle.stub:refine_test_func",
                        max_iterations = 6,
                        initial_state = {
                            refinement_count = 0,
                            quality_score = 0.3
                        },
                        data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_OUTPUT, key = "cycle_output", content_type = consts.CONTENT_TYPE.JSON }
                        }
                    },
                    metadata = { title = "Cycle Template" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = input_id,
                    data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                    content = { items = make_items(4, 200) },
                    content_type = consts.CONTENT_TYPE.JSON
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(),
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = parallel_node_id,
                    key = input_id,
                    content = "",
                    content_type = consts.CONTENT_TYPE.REFERENCE
                }}
            })

            c:start(df_id)
            test.not_nil(wait_until(function()
                local rows = data_reader.with_dataflow(df_id)
                    :with_data_types("cycle.state")
                    :all()
                if #rows > 0 then
                    return rows
                end
                return nil
            end, 15000, 100), "nested cycle state persisted")

            kill_orchestrator(df_id)
            c:start(df_id)

            test.is_true(wait_complete(df_id, 25000), "parallel of cycles recovered")
            local output = workflow_output(df_id)
            test.not_nil(output, "workflow output exists")
            local content = (output :: any).content
            test.eq(content.success_count, 4)
            test.eq(content.failure_count, 0)
        end)

        it("recovers a cycle whose iterations yield through signals", function()
            local cycle_node_id = uuid.v7()
            local signal_prefix = "nested-cycle-signal-" .. uuid.v7()

            local df_id = c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = cycle_node_id,
                    node_type = "userspace.dataflow.node.cycle:cycle",
                    status = consts.STATUS.PENDING,
                    config = {
                        func_id = "userspace.dataflow.node.cycle.stub:signal_wait_test_func",
                        max_iterations = 5,
                        data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }
                    },
                    metadata = { title = "Cycle Of Signals" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(),
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = cycle_node_id,
                    key = "default",
                    content = {
                        signal_prefix = signal_prefix,
                        required_iterations = 2
                    },
                    content_type = consts.CONTENT_TYPE.JSON
                }}
            })

            c:start(df_id)
            test.not_nil(wait_until(function()
                local reader = (node_reader.with_dataflow(df_id) :: any)
                local rows = reader:with_parent_nodes(cycle_node_id)
                    :with_node_types("userspace.dataflow.node.signal:node")
                    :all()
                if rows and #rows >= 1 then
                    return rows
                end
                return nil
            end, 15000, 100), "first nested signal created")

            kill_orchestrator(df_id)
            c:start(df_id)
            test.not_nil(wait_until(function()
                local status = c:get_status(df_id)
                if status == consts.STATUS.WAITING or status == consts.STATUS.RUNNING then
                    return true
                end
                return nil
            end, 10000, 100), "workflow is recoverable before first nested signal")
            c:signal(df_id, signal_prefix .. "-1", { step = 1, ok = true })

            test.not_nil(wait_until(function()
                local reader = (node_reader.with_dataflow(df_id) :: any)
                local rows = reader:with_parent_nodes(cycle_node_id)
                    :with_node_types("userspace.dataflow.node.signal:node")
                    :all()
                if rows and #rows >= 2 then
                    return rows
                end
                return nil
            end, 15000, 100), "second nested signal created after recovery")

            c:signal(df_id, signal_prefix .. "-2", { step = 2, ok = true })

            test.is_true(wait_complete(df_id, 25000), "cycle of signals recovered")
            local output = workflow_output(df_id)
            test.not_nil(output, "workflow output exists")
            local content = (output :: any).content
            test.eq(content.total, 2)
            test.eq(content.received[1].step, 1)
            test.eq(content.received[2].step, 2)
        end)

        it("recovers a signal that fans out into parallel work", function()
            local signal_node_id = uuid.v7()
            local parallel_node_id = uuid.v7()
            local template_id = uuid.v7()
            local signal_id = "nested-signal-parallel-" .. uuid.v7()

            local df_id = c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = signal_node_id,
                    node_type = "userspace.dataflow.node.signal:node",
                    status = consts.STATUS.PENDING,
                    config = {
                        signal_id = signal_id,
                        data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = parallel_node_id, discriminator = "default" }
                        }
                    },
                    metadata = { title = "Signal Root" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(),
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = signal_node_id,
                    key = "default",
                    content = { request = "start-parallel" },
                    content_type = consts.CONTENT_TYPE.JSON
                }},
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = parallel_node_id,
                    node_type = "userspace.dataflow.node.parallel:parallel",
                    status = consts.STATUS.PENDING,
                    config = {
                        source_array_key = "items",
                        iteration_input_key = "default",
                        batch_size = 2,
                        failure_strategy = "collect_errors",
                        inputs = { required = { "default" } },
                        data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }
                    },
                    metadata = { title = "Parallel After Signal" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = template_id,
                    node_type = "userspace.dataflow.node.func:node",
                    parent_node_id = parallel_node_id,
                    status = consts.STATUS.TEMPLATE,
                    config = {
                        func_id = "userspace.dataflow.node.func:test_func",
                        data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_OUTPUT, key = "processed", content_type = consts.CONTENT_TYPE.JSON }
                        }
                    },
                    metadata = { title = "Parallel Template" }
                }}
            })

            c:start(df_id)
            test.not_nil(wait_until(function()
                if c:get_status(df_id) == consts.STATUS.WAITING then
                    return true
                end
                return nil
            end, 10000, 100), "signal root is waiting")
            c:signal(df_id, signal_id, { items = make_items(4, 300) })

            test.not_nil(wait_until(function()
                local rows = data_reader.with_dataflow(df_id)
                    :with_nodes(parallel_node_id)
                    :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                    :all()
                if #rows >= 1 then
                    return rows
                end
                return nil
            end, 15000, 100), "parallel received the signal payload")

            test.not_nil(wait_until(function()
                local rows = data_reader.with_dataflow(df_id)
                    :with_nodes(parallel_node_id)
                    :with_data_types(consts.DATA_TYPE.ITERATION_RESULT)
                    :all()
                if #rows >= 1 then
                    return rows
                end
                return nil
            end, 15000, 100), "parallel work started after signal")

            kill_orchestrator(df_id)
            c:start(df_id)

            test.is_true(wait_complete(df_id, 25000), "signal of parallel recovered")
            local output = workflow_output(df_id)
            test.not_nil(output, "workflow output exists")
            local content = (output :: any).content
            test.eq(content.success_count, 4)
            test.eq(content.failure_count, 0)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
