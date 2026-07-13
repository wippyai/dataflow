local test = require("test")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")

local function define_tests()
    describe("Parallel Recovery Tests", function()
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
            timeout_ms = timeout_ms or 15000
            local iterations = math.ceil(timeout_ms / 100)

            for _ = 1, iterations do
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_SUCCESS then
                    return true
                end
                if status == consts.STATUS.COMPLETED_FAILURE then
                    return false
                end
                time.sleep("100ms")
            end

            return false
        end

        local function wait_for_iteration_results(df_id, parent_node_id, expected_count, timeout_ms)
            timeout_ms = timeout_ms or 8000
            local iterations = math.ceil(timeout_ms / 50)

            for _ = 1, iterations do
                local results = data_reader.with_dataflow(df_id)
                    :with_nodes(parent_node_id)
                    :with_data_types(consts.DATA_TYPE.ITERATION_RESULT)
                    :all()

                if #results >= expected_count then
                    return true
                end

                time.sleep("50ms")
            end

            return false
        end

        local function make_items(count, delay_ms)
            local items = {}
            for i = 1, count do
                table.insert(items, {
                    message = "item-" .. i,
                    delay_ms = delay_ms or 250,
                    should_fail = false
                })
            end
            return items
        end

        local function make_parallel_wf(items, batch_size)
            local parallel_node_id = uuid.v7()
            local template_id = uuid.v7()
            local input_id = uuid.v7()
            local node_input_id = uuid.v7()

            local df_id = c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = parallel_node_id,
                    node_type = "userspace.dataflow.node.parallel:parallel",
                    status = consts.STATUS.PENDING,
                    config = {
                        source_array_key = "items",
                        iteration_input_key = "default",
                        batch_size = batch_size or 2,
                        failure_strategy = "collect_errors",
                        data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        },
                    },
                    metadata = { title = "Recovery Parallel" },
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
                    node_id = parallel_node_id,
                    key = input_id,
                    content = "",
                    content_type = consts.CONTENT_TYPE.REFERENCE,
                }},
            })

            return {
                dataflow_id = df_id,
                parallel_node_id = parallel_node_id
            }
        end

        it("resumes from the last completed batch after orchestrator restart", function()
            local workflow = make_parallel_wf(make_items(6, 250), 2)

            c:start(workflow.dataflow_id)

            test.is_true(
                wait_for_iteration_results(workflow.dataflow_id, workflow.parallel_node_id, 2, 8000),
                "first batch completed before kill"
            )

            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 15000), "parallel recovered")
            test.eq(c:get_status(workflow.dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "parallel succeeded")

            local iteration_results = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.parallel_node_id)
                :with_data_types(consts.DATA_TYPE.ITERATION_RESULT)
                :all()

            local counts_by_iteration = {}
            for _, row in ipairs(iteration_results) do
                local iteration_index = row.metadata and row.metadata.iteration
                if iteration_index ~= nil then
                    counts_by_iteration[iteration_index] = (counts_by_iteration[iteration_index] or 0) + 1
                end
            end

            test.eq(#iteration_results, 6, "one iteration result row per item")
            for iteration_index = 1, 6 do
                test.eq(counts_by_iteration[iteration_index], 1, "iteration " .. iteration_index .. " written once")
            end

            local progress_rows = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.parallel_node_id)
                :with_data_types(consts.DATA_TYPE.PARALLEL_PROGRESS)
                :all()
            test.gt(#progress_rows, 0, "parallel progress persisted")

            local outputs, output_err = c:output(workflow.dataflow_id)
            test.is_nil(output_err, "workflow output available")
            test.eq(outputs.result.success_count, 6, "all items succeeded")
            test.eq(outputs.result.failure_count, 0, "no failures")
        end)

        it("resumes an in-flight child barrier without rerunning completed items", function()
            local workflow = make_parallel_wf(make_items(4, 700), 4)
            c:start(workflow.dataflow_id)

            local children_active = false
            for _ = 1, 50 do
                local rows = (node_reader.with_dataflow(workflow.dataflow_id) :: any)
                    :with_parent_nodes(workflow.parallel_node_id)
                    :with_statuses(consts.STATUS.RUNNING)
                    :all()
                if rows and #rows > 0 then
                    children_active = true
                    break
                end
                time.sleep("50ms")
            end
            test.is_true(children_active, "iteration children active before crash")
            kill_orchestrator(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 15000), "durable child barrier recovered")
            local iteration_results = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.parallel_node_id)
                :with_data_types(consts.DATA_TYPE.ITERATION_RESULT)
                :all()
            local counts = {}
            for _, row in ipairs(iteration_results or {}) do
                local iteration = row.metadata and row.metadata.iteration
                counts[iteration] = (counts[iteration] or 0) + 1
            end
            test.eq(#iteration_results, 4, "one result per input")
            for index = 1, 4 do test.eq(counts[index], 1, "iteration did not rerun") end
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
