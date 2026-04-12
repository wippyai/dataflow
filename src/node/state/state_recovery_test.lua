local test = require("test")
local uuid = require("uuid")
local json = require("json")
local time = require("time")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function define_tests()
    describe("State Recovery Tests", function()
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
            local timeout = timeout_ms or 15000
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
            end, timeout_ms or 20000, 100)
        end

        local function wait_for_state_inputs(df_id, state_node_id, minimum_count, timeout_ms)
            return wait_until(function()
                local rows = data_reader.with_dataflow(df_id)
                    :with_nodes(state_node_id)
                    :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                    :all()
                if #rows >= minimum_count then
                    return rows
                end
                return nil
            end, timeout_ms or 12000, 100)
        end

        local function create_state_workflow(left_delay_ms, right_delay_ms)
            local left_node_id = uuid.v7()
            local right_node_id = uuid.v7()
            local state_node_id = uuid.v7()

            local dataflow_id = c:create_workflow({
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = left_node_id,
                    node_type = "userspace.dataflow.node.func:node",
                    status = consts.STATUS.PENDING,
                    config = {
                        func_id = "userspace.dataflow.node.func:test_func",
                        data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = state_node_id, discriminator = "left" }
                        }
                    },
                    metadata = { title = "State Left Branch" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = right_node_id,
                    node_type = "userspace.dataflow.node.func:node",
                    status = consts.STATUS.PENDING,
                    config = {
                        func_id = "userspace.dataflow.node.func:test_func",
                        data_targets = {
                            { data_type = consts.DATA_TYPE.NODE_INPUT, node_id = state_node_id, discriminator = "right" }
                        }
                    },
                    metadata = { title = "State Right Branch" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = state_node_id,
                    node_type = "userspace.dataflow.node.state:state",
                    status = consts.STATUS.PENDING,
                    config = {
                        inputs = {
                            required = { "left", "right" }
                        },
                        data_targets = {
                            { data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "result", content_type = consts.CONTENT_TYPE.JSON }
                        }
                    },
                    metadata = { title = "State Collector" }
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(),
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = left_node_id,
                    key = "default",
                    content = { message = "left-branch", delay_ms = left_delay_ms, should_fail = false },
                    content_type = consts.CONTENT_TYPE.JSON
                }},
                { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(),
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    node_id = right_node_id,
                    key = "default",
                    content = { message = "right-branch", delay_ms = right_delay_ms, should_fail = false },
                    content_type = consts.CONTENT_TYPE.JSON
                }}
            })

            return {
                dataflow_id = dataflow_id,
                state_node_id = state_node_id
            }
        end

        local function assert_single_state_input_per_branch(df_id, state_node_id)
            local state_inputs = data_reader.with_dataflow(df_id)
                :with_nodes(state_node_id)
                :with_data_types(consts.DATA_TYPE.NODE_INPUT)
                :all()

            local counts = {}
            for _, row in ipairs(state_inputs) do
                local discriminator = row.discriminator or row.key or "default"
                counts[discriminator] = (counts[discriminator] or 0) + 1
            end

            test.eq(counts.left, 1, "left branch input persisted once")
            test.eq(counts.right, 1, "right branch input persisted once")
        end

        local function assert_state_output(df_id)
            local output = data_reader.with_dataflow(df_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :one()

            test.not_nil(output, "workflow output exists")
            local content = output.content
            if type(content) == "string" then
                content = json.decode(content :: string)
            end
            test.eq(content.left.message, "left-branch")
            test.eq(content.right.message, "right-branch")
        end

        it("preserves the first branch input across a crash between updates", function()
            local workflow = create_state_workflow(50, 1200)

            c:start(workflow.dataflow_id)
            test.not_nil(wait_for_state_inputs(workflow.dataflow_id, workflow.state_node_id, 1, 12000),
                "first branch input persisted")

            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 20000), "state workflow recovered")
            assert_state_output(workflow.dataflow_id)
            assert_single_state_input_per_branch(workflow.dataflow_id, workflow.state_node_id)
        end)

        it("finishes aggregation after a crash during the second branch update", function()
            local workflow = create_state_workflow(100, 1500)

            c:start(workflow.dataflow_id)
            test.not_nil(wait_for_state_inputs(workflow.dataflow_id, workflow.state_node_id, 1, 12000),
                "first branch input persisted")
            time.sleep("400ms")

            kill_orchestrator(workflow.dataflow_id)
            c:start(workflow.dataflow_id)

            test.is_true(wait_complete(workflow.dataflow_id, 20000), "state workflow recovered")
            assert_state_output(workflow.dataflow_id)
            assert_single_state_input_per_branch(workflow.dataflow_id, workflow.state_node_id)
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
