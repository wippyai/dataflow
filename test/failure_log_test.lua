local test = require("test")
local uuid = require("uuid")
local logger = require("logger")
local client = require("client")
local consts = require("consts")
local node_reader = require("node_reader")
local commit = require("commit")
local json = require("json")

local function define_tests()
    describe("Failure log reporter", function()
        for _, handled in ipairs({false, true}) do
            it(handled and "logs a handled node failure in a successful workflow" or
                "logs a failed node and an unsuccessful workflow", function()
                local c = test.not_nil(client.new()) :: any
                local node_id = uuid.v7()
                local handler_id = uuid.v7()
                local commands = {
                    { type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = node_id, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            func_id = "userspace.dataflow.node.func:native_error_func",
                            error_targets = handled and {{data_type = consts.DATA_TYPE.NODE_INPUT,
                                node_id = handler_id, key = "default"}} or nil,
                        },
                        metadata = {error_observed = handled},
                    } },
                    { type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                        data_id = uuid.v7(), data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id, key = "default", content = {
                            kind = "Unavailable", message = "Service unavailable",
                            details = { credential = "private-error-detail" },
                        },
                    } },
                }
                if handled then
                    table.insert(commands, {type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                        node_id = handler_id, node_type = "userspace.dataflow.node.func:node",
                        status = consts.STATUS.PENDING,
                        config = {func_id = "userspace.dataflow.node.func:test_func",
                            data_targets = {{data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT}}},
                    }})
                end
                local dataflow_id, create_err = c:create_workflow(commands)
                test.is_nil(create_err)
                local result, execute_err = c:execute(dataflow_id)
                if handled then test.is_nil(execute_err) else test.not_nil(execute_err) end
                test.eq(result.success, handled)
                test.eq(c:get_status(dataflow_id), handled and consts.STATUS.COMPLETED_SUCCESS or consts.STATUS.COMPLETED_FAILURE)
                local node = node_reader.with_dataflow(dataflow_id):with_nodes(node_id):one()
                test.eq(node.status, consts.STATUS.COMPLETED_FAILURE)
                logger:info("failure log probe", { dataflow_id = dataflow_id, node_id = node_id, workflow_failed = not handled,
                    node_type = "userspace.dataflow.node.func:node", error_kind = "FUNCTION_EXECUTION_FAILED" })
            end)
        end
        it("logs a failed tool call consumed by a successful agent", function()
            local c = test.not_nil(client.new()) :: any
            local node_id = uuid.v7()
            local dataflow_id, create_err = c:create_workflow({
                {type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = node_id, node_type = "userspace.dataflow.node.agent:node",
                    status = consts.STATUS.PENDING,
                    config = {
                        agent = "userspace.dataflow.node.agent.stub:recovery_test_agent",
                        arena = {prompt = "Run the tool.", max_iterations = 4,
                            tools = {"userspace.dataflow.node.agent.stub:recovery_tool"}},
                        data_targets = {{data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT}},
                    },
                }},
                {type = consts.COMMAND_TYPES.CREATE_DATA, payload = {
                    data_id = uuid.v7(), node_id = node_id, key = "default",
                    data_type = consts.DATA_TYPE.NODE_INPUT,
                    content = {scenario_id = uuid.v7(), mode = "failing_tool_then_final",
                        fail_message = "Service unavailable"},
                }},
            })
            test.is_nil(create_err)
            local result, execute_err = c:execute(dataflow_id)
            test.is_nil(execute_err)
            test.is_true(result.success)
            local nodes = node_reader.with_dataflow(dataflow_id):with_node_types("tool.call"):all()
            test.eq(#nodes, 1)
            test.eq(nodes[1].status, consts.STATUS.COMPLETED_FAILURE)
            local _, replay_err = commit.execute(dataflow_id, uuid.v7(), {
                {type = consts.COMMAND_TYPES.UPDATE_NODE, payload = {
                    node_id = nodes[1].node_id, status = consts.STATUS.COMPLETED_FAILURE,
                }},
            })
            test.is_nil(replay_err)
            logger:info("failure log probe", {dataflow_id = dataflow_id, node_id = nodes[1].node_id,
                workflow_failed = false, node_type = "tool.call", error_kind = "NodeFailed"})
        end)
        it("logs scalar defaults when structured errors carry no message", function()
            local c = test.not_nil(client.new()) :: any
            local node_id = uuid.v7()
            local dataflow_id, create_err = c:create_workflow({
                {type = consts.COMMAND_TYPES.CREATE_NODE, payload = {
                    node_id = node_id, node_type = "tool.call", status = consts.STATUS.RUNNING,
                }},
            })
            test.is_nil(create_err)
            local _, node_err = commit.execute(dataflow_id, uuid.v7(), {
                {type = consts.COMMAND_TYPES.UPDATE_NODE, payload = {
                    node_id = node_id, status = consts.STATUS.COMPLETED_FAILURE,
                    metadata = {error = {code = "NO_MESSAGE", details = {credential = "private-error-detail"}}},
                }},
            })
            test.is_nil(node_err)
            local activation, activation_err = commit.request_activation(dataflow_id, {}, {notify = false})
            test.is_nil(activation_err)
            local _, completion_err = commit.execute(dataflow_id, uuid.v7(), {
                {type = consts.COMMAND_TYPES.COMPLETE_WORKFLOW, payload = {
                    activation_generation = activation.generation, status = consts.STATUS.COMPLETED_FAILURE,
                    metadata = {error = json.encode({details = {credential = "private-error-detail"}})},
                }},
            })
            test.is_nil(completion_err)
            logger:info("failure log probe", {dataflow_id = dataflow_id, node_id = node_id,
                workflow_failed = true, node_type = "tool.call", error_kind = "NO_MESSAGE",
                node_message = "Node failed", workflow_message = "Workflow failed"})
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
