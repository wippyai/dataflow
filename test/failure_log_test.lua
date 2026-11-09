local test = require("test")
local uuid = require("uuid")
local logger = require("logger")
local client = require("client")
local consts = require("consts")
local node_reader = require("node_reader")

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
                logger:info("failure log probe", { dataflow_id = dataflow_id, node_id = node_id, workflow_failed = not handled })
            end)
        end
    end)
end

return { run_tests = test.run_cases(define_tests) }
