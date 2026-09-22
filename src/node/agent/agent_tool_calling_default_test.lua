local test = require("test")
local uuid = require("uuid")
local time = require("time")
local json = require("json")
local funcs = require("funcs")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")

local function as_table(content: any): table
    if type(content) == "table" then
        return content
    end
    if type(content) ~= "string" then
        return {}
    end
    local decoded, decode_err = json.decode(content)
    if decode_err or type(decoded) ~= "table" then
        return {}
    end
    return decoded
end

local function define_tests()
    describe("Agent Node tool_calling default", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function wait_terminal(df_id)
            for _ = 1, 250 do
                local ok, status = pcall(function()
                    return c:get_status(df_id)
                end)
                if ok and (status == consts.STATUS.COMPLETED_SUCCESS or
                        status == consts.STATUS.COMPLETED_FAILURE or
                        status == consts.STATUS.CANCELLED or
                        status == consts.STATUS.TERMINATED) then
                    return status
                end
                time.sleep("100ms")
            end
            return nil
        end

        local function metric(scenario_id, name)
            local metrics, err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_get",
                { scenario_id = scenario_id }
            )
            test.is_nil(err, "metrics fetch")
            return (metrics or {})[name]
        end

        -- A hand-built node config that declares an exit_schema and leaves
        -- tool_calling unset, as configs written outside the flow builder do.
        local function create_workflow()
            local node_id = uuid.v7()
            local input_id = uuid.v7()
            local node_input_id = uuid.v7()
            local scenario_id = "agent-tool-calling-default-" .. uuid.v7()

            local _, reset_err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_reset",
                { scenario_id = scenario_id }
            )
            test.is_nil(reset_err, "metrics reset")

            local commands = {
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "userspace.dataflow.node.agent:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            agent = "userspace.dataflow.node.agent.stub:recovery_test_agent",
                            arena = {
                                prompt = "Finish with a structured answer.",
                                max_iterations = 3,
                                exit_schema = {
                                    type = "object",
                                    properties = { answer = { type = "string" } },
                                    required = { "answer" }
                                }
                            },
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                    key = "result",
                                    content_type = consts.CONTENT_TYPE.JSON
                                }
                            }
                        },
                        metadata = { title = "Agent Tool Calling Default Test" }
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = { scenario_id = scenario_id, mode = "finish_when_offered" },
                        content_type = consts.CONTENT_TYPE.JSON
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = node_input_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id,
                        key = input_id,
                        content = "",
                        content_type = consts.CONTENT_TYPE.REFERENCE
                    }
                }
            }

            local dataflow_id, err = c:create_workflow(commands, {
                metadata = { title = "Agent Tool Calling Default Workflow" }
            })
            test.is_nil(err, "workflow created")

            return { dataflow_id = dataflow_id, node_id = node_id, scenario_id = scenario_id }
        end

        it("offers the finish tool and completes on it when an exit_schema leaves tool_calling unset", function()
            local workflow = create_workflow()

            c:start(workflow.dataflow_id)

            test.eq(wait_terminal(workflow.dataflow_id), consts.STATUS.COMPLETED_SUCCESS,
                "the node completes through the finish tool")
            test.eq(metric(workflow.scenario_id, "finish_offered"), 1,
                "the first request offers the finish tool")
            test.eq(metric(workflow.scenario_id, "llm_calls"), 1, "the finish call ends the run on the first turn")

            local outputs = data_reader.with_dataflow(workflow.dataflow_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :with_data_keys("result")
                :fetch_options({ replace_references = true })
                :all() or {}
            test.eq(#outputs, 1, "workflow output produced")
            test.eq(as_table((outputs[1] :: any).content).answer, "finished:" .. workflow.scenario_id,
                "the output is the finish tool's arguments")
        end)
    end)
end

return test.run_cases(define_tests)
