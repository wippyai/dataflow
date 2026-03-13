local test = require("test")
local uuid = require("uuid")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local agent_consts = require("agent_consts")

local function define_tests()
    describe("Agent Node Truncation Handling", function()
        it("should recover from truncated LLM response and complete", function()
            local c, err = client.new()
            test.is_nil(err)
            test.not_nil(c)

            local node_id = uuid.v7()
            local input_data_id = uuid.v7()
            local node_input_id = uuid.v7()

            local workflow_commands = {
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "userspace.dataflow.node.agent:node",
                        status = consts.STATUS.PENDING,
                        config = {
                            agent = "userspace.dataflow.node.agent.stub:truncation_test_agent",
                            show_tool_calls = false,
                            arena = {
                                prompt = "Complete the task.",
                                max_iterations = 5,
                                tool_calling = "auto",
                            },
                            data_targets = {
                                {
                                    data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                                    key = "result",
                                    content_type = consts.CONTENT_TYPE.TEXT
                                }
                            }
                        },
                        metadata = {
                            title = "Truncation Recovery Test"
                        }
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = input_data_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = { task = "test truncation recovery" },
                        content_type = consts.CONTENT_TYPE.JSON
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = node_input_id,
                        data_type = consts.DATA_TYPE.NODE_INPUT,
                        node_id = node_id,
                        key = input_data_id,
                        content = "",
                        content_type = "dataflow/reference"
                    }
                }
            }

            local dataflow_id, create_err = c:create_workflow(workflow_commands, {
                metadata = { title = "Truncation Recovery Test Workflow" }
            })
            test.is_nil(create_err)
            test.not_nil(dataflow_id)

            local result, exec_err = c:execute(dataflow_id)
            test.is_nil(exec_err)
            test.not_nil(result)
            test.is_true(result.success)

            local observations = data_reader.with_dataflow(dataflow_id)
                :with_nodes(node_id)
                :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
                :all()

            local found_truncation_warning = false
            for _, obs in ipairs(observations) do
                if obs.key and string.find(obs.key, "truncation_warning") then
                    found_truncation_warning = true
                    test.not_nil(obs.metadata)
                    test.is_true(obs.metadata.truncated)
                    break
                end
            end
            test.is_true(found_truncation_warning, "truncation warning observation should be stored")

            local actions = data_reader.with_dataflow(dataflow_id)
                :with_nodes(node_id)
                :with_data_types(agent_consts.DATA_TYPE.AGENT_ACTION)
                :all()

            test.gt(#actions, 1, "should have multiple agent actions (truncated + recovery)")
        end)
    end)
end

return test.run_cases(define_tests)
