local test = require("test")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local consts = require("consts")
local agent_consts = require("agent_consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")
local dataflow_repo = require("dataflow_repo")

local function define_tests()
    describe("Agent Tool Failure Aggregate Status", function()
        local c

        before_all(function()
            c = client.new()
            test.not_nil(c, "client created")
        end)

        local function wait_until(predicate, timeout_ms, interval_ms)
            local timeout = timeout_ms or 25000
            local interval = interval_ms or 100
            local attempts = math.ceil(timeout / interval)

            for _ = 1, attempts do
                local ok, value = pcall(predicate)
                if ok and value ~= nil then
                    return value
                end
                time.sleep(tostring(interval) .. "ms")
            end

            return nil
        end

        local function wait_terminal(df_id, timeout_ms)
            return wait_until(function()
                local status = c:get_status(df_id)
                if status == consts.STATUS.COMPLETED_SUCCESS or
                    status == consts.STATUS.COMPLETED_FAILURE or
                    status == consts.STATUS.CANCELLED or
                    status == consts.STATUS.TERMINATED then
                    return status
                end
                return nil
            end, timeout_ms or 25000, 100)
        end

        local function create_failing_tool_workflow(fail_message, mode)
            local node_id = uuid.v7()
            local input_id = uuid.v7()
            local node_input_id = uuid.v7()
            local scenario_id = "agent-tool-failure-" .. uuid.v7()

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
                                prompt = "Execute the failing tool scenario.",
                                max_iterations = 4,
                                tool_calling = "auto",
                                tools = {
                                    "userspace.dataflow.node.agent.stub:recovery_tool"
                                }
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
                            title = "Agent Tool Failure Test"
                        }
                    }
                },
                {
                    type = consts.COMMAND_TYPES.CREATE_DATA,
                    payload = {
                        data_id = input_id,
                        data_type = consts.DATA_TYPE.WORKFLOW_INPUT,
                        content = {
                            scenario_id = scenario_id,
                            mode = mode or "failing_tool_then_final",
                            fail_message = fail_message
                        },
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
                metadata = { title = "Agent Tool Failure Test Workflow" }
            })
            test.is_nil(err, "workflow created")

            return {
                dataflow_id = dataflow_id,
                node_id = node_id,
                scenario_id = scenario_id
            }
        end

        it("keeps the aggregate status derived from the agent outcome when a tool call fails", function()
            local fail_message = "Page returned status 403"
            local workflow = create_failing_tool_workflow(fail_message)

            c:start(workflow.dataflow_id)

            local final_status = wait_terminal(workflow.dataflow_id)
            test.not_nil(final_status, "workflow reached a terminal status")

            -- The tool error is delivered to the agent as an observation.
            local observations = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
                :all() or {}
            local error_observation = nil
            for _, row in ipairs(observations) do
                if row.metadata and row.metadata.is_error == true then
                    error_observation = row
                end
            end
            test.not_nil(error_observation, "tool error observation recorded for the agent")

            -- The tool.call child keeps per-node error visibility.
            local tool_nodes = node_reader.with_dataflow(workflow.dataflow_id)
                :with_node_types("tool.call")
                :all() or {}
            test.eq(#tool_nodes, 1, "one tool.call child node created")
            test.eq(tool_nodes[1].status, consts.STATUS.COMPLETED_FAILURE, "tool.call child records the failure")
            test.is_true(tool_nodes[1].metadata.has_error == true, "tool.call child carries has_error metadata")

            -- The agent consumed the error and finished its run.
            local agent_result = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                :one()
            test.not_nil(agent_result, "agent node produced a result")
            test.eq(agent_result.discriminator, "result.success", "agent completed successfully after observing the error")

            -- The engine keeps driving the agent past the tool-child error: the
            -- agent node reaches its own terminal status instead of staying a
            -- zombie 'running' row.
            local agent_nodes = node_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :all() or {}
            test.eq(#agent_nodes, 1, "agent node row present")
            test.eq(agent_nodes[1].status, consts.STATUS.COMPLETED_SUCCESS,
                "agent node is driven to completion after the tool-child error")

            -- The terminal aggregate is backed by a true terminal outcome.
            local output = data_reader.with_dataflow(workflow.dataflow_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :one()
            test.not_nil(output, "workflow output produced by the agent terminal outcome")

            -- A handled tool failure must not flip the dataflow aggregate.
            test.eq(final_status, consts.STATUS.COMPLETED_SUCCESS,
                "aggregate status derives from the agent terminal outcome, not the failed tool child")
        end)

        it("attributes an unhandled agent failure to the agent, not the consumed tool child", function()
            local workflow = create_failing_tool_workflow("Page returned status 403", "failing_tool_then_llm_error")

            c:start(workflow.dataflow_id)

            local final_status = wait_terminal(workflow.dataflow_id)
            test.eq(final_status, consts.STATUS.COMPLETED_FAILURE,
                "unhandled agent failure terminates the workflow as failed")

            local tool_nodes = node_reader.with_dataflow(workflow.dataflow_id)
                :with_node_types("tool.call")
                :all() or {}
            test.eq(#tool_nodes, 1, "one tool.call child node created")
            test.eq(tool_nodes[1].status, consts.STATUS.COMPLETED_FAILURE, "tool.call child records the failure")

            local row, row_err = dataflow_repo.get(workflow.dataflow_id)
            test.is_nil(row_err, "dataflow row loaded")
            local aggregate_error = tostring(row.metadata and row.metadata.error or "")
            test.is_true(aggregate_error ~= "", "aggregate failure carries error details")
            test.is_true(string.find(aggregate_error, workflow.node_id, 1, true) ~= nil,
                "aggregate failure names the agent node")
            test.is_true(string.find(aggregate_error, tool_nodes[1].node_id, 1, true) == nil,
                "aggregate failure does not blame the tool child whose error the agent consumed")
        end)
    end)
end

return test.run_cases(define_tests)
