local test = require("test")
local uuid = require("uuid")
local time = require("time")
local client = require("client")
local consts = require("consts")
local agent_consts = require("agent_consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")

local function define_tests()
    describe("Agent Node empty final turn", function()
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

        local function create_workflow(mode, max_iterations)
            local node_id = uuid.v7()
            local input_id = uuid.v7()
            local node_input_id = uuid.v7()
            local scenario_id = "agent-empty-result-" .. uuid.v7()

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
                                prompt = "Execute the empty result scenario.",
                                max_iterations = max_iterations,
                                tool_calling = "auto",
                                tools = {
                                    "userspace.dataflow.node.agent.stub:artifact_tool"
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
                            title = "Agent Empty Result Test"
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
                            mode = mode,
                            artifact_content = "# Report\n\nFull findings."
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
                metadata = { title = "Agent Empty Result Test Workflow" }
            })
            test.is_nil(err, "workflow created")

            return {
                dataflow_id = dataflow_id,
                node_id = node_id,
                scenario_id = scenario_id
            }
        end

        local function empty_result_observations(workflow)
            local observations = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
                :all() or {}
            local matched = {}
            for _, row in ipairs(observations) do
                if row.content == agent_consts.FEEDBACK.EMPTY_RESULT then
                    table.insert(matched, row)
                end
            end
            return matched
        end

        it("keeps the artifact, rejects the empty turn, and completes on the next real answer", function()
            local workflow = create_workflow("artifact_then_empty_then_final", 5)

            c:start(workflow.dataflow_id)

            local final_status = wait_terminal(workflow.dataflow_id)
            test.eq(final_status, consts.STATUS.COMPLETED_SUCCESS, "workflow completes on the real answer")

            local artifacts = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :with_data_types(consts.DATA_TYPE.ARTIFACT)
                :all() or {}
            test.eq(#artifacts, 1, "the control artifact is persisted")
            local artifact = artifacts[1] :: any
            test.eq(artifact.content, "# Report\n\nFull findings.", "artifact content is stored verbatim")
            test.eq(artifact.content_type, "text/markdown", "artifact content type is preserved")
            test.is_true((artifact.metadata or {}).created_in_control == true, "artifact is marked as control-created")

            local feedback = empty_result_observations(workflow)
            test.eq(#feedback, 1, "the empty turn produced exactly one empty-result observation")
            test.eq((feedback[1] :: any).key, "2_empty_result", "the observation is keyed to the empty iteration")

            local output = data_reader.with_dataflow(workflow.dataflow_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :one()
            test.not_nil(output, "workflow output produced")
            test.eq((output :: any).content, "final:artifact_then_empty_then_final:" .. workflow.scenario_id .. ":1",
                "the output is the real answer, never the empty turn")

            local agent_result = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                :one()
            test.not_nil(agent_result, "agent node produced a result")
            test.eq((agent_result :: any).discriminator, "result.success", "agent completed on the real answer")
        end)

        it("fails at the iteration limit when every turn is empty", function()
            local workflow = create_workflow("empty_until_limit", 3)

            c:start(workflow.dataflow_id)

            local final_status = wait_terminal(workflow.dataflow_id)
            test.eq(final_status, consts.STATUS.COMPLETED_FAILURE, "empty turns never complete the workflow")

            local agent_nodes = node_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :all() or {}
            test.eq(#agent_nodes, 1, "agent node row present")
            test.eq((agent_nodes[1] :: any).status, consts.STATUS.COMPLETED_FAILURE, "agent node fails on the iteration limit")

            local feedback = empty_result_observations(workflow)
            test.eq(#feedback, 3, "every empty turn is answered with empty-result feedback")

            local output = data_reader.with_dataflow(workflow.dataflow_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :one()
            test.is_nil(output, "no workflow output is produced from empty turns")
        end)
    end)
end

return test.run_cases(define_tests)
