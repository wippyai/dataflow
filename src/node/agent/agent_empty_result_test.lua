local test = require("test")
local uuid = require("uuid")
local time = require("time")
local json = require("json")
local funcs = require("funcs")
local client = require("client")
local consts = require("consts")
local agent_consts = require("agent_consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")

local function as_table(content)
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

        local function reset_metrics(scenario_id)
            local _, err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_reset",
                { scenario_id = scenario_id }
            )
            test.is_nil(err, "metrics reset")
        end

        local function get_metrics(scenario_id)
            local result, err = funcs.new():call(
                "userspace.dataflow.node.agent.stub:recovery_metrics_get",
                { scenario_id = scenario_id }
            )
            test.is_nil(err, "metrics fetch")
            return result or {}
        end

        local function create_workflow(opts)
            local node_id = uuid.v7()
            local input_id = uuid.v7()
            local node_input_id = uuid.v7()
            local scenario_id = "agent-empty-result-" .. uuid.v7()

            reset_metrics(scenario_id)

            local node_config: any = {
                agent = "userspace.dataflow.node.agent.stub:recovery_test_agent",
                arena = {
                    prompt = "Execute the empty result scenario.",
                    max_iterations = opts.max_iterations,
                    max_empty_turns = opts.max_empty_turns,
                    tool_calling = "auto",
                    tools = {
                        "userspace.dataflow.node.agent.stub:artifact_tool",
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
            }

            if opts.route_errors then
                node_config.error_targets = {
                    {
                        data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT,
                        key = "error"
                    }
                }
            end

            local commands = {
                {
                    type = consts.COMMAND_TYPES.CREATE_NODE,
                    payload = {
                        node_id = node_id,
                        node_type = "userspace.dataflow.node.agent:node",
                        status = consts.STATUS.PENDING,
                        config = node_config,
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
                            mode = opts.mode,
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

        local function outputs_for(workflow, key)
            return data_reader.with_dataflow(workflow.dataflow_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :with_data_keys(key)
                :fetch_options({ replace_references = true })
                :all() or {}
        end

        it("keeps the artifact, rejects the empty turn, and completes on the next real answer", function()
            local workflow = create_workflow({
                mode = "artifact_then_empty_then_final",
                max_iterations = 5
            })

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

            local outputs = outputs_for(workflow, "result")
            test.eq(#outputs, 1, "workflow output produced")
            test.eq((outputs[1] :: any).content, "final:artifact_then_empty_then_final:" .. workflow.scenario_id .. ":1",
                "the output is the real answer, never the empty turn")

            local agent_result = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                :one()
            test.not_nil(agent_result, "agent node produced a result")
            test.eq((agent_result :: any).discriminator, "result.success", "agent completed on the real answer")
        end)

        it("fails on the consecutive-empty bound instead of spinning to the iteration cap", function()
            local workflow = create_workflow({
                mode = "empty_until_limit",
                max_iterations = 12,
                max_empty_turns = 3
            })

            c:start(workflow.dataflow_id)

            local final_status = wait_terminal(workflow.dataflow_id)
            test.eq(final_status, consts.STATUS.COMPLETED_FAILURE, "empty turns never complete the workflow")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.llm_calls, 3, "the model is called exactly the bounded number of times")

            local agent_nodes = node_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :all() or {}
            test.eq(#agent_nodes, 1, "agent node row present")
            test.eq((agent_nodes[1] :: any).status, consts.STATUS.COMPLETED_FAILURE,
                "agent node fails on the empty-turn bound")

            local feedback = empty_result_observations(workflow)
            test.eq(#feedback, 3, "every empty turn is answered with empty-result feedback")

            local agent_result = data_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :with_data_types(consts.DATA_TYPE.NODE_RESULT)
                :one()
            test.not_nil(agent_result, "agent node produced a result")
            test.eq((agent_result :: any).discriminator, "result.error", "the node reports a failure")
            local result_error = as_table(as_table((agent_result :: any).content).error)
            test.eq(result_error.code, agent_consts.ERROR.EMPTY_TURNS_EXCEEDED, "the failure carries the named code")
            test.contains(tostring(result_error.message), "3 consecutive turns",
                "the failure names how many empty turns were seen")

            test.eq(#outputs_for(workflow, "result"), 0, "no result output is produced from empty turns")
        end)

        it("routes the empty-turn failure through the node error target", function()
            local workflow = create_workflow({
                mode = "empty_until_limit",
                max_iterations = 12,
                max_empty_turns = 3,
                route_errors = true
            })

            c:start(workflow.dataflow_id)

            wait_terminal(workflow.dataflow_id)

            local agent_nodes = node_reader.with_dataflow(workflow.dataflow_id)
                :with_nodes(workflow.node_id)
                :all() or {}
            test.eq((agent_nodes[1] :: any).status, consts.STATUS.COMPLETED_FAILURE,
                "agent node fails on the empty-turn bound")

            local routed = outputs_for(workflow, "error")
            test.eq(#routed, 1, "the failure reaches the node error target like any other node failure")
            local routed_error = as_table((routed[1] :: any).content)
            test.eq(routed_error.code, agent_consts.ERROR.EMPTY_TURNS_EXCEEDED,
                "the routed error carries the named code so a workflow can react")
        end)

        it("resets the bound on any turn that carries a tool call", function()
            local workflow = create_workflow({
                mode = "empty_tool_empty_empty_final",
                max_iterations = 8,
                max_empty_turns = 3
            })

            c:start(workflow.dataflow_id)

            local final_status = wait_terminal(workflow.dataflow_id)
            test.eq(final_status, consts.STATUS.COMPLETED_SUCCESS,
                "two consecutive empty turns never reach a bound of three")

            local metrics = get_metrics(workflow.scenario_id)
            test.eq(metrics.llm_calls, 5, "the run spends exactly the five turns the scenario scripts")
            test.eq(metrics.tool_effects, 1, "the tool turn ran after the first empty turn")

            local feedback = empty_result_observations(workflow)
            test.eq(#feedback, 3, "each empty turn is answered, none of them terminal")

            local outputs = outputs_for(workflow, "result")
            test.eq(#outputs, 1, "workflow output produced")
            test.contains(tostring((outputs[1] :: any).content), "final:empty_tool_empty_empty_final",
                "the run completes on the real answer")
        end)
    end)
end

return test.run_cases(define_tests)
