local test = require("test")
local uuid = require("uuid")
local time = require("time")
local json = require("json")
local funcs = require("funcs")
local client = require("client")
local consts = require("consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")
local agent_consts = require("agent_consts")

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
        local function create_workflow(tool_calling: string?, model: string?, scenario_mode: string?, options: any?)
            options = options or {}
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
                            model = model,
                            arena = {
                                prompt = "Finish with a structured answer.",
                                tool_calling = tool_calling,
                                tools = scenario_mode == "multiple_finish_then_retry"
                                    and { "userspace.dataflow.node.agent.stub:recovery_tool" } or nil,
                                max_iterations = options.max_iterations or 3,
                                max_unproductive_steps = options.max_unproductive_steps,
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
                        content = { scenario_id = scenario_id, mode = scenario_mode or "finish_when_offered",
                            finish_rejections = options.finish_rejections },
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

        for _, mode in ipairs({"auto", "any"}) do
            local allowance = mode == "auto" and 2 or 3
            it("bounds repeated exit-schema rejections in " .. mode .. " mode", function()
                local workflow = create_workflow(mode, nil, nil, {
                    finish_rejections = 20, max_iterations = 10, max_unproductive_steps = allowance,
                })
                c:start(workflow.dataflow_id)
                test.eq(wait_terminal(workflow.dataflow_id), consts.STATUS.COMPLETED_FAILURE)
                test.eq(metric(workflow.scenario_id, "llm_calls"), allowance)
                local node = node_reader.with_dataflow(workflow.dataflow_id):with_nodes(workflow.node_id):one()
                test.eq(node.metadata.error.code, agent_consts.ERROR.UNPRODUCTIVE_STEPS)
                test.eq(node.metadata.state.unproductive_steps, allowance)
                local observations = data_reader.with_dataflow(workflow.dataflow_id):with_nodes(workflow.node_id)
                    :with_data_types(agent_consts.DATA_TYPE.AGENT_OBSERVATION):all()
                local rejections = 0
                for _, row in ipairs(observations) do
                    if row.metadata and row.metadata.exit_validation then
                        rejections = rejections + 1
                        test.contains(tostring(row.content), "missing required fields: answer")
                    end
                end
                test.eq(rejections, allowance)
            end)
        end

        for _, rejections in ipairs({1, 2}) do
            it("accepts a corrected finish after " .. rejections .. " rejected calls", function()
                local workflow = create_workflow("any", nil, nil, {
                    finish_rejections = rejections, max_iterations = 10, max_unproductive_steps = 3,
                })
                c:start(workflow.dataflow_id)
                test.eq(wait_terminal(workflow.dataflow_id), consts.STATUS.COMPLETED_SUCCESS)
                test.eq(metric(workflow.scenario_id, "llm_calls"), rejections + 1)
                local outputs = data_reader.with_dataflow(workflow.dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT):with_data_keys("result")
                    :fetch_options({replace_references = true}):all()
                test.eq(#outputs, 1)
                test.eq(as_table(outputs[1].content).answer, "finished:" .. workflow.scenario_id)
            end)
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

        it("pairs every finish and sibling result before retrying a rejected completion", function()
            local workflow = create_workflow("any", nil, "multiple_finish_then_retry")
            c:start(workflow.dataflow_id)

            test.eq(wait_terminal(workflow.dataflow_id), consts.STATUS.COMPLETED_SUCCESS,
                "the provider accepts the fully paired retry history")
            test.eq(metric(workflow.scenario_id, "llm_calls"), 2, "the rejected turn is followed by one retry")
            test.eq(metric(workflow.scenario_id, "tool_attempts"), 1, "the ordinary sibling executes once")

            local outputs = data_reader.with_dataflow(workflow.dataflow_id)
                :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT)
                :with_data_keys("result")
                :fetch_options({ replace_references = true })
                :all() or {}
            test.eq(#outputs, 1)
            test.eq(as_table((outputs[1] :: any).content).answer, "retried:" .. workflow.scenario_id,
                "a later finish in the rejected turn cannot prematurely complete the workflow")
        end)

        it("asks for a forced tool call and permits the auto fallback in any mode", function()
            local workflow = create_workflow("any")

            c:start(workflow.dataflow_id)

            test.eq(wait_terminal(workflow.dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "the node completes")
            test.eq(metric(workflow.scenario_id, "tool_choice_any"), 1, "the request still asks for a forced tool call")
            test.eq(metric(workflow.scenario_id, "tool_choice_fallback_auto"), 1,
                "the node, which enforces completion through finish, permits the auto fallback")
        end)

        it("falls back through the published LLM route but does not complete on a text-only turn", function()
            local workflow = create_workflow("any", "recovery-auto-test-model", "finish_after_text")

            c:start(workflow.dataflow_id)

            test.eq(wait_terminal(workflow.dataflow_id), consts.STATUS.COMPLETED_SUCCESS,
                "the node completes through the finish tool")
            test.eq(metric(workflow.scenario_id, "llm_calls"), 2,
                "the first text-only turn must not complete an any-mode arena")
            test.eq(metric(workflow.scenario_id, "tool_choice_any"), 0,
                "the LLM must never send forced choice to a route that rejects it")
            test.eq(metric(workflow.scenario_id, "tool_choice_auto"), 2,
                "the published LLM converts both forced requests to auto")
            test.eq(metric(workflow.scenario_id, "finish_offered"), 1,
                "the second turn finishes through the tool")
        end)

        it("permits no fallback when the arena itself runs in auto mode", function()
            local workflow = create_workflow("auto")

            c:start(workflow.dataflow_id)

            test.eq(wait_terminal(workflow.dataflow_id), consts.STATUS.COMPLETED_SUCCESS, "the node completes")
            test.eq(metric(workflow.scenario_id, "tool_choice_any"), 0, "auto mode forces nothing")
            test.eq(metric(workflow.scenario_id, "tool_choice_fallback_auto"), 0, "auto mode has nothing to fall back from")
        end)
    end)
end

return test.run_cases(define_tests)
