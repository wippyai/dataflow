local test = require("test")
local uuid = require("uuid")
local json = require("json")
local client = require("client")
local commit = require("commit")
local consts = require("consts")
local data_reader = require("data_reader")
local node_reader = require("node_reader")
local node_sdk = require("node_sdk")
local agent_context = require("agent_context")
local agent_node = require("agent_node")
local agent_consts = require("agent_consts")

local function define_tests()
    describe("Agent recovery completion", function()
        local function exercise(scenario)
            local node_id = uuid.v7()
            local arena = {
                prompt = "Complete the task.", tool_calling = scenario.tool_calling or "auto",
                min_iterations = scenario.min_iterations or 0, max_iterations = 5,
            }
            if arena.tool_calling == "any" then
                arena.exit_schema = {
                    type = "object", properties = { answer = { type = "string" } }, required = { "answer" },
                }
            end
            local c = client.new()
            local dataflow_id, create_err = c:create_workflow({ {
                type = consts.COMMAND_TYPES.CREATE_NODE,
                payload = {
                    node_id = node_id, node_type = "userspace.dataflow.node.agent:node",
                    status = consts.STATUS.RUNNING,
                    config = {
                        agent = "test_agent", model = "test_model", show_tool_calls = false, arena = arena,
                        data_targets = { {
                            data_type = consts.DATA_TYPE.WORKFLOW_OUTPUT, key = "answer",
                            content_type = consts.CONTENT_TYPE.JSON,
                        } },
                    },
                },
            } })
            test.is_nil(create_err)

            local function args()
                local row, read_err = node_reader.with_dataflow(dataflow_id):with_nodes(node_id):one()
                test.is_nil(read_err)
                test.not_nil(row)
                return { dataflow_id = dataflow_id, node_id = node_id, node = row }
            end

            local function rows(kind)
                local result, read_err = data_reader.with_dataflow(dataflow_id):with_nodes(node_id)
                    :with_data_types(kind):all()
                test.is_nil(read_err)
                return result or {}
            end

            local restarting = false
            local recovered_feedback_writes = 0
            local steps = {}
            local instance = { id = "test_agent", model = "test_model" }
            instance.step = function(_, prompt, options)
                local iteration = options.context.agent_run.host.iteration
                if not restarting and iteration == 2 then
                    error("interrupt before the next action")
                end
                if restarting and scenario.feedback then
                    local observations = rows(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
                    test.eq(#observations, 1, "feedback commits before the next model turn")
                    test.eq(observations[1].key, scenario.feedback)
                    test.eq(observations[1].content, scenario.feedback_text)
                    test.eq(recovered_feedback_writes, scenario.interrupt_after_action and 1 or 0,
                        "recovery records feedback only when its key is absent")
                end
                steps[#steps + 1] = { iteration = iteration, messages = prompt:get_messages() }
                if iteration == 1 then
                    return {
                        result = scenario.result or "rejected answer", tool_calls = scenario.exit and { {
                            id = "finish_first", name = "finish", arguments = { answer = "accepted exit" },
                        } } or {},
                        truncated = scenario.truncated, finish_reason = scenario.truncated and "length" or "stop",
                    }
                end
                if arena.tool_calling == "any" then
                    return { tool_calls = { {
                        id = "finish_next", name = "finish", arguments = { answer = "next answer" },
                    } } }
                end
                return { result = "next answer", tool_calls = {}, finish_reason = "stop" }
            end
            local ctx = {
                configure_delegate_tools = function() end, add_tools = function() end,
                load_agent = function() return instance end,
                get_current_agent = function() return instance end,
                get_config = function() return { current_agent_id = instance.id, current_model = instance.model } end,
            }
            local saved_node, saved_ctx = node_sdk.new, agent_context.new
            -- Apply commits synchronously, as the integration tests do without an orchestrator.
            node_sdk.new = function(run_args)
                local n, err = saved_node(run_args, {
                    data_reader = data_reader,
                    commit = { submit = function(df_id, op_id, commands)
                        return commit.execute(df_id, op_id, commands, { publish = false })
                    end },
                    process = {
                        listen = function(topic: string, _options: any?): (Channel<any>, any?)
                            return process.listen(topic)
                        end,
                        send = function(pid: string, topic: string, payload: any): (boolean, any?)
                            return process.send(pid, topic, payload)
                        end,
                    },
                })
                if n then
                    n.yield = function(self)
                        local result, submit_err = self:submit()
                        if not restarting and scenario.interrupt_after_action
                            and #rows(agent_consts.DATA_TYPE.AGENT_ACTION) > 0 then
                            error("interrupt after the action commit")
                        end
                        return result, submit_err
                    end
                    local saved_complete, saved_data = n.complete, n.data
                    n.complete = function(self, ...)
                        if not restarting then error("interrupt before terminal output") end
                        return saved_complete(self, ...)
                    end
                    n.data = function(self, kind, content, options)
                        if restarting and kind == agent_consts.DATA_TYPE.AGENT_OBSERVATION
                            and options.key == scenario.feedback then
                            recovered_feedback_writes = recovered_feedback_writes + 1
                        end
                        return saved_data(self, kind, content, options)
                    end
                end
                return n, err
            end
            agent_context.new = function() return ctx end
            local ok, failure = pcall(function()
                local first_ok, first_result = pcall(agent_node.run, args())
                test.is_false(first_ok, "the first run interrupts before completion")
                if scenario.accepted then
                    test.contains(tostring(first_result), "interrupt before terminal output")
                elseif scenario.interrupt_after_action then
                    test.contains(tostring(first_result), "interrupt after the action commit")
                else
                    test.contains(tostring(first_result), "interrupt before the next action")
                end
                test.eq(#rows(consts.DATA_TYPE.WORKFLOW_OUTPUT), 0,
                    "the first run does not commit terminal output")
                test.eq(#steps, 1)
                local actions = rows(agent_consts.DATA_TYPE.AGENT_ACTION)
                test.eq(#actions, 1, "only the first action commits before the restart")
                test.eq(actions[1].metadata.iteration, 1)
                if scenario.feedback then
                    local observations = rows(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
                    test.eq(#observations, scenario.interrupt_after_action and 0 or 1)
                    if not scenario.interrupt_after_action then
                        test.eq(observations[1].key, scenario.feedback)
                        test.eq(observations[1].content, scenario.feedback_text)
                    end
                end

                restarting = true
                local recovered_args = args()
                test.eq(recovered_args.node.metadata.state.current_iteration, 1)
                local result = agent_node.run(recovered_args)
                test.is_true(result.success, tostring(result.message))
                test.eq(#steps, scenario.accepted and 1 or 2,
                    "recovery continues rejected turns and preserves accepted completions")
                if not scenario.accepted then
                    test.eq(steps[2].iteration, 2, "recovery resumes at the next iteration")
                    local history = json.encode(steps[2].messages)
                    test.contains(history, scenario.result or "rejected answer", "the prior action stays in history")
                    if scenario.feedback then
                        local observations = rows(agent_consts.DATA_TYPE.AGENT_OBSERVATION)
                        test.eq(#observations, 1, "recovery keeps one feedback row")
                        test.contains(history, observations[1].content, "feedback reaches the next prompt")
                    end
                end
                local outputs, output_err = data_reader.with_dataflow(dataflow_id)
                    :with_data_types(consts.DATA_TYPE.WORKFLOW_OUTPUT):with_data_keys("answer"):all()
                test.is_nil(output_err)
                test.eq(#outputs, 1)
                local content = outputs[1].content
                if type(content) == "string" and arena.tool_calling == "any" then
                    local decode_err
                    content, decode_err = json.decode(content)
                    test.is_nil(decode_err)
                end
                if scenario.exit then
                    test.eq(content.answer, "accepted exit")
                elseif arena.tool_calling == "any" then
                    test.eq(content.answer, "next answer")
                else
                    test.eq(content, scenario.accepted and scenario.result or "next answer")
                end
            end)
            node_sdk.new, agent_context.new = saved_node, saved_ctx
            test.is_true(ok, tostring(failure))
        end

        for _, scenario in ipairs({
            { name = "minimum iterations", min_iterations = 2 },
            { name = "minimum iterations without tools", min_iterations = 2, tool_calling = "none" },
            { name = "required tool use", tool_calling = "any", feedback = "1_no_tools_called",
                feedback_text = agent_consts.FEEDBACK.NO_TOOLS_CALLED .. " " ..
                    string.format(agent_consts.FEEDBACK.EXIT_AVAILABLE, "finish") },
            { name = "truncated output", truncated = true, feedback = "1_truncation_warning",
                feedback_text = agent_consts.FEEDBACK.OUTPUT_TRUNCATED },
            { name = "whitespace output", result = "   ", feedback = "1_empty_result",
                feedback_text = agent_consts.FEEDBACK.EMPTY_RESULT },
            { name = "required tool use before feedback", tool_calling = "any", feedback = "1_no_tools_called",
                interrupt_after_action = true, feedback_text = agent_consts.FEEDBACK.NO_TOOLS_CALLED .. " " ..
                    string.format(agent_consts.FEEDBACK.EXIT_AVAILABLE, "finish") },
            { name = "whitespace output before feedback", result = "   ", feedback = "1_empty_result",
                interrupt_after_action = true, feedback_text = agent_consts.FEEDBACK.EMPTY_RESULT },
            { name = "accepted text", accepted = true, result = "accepted answer" },
            { name = "accepted text without tools", accepted = true, tool_calling = "none", result = "accepted answer" },
            { name = "accepted exit", accepted = true, tool_calling = "any", exit = true },
        }) do
            it("replays the live completion decision for " .. scenario.name, function() exercise(scenario) end)
        end
    end)
end

return test.run_cases(define_tests)
