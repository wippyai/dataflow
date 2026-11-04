local helpers = require("helpers")
local time = require("time")

-- cache_read_tokens reports cached prompt input next to the uncached
-- prompt_tokens, as providers do when the prompt cache is hit.
local function response_tokens(prompt_tokens, completion_tokens, cache_read_tokens)
    return {
        prompt_tokens = prompt_tokens,
        completion_tokens = completion_tokens,
        cache_read_tokens = cache_read_tokens or 0,
        thinking_tokens = 0,
        total_tokens = prompt_tokens + completion_tokens
    }
end

local function tool_call_response(scenario_id, step, delay_ms, prompt_tokens, completion_tokens, tool_name, extra_args,
                                  cache_read_tokens)
    local arguments = {
        scenario_id = scenario_id,
        step = step,
        delay_ms = delay_ms or 0
    }
    for key, value in pairs(extra_args or {}) do
        arguments[key] = value
    end

    return {
        success = true,
        result = {
            content = "Requesting tool step " .. tostring(step),
            tool_calls = {
                {
                    id = helpers.call_id(scenario_id, step),
                    name = tool_name or "recovery_tool",
                    arguments = arguments
                }
            }
        },
        finish_reason = "tool_call",
        tokens = response_tokens(prompt_tokens, completion_tokens, cache_read_tokens),
        metadata = {}
    }
end

local function final_response(scenario_id, mode, function_result_count, prompt_tokens, completion_tokens,
                              cache_read_tokens)
    return {
        success = true,
        result = {
            content = string.format("final:%s:%s:%d", mode, scenario_id, function_result_count),
            tool_calls = {}
        },
        finish_reason = "stop",
        tokens = response_tokens(prompt_tokens, completion_tokens, cache_read_tokens),
        metadata = {}
    }
end

-- An assistant turn with no text and no tool calls: the model declining to
-- answer. finish_reason is "stop", exactly what providers report for it.
local function empty_response(prompt_tokens, completion_tokens)
    return {
        success = true,
        result = {
            content = "",
            tool_calls = {}
        },
        finish_reason = "stop",
        tokens = response_tokens(prompt_tokens, completion_tokens),
        metadata = {}
    }
end

local EMPTY_RESULT_FEEDBACK = "Your response was empty"

local function offers_tool(contract_args, tool_name)
    for _, tool in ipairs(contract_args and contract_args.tools or {}) do
        if tool.name == tool_name then
            return true
        end
    end
    return false
end

-- The default checkpoint summarizer names its node in the user message.
local function summarizer_node(messages)
    for _, message in ipairs(messages) do
        local text = message.role == "user" and message.content and message.content[1] and message.content[1].text
        local node_id = type(text) == "string" and text:match("^dataflow_id=%S*\nnode_id=(%S+)") or nil
        if node_id then return node_id end
    end
    return nil
end

local function handler(contract_args)
    local messages = contract_args and contract_args.messages or {}
    local summarized_node = summarizer_node(messages)
    if summarized_node then
        local attempt = helpers.bump_metric(summarized_node, "summarizer_calls", 1)
        helpers.record_request(summarized_node, attempt, messages)
        helpers.set_metric(summarized_node, "summarizer_max_tokens", (contract_args.options or {}).max_tokens)
        local summary = final_response(summarized_node, "summary", 0, 7, 4)
        -- A model that keeps everything: each merge is the previous summary plus 1500
        -- characters, cut off past max_tokens at 4 characters a token.
        if helpers.count_text_matches(messages, "GROW-SUMMARY") > 0 then
            local previous = 0
            for _, message in ipairs(messages) do
                local body = message.content and message.content[1] and message.content[1].text
                if type(body) == "string" then
                    for run_of_x in string.gmatch(body, "x+") do
                        if #run_of_x > previous then previous = #run_of_x end
                    end
                end
            end
            local wanted = previous + 1500
            local limit = tonumber((contract_args.options or {}).max_tokens) or 0
            summary.result.content = string.rep("x", math.min(wanted, limit * 4))
            if wanted > limit * 4 then summary.finish_reason = "length" end
        end
        if helpers.count_text_matches(messages, "BLANK-SUMMARY") > 0 then
            summary.result.content = "  \n "
        end
        if helpers.count_text_matches(messages, "TRUNCATE-SUMMARY") > 0 then
            summary.finish_reason = "length"
        end
        return summary
    end
    local scenario: any = helpers.parse_scenario(messages)
    local result_count = helpers.count_function_results(messages)

    local llm_calls = helpers.bump_metric(scenario.scenario_id, "llm_calls", 1)
    helpers.record_function_results(scenario.scenario_id, messages)
    if scenario.capture_requests then
        helpers.record_request(scenario.scenario_id, llm_calls, messages)
    end
    for _, message in ipairs(messages or {}) do
        local text = message.content and message.content[1] and message.content[1].text
        if type(text) == "string" and (
            string.find(text, "lifecycle-start:" .. tostring(scenario.scenario_id), 1, true)
            or (scenario.mode == "lifecycle_overlay" and string.find(text, "lifecycle-start:", 1, true))
        ) then
            helpers.bump_metric(scenario.scenario_id, "lifecycle_prompt_seen", 1)
            break
        end
    end

    -- scenario.prompt_tokens override lets checkpoint tests force the per-turn
    -- prompt token count above the checkpoint threshold deterministically
    local base_prompt = tonumber(scenario.prompt_tokens) or nil

    -- Exercise the real persisted-history path: reject the first finish, then
    -- refuse the next request unless every call in that turn has one result.
    if scenario.mode == "multiple_finish_then_retry" then
        local first_id = helpers.call_id(scenario.scenario_id, "finish-first")
        local second_id = helpers.call_id(scenario.scenario_id, "finish-second")
        local third_id = helpers.call_id(scenario.scenario_id, "finish-third")
        local sibling_id = helpers.call_id(scenario.scenario_id, 1)
        if helpers.get_metric(scenario.scenario_id, "llm_calls", 0) == 1 then
            return {
                success = true,
                result = {
                    content = "",
                    tool_calls = {
                        { id = first_id, name = "finish", arguments = {} },
                        { id = sibling_id, name = "recovery_tool", arguments = { scenario_id = scenario.scenario_id } },
                        { id = second_id, name = "finish", arguments = { answer = "must-not-win" } },
                        { id = third_id, name = "finish", arguments = {} }
                    }
                },
                finish_reason = "tool_call",
                tokens = response_tokens(13, 8),
                metadata = {}
            }
        end

        local expected: {[string]: number} = { [first_id] = 0, [second_id] = 0, [third_id] = 0, [sibling_id] = 0 }
        for _, message in ipairs(messages) do
            local id = message.function_call_id
            if message.role == "function_result" and type(id) == "string" then
                local count = expected[id]
                if count ~= nil then expected[id] = count + 1 end
            end
        end
        for id, count in pairs(expected) do
            if count ~= 1 then
                return nil, "unpaired tool call in reconstructed request: " .. id .. " (results=" .. count .. ")"
            end
        end
        return {
            success = true,
            result = {
                content = "",
                tool_calls = {
                    {
                        id = helpers.call_id(scenario.scenario_id, "finish-retry"),
                        name = "finish",
                        arguments = { answer = "retried:" .. scenario.scenario_id }
                    }
                }
            },
            finish_reason = "tool_call",
            tokens = response_tokens(15, 4),
            metadata = {}
        }
    end

    -- The tool reports its failure as data: { success = false, error = ... }.
    if scenario.mode == "soft_failing_tool_then_final" then
        if result_count == 0 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8, "recovery_tool", {
                soft_fail_message = scenario.soft_fail_message
            })
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    -- Two turns issue the same call id, as a provider that reuses ids does.
    if scenario.mode == "reused_call_id_then_final" then
        if result_count < 2 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8)
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    if scenario.mode == "failing_tool_then_final" then
        if result_count == 0 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8, "recovery_tool", {
                fail_message = scenario.fail_message or "Page returned status 403"
            })
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    -- Deliberately return a whitespace-only name. It crosses the LLM response
    -- contract (unlike an omitted required field) but is still an empty tool
    -- name at the agent-node boundary. The node must fail terminally rather
    -- than invoke it or build an observation key.
    if scenario.mode == "malformed_tool_call" then
        return {
            success = true,
            result = {
                content = "",
                tool_calls = {
                    {
                        id = helpers.call_id(scenario.scenario_id, "malformed-progress"),
                        name = " ",
                        arguments = {},
                        arguments_error = "unexpected EOF within \"/tool/arguments\""
                    }
                }
            },
            finish_reason = "tool_call",
            tokens = response_tokens(base_prompt or 13, 8),
            metadata = {}
        }
    end

    if scenario.mode == "failing_tool_then_llm_error" then
        if result_count == 0 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8, "recovery_tool", {
                fail_message = scenario.fail_message or "Page returned status 403"
            })
        end

        return nil, "recovery provider unavailable"
    end

    if scenario.mode == "behavior_restart" then
        if helpers.count_text_matches(messages, "BEHAVIOR-RESTART-TARGET") == 0 then
            if llm_calls == 1 then
                return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, 13, 8)
            end
            return nil, "recovered proposal ran the previous agent"
        end
        return final_response(scenario.scenario_id, scenario.mode, result_count, 9, 4)
    end

    -- The first turn reaches the token limit inside a tool call, so it is
    -- stored as a truncated action and retried.
    if scenario.mode == "truncated_then_final" then
        if llm_calls == 1 then
            local truncated = tool_call_response(scenario.scenario_id, 1, 0, 13, 100)
            truncated.finish_reason = "length"
            return truncated
        end
        return final_response(scenario.scenario_id, scenario.mode, result_count, 9, 4)
    end

    if scenario.mode == "commit_failure_round" then
        local response = tool_call_response(scenario.scenario_id, 1, 0, 13, 8)
        for step = 2, 12 do
            local call = tool_call_response(scenario.scenario_id, step, step == 2 and 200 or 10000, 13, 8,
                "recovery_tool", { fail_message = step == 2 and "tool failure before commit refusal" or nil })
            table.insert(response.result.tool_calls, call.result.tool_calls[1])
        end
        return response
    end

    if scenario.mode == "scalar_tools" then
        if result_count == 0 then
            local response = tool_call_response(scenario.scenario_id, 1, 0, 13, 8)
            response.result.tool_calls = {}
            for step, kind in ipairs({ "false", "zero", "empty", "nil" }) do
                local call = tool_call_response(scenario.scenario_id, step, 0, 13, 8, "recovery_tool",
                    { scalar_kind = kind })
                table.insert(response.result.tool_calls, call.result.tool_calls[1])
            end
            return response
        end
        return final_response(scenario.scenario_id, scenario.mode, result_count, 9, 4)
    end

    -- Two tool calls in one turn, each holding its worker for tool_delay_ms.
    if scenario.mode == "two_tools_one_turn" then
        if llm_calls == 1 then
            local first = tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8)
            local second = tool_call_response(scenario.scenario_id, 2, scenario.tool_delay_ms, base_prompt or 13, 8)
            table.insert(first.result.tool_calls, second.result.tool_calls[1])
            return first
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    -- One turn with a fast and a slow call; the final answer reports how many
    -- times each call ran, keyed by its original call id.
    if scenario.mode == "fast_and_slow_one_turn" then
        if result_count == 0 then
            local fast = tool_call_response(scenario.scenario_id, 1, scenario.fast_delay_ms, base_prompt or 13, 8)
            local slow = tool_call_response(scenario.scenario_id, 2, scenario.slow_delay_ms, base_prompt or 13, 8)
            table.insert(fast.result.tool_calls, slow.result.tool_calls[1])
            return fast
        end

        local final = final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
        final.result.content = string.format("%s:fast=%d:slow=%d", final.result.content,
            tonumber(helpers.get_metric(scenario.scenario_id,
                "tool_attempts:" .. helpers.call_id(scenario.scenario_id, 1), 0)) or 0,
            tonumber(helpers.get_metric(scenario.scenario_id,
                "tool_attempts:" .. helpers.call_id(scenario.scenario_id, 2), 0)) or 0)
        return final
    end

    if scenario.mode == "single_tool_then_final" then
        if result_count == 0 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8)
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    if scenario.mode == "control_child_then_final" then
        if result_count == 0 then
            return tool_call_response(
                scenario.scenario_id,
                1,
                scenario.tool_delay_ms,
                base_prompt or 13,
                8,
                "recovery_control_tool"
            )
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    -- Tool rounds whose responses each carry one thinking block; the round named
    -- by switch_at calls the control tool to change the agent.
    if scenario.mode == "thinking_rounds" then
        if scenario.slow_request == llm_calls then
            time.sleep(tostring(scenario.slow_ms) .. "ms")
        end
        local checkpoint_here = scenario.checkpoint_at == llm_calls
        for _, at in ipairs(type(scenario.checkpoint_at) == "table" and scenario.checkpoint_at or {}) do
            if at == llm_calls then checkpoint_here = true end
        end
        local tokens = checkpoint_here and 500 or base_prompt or 9
        local control_args = nil
        if scenario.empty_delegate_at == llm_calls then
            control_args = { empty_delegate = true }
        elseif scenario.artifact_at == llm_calls then
            control_args = { artifact = true }
        elseif scenario.yield_commands_at == llm_calls then
            control_args = { yield_child_ms = scenario.child_delay_ms or 10 }
        elseif scenario.switch_at == llm_calls then
            control_args = { switch_agent = scenario.switch_agent }
        elseif scenario.delegate_at == llm_calls then
            control_args = { delegate_to = scenario.delegate_to }
        elseif scenario.memory_at == llm_calls then
            control_args = { memory_add = "added at " .. tostring(llm_calls) }
        elseif scenario.commands_memory_at == llm_calls then
            control_args = { commands_memory = "added at " .. tostring(llm_calls), child_delay_ms = scenario.child_delay_ms }
        elseif scenario.delegate_without_input_at == llm_calls then
            control_args = { delegate_without_input = scenario.delegate_to }
        elseif scenario.commands_yield_at == llm_calls then
            control_args = { commands_children = 1, commands_yield = true, child_delay_ms = scenario.child_delay_ms }
        elseif scenario.artifact_delegate_at == llm_calls then
            control_args = { delegate_with_artifact = scenario.delegate_to, delegate_input = scenario.delegate_input }
        elseif scenario.two_delegates_at == llm_calls then
            control_args = { two_delegates = scenario.delegate_to }
        elseif scenario.two_children_at == llm_calls then
            control_args = { commands_children = 2, child_delay_ms = 10 }
        end
        if scenario.whitespace_at == llm_calls then
            local blank = final_response(scenario.scenario_id, scenario.mode, result_count, tokens, 4)
            blank.result.content = "  \n "
            return blank
        end
        if scenario.command_delegate_at == llm_calls then
            local commands = tool_call_response(scenario.scenario_id, 1, 0, tokens, 4, "recovery_control_tool",
                scenario.yield_commands and { yield_child_ms = scenario.child_delay_ms or 10 }
                    or { commands_children = scenario.commands_children or 1,
                        child_delay_ms = scenario.child_delay_ms or 10,
                        child_delays = scenario.child_delays, failing_child = scenario.failing_child })
            local delegate = tool_call_response(scenario.scenario_id, 2, 0, tokens, 4, "recovery_control_tool",
                { delegate_to = scenario.delegate_to, delegate_input = scenario.delegate_input })
            table.insert(commands.result.tool_calls, delegate.result.tool_calls[1])
            return commands
        end
        if scenario.delegate_and_memory_at == llm_calls then
            return tool_call_response(scenario.scenario_id, 1, 0, tokens, 4, "recovery_control_tool",
                { memory_and_delegate = "added at " .. tostring(llm_calls),
                    delegate_to = scenario.delegate_to, delegate_input = scenario.delegate_input })
        end
        if scenario.delegate_once_at == llm_calls then
            local memory = tool_call_response(scenario.scenario_id, 1, 0, tokens, 4, "recovery_control_tool",
                { memory_add = "added at " .. tostring(llm_calls) })
            local delegate = tool_call_response(scenario.scenario_id, 2, 0, tokens, 4, "recovery_control_tool",
                { delegate_to = scenario.delegate_to, delegate_input = scenario.delegate_input, delegate_once = true,
                    delegate_memory = scenario.delegate_memory })
            table.insert(memory.result.tool_calls, delegate.result.tool_calls[1])
            return memory
        end
        if scenario.yield_at == llm_calls then
            local yielding = tool_call_response(scenario.scenario_id, 1, 0, tokens, 4, "recovery_control_tool",
                { yield_child_ms = 6000 })
            local switching = tool_call_response(scenario.scenario_id, 2, 0, tokens, 4, "recovery_control_tool",
                { switch_agent = scenario.switch_agent })
            local adding = tool_call_response(scenario.scenario_id, 3, 0, tokens, 4, "recovery_control_tool",
                { memory_add = "added at " .. tostring(llm_calls) })
            table.insert(yielding.result.tool_calls, switching.result.tool_calls[1])
            table.insert(yielding.result.tool_calls, adding.result.tool_calls[1])
            return yielding
        end
        if scenario.delegate_with_memory_at == llm_calls then
            local memory = tool_call_response(scenario.scenario_id, 1, 0, tokens, 4, "recovery_control_tool",
                { memory_add = "added at " .. tostring(llm_calls) })
            local delegate = tool_call_response(scenario.scenario_id, 2, 0, tokens, 4, "recovery_control_tool",
                { delegate_to = scenario.delegate_to, delegate_input = scenario.delegate_input })
            table.insert(memory.result.tool_calls, delegate.result.tool_calls[1])
            return memory
        end
        if tonumber(scenario.pad) then
            control_args = nil
        end
        local response
        if llm_calls <= (tonumber(scenario.rounds) or 1) then
            response = tool_call_response(scenario.scenario_id, llm_calls, 0, tokens, 4,
                control_args and "recovery_control_tool" or "recovery_tool",
                control_args or (tonumber(scenario.pad) and { pad = string.rep("p", tonumber(scenario.pad)) } or nil))
        else
            response = final_response(scenario.scenario_id, scenario.mode, result_count, tokens, 4)
        end
        response.metadata = {
            thinking_blocks = {
                { type = "thinking", thinking = "block-" .. tostring(llm_calls), signature = "signature" }
            }
        }
        return response
    end

    if scenario.mode == "lifecycle_overlay" then
        local lifecycle_trait = "userspace.dataflow.node.agent.stub:lifecycle_test_trait"
        if result_count < 2 then
            return tool_call_response(scenario.scenario_id, result_count + 1, scenario.tool_delay_ms,
                base_prompt or 13, 8, "recovery_control_tool", {
                    overlay_traits = result_count == 0 and {} or { lifecycle_trait }
                })
        end
        if result_count == 2 then
            return tool_call_response(scenario.scenario_id, 3, scenario.tool_delay_ms,
                base_prompt or 13, 8, "recovery_tool")
        end
        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    if scenario.mode == "two_tool_turns" then
        if result_count == 0 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 12, 9)
        end

        if result_count == 1 then
            return tool_call_response(scenario.scenario_id, 2, scenario.tool_delay_ms, base_prompt or 11, 11)
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 10, 3)
    end

    -- artifact_then_empty_then_final: one artifact-creating tool turn, then an
    -- empty assistant turn, then a real answer once the engine has pushed the
    -- empty-result feedback back into the conversation.
    if scenario.mode == "artifact_then_empty_then_final" then
        if result_count == 0 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8,
                "artifact_tool", {
                    title = "Report",
                    content = scenario.artifact_content or "# Report\n\nFull findings."
                })
        end

        if helpers.count_text_matches(messages, EMPTY_RESULT_FEEDBACK) == 0 then
            return empty_response(base_prompt or 9, 0)
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    -- empty_until_limit: every turn is empty; the node must stop on the
    -- consecutive-empty bound instead of completing on nothing.
    if scenario.mode == "empty_until_limit" then
        return empty_response(base_prompt or 9, 0)
    end

    -- empty_tool_empty_empty_final: turn 1 empty, turn 2 a tool call, turns 3
    -- and 4 empty, turn 5 a real answer. The turn number comes from the
    -- llm_calls metric this handler already bumped, so the sequence is exact.
    -- Runs of at most two consecutive empty turns stay below a bound of three.
    if scenario.mode == "empty_tool_empty_empty_final" then
        local turn = tonumber(helpers.get_metric(scenario.scenario_id, "llm_calls", 0)) or 0

        if turn == 2 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8)
        end

        if turn >= 5 then
            return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
        end

        return empty_response(base_prompt or 9, 0)
    end

    -- finish_when_offered: calls the finish tool as soon as the request offers it
    -- and answers in plain text otherwise, so a node that withholds the finish
    -- tool never receives a terminal call.
    if scenario.mode == "finish_when_offered" or scenario.mode == "finish_after_text" then
        if contract_args.tool_choice == "any" then
            helpers.bump_metric(scenario.scenario_id, "tool_choice_any", 1)
        end
        if contract_args.tool_choice == "auto" then
            helpers.bump_metric(scenario.scenario_id, "tool_choice_auto", 1)
        end
        local options = contract_args.options or {}
        if options.tool_choice_fallback == "auto" then
            helpers.bump_metric(scenario.scenario_id, "tool_choice_fallback_auto", 1)
        end
        if scenario.mode == "finish_after_text"
            and helpers.get_metric(scenario.scenario_id, "llm_calls", 0) == 1 then
            return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
        end
        if offers_tool(contract_args, "finish") then
            helpers.bump_metric(scenario.scenario_id, "finish_offered", 1)
            return {
                success = true,
                result = {
                    content = "",
                    tool_calls = {
                        {
                            id = helpers.call_id(scenario.scenario_id, "finish"),
                            name = "finish",
                            arguments = { answer = "finished:" .. tostring(scenario.scenario_id) }
                        }
                    }
                },
                finish_reason = "tool_call",
                tokens = response_tokens(base_prompt or 9, 4),
                metadata = {}
            }
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    -- checkpoint_stress: drives three tool turns then a final response, with
    -- prompt_tokens forced via scenario.prompt_tokens so tests can deterministically
    -- exceed config.checkpoint.token_threshold.
    if scenario.mode == "checkpoint_stress" then
        local max_steps = tonumber(scenario.max_steps) or 3
        if llm_calls <= max_steps then
            return tool_call_response(scenario.scenario_id, llm_calls,
                scenario.tool_delay_ms, base_prompt or 500, 8, nil, nil, tonumber(scenario.cache_read_tokens))
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 500, 4,
            tonumber(scenario.cache_read_tokens))
    end

    return final_response(scenario.scenario_id, scenario.mode or "text_final", result_count, base_prompt or 7, 4)
end

return { handler = handler }
