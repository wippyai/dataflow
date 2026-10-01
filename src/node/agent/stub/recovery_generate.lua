local helpers = require("helpers")

local function response_tokens(prompt_tokens, completion_tokens)
    return {
        prompt_tokens = prompt_tokens,
        completion_tokens = completion_tokens,
        thinking_tokens = 0,
        total_tokens = prompt_tokens + completion_tokens
    }
end

local function tool_call_response(scenario_id, step, delay_ms, prompt_tokens, completion_tokens, tool_name, extra_args)
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
        tokens = response_tokens(prompt_tokens, completion_tokens),
        metadata = {}
    }
end

local function final_response(scenario_id, mode, function_result_count, prompt_tokens, completion_tokens)
    return {
        success = true,
        result = {
            content = string.format("final:%s:%s:%d", mode, scenario_id, function_result_count),
            tool_calls = {}
        },
        finish_reason = "stop",
        tokens = response_tokens(prompt_tokens, completion_tokens),
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

local function handler(contract_args)
    local messages = contract_args and contract_args.messages or {}
    local scenario: any = helpers.parse_scenario(messages)
    local result_count = helpers.count_function_results(messages)

    helpers.bump_metric(scenario.scenario_id, "llm_calls", 1)
    for _, message in ipairs(messages or {}) do
        local text = message.content and message.content[1] and message.content[1].text
        if type(text) == "string" and string.find(text, "lifecycle-start:" .. tostring(scenario.scenario_id), 1, true) then
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

        local expected = { [first_id] = 0, [second_id] = 0, [third_id] = 0, [sibling_id] = 0 }
        for _, message in ipairs(messages) do
            if message.role == "function_result" and expected[message.function_call_id] ~= nil then
                expected[message.function_call_id] = expected[message.function_call_id] + 1
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

    if scenario.mode == "failing_tool_then_final" then
        if result_count == 0 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8, "recovery_tool", {
                fail_message = scenario.fail_message or "Page returned status 403"
            })
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 9, 4)
    end

    if scenario.mode == "failing_tool_then_llm_error" then
        if result_count == 0 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8, "recovery_tool", {
                fail_message = scenario.fail_message or "Page returned status 403"
            })
        end

        return nil, "recovery provider unavailable"
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
        if result_count < max_steps then
            return tool_call_response(scenario.scenario_id, result_count + 1,
                scenario.tool_delay_ms, base_prompt or 500, 8)
        end

        return final_response(scenario.scenario_id, scenario.mode, result_count, base_prompt or 500, 4)
    end

    return final_response(scenario.scenario_id, scenario.mode or "text_final", result_count, base_prompt or 7, 4)
end

return { handler = handler }
