local helpers = require("helpers")

local function response_tokens(prompt_tokens, completion_tokens)
    return {
        prompt_tokens = prompt_tokens,
        completion_tokens = completion_tokens,
        thinking_tokens = 0,
        total_tokens = prompt_tokens + completion_tokens
    }
end

local function tool_call_response(scenario_id, step, delay_ms, prompt_tokens, completion_tokens)
    return {
        success = true,
        result = {
            content = "Requesting tool step " .. tostring(step),
            tool_calls = {
                {
                    id = helpers.call_id(scenario_id, step),
                    name = "recovery_tool",
                    arguments = {
                        scenario_id = scenario_id,
                        step = step,
                        delay_ms = delay_ms or 0
                    }
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

    -- scenario.prompt_tokens override lets compaction tests force the per-turn
    -- prompt token count above the compaction threshold deterministically
    local base_prompt = tonumber(scenario.prompt_tokens) or nil

    if scenario.mode == "single_tool_then_final" then
        if result_count == 0 then
            return tool_call_response(scenario.scenario_id, 1, scenario.tool_delay_ms, base_prompt or 13, 8)
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

    -- compaction_stress: drives three tool turns then a final response, with
    -- prompt_tokens forced via scenario.prompt_tokens so tests can deterministically
    -- exceed config.compact.token_threshold.
    if scenario.mode == "compaction_stress" then
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
