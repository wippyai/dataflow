local helpers = require("helpers")

-- Deterministic checkpoint summarizer for agent checkpoint tests.
-- Records a metric so tests can assert call count per scenario and
-- returns a summary string mentioning the number of history rows seen.
local function handler(args)
    local scenario_id = nil
    if type(args) == "table" then
        if type(args.scenario_id) == "string" then
            scenario_id = args.scenario_id
        elseif type(args.history) == "table" then
            for _, row in ipairs(args.history) do
                local meta = row.metadata or {}
                if type(meta.scenario_id) == "string" then
                    scenario_id = meta.scenario_id
                    break
                end
            end
        end
    end

    local history_count = 0
    if type(args) == "table" and type(args.history) == "table" then
        history_count = #args.history
    end

    local prompt_tokens = 0
    if type(args) == "table" and tonumber(args.prompt_tokens) then
        prompt_tokens = tonumber(args.prompt_tokens) or 0
    end

    local context_tokens = 0
    if type(args) == "table" and tonumber(args.context_tokens) then
        context_tokens = tonumber(args.context_tokens) or 0
    end

    if scenario_id then
        local checkpoint_number = helpers.bump_metric(scenario_id, "checkpoint_calls", 1)
        helpers.record_checkpoint_history(scenario_id, checkpoint_number, type(args.history) == "table" and args.history or {})
        for _, row in ipairs(type(args.history) == "table" and args.history or {}) do
            if type(row.metadata) == "table" and row.metadata.turn_context == true then
                helpers.set_metric(scenario_id, "checkpoint_saw_turn_context", 1)
            end
            if type(row.metadata) == "table" and row.metadata.checkpoint_skipped == true then
                helpers.set_metric(scenario_id, "checkpoint_saw_audit_row", 1)
            end
            if type(row.metadata) == "table" and type(row.metadata.tool_call_id) == "string" then
                helpers.bump_metric(scenario_id, "checkpoint_result_rows", 1)
                helpers.set_metric(scenario_id, "checkpoint_last_result_content", tostring(row.content))
            end
        end
        helpers.set_metric(scenario_id, "last_checkpoint_history_count", history_count)
        helpers.set_metric(scenario_id, "last_checkpoint_prompt_tokens", prompt_tokens)
        helpers.set_metric(scenario_id, "last_checkpoint_context_tokens", context_tokens)
        local options = type(args.options) == "table" and args.options or {}
        helpers.set_metric(scenario_id, "last_checkpoint_max_tokens", tonumber(options.max_tokens) or 0)
        helpers.set_metric(scenario_id, "last_checkpoint_threshold", tonumber(options.token_threshold) or 0)
    end

    return {
        memory = string.format(
            "checkpointed %d history rows at %d prompt tokens",
            history_count,
            prompt_tokens
        )
    }
end

return { handler = handler }
