local helpers = require("helpers")

-- Deterministic compaction summarizer for agent compaction tests.
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

    if scenario_id then
        helpers.bump_metric(scenario_id, "compaction_calls", 1)
        helpers.set_metric(scenario_id, "last_compaction_history_count", history_count)
        helpers.set_metric(scenario_id, "last_compaction_prompt_tokens", prompt_tokens)
    end

    return {
        memory = string.format(
            "compacted %d history rows at %d prompt tokens",
            history_count,
            prompt_tokens
        )
    }
end

return { handler = handler }
