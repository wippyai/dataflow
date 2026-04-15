local ctx = require("ctx")
local time = require("time")
local helpers = require("helpers")

local function handler(input)
    if type(input) ~= "table" or not input.scenario_id then
        return nil, "scenario_id is required"
    end

    local scenario_id = tostring(input.scenario_id)
    local step = input.step or 1
    local call_id = ctx.get("call_id") or helpers.call_id(scenario_id, step)

    helpers.bump_metric(scenario_id, "tool_attempts", 1)

    local effect_key = "effect_applied:" .. tostring(call_id)
    local effect_applied = false
    if tonumber(helpers.get_metric(scenario_id, effect_key, 0)) ~= 1 then
        helpers.set_metric(scenario_id, effect_key, 1)
        helpers.bump_metric(scenario_id, "tool_effects", 1)
        effect_applied = true
    end

    local delay_ms = tonumber(input.delay_ms) or 0
    if delay_ms > 0 then
        time.sleep(tostring(delay_ms) .. "ms")
    end

    return {
        ok = true,
        scenario_id = scenario_id,
        step = step,
        call_id = tostring(call_id),
        effect_applied = effect_applied
    }
end

return { handler = handler }
