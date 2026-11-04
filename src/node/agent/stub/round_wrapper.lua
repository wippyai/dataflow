local time = require("time")
local helpers = require("helpers")

-- Holds the first after_execute long enough for a runtime to be killed in it, and
-- proposes a session value on the run that recovery repeats.
local function apply(payload)
    if payload.phase ~= "after_execute" then
        return {}
    end
    local call = (payload.tool_calls or {})[1]
    if not call then
        return {}
    end
    local scenario_id = call.arguments and call.arguments.scenario_id
    local attempt = helpers.bump_metric(scenario_id, "wrapper_attempts", 1)
    if attempt == 1 then
        time.sleep("60s")
    end
    return { _control = { context = { session = { set = { settled_by = "wrapper" } } } } }
end

return { apply = apply }
