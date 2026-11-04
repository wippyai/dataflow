local json = require("json")
local time = require("time")
local helpers = require("helpers")

-- Records each after_execute payload. In the restart scenario it holds the first one
-- long enough for the runtime to be killed in it. Every round with calls proposes a
-- session value.
local function apply(payload)
    if payload.phase ~= "after_execute" then
        return {}
    end
    local node_id = payload.host and payload.host.node_id
    local attempt = helpers.bump_metric(node_id, "wrapper_payloads", 1)
    helpers.set_metric(node_id, "wrapper_payload:" .. tostring(attempt),
        json.encode({ tool_calls = payload.tool_calls, tool_results = payload.tool_results }))
    local call = (payload.tool_calls or {})[1]
    if not call then
        return {}
    end
    local scenario_id = call.arguments and call.arguments.scenario_id
    if scenario_id == "round-settle" and helpers.bump_metric(scenario_id, "wrapper_attempts", 1) == 1 then
        time.sleep("60s")
    end
    return { _control = { context = { session = { set = { settled_by = "wrapper" } } } } }
end

return { apply = apply }
