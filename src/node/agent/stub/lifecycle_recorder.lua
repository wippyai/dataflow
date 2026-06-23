local ctx = require("ctx")
local helpers = require("helpers")

local function handler(payload)
    payload = payload or {}
    local scenario_id = ctx.get("scenario_id") or "unknown"
    local phase = tostring(payload.phase or "unknown")
    local metric_name = "lifecycle_" .. phase

    helpers.bump_metric(scenario_id, metric_name, 1)

    local host = payload.host or {}
    if tonumber(host.iteration) then
        helpers.set_metric(scenario_id, "lifecycle_last_iteration", tonumber(host.iteration))
    end

    if phase == "activate" then
        return {
            messages = {
                {
                    role = "developer",
                    content = "lifecycle-start:" .. tostring(scenario_id)
                }
            },
            metadata = {
                scenario_id = scenario_id,
                host_kind = host.kind,
            }
        }
    end

    return {
        metadata = {
            scenario_id = scenario_id,
            host_kind = host.kind,
        }
    }
end

return { handler = handler }
