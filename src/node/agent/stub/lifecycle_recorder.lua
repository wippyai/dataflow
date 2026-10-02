local ctx = require("ctx")
local helpers = require("helpers")

local function handler(payload)
    payload = payload or {}
    local host = payload.host or {}
    local scenario_id = ctx.get("scenario_id") or host.dataflow_id or "unknown"
    local phase = tostring(payload.phase or "unknown")
    local metric_name = "lifecycle_" .. phase

    helpers.bump_metric(scenario_id, metric_name, 1)
    if phase == "deactivate" and payload.reason == "agent_switch" then
        helpers.bump_metric(scenario_id, "lifecycle_switch_deactivate", 1)
        if payload.refs ~= nil then
            helpers.bump_metric(scenario_id, "lifecycle_switch_deactivate_refs_present", 1)
        end
    end
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

    local options = ctx.get("options") or {}
    if phase == "after_step" and options.propose_compaction == true then
        return { _control = {
            config = options.switch_agent and { agent = options.switch_agent } or nil,
            memory = { compact = true },
        } }
    end

    return {
        metadata = {
            scenario_id = scenario_id,
            host_kind = host.kind,
        }
    }
end

return { handler = handler }
