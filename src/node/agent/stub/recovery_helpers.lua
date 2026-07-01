local json = require("json")
local store = require("store")

local STORE_ID = "userspace.dataflow.node.agent.stub:recovery_store"

local helpers = {}

local function scenario_prefix(scenario_id)
    return "agent_recovery:" .. tostring(scenario_id) .. ":"
end

function helpers.get_store()
    local s, err = store.get(STORE_ID)
    if not s then
        error("Failed to open recovery test store: " .. tostring(err))
    end
    return s
end

function helpers.metric_key(scenario_id, metric_name)
    return scenario_prefix(scenario_id) .. metric_name
end

function helpers.call_id(scenario_id, step)
    return "call-" .. tostring(scenario_id) .. "-" .. tostring(step)
end

function helpers.extract_input_payload(text)
    if type(text) ~= "string" then
        return nil
    end

    local wrapped = text:match('<input key="[^"]+">%s*(.-)%s*</input>')
    if wrapped and wrapped ~= "" then
        return wrapped
    end

    return text
end

function helpers.parse_scenario(messages)
    for _, message in ipairs(messages or {}) do
        if message.role == "user" and message.content and message.content[1] and message.content[1].text then
            local payload = helpers.extract_input_payload(message.content[1].text)
            local decoded, decode_err = json.decode(payload or "")
            if not decode_err and type(decoded) == "table" and decoded.scenario_id then
                return decoded
            end
        end
    end

    return {
        scenario_id = "unknown",
        mode = "text_final",
        tool_delay_ms = 0
    }
end

function helpers.count_function_results(messages)
    local count = 0
    for _, message in ipairs(messages or {}) do
        if message.role == "function_result" then
            count = count + 1
        end
    end
    return count
end

function helpers.get_metric(scenario_id, metric_name, default_value)
    local s = helpers.get_store()
    local value = s:get(helpers.metric_key(scenario_id, metric_name))
    if value == nil then
        return default_value
    end
    return value
end

function helpers.set_metric(scenario_id, metric_name, value)
    local s = helpers.get_store()
    s:set(helpers.metric_key(scenario_id, metric_name), value)
    return value
end

function helpers.bump_metric(scenario_id, metric_name, amount)
    local step = amount or 1
    local current = tonumber(helpers.get_metric(scenario_id, metric_name, 0)) or 0
    current = current + step
    helpers.set_metric(scenario_id, metric_name, current)
    return current
end

function helpers.reset_metrics(scenario_id)
    helpers.set_metric(scenario_id, "llm_calls", 0)
    helpers.set_metric(scenario_id, "tool_attempts", 0)
    helpers.set_metric(scenario_id, "tool_effects", 0)
    helpers.set_metric(scenario_id, "checkpoint_calls", 0)
    helpers.set_metric(scenario_id, "last_checkpoint_history_count", 0)
    helpers.set_metric(scenario_id, "last_checkpoint_prompt_tokens", 0)
    helpers.set_metric(scenario_id, "lifecycle_activate", 0)
    helpers.set_metric(scenario_id, "lifecycle_before_step", 0)
    helpers.set_metric(scenario_id, "lifecycle_after_step", 0)
    helpers.set_metric(scenario_id, "lifecycle_deactivate", 0)
    helpers.set_metric(scenario_id, "lifecycle_prompt_seen", 0)
    helpers.set_metric(scenario_id, "lifecycle_last_iteration", 0)
    helpers.set_metric(scenario_id, "effect_applied:" .. helpers.call_id(scenario_id, 1), 0)
    helpers.set_metric(scenario_id, "effect_applied:" .. helpers.call_id(scenario_id, 2), 0)
end

function helpers.get_metrics(scenario_id)
    return {
        llm_calls = tonumber(helpers.get_metric(scenario_id, "llm_calls", 0)) or 0,
        tool_attempts = tonumber(helpers.get_metric(scenario_id, "tool_attempts", 0)) or 0,
        tool_effects = tonumber(helpers.get_metric(scenario_id, "tool_effects", 0)) or 0,
        checkpoint_calls = tonumber(helpers.get_metric(scenario_id, "checkpoint_calls", 0)) or 0,
        last_checkpoint_history_count = tonumber(
            helpers.get_metric(scenario_id, "last_checkpoint_history_count", 0)) or 0,
        last_checkpoint_prompt_tokens = tonumber(
            helpers.get_metric(scenario_id, "last_checkpoint_prompt_tokens", 0)) or 0,
        lifecycle_activate = tonumber(helpers.get_metric(scenario_id, "lifecycle_activate", 0)) or 0,
        lifecycle_before_step = tonumber(helpers.get_metric(scenario_id, "lifecycle_before_step", 0)) or 0,
        lifecycle_after_step = tonumber(helpers.get_metric(scenario_id, "lifecycle_after_step", 0)) or 0,
        lifecycle_deactivate = tonumber(helpers.get_metric(scenario_id, "lifecycle_deactivate", 0)) or 0,
        lifecycle_prompt_seen = tonumber(helpers.get_metric(scenario_id, "lifecycle_prompt_seen", 0)) or 0,
        lifecycle_last_iteration = tonumber(helpers.get_metric(scenario_id, "lifecycle_last_iteration", 0)) or 0,
        step1_effect = tonumber(helpers.get_metric(scenario_id,
            "effect_applied:" .. helpers.call_id(scenario_id, 1), 0)) or 0,
        step2_effect = tonumber(helpers.get_metric(scenario_id,
            "effect_applied:" .. helpers.call_id(scenario_id, 2), 0)) or 0
    }
end

return helpers
