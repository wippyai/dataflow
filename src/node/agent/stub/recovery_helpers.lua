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

function helpers.count_text_matches(messages, needle)
    local count = 0
    for _, message in ipairs(messages or {}) do
        local text = message.content and message.content[1] and message.content[1].text
        if type(text) == "string" and string.find(text, needle, 1, true) then
            count = count + 1
        end
    end
    return count
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

-- Records the result the model received for each call id, so tests can assert
-- what reached the model rather than what the node stored.
function helpers.record_function_results(scenario_id, messages)
    for _, message in ipairs(messages or {}) do
        if message.role == "function_result" and message.function_call_id then
            local text = message.content and message.content[1] and message.content[1].text
            local call_id = tostring(message.function_call_id)
            helpers.set_metric(scenario_id, "result_text:" .. call_id, tostring(text))
        end
    end
end

-- Records each request as the model received it, so tests can compare requests.
function helpers.record_request(scenario_id, request_number, messages)
    helpers.set_metric(scenario_id, "request:" .. tostring(request_number), json.encode(messages))
end

function helpers.get_request(scenario_id, request_number)
    local encoded = helpers.get_metric(scenario_id, "request:" .. tostring(request_number), nil)
    return encoded and json.decode(encoded) or nil
end

function helpers.conversation(request)
    local messages = {}
    for _, message in ipairs(request or {}) do
        if message.role ~= "cache_marker" then
            messages[#messages + 1] = message
        end
    end
    return messages
end

-- What the provider prefix binds: everything but the metadata, which carries
-- thinking blocks the agent may strip from older turns.
function helpers.signature(message)
    local copy = {}
    for key, value in pairs(message) do
        if key ~= "metadata" then copy[key] = value end
    end
    local encoded = json.encode(copy)
    return encoded
end

local function is_hoisted_system(message)
    return message.role == "system" and not (message.metadata and message.metadata.preserve_position)
end

-- Returns an error text unless later starts with every message of earlier. An
-- agent switch replaces the top-level system text by design, so options.ignore_system
-- compares the conversation without it.
function helpers.prefix_error(earlier, later, options)
    local before, after = helpers.conversation(earlier), helpers.conversation(later)
    if options and options.ignore_system then
        local keep_before, keep_after = {}, {}
        for _, message in ipairs(before) do
            if not is_hoisted_system(message) then keep_before[#keep_before + 1] = message end
        end
        for _, message in ipairs(after) do
            if not is_hoisted_system(message) then keep_after[#keep_after + 1] = message end
        end
        before, after = keep_before, keep_after
    end
    if #after <= #before then
        return "the later request is not longer: " .. #before .. " -> " .. #after
    end
    for index, message in ipairs(before) do
        if helpers.signature(after[index]) ~= helpers.signature(message) then
            return "message " .. index .. " changed: " .. helpers.signature(message)
                .. " -> " .. helpers.signature(after[index])
        end
    end
    return nil, after[#before + 1]
end

-- Returns an error text unless later extends earlier with the response to earlier
-- as the very next message: nothing can enter the history that was not sent.
function helpers.extension_error(earlier, later, options)
    local prefix_err, next_message = helpers.prefix_error(earlier, later, options)
    if prefix_err then
        return prefix_err
    end
    if next_message.role ~= "assistant" then
        return "expected the response right after the request, got " .. helpers.signature(next_message)
    end
    return nil
end

-- Returns an error text unless each function result answers a call of the same
-- turn, with no other message between them.
function helpers.pairing_error(request)
    local open = {}
    for index, message in ipairs(helpers.conversation(request)) do
        if message.role == "function_call" then
            open[message.function_call.id] = true
        elseif message.role == "function_result" then
            if not open[message.function_call_id] then
                return "message " .. index .. " is a result without its call: " .. tostring(message.function_call_id)
            end
            open[message.function_call_id] = nil
        elseif next(open) then
            return "message " .. index .. " sits between a call and its result: " .. helpers.signature(message)
        end
    end
    if next(open) then
        return "a call has no result: " .. tostring(next(open))
    end
    return nil
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

-- Read-modify-write on the store: exact for one writer at a time. Calls that
-- run concurrently count their attempts under per-call keys.
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
    helpers.set_metric(scenario_id, "last_checkpoint_max_tokens", 0)
    helpers.set_metric(scenario_id, "last_checkpoint_threshold", 0)
    helpers.set_metric(scenario_id, "lifecycle_activate", 0)
    helpers.set_metric(scenario_id, "lifecycle_before_step", 0)
    helpers.set_metric(scenario_id, "lifecycle_after_step", 0)
    helpers.set_metric(scenario_id, "lifecycle_deactivate", 0)
    helpers.set_metric(scenario_id, "lifecycle_switch_deactivate", 0)
    helpers.set_metric(scenario_id, "lifecycle_switch_deactivate_refs_present", 0)
    helpers.set_metric(scenario_id, "lifecycle_prompt_seen", 0)
    helpers.set_metric(scenario_id, "lifecycle_last_iteration", 0)
    helpers.set_metric(scenario_id, "finish_offered", 0)
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
        last_checkpoint_max_tokens = tonumber(
            helpers.get_metric(scenario_id, "last_checkpoint_max_tokens", 0)) or 0,
        last_checkpoint_threshold = tonumber(
            helpers.get_metric(scenario_id, "last_checkpoint_threshold", 0)) or 0,
        lifecycle_activate = tonumber(helpers.get_metric(scenario_id, "lifecycle_activate", 0)) or 0,
        lifecycle_before_step = tonumber(helpers.get_metric(scenario_id, "lifecycle_before_step", 0)) or 0,
        lifecycle_after_step = tonumber(helpers.get_metric(scenario_id, "lifecycle_after_step", 0)) or 0,
        lifecycle_deactivate = tonumber(helpers.get_metric(scenario_id, "lifecycle_deactivate", 0)) or 0,
        lifecycle_switch_deactivate = tonumber(
            helpers.get_metric(scenario_id, "lifecycle_switch_deactivate", 0)) or 0,
        lifecycle_switch_deactivate_refs_present = tonumber(
            helpers.get_metric(scenario_id, "lifecycle_switch_deactivate_refs_present", 0)) or 0,
        lifecycle_prompt_seen = tonumber(helpers.get_metric(scenario_id, "lifecycle_prompt_seen", 0)) or 0,
        lifecycle_last_iteration = tonumber(helpers.get_metric(scenario_id, "lifecycle_last_iteration", 0)) or 0,
        finish_offered = tonumber(helpers.get_metric(scenario_id, "finish_offered", 0)) or 0,
        tool_choice_any = tonumber(helpers.get_metric(scenario_id, "tool_choice_any", 0)) or 0,
        tool_choice_auto = tonumber(helpers.get_metric(scenario_id, "tool_choice_auto", 0)) or 0,
        tool_choice_fallback_auto = tonumber(helpers.get_metric(scenario_id, "tool_choice_fallback_auto", 0)) or 0,
        step1_effect = tonumber(helpers.get_metric(scenario_id,
            "effect_applied:" .. helpers.call_id(scenario_id, 1), 0)) or 0,
        step2_effect = tonumber(helpers.get_metric(scenario_id,
            "effect_applied:" .. helpers.call_id(scenario_id, 2), 0)) or 0,
        step1_attempts = tonumber(helpers.get_metric(scenario_id,
            "tool_attempts:" .. helpers.call_id(scenario_id, 1), 0)) or 0,
        step2_attempts = tonumber(helpers.get_metric(scenario_id,
            "tool_attempts:" .. helpers.call_id(scenario_id, 2), 0)) or 0,
        step1_started = tonumber(helpers.get_metric(scenario_id,
            "tool_started:" .. helpers.call_id(scenario_id, 1), 0)) or 0,
        step1_finished = tonumber(helpers.get_metric(scenario_id,
            "tool_finished:" .. helpers.call_id(scenario_id, 1), 0)) or 0,
        step2_started = tonumber(helpers.get_metric(scenario_id,
            "tool_started:" .. helpers.call_id(scenario_id, 2), 0)) or 0,
        step2_finished = tonumber(helpers.get_metric(scenario_id,
            "tool_finished:" .. helpers.call_id(scenario_id, 2), 0)) or 0
    }
end

return helpers
