local time = require("time")

local retry = {}
local fields = { "retry_policy", "start_to_close_timeout", "schedule_to_close_timeout" }
local MAX_SAFE_ATTEMPTS = 9007199254740991

local function finite(value)
    return type(value) == "number" and value == value and value ~= math.huge and value ~= -math.huge
end

local function duration(value, name)
    if value == nil then return nil, nil end
    if type(value) ~= "string" and not finite(value) then
        return nil, name .. " requires a duration string or finite millisecond number"
    end
    local parsed, err = time.parse_duration(type(value) == "number" and value * time.MILLISECOND or value)
    if err or not parsed or parsed:nanoseconds() <= 0 then
        return nil, name .. " must be a positive duration"
    end
    return parsed:nanoseconds() / time.MILLISECOND, nil
end

function retry.normalize(config)
    config = config or {}
    local normalized = {}
    for _, field in ipairs({ "start_to_close_timeout", "schedule_to_close_timeout" }) do
        local value, err = duration(config[field], field)
        if err then return nil, err end
        normalized[field] = value
    end
    if config.retry_policy == nil then return normalized, nil end
    if type(config.retry_policy) ~= "table" then return nil, "retry_policy must be a table" end
    local policy = config.retry_policy
    local initial, err = duration(policy.initial_interval == nil and "1s" or policy.initial_interval, "initial_interval")
    if err then return nil, err end
    initial = initial :: number
    local maximum, max_err = duration(policy.maximum_interval == nil and initial * 100 or policy.maximum_interval,
        "maximum_interval")
    if max_err then return nil, max_err end
    if maximum < initial then return nil, "maximum_interval must be at least initial_interval" end
    local coefficient = policy.backoff_coefficient == nil and 2.0 or policy.backoff_coefficient
    if not finite(coefficient) or coefficient < 1 then return nil, "backoff_coefficient must be finite and >= 1" end
    local attempts = policy.maximum_attempts == nil and 3 or policy.maximum_attempts
    if not finite(attempts) or attempts < 0 or attempts > MAX_SAFE_ATTEMPTS or attempts % 1 ~= 0 then
        return nil, "maximum_attempts must be a non-negative safe integer"
    end
    if attempts == 0 and normalized.schedule_to_close_timeout == nil then
        return nil, "maximum_attempts = 0 requires schedule_to_close_timeout"
    end
    local kinds = policy.non_retryable_error_types
    if kinds ~= nil and type(kinds) ~= "table" then
        return nil, "non_retryable_error_types must be an array of non-empty strings"
    end
    local copied_kinds = {}
    local kind_count = 0
    for i, kind in pairs(kinds or {}) do
        if type(i) ~= "number" or i % 1 ~= 0 or i < 1 or i > #kinds or type(kind) ~= "string" or kind == "" then
            return nil, "non_retryable_error_types must be an array of non-empty strings"
        end
        kind_count = kind_count + 1
        copied_kinds[i] = kind
    end
    if kinds and kind_count ~= #kinds then
        return nil, "non_retryable_error_types must be a dense array"
    end
    normalized.retry_policy = { initial_interval = initial, maximum_interval = maximum,
        backoff_coefficient = coefficient, maximum_attempts = attempts, non_retryable_error_types = copied_kinds }
    return normalized, nil
end

function retry.validate(config, node_type)
    config = config or {}
    if node_type ~= "func" and node_type ~= "userspace.dataflow.node.func:node" then
        for _, field in ipairs(fields) do
            if config[field] ~= nil then return nil, field .. " is supported only on func nodes" end
        end
        return true, nil
    end
    local normalized, err = retry.normalize(config)
    if err then return nil, err end
    return normalized ~= nil, nil
end

function retry.extract_error(err)
    if type(err) == "userdata" then
        return {kind = err:kind(), message = err:message(), retryable = err:retryable(), details = err:details()}
    end
    if type(err) == "table" then
        return {kind = err.kind, message = err.message or "Function execution failed",
            retryable = err.retryable, details = err.details}
    end
    return {message = tostring(err or "Function execution failed")}
end

function retry.is_retryable(policy, outcome)
    if outcome.kind == errors.CANCELED then return false end
    if outcome.retryable == false then return false end
    for _, kind in ipairs(policy.non_retryable_error_types) do
        if outcome.kind == kind then return false end
    end
    return true
end

function retry.next_delay(policy, attempt)
    return math.min(policy.initial_interval * policy.backoff_coefficient ^ (attempt - 1), policy.maximum_interval)
end

-- All times and delays are milliseconds. The caller owns clocks and persistence.
function retry.decide(config, outcome, attempt, first_attempt_at, now)
    local policy = config.retry_policy
    if not policy or not retry.is_retryable(policy, outcome) then return {retry = false} end
    if policy.maximum_attempts > 0 and attempt >= policy.maximum_attempts then return {retry = false} end
    local delay = retry.next_delay(policy, attempt)
    local next_at = now + delay
    if config.schedule_to_close_timeout and next_at > first_attempt_at + config.schedule_to_close_timeout then
        return {retry = false}
    end
    return {retry = true, delay = delay, next_at = next_at}
end

return retry
