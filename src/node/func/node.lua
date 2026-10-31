local func = {}

local ERROR_MISSING_FUNC_ID = "MISSING_FUNC_ID"
local ERROR_NO_INPUT_DATA = "NO_INPUT_DATA"
local ERROR_FUNCTION_CANCELED = "FUNCTION_CANCELED"
local ERROR_FUNCTION_EXECUTION_FAILED = "FUNCTION_EXECUTION_FAILED"

func._deps = {
    time = require("time"),
    retry = require("retry"),
    process = process,
    channel = channel,
    node = require("node"),
    funcs = require("funcs"),
    consts = require("consts"),
    child_output = require("child_output")
}

local function fail_node(n, details, message)
    local reservation = (n:metadata() or {}).func_retry
    if type(reservation) == "table" and (reservation.in_flight or reservation.wait_until) then
        n:update_metadata({func_retry = false})
    end
    return n:fail(details, message)
end

local function error_message(error_value: any, fallback: string)
    if type(error_value) == "string" then
        return error_value
    end
    if type(error_value) == "table" then
        local error_table = error_value :: any
        if type(error_table.message) == "string" then
            return error_table.message
        end
        if type(error_table.status) == "string" then
            return error_table.status
        end
        if error_table.error ~= nil then
            return error_message(error_table.error, fallback)
        end
        if type(error_table.code) == "string" then
            return error_table.code
        end
        return fallback
    end
    if error_value == nil then
        return fallback
    end
    return tostring(error_value)
end

local function prefixed_error(prefix: string, error_value: any, fallback: string)
    if type(error_value) == "table" then
        return error_value
    end
    return prefix .. error_message(error_value, fallback)
end

local function build_execution_context_with_dataflow(base_context, dataflow_id, node_id, path)
    local execution_context = {}

    if base_context then
        for k, v in pairs(base_context) do
            execution_context[k] = v
        end
    end

    execution_context.dataflow_id = dataflow_id
    execution_context.node_id = node_id
    execution_context.path = path or {}
    execution_context.runtime_type = "dataflow_function"
    execution_context.execution_timestamp = os.time()
    execution_context.parent_node_id = node_id

    return execution_context
end

local function merge_inputs_with_base_args(inputs, base_args)
    if not inputs or next(inputs) == nil then
        if not base_args then
            return nil
        end
        return base_args
    end

    if not base_args then
        if inputs["default"] and inputs["default"].content ~= nil then
            return inputs["default"].content
        end

        if inputs[""] and inputs[""].content ~= nil then
            return inputs[""].content
        end

        local input_count = 0
        for _ in pairs(inputs) do
            input_count = input_count + 1
        end

        if input_count == 1 then
            for key, input in pairs(inputs) do
                local discriminator = input.discriminator

                if discriminator == nil or discriminator == "" or discriminator == "default" then
                    return input.content
                end

                return {
                    [discriminator or key] = input.content
                }
            end
        end

        local result = {}
        for key, input in pairs(inputs) do
            result[key] = input.content
        end
        return result
    end

    local result = {}
    for k, v in pairs(base_args) do
        result[k] = v
    end

    for discriminator, input_data in pairs(inputs) do
        if discriminator ~= "" and discriminator ~= "default" then
            result[discriminator] = input_data.content
        end
    end

    return result
end

local function safe_inputs(n)
    local ok, inputs_or_err, inputs_err = pcall(function()
        return n:inputs()
    end)

    if not ok then
        return nil, inputs_or_err
    end

    if inputs_err then
        return nil, inputs_err
    end

    return inputs_or_err, nil
end

local function build_child_output_result(output_data)
    if output_data and #output_data > 0 then
        for _, output in ipairs(output_data) do
            if output.discriminator == "error" then
                local error_content = output.content
                return nil, {
                    code = "CHILD_WORKFLOW_FAILED",
                    message = type(error_content) == "string" and error_content or "Child workflow failed",
                    status = "Child workflow failed"
                }
            end
        end

        if #output_data == 1 then
            return {
                _dataflow_ref = output_data[1].data_id
            }, nil
        else
            local ref_array = {}
            for _, output in ipairs(output_data) do
                table.insert(ref_array, {
                    _dataflow_ref = output.data_id
                })
            end
            return ref_array, nil
        end
    end

    return nil, nil
end

local function collect_child_result(n, child_node_ids, yield_result)
    local output_data, collect_err = func._deps.child_output.collect_outputs(n, child_node_ids, yield_result)
    if collect_err then
        if type(collect_err) == "table" then
            return nil, collect_err
        end
        return nil, prefixed_error("Failed to collect child outputs: ", collect_err, "unknown")
    end

    return build_child_output_result(output_data)
end

local function fail_child_collect(n, collect_err)
    if type(collect_err) == "table" then
        return fail_node(n, collect_err, error_message(collect_err, "Child workflow failed"))
    end

    return fail_node(n, {
        code = ERROR_FUNCTION_EXECUTION_FAILED,
        message = error_message(collect_err, "Child workflow failed")
    }, error_message(collect_err, "Child workflow failed"))
end

local function complete_from_children(n, child_node_ids, fallback_result)
    local final_output, collect_err = func._deps.child_output.resume_children(n, child_node_ids, collect_child_result)
    if collect_err then
        return fail_child_collect(n, collect_err)
    end
    if final_output ~= nil then
        return (n:complete(final_output, "Function executed successfully"))
    end

    return (n:complete(fallback_result, "Function executed successfully"))
end

-- Checkpoints child ids before yielding so recovery resumes the same children
-- instead of re-running the function and creating duplicate children.
local function finish_with_children(n, created_node_ids, fallback_result)
    n:update_metadata({ func_pending = { child_node_ids = created_node_ids } })

    local yield_result, yield_err = n:yield({ run_nodes = created_node_ids })
    if yield_err then
        if type(yield_err) == "table" then
            return fail_node(n, yield_err, "Control command execution failed")
        end
        return fail_node(n, {
            code = ERROR_FUNCTION_EXECUTION_FAILED,
            message = prefixed_error("Control command execution failed: ", yield_err, "unknown")
        }, "Control command execution failed")
    end

    local final_output, collect_err = collect_child_result(n, created_node_ids, yield_result)
    if collect_err then
        return fail_child_collect(n, collect_err)
    end
    if final_output ~= nil then
        return (n:complete(final_output, "Function executed successfully"))
    end

    return (n:complete(fallback_result, "Function executed successfully"))
end

local function now_ms()
    return func._deps.time.now():unix_nano() / func._deps.time.MILLISECOND
end

local function call_once(executor, func_id, input_data, timeout_ms)
    local command, start_err = executor:async(func_id, input_data)
    if start_err then return nil, start_err end

    local response_channel = command:response()
    local events_channel = func._deps.process.events()
    local timeout_channel = timeout_ms and func._deps.time.after(math.max(timeout_ms * func._deps.time.MILLISECOND, 1))
    local receive_cases = { response_channel:case_receive(), events_channel:case_receive() }
    if timeout_channel then table.insert(receive_cases, timeout_channel:case_receive()) end

    while true do
        local result = func._deps.channel.select(receive_cases)
        if result.channel == response_channel then break end
        if result.channel == timeout_channel then
            command:cancel()
            return nil, {kind = errors.TIMEOUT, message = "Function attempt timed out", retryable = true}
        end
        if result.channel == events_channel then
            if not result.ok then
                -- Keep the timeout case dense when the event channel closes.
                receive_cases = {response_channel:case_receive()}
                if timeout_channel then table.insert(receive_cases, timeout_channel:case_receive()) end
            elseif result.value.kind == func._deps.process.event.CANCEL then
                command:cancel()
                return nil, {kind = errors.CANCELED, message = "Function execution was canceled by system event"}
            end
        end
    end
    if command:is_canceled() then
        return nil, {kind = errors.CANCELED, message = "Function execution was canceled"}
    end
    return command:result()
end

local function final_call_failure(n, cause, record)
    local canceled = type(cause) == "table" and cause.kind == errors.CANCELED
    if not record then
        local message = type(cause) == "table" and cause.message or cause
        return fail_node(n, {code = canceled and ERROR_FUNCTION_CANCELED or ERROR_FUNCTION_EXECUTION_FAILED,
            message = message, cause = type(cause) == "table" and cause or nil},
            canceled and "Function execution was canceled" or "Function execution failed: " .. tostring(message))
    end
    n:update_metadata({func_retry = {attempt = record.attempt, first_attempt_at = record.first_attempt_at,
        in_flight = false, last_error = cause}})
    return fail_node(n, {code = canceled and ERROR_FUNCTION_CANCELED or ERROR_FUNCTION_EXECUTION_FAILED,
        message = cause.message, cause = cause, attempts = record.attempt, last_error = cause},
        "Function execution failed: " .. cause.message)
end

local function execute_call(n, config, normalized, input_data)
    local policy = normalized.retry_policy
    local record = policy and (n:metadata() or {}).func_retry or nil
    if type(record) ~= "table" then record = nil end
    local outcome = nil
    if record and record.in_flight then
        outcome = {kind = errors.UNAVAILABLE, retryable = true, message = "Function attempt interrupted before its outcome is committed"}
    end

    while true do
        if outcome then
            local decision = func._deps.retry.decide(normalized, outcome, record.attempt, record.first_attempt_at, now_ms())
            if not decision.retry then return nil, final_call_failure(n, outcome, record) end
            record = {attempt = record.attempt, first_attempt_at = record.first_attempt_at,
                in_flight = false, last_error = outcome,
                wait_until = func._deps.time.unix(0, decision.next_at * func._deps.time.MILLISECOND):format(func._deps.time.RFC3339NANO)}
            n:update_metadata({func_retry = record})
            outcome = nil
        end
        if record and record.wait_until then
            local yield_id = n.node_id .. ":retry:" .. tostring(record.attempt)
            local resumed, wait_err = n:yield({wait_for_timer = true, timeout_deadline = record.wait_until,
                yield_id = yield_id})
            if wait_err then
                return nil, fail_node(n, {code = "RETRY_WAIT_FAILED", message = tostring(wait_err)}, "Retry wait failed")
            end
            if not resumed then return nil, nil end
        end

        local started_at = now_ms()
        local first_attempt_at = record and record.first_attempt_at or started_at
        local remaining = normalized.schedule_to_close_timeout and
            (first_attempt_at + normalized.schedule_to_close_timeout - started_at)
        if remaining and remaining < 0 then
            local cause = record and record.last_error or {kind = errors.TIMEOUT, message = "Function schedule-to-close timeout elapsed"}
            return nil, final_call_failure(n, cause, record)
        end
        local attempt = record and record.attempt + 1 or 1
        if policy then
            record = {attempt = attempt, first_attempt_at = first_attempt_at, in_flight = true}
            n:update_metadata({func_retry = record, error = false, status_message = false})
            local submitted, submit_err = n:submit()
            if not submitted then
                return nil, fail_node(n, {code = "RETRY_RESERVATION_FAILED", message = tostring(submit_err)}, "Attempt reservation failed")
            end
        end
        if normalized.schedule_to_close_timeout then
            remaining = first_attempt_at + normalized.schedule_to_close_timeout - now_ms()
            if remaining < 0 then
                return nil, final_call_failure(n, {kind = errors.TIMEOUT, message = "Function schedule-to-close timeout elapsed"}, record)
            end
        end
        local execution_context = build_execution_context_with_dataflow(config.context, n.dataflow_id, n.node_id, n.path)
        if policy then
            execution_context.attempt = attempt
            execution_context.idempotency_key = n.dataflow_id .. ":" .. n.node_id
        end
        local executor = func._deps.funcs.new():with_context(execution_context)
        local timeout = normalized.start_to_close_timeout
        if remaining then timeout = math.min(timeout or remaining, remaining) end
        local payload, call_err = call_once(executor, config.func_id, input_data, timeout)
        if not call_err then
            if policy then n:update_metadata({func_retry = false}) end
            return payload, nil
        end
        if not policy then return nil, final_call_failure(n, call_err, nil) end
        outcome = func._deps.retry.extract_error(call_err)
    end
end

local function run(args)
    local n, err = func._deps.node.new(args) :: any
    if err then
        error(err)
    end

    local config = n:config()

    local pending = (n:metadata() or {}).func_pending
    if type(pending) == "table" and type(pending.child_node_ids) == "table" and #pending.child_node_ids > 0 then
        return complete_from_children(n, pending.child_node_ids, {})
    end
    local func_id = config.func_id
    if not func_id or func_id == "" then
        return fail_node(n, {
            code = ERROR_MISSING_FUNC_ID,
            message = "Function ID not specified in node configuration"
        }, "Missing func_id in node config")
    end

    local base_args = config.args

    local inputs, inputs_err = safe_inputs(n)
    if inputs_err then
        if type(inputs_err) == "table" then
            return fail_node(n, inputs_err, error_message(inputs_err, "Failed to load inputs"))
        end
        return fail_node(n, {
            code = "INPUT_VALIDATION_FAILED",
            message = inputs_err
        }, inputs_err)
    end

    local input_data = merge_inputs_with_base_args(inputs, base_args)

    if input_data == nil then
        return fail_node(n, {
            code = ERROR_NO_INPUT_DATA,
            message = "No input data provided for function node"
        }, "Function node requires input data")
    end

    local normalized, validation_err = func._deps.retry.normalize(config)
    if validation_err then
        return fail_node(n, {code = "INVALID_RETRY_POLICY", message = validation_err}, validation_err)
    end
    local payload, failure = execute_call(n, config, normalized, input_data)
    if not payload then return failure end

    local function_result = payload:data()

    if type(function_result) == "table" and function_result._control then
        local control = function_result._control
        local created_node_ids = {}

        if control.commands and type(control.commands) == "table" then
            for _, cmd in ipairs(control.commands) do
                if cmd.type and cmd.payload then
                    if cmd.type == func._deps.consts.COMMAND_TYPES.CREATE_NODE then
                        if not cmd.payload.parent_node_id then
                            cmd.payload.parent_node_id = n.node_id
                        end

                        if cmd.payload.node_id then
                            table.insert(created_node_ids, cmd.payload.node_id)
                        end
                    end

                    n:command(cmd)
                end
            end
        end

        local cleaned_result = {}
        for k, v in pairs(function_result) do
            if k ~= "_control" then
                cleaned_result[k] = v
            end
        end

        if #created_node_ids > 0 then
            return finish_with_children(n, created_node_ids, cleaned_result)
        end

        return (n:complete(cleaned_result, "Function executed successfully"))
    end

    return (n:complete(function_result, "Function executed successfully"))
end

func.run = run
return func
