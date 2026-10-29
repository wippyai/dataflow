local test = require("test")
local func = require("func")
local time = require("time")

local function define_tests()
    describe("Func durable attempts", function()
        local original = {}
        before_each(function()
            for _, name in ipairs({"node", "funcs", "process", "channel", "time"}) do original[name] = func._deps[name] end
        end)
        after_each(function()
            for name, value in pairs(original) do func._deps[name] = value end
        end)

        local function fixture(config, metadata)
            local state: any = {metadata = metadata or {}, now = 1000000, calls = {}, waits = {}, reservations = {}, failures = 0}
            func._deps.time = {MILLISECOND = time.MILLISECOND, RFC3339NANO = time.RFC3339NANO, unix = time.unix,
                now = function() return time.unix(0, state.now * time.MILLISECOND) end,
                after = function(duration)
                    state.timeout_duration = duration
                    return state.timeout_channel
                end}
            local n: any = {dataflow_id = "workflow", node_id = "node"}
            n.config = function() return config end
            n.metadata = function() return state.metadata end
            n.inputs = function() return {default = {content = {}}} end
            n.update_metadata = function(_, updates)
                for key, value in pairs(updates) do state.metadata[key] = value end
                return n
            end
            n.submit = function()
                local record = state.metadata.func_retry
                table.insert(state.reservations, {attempt = record.attempt, first_attempt_at = record.first_attempt_at,
                    in_flight = record.in_flight, error = state.metadata.error, status_message = state.metadata.status_message})
                return true, nil
            end
            n.yield = function(_, options)
                test.is_false(state.metadata.func_retry.in_flight)
                table.insert(state.waits, options)
                if state.park then return nil, nil end
                state.now = time.parse(time.RFC3339NANO, options.timeout_deadline):unix_nano() / time.MILLISECOND
                return {timer_completed = true}, nil
            end
            n.fail = function(_, failure)
                state.failures = state.failures + 1
                return {success = false, error = failure}
            end
            n.complete = function(_, output) return {success = true, output = output} end
            func._deps.node = {new = function() return n end}
            state.response = {case_receive = function() return {} end}
            state.events = {case_receive = function() return {} end}
            state.timeout_channel = {case_receive = function() return {} end}
            func._deps.process = {event = {CANCEL = "cancel"}, events = function() return state.events end}
            func._deps.channel = {select = function()
                if state.cancel then return {channel = state.events, ok = true, value = {kind = "cancel"}} end
                if state.timeout_attempt == #state.calls then return {channel = state.timeout_channel} end
                return {channel = state.response}
            end}
            func._deps.funcs = {new = function()
                local context
                return {with_context = function(self, value) context = value; return self end,
                    async = function()
                        if config.retry_policy then
                            test.eq(#state.reservations, #state.calls + 1, "reservation commits before dispatch")
                            test.is_true(state.metadata.func_retry.in_flight)
                        end
                        table.insert(state.calls, {attempt = context.attempt, key = context.idempotency_key, at = state.now})
                        return {response = function() return state.response end,
                            cancel = function() state.canceled = true end, is_canceled = function() return false end,
                            result = function()
                                if #state.calls <= (state.fail_attempts or 0) then
                                    return nil, errors.new({kind = state.kind or "Unavailable", message = "Call failed",
                                        retryable = state.retryable, details = {code = "CALL_FAILED"}})
                                end
                                return {data = function() return {value = "done"} end}
                            end}
                    end}
            end}
            return state
        end

        local function config(policy)
            return {func_id = "example:call", retry_policy = policy or {initial_interval = 100, maximum_attempts = 4}}
        end

        it("retries unknown outcomes with exact exponential delays and stable context", function()
            local state = fixture(config())
            state.fail_attempts = 2
            local result = func.run({})
            test.is_true(result.success)
            test.eq(result.output.value, "done")
            test.eq(#state.calls, 3)
            test.eq(state.calls[2].at - state.calls[1].at, 100)
            test.eq(state.calls[3].at - state.calls[2].at, 200)
            for i, call in ipairs(state.calls) do
                test.eq(call.attempt, i)
                test.eq(call.key, "workflow:node")
            end
            test.eq(state.failures, 0)
            test.is_false(state.metadata.func_retry)
        end)

        it("makes one final structured failure on exhaustion", function()
            local state = fixture(config({maximum_attempts = 3, initial_interval = 100}))
            state.fail_attempts = 10
            local result = func.run({})
            test.is_false(result.success)
            test.eq(#state.calls, 3)
            test.eq(#state.waits, 2)
            test.eq(state.failures, 1)
            test.eq(result.error.attempts, 3)
            test.eq(result.error.cause.kind, "Unavailable")
            test.eq(result.error.last_error.details.code, "CALL_FAILED")
            test.eq(state.metadata.func_retry.attempt, 3)
            test.is_false(state.metadata.func_retry.in_flight)
        end)

        for _, case in ipairs({{name = "native flag", retryable = false}, {name = "kind list", kind = "Invalid"},
            {name = "cancellation", cancel = true}}) do
            it("does not retry " .. case.name, function()
                local state = fixture(config({non_retryable_error_types = {"Invalid"}}))
                state.fail_attempts, state.retryable, state.kind, state.cancel = 10, case.retryable, case.kind, case.cancel
                local result = func.run({})
                test.is_false(result.success)
                test.eq(result.error.attempts, 1)
                test.eq(#state.calls, 1)
                test.eq(#state.waits, 0)
                test.eq(state.failures, 1)
                if case.cancel then test.is_true(state.canceled) end
            end)
        end

        it("cancels timed-out attempts and retries Timeout before success", function()
            local declaration = config()
            declaration.start_to_close_timeout = 50
            local state = fixture(declaration)
            state.timeout_attempt = 1
            local result = func.run({})
            test.is_true(result.success)
            test.is_true(state.canceled)
            test.eq(#state.calls, 2)
            test.eq(state.timeout_duration, 50 * time.MILLISECOND)
        end)

        it("stops when the next backoff is outside schedule-to-close", function()
            local declaration = config()
            declaration.schedule_to_close_timeout = 200
            local state = fixture(declaration)
            state.fail_attempts = 10
            local result = func.run({})
            test.is_false(result.success)
            test.eq(result.error.attempts, 2)
            test.eq(#state.calls, 2)
            test.eq(#state.waits, 1)
            test.eq(state.failures, 1)
        end)

        it("counts an interrupted reservation exactly once and backs off before dispatch", function()
            local state = fixture(config(), {func_retry = {attempt = 2, first_attempt_at = 999000, in_flight = true}})
            state.park = true
            test.is_nil(func.run({}))
            test.eq(#state.calls, 0)
            test.eq(#state.waits, 1)
            test.eq(state.metadata.func_retry.attempt, 2)
            test.eq(state.metadata.func_retry.last_error.kind, "Unavailable")
            test.contains(state.metadata.func_retry.last_error.message, "interrupted")
            local deadline = state.metadata.func_retry.wait_until
            state.now = state.now + 20
            test.is_nil(func.run({}))
            test.eq(state.metadata.func_retry.wait_until, deadline)
            test.eq(state.waits[2].timeout_deadline, deadline)
            test.eq(state.metadata.func_retry.attempt, 2)
            state.park = false
            local result = func.run({})
            test.is_true(result.success)
            test.eq(#state.calls, 1)
            test.eq(state.calls[1].attempt, 3)
            test.eq(state.calls[1].key, "workflow:node")
            test.eq(state.reservations[1].first_attempt_at, 999000)
        end)

        it("exhausts an interrupted final reservation without dispatching", function()
            local state = fixture(config({maximum_attempts = 2}), {func_retry = {
                attempt = 2, first_attempt_at = 999000, in_flight = true}})
            local result = func.run({})
            test.is_false(result.success)
            test.eq(result.error.attempts, 2)
            test.eq(#state.calls, 0)
            test.eq(#state.waits, 0)
        end)

        it("does not retry input transforms, validation or failure after a successful call", function()
            for _, stage in ipairs({"inputs", "validation", "completion"}) do
                local declaration = config()
                if stage == "validation" then declaration.retry_policy.maximum_attempts = -1 end
                local state = fixture(declaration)
                local new_node = func._deps.node.new
                func._deps.node.new = function()
                    local n = new_node({})
                    if stage == "inputs" then
                        n.inputs = function() return nil, {code = "INPUT_VALIDATION_FAILED", message = "Invalid input"} end
                    elseif stage == "completion" then
                        n.complete = function() return n:fail({code = "ROUTING_FAILED", message = "Invalid output route"}) end
                    end
                    return n
                end
                local result = func.run({})
                test.is_false(result.success)
                test.eq(#state.calls, stage == "completion" and 1 or 0)
                test.eq(#state.waits, 0)
                test.eq(state.failures, 1)
            end
        end)

        it("does not dispatch when a recovered backoff wakes after schedule-to-close", function()
            local declaration = config()
            declaration.schedule_to_close_timeout = 100
            local state = fixture(declaration, {func_retry = {attempt = 1, first_attempt_at = 999000,
                in_flight = false, wait_until = time.unix(0, 999100 * time.MILLISECOND):format(time.RFC3339NANO),
                last_error = {kind = "Unavailable", message = "Call failed"}}})
            local yield_node = func._deps.node.new
            func._deps.node.new = function()
                local n = yield_node({})
                n.yield = function() return {timer_completed = true} end
                return n
            end
            local result = func.run({})
            test.is_false(result.success)
            test.eq(result.error.attempts, 1)
            test.eq(#state.calls, 0)
        end)

        it("includes reservation persistence time in schedule-to-close", function()
            local declaration = config()
            declaration.schedule_to_close_timeout = 100
            local state = fixture(declaration)
            local new_node = func._deps.node.new
            func._deps.node.new = function()
                local n = new_node({})
                local submit = n.submit
                n.submit = function()
                    state.now = state.now + 200
                    return submit()
                end
                return n
            end
            local result = func.run({})
            test.is_false(result.success)
            test.eq(result.error.cause.kind, "Timeout")
            test.eq(result.error.attempts, 1)
            test.eq(#state.calls, 0)
        end)

        for _, stage in ipairs({"reservation", "wait", "inputs", "missing_func", "missing_input", "policy"}) do
            it("clears an active reservation on non-call failure at " .. stage, function()
                local declaration = config()
                if stage == "missing_func" then declaration.func_id = nil end
                if stage == "policy" then declaration.retry_policy.maximum_attempts = -1 end
                local state = fixture(declaration, {func_retry = {
                    attempt = 1, first_attempt_at = 999000, in_flight = true,
                }})
                local new_node = func._deps.node.new
                func._deps.node.new = function()
                    local n = new_node({})
                    if stage == "reservation" then
                        state.metadata.func_retry = false
                        n.submit = function() return nil, "Reservation rejected" end
                    elseif stage == "wait" then
                        n.yield = function() return nil, "Wait rejected" end
                    elseif stage == "inputs" then
                        n.inputs = function() return nil, "Inputs rejected" end
                    elseif stage == "missing_input" then
                        n.inputs = function() return {} end
                    end
                    return n
                end
                local result = func.run({})
                test.is_false(result.success)
                test.is_false(state.metadata.func_retry, "terminal non-call failure clears the reservation")
                test.eq(state.failures, 1)
                test.eq(#state.calls, 0)
            end)
        end

        it("retires earlier terminal diagnostics before reserving a new call", function()
            local state = fixture(config(), {error = "Earlier failure", status_message = "Earlier failure"})
            test.is_true(func.run({}).success)
            test.is_false(state.reservations[1].error)
            test.is_false(state.reservations[1].status_message)
        end)

        it("keeps default single-attempt restart behavior without a policy", function()
            local state = fixture({func_id = "example:call"})
            state.fail_attempts = 10
            test.is_false(func.run({}).success)
            test.eq(#state.calls, 1)
            test.eq(#state.reservations, 0)
            test.eq(#state.waits, 0)
        end)

        it("preserves caller context when no retry policy is selected", function()
            local state = fixture({func_id = "example:call", context = {
                attempt = 41, idempotency_key = "caller-owned-key",
            }})
            test.is_true(func.run({}).success)
            test.eq(#state.calls, 1)
            test.eq(state.calls[1].attempt, 41)
            test.eq(state.calls[1].key, "caller-owned-key")
            test.eq(#state.reservations, 0)
        end)
    end)
end

return test.run_cases(define_tests)
