local test = require("test")
local retry = require("retry")

local function define_tests()
    describe("Func retry policy", function()
        it("preserves the opt-in boundary and normalizes Temporal defaults and durations", function()
            local config, err = retry.normalize({})
            test.is_nil(err)
            test.is_nil(config.retry_policy)
            config, err = retry.normalize({retry_policy = {}, start_to_close_timeout = "30s", schedule_to_close_timeout = "6h"})
            test.is_nil(err)
            test.eq(config.start_to_close_timeout, 30000)
            test.eq(config.schedule_to_close_timeout, 21600000)
            test.eq(config.retry_policy.initial_interval, 1000)
            test.eq(config.retry_policy.maximum_interval, 100000)
            test.eq(config.retry_policy.backoff_coefficient, 2)
            test.eq(config.retry_policy.maximum_attempts, 0)
            test.eq(#config.retry_policy.non_retryable_error_types, 0)
            local numeric = retry.normalize({retry_policy = {initial_interval = 12.5}, start_to_close_timeout = 250})
            test.eq(numeric.retry_policy.maximum_interval, 1250)
            test.eq(numeric.start_to_close_timeout, 250)
        end)

        for _, case in ipairs({
            {field = "initial_interval", value = "bad"}, {field = "initial_interval", value = 0},
            {field = "initial_interval", value = -1}, {field = "initial_interval", value = true},
            {field = "maximum_interval", value = "1ms"}, {field = "maximum_interval", value = math.huge},
            {field = "backoff_coefficient", value = 0.9}, {field = "backoff_coefficient", value = "2"},
            {field = "backoff_coefficient", value = 0/0}, {field = "maximum_attempts", value = -1},
            {field = "maximum_attempts", value = 1.5}, {field = "maximum_attempts", value = "3"},
            {field = "non_retryable_error_types", value = "Invalid"},
            {field = "non_retryable_error_types", value = {false}},
            {field = "non_retryable_error_types", value = {""}},
            {field = "non_retryable_error_types", value = {Invalid = true}},
        }) do
            it("rejects invalid " .. case.field .. "=" .. tostring(case.value), function()
                local _, err = retry.normalize({retry_policy = {[case.field] = case.value}})
                test.not_nil(err)
                test.contains(err, case.field)
            end)
        end
        it("rejects invalid policy and timeout declarations", function()
            test.not_nil(select(2, retry.normalize({retry_policy = false})))
            for _, field in ipairs({"start_to_close_timeout", "schedule_to_close_timeout"}) do
                for _, value in ipairs({"bad", "0s", -1, {}, math.huge}) do
                    local _, err = retry.normalize({[field] = value})
                    test.not_nil(err)
                    test.contains(err, field)
                end
            end
        end)

        it("computes exponential delays, caps, constant intervals and very large attempt numbers", function()
            local config = retry.normalize({retry_policy = {initial_interval = 100, maximum_interval = 500}})
            for attempt, expected in ipairs({100, 200, 400, 500, 500}) do
                test.eq(retry.next_delay(config.retry_policy, attempt), expected)
            end
            test.eq(retry.next_delay(config.retry_policy, 100000), 500)
            config = retry.normalize({retry_policy = {backoff_coefficient = 1}})
            test.eq(retry.next_delay(config.retry_policy, 100000), 1000)
        end)

        for _, case in ipairs({
            {kind = "Canceled", retryable = true, expected = false},
            {kind = "Unavailable", retryable = false, expected = false},
            {kind = "Invalid", retryable = true, expected = false},
            {kind = "Timeout", retryable = true, expected = true},
            {kind = "Unavailable", expected = true},
            {expected = true},
        }) do
            it("classifies " .. tostring(case.kind) .. " retryable=" .. tostring(case.retryable), function()
                local config = retry.normalize({retry_policy = {non_retryable_error_types = {"Invalid"}}})
                local cause = errors.new({kind = case.kind or "Internal", message = "Call failed", retryable = case.retryable,
                    details = {code = "DETAIL"}})
                local extracted = retry.extract_error(cause)
                test.eq(extracted.kind, case.kind or "Internal")
                test.eq(extracted.retryable, case.retryable)
                test.eq(extracted.details.code, "DETAIL")
                test.eq(retry.is_retryable(config.retry_policy, extracted), case.expected)
            end)
        end

        it("stops at attempt limits and the next-start schedule deadline with no jitter", function()
            local config = retry.normalize({retry_policy = {initial_interval = 100, maximum_attempts = 3},
                schedule_to_close_timeout = 1000})
            local cause = {kind = "Unavailable"}
            test.eq(retry.decide(config, cause, 1, 1000, 1000).next_at, 1100)
            test.eq(retry.decide(config, cause, 2, 1000, 1000).delay, 200)
            test.is_false(retry.decide(config, cause, 3, 1000, 1000).retry)
            test.is_true(retry.decide(config, cause, 1, 1000, 1900).retry)
            test.is_false(retry.decide(config, cause, 1, 1000, 1901).retry)
            config.retry_policy.maximum_attempts = 0
            config.schedule_to_close_timeout = nil
            test.is_true(retry.decide(config, cause, 100000, 0, 0).retry)
            test.is_false(retry.decide({}, cause, 1, 0, 0).retry)
        end)
    end)
end

return test.run_cases(define_tests)
