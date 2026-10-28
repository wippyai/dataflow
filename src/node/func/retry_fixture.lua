local ctx = require("ctx")
local time = require("time")

local function run(input)
    local attempt = ctx.get("attempt") or 1
    if input.timeout_attempts and attempt <= input.timeout_attempts then time.sleep("200ms") end
    if attempt <= (input.fail_attempts or 0) then
        return nil, errors.new({kind = input.kind or "Unavailable", message = "Call failed",
            retryable = input.retryable, details = {code = "CALL_FAILED"}})
    end
    return {attempt = attempt, idempotency_key = ctx.get("idempotency_key"), value = input.value}
end

return {run = run}
