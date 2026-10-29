local time = require("time")

local function run(input)
    if input.rich_error then
        return nil, errors.new({ kind = errors.PERMISSION_DENIED,
            message = "Function access denied", retryable = false })
    end
    time.sleep(input.delay)
    return input.value
end

local function exit()
    process.listen("exit"):receive()
    time.sleep("30ms")
    return true
end

return { run = run, exit = exit }
