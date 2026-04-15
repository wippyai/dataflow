local uuid = require("uuid")
local json = require("json")

local signal = {}

signal._deps = {
    node = require("node"),
    consts = require("consts")
}

local function run(args)
    local n, err = signal._deps.node.new(args)
    if err then
        error(err)
    end

    local config = n:config()
    local signal_id = config.signal_id
    if not signal_id or signal_id == "" then
        signal_id = uuid.v7()
    end

    -- yield with wait_for_signal flag
    -- orchestrator tracks the yield and waits for external NODE_SIGNAL commit
    -- this blocks until the signal arrives or the process is terminated
    local results, yield_err = n:yield({
        wait_for_signal = true,
        signal_id = signal_id,
    })

    if yield_err then
        return n:fail({
            code = "SIGNAL_YIELD_FAILED",
            message = tostring(yield_err)
        }, "Signal yield failed: " .. tostring(yield_err))
    end

    -- nil results means the channel was closed (graceful shutdown)
    -- the yield state is persisted, so on restart it resumes
    if results == nil then
        return 0
    end

    return n:complete(results, "Signal received: " .. signal_id)
end

signal.run = run
return signal
