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

    local consts = signal._deps.consts

    -- read inputs (passthrough from upstream node)
    local inputs, inputs_err = nil, nil
    local ok_inputs, inputs_or_err, ie = pcall(function() return n:inputs() end)
    if ok_inputs then
        inputs = inputs_or_err
        inputs_err = ie
    end

    -- publish signal metadata so external code can discover the signal_id
    n:data(consts.DATA_TYPE.NODE_SIGNAL, {
        signal_id = signal_id,
        node_id = n.node_id,
        dataflow_id = n.dataflow_id,
        status = "waiting",
    }, { key = "signal_meta_" .. signal_id })

    -- yield with wait_for_signal flag; orchestrator will not reply until
    -- an external commit delivers NODE_SIGNAL data with matching signal_id
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

    -- merge upstream inputs with signal data if both exist
    local output = results
    if type(results) == "table" and inputs and next(inputs) then
        local merged = {}
        for key, input in pairs(inputs) do
            if key ~= "" then
                merged[key] = input.content
            end
        end
        merged._signal = results
        output = merged
    end

    return n:complete(output, "Signal received: " .. signal_id)
end

signal.run = run
return signal
