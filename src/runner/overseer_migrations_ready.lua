local M = {}
local uuid = require("uuid")

local RUNTIME_EPOCH_ENV = "userspace.dataflow.env:runtime_epoch"

local function run()
    -- Memory-backed epoch state lives for exactly one application runtime.
    -- A restarted overseer reads the same epoch and therefore cannot mistake
    -- its own crash for a full application reboot.
    local epoch = uuid.v7()
    local stored, store_err = env.set(RUNTIME_EPOCH_ENV, epoch)
    if store_err or stored == false then
        error("failed to establish Dataflow runtime epoch: " .. tostring(store_err))
    end
    process.send("dataflow.overseer", "dataflow.activation.changed", {
        source = "runtime_boot",
        runtime_epoch = epoch,
    })
    return {
        status = "success",
        message = "Dataflow runtime epoch established",
    }
end

M.run = run
return M
