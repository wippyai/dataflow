-- Module-owned reader of the full-runtime boot epoch. The entry runs with the
-- epoch reader group, so an orchestrator started synchronously under a caller
-- scope that cannot read module environment still obtains the epoch.
local M = {}

local RUNTIME_EPOCH_ENV = "userspace.dataflow.env:runtime_epoch"

function M.run(): (any, string?)
    local value, err = env.get(RUNTIME_EPOCH_ENV)
    if err then return nil, tostring(err) end
    if value == nil or tostring(value) == "" then return { epoch = nil }, nil end
    return { epoch = tostring(value) }, nil
end

return M
