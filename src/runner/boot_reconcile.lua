local logger = require("logger"):named("dataflow.boot_reconcile")

local M = {
    dataflow_repo = require("dataflow_repo"),
    client = require("client"),
    process = process,
}

function M.reconcile_once(on_live)
    local rows, list_err = M.dataflow_repo.list_non_terminal()
    if list_err then return nil, list_err end
    local c, client_err = M.client.new()
    if client_err then return nil, client_err end
    local revived = 0
    for _, row in ipairs(rows or {}) do
        local id = row.dataflow_id
        local live_pid = type(id) == "string" and id ~= "" and
            M.process.registry.lookup("dataflow." .. id) or nil
        if live_pid then
            if type(on_live) == "function" then on_live(id, live_pid) end
        elseif type(id) == "string" and id ~= "" then
            local _, revive_err = c:revive(id)
            if revive_err then
                logger:warn("boot recovery could not revive dataflow", { dataflow_id = id, error = tostring(revive_err) })
            else
                revived = revived + 1
            end
        end
    end
    return revived, nil
end

function M.run(_args)
    local revived, err = M.reconcile_once()
    if err then error(err) end
    return { revived = revived or 0 }
end

return M
