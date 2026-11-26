local sql = require("sql")

local M = {}

-- Family identity is immutable on engine paths. Reserve SQLite's writer before
-- reading; PostgreSQL serializes all family writers and retirement on the root.
-- Missing parents are rejected before INSERT, including invisible PG parents.
function M.lock(tx, dataflow_id)
    local dialect, type_err = tx:db_type()
    if type_err then return nil, type_err end
    local postgres = tostring(dialect) == "postgres"
    if not postgres then
        local _, err = tx:execute("UPDATE dataflows SET status=status WHERE 1=0")
        if err then return nil, err end
    end
    local rows, err = sql.builder.select("dataflow_id", "retention_root_id", "actor_id", "parent_dataflow_id")
        :from("dataflows"):where("dataflow_id = ?", dataflow_id):run_with(tx):query()
    if err then return nil, err end
    if not rows[1] then return nil, nil end
    local flow = rows[1]
    -- Legacy unrooted rows remain writable but cannot qualify for retirement.
    local root = flow.retention_root_id or flow.dataflow_id
    local query = sql.builder.select("retention_started", "retention_held")
        :from("dataflows"):where("dataflow_id = ?", root)
    if postgres then query = query:suffix("FOR UPDATE") end
    local roots, root_err = query:run_with(tx):query()
    if root_err then return nil, root_err end
    if not roots[1] then return nil, "dataflow not found" end
    if roots[1].retention_started then return nil, "dataflow family is retiring" end
    return flow, nil
end

return M
