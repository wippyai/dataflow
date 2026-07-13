local sql = require("sql")
local consts = require("dataflow_consts")

local wake_repo = {}

local function get_db()
    local db, err = sql.get(consts.APP_DB)
    if err then return nil, err end
    return db, nil
end

function wake_repo.clear(dataflow_id)
    if type(dataflow_id) ~= "string" or dataflow_id == "" then return nil, "dataflow_id is required" end
    local db, db_err = get_db()
    if db_err then return nil, db_err end
    local _, write_err = db:execute("DELETE FROM dataflow_wakes WHERE dataflow_id = ?", { dataflow_id })
    if write_err then db:release(); return nil, write_err end
    db:release()
    return true, nil
end

function wake_repo.next()
    local db, db_err = get_db()
    if db_err then return nil, db_err end
    local rows, query_err = db:query(
        "SELECT dataflow_id, wake_key, wake_at FROM dataflow_wakes ORDER BY wake_at ASC LIMIT 1"
    )
    db:release()
    if query_err then return nil, query_err end
    return rows and rows[1] or nil, nil
end

function wake_repo.due(now_value, limit)
    local db, db_err = get_db()
    if db_err then return nil, db_err end
    local rows, query_err = db:query(
        "SELECT dataflow_id, wake_key, wake_at FROM dataflow_wakes WHERE wake_at <= ? ORDER BY wake_at ASC LIMIT ?",
        { now_value, tonumber(limit) or 100 }
    )
    db:release()
    if query_err then return nil, query_err end
    return rows or {}, nil
end

function wake_repo.remove(dataflow_id, wake_key)
    if type(dataflow_id) ~= "string" or dataflow_id == "" then return nil, "dataflow_id is required" end
    if type(wake_key) ~= "string" or wake_key == "" then return nil, "wake_key is required" end
    local db, db_err = get_db()
    if db_err then return nil, db_err end
    local _, write_err = db:execute(
        "DELETE FROM dataflow_wakes WHERE dataflow_id = ? AND wake_key = ?",
        { dataflow_id, wake_key })
    db:release()
    if write_err then return nil, write_err end
    return true, nil
end

return wake_repo
