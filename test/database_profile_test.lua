local sql = require("sql")

local M = {}

function M.run()
    local db, db_err = sql.get("app:db")
    if db_err then error("Failed to acquire test database: " .. tostring(db_err)) end

    local db_type, type_err = db:type()
    if type_err then
        db:release()
        error("Failed to identify test database: " .. tostring(type_err))
    end

    local probe
    if db_type == "postgres" then
        probe = "SELECT current_database() AS database_name"
    elseif db_type == "sqlite" then
        probe = "SELECT sqlite_version() AS database_version"
    else
        db:release()
        error("Unsupported dataflow test database: " .. tostring(db_type))
    end

    local rows, probe_err = db:query(probe)
    db:release()
    if probe_err then error("Failed " .. db_type .. " database probe: " .. tostring(probe_err)) end
    if type(rows) ~= "table" or #rows ~= 1 then
        error("Unexpected " .. db_type .. " database probe result")
    end

    return { database_type = db_type }
end

return M
