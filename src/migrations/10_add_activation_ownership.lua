local function execute_or_error(db, query)
    local success, err = db:execute(query)
    if err then error(err) end
    return success
end

-- The orchestrator-owned ownership record. owner_token identifies one
-- orchestrator incarnation, owner_pid its process, owner_epoch (added by
-- migration 08) the runtime it runs in, and owner_phase is running or
-- released; a row without a phase has never been owned.
local SQLITE_COLUMNS = {
    { name = "owner_token", type = "TEXT" },
    { name = "owner_pid", type = "TEXT" },
    { name = "owner_phase", type = "TEXT" },
}

local POSTGRES_COLUMNS = {
    { name = "owner_token", type = "TEXT" },
    { name = "owner_pid", type = "TEXT" },
    { name = "owner_phase", type = "TEXT" },
}

local function sqlite_columns(db)
    local columns, columns_err = db:query("PRAGMA table_info(dataflow_activations)")
    if columns_err then error(columns_err) end
    local present = {}
    for _, column in ipairs(columns or {}) do present[column.name] = true end
    return present
end

return require("migration").define(function()
    migration("Record the orchestrator that owns an activation", function()
        database("postgres", function()
            up(function(db)
                for _, column in ipairs(POSTGRES_COLUMNS) do
                    execute_or_error(db, "ALTER TABLE dataflow_activations ADD COLUMN IF NOT EXISTS " ..
                        column.name .. " " .. column.type)
                end
            end)
            down(function(db)
                for _, column in ipairs(POSTGRES_COLUMNS) do
                    execute_or_error(db, "ALTER TABLE dataflow_activations DROP COLUMN IF EXISTS " ..
                        column.name)
                end
            end)
        end)

        database("sqlite", function()
            up(function(db)
                local present = sqlite_columns(db)
                for _, column in ipairs(SQLITE_COLUMNS) do
                    if not present[column.name] then
                        execute_or_error(db, "ALTER TABLE dataflow_activations ADD COLUMN " ..
                            column.name .. " " .. column.type)
                    end
                end
            end)
            down(function(db)
                local present = sqlite_columns(db)
                for _, column in ipairs(SQLITE_COLUMNS) do
                    if present[column.name] then
                        execute_or_error(db, "ALTER TABLE dataflow_activations DROP COLUMN " .. column.name)
                    end
                end
            end)
        end)
    end)
end)
