local function execute_or_error(db, query)
    local success, err = db:execute(query)
    if err then error(err) end
    return success
end

-- The ownership record of an activation. owner_token identifies one
-- orchestrator incarnation, owner_pid its process, owner_epoch (added by
-- migration 08) the runtime it runs in, and owner_phase is running or
-- released; a row without a phase has never been owned. An orchestrator
-- admits itself as the running owner; a completion releases it.
local COLUMNS = { "owner_token", "owner_pid", "owner_phase" }

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
                for _, column in ipairs(COLUMNS) do
                    execute_or_error(db, "ALTER TABLE dataflow_activations ADD COLUMN IF NOT EXISTS " ..
                        column .. " TEXT")
                end
            end)
            down(function(db)
                for _, column in ipairs(COLUMNS) do
                    execute_or_error(db, "ALTER TABLE dataflow_activations DROP COLUMN IF EXISTS " .. column)
                end
            end)
        end)

        database("sqlite", function()
            up(function(db)
                local present = sqlite_columns(db)
                for _, column in ipairs(COLUMNS) do
                    if not present[column] then
                        execute_or_error(db, "ALTER TABLE dataflow_activations ADD COLUMN " .. column .. " TEXT")
                    end
                end
            end)
            down(function(db)
                local present = sqlite_columns(db)
                for _, column in ipairs(COLUMNS) do
                    if present[column] then
                        execute_or_error(db, "ALTER TABLE dataflow_activations DROP COLUMN " .. column)
                    end
                end
            end)
        end)
    end)
end)
