-- Keep this allowlist aligned with the diagnostic sweeper.
local DIAGNOSTICS = "('cycle.state','cycle.function_result','node.input','node.yield','node.yield.result','parallel.progress')"

local function install(db, age)
    local _, err = db:execute("CREATE INDEX IF NOT EXISTS idx_diagnostic_data_age ON dataflow_data("
        .. age .. ",data_id) WHERE type IN " .. DIAGNOSTICS)
    if err then error(tostring(err)) end
end

return require("migration").define(function()
    migration("Add diagnostic data age index", function()
        database("postgres", function() up(function(db) install(db, "created_at") end) end)
        database("sqlite", function() up(function(db) install(db, "julianday(created_at)") end) end)
    end)
end)
