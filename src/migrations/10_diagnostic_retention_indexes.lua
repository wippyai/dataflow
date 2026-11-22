-- Keep this allowlist aligned with the diagnostic sweeper.
local DIAGNOSTICS = "('cycle.state','cycle.function_result','node.input','node.yield','node.yield.result','parallel.progress')"

local function install(db, age)
    local statements = {
        "CREATE INDEX IF NOT EXISTS idx_diagnostic_data_age ON dataflow_data(" .. age .. ",data_id) WHERE type IN " .. DIAGNOSTICS,
        "CREATE INDEX IF NOT EXISTS idx_diagnostic_commit_age ON dataflow_commits(" .. age .. ",commit_id) WHERE op_id IS NOT NULL",
    }
    for _, statement in ipairs(statements) do
        local _, err = db:execute(statement)
        if err then error(tostring(err)) end
    end
end

return require("migration").define(function()
    migration("Add diagnostic retention age indexes", function()
        database("postgres", function() up(function(db) install(db, "created_at") end) end)
        database("sqlite", function() up(function(db) install(db, "julianday(created_at)") end) end)
    end)
end)
