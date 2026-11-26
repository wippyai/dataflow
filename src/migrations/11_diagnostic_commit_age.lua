-- Keep the two index builds in separate migrations. A released writer may
-- hold a commit-table write lock while it publishes dataflow_data outputs.
local function install(db, age)
    local _, err = db:execute("CREATE INDEX IF NOT EXISTS idx_diagnostic_commit_age ON dataflow_commits("
        .. age .. ",commit_id) WHERE op_id IS NOT NULL")
    if err then error(tostring(err)) end
end

return require("migration").define(function()
    migration("Add applied commit age index", function()
        database("postgres", function() up(function(db) install(db, "created_at") end) end)
        database("sqlite", function() up(function(db) install(db, "julianday(created_at)") end) end)
    end)
end)
