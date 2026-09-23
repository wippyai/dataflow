local function execute_or_error(db, query)
    local success, err = db:execute(query)
    if err then error(err) end
    return success
end

-- SQLite does not enforce the cascading foreign keys, and releases before the
-- activation ownership record deleted workflows without their activation row.
-- Such rows, and wakes left the same way, belong to no workflow.
local ORPHANS = {
    [[DELETE FROM dataflow_wakes WHERE NOT EXISTS (
        SELECT 1 FROM dataflows WHERE dataflows.dataflow_id = dataflow_wakes.dataflow_id)]],
    [[DELETE FROM dataflow_activations WHERE NOT EXISTS (
        SELECT 1 FROM dataflows WHERE dataflows.dataflow_id = dataflow_activations.dataflow_id)]],
}

local function remove_orphans(db)
    for _, statement in ipairs(ORPHANS) do execute_or_error(db, statement) end
end

return require("migration").define(function()
    migration("Remove activation and wake rows of deleted workflows", function()
        database("postgres", function()
            up(remove_orphans)
            down(function() end)
        end)
        database("sqlite", function()
            up(remove_orphans)
            down(function() end)
        end)
    end)
end)
