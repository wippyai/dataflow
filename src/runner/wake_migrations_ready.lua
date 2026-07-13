local M = {}

local function run()
    -- The service can start before or after migrations. If it is already
    -- waiting this recalculates immediately; if it is not registered yet, its
    -- first query sees the migrated table. Either startup order is valid.
    process.send("dataflow.wakes", "dataflow.wake.changed", {
        source = "migrations",
    })
    return {
        status = "success",
        message = "Dataflow wake service migration boundary reached",
    }
end

M.run = run
return M
