local function execute_or_error(db, query)
    local success, err = db:execute(query)
    if err then error(err) end
    return success
end

return require("migration").define(function()
    migration("Add versioned uniqueness guard for iteration terminal emissions", function()
        database("postgres", function()
            up(function(db)
                execute_or_error(db, [[
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_iteration_terminal_unique_slot
                    ON dataflow_data (node_id, discriminator, COALESCE(key, ''))
                    WHERE type IN ('iteration.result', 'iteration.error')
                      AND node_id IS NOT NULL
                      AND discriminator IS NOT NULL
                      AND metadata->>'terminal_emission_key_version' = '1'
                ]])
            end)
            down(function(db)
                execute_or_error(db, "DROP INDEX IF EXISTS idx_iteration_terminal_unique_slot")
            end)
        end)

        database("sqlite", function()
            up(function(db)
                execute_or_error(db, [[
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_iteration_terminal_unique_slot
                    ON dataflow_data (node_id, discriminator, COALESCE(key, ''))
                    WHERE type IN ('iteration.result', 'iteration.error')
                      AND node_id IS NOT NULL
                      AND discriminator IS NOT NULL
                      AND json_valid(metadata)
                      AND json_extract(metadata, '$.terminal_emission_key_version') = 1
                ]])
            end)
            down(function(db)
                execute_or_error(db, "DROP INDEX IF EXISTS idx_iteration_terminal_unique_slot")
            end)
        end)
    end)
end)
