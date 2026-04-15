local function execute_or_error(db, query)
    local success, err = db:execute(query)
    if err then
        error(err)
    end
    return success
end

return require("migration").define(function()
    migration("Add hard uniqueness guards for durable outputs", function()
        database("postgres", function()
            up(function(db)
                execute_or_error(db, [[
                    DELETE FROM dataflow_data
                    WHERE type = 'dataflow.output'
                      AND ctid NOT IN (
                          SELECT ctid
                          FROM dataflow_data
                          WHERE type = 'dataflow.output'
                            AND data_id IN (
                                SELECT MAX(data_id::text)::uuid
                                FROM dataflow_data
                                WHERE type = 'dataflow.output'
                                GROUP BY dataflow_id, COALESCE(key, ''), COALESCE(discriminator, '')
                            )
                      )
                ]])

                execute_or_error(db, [[
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_dataflow_output_unique_slot
                    ON dataflow_data (dataflow_id, COALESCE(key, ''), COALESCE(discriminator, ''))
                    WHERE type = 'dataflow.output'
                ]])

                execute_or_error(db, [[
                    DELETE FROM dataflow_data
                    WHERE type = 'node.result'
                      AND discriminator = 'result.success'
                      AND node_id IS NOT NULL
                      AND ctid NOT IN (
                          SELECT ctid
                          FROM dataflow_data
                          WHERE type = 'node.result'
                            AND discriminator = 'result.success'
                            AND node_id IS NOT NULL
                            AND data_id IN (
                                SELECT MAX(data_id::text)::uuid
                                FROM dataflow_data
                                WHERE type = 'node.result'
                                  AND discriminator = 'result.success'
                                  AND node_id IS NOT NULL
                                GROUP BY node_id
                            )
                      )
                ]])

                execute_or_error(db, [[
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_node_result_success_unique_node
                    ON dataflow_data (node_id)
                    WHERE type = 'node.result'
                      AND discriminator = 'result.success'
                      AND node_id IS NOT NULL
                ]])
            end)

            down(function(db)
                execute_or_error(db, "DROP INDEX IF EXISTS idx_node_result_success_unique_node")
                execute_or_error(db, "DROP INDEX IF EXISTS idx_dataflow_output_unique_slot")
            end)
        end)

        database("sqlite", function()
            up(function(db)
                execute_or_error(db, [[
                    DELETE FROM dataflow_data
                    WHERE type = 'dataflow.output'
                      AND rowid NOT IN (
                          SELECT rowid
                          FROM dataflow_data
                          WHERE type = 'dataflow.output'
                            AND data_id IN (
                                SELECT MAX(data_id)
                                FROM dataflow_data
                                WHERE type = 'dataflow.output'
                                GROUP BY dataflow_id, COALESCE(key, ''), COALESCE(discriminator, '')
                            )
                      )
                ]])

                execute_or_error(db, [[
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_dataflow_output_unique_slot
                    ON dataflow_data (dataflow_id, COALESCE(key, ''), COALESCE(discriminator, ''))
                    WHERE type = 'dataflow.output'
                ]])

                execute_or_error(db, [[
                    DELETE FROM dataflow_data
                    WHERE type = 'node.result'
                      AND discriminator = 'result.success'
                      AND node_id IS NOT NULL
                      AND rowid NOT IN (
                          SELECT rowid
                          FROM dataflow_data
                          WHERE type = 'node.result'
                            AND discriminator = 'result.success'
                            AND node_id IS NOT NULL
                            AND data_id IN (
                                SELECT MAX(data_id)
                                FROM dataflow_data
                                WHERE type = 'node.result'
                                  AND discriminator = 'result.success'
                                  AND node_id IS NOT NULL
                                GROUP BY node_id
                            )
                      )
                ]])

                execute_or_error(db, [[
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_node_result_success_unique_node
                    ON dataflow_data (node_id)
                    WHERE type = 'node.result'
                      AND discriminator = 'result.success'
                      AND node_id IS NOT NULL
                ]])
            end)

            down(function(db)
                execute_or_error(db, "DROP INDEX IF EXISTS idx_node_result_success_unique_node")
                execute_or_error(db, "DROP INDEX IF EXISTS idx_dataflow_output_unique_slot")
            end)
        end)
    end)
end)
