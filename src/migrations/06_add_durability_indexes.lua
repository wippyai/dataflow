return require("migration").define(function()
    migration("Add durability indexes for actor listing and pending-commit scans", function()
        database("postgres", function()
            up(function(db)
                local success, err = db:execute("ALTER TABLE dataflows ADD COLUMN IF NOT EXISTS actor_context JSONB")
                if err then
                    error(err)
                end

                success, err = db:execute(
                    "CREATE INDEX idx_dataflows_actor_created ON dataflows(actor_id, created_at DESC)"
                )
                if err then
                    error(err)
                end

                success, err = db:execute(
                    "CREATE INDEX idx_dataflows_actor_status_created ON dataflows(actor_id, status, created_at DESC)"
                )
                if err then
                    error(err)
                end

                success, err = db:execute(
                    "CREATE INDEX idx_dataflow_commits_pending ON dataflow_commits(dataflow_id) WHERE op_id IS NULL"
                )
                if err then
                    error(err)
                end
            end)

            down(function(db)
                local success, err = db:execute("DROP INDEX IF EXISTS idx_dataflow_commits_pending")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_dataflows_actor_status_created")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_dataflows_actor_created")
                if err then
                    error(err)
                end
            end)
        end)

        database("sqlite", function()
            up(function(db)
                local columns, columns_err = db:query("PRAGMA table_info(dataflows)")
                if columns_err then
                    error(columns_err)
                end
                local has_actor_context = false
                for _, column in ipairs(columns or {}) do
                    if column.name == "actor_context" then
                        has_actor_context = true
                        break
                    end
                end
                if not has_actor_context then
                    local success, err = db:execute("ALTER TABLE dataflows ADD COLUMN actor_context TEXT")
                    if err then
                        error(err)
                    end
                end

                local success, err = db:execute(
                    "CREATE INDEX idx_dataflows_actor_created ON dataflows(actor_id, created_at DESC)"
                )
                if err then
                    error(err)
                end

                success, err = db:execute(
                    "CREATE INDEX idx_dataflows_actor_status_created ON dataflows(actor_id, status, created_at DESC)"
                )
                if err then
                    error(err)
                end

                success, err = db:execute(
                    "CREATE INDEX idx_dataflow_commits_pending ON dataflow_commits(dataflow_id) WHERE op_id IS NULL"
                )
                if err then
                    error(err)
                end
            end)

            down(function(db)
                local success, err = db:execute("DROP INDEX IF EXISTS idx_dataflow_commits_pending")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_dataflows_actor_status_created")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_dataflows_actor_created")
                if err then
                    error(err)
                end
            end)
        end)
    end)
end)
