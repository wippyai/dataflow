return require("migration").define(function()
    migration("Create durable dataflow activation state", function()
        database("postgres", function()
            up(function(db)
                local _, err = db:execute([[
                    CREATE TABLE IF NOT EXISTS dataflow_activations (
                        dataflow_id UUID PRIMARY KEY REFERENCES dataflows(dataflow_id) ON DELETE CASCADE,
                        generation BIGINT NOT NULL CHECK (generation > 0),
                        desired_active BOOLEAN NOT NULL,
                        launch_args JSONB,
                        requested_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL
                    )
                ]])
                if err then error(err) end

                _, err = db:execute([[
                    ALTER TABLE dataflow_wakes
                    ADD COLUMN IF NOT EXISTS activation_generation BIGINT
                ]])
                if err then error(err) end

                _, err = db:execute("DROP INDEX IF EXISTS idx_dataflow_wakes_due")
                if err then error(err) end
                _, err = db:execute([[
                    CREATE INDEX idx_dataflow_wakes_due
                    ON dataflow_wakes(wake_at, dataflow_id, wake_key)
                ]])
                if err then error(err) end

                _, err = db:execute([[
                    CREATE INDEX IF NOT EXISTS idx_dataflow_activations_active
                    ON dataflow_activations(desired_active, updated_at)
                ]])
                if err then error(err) end
            end)

            down(function(db)
                local _, err = db:execute("DROP INDEX IF EXISTS idx_dataflow_activations_active")
                if err then error(err) end
                _, err = db:execute("DROP TABLE IF EXISTS dataflow_activations")
                if err then error(err) end
                _, err = db:execute("DROP INDEX IF EXISTS idx_dataflow_wakes_due")
                if err then error(err) end
                _, err = db:execute("CREATE INDEX idx_dataflow_wakes_due ON dataflow_wakes(wake_at)")
                if err then error(err) end
                _, err = db:execute("ALTER TABLE dataflow_wakes DROP COLUMN IF EXISTS activation_generation")
                if err then error(err) end
            end)
        end)

        database("sqlite", function()
            up(function(db)
                local _, err = db:execute([[
                    CREATE TABLE IF NOT EXISTS dataflow_activations (
                        dataflow_id TEXT PRIMARY KEY REFERENCES dataflows(dataflow_id) ON DELETE CASCADE,
                        generation INTEGER NOT NULL CHECK (generation > 0),
                        desired_active INTEGER NOT NULL CHECK (desired_active IN (0, 1)),
                        launch_args TEXT,
                        requested_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                ]])
                if err then error(err) end

                local columns, columns_err = db:query("PRAGMA table_info(dataflow_wakes)")
                if columns_err then error(columns_err) end
                local has_generation = false
                for _, column in ipairs(columns or {}) do
                    if column.name == "activation_generation" then
                        has_generation = true
                        break
                    end
                end
                if not has_generation then
                    _, err = db:execute("ALTER TABLE dataflow_wakes ADD COLUMN activation_generation INTEGER")
                    if err then error(err) end
                end

                _, err = db:execute("DROP INDEX IF EXISTS idx_dataflow_wakes_due")
                if err then error(err) end
                _, err = db:execute([[
                    CREATE INDEX idx_dataflow_wakes_due
                    ON dataflow_wakes(wake_at, dataflow_id, wake_key)
                ]])
                if err then error(err) end

                _, err = db:execute([[
                    CREATE INDEX IF NOT EXISTS idx_dataflow_activations_active
                    ON dataflow_activations(desired_active, updated_at)
                ]])
                if err then error(err) end
            end)

            down(function(db)
                local _, err = db:execute("DROP INDEX IF EXISTS idx_dataflow_activations_active")
                if err then error(err) end
                _, err = db:execute("DROP TABLE IF EXISTS dataflow_activations")
                if err then error(err) end
                _, err = db:execute("DROP INDEX IF EXISTS idx_dataflow_wakes_due")
                if err then error(err) end
                _, err = db:execute("CREATE INDEX idx_dataflow_wakes_due ON dataflow_wakes(wake_at)")
                if err then error(err) end
                -- SQLite deployments may predate DROP COLUMN support. The
                -- nullable fence is harmless when this migration is rolled
                -- back, and migration 07 drops the table during full removal.
            end)
        end)
    end)
end)
