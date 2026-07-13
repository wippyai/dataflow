return require("migration").define(function()
    migration("Create targeted dataflow wake queue", function()
        database("postgres", function()
            up(function(db)
                local _, err = db:execute([[
                    CREATE TABLE IF NOT EXISTS dataflow_wakes (
                        dataflow_id UUID NOT NULL REFERENCES dataflows(dataflow_id) ON DELETE CASCADE,
                        wake_key TEXT NOT NULL,
                        wake_at TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (dataflow_id, wake_key)
                    )
                ]])
                if err then error(err) end
                _, err = db:execute("CREATE INDEX IF NOT EXISTS idx_dataflow_wakes_due ON dataflow_wakes(wake_at)")
                if err then error(err) end
            end)
            down(function(db)
                local _, err = db:execute("DROP TABLE IF EXISTS dataflow_wakes")
                if err then error(err) end
            end)
        end)

        database("sqlite", function()
            up(function(db)
                local _, err = db:execute([[
                    CREATE TABLE IF NOT EXISTS dataflow_wakes (
                        dataflow_id TEXT NOT NULL REFERENCES dataflows(dataflow_id) ON DELETE CASCADE,
                        wake_key TEXT NOT NULL,
                        wake_at TEXT NOT NULL,
                        PRIMARY KEY (dataflow_id, wake_key)
                    )
                ]])
                if err then error(err) end
                _, err = db:execute("CREATE INDEX IF NOT EXISTS idx_dataflow_wakes_due ON dataflow_wakes(wake_at)")
                if err then error(err) end
            end)
            down(function(db)
                local _, err = db:execute("DROP TABLE IF EXISTS dataflow_wakes")
                if err then error(err) end
            end)
        end)
    end)
end)
