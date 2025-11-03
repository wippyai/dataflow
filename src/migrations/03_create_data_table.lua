return require("migration").define(function()
    migration("Create data table with discriminator", function()
        database("postgres", function()
            up(function(db)
                local success, err = db:execute([[
                    CREATE TABLE dataflow_data (
                        data_id UUID PRIMARY KEY,
                        dataflow_id UUID NOT NULL,
                        node_id UUID,
                        type TEXT NOT NULL,
                        discriminator TEXT,
                        key TEXT,
                        content BYTEA NOT NULL,
                        content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                        metadata JSONB DEFAULT '{}',
                        created_at TIMESTAMP NOT NULL DEFAULT now(),
                        FOREIGN KEY (dataflow_id) REFERENCES dataflows(dataflow_id) ON DELETE CASCADE,
                        FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
                    )
                ]])

                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_dataflow ON dataflow_data(dataflow_id)")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_node ON dataflow_data(node_id) WHERE node_id IS NOT NULL")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_type ON dataflow_data(type)")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_discriminator ON dataflow_data(discriminator)")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_key ON dataflow_data(key)")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_dataflow_type_discriminator ON dataflow_data(dataflow_id, type, discriminator)")
                if err then
                    error(err)
                end

                success, err = db:execute([[
                    CREATE INDEX idx_data_unique_node_specific
                    ON dataflow_data (dataflow_id, node_id, type, discriminator, key)
                    WHERE node_id IS NOT NULL
                ]])
                if err then
                    error(err)
                end

                success, err = db:execute([[
                    CREATE INDEX idx_data_unique_dataflow_specific
                    ON dataflow_data (dataflow_id, type, discriminator, key)
                    WHERE node_id IS NULL
                ]])
                if err then
                    error(err)
                end
            end)

            down(function(db)
                local success, err = db:execute("DROP INDEX IF EXISTS idx_data_unique_dataflow_specific")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_unique_node_specific")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_dataflow_type_discriminator")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_key")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_discriminator")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_type")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_node")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_dataflow")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP TABLE IF EXISTS dataflow_data")
                if err then
                    error(err)
                end
            end)
        end)

        database("sqlite", function()
            up(function(db)
                local success, err = db:execute([[
                    CREATE TABLE dataflow_data (
                        data_id TEXT NOT NULL UNIQUE,
                        dataflow_id TEXT NOT NULL,
                        node_id TEXT,
                        type TEXT NOT NULL,
                        discriminator TEXT,
                        key TEXT,
                        content BLOB NOT NULL,
                        content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                        metadata TEXT DEFAULT '{}',
                        created_at INTEGER NOT NULL,
                        FOREIGN KEY (dataflow_id) REFERENCES dataflows(dataflow_id) ON DELETE CASCADE,
                        FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE
                    )
                ]])

                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_data_id ON dataflow_data(data_id)")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_dataflow ON dataflow_data(dataflow_id)")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_node ON dataflow_data(node_id) WHERE node_id IS NOT NULL")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_type ON dataflow_data(type)")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_discriminator ON dataflow_data(discriminator)")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_key ON dataflow_data(key)")
                if err then
                    error(err)
                end

                success, err = db:execute("CREATE INDEX idx_data_dataflow_type_discriminator ON dataflow_data(dataflow_id, type, discriminator)")
                if err then
                    error(err)
                end

                success, err = db:execute([[
                    CREATE INDEX idx_data_unique_node_specific
                    ON dataflow_data (dataflow_id, node_id, type, discriminator, key)
                    WHERE node_id IS NOT NULL
                ]])
                if err then
                    error(err)
                end

                success, err = db:execute([[
                    CREATE INDEX idx_data_unique_dataflow_specific
                    ON dataflow_data (dataflow_id, type, discriminator, key)
                    WHERE node_id IS NULL
                ]])
                if err then
                    error(err)
                end
            end)

            down(function(db)
                local success, err = db:execute("DROP INDEX IF EXISTS idx_data_unique_dataflow_specific")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_unique_node_specific")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_dataflow_type_discriminator")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_key")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_discriminator")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_type")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_node")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_dataflow")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP INDEX IF EXISTS idx_data_data_id")
                if err then
                    error(err)
                end

                success, err = db:execute("DROP TABLE IF EXISTS dataflow_data")
                if err then
                    error(err)
                end
            end)
        end)
    end)
end)