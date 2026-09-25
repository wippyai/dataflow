local function run(db, statements)
    for _, statement in ipairs(statements) do
        local _, err = db:execute(statement)
        if err then error(err) end
    end
end

return require("migration").define(function()
    migration("Retain dataflow admission and terminal evidence", function()
        database("postgres", function()
            up(function(db)
                run(db, {
                    "ALTER TABLE dataflow_activations ADD COLUMN admission_key TEXT",
                    "ALTER TABLE dataflow_activations ADD COLUMN ever_activated BOOLEAN NOT NULL DEFAULT FALSE",
                    "ALTER TABLE dataflow_activations ADD COLUMN terminal_status TEXT",
                    "ALTER TABLE dataflow_activations ADD COLUMN terminal_outcome_json TEXT",
                    "ALTER TABLE dataflow_activations ADD COLUMN terminal_generation BIGINT",
                    "ALTER TABLE dataflow_activations ADD COLUMN terminal_ack_at TIMESTAMPTZ",
                    "CREATE UNIQUE INDEX uq_dataflow_activation_admission ON dataflow_activations(dataflow_id,admission_key) WHERE admission_key IS NOT NULL",
                    "CREATE INDEX idx_dataflow_terminal_ack ON dataflow_activations(terminal_ack_at,dataflow_id) WHERE terminal_status IS NOT NULL",
                    "UPDATE dataflow_activations SET ever_activated = TRUE WHERE generation > 0",
                })
            end)
            down(function(db)
                run(db, {
                    "DROP INDEX idx_dataflow_terminal_ack",
                    "DROP INDEX uq_dataflow_activation_admission",
                    "ALTER TABLE dataflow_activations DROP COLUMN terminal_ack_at",
                    "ALTER TABLE dataflow_activations DROP COLUMN terminal_generation",
                    "ALTER TABLE dataflow_activations DROP COLUMN terminal_outcome_json",
                    "ALTER TABLE dataflow_activations DROP COLUMN terminal_status",
                    "ALTER TABLE dataflow_activations DROP COLUMN ever_activated",
                    "ALTER TABLE dataflow_activations DROP COLUMN admission_key",
                })
            end)
        end)
        database("sqlite", function()
            up(function(db)
                run(db, {
                    "ALTER TABLE dataflow_activations ADD COLUMN admission_key TEXT",
                    "ALTER TABLE dataflow_activations ADD COLUMN ever_activated INTEGER NOT NULL DEFAULT 0 CHECK(ever_activated IN (0,1))",
                    "ALTER TABLE dataflow_activations ADD COLUMN terminal_status TEXT",
                    "ALTER TABLE dataflow_activations ADD COLUMN terminal_outcome_json TEXT",
                    "ALTER TABLE dataflow_activations ADD COLUMN terminal_generation INTEGER",
                    "ALTER TABLE dataflow_activations ADD COLUMN terminal_ack_at TEXT",
                    "CREATE UNIQUE INDEX uq_dataflow_activation_admission ON dataflow_activations(dataflow_id,admission_key) WHERE admission_key IS NOT NULL",
                    "CREATE INDEX idx_dataflow_terminal_ack ON dataflow_activations(terminal_ack_at,dataflow_id) WHERE terminal_status IS NOT NULL",
                    "UPDATE dataflow_activations SET ever_activated = 1 WHERE generation > 0",
                })
            end)
            down(function(db)
                run(db, {
                    "DROP INDEX idx_dataflow_terminal_ack",
                    "DROP INDEX uq_dataflow_activation_admission",
                    "ALTER TABLE dataflow_activations DROP COLUMN terminal_ack_at",
                    "ALTER TABLE dataflow_activations DROP COLUMN terminal_generation",
                    "ALTER TABLE dataflow_activations DROP COLUMN terminal_outcome_json",
                    "ALTER TABLE dataflow_activations DROP COLUMN terminal_status",
                    "ALTER TABLE dataflow_activations DROP COLUMN ever_activated",
                    "ALTER TABLE dataflow_activations DROP COLUMN admission_key",
                })
            end)
        end)
    end)
end)
