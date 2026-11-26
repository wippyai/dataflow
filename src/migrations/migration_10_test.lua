local test = require("test")
local sql = require("sql")
local uuid = require("uuid")
local sweeper = require("sweeper")
local historical = { require("migration_01"), require("migration_02"), require("migration_03"), require("migration_05") }
local diagnostic_data = require("diagnostic_data")
local diagnostic_commits = require("diagnostic_commits")

local migrations = { require("migration_01"), require("migration_02"), require("migration_03"),
    require("migration_05"), require("migration_07"), require("migration_08") }
local retention = require("migration_12")

local function define_tests()
    describe("Populated diagnostic migration", function()
        it("adds and reruns age indexes on historical rows without changing flow storage", function()
            local db, err = sql.get("app:retention_upgrade_db")
            test.is_nil(err)
            local postgres = tostring(db:type()) == "postgres"
            local function run(statement)
                local result, read_err = db:query(statement)
                if read_err then error(tostring(read_err)) end
                return result
            end
            local function write(statement)
                local _, write_err = db:execute(statement)
                if write_err then error(tostring(write_err)) end
            end
            local function reset()
                for _, table_name in ipairs({ "dataflow_commits", "dataflow_data", "dataflow_nodes", "dataflows", "_migrations" }) do
                    write("DROP TABLE IF EXISTS " .. table_name .. (postgres and " CASCADE" or ""))
                end
            end
            local function migrate(fn)
                local result = fn({ db = db, direction = "up", id = "diagnostic_probe_" .. uuid.v7() })
                test.not_nil(result)
                test.neq(result.status, "error")
                test.eq(result.failed or 0, 0)
            end
            local ok, failure = pcall(function()
                reset()
                write("CREATE TABLE _migrations (id VARCHAR(512) PRIMARY KEY, applied_at BIGINT NOT NULL DEFAULT 0, description TEXT)")
                for _, step in ipairs(historical) do migrate(step) end
                local id = uuid.v7()
                local oldest = "'2000-01-01T00:00:00Z'"
                write("INSERT INTO dataflows(dataflow_id,actor_id,type,status,created_at,updated_at) VALUES ('" .. id
                    .. "','fixture','fixture','completed'," .. oldest .. "," .. oldest .. ")")
                for _, kind in ipairs({ "node.input", "dataflow.output" }) do
                    write("INSERT INTO dataflow_data(data_id,dataflow_id,type,content,created_at) VALUES ('" .. uuid.v7()
                        .. "','" .. id .. "','" .. kind .. "','fixture'," .. oldest .. ")")
                end
                local index_source = postgres and "pg_indexes WHERE schemaname=current_schema() AND indexname" or "sqlite_master WHERE type='index' AND name"
                migrate(diagnostic_data)
                test.eq(tonumber(run("SELECT COUNT(*) AS n FROM " .. index_source .. " = 'idx_diagnostic_commit_age'")[1].n), 0)
                migrate(diagnostic_commits)
                migrate(diagnostic_data)
                migrate(diagnostic_commits)
                local indexes = run("SELECT COUNT(*) AS n FROM " .. index_source .. " IN ('idx_diagnostic_data_age','idx_diagnostic_commit_age')")
                test.eq(tonumber(indexes[1].n), 2)
                test.eq(tonumber(run("SELECT COUNT(*) AS n FROM dataflow_data")[1].n), 2)
                local column_count = postgres and "information_schema.columns WHERE table_schema=current_schema() AND table_name='dataflows'"
                    or "pragma_table_info('dataflows')"
                test.eq(tonumber(run("SELECT COUNT(*) AS n FROM " .. column_count)[1].n), 10)
                local result, sweep_err = sweeper.run(db, { days = 30, flow_days = 0 })
                test.is_nil(sweep_err)
                test.eq(result.data, 1)
                test.eq(run("SELECT type FROM dataflow_data")[1].type, "dataflow.output")
                test.eq(tonumber(run("SELECT COUNT(*) AS n FROM dataflows")[1].n), 1)
            end)
            reset()
            db:release()
            if not ok then error(tostring(failure)) end
        end)
        it("backfills existing families, preserves payload, installs seeks and safely reruns on both engines", function()
            local db, err = sql.get("app:retention_upgrade_db")
            test.is_nil(err)
            local postgres = tostring(db:type()) == "postgres"
            local function execute(q)
                local _, e = db:execute(q)
                if e then error(tostring(e)) end
            end
            local function query(q)
                local rows, e = db:query(q)
                if e then error(tostring(e)) end
                return rows
            end
            local function clear()
                for _, name in ipairs({ "dataflow_wakes", "dataflow_activations", "dataflow_commits",
                    "dataflow_data", "dataflow_nodes", "dataflows", "_migrations" }) do
                    execute("DROP TABLE IF EXISTS " .. name .. (postgres and " CASCADE" or ""))
                end
            end
            local function migrate(fn)
                local result = fn({ db = db, direction = "up", id = "retention_upgrade_" .. uuid.v7() })
                test.not_nil(result)
                test.neq(result.status, "error")
                test.eq(result.failed or 0, 0)
            end
            local ok, failure = pcall(function()
                clear()
                execute("CREATE TABLE _migrations (id VARCHAR(512) PRIMARY KEY, applied_at BIGINT NOT NULL DEFAULT 0, description TEXT)")
                for _, fn in ipairs(migrations) do migrate(fn) end
                local root, child, active, active_child = uuid.v7(), uuid.v7(), uuid.v7(), uuid.v7()
                local function flow(id, parent, status)
                    execute("INSERT INTO dataflows(dataflow_id,parent_dataflow_id,actor_id,type,status,created_at,updated_at) VALUES ('" ..
                        id .. "'," .. (parent and ("'" .. parent .. "'") or "NULL") ..
                        ",'upgrade','test','" .. status .. "','2000-01-01T00:00:00Z','2000-01-01T00:00:00Z')")
                    for _ = 1, 7 do
                        execute("INSERT INTO dataflow_data(data_id,dataflow_id,type,content,created_at) VALUES ('" ..
                            uuid.v7() .. "','" .. id .. "','node.input','payload','2000-01-01T00:00:00Z')")
                    end
                end
                flow(root, nil, "completed"); flow(child, root, "failed")
                flow(active, nil, "completed"); flow(active_child, active, "running")
                migrate(diagnostic_data)
                migrate(diagnostic_commits)
                migrate(retention)
                migrate(retention)
                local rows = query("SELECT retention_root_id, retention_held FROM dataflows WHERE dataflow_id='" .. child .. "'")
                test.eq(rows[1].retention_root_id, root)
                test.eq(tonumber(rows[1].retention_held), 1)
                test.eq(tonumber(query("SELECT COUNT(*) AS n FROM dataflow_data")[1].n), 28)
                local stats, sweep_err = sweeper.run(db, { days = 30, batch_size = 2 })
                test.is_nil(sweep_err)
                test.eq(stats.flows, 0)
                test.eq(tonumber(query("SELECT COUNT(*) AS n FROM dataflows")[1].n), 4)
                -- Explicit engine-side owner release is required for legacy roots.
                execute("UPDATE dataflows SET retention_held=0 WHERE dataflow_id='" .. root .. "'")
                migrate(retention)
                test.eq(tonumber(query("SELECT retention_held FROM dataflows WHERE dataflow_id='" .. root .. "'")[1].retention_held), 0)
                stats, sweep_err = sweeper.run(db, { days = 0, flow_days = 30, batch_size = 2 })
                test.is_nil(sweep_err)
                test.eq(stats.flows, 2)
                test.eq(tonumber(query("SELECT COUNT(*) AS n FROM dataflow_data")[1].n), 14)
                test.eq(tonumber(query("SELECT COUNT(*) AS n FROM dataflows")[1].n), 2)
            end)
            clear()
            db:release()
            if not ok then error(tostring(failure)) end
        end)
    end)
end

return test.run_cases(define_tests)
