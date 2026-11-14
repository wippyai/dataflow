local test = require("test")
local sql = require("sql")
local uuid = require("uuid")
local sweeper = require("sweeper")
local migrations = { require("migration_01"), require("migration_02"), require("migration_03"),
    require("migration_05"), require("migration_07"), require("migration_08") }
local retention = require("migration_10")

local function define_tests()
    describe("Populated retention upgrade", function()
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
