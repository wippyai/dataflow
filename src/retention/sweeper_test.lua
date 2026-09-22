local test = require("test")
local sql = require("sql")
local uuid = require("uuid")
local time = require("time")
local sweeper = require("sweeper")

local function define_tests()
    describe("Native diagnostic retention", function()
        local db, ids
        local old = "2000-01-01T00:00:00Z"
        local function exec(q, params)
            if tostring(db:type()) == "postgres" then
                local n = 0
                q = q:gsub("%?", function() n = n + 1; return "$" .. n end)
            end
            local result, err = db:execute(q, params or {})
            if err then error(tostring(err)) end
            return result
        end
        local function insert(table_name, values)
            local _, err = sql.builder.insert(table_name):set_map(values):run_with(db):exec()
            if err then error(tostring(err)) end
        end
        local function count(table_name, flow)
            local rows, err = sql.builder.select("COUNT(*) AS n"):from(table_name)
                :where("dataflow_id = ?", flow):run_with(db):query()
            if err then error(tostring(err)) end
            return tonumber(rows[1].n)
        end
        local function flow(status, parent, recent)
            local id = uuid.v7()
            insert("dataflows", { dataflow_id = id, parent_dataflow_id = parent,
                actor_id = "retention-test", type = "retention-test", status = status,
                created_at = old, updated_at = recent and time.now():format(time.RFC3339) or old })
            ids[#ids + 1] = id
            for _, kind in ipairs({ "cycle.state", "node.input", "dataflow.input",
                "dataflow.output", "node.result", "agent.observation" }) do
                insert("dataflow_data", { data_id = uuid.v7(), dataflow_id = id,
                    type = kind, content = "evidence", created_at = old })
            end
            local last = uuid.v7()
            insert("dataflow_commits", { commit_id = uuid.v7(), dataflow_id = id,
                op_id = uuid.v7(), payload = "{}", created_at = old })
            insert("dataflow_commits", { commit_id = last, dataflow_id = id,
                op_id = uuid.v7(), payload = "{}", created_at = old })
            insert("dataflow_commits", { commit_id = uuid.v7(), dataflow_id = id,
                payload = "{}", created_at = old })
            exec("UPDATE dataflows SET last_commit_id = ? WHERE dataflow_id = ?", { last, id })
            return id
        end
        before_each(function()
            local err
            db, err = sql.get("app:db")
            if err then error(tostring(err)) end
            ids = {}
        end)
        after_each(function()
            for i = #ids, 1, -1 do
                exec("DELETE FROM dataflow_data WHERE dataflow_id = ?", { ids[i] })
                exec("DELETE FROM dataflow_commits WHERE dataflow_id = ?", { ids[i] })
                exec("DELETE FROM dataflows WHERE dataflow_id = ?", { ids[i] })
            end
            db:release()
        end)
        it("defaults to disabled and rejects malformed or unbounded configuration", function()
            local stats, err = sweeper.run(nil, {})
            test.is_nil(err)
            test.eq(stats.enabled, false)
            for _, options in ipairs({ { days = -1 }, { days = 1.5 }, { days = "bad" },
                { days = 30, batch_size = 0 }, { days = 30, batch_size = 1001 } }) do
                local valid, validation_err = sweeper.validate(options)
                test.is_nil(valid)
                test.not_nil(validation_err)
            end
        end)
        it("dry runs count candidates without deleting them", function()
            local id = flow("completed")
            local stats, err = sweeper.run(db, { days = 30, dry_run = true })
            test.is_nil(err)
            test.eq(stats.data_candidates, 2)
            test.eq(stats.commit_candidates, 1)
            test.eq(stats.data, 0)
            test.eq(count("dataflow_data", id), 6)
        end)
        it("prunes only old diagnostics and preserves results evidence and last/pending commits", function()
            local id = flow("completed")
            local stats, err = sweeper.run(db, { days = 30 })
            test.is_nil(err)
            test.eq(stats.data, 2)
            test.eq(stats.commits, 1)
            test.eq(count("dataflow_data", id), 4)
            test.eq(count("dataflow_commits", id), 2)
            test.eq(count("dataflows", id), 1)
            local again, again_err = sweeper.run(db, { days = 30 })
            test.is_nil(again_err)
            test.eq(again.data + again.commits, 0)
        end)
        it("honors the batch limit and continues remaining work on the next sweep", function()
            local id = flow("failed")
            local stats, err = sweeper.run(db, { days = 30, batch_size = 1 })
            test.is_nil(err)
            test.eq(stats.data, 1)
            test.eq(stats.commits, 1)
            test.eq(count("dataflow_data", id), 5)
            local next_stats, next_err = sweeper.run(db, { days = 30, batch_size = 1 })
            test.is_nil(next_err)
            test.eq(next_stats.data, 1)
        end)
        it("protects a whole family with an active ancestor or descendant", function()
            local active = flow("running")
            local child = flow("completed", active)
            local root = flow("completed")
            local running_child = flow("waiting", root)
            local stats, err = sweeper.run(db, { days = 30 })
            test.is_nil(err)
            test.eq(stats.data + stats.commits, 0)
            for _, id in ipairs({ active, child, root, running_child }) do
                test.eq(count("dataflow_data", id), 6)
            end
        end)
        it("protects recent descendants and paused or unknown states", function()
            local root = flow("completed")
            flow("completed", root, true)
            flow("paused")
            flow("future-status")
            local stats, err = sweeper.run(db, { days = 30 })
            test.is_nil(err)
            test.eq(stats.data + stats.commits, 0)
        end)
        it("prunes old terminal parent-child families including cancelled and terminated", function()
            local root = flow("cancelled")
            flow("terminated", root)
            local stats, err = sweeper.run(db, { days = 30 })
            test.is_nil(err)
            test.eq(stats.data, 4)
            test.eq(stats.commits, 2)
        end)
        it("preserves newly written diagnostics even on an old terminal flow", function()
            local id = flow("failed")
            exec("UPDATE dataflow_data SET created_at = ? WHERE dataflow_id = ?",
                { time.now():format(time.RFC3339), id })
            local stats, err = sweeper.run(db, { days = 30 })
            test.is_nil(err)
            test.eq(stats.data, 0)
            test.eq(stats.commits, 1)
        end)
    end)
end

return test.run_cases(define_tests)
