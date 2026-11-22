local test = require("test")
local sql = require("sql")
local uuid = require("uuid")
local time = require("time")
local sweeper = require("sweeper")

local function define_tests()
    describe("Native diagnostic retention", function()
        local db, ids
        local old = "2000-01-01T00:00:00Z"
        local function exec(q, params, executor)
            if tostring(db:type()) == "postgres" then
                local n = 0
                q = q:gsub("%?", function() n = n + 1; return "$" .. n end)
            end
            local result, err = (executor or db):execute(q, params or {})
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
        local function backlog(flow_id, number, stamp)
            local tx, begin_err = db:begin()
            if begin_err then error(tostring(begin_err)) end
            local ok, err = pcall(function()
                for first = 1, number, 100 do
                    local data, commits, placeholders = {}, {}, {}
                    for _ = first, math.min(first + 99, number) do
                        placeholders[#placeholders + 1] = "(?,?,?,?,?)"
                        for _, v in ipairs({ uuid.v7(), flow_id, "node.input", "old input", stamp }) do data[#data + 1] = v end
                        for _, v in ipairs({ uuid.v7(), flow_id, uuid.v7(), "{}", stamp }) do commits[#commits + 1] = v end
                    end
                    local values = table.concat(placeholders, ",")
                    exec("INSERT INTO dataflow_data(data_id,dataflow_id,type,content,created_at) VALUES " .. values, data, tx)
                    exec("INSERT INTO dataflow_commits(commit_id,dataflow_id,op_id,payload,created_at) VALUES " .. values, commits, tx)
                end
            end)
            if not ok then tx:rollback(); error(tostring(err)) end
            local _, commit_err = tx:commit()
            if commit_err then tx:rollback(); error(tostring(commit_err)) end
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
            test.eq(again.data, 0)
            test.eq(again.commits, 0)
        end)
        it("yields outside the transaction and restarts safely after cancellation", function()
            local id = flow("failed")
            local stats, err = sweeper.run(db, { days = 30, batch_size = 1 }, function()
                test.eq(count("dataflow_data", id), 5)
                return false
            end)
            test.is_nil(err)
            test.eq(stats.data, 1)
            test.eq(stats.commits, 0)
            test.eq(count("dataflow_data", id), 5)
            local next_stats, next_err = sweeper.run(db, { days = 30, batch_size = 1 })
            test.is_nil(next_err)
            test.eq(next_stats.data, 1)
            test.eq(next_stats.commits, 1)
        end)
        it("drains mixed retained history tiebreaks at the installed batch size", function()
            local protected = flow("completed")
            local waiting = flow("waiting", protected)
            local eligible = flow("terminated")
            backlog(protected, 1003, "1999-01-01T00:00:00Z")
            backlog(eligible, 1004, old)
            insert("dataflow_data", { data_id = uuid.v7(), dataflow_id = eligible, type = "node.input",
                content = "recent input", created_at = time.now():format(time.RFC3339) })

            local yielded = 0
            local stats, err = sweeper.run(db, { days = 30, batch_size = 1000 }, function(pause)
                test.is_true(pause == "1ms" or pause == "20ms")
                yielded = yielded + 1
                return true
            end)
            test.is_nil(err)
            test.eq(stats.data, 1006)
            test.eq(stats.commits, 1005)
            test.is_true(yielded >= 4)
            test.eq(count("dataflow_data", protected), 1009)
            test.eq(count("dataflow_commits", protected), 1006)
            test.eq(count("dataflow_data", waiting), 6)
            test.eq(count("dataflow_data", eligible), 5)
            test.eq(count("dataflow_commits", eligible), 2)
            for _, id in ipairs({ protected, waiting, eligible }) do test.eq(count("dataflows", id), 1) end
        end)
        it("protects a whole family with an active ancestor or descendant", function()
            local active = flow("running")
            local child = flow("completed", active)
            local root = flow("completed")
            local running_child = flow("waiting", root)
            local stats, err = sweeper.run(db, { days = 30 })
            test.is_nil(err)
            test.eq(stats.data, 0)
            test.eq(stats.commits, 0)
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
            test.eq(stats.data, 0)
            test.eq(stats.commits, 0)
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
