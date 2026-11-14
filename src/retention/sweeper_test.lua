local test = require("test")
local sql = require("sql")
local uuid = require("uuid")
local time = require("time")
local sweeper = require("sweeper")
local registry = require("registry")
local ops = require("ops")
local client = require("client")
local commit = require("commit")
local commit_repo = require("commit_repo")
local activation_repo = require("activation_repo")
local dataflow_repo = require("dataflow_repo")

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
            local root_id = id
            if parent then
                local rows = sql.builder.select("retention_root_id"):from("dataflows")
                    :where("dataflow_id = ?", parent):run_with(db):query()
                root_id = rows[1].retention_root_id
            end
            insert("dataflows", { dataflow_id = id, parent_dataflow_id = parent,
                retention_root_id = root_id, retention_held = 0,
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
        it("grants the service access to every retention setting", function()
            local entry, err = registry.get("userspace.dataflow.security:retention.configuration")
            test.is_nil(err)
            test.not_nil(entry)
            local allowed = {}
            for _, value in ipairs(entry.data.policy.resources) do allowed[tostring(value)] = true end
            for _, name in ipairs({ "diagnostic_retention_days", "flow_retention_days",
                "retention_interval_seconds", "retention_batch_size", "retention_db" }) do
                test.eq(allowed["userspace.dataflow.env:" .. name], true)
            end
        end)
        it("defaults to disabled and rejects malformed or unbounded configuration", function()
            local stats, err = sweeper.run(nil, {})
            test.is_nil(err)
            test.eq(stats.enabled, false)
            for _, options in ipairs({ { days = -1 }, { days = 1.5 }, { days = "bad" },
                { days = 30, flow_days = 0, batch_size = 0 }, { days = 30, flow_days = 0, batch_size = 1001 } }) do
                local valid, validation_err = sweeper.validate(options)
                test.is_nil(valid)
                test.not_nil(validation_err)
            end
        end)
        it("dry runs count candidates without deleting them", function()
            local id = flow("completed")
            local stats, err = sweeper.run(db, { days = 30, flow_days = 0, dry_run = true })
            test.is_nil(err)
            test.eq(stats.data_candidates, 2)
            test.eq(stats.commit_candidates, 1)
            test.eq(stats.data, 0)
            test.eq(count("dataflow_data", id), 6)
        end)
        it("prunes only old diagnostics and preserves results evidence and last/pending commits", function()
            local id = flow("completed")
            local stats, err = sweeper.run(db, { days = 30, flow_days = 0 })
            test.is_nil(err)
            test.eq(stats.data, 2)
            test.eq(stats.commits, 1)
            test.eq(count("dataflow_data", id), 4)
            test.eq(count("dataflow_commits", id), 2)
            test.eq(count("dataflows", id), 1)
            local again, again_err = sweeper.run(db, { days = 30, flow_days = 0 })
            test.is_nil(again_err)
            test.eq(again.data + again.commits, 0)
        end)
        it("drains more than several diagnostic batches in one pass", function()
            local id = flow("failed")
            for _ = 1, 17 do
                insert("dataflow_data", { data_id = uuid.v7(), dataflow_id = id,
                    type = "node.yield", content = "diagnostic", created_at = old })
                insert("dataflow_commits", { commit_id = uuid.v7(), dataflow_id = id,
                    op_id = uuid.v7(), payload = "{}", created_at = old })
            end
            local stats, err = sweeper.run(db, { days = 30, flow_days = 0, batch_size = 3 })
            test.is_nil(err)
            test.eq(stats.data, 19)
            test.eq(stats.commits, 18)
            test.eq(count("dataflow_data", id), 4)
            test.eq(count("dataflow_commits", id), 2)
        end)
        it("removes expired terminal families and every payload class in bounded pages", function()
            local root = flow("completed")
            local child = flow("failed", root)
            for _, id in ipairs({ root, child }) do
                local parent_node
                for _ = 1, 9 do
                    local node = uuid.v7()
                    insert("dataflow_nodes", { node_id = node, dataflow_id = id,
                        parent_node_id = parent_node, type = "test", status = "completed",
                        created_at = old, updated_at = old })
                    parent_node = node
                    insert("dataflow_data", { data_id = uuid.v7(), dataflow_id = id,
                        node_id = node, type = "node.result", content = "result", created_at = old })
                end
                insert("dataflow_activations", { dataflow_id = id, generation = 1,
                    desired_active = false, requested_at = old, updated_at = old })
                insert("dataflow_wakes", { dataflow_id = id, wake_key = "test", wake_at = old })
            end
            local stats, err = sweeper.run(db, { days = 30, batch_size = 2 })
            test.is_nil(err)
            test.eq(stats.flows, 2)
            for _, id in ipairs({ root, child }) do
                for _, name in ipairs({ "dataflows", "dataflow_nodes", "dataflow_data",
                    "dataflow_commits", "dataflow_activations", "dataflow_wakes" }) do
                    test.eq(count(name, id), 0)
                end
            end
        end)
        it("uses bounded indexed candidate seeks, including tied ages and protected head pages", function()
            for _ = 1, 8 do flow("running") end
            local id = flow("completed")
            local seen, tied_seek = {}, false
            local postgres = tostring(db:type()) == "postgres"
            local proxy = {}
            function proxy:type() return db:type() end
            function proxy:query(q, p) return db:query(q, p) end
            function proxy:begin()
                local tx, err = db:begin()
                if err then return nil, err end
                local wrapped = {}
                function wrapped:execute(q, p)
                    test.is_nil(q:find("LOCK TABLE", 1, true))
                    return tx:execute(q, p)
                end
                function wrapped:query(q, p)
                    test.is_nil(q:find("RECURSIVE", 1, true))
                    if q:find(" AS age FROM ", 1, true) then
                        if postgres then
                            local _, e = tx:execute("SET LOCAL enable_seqscan = off")
                            test.is_nil(e)
                        end
                        local plan, e = tx:query((postgres and "EXPLAIN " or "EXPLAIN QUERY PLAN ") .. q, p)
                        test.is_nil(e)
                        local description = ""
                        for _, row in ipairs(plan) do
                            for _, value in pairs(row) do description = description .. tostring(value) .. " " end
                        end
                        test.is_true(description:find(postgres and "Sort" or "TEMP B-TREE", 1, true) == nil)
                        test.is_true(description:find("idx_retention_", 1, true) ~= nil)
                        seen[#seen + 1] = description
                        if q:find("data_id >", 1, true) then tied_seek = true end
                    end
                    local rows, e = tx:query(q, p)
                    if q:find(" AS age FROM ", 1, true) and rows then test.is_true(#rows <= 2) end
                    return rows, e
                end
                function wrapped:commit() return tx:commit() end
                function wrapped:rollback() return tx:rollback() end
                return wrapped
            end
            local stats, err = sweeper.run(proxy, { days = 30, batch_size = 2 })
            test.is_nil(err)
            test.eq(stats.flows, 1)
            test.is_true(#seen > 5)
            test.is_true(tied_seek)
            test.eq(count("dataflows", id), 0)
        end)
        it("holds owner families at public creation and releases atomically with owner retention", function()
            local cli, cli_err = client.new({ commit = commit, dataflow_repo = dataflow_repo,
                security = { actor = function() return { id = function() return "retention-test" end } end,
                    scope = function() return {} end },
                execution_frame = { capture = function() return { actor_id = "retention-test", actor_context = "{}" } end } })
            test.is_nil(cli_err)
            local id, create_err = cli:create_workflow({}, { retain = true })
            test.is_nil(create_err)
            ids[#ids + 1] = id
            exec("UPDATE dataflows SET status='completed', updated_at=? WHERE dataflow_id=?", { old, id })
            local stats, err = sweeper.run(db, { days = 0, flow_days = 30, batch_size = 2 })
            test.is_nil(err)
            test.eq(stats.flows, 0)
            local tx = db:begin()
            local released, release_err = cli:release(id, { transaction = tx })
            test.is_nil(release_err)
            test.eq(released, true)
            tx:rollback()
            stats, err = sweeper.run(db, { days = 0, flow_days = 30 })
            test.is_nil(err)
            test.eq(stats.flows, 0)
            released, release_err = cli:release(id)
            test.is_nil(release_err)
            test.eq(released, true)
            test.eq(cli:release(id), true)
            stats, err = sweeper.run(db, { days = 0, flow_days = 30 })
            test.is_nil(err)
            test.eq(stats.flows, 1)
            test.eq(cli:release(id), true)
            test.eq(cli:release(uuid.v7()), true)
        end)
        it("protects held descendants and rejects another actor or a child releasing the root hold", function()
            local root = flow("completed")
            local child = flow("completed", root)
            exec("UPDATE dataflows SET retention_held=1 WHERE dataflow_id=?", { root })
            local released, release_err = dataflow_repo.release_hold(root, "another-owner")
            test.is_nil(released)
            test.contains(tostring(release_err), "access denied")
            released, release_err = dataflow_repo.release_hold(child, "retention-test")
            test.is_nil(released)
            test.contains(tostring(release_err), "only the family root")
            local stats, err = sweeper.run(db, { days = 0, flow_days = 30, batch_size = 2 })
            test.is_nil(err)
            test.eq(stats.flows, 0)
            test.eq(count("dataflow_data", child), 6)
            released, release_err = dataflow_repo.release_hold(root, "retention-test")
            test.eq(released, true)
            test.is_nil(release_err)
            stats, err = sweeper.run(db, { days = 0, flow_days = 30, batch_size = 2 })
            test.is_nil(err)
            test.eq(stats.flows, 2)
        end)
        it("rejects malformed holds and cannot change family identity through workflow updates", function()
            local root = flow("completed")
            local tx = db:begin()
            local _, err = ops.execute(tx, uuid.v7(), nil, { type = "CREATE_WORKFLOW",
                payload = { actor_id = "test", type = "test", retain = "true" } })
            test.contains(tostring(err), "retain must be a boolean")
            tx:rollback()
            tx = db:begin()
            local _, child_err = ops.execute(tx, uuid.v7(), nil, { type = "CREATE_WORKFLOW",
                payload = { actor_id = "test", type = "test", parent_dataflow_id = root, retain = true } })
            test.contains(tostring(child_err), "only supported on family roots")
            tx:rollback()
            tx = db:begin()
            local result, update_err = ops.execute(tx, root, nil, { type = "UPDATE_WORKFLOW",
                payload = { parent_dataflow_id = uuid.v7(), retention_root_id = uuid.v7(), status = "failed" } })
            test.is_nil(update_err)
            test.not_nil(result)
            local rows, read_err = sql.builder.select("retention_root_id", "parent_dataflow_id")
                :from("dataflows"):where("dataflow_id = ?", root):run_with(tx):query()
            test.is_nil(read_err)
            test.eq(rows[1].retention_root_id, root)
            test.is_nil(rows[1].parent_dataflow_id)
            tx:rollback()
        end)
        it("installs no database triggers or stored procedures", function()
            local rows, err
            if tostring(db:type()) == "postgres" then
                rows, err = db:query("SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN ('dataflows'::regclass,'dataflow_data'::regclass,'dataflow_nodes'::regclass,'dataflow_commits'::regclass,'dataflow_activations'::regclass,'dataflow_wakes'::regclass)")
            else
                rows, err = db:query("SELECT name FROM sqlite_master WHERE type='trigger'")
            end
            test.is_nil(err)
            test.eq(#rows, 0)
            if tostring(db:type()) == "postgres" then
                rows, err = db:query("SELECT proname FROM pg_proc WHERE pronamespace=current_schema()::regnamespace AND proname LIKE 'dataflow_retention_%'")
                test.is_nil(err)
                test.eq(#rows, 0)
            end
        end)
        it("resumes a committed retirement after interruption and fences late writes", function()
            local root = flow("completed")
            local child = flow("completed", root)
            local stats, err = sweeper.run(db, { days = 0, flow_days = 30, batch_size = 2 }, function()
                local rows, e = sql.builder.select("retention_started"):from("dataflows")
                    :where("dataflow_id = ?", root):run_with(db):query()
                test.is_nil(e)
                return rows[1].retention_started == nil
            end)
            test.is_nil(stats)
            test.not_nil(err)
            local function rejected(command)
                local tx = db:begin()
                local _, write_err = ops.execute(tx, child, nil, command)
                tx:rollback()
                test.not_nil(write_err)
                test.contains(tostring(write_err), "dataflow family is retiring")
            end
            rejected({ type = "UPDATE_WORKFLOW", payload = { status = "running" } })
            rejected({ type = "CREATE_WORKFLOW", payload = { dataflow_id = uuid.v7(),
                parent_dataflow_id = child, actor_id = "test", type = "test" } })
            rejected({ type = "CREATE_DATA", payload = { data_type = "node.result", content = "late" } })
            local _, commit_err = commit_repo.create(uuid.v7(), child, {})
            test.contains(tostring(commit_err), "dataflow family is retiring")
            local _, submit_err = commit._create_commit_only(uuid.v7(), child, { commands = {} }, {})
            test.contains(tostring(submit_err), "dataflow family is retiring")
            local tx = db:begin()
            local _, wake_err = activation_repo.register_yield_wake_tx(tx, child, "late", old)
            tx:rollback()
            test.contains(tostring(wake_err), "dataflow family is retiring")
            local _, context_err = dataflow_repo.capture_context_if_empty(child, "retention-test", "{}")
            test.contains(tostring(context_err), "dataflow family is retiring")
            local resumed, resume_err = sweeper.run(db, { days = 30, batch_size = 2 })
            test.is_nil(resume_err)
            test.eq(resumed.flows, 2)
        end)
        it("creates inherited identity in the insert and locks the parent root before child insertion", function()
            local root = flow("completed")
            local parent, child = uuid.v7(), uuid.v7()
            local tx = db:begin()
            local _, parent_err = ops.execute(tx, parent, nil, { type = "CREATE_WORKFLOW", payload = {
                dataflow_id = parent, parent_dataflow_id = root, actor_id = "test", type = "test" } })
            test.is_nil(parent_err)
            if tostring(db:type()) == "postgres" then
                local concurrent = db:begin()
                concurrent:execute("SET LOCAL lock_timeout = '50ms'")
                local _, invisible_err = ops.execute(concurrent, child, nil, { type = "CREATE_WORKFLOW", payload = {
                    dataflow_id = child, parent_dataflow_id = parent, actor_id = "test", type = "test" } })
                test.contains(tostring(invisible_err), "parent dataflow is not visible")
                concurrent:rollback()
                concurrent = db:begin()
                local _, invisible_commit_err = commit.tx_execute(concurrent, parent, uuid.v7(), {
                    { type = "CREATE_DATA", payload = { data_type = "node.result", content = "late" } } })
                test.contains(tostring(invisible_commit_err), "Workflow not found")
                concurrent:rollback()
                concurrent = db:begin()
                concurrent:execute("SET LOCAL lock_timeout = '50ms'")
                local _, blocked_err = ops.execute(concurrent, child, nil, { type = "CREATE_WORKFLOW", payload = {
                    dataflow_id = child, parent_dataflow_id = root, actor_id = "test", type = "test" } })
                test.not_nil(blocked_err)
                test.contains(tostring(blocked_err), "lock timeout")
                concurrent:rollback()
            end
            local _, child_err = ops.execute(tx, child, nil, { type = "CREATE_WORKFLOW", payload = {
                dataflow_id = child, parent_dataflow_id = parent, actor_id = "test", type = "test" } })
            test.is_nil(child_err)
            local rows, err = sql.builder.select("retention_root_id"):from("dataflows")
                :where("dataflow_id = ?", child):run_with(tx):query()
            test.is_nil(err)
            test.eq(rows[1].retention_root_id, root)
            tx:rollback()
        end)
        it("does not delete a family before its separately configured flow window", function()
            local id = flow("completed")
            exec("UPDATE dataflows SET updated_at = ? WHERE dataflow_id = ?",
                { time.now():add(-40 * 24 * time.HOUR):format(time.RFC3339NANO), id })
            local stats, err = sweeper.run(db, { days = 30, flow_days = 60, batch_size = 2 })
            test.is_nil(err)
            test.eq(stats.data, 2)
            test.eq(stats.flows, 0)
            test.eq(count("dataflow_data", id), 4)
        end)
        it("scopes wake pages to the retired flow", function()
            local retired, active = flow("completed"), flow("running")
            for _, id in ipairs({ retired, active }) do
                insert("dataflow_wakes", { dataflow_id = id, wake_key = "shared-key", wake_at = old })
            end
            local stats, err = sweeper.run(db, { days = 30, batch_size = 2 })
            test.is_nil(err)
            test.eq(stats.wakes, 1)
            test.eq(count("dataflow_wakes", active), 1)
            exec("DELETE FROM dataflow_wakes WHERE dataflow_id = ?", { active })
        end)
        it("keeps the flow window at least as long as workflow run retention", function()
            local config = sweeper.validate({ days = 7 })
            test.eq(config.flow_days, 30)
            local invalid, err = sweeper.validate({ days = 30, flow_days = 29 })
            test.is_nil(invalid)
            test.not_nil(err)
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
            local stats, err = sweeper.run(db, { days = 30, flow_days = 0 })
            test.is_nil(err)
            test.eq(stats.data, 4)
            test.eq(stats.commits, 2)
        end)
        it("preserves newly written diagnostics even on an old terminal flow", function()
            local id = flow("failed")
            exec("UPDATE dataflow_data SET created_at = ? WHERE dataflow_id = ?",
                { time.now():format(time.RFC3339), id })
            local stats, err = sweeper.run(db, { days = 30, flow_days = 0 })
            test.is_nil(err)
            test.eq(stats.data, 0)
            test.eq(stats.commits, 1)
        end)
    end)
end

return test.run_cases(define_tests)
