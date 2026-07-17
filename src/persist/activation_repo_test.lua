local test = require("test")
local sql = require("sql")
local uuid = require("uuid")
local time = require("time")
local activation_repo = require("activation_repo")
local consts = require("dataflow_consts")

local function define_tests()
    test.describe("Dataflow activation repository", function()
        local created = {}

        local function now(offset)
            return time.now():add((offset or 0) * time.SECOND):format(time.RFC3339NANO)
        end

        local function create_dataflow(status)
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local id = uuid.v7()
            local timestamp = now()
            local _, insert_err = sql.builder.insert("dataflows"):set_map({
                dataflow_id = id,
                actor_id = "activation-test",
                type = "activation-test",
                status = status,
                metadata = "{}",
                created_at = timestamp,
                updated_at = timestamp,
            }):run_with(db):exec()
            db:release()
            test.is_nil(insert_err)
            table.insert(created, id)
            return id
        end

        local function transaction(fn, should_commit)
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local tx, begin_err = db:begin()
            test.is_nil(begin_err)
            local result, operation_err = fn(tx)
            if operation_err then
                tx:rollback()
                db:release()
                return result, operation_err
            end
            if should_commit == false then
                tx:rollback()
            else
                local committed, commit_err = tx:commit()
                test.is_true(committed)
                test.is_nil(commit_err)
            end
            db:release()
            return result, nil
        end

        local function find_active(dataflow_id)
            local rows, list_err = activation_repo.list_active()
            test.is_nil(list_err)
            for _, row in ipairs(rows or {}) do
                if row.dataflow_id == dataflow_id then return row end
            end
            return nil
        end

        local function wake_generation(dataflow_id, wake_key)
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local rows, query_err = db:query([[
                SELECT activation_generation FROM dataflow_wakes
                WHERE dataflow_id = ? AND wake_key = ?
            ]], { dataflow_id, wake_key })
            db:release()
            test.is_nil(query_err)
            return rows and rows[1] and tonumber(rows[1].activation_generation) or nil
        end

        local function wake_count(dataflow_id)
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local rows, query_err = db:query(
                "SELECT COUNT(*) AS total FROM dataflow_wakes WHERE dataflow_id = ?",
                { dataflow_id })
            db:release()
            test.is_nil(query_err)
            return tonumber(rows and rows[1] and rows[1].total) or 0
        end

        test.after_all(function()
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            for _, id in ipairs(created) do
                db:execute("DELETE FROM dataflows WHERE dataflow_id = ?", { id })
            end
            db:release()
        end)

        test.it("keeps bare PENDING inert and recovers only legacy RUNNING rows", function()
            local pending_id = create_dataflow(consts.STATUS.PENDING)
            local running_id = create_dataflow(consts.STATUS.RUNNING)

            local pending = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.ensure_running_recovery_tx(tx, pending_id, now())
            end))) :: any
            test.is_false(pending.changed)
            test.eq(pending.status, consts.STATUS.PENDING)
            test.is_nil(select(1, activation_repo.get(pending_id)))
            test.is_nil(find_active(pending_id))

            local running = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.ensure_running_recovery_tx(tx, running_id, now())
            end))) :: any
            test.is_true(running.changed)
            test.is_true(running.recovered)
            test.eq(running.generation, 1)
            test.is_true(running.desired_active)

            local repeated = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.ensure_running_recovery_tx(tx, running_id, now(1))
            end))) :: any
            test.is_false(repeated.changed)
            test.eq(repeated.generation, 1)
        end)

        test.it("persists plain-object launch arguments and advances requests monotonically", function()
            local id = create_dataflow(consts.STATUS.PENDING)
            local first = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {
                    init_func_id = "app:init",
                    context = { attempt = 1, tags = { "one", "two" } },
                }, now())
            end))) :: any
            test.eq(first.generation, 1)
            test.is_true(first.desired_active)

            local stored = test.not_nil(select(1, activation_repo.get(id))) :: any
            test.eq(stored.launch_args.init_func_id, "app:init")
            test.eq(stored.launch_args.context.attempt, 1)
            test.eq(stored.launch_args.context.tags[2], "two")
            test.not_nil(find_active(id))

            local second = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, { on_complete = "app:done" }, now(1))
            end))) :: any
            test.eq(second.generation, 2)
            test.eq(second.launch_args.on_complete, "app:done")
            test.is_nil(second.launch_args.init_func_id)

            local invalid, invalid_err = transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, { "not", "an", "object" }, now(2))
            end)
            test.is_nil(invalid)
            test.contains(invalid_err, "plain object")
            stored = test.not_nil(select(1, activation_repo.get(id))) :: any
            test.eq(stored.generation, 2)
        end)

        test.it("rejects activation requests for terminal workflows without creating state", function()
            local id = create_dataflow(consts.STATUS.COMPLETED_SUCCESS)
            local result = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end))) :: any
            test.is_true(result.terminal)
            test.is_false(result.changed)
            test.is_nil(select(1, activation_repo.get(id)))
        end)

        test.it("advances only when a newly inserted signal wake wins", function()
            local id = create_dataflow(consts.STATUS.WAITING)
            local wake_key = "signal:" .. uuid.v7()
            local first = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_for_signal_tx(tx, id, wake_key, now(), now())
            end))) :: any
            test.is_true(first.wake_inserted)
            test.eq(first.generation, 1)
            test.eq(wake_generation(id, wake_key), 1)

            local duplicate = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_for_signal_tx(tx, id, wake_key, now(1), now(1))
            end))) :: any
            test.is_false(duplicate.changed)
            test.is_false(duplicate.wake_inserted)
            test.eq(duplicate.generation, 1)
            test.eq((test.not_nil(select(1, activation_repo.get(id))) :: any).generation, 1)

            local second_key = "signal:" .. uuid.v7()
            local second = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_for_signal_tx(tx, id, second_key, now(2), now(2))
            end))) :: any
            test.eq(second.generation, 2)
            test.eq(wake_generation(id, second_key), 2)

            local stale_release = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_if_generation_tx(tx, id, 1, now(3))
            end))) :: any
            test.is_false(stale_release.released)
            test.eq(stale_release.generation, 2)
            test.is_true((test.not_nil(select(1, activation_repo.get(id))) :: any).desired_active)

            local current_release = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_if_generation_tx(tx, id, 2, now(4))
            end))) :: any
            test.is_true(current_release.released)
            test.is_false((test.not_nil(select(1, activation_repo.get(id))) :: any).desired_active)
        end)

        test.it("promotes a due timer exactly once and consumes only its fenced row", function()
            local id = create_dataflow(consts.STATUS.WAITING)
            local wake_key = "yield:" .. uuid.v7()
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local _, insert_err = db:execute([[
                INSERT INTO dataflow_wakes(dataflow_id, wake_key, wake_at)
                VALUES (?, ?, ?)
            ]], { id, wake_key, now(-2) })
            db:release()
            test.is_nil(insert_err)

            local first = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_due_tx(tx, id, wake_key, now())
            end))) :: any
            test.is_true(first.promoted)
            test.eq(first.generation, 1)
            test.eq(wake_generation(id, wake_key), 1)

            local repeated = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_due_tx(tx, id, wake_key, now(1))
            end))) :: any
            test.is_false(repeated.promoted)
            test.is_true(repeated.already_promoted)
            test.eq(repeated.generation, 1)
            test.eq((test.not_nil(select(1, activation_repo.get(id))) :: any).generation, 1)

            local wrong_fence = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.consume_wake_tx(tx, id, wake_key, 2)
            end))) :: any
            test.is_false(wrong_fence.consumed)
            test.eq(wake_generation(id, wake_key), 1)

            local exact = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.consume_wake_tx(tx, id, wake_key, 1)
            end))) :: any
            test.is_true(exact.consumed)
            test.is_nil(wake_generation(id, wake_key))
        end)

        test.it("does not promote timers before their deadline", function()
            local id = create_dataflow(consts.STATUS.WAITING)
            local wake_key = "yield:" .. uuid.v7()
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            test.is_nil(select(2, db:execute([[
                INSERT INTO dataflow_wakes(dataflow_id, wake_key, wake_at)
                VALUES (?, ?, ?)
            ]], { id, wake_key, now(60) })))
            db:release()

            local result = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_due_tx(tx, id, wake_key, now())
            end))) :: any
            test.is_false(result.promoted)
            test.is_false(result.due)
            test.is_nil(select(1, activation_repo.get(id)))
            test.is_nil(wake_generation(id, wake_key))
        end)

        test.it("converges terminal activation and every stale wake during due promotion", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, { init_func_id = "app:init" }, now())
            end)))
            local due_key = "yield:" .. uuid.v7()
            local future_key = "yield:" .. uuid.v7()
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local _, insert_err = db:execute([[
                INSERT INTO dataflow_wakes(dataflow_id, wake_key, wake_at)
                VALUES (?, ?, ?), (?, ?, ?)
            ]], { id, due_key, now(-2), id, future_key, now(60) })
            db:release()
            test.is_nil(insert_err)
            test.eq(wake_count(id), 2)

            local result = test.not_nil(select(1, transaction(function(tx)
                local _, status_err = tx:execute(
                    "UPDATE dataflows SET status = ? WHERE dataflow_id = ?",
                    { consts.STATUS.COMPLETED_SUCCESS, id })
                if status_err then return nil, status_err end
                return activation_repo.activate_due_tx(tx, id, due_key, now())
            end))) :: any

            test.is_true(result.terminal)
            test.is_false(result.promoted)
            test.is_true(result.activation_disabled)
            test.is_true(result.wake_index_changed)
            test.eq(wake_count(id), 0)
            local stored = test.not_nil(select(1, activation_repo.get(id))) :: any
            test.is_false(stored.desired_active)
            test.is_nil(stored.launch_args)
            test.is_nil(find_active(id))

            local repeated = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_due_tx(tx, id, due_key, now(1))
            end))) :: any
            test.is_true(repeated.terminal)
            test.is_false(repeated.promoted)
            test.is_false(repeated.changed)
            test.is_false(repeated.activation_disabled)
            test.is_false(repeated.wake_index_changed)
        end)

        test.it("rolls signal wake and generation back as one transaction", function()
            local id = create_dataflow(consts.STATUS.WAITING)
            local wake_key = "signal:" .. uuid.v7()
            local result = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_for_signal_tx(tx, id, wake_key, now(), now())
            end, false))) :: any
            test.is_true(result.wake_inserted)
            test.is_nil(select(1, activation_repo.get(id)))
            test.is_nil(wake_generation(id, wake_key))
        end)

        test.it("disables terminal activation and clears all wakes in the terminal transaction", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, { init_func_id = "app:init" }, now())
            end)))
            local wake_key = "signal:" .. uuid.v7()
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_for_signal_tx(tx, id, wake_key, now(), now())
            end)))

            local disabled = test.not_nil(select(1, transaction(function(tx)
                local _, status_err = tx:execute(
                    "UPDATE dataflows SET status = ? WHERE dataflow_id = ?",
                    { consts.STATUS.COMPLETED_FAILURE, id })
                if status_err then return nil, status_err end
                return activation_repo.disable_terminal_tx(tx, id, now(1))
            end))) :: any
            test.is_true(disabled.terminal)
            test.is_true(disabled.wake_index_changed)
            local stored = test.not_nil(select(1, activation_repo.get(id))) :: any
            test.is_false(stored.desired_active)
            test.is_nil(stored.launch_args)
            test.is_nil(wake_generation(id, wake_key))
            test.is_nil(find_active(id))
        end)
    end)
end

return { run_tests = test.run_cases(define_tests) }
