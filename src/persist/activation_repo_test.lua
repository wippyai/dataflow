local test = require("test")
local sql = require("sql")
local uuid = require("uuid")
local time = require("time")
local activation_repo = require("activation_repo")
local consts = require("dataflow_consts")

local function rebind(query: string, db_type: any): string
    if db_type ~= sql.type.POSTGRES and db_type ~= "postgres" then return query end
    local index = 0
    return (query:gsub("%?", function()
        index = index + 1
        return "$" .. index
    end))
end

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
            local rows, query_err = sql.builder.select("activation_generation")
                :from("dataflow_wakes")
                :where("dataflow_id = ?", dataflow_id)
                :where("wake_key = ?", wake_key)
                :run_with(db):query()
            db:release()
            test.is_nil(query_err)
            return rows and rows[1] and tonumber(rows[1].activation_generation) or nil
        end

        local function wake_count(dataflow_id)
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local rows, query_err = sql.builder.select("COUNT(*) AS total")
                :from("dataflow_wakes")
                :where("dataflow_id = ?", dataflow_id)
                :run_with(db):query()
            db:release()
            test.is_nil(query_err)
            return tonumber(rows and rows[1] and rows[1].total) or 0
        end

        -- SQLite connections do not enforce the cascading foreign keys, so the
        -- dependent rows are removed with their dataflow explicitly.
        test.after_all(function()
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            for _, id in ipairs(created) do
                for _, table_name in ipairs({ "dataflow_wakes", "dataflow_activations", "dataflows" }) do
                    sql.builder.delete(table_name)
                        :where("dataflow_id = ?", id)
                        :run_with(db):exec()
                end
            end
            db:release()
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

        local function owner(token: string, runtime_epoch: string): any
            return { token = token, pid = "pid-" .. token, runtime_epoch = runtime_epoch }
        end

        test.it("admits one owner per runtime and lets the same token retry its admission", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            local requested = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end))) :: any
            test.is_nil(requested.owner_token)
            test.is_nil(requested.owner_phase)

            local first = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(1))
            end))) :: any
            test.is_true(first.admitted)
            test.eq(first.generation, 1)
            test.eq(first.owner_token, "t1")
            test.eq(first.owner_pid, "pid-t1")
            test.eq(first.owner_epoch, "runtime-a")
            test.eq(first.owner_phase, "running")

            local rival = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t2", "runtime-a"), now(2))
            end))) :: any
            test.is_false(rival.admitted)
            test.eq(rival.refused, "owned")
            test.eq(rival.owner_token, "t1")

            local retried = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(3))
            end))) :: any
            test.is_true(retried.admitted)
            test.eq(retried.owner_token, "t1")

            local rebooted = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t3", "runtime-b"), now(4))
            end))) :: any
            test.is_true(rebooted.admitted, "a running owner of an earlier runtime is gone")
            test.eq(rebooted.owner_token, "t3")
            test.eq(rebooted.owner_epoch, "runtime-b")
        end)

        test.it("refuses admission for terminal, inactive and older-than-requested activations", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end)))
            local stale = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 2, owner("t1", "runtime-a"), now(1))
            end))) :: any
            test.is_false(stale.admitted)
            test.eq(stale.refused, "stale")
            test.is_nil(stale.owner_token)

            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_owner_tx(tx, id, { generation = 1 }, now(2))
            end))) :: any).released)
            local inactive = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(3))
            end))) :: any
            test.is_false(inactive.admitted)
            test.eq(inactive.refused, "inactive")

            local done = create_dataflow(consts.STATUS.COMPLETED_SUCCESS)
            local terminal = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, done, 1, owner("t1", "runtime-a"), now(4))
            end))) :: any
            test.is_false(terminal.admitted)
            test.eq(terminal.refused, "terminal")
        end)

        test.it("keeps the live owner across generation advances", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end)))
            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(1))
            end))) :: any).admitted)
            local signaled = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_for_signal_tx(
                    tx, id, "signal:" .. uuid.v7(), now(2), now(2))
            end))) :: any
            test.eq(signaled.generation, 2)
            local requested = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now(3))
            end))) :: any
            test.eq(requested.generation, 3)
            for _, row in ipairs({ signaled, requested, test.not_nil(find_active(id)) }) do
                test.eq((row :: any).owner_token, "t1")
                test.eq((row :: any).owner_phase, "running")
                test.eq((row :: any).owner_epoch, "runtime-a")
            end
        end)

        test.it("releases only for the owning token at the current generation", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end)))
            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(1))
            end))) :: any).admitted)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_for_signal_tx(
                    tx, id, "signal:" .. uuid.v7(), now(2), now(2))
            end)))

            local stale = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_owner_tx(tx, id,
                    { owner_token = "t1", owner_phase = "running", generation = 1 }, now(3))
            end))) :: any
            test.is_false(stale.released)
            test.eq(stale.generation, 2)

            local foreign = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_owner_tx(tx, id,
                    { owner_token = "t2", owner_phase = "running", generation = 2 }, now(4))
            end))) :: any
            test.is_false(foreign.released)
            local owned = test.not_nil(select(1, activation_repo.get(id))) :: any
            test.eq(owned.owner_token, "t1")
            test.eq(owned.owner_phase, "running")
            test.is_true(owned.desired_active)

            local released = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_owner_tx(tx, id,
                    { owner_token = "t1", owner_phase = "running", generation = 2 }, now(5))
            end))) :: any
            test.is_true(released.released)
            test.eq(released.generation, 2)
            local row = test.not_nil(select(1, activation_repo.get(id))) :: any
            test.is_false(row.desired_active)
            test.eq(row.owner_phase, "released")
            test.eq(row.owner_token, "t1")

            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now(6))
            end)))
            local successor = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 3, owner("t4", "runtime-a"), now(7))
            end))) :: any
            test.is_true(successor.admitted, "a released owner is replaced in the same runtime")
        end)

        test.it("fails a lost owner by token alone at the latest generation", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end)))
            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(1))
            end))) :: any).admitted)
            for offset = 2, 3 do
                test.not_nil(select(1, transaction(function(tx)
                    return activation_repo.activate_for_signal_tx(
                        tx, id, "signal:" .. uuid.v7(), now(offset), now(offset))
                end)))
            end
            local failed = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_owner_tx(tx, id,
                    { owner_token = "t1", owner_phase = "running" }, now(4))
            end))) :: any
            test.is_true(failed.released)
            test.eq(failed.generation, 3)
        end)

        test.it("verifies the owning token inside a transaction", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end)))
            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(1))
            end))) :: any).admitted)
            test.is_true(select(1, transaction(function(tx)
                return activation_repo.verify_owner_tx(tx, id, "t1")
            end)))
            local _, foreign_err = transaction(function(tx)
                return activation_repo.verify_owner_tx(tx, id, "t2")
            end)
            test.contains(tostring(foreign_err), "ownership lost")
            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_owner_tx(tx, id,
                    { owner_token = "t1", owner_phase = "running", generation = 1 }, now(2))
            end))) :: any).released)
            local _, released_err = transaction(function(tx)
                return activation_repo.verify_owner_tx(tx, id, "t1")
            end)
            test.contains(tostring(released_err), "ownership lost")
        end)

        test.it("rejects owner writes once the workflow is terminal or the request inactive", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end)))
            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(1))
            end))) :: any).admitted)

            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local _, inactive_err = db:execute(rebind(
                "UPDATE dataflow_activations SET desired_active = ? WHERE dataflow_id = ?", db:type()),
                { false, id })
            test.is_nil(inactive_err)
            local _, inactive_write = transaction(function(tx)
                return activation_repo.verify_owner_tx(tx, id, "t1")
            end)
            test.contains(tostring(inactive_write), "not active")

            local _, active_err = db:execute(rebind(
                "UPDATE dataflow_activations SET desired_active = ? WHERE dataflow_id = ?", db:type()),
                { true, id })
            test.is_nil(active_err)
            local _, cancel_err = db:execute(rebind(
                "UPDATE dataflows SET status = ? WHERE dataflow_id = ?", db:type()),
                { consts.STATUS.CANCELLED, id })
            db:release()
            test.is_nil(cancel_err)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.disable_terminal_tx(tx, id, now(2))
            end)))
            local _, terminal_write = transaction(function(tx)
                return activation_repo.verify_owner_tx(tx, id, "t1")
            end)
            test.contains(tostring(terminal_write), "not active")
        end)

        test.it("re-admits the same token only as a running owner of an active request", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end)))
            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(1))
            end))) :: any).admitted)
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local _, inactive_err = db:execute(rebind(
                "UPDATE dataflow_activations SET desired_active = ? WHERE dataflow_id = ?", db:type()),
                { false, id })
            db:release()
            test.is_nil(inactive_err)
            local inactive = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(2))
            end))) :: any
            test.is_false(inactive.admitted)
            test.eq(inactive.refused, "inactive")

            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now(3))
            end)))
            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_owner_tx(tx, id,
                    { owner_token = "t1", owner_phase = "running", generation = 2 }, now(4))
            end))) :: any).released)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now(5))
            end)))
            local readmitted = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 3, owner("t1", "runtime-a"), now(6))
            end))) :: any
            test.is_true(readmitted.admitted)
            test.eq(readmitted.owner_phase, "running")
            test.is_true(select(1, transaction(function(tx)
                return activation_repo.verify_owner_tx(tx, id, "t1")
            end)))
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
                return activation_repo.release_owner_tx(tx, id, { generation = 1 }, now(3))
            end))) :: any
            test.is_false(stale_release.released)
            test.eq(stale_release.generation, 2)
            test.is_true((test.not_nil(select(1, activation_repo.get(id))) :: any).desired_active)

            local current_release = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.release_owner_tx(tx, id, { generation = 2 }, now(4))
            end))) :: any
            test.is_true(current_release.released)
            test.is_false((test.not_nil(select(1, activation_repo.get(id))) :: any).desired_active)
        end)

        test.it("reads the activation and workflow status under the workflow lock", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, {}, now())
            end)))
            test.is_true((test.not_nil(select(1, transaction(function(tx)
                return activation_repo.admit_owner_tx(tx, id, 1, owner("t1", "runtime-a"), now(1))
            end))) :: any).admitted)

            local observed = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.read_locked_tx(tx, id)
            end))) :: any
            test.eq(observed.status, consts.STATUS.RUNNING)
            local activation = test.not_nil(observed.activation) :: any
            test.eq(activation.generation, 1)
            test.eq(activation.owner_token, "t1")
            test.eq(activation.owner_phase, "running")

            local _, missing_err = transaction(function(tx)
                return activation_repo.read_locked_tx(tx, uuid.v7())
            end)
            test.contains(tostring(missing_err), "dataflow not found")
        end)

        test.it("promotes a due timer exactly once and consumes only its fenced row", function()
            local id = create_dataflow(consts.STATUS.WAITING)
            local wake_key = "yield:" .. uuid.v7()
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local _, insert_err = sql.builder.insert("dataflow_wakes"):set_map({
                dataflow_id = id,
                wake_key = wake_key,
                wake_at = now(-2),
            }):run_with(db):exec()
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
            test.is_nil(select(2, sql.builder.insert("dataflow_wakes"):set_map({
                dataflow_id = id,
                wake_key = wake_key,
                wake_at = now(60),
            }):run_with(db):exec()))
            db:release()

            local result = test.not_nil(select(1, transaction(function(tx)
                return activation_repo.activate_due_tx(tx, id, wake_key, now())
            end))) :: any
            test.is_false(result.promoted)
            test.is_false(result.due)
            test.is_nil(select(1, activation_repo.get(id)))
            test.is_nil(wake_generation(id, wake_key))
        end)

        test.it("registers a yield deadline only when it is a valid RFC 3339 time", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            local _, malformed_err = transaction(function(tx)
                return activation_repo.register_yield_wake_tx(tx, id, "malformed", "next tuesday")
            end)
            test.contains(tostring(malformed_err), "RFC 3339")
            test.eq(wake_count(id), 0)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.register_yield_wake_tx(tx, id, "valid", now(60))
            end)))
            test.eq(wake_count(id), 1)
        end)

        test.it("converges terminal activation and every stale wake during due promotion", function()
            local id = create_dataflow(consts.STATUS.RUNNING)
            test.not_nil(select(1, transaction(function(tx)
                return activation_repo.request_activation_tx(tx, id, { init_func_id = "app:init" }, now())
            end)))
            local due_key = "yield:" .. uuid.v7()
            local future_key = "yield:" .. uuid.v7()
            local db = test.not_nil(select(1, sql.get("app:db"))) :: any
            local _, insert_err = sql.builder.insert("dataflow_wakes"):set_map({
                dataflow_id = id,
                wake_key = due_key,
                wake_at = now(-2),
            }):run_with(db):exec()
            if not insert_err then
                _, insert_err = sql.builder.insert("dataflow_wakes"):set_map({
                    dataflow_id = id,
                    wake_key = future_key,
                    wake_at = now(60),
                }):run_with(db):exec()
            end
            db:release()
            test.is_nil(insert_err)
            test.eq(wake_count(id), 2)

            local result = test.not_nil(select(1, transaction(function(tx)
                local _, status_err = sql.builder.update("dataflows")
                    :set("status", consts.STATUS.COMPLETED_SUCCESS)
                    :where("dataflow_id = ?", id)
                    :run_with(tx):exec()
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
                local _, status_err = sql.builder.update("dataflows")
                    :set("status", consts.STATUS.COMPLETED_FAILURE)
                    :where("dataflow_id = ?", id)
                    :run_with(tx):exec()
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
