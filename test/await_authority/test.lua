local test = require("test")
local sql = require("sql")
local json = require("json")
local uuid = require("uuid")
local time = require("time")
local security = require("security")
local funcs = require("funcs")
local consts = require("consts")
local activation_repo = require("activation_repo")
local dataflow_repo = require("dataflow_repo")

local function wait_until(predicate, timeout_ms: number): boolean
    for _ = 1, math.ceil(timeout_ms / 50) do
        if predicate() then return true end
        time.sleep("50ms")
    end
    return false
end

local function run_tests()
    test.describe("UPSTREAM-DATAFLOW-LIVE-AUTHORITY", function()
        local db
        local token_store
        test.before_all(function()
            db = assert(sql.get("app:db"))
            token_store = assert(security.token_store("app.await_authority:tokens"))
            local _, err = db:execute([[CREATE TABLE IF NOT EXISTS await_authority_subjects (
                subject_id TEXT PRIMARY KEY, groups_json TEXT NOT NULL,
                active INTEGER NOT NULL, effects INTEGER NOT NULL DEFAULT 0
            )]])
            test.is_nil(err)
        end)
        test.after_all(function() db:release() end)

        test.it("denies a protected node after membership removal during Await", function()
            local subject_id = uuid.v7()
            local groups = {"app.await_authority:member", "app.await_authority:privileged"}
            local _, insert_err = db:execute(
                "INSERT INTO await_authority_subjects (subject_id, groups_json, active) VALUES ($1, $2, 1)",
                {subject_id, assert(json.encode(groups))})
            test.is_nil(insert_err)
            local issued_actor = assert(security.new_actor(subject_id, {marker = "await-principal"}))
            local issued_scope = assert(security.named_scope("app.await_authority:privileged"))
            local token, token_err = token_store:create(issued_actor, issued_scope)
            test.is_nil(token_err)
            token = test.not_nil(token) :: string
            local actor, scope, validation_err = token_store:validate(token)
            test.is_nil(validation_err)
            test.eq(actor:id(), subject_id)
            test.eq(scope:evaluate(actor, "funcs.call", "app.await_authority:protected_effect"), "allow")

            local signal_id = uuid.v7()
            local dataflow_id, start_err = funcs.new():with_actor(actor):with_scope(scope)
                :call("app.await_authority:start", {signal_id = signal_id})
            test.is_nil(start_err)
            dataflow_id = test.not_nil(dataflow_id) :: string
            test.is_true(wait_until(function()
                local activation, activation_err = activation_repo.get(dataflow_id)
                test.is_nil(activation_err)
                return activation and activation.desired_active == false and
                    process.registry.lookup("dataflow." .. dataflow_id) == nil
            end, 10000), "Await passivates before membership changes")
            local before = test.not_nil(dataflow_repo.get(dataflow_id)) :: any
            test.eq(before.actor_id, subject_id)
            local before_effects = assert(db:query("SELECT effects FROM await_authority_subjects WHERE subject_id = $1", {subject_id}))
            test.eq(before_effects[1].effects, 0)

            local _, update_err = db:execute("UPDATE await_authority_subjects SET groups_json = $1 WHERE subject_id = $2",
                {assert(json.encode({"app.await_authority:member"})), subject_id})
            test.is_nil(update_err)
            local live_actor, live_scope, live_err = token_store:validate(token)
            test.is_nil(live_err)
            test.eq(live_actor:id(), subject_id)
            test.neq(live_scope:evaluate(live_actor, "funcs.call", "app.await_authority:protected_effect"), "allow",
                "native subject_lookup revokes the removed permission")
            local signalled, signal_err = funcs.new():with_actor(live_actor):with_scope(live_scope)
                :call("app.await_authority:signal", {dataflow_id = dataflow_id, signal_id = signal_id})
            test.is_nil(signal_err)
            test.not_nil(signalled)
            test.is_true(wait_until(function()
                local status = assert(dataflow_repo.get(dataflow_id)).status
                return status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE
            end, 10000), "resumed workflow reaches a terminal result")
            local effects = assert(db:query("SELECT effects FROM await_authority_subjects WHERE subject_id = $1", {subject_id}))
            print("AWAIT_AUTHORITY_RECEIPT " .. assert(json.encode({
                issue = "UPSTREAM-DATAFLOW-LIVE-AUTHORITY", subject_id = subject_id,
                dataflow_id = dataflow_id, remaining_groups = {"app.await_authority:member"},
                native_decision = live_scope:evaluate(live_actor, "funcs.call", "app.await_authority:protected_effect"),
                effects_before = before_effects[1].effects, effects_after = effects[1].effects,
                final_status = assert(dataflow_repo.get(dataflow_id)).status,
            })))
            test.eq(effects[1].effects, 0, "removed authority never executes the post-Await effect")
            test.eq(assert(dataflow_repo.get(dataflow_id)).status, consts.STATUS.COMPLETED_FAILURE)
        end)
    end)
end

return {run_tests = test.run_cases(run_tests)}
