local test = require("test")
local sql = require("sql")
local json = require("json")
local uuid = require("uuid")
local time = require("time")
local security = require("security")
local funcs = require("funcs")
local consts = require("consts")
local activation_repo = require("activation_repo")
local authority = require("authority")
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
            local _, err = authority.create_schema(db)
            test.is_nil(err)
        end)
        test.after_all(function() db:release() end)

        for _, operation in ipairs({"capture", "resolve"}) do
            test.it("returns a native missing-credential error from " .. operation, function()
                local subject_id = uuid.v7()
                local value, err
                if operation == "capture" then
                    value, err = authority.capture(assert(security.new_actor(subject_id, {})),
                        assert(security.new_scope()))
                else
                    local scope
                    value, scope, err = authority.resolve({subject_id = subject_id})
                    test.is_nil(scope)
                end
                test.is_nil(value)
                test.eq(err:kind(), errors.NOT_FOUND)
                test.eq(err:retryable(), false)
            end)
        end

        local function run_case(spec)
            local subject_id = uuid.v7()
            local groups = {"app.await_authority:member", "app.await_authority:privileged"}
            local _, insert_err = db:execute(
                "INSERT INTO await_authority_subjects (subject_id, groups_json, active) VALUES ($1, $2, 1)",
                {subject_id, assert(json.encode(groups))})
            test.is_nil(insert_err)
            local issued_actor = assert(security.new_actor(subject_id, {marker = spec.name, ceiling = spec.ceiling}))
            local issued_scope = assert(security.named_scope("app.await_authority:privileged"))
            local token, token_err = token_store:create(issued_actor, issued_scope)
            test.is_nil(token_err)
            token = test.not_nil(token) :: string
            local _, credential_err = db:execute("UPDATE await_authority_subjects SET token = $1, credential_id = $2 WHERE subject_id = $2", {token, subject_id})
            test.is_nil(credential_err)
            local actor, scope, validation_err = token_store:validate(token)
            test.is_nil(validation_err)
            test.eq(actor:id(), subject_id)
            test.eq(scope:evaluate(actor, "funcs.call", "app.await_authority:protected_effect"), spec.ceiling and "undefined" or "allow")

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

            if spec.mutate then spec.mutate(subject_id, token) end
            local live_actor, live_scope, live_err = token_store:validate(token)
            if spec.invalid_credential then
                test.not_nil(live_err)
                test.is_nil(live_actor)
                test.is_nil(live_scope)
            else
                test.is_nil(live_err)
                test.eq(live_actor:id(), subject_id)
                local expected = spec.expected_effects == 1 and "allow" or "undefined"
                test.eq(live_scope:evaluate(live_actor, "funcs.call", "app.await_authority:protected_effect"), expected,
                    "native subject_lookup evaluates current authority and ceiling")
            end
            local signalled, signal_err = funcs.new():with_actor(actor):with_scope(scope)
                :call("app.await_authority:signal", {dataflow_id = dataflow_id, signal_id = signal_id})
            test.is_nil(signal_err)
            test.not_nil(signalled)
            test.is_true(wait_until(function()
                local status = assert(dataflow_repo.get(dataflow_id)).status
                return status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE
            end, 10000), "resumed workflow reaches a terminal result")
            local effects = assert(db:query("SELECT effects FROM await_authority_subjects WHERE subject_id = $1", {subject_id}))
            test.eq(effects[1].effects, spec.expected_effects, "current authority and ceiling govern the post-Await effect")
            test.eq(assert(dataflow_repo.get(dataflow_id)).status, spec.expected_effects == 1 and
                consts.STATUS.COMPLETED_SUCCESS or consts.STATUS.COMPLETED_FAILURE)
        end

        test.it("denies a protected node after membership removal during Await", function()
            run_case({name = "group_reduction", expected_effects = 0, mutate = function(subject_id)
                local _, err = db:execute("UPDATE await_authority_subjects SET groups_json = $1 WHERE subject_id = $2",
                    {assert(json.encode({"app.await_authority:member"})), subject_id})
                test.is_nil(err)
            end})
        end)

        test.it("preserves an explicit native credential ceiling across Await", function()
            run_case({name = "credential_ceiling", expected_effects = 0,
                ceiling = {groups = {"app.await_authority:member"}}})
        end)

        test.it("refuses a subject disabled during Await", function()
            run_case({name = "subject_disabled", expected_effects = 0, invalid_credential = true,
                mutate = function(subject_id)
                    local _, err = db:execute("UPDATE await_authority_subjects SET active = 0 WHERE subject_id = $1", {subject_id})
                    test.is_nil(err)
                end})
        end)

        test.it("refuses a credential revoked during Await", function()
            run_case({name = "credential_revoked", expected_effects = 0, invalid_credential = true,
                mutate = function(_, token)
                    local ok, err = token_store:revoke(token)
                    test.is_nil(err)
                    test.is_true(ok)
                end})
        end)

        test.it("propagates an unavailable authority group on resume", function()
            run_case({name = "authority_unavailable", expected_effects = 0, invalid_credential = true,
                mutate = function(subject_id)
                    local _, err = db:execute("UPDATE await_authority_subjects SET groups_json = $1 WHERE subject_id = $2",
                        {assert(json.encode({"app.await_authority:unavailable"})), subject_id})
                    test.is_nil(err)
                end})
        end)

        test.it("resumes the same principal when authority remains valid", function()
            run_case({name = "authority_unchanged", expected_effects = 1})
        end)
    end)
end

return {run_tests = test.run_cases(run_tests)}
