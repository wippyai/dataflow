local test = require("test")
local registry = require("registry")
local security = require("security")
local funcs = require("funcs")
local time = require("time")
local json = require("json")
local client = require("client")
local flow = require("flow")
local consts = require("consts")
local dataflow_repo = require("dataflow_repo")
local execution_frame = require("execution_frame")
local sql = require("sql")
local uuid = require("uuid")
local authority = require("authority")

local M = {}

function M.run_workflow()
    local actor, scope = security.actor(), security.scope()
    test.eq(actor:meta().role, "admin")
    test.eq(#scope:policies(), 514)
    test.eq(scope:evaluate(actor, "scope.test", "protected"), "deny")
    local c = assert(client.new())
    local id, start_err = flow.create():with_input({ message = "large scope" })
        :func("userspace.dataflow.node.func:test_func"):to("@success"):start()
    test.is_nil(start_err)
    id = test.not_nil(id)
    local status, status_err
    for _ = 1, 100 do
        status, status_err = c:get_status(id)
        test.is_nil(status_err)
        if status == consts.STATUS.COMPLETED_SUCCESS or status == consts.STATUS.COMPLETED_FAILURE then break end
        time.sleep("50ms")
    end
    test.eq(status, consts.STATUS.COMPLETED_SUCCESS)
    local row, read_err = dataflow_repo.get(id)
    test.is_nil(read_err)
    local context = assert(json.decode(row.actor_context))
    test.eq(#context.policy_ids, #scope:policies())
    test.eq(context.actor_meta.role, "admin")
    local restored_actor, restored_scope, restore_err = execution_frame.reconstruct(row.actor_id, row.actor_context)
    test.is_nil(restore_err)
    test.eq(#restored_scope:policies(), #scope:policies())
    test.eq(restored_scope:evaluate(restored_actor, "scope.test", "protected"), "deny")
    return id
end

local function define_tests()
    test.describe("large execution scopes", function()
        local ids = {}
        test.after_each(function()
            if #ids == 0 then return end
            local changes = assert(registry.snapshot()):changes()
            for _, id in ipairs(ids) do changes:delete(id) end
            local _, err = changes:apply()
            test.is_nil(err)
            ids = {}
        end)

        test.it("starts and observes a workflow under an admin-sized native scope", function()
            local changes = assert(registry.snapshot()):changes()
            for index = 1, 513 do
                local id = string.format("app.large_scope:policy_%04d", index)
                ids[#ids + 1] = id
                changes:create({id = id, kind = "security.policy", data = {
                    groups = {"app.large_scope:group"},
                    policy = {resources = "protected", actions = "scope.test",
                        effect = index == 513 and "deny" or "allow"},
                }})
            end
            local _, apply_err = changes:apply()
            test.is_nil(apply_err)
            local policies = {assert(security.policy("app:test_policy"))}
            for _, id in ipairs(ids) do policies[#policies + 1] = assert(security.policy(id)) end
            local actor = assert(security.new_actor(uuid.v7(), {role = "admin"}))
            local scope = assert(security.new_scope(policies))
            local db = assert(sql.get("app:db"))
            local _, schema_err = authority.create_schema(db)
            test.is_nil(schema_err)
            local tokens = assert(security.token_store("app.await_authority:tokens"))
            local token = assert(tokens:create(actor, scope))
            local _, insert_err = db:execute([[INSERT INTO await_authority_subjects
                (subject_id, groups_json, active, credential_id, token) VALUES ($1, $2, 1, $1, $3)]],
                {actor:id(), assert(json.encode({"app:test_runtime", "app.large_scope:group"})), token})
            db:release()
            test.is_nil(insert_err)
            local id, err = funcs.new():with_actor(actor):with_scope(scope):call("app:large_scope_run")
            test.is_nil(err)
            test.not_nil(id)
        end)
    end)
end

M.run_tests = test.run_cases(define_tests)
return M
