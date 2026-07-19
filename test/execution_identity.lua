local json = require("json")
local process = require("process")
local security = require("security")
local dataflow_repo = require("dataflow_repo")

local M = {}

local function failure(message: any): any
    return { success = false, error = tostring(message or "unknown error") }
end

local function policy_ids(scope: any): (string[]?, string?)
    if not scope or type(scope.policies) ~= "function" then
        return nil, "current scope is unavailable"
    end
    local ids = {} :: string[]
    for _, policy in ipairs(scope:policies()) do
        local id = policy and tostring(policy:id()) or ""
        if id == "" then return nil, "scope contains an invalid policy" end
        ids[#ids + 1] = id
    end
    table.sort(ids)
    return ids, nil
end

local function reconstruct_scope(ids: any): (any?, string?)
    if type(ids) ~= "table" then return nil, "persisted policies are invalid" end
    local policies = {} :: security.Policy[]
    for _, id in ipairs(ids) do
        local policy, policy_err = security.policy(tostring(id))
        if policy_err or not policy then
            return nil, tostring(policy_err or ("persisted policy is unavailable: " .. tostring(id)))
        end
        policies[#policies + 1] = policy
    end
    local scope, scope_err = security.new_scope(policies)
    if scope_err or not scope then
        return nil, tostring(scope_err or "test scope is unavailable")
    end
    return scope, nil
end

function M.capture(): any
    local actor = security.actor()
    local actor_id = actor and tostring(actor:id()) or ""
    if actor_id == "" then return failure("current actor is unavailable") end
    local ids, ids_err = policy_ids(security.scope())
    if ids_err or not ids then return failure(ids_err) end

    local actor_context, context_err = json.encode({ policies = ids })
    if context_err or not actor_context then
        return failure(context_err or "identity encoding failed")
    end
    return {
        success = true,
        actor_id = actor_id,
        actor_context = actor_context,
    }
end

function M.spawn_orchestrator(args: any): any
    if type(args) ~= "table" then return failure("args must be a table") end
    local dataflow_id = tostring(args.dataflow_id or "")
    local process_id = tostring(args.process_id or "")
    local host_id = tostring(args.host_id or "")
    if dataflow_id == "" then return failure("dataflow_id is required") end
    if process_id == "" then return failure("process_id is required") end
    if host_id == "" then return failure("host_id is required") end

    local workflow, workflow_err = dataflow_repo.get(dataflow_id)
    if workflow_err or type(workflow) ~= "table" then
        return failure(workflow_err or "dataflow not found")
    end

    local actor_id = tostring(workflow.actor_id or "")
    if actor_id == "" then return failure("persisted actor is unavailable") end
    local actor_context, context_err = json.decode(tostring(workflow.actor_context or ""))
    if context_err or type(actor_context) ~= "table" or type(actor_context.policies) ~= "table" then
        return failure(context_err or "persisted test identity is invalid")
    end

    local actor, actor_err = security.new_actor(actor_id, {})
    if actor_err or not actor then return failure(actor_err or "actor reconstruction failed") end
    local scope, scope_err = reconstruct_scope(actor_context.policies)
    if scope_err or not scope then return failure(scope_err) end

    local pid, spawn_err = process.with_context({})
        :with_actor(actor)
        :with_scope(scope)
        :spawn(process_id, host_id, args.args or { dataflow_id = dataflow_id })
    if spawn_err or not pid then return failure(spawn_err or "spawn failed") end
    return { success = true, pid = tostring(pid) }
end

return M
