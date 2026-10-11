local sql = require("sql")
local json = require("json")
local security = require("security")
local flow = require("flow")
local client = require("client")

local M = {}

function M.lookup(args)
    local db = assert(sql.get("app:db"))
    local rows, err = db:query("SELECT groups_json, active FROM await_authority_subjects WHERE subject_id = $1", {args.subject_id})
    db:release()
    if err then error(err) end
    if #rows ~= 1 or rows[1].active ~= 1 then
        return {success = false, error = {kind = "PermissionDenied", message = "subject is inactive"}}
    end
    local groups = assert(json.decode(rows[1].groups_json))
    return {subject_id = args.subject_id, groups = groups, meta = args.actor_meta or {}}
end

function M.start(args)
    return flow.create():with_input({approved = false})
        :signal({signal_id = args.signal_id}):to("protected")
        :func("app.await_authority:protected_effect", {inputs = {required = {"default"}}})
        :as("protected"):to("@success"):start()
end

function M.signal(args)
    local c = assert(client.new())
    return c:signal(args.dataflow_id, args.signal_id, {approved = true})
end

function M.effect()
    local actor = assert(security.actor())
    local db = assert(sql.get("app:db"))
    local _, err = db:execute("UPDATE await_authority_subjects SET effects = effects + 1 WHERE subject_id = $1", {actor:id()})
    db:release()
    if err then error(err) end
    return {subject_id = actor:id(), executed = true}
end

return M
