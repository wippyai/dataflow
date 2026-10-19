local sql = require("sql")
local json = require("json")
local security = require("security")
local boot = require("wait_for_boot")
local authority = require("authority")

local function run()
    boot.run()
    local db = assert(sql.get("app:db"))
    local _, schema_err = authority.create_schema(db)
    if schema_err then db:release(); error(schema_err) end
    local _, insert_err = db:execute([[INSERT INTO await_authority_subjects
        (subject_id, groups_json, active, credential_id) VALUES ($1, $2, 1, $1)
        ON CONFLICT(subject_id) DO NOTHING]], {"dataflow.test", assert(json.encode({"app:test_runtime"}))})
    if insert_err then db:release(); error(insert_err) end
    local tokens = assert(security.token_store("app.await_authority:tokens"))
    local actor = assert(security.new_actor("dataflow.test", {}))
    local scope = assert(security.named_scope("app:test_runtime"))
    local token, token_err = tokens:create(actor, scope)
    if token_err then db:release(); error(token_err) end
    local _, update_err = db:execute("UPDATE await_authority_subjects SET token = $1 WHERE subject_id = $2", {token, actor:id()})
    db:release()
    if update_err then error(update_err) end
    return true
end

return {run = run}
